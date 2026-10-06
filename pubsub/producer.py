"""
Producer client.

Workflow:
  1. Bootstrap with a list of broker addresses.
  2. Fetch cluster metadata (tries each bootstrap broker until one responds).
  3. On send(topic, value, key):
       - choose a partition (key hash if provided, round-robin otherwise);
       - send PRODUCE to the partition's leader;
       - on ERR_NOT_LEADER, refresh metadata and retry up to N times.

Partitioning preserves per-key ordering: every record with the same key lands
on the same partition (and the leader for that partition serializes appends),
so all records for `user-42` are strictly ordered.
"""

import logging
import threading
import time
from typing import List, Optional

from pubsub.protocol import (
    CREATE_TOPIC, ERR_NOT_CONTROLLER, ERR_NOT_LEADER, ERR_UNKNOWN_TOPIC,
    METADATA, OK, PRODUCE, request,
)

log = logging.getLogger("producer")


class Producer:
    def __init__(self, bootstrap_brokers: List[str], max_retries: int = 5):
        """
        bootstrap_brokers: ["host:port", ...]
        """
        self.bootstrap = [self._parse(b) for b in bootstrap_brokers]
        self.max_retries = max_retries
        self._lock = threading.Lock()
        self._metadata = None  # last-known metadata
        self._rr_counter = 0   # round-robin counter for keyless sends

    @staticmethod
    def _parse(addr):
        h, _, p = addr.partition(":")
        return (h, int(p))

    # ------------------------------------------------------------------
    def _fetch_metadata(self):
        last_err = None
        for host, port in self.bootstrap + self._known_broker_addrs():
            try:
                resp = request(host, port, {"type": METADATA}, timeout=3.0)
                if resp.get("status") == OK:
                    with self._lock:
                        self._metadata = resp
                    return resp
            except Exception as e:
                last_err = e
        raise ConnectionError(f"failed to fetch metadata from any broker: {last_err}")

    def _known_broker_addrs(self):
        with self._lock:
            if not self._metadata:
                return []
            return [(b["host"], b["port"]) for b in self._metadata["brokers"].values()]

    def _metadata_cached(self):
        with self._lock:
            return self._metadata

    # ------------------------------------------------------------------
    def create_topic(self, topic, partitions=1, replication_factor=1):
        """Send CREATE_TOPIC to the controller."""
        meta = self._fetch_metadata()
        controller = meta["controller"]
        resp = request(controller["host"], controller["port"], {
            "type": CREATE_TOPIC,
            "topic": topic,
            "partitions": partitions,
            "replication_factor": replication_factor,
        }, timeout=5.0)
        if resp.get("status") == ERR_NOT_CONTROLLER:
            c = resp["controller"]
            resp = request(c["host"], c["port"], {
                "type": CREATE_TOPIC, "topic": topic,
                "partitions": partitions, "replication_factor": replication_factor,
            }, timeout=5.0)
        if resp.get("status") != OK:
            raise RuntimeError(f"create_topic failed: {resp}")
        # Refresh metadata so we have the new topic.
        self._fetch_metadata()
        return resp

    # ------------------------------------------------------------------
    def _pick_partition(self, topic, key: Optional[str], tinfo: dict) -> int:
        n = tinfo["partitions"]
        if key is None:
            with self._lock:
                p = self._rr_counter % n
                self._rr_counter += 1
            return p
        # Stable hash. Python's hash() is randomized per process, so use a fixed
        # variant: simple FNV-1a 32-bit to keep the same key on the same partition
        # across all producer instances.
        h = 2166136261
        for b in key.encode("utf-8"):
            h ^= b
            h = (h * 16777619) & 0xFFFFFFFF
        return h % n

    def send(self, topic: str, value, key: Optional[str] = None) -> dict:
        """Synchronous send. Returns the broker's response dict."""
        meta = self._metadata_cached() or self._fetch_metadata()

        last_err = None
        for attempt in range(self.max_retries):
            tinfo = meta["topics"].get(topic)
            if tinfo is None:
                # Maybe topic was created since our last metadata refresh.
                meta = self._fetch_metadata()
                tinfo = meta["topics"].get(topic)
                if tinfo is None:
                    raise RuntimeError(f"unknown topic: {topic}")
            partition = self._pick_partition(topic, key, tinfo)
            leader_id = tinfo["assignments"][partition]["leader"]
            leader = meta["brokers"].get(str(leader_id)) if leader_id is not None else None
            if leader is None:
                log.warning("no leader for %s-%d yet; refreshing metadata", topic, partition)
                time.sleep(0.3)
                meta = self._fetch_metadata()
                continue
            try:
                resp = request(leader["host"], leader["port"], {
                    "type": PRODUCE, "topic": topic, "partition": partition,
                    "records": [{"key": key, "value": value}],
                }, timeout=5.0)
            except Exception as e:
                log.warning("PRODUCE to leader %s failed: %s", leader_id, e)
                last_err = e
                meta = self._fetch_metadata()
                continue
            status = resp.get("status")
            if status == OK:
                return {"partition": partition, "offset": resp["base_offset"]}
            if status in (ERR_NOT_LEADER, ERR_UNKNOWN_TOPIC):
                log.info("stale metadata (%s); refreshing", status)
                meta = self._fetch_metadata()
                continue
            raise RuntimeError(f"PRODUCE failed: {resp}")
        raise RuntimeError(f"send failed after {self.max_retries} retries: {last_err}")
