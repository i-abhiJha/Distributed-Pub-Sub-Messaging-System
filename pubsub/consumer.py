"""
Consumer client.

Joins a consumer group with a list of topics. The group coordinator (the
controller broker) assigns a subset of the topic's partitions to this consumer.
The consumer:

  - sends a background heartbeat to the coordinator (so it stays in the group);
  - polls FETCH from each assigned partition's leader;
  - tracks in-memory next-offsets and commits them back via COMMIT_OFFSET;
  - re-joins on ERR_REBALANCE_NEEDED (e.g. after a peer crashes or joins).

Load balancing happens at JOIN_GROUP: the coordinator round-robins partitions
across the alive members of the group. Multiple consumers in the same group
split the work; multiple groups on the same topic each see every message
(fan-out across groups).
"""

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

from pubsub.protocol import (
    COMMIT_OFFSET, ERR_NOT_CONTROLLER, ERR_NOT_LEADER, ERR_REBALANCE_NEEDED,
    ERR_UNKNOWN_MEMBER, FETCH, FETCH_OFFSET, GROUP_HEARTBEAT, JOIN_GROUP,
    LEAVE_GROUP, METADATA, OK, request,
)

log = logging.getLogger("consumer")


class Consumer:
    HEARTBEAT_INTERVAL_S = 2.0

    def __init__(self, bootstrap_brokers: List[str], group_id: str, topics: List[str]):
        self.bootstrap = [self._parse(b) for b in bootstrap_brokers]
        self.group_id = group_id
        self.topics = topics
        self.member_id: Optional[str] = None
        self.generation: int = -1

        self._lock = threading.Lock()
        self._assignments: List[dict] = []  # [{topic, partition, leader_host, leader_port}]
        self._next_offsets: Dict[Tuple[str, int], int] = {}
        self._metadata = None
        self._controller: Optional[Tuple[str, int]] = None
        self._stopped = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None

    @staticmethod
    def _parse(addr):
        h, _, p = addr.partition(":")
        return (h, int(p))

    # ------------------------------------------------------------------
    def _fetch_metadata(self):
        last_err = None
        candidates = self.bootstrap[:]
        if self._metadata:
            candidates += [(b["host"], b["port"]) for b in self._metadata["brokers"].values()]
        for host, port in candidates:
            try:
                resp = request(host, port, {"type": METADATA}, timeout=3.0)
                if resp.get("status") == OK:
                    self._metadata = resp
                    self._controller = (resp["controller"]["host"], resp["controller"]["port"])
                    return resp
            except Exception as e:
                last_err = e
        raise ConnectionError(f"failed to fetch metadata: {last_err}")

    def _coordinator(self):
        if self._controller is None:
            self._fetch_metadata()
        return self._controller

    # ------------------------------------------------------------------
    def join(self):
        """Join the group, fetch assignments, start heartbeat thread."""
        self._fetch_metadata()
        self._do_join()
        # Restore committed offsets so we resume where we left off.
        self._load_committed_offsets()
        if self._heartbeat_thread is None:
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()
        log.info("consumer %s joined group '%s' gen=%d assignments=%s",
                 self.member_id, self.group_id, self.generation,
                 [(a["topic"], a["partition"]) for a in self._assignments])

    def _do_join(self):
        host, port = self._coordinator()
        try:
            resp = request(host, port, {
                "type": JOIN_GROUP,
                "group_id": self.group_id,
                "member_id": self.member_id,
                "topics": self.topics,
            }, timeout=5.0)
        except Exception as e:
            log.warning("JOIN_GROUP transport error: %s; refreshing metadata", e)
            self._fetch_metadata()
            host, port = self._coordinator()
            resp = request(host, port, {
                "type": JOIN_GROUP, "group_id": self.group_id,
                "member_id": self.member_id, "topics": self.topics,
            }, timeout=5.0)

        if resp.get("status") == ERR_NOT_CONTROLLER:
            c = resp["controller"]
            self._controller = (c["host"], c["port"])
            resp = request(c["host"], c["port"], {
                "type": JOIN_GROUP, "group_id": self.group_id,
                "member_id": self.member_id, "topics": self.topics,
            }, timeout=5.0)

        if resp.get("status") != OK:
            raise RuntimeError(f"JOIN_GROUP failed: {resp}")

        with self._lock:
            self.member_id = resp["member_id"]
            self.generation = resp["generation"]
            new_assignments = resp["assignments"]
            # Clear next_offsets for partitions no longer assigned.
            assigned_set = {(a["topic"], a["partition"]) for a in new_assignments}
            for tp in list(self._next_offsets.keys()):
                if tp not in assigned_set:
                    del self._next_offsets[tp]
            self._assignments = new_assignments

    def _load_committed_offsets(self):
        if not self._assignments:
            return
        host, port = self._coordinator()
        try:
            resp = request(host, port, {
                "type": FETCH_OFFSET, "group_id": self.group_id,
                "topics": [{"topic": a["topic"], "partition": a["partition"]}
                           for a in self._assignments],
            }, timeout=3.0)
            if resp.get("status") == OK:
                with self._lock:
                    for o in resp["offsets"]:
                        tp = (o["topic"], o["partition"])
                        # If we already have a higher in-flight offset, keep it.
                        self._next_offsets.setdefault(tp, o["offset"])
        except Exception as e:
            log.warning("FETCH_OFFSET failed: %s", e)

    def _heartbeat_loop(self):
        while not self._stopped.wait(self.HEARTBEAT_INTERVAL_S):
            if self.member_id is None:
                continue
            host, port = self._coordinator()
            try:
                resp = request(host, port, {
                    "type": GROUP_HEARTBEAT, "group_id": self.group_id,
                    "member_id": self.member_id, "generation": self.generation,
                }, timeout=3.0)
            except Exception as e:
                log.warning("heartbeat transport error: %s", e)
                try:
                    self._fetch_metadata()
                except Exception:
                    pass
                continue
            status = resp.get("status")
            if status == ERR_REBALANCE_NEEDED:
                log.info("rebalance needed; re-joining")
                try:
                    self._do_join()
                except Exception as e:
                    log.warning("re-join failed: %s", e)
            elif status == ERR_UNKNOWN_MEMBER:
                log.info("kicked out of group; re-joining")
                with self._lock:
                    self.member_id = None
                try:
                    self._do_join()
                    self._load_committed_offsets()
                except Exception as e:
                    log.warning("re-join failed: %s", e)

    # ------------------------------------------------------------------
    def poll(self, max_records_per_partition: int = 50, timeout_s: float = 1.0) -> List[dict]:
        """One round of fetches across all assigned partitions. Returns a list
        of records (in arrival-from-each-partition order, partitions interleaved)."""
        out = []
        deadline = time.time() + timeout_s
        with self._lock:
            assignments = list(self._assignments)
        for a in assignments:
            if time.time() > deadline:
                break
            tp = (a["topic"], a["partition"])
            from_offset = self._next_offsets.get(tp, 0)
            try:
                resp = request(a["leader_host"], a["leader_port"], {
                    "type": FETCH, "topic": a["topic"], "partition": a["partition"],
                    "from_offset": from_offset, "max_records": max_records_per_partition,
                }, timeout=3.0)
            except Exception as e:
                log.warning("FETCH from %s:%s for %s-%d failed: %s",
                            a["leader_host"], a["leader_port"], a["topic"], a["partition"], e)
                # Maybe the leader moved; refresh by re-joining.
                try:
                    self._fetch_metadata()
                    self._do_join()
                except Exception as je:
                    log.warning("re-join after FETCH failure failed: %s", je)
                continue
            status = resp.get("status")
            if status == ERR_NOT_LEADER:
                log.info("FETCH %s-%d: not leader anymore, re-joining",
                         a["topic"], a["partition"])
                try:
                    self._fetch_metadata()
                    self._do_join()
                except Exception as e:
                    log.warning("re-join after NOT_LEADER failed: %s", e)
                continue
            if status != OK:
                log.warning("FETCH returned %s: %s", status, resp)
                continue
            recs = resp.get("records", [])
            if recs:
                for r in recs:
                    r["topic"] = a["topic"]
                    r["partition"] = a["partition"]
                with self._lock:
                    self._next_offsets[tp] = recs[-1]["offset"] + 1
                out.extend(recs)
        return out

    def commit(self):
        """Commit current next-offsets to the coordinator."""
        with self._lock:
            offsets = [
                {"topic": t, "partition": p, "offset": off}
                for (t, p), off in self._next_offsets.items()
            ]
        if not offsets:
            return
        host, port = self._coordinator()
        resp = request(host, port, {
            "type": COMMIT_OFFSET, "group_id": self.group_id,
            "member_id": self.member_id, "generation": self.generation,
            "offsets": offsets,
        }, timeout=3.0)
        if resp.get("status") != OK:
            log.warning("commit returned %s", resp)

    def close(self):
        self._stopped.set()
        if self.member_id:
            try:
                host, port = self._coordinator()
                request(host, port, {
                    "type": LEAVE_GROUP, "group_id": self.group_id,
                    "member_id": self.member_id,
                }, timeout=2.0)
            except Exception:
                pass
