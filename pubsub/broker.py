"""
Broker process.

A broker plays three overlapping roles:

1. Data broker — hosts partition replicas. The replica designated as "leader"
   for a partition serves PRODUCE and FETCH; followers receive synchronous
   REPLICATE pushes from the leader.

2. Controller — exactly one broker in the cluster. Holds authoritative cluster
   metadata (broker registry, topic assignments, ISR). Detects broker failures
   via missed heartbeats and reassigns partition leadership. In this MVP the
   controller is the broker started with no --controller flag (the bootstrap).
   Controller-failover is intentionally out of scope.

3. Group coordinator — also lives on the controller. Manages consumer-group
   membership, partition-assignment, offset commits, and generation-bumped
   rebalances on member join/leave/timeout.

Threading: socketserver.ThreadingTCPServer gives a thread per inbound
connection. Long-lived background threads run heartbeats, failure detection,
and group rebalancing. Cross-thread state is protected by three RLocks:
metadata, logs, and groups.
"""

import argparse
import logging
import os
import socket
import socketserver
import threading
import time
import uuid
from typing import Dict, Optional, Tuple

from pubsub.log import PartitionLog
from pubsub.protocol import (
    BROKER_HEARTBEAT, COMMIT_OFFSET, CREATE_TOPIC, ERR_NOT_CONTROLLER,
    ERR_NOT_LEADER, ERR_REBALANCE_NEEDED, ERR_UNKNOWN, ERR_UNKNOWN_MEMBER,
    ERR_UNKNOWN_TOPIC, FETCH, FETCH_OFFSET, GROUP_HEARTBEAT, JOIN_GROUP,
    LEAVE_GROUP, METADATA, METADATA_PUSH, OK, PRODUCE, REGISTER_BROKER,
    REPLICATE, recv_message, request, send_message,
)

log = logging.getLogger("broker")

BROKER_HEARTBEAT_INTERVAL_S = 2.0
BROKER_FAIL_TIMEOUT_S = 6.0
GROUP_MEMBER_TIMEOUT_S = 8.0


class Broker:
    def __init__(self, broker_id, host, port, data_dir,
                 controller_host=None, controller_port=None):
        self.id = broker_id
        self.host = host
        self.port = port
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self.is_controller = controller_host is None
        self.controller_host = host if self.is_controller else controller_host
        self.controller_port = port if self.is_controller else controller_port

        # Cluster metadata. Authoritative on controller; cached snapshot elsewhere.
        self.meta_lock = threading.RLock()
        self.brokers: Dict[int, dict] = {}
        self.topics: Dict[str, dict] = {}
        self.metadata_version = 0

        # Local partition logs.
        self.logs_lock = threading.RLock()
        self.partition_logs: Dict[Tuple[str, int], PartitionLog] = {}

        # Consumer-group state (controller-only).
        self.groups_lock = threading.RLock()
        self.groups: Dict[str, dict] = {}

        self.server: Optional[socketserver.ThreadingTCPServer] = None
        self.stopped = threading.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self):
        if self.is_controller:
            with self.meta_lock:
                self.brokers[self.id] = {
                    "id": self.id, "host": self.host, "port": self.port,
                    "last_heartbeat": time.time(), "alive": True,
                }

        self.server = _ReusableTCPServer((self.host, self.port), _make_handler(self))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        log.info("broker %s listening on %s:%s (controller=%s)",
                 self.id, self.host, self.port, self.is_controller)

        if not self.is_controller:
            self._register_with_controller()

        threading.Thread(target=self._broker_heartbeat_loop, daemon=True).start()
        if self.is_controller:
            threading.Thread(target=self._failure_detector_loop, daemon=True).start()
            threading.Thread(target=self._group_rebalance_loop, daemon=True).start()

    def stop(self):
        self.stopped.set()
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        with self.logs_lock:
            for pl in self.partition_logs.values():
                pl.close()
        log.info("broker %s stopped", self.id)

    def _register_with_controller(self):
        for attempt in range(20):
            try:
                resp = request(self.controller_host, self.controller_port, {
                    "type": REGISTER_BROKER,
                    "broker_id": self.id,
                    "host": self.host,
                    "port": self.port,
                })
                if resp.get("status") == OK:
                    self._apply_metadata(resp["brokers"], resp["topics"], resp["version"])
                    log.info("broker %s registered; metadata v%d", self.id, resp["version"])
                    return
            except (ConnectionRefusedError, ConnectionError, socket.timeout, OSError) as e:
                log.warning("controller not yet reachable (attempt %d): %s", attempt + 1, e)
                time.sleep(0.5)
        raise RuntimeError("failed to register with controller after 20 attempts")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    def _broker_heartbeat_loop(self):
        while not self.stopped.wait(BROKER_HEARTBEAT_INTERVAL_S):
            if self.is_controller:
                with self.meta_lock:
                    if self.id in self.brokers:
                        self.brokers[self.id]["last_heartbeat"] = time.time()
                continue
            try:
                resp = request(self.controller_host, self.controller_port, {
                    "type": BROKER_HEARTBEAT,
                    "broker_id": self.id,
                    "metadata_version": self.metadata_version,
                }, timeout=3.0)
                if resp.get("metadata_stale"):
                    self._apply_metadata(resp["brokers"], resp["topics"], resp["version"])
            except Exception as e:
                log.warning("heartbeat to controller failed: %s", e)

    def _failure_detector_loop(self):
        while not self.stopped.wait(1.0):
            now = time.time()
            dead = []
            with self.meta_lock:
                for bid, b in self.brokers.items():
                    if not b.get("alive", True):
                        continue
                    if now - b["last_heartbeat"] > BROKER_FAIL_TIMEOUT_S:
                        dead.append(bid)
            if dead:
                log.warning("controller detected dead brokers: %s", dead)
                self._handle_dead_brokers(dead)

    def _handle_dead_brokers(self, dead_ids):
        with self.meta_lock:
            for bid in dead_ids:
                if bid in self.brokers:
                    self.brokers[bid]["alive"] = False
            for topic, tinfo in self.topics.items():
                for p_idx, assignment in enumerate(tinfo["assignments"]):
                    assignment["isr"] = [r for r in assignment["isr"] if r not in dead_ids]
                    if assignment["leader"] in dead_ids:
                        new_leader = None
                        for r in assignment["isr"]:
                            if self.brokers.get(r, {}).get("alive"):
                                new_leader = r
                                break
                        if new_leader is None:
                            for r in assignment["replicas"]:
                                if r not in dead_ids and self.brokers.get(r, {}).get("alive"):
                                    new_leader = r
                                    break
                        if new_leader != assignment["leader"]:
                            log.warning("partition %s-%d: leader %s -> %s",
                                        topic, p_idx, assignment["leader"], new_leader)
                        assignment["leader"] = new_leader
            self.metadata_version += 1
        self._broadcast_metadata()
        with self.groups_lock:
            for g in self.groups.values():
                g["rebalance_needed"] = True

    def _broadcast_metadata(self):
        with self.meta_lock:
            snapshot = self._meta_snapshot()
            targets = [(b["id"], b["host"], b["port"])
                       for b in self.brokers.values()
                       if b.get("alive") and b["id"] != self.id]
        for bid, host, port in targets:
            try:
                request(host, port, {"type": METADATA_PUSH, **snapshot}, timeout=2.0)
            except Exception as e:
                log.warning("metadata push to broker %s (%s:%s) failed: %s", bid, host, port, e)

    def _meta_snapshot(self):
        # Caller holds meta_lock.
        return {
            "brokers": {
                str(bid): {k: v for k, v in b.items() if k != "last_heartbeat"}
                for bid, b in self.brokers.items()
            },
            "topics": self.topics,
            "version": self.metadata_version,
        }

    def _apply_metadata(self, brokers, topics, version):
        with self.meta_lock:
            if self.metadata_version != 0 and version <= self.metadata_version:
                return
            self.brokers = {int(bid): b for bid, b in brokers.items()}
            self.topics = topics
            self.metadata_version = version
        self._ensure_partition_logs()

    def _ensure_partition_logs(self):
        with self.meta_lock:
            mine = []
            for topic, tinfo in self.topics.items():
                for p_idx, assignment in enumerate(tinfo["assignments"]):
                    if self.id in assignment["replicas"]:
                        mine.append((topic, p_idx))
        with self.logs_lock:
            for tp in mine:
                if tp not in self.partition_logs:
                    self.partition_logs[tp] = PartitionLog(self.data_dir, tp[0], tp[1])

    def _group_rebalance_loop(self):
        while not self.stopped.wait(2.0):
            now = time.time()
            with self.groups_lock:
                for group_id, g in self.groups.items():
                    timed_out = [
                        mid for mid, m in g["members"].items()
                        if now - m["last_heartbeat"] > GROUP_MEMBER_TIMEOUT_S
                    ]
                    if timed_out:
                        for mid in timed_out:
                            log.warning("group %s: member %s timed out", group_id, mid)
                            del g["members"][mid]
                    if (timed_out or g.get("rebalance_needed")) and g["members"]:
                        self._reassign_group(group_id)
                    g["rebalance_needed"] = False

    def _reassign_group(self, group_id):
        # Caller holds groups_lock.
        g = self.groups[group_id]
        topics = set()
        for m in g["members"].values():
            topics.update(m["topics"])
        all_partitions = []
        with self.meta_lock:
            for t in sorted(topics):
                tinfo = self.topics.get(t)
                if tinfo is None:
                    continue
                for p_idx in range(tinfo["partitions"]):
                    all_partitions.append((t, p_idx))
        member_ids = sorted(g["members"].keys())
        for m in g["members"].values():
            m["assignments"] = []
        for i, (t, p) in enumerate(all_partitions):
            mid = member_ids[i % len(member_ids)]
            g["members"][mid]["assignments"].append({"topic": t, "partition": p})
        g["generation"] += 1
        log.info("group %s: rebalanced to gen %d, members=%s, partitions=%d",
                 group_id, g["generation"], member_ids, len(all_partitions))

    # ------------------------------------------------------------------
    # Request dispatch
    # ------------------------------------------------------------------
    def handle(self, msg):
        t = msg.get("type")
        try:
            handler = _DISPATCH.get(t)
            if handler is None:
                return {"status": ERR_UNKNOWN, "error": f"unknown type: {t}"}
            return handler(self, msg)
        except Exception as e:
            log.exception("handler error for %s", t)
            return {"status": ERR_UNKNOWN, "error": str(e)}

    # ---- handlers ----
    def _h_metadata(self, msg):
        with self.meta_lock:
            return {"status": OK, **self._meta_snapshot(),
                    "controller": {"host": self.controller_host, "port": self.controller_port}}

    def _h_create_topic(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER,
                    "controller": {"host": self.controller_host, "port": self.controller_port}}
        topic = msg["topic"]
        partitions = int(msg.get("partitions", 1))
        rf = int(msg.get("replication_factor", 1))
        with self.meta_lock:
            if topic in self.topics:
                return {"status": OK, "note": "already exists"}
            alive_ids = sorted([b["id"] for b in self.brokers.values() if b.get("alive")])
            if len(alive_ids) < rf:
                return {"status": ERR_UNKNOWN,
                        "error": f"replication_factor {rf} > alive brokers {len(alive_ids)}"}
            assignments = []
            for p in range(partitions):
                replicas = [alive_ids[(p + r) % len(alive_ids)] for r in range(rf)]
                assignments.append({
                    "leader": replicas[0],
                    "replicas": replicas,
                    "isr": list(replicas),
                })
            self.topics[topic] = {
                "partitions": partitions,
                "replication_factor": rf,
                "assignments": assignments,
            }
            self.metadata_version += 1
            log.info("created topic '%s' partitions=%d rf=%d assignments=%s",
                     topic, partitions, rf, assignments)
        self._ensure_partition_logs()
        self._broadcast_metadata()
        return {"status": OK}

    def _h_produce(self, msg):
        topic = msg["topic"]
        partition = int(msg["partition"])
        records = msg["records"]
        with self.meta_lock:
            tinfo = self.topics.get(topic)
            if tinfo is None:
                return {"status": ERR_UNKNOWN_TOPIC}
            assignment = tinfo["assignments"][partition]
            if assignment["leader"] != self.id:
                leader_id = assignment["leader"]
                leader = self.brokers.get(leader_id) if leader_id is not None else None
                return {"status": ERR_NOT_LEADER, "leader_id": leader_id,
                        "leader_host": leader["host"] if leader else None,
                        "leader_port": leader["port"] if leader else None}
            followers = [r for r in assignment["replicas"] if r != self.id]
            follower_addrs = [(self.brokers[f]["host"], self.brokers[f]["port"])
                              for f in followers
                              if self.brokers.get(f, {}).get("alive")]

        with self.logs_lock:
            pl = self.partition_logs.get((topic, partition))
        if pl is None:
            return {"status": ERR_UNKNOWN, "error": "log not initialized"}

        appended = []
        for rec in records:
            offset = pl.append(rec.get("key"), rec.get("value", ""))
            appended.append({"offset": offset, "key": rec.get("key"), "value": rec.get("value", "")})

        # Synchronous replication to alive followers (acks=all).
        replicate_msg = {"type": REPLICATE, "topic": topic, "partition": partition, "records": appended}
        for host, port in follower_addrs:
            try:
                request(host, port, replicate_msg, timeout=3.0)
            except Exception as e:
                log.warning("replication to %s:%s failed: %s", host, port, e)

        return {"status": OK,
                "base_offset": appended[0]["offset"] if appended else None,
                "offsets": [a["offset"] for a in appended]}

    def _h_fetch(self, msg):
        topic = msg["topic"]
        partition = int(msg["partition"])
        from_offset = int(msg.get("from_offset", 0))
        max_records = int(msg.get("max_records", 100))
        with self.meta_lock:
            tinfo = self.topics.get(topic)
            if tinfo is None:
                return {"status": ERR_UNKNOWN_TOPIC}
            assignment = tinfo["assignments"][partition]
            if assignment["leader"] != self.id:
                leader_id = assignment["leader"]
                leader = self.brokers.get(leader_id) if leader_id is not None else None
                return {"status": ERR_NOT_LEADER, "leader_id": leader_id,
                        "leader_host": leader["host"] if leader else None,
                        "leader_port": leader["port"] if leader else None}
        with self.logs_lock:
            pl = self.partition_logs.get((topic, partition))
        if pl is None:
            return {"status": ERR_UNKNOWN, "error": "log not initialized"}
        recs = pl.read(from_offset, max_records=max_records)
        return {"status": OK, "records": recs, "high_watermark": pl.high_watermark()}

    def _h_replicate(self, msg):
        topic = msg["topic"]
        partition = int(msg["partition"])
        records = msg["records"]
        with self.logs_lock:
            pl = self.partition_logs.get((topic, partition))
            if pl is None:
                pl = PartitionLog(self.data_dir, topic, partition)
                self.partition_logs[(topic, partition)] = pl
        for r in records:
            try:
                pl.append(r.get("key"), r.get("value", ""), offset=r["offset"])
            except ValueError as e:
                log.warning("replication offset mismatch on %s-%d: %s", topic, partition, e)
        return {"status": OK}

    def _h_register_broker(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER}
        bid = int(msg["broker_id"])
        with self.meta_lock:
            self.brokers[bid] = {
                "id": bid, "host": msg["host"], "port": int(msg["port"]),
                "last_heartbeat": time.time(), "alive": True,
            }
            self.metadata_version += 1
            snap = self._meta_snapshot()
        log.info("controller registered broker %s (%s:%s)", bid, msg["host"], msg["port"])
        threading.Thread(target=self._broadcast_metadata, daemon=True).start()
        return {"status": OK, **snap}

    def _h_broker_heartbeat(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER}
        bid = int(msg["broker_id"])
        client_ver = int(msg.get("metadata_version", 0))
        with self.meta_lock:
            if bid in self.brokers:
                self.brokers[bid]["last_heartbeat"] = time.time()
                if not self.brokers[bid].get("alive"):
                    self.brokers[bid]["alive"] = True
                    log.info("broker %s came back alive", bid)
                    # Bump version so others learn it's back.
                    self.metadata_version += 1
            stale = client_ver < self.metadata_version
            snap = self._meta_snapshot() if stale else {}
        if stale:
            return {"status": OK, "metadata_stale": True, **snap}
        return {"status": OK}

    def _h_metadata_push(self, msg):
        self._apply_metadata(msg["brokers"], msg["topics"], msg["version"])
        return {"status": OK}

    def _h_join_group(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER,
                    "controller": {"host": self.controller_host, "port": self.controller_port}}
        group_id = msg["group_id"]
        member_id = msg.get("member_id") or str(uuid.uuid4())
        topics = msg.get("topics", [])
        with self.groups_lock:
            if group_id not in self.groups:
                self.groups[group_id] = {
                    "generation": 0, "members": {}, "offsets": {}, "rebalance_needed": False,
                }
            g = self.groups[group_id]
            is_new = member_id not in g["members"]
            g["members"][member_id] = {
                "topics": topics,
                "last_heartbeat": time.time(),
                "assignments": g["members"].get(member_id, {}).get("assignments", []),
            }
            if is_new:
                self._reassign_group(group_id)
            assignments = list(g["members"][member_id]["assignments"])
            generation = g["generation"]
        # Resolve current leader address for each assigned partition.
        enriched = []
        with self.meta_lock:
            for a in assignments:
                tinfo = self.topics.get(a["topic"])
                if not tinfo:
                    continue
                pa = tinfo["assignments"][a["partition"]]
                lid = pa["leader"]
                if lid is None or lid not in self.brokers:
                    continue
                lb = self.brokers[lid]
                enriched.append({**a, "leader_host": lb["host"], "leader_port": lb["port"]})
        return {"status": OK, "member_id": member_id,
                "generation": generation, "assignments": enriched}

    def _h_group_heartbeat(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER}
        group_id = msg["group_id"]
        member_id = msg["member_id"]
        gen = int(msg.get("generation", -1))
        with self.groups_lock:
            g = self.groups.get(group_id)
            if g is None or member_id not in g["members"]:
                return {"status": ERR_UNKNOWN_MEMBER}
            if gen != g["generation"]:
                return {"status": ERR_REBALANCE_NEEDED, "generation": g["generation"]}
            g["members"][member_id]["last_heartbeat"] = time.time()
        return {"status": OK}

    def _h_commit_offset(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER}
        group_id = msg["group_id"]
        with self.groups_lock:
            g = self.groups.get(group_id)
            if g is None:
                return {"status": ERR_UNKNOWN_MEMBER}
            for o in msg["offsets"]:
                key = f"{o['topic']}:{o['partition']}"
                g["offsets"][key] = int(o["offset"])
        return {"status": OK}

    def _h_fetch_offset(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER}
        group_id = msg["group_id"]
        out = []
        with self.groups_lock:
            g = self.groups.get(group_id, {"offsets": {}})
            for o in msg["topics"]:
                key = f"{o['topic']}:{o['partition']}"
                out.append({"topic": o["topic"], "partition": o["partition"],
                            "offset": g["offsets"].get(key, 0)})
        return {"status": OK, "offsets": out}

    def _h_leave_group(self, msg):
        if not self.is_controller:
            return {"status": ERR_NOT_CONTROLLER}
        group_id = msg["group_id"]
        member_id = msg["member_id"]
        with self.groups_lock:
            g = self.groups.get(group_id)
            if g and member_id in g["members"]:
                del g["members"][member_id]
                if g["members"]:
                    self._reassign_group(group_id)
        return {"status": OK}


_DISPATCH = {
    METADATA: Broker._h_metadata,
    CREATE_TOPIC: Broker._h_create_topic,
    PRODUCE: Broker._h_produce,
    FETCH: Broker._h_fetch,
    REPLICATE: Broker._h_replicate,
    REGISTER_BROKER: Broker._h_register_broker,
    BROKER_HEARTBEAT: Broker._h_broker_heartbeat,
    METADATA_PUSH: Broker._h_metadata_push,
    JOIN_GROUP: Broker._h_join_group,
    GROUP_HEARTBEAT: Broker._h_group_heartbeat,
    COMMIT_OFFSET: Broker._h_commit_offset,
    FETCH_OFFSET: Broker._h_fetch_offset,
    LEAVE_GROUP: Broker._h_leave_group,
}


class _ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _make_handler(broker):
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            sock = self.request
            try:
                while True:
                    msg = recv_message(sock)
                    if msg is None:
                        return
                    resp = broker.handle(msg)
                    send_message(sock, resp)
            except (ConnectionResetError, BrokenPipeError, OSError):
                return
    return Handler


def main():
    parser = argparse.ArgumentParser(description="My Pub-Sub broker")
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--controller",
                        help="host:port of the controller broker; omit to BE the controller")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format=f"%(asctime)s [B{args.id}] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    chost, cport = None, None
    if args.controller:
        chost, _, cport_s = args.controller.partition(":")
        cport = int(cport_s)

    b = Broker(args.id, args.host, args.port, args.data_dir, chost, cport)
    b.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        b.stop()


if __name__ == "__main__":
    main()
