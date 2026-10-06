"""
Demo 3 — leader failover (fault tolerance).

  1. Create a topic with replication_factor=2.
  2. Produce a batch.
  3. Identify a partition whose leader is NOT broker 1 (the controller),
     so we can kill that leader without taking the cluster down.
  4. Kill that leader process.
  5. Wait for the controller's failure detector to promote a follower.
  6. Produce another batch — should succeed against the new leader.
  7. Consume from offset 0 — should see ALL messages (both batches).

This script needs to manage broker processes itself, so don't run it against
a cluster started with run_cluster.sh — it spawns its own cluster.
"""

import os
import shutil
import signal
import subprocess
import sys
import time

from pubsub.producer import Producer
from pubsub.consumer import Consumer
from pubsub.protocol import request, METADATA

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
BOOTSTRAP = ["127.0.0.1:9201", "127.0.0.1:9202", "127.0.0.1:9203"]
TOPIC = "failover-test"


def spawn_broker(broker_id, port, controller=None, data_root="data-failover"):
    data_dir = os.path.join(ROOT, data_root, f"broker-{broker_id}")
    log_path = os.path.join(ROOT, "logs", f"failover-broker-{broker_id}.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    args = [
        sys.executable, "-m", "pubsub.broker",
        "--id", str(broker_id),
        "--host", "127.0.0.1",
        "--port", str(port),
        "--data-dir", data_dir,
    ]
    if controller:
        args.extend(["--controller", controller])
    fout = open(log_path, "w")
    env = os.environ.copy()
    env["PYTHONPATH"] = ROOT
    return subprocess.Popen(args, stdout=fout, stderr=subprocess.STDOUT, env=env)


def main():
    # Clean up data from previous runs
    for d in ("data-failover",):
        full = os.path.join(ROOT, d)
        if os.path.exists(full):
            shutil.rmtree(full)

    print("spawning fresh 3-broker cluster on ports 9201-9203")
    procs = {}
    procs[1] = spawn_broker(1, 9201)
    time.sleep(1.0)
    procs[2] = spawn_broker(2, 9202, controller="127.0.0.1:9201")
    procs[3] = spawn_broker(3, 9203, controller="127.0.0.1:9201")
    time.sleep(1.5)

    try:
        p = Producer(BOOTSTRAP)
        p.create_topic(TOPIC, partitions=3, replication_factor=2)
        print(f"created topic '{TOPIC}' (partitions=3, RF=2)")

        print("\nproducing batch A (10 messages)...")
        for i in range(10):
            p.send(TOPIC, value=f"A-{i}", key=f"k{i}")

        # Find a partition whose leader is broker 2 or 3 (NOT controller=1).
        meta = request("127.0.0.1", 9201, {"type": METADATA})
        victim_leader = None
        victim_partition = None
        for p_idx, a in enumerate(meta["topics"][TOPIC]["assignments"]):
            if a["leader"] in (2, 3):
                victim_leader = a["leader"]
                victim_partition = p_idx
                break
        if victim_leader is None:
            print("could not find a non-controller leader; aborting")
            return
        print(f"\nidentified victim: broker {victim_leader} is leader for partition {victim_partition}")

        print(f"\n--- killing broker {victim_leader} ---")
        procs[victim_leader].send_signal(signal.SIGKILL)
        procs[victim_leader].wait()
        del procs[victim_leader]

        print("waiting for failure detector to promote a follower (up to 10s)...")
        new_leader = None
        deadline = time.time() + 10
        while time.time() < deadline:
            time.sleep(0.5)
            try:
                meta = request("127.0.0.1", 9201, {"type": METADATA})
                a = meta["topics"][TOPIC]["assignments"][victim_partition]
                if a["leader"] is not None and a["leader"] != victim_leader:
                    new_leader = a["leader"]
                    break
            except Exception:
                continue
        if new_leader is None:
            print("FAILED — no new leader promoted within timeout")
            return
        print(f"new leader for partition {victim_partition}: broker {new_leader}")

        print("\nproducing batch B (10 messages) — should succeed on the new leader...")
        for i in range(10):
            p.send(TOPIC, value=f"B-{i}", key=f"k{i}")
        print("batch B produced.")

        print("\nconsuming from offset 0 across all partitions (should see all 20 messages)...")
        c = Consumer(BOOTSTRAP, group_id="failover-verifier", topics=[TOPIC])
        c.join()
        all_recs = []
        deadline = time.time() + 10
        while time.time() < deadline and len(all_recs) < 20:
            recs = c.poll(timeout_s=1.0)
            for r in recs:
                all_recs.append(r["value"])
            if recs:
                c.commit()
        c.close()
        print(f"received {len(all_recs)} messages.")

        a_count = sum(1 for v in all_recs if v.startswith("A-"))
        b_count = sum(1 for v in all_recs if v.startswith("B-"))
        print(f"  batch A: {a_count}/10")
        print(f"  batch B: {b_count}/10")
        if a_count == 10 and b_count == 10:
            print("\nPASS — leader failover preserved all messages and accepted new writes.")
        else:
            print("\nFAIL — message counts don't match.")

    finally:
        print("\ntearing down brokers...")
        for proc in procs.values():
            proc.send_signal(signal.SIGTERM)
        for proc in procs.values():
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    main()
