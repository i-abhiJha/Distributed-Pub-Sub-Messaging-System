"""
Demo 2 — consumer-group load balancing (multiple consumers split partitions).

Two consumers in the same group subscribe to the same topic. The coordinator
round-robins partitions across them, so each consumer ends up reading a
disjoint subset. Together they see every message exactly once.

A third consumer in a *different* group also runs — it sees ALL messages
(fan-out across groups), demonstrating the multi-subscriber pattern.
"""

import threading
import time

from pubsub.producer import Producer
from pubsub.consumer import Consumer

BOOTSTRAP = ["127.0.0.1:9101", "127.0.0.1:9102", "127.0.0.1:9103"]
TOPIC = "events"
N_MESSAGES = 60


def run_consumer(name, group, results, start_event, total_expected):
    c = Consumer(BOOTSTRAP, group_id=group, topics=[TOPIC])
    c.join()
    assigned = [(a["topic"], a["partition"]) for a in c._assignments]
    print(f"[{name}] joined group '{group}' initial_assignments={assigned}")
    # Wait until the orchestrator says everyone is ready and stable.
    start_event.wait()
    # Re-check assignments after the rebalance has settled.
    final = [(a["topic"], a["partition"]) for a in c._assignments]
    if final != assigned:
        print(f"[{name}] post-rebalance assignments={final}")
    seen = 0
    idle_polls = 0
    deadline = time.time() + 20
    while time.time() < deadline:
        records = c.poll(timeout_s=0.5)
        for r in records:
            results.append((name, r["partition"], r["offset"], r["key"]))
            seen += 1
        if records:
            c.commit()
            idle_polls = 0
        else:
            idle_polls += 1
            if idle_polls >= 6:  # ~3s of empty polls -> assume drained
                break
    print(f"[{name}] consumed {seen} messages")
    c.close()


def main():
    p = Producer(BOOTSTRAP)
    p.create_topic(TOPIC, partitions=4, replication_factor=2)

    group_a_results = []   # group "workers" — two consumers split the load
    group_b_results = []   # group "auditor" — one consumer, sees everything
    start_event = threading.Event()

    threads = [
        threading.Thread(target=run_consumer,
                         args=("workers-1", "workers", group_a_results, start_event, N_MESSAGES)),
        threading.Thread(target=run_consumer,
                         args=("workers-2", "workers", group_a_results, start_event, N_MESSAGES)),
        threading.Thread(target=run_consumer,
                         args=("auditor",   "auditor", group_b_results, start_event, N_MESSAGES)),
    ]
    threads[0].start()
    time.sleep(0.3)
    threads[1].start()
    time.sleep(0.3)
    threads[2].start()

    # Wait long enough for the rebalance heartbeats to settle.
    # (Consumer.HEARTBEAT_INTERVAL_S = 2s, give it a margin.)
    print("waiting 4s for rebalance to settle before producing...\n")
    time.sleep(4)

    print(f"producing {N_MESSAGES} messages...")
    for i in range(N_MESSAGES):
        key = f"k-{i % 8}"
        p.send(TOPIC, value=f"event #{i}", key=key)
    print("done producing.\n")

    start_event.set()
    for t in threads:
        t.join()

    print("\n--- summary ---")
    by_name = {}
    for name, _, _, _ in group_a_results:
        by_name[name] = by_name.get(name, 0) + 1
    print(f"group 'workers' split: {by_name}  (total={sum(by_name.values())})")
    print(f"group 'auditor' saw:   {len(group_b_results)} messages (should equal {N_MESSAGES})")

    workers_total = sum(by_name.values())
    if workers_total == N_MESSAGES and len(group_b_results) == N_MESSAGES:
        print("\nPASS — load balancing + fan-out work as expected.")
    else:
        print("\nNOTE — totals differ; check broker logs.")


if __name__ == "__main__":
    main()
