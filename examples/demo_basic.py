"""
Demo 1 — basic produce and consume.

Pre-req: run `./scripts/run_cluster.sh` in another terminal.

This creates a topic, produces 30 keyed messages (load distributed across
3 partitions via key hashing), and consumes them all back from a single
consumer in a fresh group.
"""

import time

from pubsub.producer import Producer
from pubsub.consumer import Consumer

BOOTSTRAP = ["127.0.0.1:9101", "127.0.0.1:9102", "127.0.0.1:9103"]
TOPIC = "orders"


def main():
    p = Producer(BOOTSTRAP)
    print(f"creating topic '{TOPIC}' with 3 partitions, RF=2")
    p.create_topic(TOPIC, partitions=3, replication_factor=2)

    print("producing 30 messages...")
    for i in range(30):
        key = f"user-{i % 5}"
        result = p.send(TOPIC, value=f"order #{i} for {key}", key=key)
        print(f"  sent #{i:02d} key={key} -> partition={result['partition']} offset={result['offset']}")

    print("\nstarting consumer in fresh group 'demo-basic'...")
    c = Consumer(BOOTSTRAP, group_id="demo-basic", topics=[TOPIC])
    c.join()
    print(f"assignments: {[(a['topic'], a['partition']) for a in c._assignments]}")

    print("\npolling until we've drained all 30...")
    seen = 0
    deadline = time.time() + 10
    while seen < 30 and time.time() < deadline:
        records = c.poll(timeout_s=1.0)
        for r in records:
            print(f"  recv p={r['partition']:>1} off={r['offset']:>3}  key={r['key']:<8} value={r['value']}")
            seen += 1
        if records:
            c.commit()
    print(f"\ndone. consumed {seen}/30 messages")
    c.close()


if __name__ == "__main__":
    main()
