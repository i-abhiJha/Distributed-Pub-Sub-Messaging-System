# My Pub-Sub

A distributed publish-subscribe messaging system, written from scratch in
Python (standard library only), modelled on Apache Kafka.

The cluster supports topic-based messaging with partitioning, replication
for fault-tolerance, consumer groups for load balancing, and automatic
leader failover when a broker dies.

## Features

- **Multi-broker cluster** with one elected controller and N data brokers.
- **Topic-based messaging** with configurable partition count and replication
  factor per topic.
- **Partitioned, ordered log** per partition (append-only, fsync-on-write).
  Per-key ordering is preserved end-to-end.
- **Synchronous replication** (acks=all): a producer only sees an ack once
  the record is durable on the leader and every alive follower.
- **Consumer groups + offset commits**: multiple consumers in a group share
  the partitions of a topic, each partition consumed by exactly one member.
  Multiple groups on the same topic see every message (fan-out).
- **Automatic failover**: heartbeat-based failure detection promotes a
  follower to leader within seconds of a broker crash.
- **Crash recovery**: each broker rebuilds its partition logs from disk on
  restart, truncating partial trailing records.
- **Zero external dependencies** — pure Python stdlib, JSON-over-TCP wire
  protocol, runs anywhere Python 3.8+ runs.

## Quick start

```bash
# Launch a 3-broker cluster (broker 1 is the controller).
./scripts/run_cluster.sh
```

In another terminal:

```bash
cd my-pub-sub

# 1. Basic produce/consume.
PYTHONPATH=. python3 examples/demo_basic.py

# 2. Consumer-group load balancing + fan-out.
PYTHONPATH=. python3 examples/demo_consumer_group.py

# 3. Leader failover (spawns its own cluster on different ports).
PYTHONPATH=. python3 examples/demo_failover.py
```

## Demos

| Script | What it demonstrates |
|---|---|
| `demo_basic.py` | Topic creation, key-based partitioning, single-consumer drain — sanity check the happy path. |
| `demo_consumer_group.py` | Two consumers in group `workers` split the partitions of a topic (load balancing); a separate consumer in group `auditor` sees every message (fan-out across groups). |
| `demo_failover.py` | Spawns a fresh 3-broker cluster, kills a partition leader mid-flow, verifies the controller promotes a follower and that all messages (before *and* after the crash) are consumable from offset 0. |

## Architecture at a glance

```
                              +-------------------+
        +-------------------> |  CONTROLLER (B1)  | <-- broker heartbeats
        |                     |  - cluster meta   |
        |                     |  - failure detect |
        |    METADATA push    |  - group coord    |
        |    (broadcast)      +---------+---------+
        |                               |
+-------+--------+              +-------+--------+
|  BROKER 2 (B2) | <--REPLICATE | BROKER 3 (B3)  |
|  topic.part-1  | -- leader -> | topic.part-1   | -- follower
|  topic.part-0  | <- follower- | topic.part-2   | -- leader
+-------^--------+              +--------^-------+
        |  PRODUCE / FETCH                |  PRODUCE / FETCH
        |                                 |
+-------+-----------+         +-----------+----------+
|     Producer       |         |  Consumer Group     |
|  (key-partitions)  |         |  (workers, auditor) |
+--------------------+         +---------------------+
```

The wire protocol, partition log format, replication state machine and
consumer-group protocol are documented in the module docstrings of
`pubsub/protocol.py`, `pubsub/log.py`, `pubsub/broker.py` and
`pubsub/consumer.py`.

## Project layout

```
.
├── pubsub/
│   ├── __init__.py
│   ├── protocol.py      # wire format, connection-pooled request()
│   ├── log.py           # per-partition append-only log on disk
│   ├── broker.py        # broker process: data + controller + coordinator
│   ├── producer.py      # producer client (partitioning, retries)
│   └── consumer.py      # consumer client (group join, heartbeat, poll)
├── examples/
│   ├── demo_basic.py
│   ├── demo_consumer_group.py
│   └── demo_failover.py
├── scripts/
│   └── run_cluster.sh   # bring up a 3-broker cluster on localhost
├── .gitignore
└── README.md
```

## Requirements

Python 3.8+. No third-party dependencies.

## What's intentionally not built

This is a study project, not a production system. The deliberate omissions:

- **Controller fault tolerance.** The controller broker is bootstrapped as a
  fixed node; if it dies, the cluster halts. Real Kafka used ZooKeeper and now
  uses KRaft (Raft consensus) for this. A drop-in extension would use a
  bully-style or Raft-based election among the remaining brokers.
- **Log compaction and tiered storage.** Logs grow forever; no segment
  rolling or retention sweep.
- **Exactly-once semantics.** Producers are not idempotent (no producerId +
  sequence number); there is no transactional commit protocol. The system
  delivers at-least-once.
- **Zero-copy reads.** FETCH copies through Python user space rather than
  using `sendfile(2)`.
