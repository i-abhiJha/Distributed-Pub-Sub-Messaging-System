#!/usr/bin/env bash
# Launch a 3-broker cluster locally.
#
#   broker 1 → controller, port 9101
#   broker 2 → follower,   port 9102
#   broker 3 → follower,   port 9103
#
# Logs are written to ./logs/broker-<id>.log, data to ./data/broker-<id>/.
# Ctrl-C tears the cluster down.

set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p logs data

PYTHON="${PYTHON:-python3}"

cleanup() {
  echo
  echo "shutting down cluster..."
  jobs -p | xargs -r kill 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

echo "starting broker 1 (controller) on :9101"
PYTHONPATH=. "$PYTHON" -m pubsub.broker \
  --id 1 --host 127.0.0.1 --port 9101 \
  --data-dir ./data/broker-1 \
  > logs/broker-1.log 2>&1 &

sleep 1

echo "starting broker 2 on :9102"
PYTHONPATH=. "$PYTHON" -m pubsub.broker \
  --id 2 --host 127.0.0.1 --port 9102 \
  --data-dir ./data/broker-2 \
  --controller 127.0.0.1:9101 \
  > logs/broker-2.log 2>&1 &

echo "starting broker 3 on :9103"
PYTHONPATH=. "$PYTHON" -m pubsub.broker \
  --id 3 --host 127.0.0.1 --port 9103 \
  --data-dir ./data/broker-3 \
  --controller 127.0.0.1:9101 \
  > logs/broker-3.log 2>&1 &

sleep 1
echo "cluster up. tailing logs (Ctrl-C to stop):"
echo "---"
tail -f logs/broker-1.log logs/broker-2.log logs/broker-3.log
