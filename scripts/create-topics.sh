#!/usr/bin/env bash
set -euo pipefail

BROKERS=${KAFKA_BROKERS:-localhost:9092}
TOPICS=(prices news alerts)

for topic in "${TOPICS[@]}"; do
  echo "creating topic $topic on $BROKERS"
  rpk topic create "$topic" --brokers "$BROKERS" 2>/dev/null || echo "topic $topic may already exist"
done
