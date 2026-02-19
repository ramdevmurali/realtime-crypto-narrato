#!/usr/bin/env bash
set -euo pipefail

BROKERS=${KAFKA_BROKERS:-localhost:9092}
TOPICS=(prices news alerts summaries news-enriched prices-deadletter news-deadletter summaries-deadletter)

if command -v rpk >/dev/null 2>&1; then
  RPK_CMD="rpk"
else
  RPK_CMD="docker exec redpanda rpk"
fi

for topic in "${TOPICS[@]}"; do
  echo "creating topic $topic on $BROKERS"
  $RPK_CMD topic create "$topic" --brokers "$BROKERS" 2>/dev/null || echo "topic $topic may already exist"
done
