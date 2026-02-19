# Anomaly Diagnostics Runbook

## Purpose
Diagnose why anomalies/summaries are not appearing without changing production defaults.

## Mandatory deploy/verify gate
Before validating anomaly/summarizer behavior, force-recreate processor + summary sidecar and verify the running container code:

```bash
make deploy-processor
make verify-runtime
```

If `verify-runtime` fails, stop and redeploy before any diagnostics.

## Recovery flow for summary-sidecar DLQ backlog
If summary requests were dropped to DLQ (for example, stale container with old code), replay deterministically:

```bash
# inspect first (no publish)
make replay-summaries-dlq ARGS="--dry-run --max-messages 20"

# replay all with bounded idle timeout
make replay-summaries-dlq ARGS="--idle-timeout-sec 5"
```

## Quick probe (hardened)
Run one bounded probe that publishes synthetic headline + prices and reports pass/fail.
This variant enforces summary-path checks:

```bash
PYTHONPATH=processor/src:. .venv/bin/python scripts/probe_anomaly_path.py \
  --check-summaries \
  --require-summary-db-update \
  --require-no-summary-dlq
```

JSON output fields:
- `ok`
- `anomaly_test_mode`
- `anomaly_delta`
- `alert_seen`
- `summary_request_seen`
- `summary_db_updated`
- `summary_dlq_seen`
- `failures`
- topic names used (`alerts`, `summaries`, `news`, `prices`)

## Interpretation matrix
- `metrics high + no alerts`: anomaly pipeline issue (decision path or publish path).
- `alerts yes + anomalies no`: persistence issue.
- `anomalies yes + summaries no`: summary request publish/consume issue.
- `summary_dlq_seen=true`: summary sidecar failed processing and dropped to DLQ.
- `headlines stale`: context quality issue (does not block anomaly emission by itself).

## Useful knobs (safe defaults)
- Normal mode (default):
  - `ANOMALY_TEST_MODE=false`
  - thresholds from `ALERT_THRESHOLD_1M/5M/15M`
  - cooldown from `ANOMALY_COOLDOWN_SEC`
- Deterministic diagnostics mode (explicit):
  - `ANOMALY_TEST_MODE=true`
  - `ANOMALY_TEST_THRESHOLD=0.001`
  - `ANOMALY_TEST_COOLDOWN_SEC=0`

## Headline freshness checks
Use processor telemetry:
- `processor.news_stale_polls`
- rolling `processor.latest_headline_age_sec`
- sampled log key: `headline_stale`
