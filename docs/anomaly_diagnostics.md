# Anomaly Diagnostics Runbook

## Purpose
Diagnose why anomalies/summaries are not appearing without changing production defaults.

## Quick probe
Run one bounded probe that publishes synthetic headline + prices and reports pass/fail:

```bash
PYTHONPATH=processor/src:. .venv/bin/python scripts/probe_anomaly_path.py --check-summaries
```

JSON output fields:
- `ok`
- `anomaly_test_mode`
- `anomaly_delta`
- `alert_seen`
- `summary_request_seen`
- topic names used (`alerts`, `summaries`, `news`, `prices`)

## Interpretation matrix
- `metrics high + no alerts`: anomaly pipeline issue (decision path or publish path).
- `alerts yes + anomalies no`: persistence issue.
- `anomalies yes + summaries no`: summary request publish/consume issue.
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
