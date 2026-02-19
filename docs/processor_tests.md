# Processor tests cheat sheet

## Coverage (summary)
Test categories include:
- windows/metrics math and invariants
- anomaly decision + alert flow
- ingest helpers and pipelines
- sidecars (summary + sentiment)
- config validation + utils
- integration (Kafka + Timescale)

Key test files (see `processor/tests/` for full coverage):
- `test_windows.py`, `test_metrics.py`, `test_anomaly.py`, `test_anomaly_decision.py`
- `test_ingest.py`, `test_price_pipeline.py`, `test_app.py`, `test_streaming_core.py`
- `test_summary_sidecar.py`, `test_sentiment_sidecar.py`, `test_sentiment_model.py`
- `test_config.py`, `test_utils.py`, `test_streaming_core_metrics.py`, `test_migrate_db.py`
- `integration/test_e2e.py`, `integration/test_sentiment_sidecar.py`, `integration/test_summary_sidecar.py`

Note: `processor/tests/` is the source of truth for exact coverage.

## How to run
From repo root or `processor/`:
```
python3 -m pytest -q processor/tests
```
Notes:
- Uses pytest-asyncio for async cases.
- Unit tests require no external services; Kafka/DB are mocked.

## Smoke test (unit + integration)
From repo root:
```
make smoke-test
```
Notes:
- Runs unit tests first, then integration tests with `RUN_INTEGRATION=1`.

## Integration tests
Requires docker compose services:
```
scripts/integration_test.sh
```
Notes:
- Runs against redpanda + timescaledb from `infra/docker-compose.yml`.
- Set `RUN_INTEGRATION=1` or run `-m integration` directly to include these tests.
