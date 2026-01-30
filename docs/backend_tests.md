# Backend tests cheat sheet

## What we test
- `/health` happy path and failure path (DB fetchval error → 503).
- `/prices` returns desc-ordered ticks, respects symbol lowercasing and limit.
- `/metrics/latest` returns data when present, 404 when absent.
- `/headlines` returns recent headlines with expected fields.
- `/alerts` returns recent anomalies with expected fields.

## Test layout
- `backend/tests_unit`: endpoint behaviors with monkeypatched DB helpers.
- `backend/tests_integration`: hits a real Timescale instance with seeded data.
- `backend/tests_integration/conftest.py`: asyncpg pool fixture, seed helper, httpx AsyncClient fixture.

## Running tests
From `backend/`:
```
PYTHONPATH=.. .venv/bin/python -m pytest tests_unit tests_integration
```

## Integration data seeded
- Symbol `testcoin` with sample prices, metrics (e.g., return_1m ~0.05), headline (sentiment -0.2), and alert (summary "test summary").

## Notes
- Tests assume TimescaleDB reachable at `DATABASE_URL` (defaults to compose timescaledb).
- No Kafka dependency in backend tests; pure DB + HTTP.
