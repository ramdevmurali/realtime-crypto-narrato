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
Integration tests require a live backend server at `http://localhost:8000`
(run uvicorn separately). If uvicorn isn't running, you'll hit `httpx.ConnectError`.
They also require a running TimescaleDB with schema applied.
Backend config loads `backend/.env` first, then `infra/.env` (set local DB URL in `backend/.env`).

Minimal runbook:
1) Start Timescale (e.g., `docker compose -f infra/docker-compose.yml up -d timescaledb`)
2) Apply migrations: `PYTHONPATH=processor/src:. .venv-backend/bin/python scripts/migrate_db.py`
3) Start uvicorn: `cd backend && ../.venv-backend/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000`
4) Run tests: `./.venv-backend/bin/python -m pytest tests_unit tests_integration`

## Integration data seeded
- Symbol `testcoin` with sample prices, metrics (e.g., return_1m ~0.05), headline (sentiment -0.2), and alert (summary "test summary").

## Notes
- Tests assume TimescaleDB reachable at `DATABASE_URL` (defaults to compose timescaledb).
- No Kafka dependency in backend tests; pure DB + HTTP.
