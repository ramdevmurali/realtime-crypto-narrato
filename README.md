# Realtime Crypto Anomaly Dashboard

Core services are implemented. Components:
- backend/ : FastAPI service (read-only API over Timescale)
- processor/ : streaming metrics/anomaly worker + sidecars (summary + sentiment)
- frontend/ : Next.js dashboard (in progress)
- public/ : static assets
- scripts/ : helper scripts
- infra/ : deployment/compose configs (docker-compose, env template)

## Resilience (processor quick notes)
High‑level resilience: backoff + retries, graceful shutdown, and basic telemetry.
See `docs/processor.md` for full details.

## Quick start (backend + processor; frontend may be partial)
1) `cp infra/.env.example infra/.env` and tweak values if needed.
2) Run DB migrations: `make migrate-db` (or `PYTHONPATH=processor/src:. .venv/bin/python scripts/migrate_db.py`).
2) `cd infra && docker compose --env-file .env up` to launch Redpanda, TimescaleDB, Redis, backend, processor, and frontend.
3) Frontend: http://localhost:3000, Backend API: http://localhost:8000.

## Sentiment default
- Sentiment sidecar defaults to `SENTIMENT_PROVIDER=onnx`.
- Place model assets under `models/finbert/` (mounted to `/models/finbert` in compose) and keep `SENTIMENT_MODEL_PATH=/models/finbert`.
- If model load/inference fails, sidecar falls back to heuristic sentiment without crashing by default.
- Set `SENTIMENT_FAIL_FAST=true` to make model load failures crash at startup.

## Backend tests (local)
```
pip install -r backend/requirements.txt
python3 -m pytest backend/tests
```
Note: backend loads `backend/.env` first (set local `DATABASE_URL` there if needed).
