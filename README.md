# Realtime Crypto Anomaly Dashboard

Core services are implemented. Components:
- backend/ : FastAPI service (read-only API over Timescale)
- processor/ : streaming metrics/anomaly worker + sidecars (summary + sentiment)
- frontend/ : Next.js dashboard (in progress)
- public/ : static assets
- scripts/ : helper scripts
- infra/ : deployment/compose configs (docker-compose, env template)

## Resilience (processor quick notes)
- backoff + jitter: reconnects on websocket/RSS use exponential wait with randomness
- retries: DB/Kafka ops wrapped with small retries + backoff
- circuit-breaker counters: if ingest keeps failing, slow down retries and log counts
- graceful cancellation: ingest tasks exit fast on stop; processor cancels tasks cleanly
- startup healthcheck: ping DB (SELECT 1) and Kafka connect before running
- structured logging + counters: key=value logs for publishes/failures/alerts

## Quick start (backend + processor; frontend may be partial)
1) `cp infra/.env.example infra/.env` and tweak values if needed.
2) `cd infra && docker compose --env-file .env up` to launch Redpanda, TimescaleDB, Redis, backend, processor, and frontend.
3) Frontend: http://localhost:3000, Backend API: http://localhost:8000.

## Backend tests (local)
```
pip install -r backend/requirements.txt
python3 -m pytest backend/tests
```
