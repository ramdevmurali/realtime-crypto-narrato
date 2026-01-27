# Realtime Crypto Anomaly Dashboard (skeleton)

Repo scaffold only. Components:
- backend/ : FastAPI service (to-be-built)
- processor/ : streaming metrics/anomaly worker (to-be-built)
- frontend/ : Next.js dashboard (to-be-built)
- public/ : static assets
- scripts/ : helper scripts
- infra/ : deployment/compose configs (docker-compose, env template)

## Quick start (once services are implemented)
1) `cp infra/.env.example infra/.env` and tweak values if needed.
2) `cd infra && docker compose --env-file .env up` to launch Redpanda, TimescaleDB, Redis, backend, processor, and frontend.
3) Frontend: http://localhost:3000, Backend API: http://localhost:8000.
