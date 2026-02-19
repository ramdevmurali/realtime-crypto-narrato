# Processor Status (Snapshot)

## Current State
The processor layer is stable and structured with clear separation between domain logic and service side effects. Migrations are out‑of‑band, runtime DDL is removed, and telemetry/metrics are in place. Unit tests are passing and integration tests are available when infra is running.

## Done Checklist
- Domain logic decoupled from config/logging.
- Runtime DDL removed; schema handled via `scripts/migrate_db.py`.
- Metrics endpoints added for processor + sidecars.
- DLQ handling includes logging/metrics (and buffering for summary sidecar).
- LLM + retry helpers split into focused modules.
- Kafka consumers wired with explicit commit/DLQ rules.
- Core tests passing (`pytest processor/tests`).

## Next Optional Improvements (Low Priority)
- Add a shared metrics HTTP handler to reduce duplication.
- Add a lightweight “run all” local dev script for processor + sidecars.
- Tighten naming consistency in docs and message schemas (minor).
- Add a small status/health summary endpoint for processor (beyond /metrics).
