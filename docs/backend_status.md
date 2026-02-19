# Backend status (summary)

## Done checklist
- DB pool lifecycle is explicit (init/close in app lifespan).
- SSE endpoints for headlines and alerts are implemented.
- `/headlines` and `/alerts` support `since` + `limit` pagination.
- Timestamps are serialized explicitly as ISO strings in API responses.
- Env precedence is documented (`backend/.env` first, then `infra/.env`).
- Integration tests run against live uvicorn; unit/integration tests passing locally.

## Current state
The backend is stable and usable for frontend integration. Core read APIs, SSE streams,
and pagination are in place with clear DB wiring and documented env behavior.
Tests provide basic regression coverage for endpoints and streams.

## Next optional improvements
- Add structured response models (Pydantic) for clearer API typing.
- Add rate limits / caching for list endpoints if load becomes an issue.
- Add more integration tests for SSE behavior under retries.
- Add OpenAPI examples for key endpoints.
