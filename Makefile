.PHONY: smoke-test migrate-db deploy-processor verify-runtime replay-summaries-dlq

smoke-test:
	PYTHONPATH=processor/src:. .venv/bin/python -m pytest processor/tests
	RUN_INTEGRATION=1 PYTHONPATH=processor/src:. .venv/bin/python -m pytest processor/tests -m integration

migrate-db:
	PYTHONPATH=processor/src:. .venv/bin/python scripts/migrate_db.py

deploy-processor:
	docker compose -f infra/docker-compose.yml up -d --build --force-recreate processor summary-sidecar

verify-runtime:
	PYTHONPATH=processor/src:. .venv/bin/python scripts/verify_runtime_build.py $(ARGS)

replay-summaries-dlq:
	PYTHONPATH=processor/src:. .venv/bin/python scripts/replay_summaries_dlq_topic.py $(ARGS)
