.PHONY: smoke-test migrate-db

smoke-test:
	PYTHONPATH=processor/src:. .venv/bin/python -m pytest processor/tests
	RUN_INTEGRATION=1 PYTHONPATH=processor/src:. .venv/bin/python -m pytest processor/tests -m integration

migrate-db:
	PYTHONPATH=processor/src:. .venv/bin/python scripts/migrate_db.py
