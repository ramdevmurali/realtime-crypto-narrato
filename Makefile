.PHONY: smoke-test

smoke-test:
	PYTHONPATH=processor/src:. .venv/bin/python -m pytest processor/tests
	RUN_INTEGRATION=1 PYTHONPATH=processor/src:. .venv/bin/python -m pytest processor/tests -m integration
