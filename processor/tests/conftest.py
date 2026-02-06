import os
import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: end-to-end integration tests")


def pytest_collection_modifyitems(config, items):
    markexpr = getattr(config.option, "markexpr", "") or ""
    run_integration = os.getenv("RUN_INTEGRATION") == "1" or "integration" in markexpr
    if run_integration:
        return
    skip_integration = pytest.mark.skip(reason="integration tests require RUN_INTEGRATION=1 or -m integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
