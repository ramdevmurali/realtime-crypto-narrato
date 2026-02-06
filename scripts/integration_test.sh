#!/usr/bin/env bash
set -euo pipefail
RUN_INTEGRATION=1 PYTHONPATH=processor/src:. .venv/bin/python -m pytest processor/tests -m integration
