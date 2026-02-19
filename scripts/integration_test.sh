#!/usr/bin/env bash
set -euo pipefail
RUN_INTEGRATION=1 PYTHONPATH=processor/src:. ${VENV_PY:-python3} -m pytest processor/tests -m integration
