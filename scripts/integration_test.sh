#!/usr/bin/env bash
set -euo pipefail

check_sentiment_startup_guard() {
  local -a compose_cmd=(docker compose -f infra/docker-compose.yml)
  if [[ -f infra/.env ]]; then
    compose_cmd+=(--env-file infra/.env)
  fi

  local logs
  if ! logs="$("${compose_cmd[@]}" logs --tail 300 sentiment-sidecar 2>&1)"; then
    echo "[FAIL] sentiment startup guard: unable to read sentiment-sidecar logs"
    echo "$logs"
    return 1
  fi

  local -a forbidden_tokens=(sentiment_model_load_failed sentiment_fallback_used)
  local -a found_tokens=()
  local token
  for token in "${forbidden_tokens[@]}"; do
    if grep -Fq "$token" <<<"$logs"; then
      found_tokens+=("$token")
    fi
  done

  if (( ${#found_tokens[@]} > 0 )); then
    echo "[FAIL] sentiment startup guard: forbidden token(s) detected: ${found_tokens[*]}"
    return 1
  fi

  echo "[PASS] sentiment startup guard: no forbidden startup tokens in sentiment-sidecar logs"
}

check_sentiment_startup_guard
RUN_INTEGRATION=1 PYTHONPATH=processor/src:. ${VENV_PY:-python3} -m pytest processor/tests -m integration
