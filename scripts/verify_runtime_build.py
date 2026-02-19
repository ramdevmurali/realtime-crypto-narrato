#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path


DEFAULT_CONTAINER = "summary-sidecar"
DEFAULT_CONTAINER_PATH = "/app/src/services/summary_sidecar.py"
DEFAULT_LOCAL_PATH = Path("processor/src/services/summary_sidecar.py")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_container_file(container: str, container_path: str) -> tuple[str | None, str | None]:
    proc = subprocess.run(
        ["docker", "exec", "-i", container, "cat", container_path],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip() or "unknown docker exec error"
        return None, stderr
    return proc.stdout, None


def _check_summary_sidecar_guards(source: str) -> list[str]:
    issues: list[str] = []
    if 'ts = parse_iso_datetime(payload["time"])' not in source:
        issues.append("missing parse_iso_datetime time parse before DB operations")

    persist_uses_ts = re.search(
        r"with_retries\(\s*persist_summary,\s*payload,\s*summary,\s*pool,\s*log,\s*ts,",
        source,
        re.DOTALL,
    )
    if not persist_uses_ts:
        issues.append("persist_summary call does not pass parsed ts")
    return issues


def verify_runtime_build(
    container: str = DEFAULT_CONTAINER,
    container_path: str = DEFAULT_CONTAINER_PATH,
    local_path: Path = DEFAULT_LOCAL_PATH,
) -> dict:
    issues: list[str] = []
    if not local_path.exists():
        return {
            "ok": False,
            "container": container,
            "container_path": container_path,
            "local_path": str(local_path),
            "issues": [f"local path missing: {local_path}"],
        }

    local_text = local_path.read_text(encoding="utf-8")
    container_text, err = _read_container_file(container, container_path)
    if err:
        return {
            "ok": False,
            "container": container,
            "container_path": container_path,
            "local_path": str(local_path),
            "issues": [f"failed to read container file: {err}"],
        }
    assert container_text is not None

    issues.extend(_check_summary_sidecar_guards(container_text))
    local_hash = _sha256(local_text.encode("utf-8"))
    container_hash = _sha256(container_text.encode("utf-8"))
    if local_hash != container_hash:
        issues.append("container/source signature mismatch (summary_sidecar.py hash differs)")

    return {
        "ok": len(issues) == 0,
        "container": container,
        "container_path": container_path,
        "local_path": str(local_path),
        "local_hash": local_hash,
        "container_hash": container_hash,
        "issues": issues,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify running summary-sidecar build matches source safeguards")
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--container-path", default=DEFAULT_CONTAINER_PATH)
    parser.add_argument("--local-path", default=str(DEFAULT_LOCAL_PATH))
    args = parser.parse_args()

    result = verify_runtime_build(
        container=args.container,
        container_path=args.container_path,
        local_path=Path(args.local_path),
    )
    print(json.dumps(result, sort_keys=True))
    if result["ok"]:
        print("runtime verification passed")
        return 0
    print("runtime verification failed")
    for issue in result["issues"]:
        print(f" - {issue}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
