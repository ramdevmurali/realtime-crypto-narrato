from pathlib import Path

from scripts import verify_runtime_build as verify_mod


def _container_source() -> str:
    return """
from ..utils import parse_iso_datetime

async def process_summary_record(msg, consumer, producer, pool, log, semaphore):
    payload = {"time": "2026-01-01T00:00:00+00:00"}
    ts = parse_iso_datetime(payload["time"])
    await with_retries(
        persist_summary,
        payload,
        summary,
        pool,
        log,
        ts,
        log=log,
        op="persist_summary",
    )
"""


def test_verify_runtime_build_passes_when_guards_present_and_hash_matches(monkeypatch, tmp_path: Path):
    local_path = tmp_path / "summary_sidecar.py"
    source = _container_source()
    local_path.write_text(source, encoding="utf-8")
    monkeypatch.setattr(verify_mod, "_read_container_file", lambda container, path: (source, None))

    result = verify_mod.verify_runtime_build(
        container="summary-sidecar",
        container_path="/app/src/services/summary_sidecar.py",
        local_path=local_path,
    )

    assert result["ok"] is True
    assert result["issues"] == []
    assert result["local_hash"] == result["container_hash"]


def test_verify_runtime_build_fails_when_parse_guard_missing(monkeypatch, tmp_path: Path):
    local_path = tmp_path / "summary_sidecar.py"
    local_path.write_text(_container_source(), encoding="utf-8")
    missing_guard_source = """
async def process_summary_record(msg, consumer, producer, pool, log, semaphore):
    payload = {"time": "2026-01-01T00:00:00+00:00"}
"""
    monkeypatch.setattr(
        verify_mod,
        "_read_container_file",
        lambda container, path: (missing_guard_source, None),
    )

    result = verify_mod.verify_runtime_build(local_path=local_path)

    assert result["ok"] is False
    assert any("missing parse_iso_datetime" in issue for issue in result["issues"])


def test_verify_runtime_build_fails_when_container_hash_differs(monkeypatch, tmp_path: Path):
    local_path = tmp_path / "summary_sidecar.py"
    local_path.write_text(_container_source(), encoding="utf-8")
    container_source = _container_source() + "\n# runtime drift\n"
    monkeypatch.setattr(verify_mod, "_read_container_file", lambda container, path: (container_source, None))

    result = verify_mod.verify_runtime_build(local_path=local_path)

    assert result["ok"] is False
    assert any("signature mismatch" in issue for issue in result["issues"])
