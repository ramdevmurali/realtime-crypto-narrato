import pytest

from scripts import migrate_db
from processor.src import config as config_module


class _FakeConn:
    def __init__(self):
        self.executed = []

    async def execute(self, query, *args):
        self.executed.append((query, args))

    async def close(self):
        return None


def _has_retention_call(executed, table: str) -> bool:
    for query, args in executed:
        if "add_retention_policy" in query and args and args[0] == table:
            return True
    return False


@pytest.mark.asyncio
async def test_migrate_db_applies_retention_policies(monkeypatch):
    conn = _FakeConn()

    async def fake_connect(dsn):
        return conn

    monkeypatch.setattr(migrate_db.asyncpg, "connect", fake_connect)

    settings = config_module.settings
    monkeypatch.setattr(settings, "retention_prices_days", 30)
    monkeypatch.setattr(settings, "retention_metrics_days", 30)
    monkeypatch.setattr(settings, "retention_headlines_days", 90)
    monkeypatch.setattr(settings, "retention_anomalies_days", 90)

    await migrate_db.migrate()

    assert _has_retention_call(conn.executed, "prices")
    assert _has_retention_call(conn.executed, "metrics")
    assert _has_retention_call(conn.executed, "headlines")
    assert _has_retention_call(conn.executed, "anomalies")


def test_migrate_db_ddl_is_idempotent():
    for stmt in migrate_db.DDL:
        normalized = stmt.strip().lower()
        assert "if not exists" in normalized or "if_not_exists" in normalized
