import json
from datetime import datetime, timezone
import pytest

from processor.src.models.messages import PriceMsg, NewsMsg, SummaryRequestMsg, AlertMsg


def test_price_msg_valid():
    msg = PriceMsg(symbol="btcusdt", price=100.5, time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc))
    assert msg.symbol == "btcusdt"


def test_news_msg_valid():
    msg = NewsMsg(
        time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        title="headline",
        url="http://x",
        source="rss",
        sentiment=0.1,
    )
    assert msg.title == "headline"


def test_summary_request_msg_valid():
    msg = SummaryRequestMsg(
        time="2026-02-01T00:00:00+00:00",
        symbol="btcusdt",
        window="1m",
        direction="up",
        ret=0.05,
        threshold=0.04,
        headline="headline",
        sentiment=0.2,
    )
    assert msg.symbol == "btcusdt"


def test_alert_msg_valid():
    msg = AlertMsg(
        time="2026-02-01T00:00:00+00:00",
        symbol="btcusdt",
        window="1m",
        direction="up",
        ret=0.05,
        threshold=0.04,
        headline="headline",
        sentiment=0.2,
        summary="summary",
    )
    assert msg.summary == "summary"


def test_price_msg_missing_field_fails():
    with pytest.raises(Exception):
        PriceMsg(symbol="btcusdt", time=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc))


def test_to_bytes_round_trip():
    msg = PriceMsg(symbol="ethusdt", price=200.0, time=datetime(2026, 2, 1, 0, 1, tzinfo=timezone.utc))
    raw = msg.to_bytes()
    payload = json.loads(raw.decode())
    parsed = PriceMsg.model_validate(payload)
    assert parsed.symbol == "ethusdt"
    assert parsed.price == 200.0


def test_optional_fields_allowed():
    news = NewsMsg(
        time=datetime(2026, 2, 1, 0, 2, tzinfo=timezone.utc),
        title="headline",
        source="rss",
        sentiment=0.0,
    )
    assert news.url is None

    summary = SummaryRequestMsg(
        time="2026-02-01T00:00:00+00:00",
        symbol="btcusdt",
        window="1m",
        direction="down",
        ret=-0.05,
        threshold=0.04,
    )
    assert summary.headline is None
    assert summary.sentiment is None
