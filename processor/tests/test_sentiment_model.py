import pytest

from processor.src import config as config_module
from processor.src.services import sentiment_model


def test_sentiment_model_stub_fallback(monkeypatch):
    monkeypatch.setattr(config_module.settings, "sentiment_provider", "stub")
    results = sentiment_model.predict(["bitcoin up", "market crashes"])
    assert len(results) == 2
    for score, label, conf in results:
        assert isinstance(score, float)
        assert label is None
        assert conf is None


def test_onnx_provider_falls_back_when_missing_model(monkeypatch):
    monkeypatch.setattr(config_module.settings, "sentiment_provider", "onnx")
    monkeypatch.setattr(config_module.settings, "sentiment_model_path", "/tmp/does-not-exist")
    results = sentiment_model.predict(["bitcoin up"])
    assert len(results) == 1
    score, label, conf = results[0]
    assert isinstance(score, float)
    assert label is None
    assert conf is None
