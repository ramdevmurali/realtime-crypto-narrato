import pytest
from pydantic import ValidationError

from processor.src.config import Settings


def test_invalid_window_labels_duplicate():
    with pytest.raises(ValidationError):
        Settings(window_labels_raw="1m,1m")


def test_invalid_window_labels_unit():
    with pytest.raises(ValidationError):
        Settings(window_labels_raw="5x")


def test_invalid_window_labels_unsupported():
    with pytest.raises(ValidationError):
        Settings(window_labels_raw="2m,5m")


def test_invalid_llm_provider():
    with pytest.raises(ValidationError):
        Settings(llm_provider="other")


def test_invalid_negative_values():
    with pytest.raises(ValidationError):
        Settings(rss_seen_max=-1)


def test_invalid_percentiles():
    with pytest.raises(ValidationError):
        Settings(return_percentile_low=0.9, return_percentile_high=0.1)
