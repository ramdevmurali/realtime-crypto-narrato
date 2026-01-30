from datetime import timedelta
from .config import settings, get_thresholds
from typing import List


def _returns_for_window(win, ts, window: timedelta) -> List[float]:
    cutoff = ts - window
    prices = [p for t, p in win.buffer if t >= cutoff]
    if len(prices) < 2:
        return []
    returns = []
    for i in range(1, len(prices)):
        prev = prices[i - 1]
        cur = prices[i]
        if prev == 0:
            continue
        returns.append((cur - prev) / prev)
    return returns


def _zscore(value: float, series: List[float]):
    if value is None or len(series) < 2:
        return None
    mean = sum(series) / len(series)
    var = sum((r - mean) ** 2 for r in series) / len(series)
    std = var ** 0.5
    if std == 0:
        return None
    return (value - mean) / std


def compute_metrics(price_windows, symbol: str, ts):
    win = price_windows[symbol]
    windows = {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
    }
    metrics = {}
    for label, delta in windows.items():
        ret = win.get_return(ts, delta)
        metrics[f"return_{label}"] = ret
        metrics[f"vol_{label}"] = win.get_vol(ts, delta)
        rets_series = _returns_for_window(win, ts, delta)
        metrics[f"return_z_{label}"] = _zscore(ret, rets_series)

    ratios = []
    thr = get_thresholds()
    for label in ["1m", "5m", "15m"]:
        r = metrics.get(f"return_{label}")
        if r is not None and thr[label] > 0:
            ratios.append(abs(r) / thr[label])
    metrics["attention"] = max(ratios) if ratios else None

    if all(metrics[f"return_{lbl}"] is None for lbl in ["1m", "5m", "15m"]):
        return None
    return metrics
