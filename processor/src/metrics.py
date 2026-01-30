from datetime import timedelta
from .config import settings, get_thresholds
from typing import List
import math


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


def _ewma(prev: float | None, value: float | None, alpha: float, cap: float = 6.0):
    if value is None:
        return None
    smoothed = value if prev is None else prev + alpha * (value - prev)
    # cap to avoid runaway values
    if smoothed > cap:
        smoothed = cap
    if smoothed < -cap:
        smoothed = -cap
    return smoothed


def _percentile(series: List[float], pct: float):
    if not series:
        return None
    s = sorted(series)
    k = (len(s) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    d0 = s[f] * (c - k)
    d1 = s[c] * (k - f)
    return d0 + d1


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
        vol = win.get_vol(ts, delta)
        metrics[f"vol_{label}"] = vol
        rets_series = _returns_for_window(win, ts, delta)
        metrics[f"return_z_{label}"] = _zscore(ret, rets_series)
        # smoothed z per window
        raw_z = metrics[f"return_z_{label}"]
        smoothed = _ewma(win.z_ewma.get(label), raw_z, settings.ewma_return_alpha)
        win.z_ewma[label] = smoothed
        metrics[f"return_z_ewma_{label}"] = smoothed
        metrics[f"vol_z_{label}"] = _zscore(vol, rets_series)
        thr_spike = settings.vol_z_spike_threshold
        vz = metrics[f"vol_z_{label}"]
        metrics[f"vol_spike_{label}"] = vz is not None and vz > thr_spike
        # percentiles on returns
        if len(rets_series) >= 3:
            metrics[f"p05_return_{label}"] = _percentile(rets_series, settings.return_percentile_low)
            metrics[f"p95_return_{label}"] = _percentile(rets_series, settings.return_percentile_high)
        else:
            metrics[f"p05_return_{label}"] = None
            metrics[f"p95_return_{label}"] = None

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
