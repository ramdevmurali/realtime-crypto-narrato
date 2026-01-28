from datetime import timedelta
from .config import settings


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

    ratios = []
    thr = {
        "1m": settings.alert_threshold_1m,
        "5m": settings.alert_threshold_5m,
        "15m": settings.alert_threshold_15m,
    }
    for label in ["1m", "5m", "15m"]:
        r = metrics.get(f"return_{label}")
        if r is not None and thr[label] > 0:
            ratios.append(abs(r) / thr[label])
    metrics["attention"] = max(ratios) if ratios else None

    if all(metrics[f"return_{lbl}"] is None for lbl in ["1m", "5m", "15m"]):
        return None
    return metrics
