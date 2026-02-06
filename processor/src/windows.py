from bisect import bisect_left, bisect_right
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Tuple, List

from .config import settings


class PriceWindow:
    def __init__(self, history_maxlen: int = 300):
        self.buffer: List[Tuple[datetime, float]] = []
        # per-window smoothed z-score state
        self.z_ewma = {}
        self.history_maxlen = history_maxlen
        self.return_history: dict[str, Deque[float]] = {}
        self.vol_history: dict[str, Deque[float]] = {}

    def _history_deque(self, store: dict[str, Deque[float]], label: str) -> Deque[float]:
        if label not in store:
            store[label] = deque(maxlen=self.history_maxlen)
        return store[label]

    def record_history(self, label: str, ret: float | None, vol: float | None):
        if ret is not None:
            self._history_deque(self.return_history, label).append(ret)
        if vol is not None:
            self._history_deque(self.vol_history, label).append(vol)

    def add(self, ts: datetime, price: float):
        if not self.buffer or ts >= self.buffer[-1][0]:
            self.buffer.append((ts, price))
        else:
            times = [t for t, _ in self.buffer]
            idx = bisect_left(times, ts)
            if idx < len(self.buffer) and self.buffer[idx][0] == ts:
                self.buffer[idx] = (ts, price)
            else:
                self.buffer.insert(idx, (ts, price))
        # drop anything older than 15m + small buffer
        self._prune(ts)

    def _prune(self, ts: datetime):
        cutoff = ts - timedelta(minutes=16)
        idx = 0
        while idx < len(self.buffer) and self.buffer[idx][0] < cutoff:
            idx += 1
        if idx:
            self.buffer = self.buffer[idx:]

    def _oldest_for_window(self, ts: datetime, window: timedelta):
        cutoff = ts - window
        if not self.buffer:
            return None
        times = [t for t, _ in self.buffer]
        idx = bisect_right(times, cutoff) - 1
        if idx < 0:
            return None
        return self.buffer[idx]

    def _latest_at_or_before(self, ts: datetime):
        if not self.buffer:
            return None
        times = [t for t, _ in self.buffer]
        idx = bisect_right(times, ts) - 1
        if idx < 0:
            return None
        return self.buffer[idx]

    def get_return(self, ts: datetime, window: timedelta):
        latest = self._latest_at_or_before(ts)
        if not latest:
            return None
        ref = self._oldest_for_window(ts, window)
        if not ref:
            return None
        ref_ts, past_price = ref
        latest_ts, latest_price = latest
        if latest_ts < ts - window:
            return None
        max_gap = timedelta(seconds=window.total_seconds() * settings.window_max_gap_factor)
        if ts - ref_ts > max_gap:
            return None
        if past_price == 0:
            return None
        return (latest_price - past_price) / past_price

    def get_vol(self, ts: datetime, window: timedelta):
        cutoff = ts - window
        window_prices = [p for t, p in self.buffer if cutoff <= t <= ts]
        if len(window_prices) < 3:
            return None
        returns = []
        for i in range(1, len(window_prices)):
            prev = window_prices[i - 1]
            cur = window_prices[i]
            if prev == 0:
                continue
            returns.append((cur - prev) / prev)
        if len(returns) < 2:
            return None
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return var ** 0.5
