from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Tuple


class PriceWindow:
    def __init__(self, history_maxlen: int = 300):
        self.buffer: Deque[Tuple[datetime, float]] = deque()
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
        self.buffer.append((ts, price))
        # drop anything older than 15m + small buffer
        cutoff = ts - timedelta(minutes=16)
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

    def _oldest_for_window(self, ts: datetime, window: timedelta):
        cutoff = ts - window
        candidate = None
        for t, p in self.buffer:
            if t <= cutoff:
                candidate = (t, p)
            else:
                break
        return candidate

    def get_return(self, ts: datetime, window: timedelta):
        ref = self._oldest_for_window(ts, window)
        if not ref:
            return None
        _, past_price = ref
        latest_price = self.buffer[-1][1]
        if past_price == 0:
            return None
        return (latest_price - past_price) / past_price

    def get_vol(self, ts: datetime, window: timedelta):
        cutoff = ts - window
        window_prices = [p for t, p in self.buffer if t >= cutoff]
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
