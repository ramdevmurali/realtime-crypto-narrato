from bisect import bisect_left, bisect_right
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Tuple, List

from ..config import settings, get_windows


class PriceWindow:
    def __init__(self, history_maxlen: int | None = None):
        self.buffer: List[Tuple[datetime, float]] = []
        # per-window smoothed z-score state
        self.z_ewma = {}
        self.history_maxlen = history_maxlen if history_maxlen is not None else settings.window_history_maxlen
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
        if not self.buffer:
            self.buffer.append((ts, price))
        elif ts == self.buffer[-1][0]:
            self.buffer[-1] = (ts, price)
        elif ts > self.buffer[-1][0]:
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

    def snapshot(self):
        return {
            "buffer": list(self.buffer),
            "z_ewma": dict(self.z_ewma),
            "return_history": {
                label: deque(values, maxlen=values.maxlen)
                for label, values in self.return_history.items()
            },
            "vol_history": {
                label: deque(values, maxlen=values.maxlen)
                for label, values in self.vol_history.items()
            },
        }

    def restore(self, snapshot) -> None:
        self.buffer = list(snapshot.get("buffer", []))
        self.z_ewma = dict(snapshot.get("z_ewma", {}))
        self.return_history = {
            label: deque(values, maxlen=values.maxlen)
            for label, values in snapshot.get("return_history", {}).items()
        }
        self.vol_history = {
            label: deque(values, maxlen=values.maxlen)
            for label, values in snapshot.get("vol_history", {}).items()
        }

    def _prune(self, ts: datetime):
        windows = get_windows()
        max_window = max(windows.values())
        cutoff = ts - (max_window + timedelta(seconds=settings.vol_resample_sec))
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
        idx = bisect_left(times, cutoff)
        if idx >= len(self.buffer):
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
        cutoff = ts - window
        if latest_ts < cutoff:
            return None
        if ref_ts == latest_ts:
            return None
        max_gap = min(window, window * settings.window_max_gap_factor)
        if ts - ref_ts > max_gap:
            return None
        if past_price == 0:
            return None
        return (latest_price - past_price) / past_price

    def get_vol(self, ts: datetime, window: timedelta):
        if not self.buffer:
            return None
        cutoff = ts - window
        step = timedelta(seconds=settings.vol_resample_sec)
        if step.total_seconds() <= 0:
            return None
        times = [t for t, _ in self.buffer]
        prices = [p for _, p in self.buffer]
        max_gap = min(window, window * settings.window_max_gap_factor)

        start_idx = bisect_left(times, cutoff)
        end_idx = bisect_right(times, ts)
        if end_idx - start_idx < 3:
            return None

        # seed with first price at/after cutoff to avoid spillover outside window
        first_idx = start_idx
        if first_idx >= len(times):
            return None
        first_time = times[first_idx]
        if first_time > ts:
            return None
        if first_time - cutoff > max_gap:
            return None
        last_idx = first_idx
        last_price = prices[last_idx]
        last_time = times[last_idx]

        resampled = []
        t = first_time
        while t <= ts:
            idx = bisect_right(times, t) - 1
            if idx < first_idx:
                return None
            last_idx = idx
            last_price = prices[last_idx]
            last_time = times[last_idx]
            if t - last_time > max_gap:
                return None
            resampled.append(last_price)
            t += step

        if len(resampled) < 3:
            return None
        returns = []
        for i in range(1, len(resampled)):
            prev = resampled[i - 1]
            cur = resampled[i]
            if prev == 0:
                continue
            returns.append((cur - prev) / prev)
        if len(returns) < 2:
            return None
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        return var ** 0.5
