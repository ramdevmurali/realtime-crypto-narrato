from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import Lock
from typing import Deque, Dict, Any

from .utils import now_utc


@dataclass
class RollingStats:
    window: int = 100
    values: Deque[float] = field(default_factory=lambda: deque(maxlen=100))

    def add(self, value: float) -> None:
        self.values.append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        values = list(self.values)
        if not values:
            return {"count": 0}
        total = sum(values)
        return {
            "count": len(values),
            "avg": total / len(values),
            "min": min(values),
            "max": max(values),
            "last": values[-1],
        }


class MetricsRegistry:
    def __init__(self, rolling_window: int = 100, service_name: str | None = None):
        self._lock = Lock()
        self._counters: Dict[str, int] = {}
        self._rolling: Dict[str, RollingStats] = {}
        self._rolling_window = rolling_window
        self._service_name = service_name
        self._start_time = now_utc().isoformat()

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            stat = self._rolling.get(name)
            if stat is None:
                stat = RollingStats(window=self._rolling_window, values=deque(maxlen=self._rolling_window))
                self._rolling[name] = stat
            stat.add(value)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            counters = dict(self._counters)
            rolling = {name: stat.snapshot() for name, stat in self._rolling.items()}
        return {
            "timestamp": now_utc().isoformat(),
            "service_name": self._service_name,
            "start_time": self._start_time,
            "counters": counters,
            "rolling": rolling,
        }

class NamespacedMetricsRegistry:
    def __init__(self, base: MetricsRegistry, namespace: str):
        self._base = base
        self._namespace = namespace.strip().rstrip(".")

    def _key(self, name: str) -> str:
        prefix = f"{self._namespace}."
        if name.startswith(prefix):
            return name
        return f"{self._namespace}.{name}"

    def inc(self, name: str, value: int = 1) -> None:
        self._base.inc(self._key(name), value=value)

    def observe(self, name: str, value: float) -> None:
        self._base.observe(self._key(name), value)

    def snapshot(self) -> Dict[str, Any]:
        return self._base.snapshot()


_GLOBAL_METRICS: MetricsRegistry | None = None


def get_metrics(namespace: str | None = None):
    global _GLOBAL_METRICS
    if _GLOBAL_METRICS is None:
        _GLOBAL_METRICS = MetricsRegistry(service_name="processor")
    if namespace:
        return NamespacedMetricsRegistry(_GLOBAL_METRICS, namespace)
    return _GLOBAL_METRICS
