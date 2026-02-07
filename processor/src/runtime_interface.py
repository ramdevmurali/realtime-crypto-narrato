from __future__ import annotations

from typing import Protocol


class RuntimeService(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...
