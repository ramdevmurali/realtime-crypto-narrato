from __future__ import annotations

import json
from typing import Callable, Awaitable, Dict, Any


def make_metrics_handler(get_snapshot: Callable[[], Dict[str, Any]]):
    async def _handle_metrics(reader, writer) -> None:
        closed = False
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                closed = True
                return
            parts = request_line.decode(errors="ignore").split()
            path = parts[1] if len(parts) > 1 else "/"
            # Consume headers.
            while True:
                line = await reader.readline()
                if not line or line == b"\r\n":
                    break
            if path != "/metrics":
                body = json.dumps({"error": "not found"}).encode()
                status = "404 Not Found"
            else:
                body = json.dumps(get_snapshot()).encode()
                status = "200 OK"
            headers = [
                f"HTTP/1.1 {status}",
                "Content-Type: application/json",
                f"Content-Length: {len(body)}",
                "Connection: close",
                "",
                "",
            ]
            writer.write("\r\n".join(headers).encode() + body)
            await writer.drain()
        finally:
            if not closed:
                writer.close()

    return _handle_metrics
