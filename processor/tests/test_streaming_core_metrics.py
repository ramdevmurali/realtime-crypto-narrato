import asyncio
import json
import pytest

from processor.src.streaming_core import StreamProcessor


class _FakeReader:
    def __init__(self):
        self._lines = [
            b"GET /metrics HTTP/1.1\r\n",
            b"Host: localhost\r\n",
            b"\r\n",
        ]

    async def readline(self):
        if not self._lines:
            return b""
        return self._lines.pop(0)


class _FakeWriter:
    def __init__(self):
        self.data = b""
        self.closed = False

    def write(self, data: bytes):
        self.data += data

    async def drain(self):
        return None

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_processor_metrics_handler_returns_json():
    proc = StreamProcessor()
    reader = _FakeReader()
    writer = _FakeWriter()

    await proc._handle_metrics(reader, writer)
    assert b"200 OK" in writer.data
    body = writer.data.split(b"\r\n\r\n", 1)[1]
    payload = json.loads(body.decode())
    assert "counters" in payload
    assert "rolling" in payload
    assert payload["service_name"] == "processor"
    assert "start_time" in payload
