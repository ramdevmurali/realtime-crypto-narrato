import pytest

from scripts import replay_summaries_dlq_topic as replay_mod


class _Msg:
    def __init__(self, value: bytes):
        self.value = value


class _FakeConsumer:
    def __init__(self, batches):
        self._batches = list(batches)
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def getmany(self, timeout_ms=0, max_records=0):
        if self._batches:
            return self._batches.pop(0)
        return {}


class _FakeProducer:
    def __init__(self):
        self.started = False
        self.stopped = False
        self.sent = []

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send_and_wait(self, topic: str, value: bytes):
        self.sent.append((topic, value))


@pytest.mark.asyncio
async def test_replay_summaries_dlq_replays_until_idle_timeout():
    consumer = _FakeConsumer(
        batches=[
            {0: [_Msg(b"a"), _Msg(b"b")]},
            {},
        ]
    )
    producer = _FakeProducer()

    result = await replay_mod.replay_summaries_dlq(
        source_topic="summaries-deadletter",
        target_topic="summaries",
        brokers=["localhost:9092"],
        idle_timeout_sec=0,
        max_messages=None,
        dry_run=False,
        consumer_factory=lambda *args, **kwargs: consumer,
        producer_factory=lambda *args, **kwargs: producer,
    )

    assert result["ok"] is True
    assert result["consumed"] == 2
    assert result["replayed"] == 2
    assert result["stop_reason"] == "idle_timeout"
    assert producer.sent == [("summaries", b"a"), ("summaries", b"b")]


@pytest.mark.asyncio
async def test_replay_summaries_dlq_dry_run():
    consumer = _FakeConsumer(batches=[{0: [_Msg(b"a")]}, {}])

    def _producer_factory(*args, **kwargs):
        raise AssertionError("producer should not be created in dry-run mode")

    result = await replay_mod.replay_summaries_dlq(
        source_topic="summaries-deadletter",
        target_topic="summaries",
        brokers=["localhost:9092"],
        idle_timeout_sec=0,
        max_messages=None,
        dry_run=True,
        consumer_factory=lambda *args, **kwargs: consumer,
        producer_factory=_producer_factory,
    )

    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["consumed"] == 1
    assert result["replayed"] == 1


@pytest.mark.asyncio
async def test_replay_summaries_dlq_respects_max_messages():
    consumer = _FakeConsumer(
        batches=[
            {0: [_Msg(b"a"), _Msg(b"b"), _Msg(b"c")]},
        ]
    )
    producer = _FakeProducer()

    result = await replay_mod.replay_summaries_dlq(
        source_topic="summaries-deadletter",
        target_topic="summaries",
        brokers=["localhost:9092"],
        idle_timeout_sec=30,
        max_messages=2,
        dry_run=False,
        consumer_factory=lambda *args, **kwargs: consumer,
        producer_factory=lambda *args, **kwargs: producer,
    )

    assert result["ok"] is True
    assert result["stop_reason"] == "max_messages"
    assert result["consumed"] == 2
    assert result["replayed"] == 2
    assert producer.sent == [("summaries", b"a"), ("summaries", b"b")]
