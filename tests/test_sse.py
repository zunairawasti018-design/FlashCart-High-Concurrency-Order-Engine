from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from redis.exceptions import RedisError

from app.main import (
    OperationsEventBroadcaster,
    SSEConnectionLimiter,
    Settings,
    _redis_reconnect_loop,
)


class Event:
    def __init__(self, event_id: int) -> None:
        self.event_id = event_id

    def as_dict(self) -> dict[str, int]:
        return {"id": self.event_id}


class FakeOperationsStorage:
    def __init__(self) -> None:
        self.poll_count = 0
        self.sent = False

    async def operation_events(self, after_id: int, limit: int) -> list[Event]:
        self.poll_count += 1
        if self.sent:
            await asyncio.sleep(0)
            return []
        self.sent = True
        return [Event(1)]


class FakeRedis:
    def __init__(self, should_fail: bool) -> None:
        self.should_fail = should_fail
        self.closed = False

    async def ping(self) -> bool:
        if self.should_fail:
            raise RedisError("not ready")
        return True

    async def aclose(self) -> None:
        self.closed = True


class SSEHardeningTests(unittest.IsolatedAsyncioTestCase):
    async def test_per_ip_connection_limit_is_enforced_and_released(self) -> None:
        limiter = SSEConnectionLimiter(limit_per_ip=2)
        self.assertTrue(await limiter.reserve("192.0.2.1"))
        self.assertTrue(await limiter.reserve("192.0.2.1"))
        self.assertFalse(await limiter.reserve("192.0.2.1"))
        self.assertTrue(await limiter.reserve("192.0.2.2"))
        await limiter.release("192.0.2.1")
        self.assertTrue(await limiter.reserve("192.0.2.1"))

    async def test_one_operations_poller_broadcasts_to_all_subscribers(self) -> None:
        storage = FakeOperationsStorage()
        broadcaster = OperationsEventBroadcaster(storage, poll_interval=0.01)
        first = await broadcaster.subscribe(after_id=0)
        second = await broadcaster.subscribe(after_id=0)
        poller = asyncio.create_task(broadcaster.run())
        try:
            first_event, second_event = await asyncio.wait_for(
                asyncio.gather(first.get(), second.get()), timeout=1
            )
        finally:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
        self.assertEqual(first_event, second_event)
        self.assertEqual(first_event["id"], 1)
        self.assertEqual(storage.poll_count, 2)

    async def test_subscriber_replay_is_bounded_to_queue_capacity(self) -> None:
        broadcaster = OperationsEventBroadcaster(FakeOperationsStorage(), 0.01)
        broadcaster.history.extend({"id": index} for index in range(1, 1_102))
        queue = await broadcaster.subscribe(after_id=0)
        self.assertEqual(queue.qsize(), queue.maxsize)
        self.assertEqual((await queue.get())["id"], 102)

    async def test_api_reconnect_loop_retries_until_redis_recovers(self) -> None:
        failed_client = FakeRedis(should_fail=True)
        ready_client = FakeRedis(should_fail=False)
        clients = iter([failed_client, ready_client])
        app = SimpleNamespace(state=SimpleNamespace(redis=None))
        settings = Settings(
            redis_url="redis://unused",
            allowed_origins=(),
            allowed_hosts=("testserver",),
            rate_limit_requests=5,
            rate_limit_window_seconds=60,
            redis_reconnect_seconds=0.001,
        )
        with patch("app.main.Redis.from_url", side_effect=lambda *args, **kwargs: next(clients)):
            task = asyncio.create_task(_redis_reconnect_loop(app, settings))
            try:
                for _ in range(100):
                    if app.state.redis is ready_client:
                        break
                    await asyncio.sleep(0.002)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(failed_client.closed)
        self.assertIs(app.state.redis, ready_client)
        await ready_client.aclose()


if __name__ == "__main__":
    unittest.main()
