from __future__ import annotations

import unittest
from typing import Any

from app.worker import DLQ_STREAM, MAX_MESSAGE_DELIVERIES, ORDER_STREAM, _handle_message_safely


class FakeRedis:
    def __init__(self) -> None:
        self.delivery_count = 1
        self.acked: list[str] = []
        self.dead_letters: list[tuple[str, dict[str, str]]] = []

    async def xpending_range(
        self, stream: str, group: str, min: str, max: str, count: int
    ) -> list[dict[str, int]]:
        assert stream == ORDER_STREAM
        assert group == "flashcart-workers"
        assert min == max
        assert count == 1
        return [{"times_delivered": self.delivery_count}]

    async def xadd(self, stream: str, fields: dict[str, str]) -> str:
        self.dead_letters.append((stream, fields))
        return "1-0"

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.acked.append(message_id)
        return 1


class FakeStorage:
    def __init__(self, fail: bool = True) -> None:
        self.fail = fail
        self.processed: list[str] = []
        self.failed: list[str | None] = []
        self.error_logs: list[str] = []

    async def process_checkout(self, checkout_id: str) -> str:
        self.processed.append(checkout_id)
        if self.fail:
            raise RuntimeError("simulated transient processing error")
        return "completed"

    async def fail_checkout_processing(self, checkout_id: str | None) -> None:
        self.failed.append(checkout_id)
        self.error_logs.append("checkout.processing_error")


class WorkerRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_message_then_moves_it_to_dead_letter_stream(self) -> None:
        redis = FakeRedis()
        storage = FakeStorage()
        message = (
            "1700000000000-0",
            {"event_type": "checkout.requested", "aggregate_id": "checkout-id"},
        )

        for delivery in range(1, MAX_MESSAGE_DELIVERIES + 1):
            redis.delivery_count = delivery
            await _handle_message_safely(redis, storage, message)

        self.assertEqual(len(storage.processed), MAX_MESSAGE_DELIVERIES - 1)
        self.assertEqual(storage.failed, ["checkout-id"])
        self.assertEqual(storage.error_logs, ["checkout.processing_error"])
        self.assertEqual(redis.dead_letters[0][0], DLQ_STREAM)
        self.assertEqual(
            redis.dead_letters[0][1]["source_message_id"], message[0]
        )
        self.assertEqual(
            redis.dead_letters[0][1]["delivery_count"],
            str(MAX_MESSAGE_DELIVERIES),
        )
        self.assertEqual(redis.acked, [message[0]])

    async def test_bad_message_does_not_prevent_later_message_processing(self) -> None:
        redis = FakeRedis()
        failing_storage = FakeStorage()
        good_storage = FakeStorage(fail=False)
        bad_message = (
            "1700000000000-0",
            {"event_type": "checkout.requested", "aggregate_id": "bad"},
        )
        good_message = (
            "1700000000001-0",
            {"event_type": "checkout.requested", "aggregate_id": "good"},
        )

        await _handle_message_safely(redis, failing_storage, bad_message)
        await _handle_message_safely(redis, good_storage, good_message)

        self.assertEqual(redis.acked, [good_message[0]])
        self.assertEqual(good_storage.processed, ["good"])


if __name__ == "__main__":
    unittest.main()
