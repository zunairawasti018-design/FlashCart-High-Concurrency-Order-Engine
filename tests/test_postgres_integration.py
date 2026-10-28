from __future__ import annotations

import asyncio
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg

from scripts.audit_ledger import audit_ledger, inventory_ledger_summary
from app.storage import MAX_BIGINT, PostgresStorage

TEST_DATABASE_URL = os.getenv("FLASHCART_TEST_DATABASE_URL")


@unittest.skipUnless(
    TEST_DATABASE_URL,
    "Set FLASHCART_TEST_DATABASE_URL to run real PostgreSQL integration tests",
)
class PostgresStorageIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.admin = await asyncpg.connect(TEST_DATABASE_URL)
        self.schema = f"flashcart_test_{uuid4().hex}"
        await self.admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.pool = await asyncpg.create_pool(
            TEST_DATABASE_URL,
            min_size=1,
            max_size=12,
            server_settings={"search_path": f"{self.schema},public"},
        )
        self.storage = PostgresStorage(self.pool)
        self.sku = f"TEST_{uuid4().hex[:12]}"
        self.empty_sku = f"EMPTY_{uuid4().hex[:12]}"
        await self.storage.initialize(
            {self.sku: (20, 125), self.empty_sku: (0, 275)}
        )

    async def asyncTearDown(self) -> None:
        await self.pool.close()
        await self.admin.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        await self.admin.close()

    async def create_checkout(
        self, quantity: int, items: list[dict[str, object]] | None = None
    ) -> str:
        checkout_items = items or [{"sku": self.sku, "quantity": quantity}]
        unique = uuid4().hex
        record = await self.storage.create_checkout(
            idempotency_key_hash=f"key-{unique}",
            request_hash=f"payload-{unique}",
            email="integration@example.test",
            items=checkout_items,
        )
        return record.checkout_id

    async def test_parallel_reservations_cannot_oversell_and_match_ledger(self) -> None:
        checkout_ids = await asyncio.gather(
            *(self.create_checkout(1) for _ in range(250))
        )
        results = await asyncio.gather(
            *(self.storage.process_checkout(checkout_id) for checkout_id in checkout_ids)
        )

        self.assertEqual(results.count("completed"), 20)
        self.assertEqual(results.count("failed"), 230)
        async with self.pool.acquire() as connection:
            remaining = await connection.fetchval(
                "SELECT available_quantity FROM flashcart_inventory WHERE sku = $1",
                self.sku,
            )
            ledger_total = await connection.fetchval(
                """
                SELECT COALESCE(SUM(quantity_delta), 0)
                FROM flashcart_inventory_ledger
                WHERE sku = $1
                """,
                self.sku,
            )
        self.assertEqual(remaining, 0)
        self.assertEqual(ledger_total, -20)

    async def test_insufficient_line_rolls_back_every_inventory_change(self) -> None:
        checkout_id = await self.create_checkout(
            1,
            [
                {"sku": self.sku, "quantity": 2},
                {"sku": self.empty_sku, "quantity": 1},
            ],
        )

        self.assertEqual(
            await self.storage.process_checkout(checkout_id), "failed"
        )
        async with self.pool.acquire() as connection:
            remaining = await connection.fetchval(
                "SELECT available_quantity FROM flashcart_inventory WHERE sku = $1",
                self.sku,
            )
            ledger_rows = await connection.fetchval(
                "SELECT COUNT(*) FROM flashcart_inventory_ledger WHERE checkout_id = $1",
                checkout_id,
            )
        self.assertEqual(remaining, 20)
        self.assertEqual(ledger_rows, 0)

    async def test_checkout_price_is_snapshotted_when_created(self) -> None:
        checkout_id = await self.create_checkout(3)

        async with self.pool.acquire() as connection:
            await connection.execute(
                "UPDATE flashcart_inventory SET unit_price_cents = 999 WHERE sku = $1",
                self.sku,
            )
            checkout = await connection.fetchrow(
                "SELECT total_cents FROM flashcart_checkouts WHERE id = $1",
                UUID(checkout_id),
            )
            item = await connection.fetchrow(
                """
                SELECT quantity, unit_price_cents, line_total_cents
                FROM flashcart_checkout_items
                WHERE checkout_id = $1 AND sku = $2
                """,
                UUID(checkout_id),
                self.sku,
            )
        self.assertEqual(checkout["total_cents"], 375)
        self.assertEqual(item["quantity"], 3)
        self.assertEqual(item["unit_price_cents"], 125)
        self.assertEqual(item["line_total_cents"], 375)

    async def test_checkout_price_multiplication_rejects_bigint_overflow(self) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                "UPDATE flashcart_inventory SET unit_price_cents = $2 WHERE sku = $1",
                self.sku,
                MAX_BIGINT,
            )
        with self.assertRaises(OverflowError):
            await self.storage.create_checkout(
                idempotency_key_hash=f"key-{uuid4().hex}",
                request_hash=f"payload-{uuid4().hex}",
                email="integration@example.test",
                items=[{"sku": self.sku, "quantity": 2}],
            )

    async def test_ledger_audit_accepts_balanced_inventory_and_reports_mismatch(self) -> None:
        checkout_id = await self.create_checkout(2)
        self.assertEqual(
            await self.storage.process_checkout(checkout_id), "completed"
        )
        self.assertEqual(await audit_ledger(self.pool), [])
        summary = await inventory_ledger_summary(self.pool, self.sku)
        self.assertEqual(summary["ledger_delta"], -2)

        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE flashcart_inventory
                SET initial_quantity = initial_quantity + 1
                WHERE sku = $1
                """,
                self.sku,
            )
        errors = await audit_ledger(self.pool)
        self.assertTrue(any("inventory mismatch" in error for error in errors))
        async with self.pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM flashcart_inventory WHERE sku = $1", self.sku
            )
        errors = await audit_ledger(self.pool)
        self.assertTrue(any("missing inventory SKU" in error for error in errors))

    async def test_housekeeping_trims_logs_and_old_published_outbox_rows(self) -> None:
        old_outbox_id = uuid4()
        recent_outbox_id = uuid4()
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO flashcart_operation_logs (level, event_type, message)
                VALUES ('INFO', $1, 'cleanup test')
                """,
                [(f"cleanup.{index}",) for index in range(5_002)],
            )
            await connection.executemany(
                """
                INSERT INTO flashcart_outbox
                    (event_type, aggregate_id, payload, published_at)
                VALUES ('test.event', $1, '{}'::JSONB, $2)
                """,
                [
                    (old_outbox_id, now - timedelta(days=365 * 5)),
                    (recent_outbox_id, now - timedelta(minutes=30)),
                ],
            )
        removed_logs, removed_outbox = await self.storage.cleanup_housekeeping()

        async with self.pool.acquire() as connection:
            log_count = await connection.fetchval(
                "SELECT COUNT(*) FROM flashcart_operation_logs"
            )
            old_remaining = await connection.fetchval(
                "SELECT COUNT(*) FROM flashcart_outbox WHERE aggregate_id = $1",
                old_outbox_id,
            )
            recent_remaining = await connection.fetchval(
                "SELECT COUNT(*) FROM flashcart_outbox WHERE aggregate_id = $1",
                recent_outbox_id,
            )
        self.assertEqual(removed_logs, 2)
        self.assertEqual(removed_outbox, 1)
        self.assertEqual(log_count, 5_000)
        self.assertEqual(old_remaining, 0)
        self.assertEqual(recent_remaining, 1)

    async def test_checkout_and_outbox_record_commit_together(self) -> None:
        checkout_id = await self.create_checkout(1)

        async with self.pool.acquire() as connection:
            checkout_count = await connection.fetchval(
                "SELECT COUNT(*) FROM flashcart_checkouts WHERE id = $1",
                UUID(checkout_id),
            )
            outbox_count = await connection.fetchval(
                """
                SELECT COUNT(*) FROM flashcart_outbox
                WHERE aggregate_id = $1 AND event_type = 'checkout.requested'
                """,
                UUID(checkout_id),
            )
        self.assertEqual(checkout_count, 1)
        self.assertEqual(outbox_count, 1)

    async def test_inventory_ledger_rejects_mutation(self) -> None:
        checkout_id = await self.create_checkout(1)
        self.assertEqual(
            await self.storage.process_checkout(checkout_id), "completed"
        )
        async with self.pool.acquire() as connection:
            with self.assertRaises(asyncpg.PostgresError):
                await connection.execute(
                    """
                    UPDATE flashcart_inventory_ledger
                    SET quantity_delta = -2
                    WHERE checkout_id = $1 AND sku = $2
                    """,
                    UUID(checkout_id),
                    self.sku,
                )

    async def test_database_polling_processes_checkout_without_redis(self) -> None:
        checkout_id = await self.create_checkout(1)

        pending = await self.storage.pending_checkouts(limit=50)
        self.assertIn(checkout_id, pending)
        self.assertEqual(
            await self.storage.process_checkout(checkout_id), "completed"
        )

    async def test_processing_failure_is_persisted_with_error_log_and_status_event(self) -> None:
        checkout_id = await self.create_checkout(1)
        await self.storage.fail_checkout_processing(checkout_id)

        status = await self.storage.get_status(checkout_id)
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["failure_reason"], "processing_error")
        async with self.pool.acquire() as connection:
            error_count = await connection.fetchval(
                """
                SELECT COUNT(*) FROM flashcart_operation_logs
                WHERE checkout_id = $1 AND level = 'ERROR'
                """,
                UUID(checkout_id),
            )
            status_event = await connection.fetchrow(
                """
                SELECT payload FROM flashcart_outbox
                WHERE aggregate_id = $1 AND event_type = 'checkout.failed'
                """,
                UUID(checkout_id),
            )
        self.assertEqual(error_count, 1)
        payload = (
            status_event["payload"]
            if isinstance(status_event["payload"], dict)
            else json.loads(status_event["payload"])
        )
        self.assertEqual(payload["failure_reason"], "processing_error")


if __name__ == "__main__":
    unittest.main()
