from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID, uuid4

import asyncpg

ORDER_STREAM = "flashcart:orders"
ORDER_CONSUMER_GROUP = "flashcart-workers"
MAX_OPERATION_LOG_ROWS = 5_000
MAX_BIGINT = 9_223_372_036_854_775_807
DEFAULT_UNIT_PRICE_CENTS = 1_999

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS flashcart_inventory (
    sku TEXT PRIMARY KEY,
    available_quantity BIGINT NOT NULL CHECK (available_quantity >= 0),
    initial_quantity BIGINT NOT NULL CHECK (initial_quantity >= available_quantity),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents > 0),
    currency TEXT NOT NULL DEFAULT 'USD',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS flashcart_checkouts (
    id UUID PRIMARY KEY,
    idempotency_key_hash TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    email TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('received', 'processing', 'completed', 'failed')),
    total_cents BIGINT NOT NULL CHECK (total_cents >= 0),
    currency TEXT NOT NULL DEFAULT 'USD',
    failure_reason TEXT CHECK (failure_reason IN ('insufficient_stock', 'processing_error')),
    worker_token UUID,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS flashcart_checkout_items (
    checkout_id UUID NOT NULL REFERENCES flashcart_checkouts(id),
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents > 0),
    line_total_cents BIGINT NOT NULL CHECK (line_total_cents > 0),
    PRIMARY KEY (checkout_id, sku)
);

CREATE TABLE IF NOT EXISTS flashcart_inventory_ledger (
    id BIGSERIAL PRIMARY KEY,
    checkout_id UUID NOT NULL REFERENCES flashcart_checkouts(id),
    sku TEXT NOT NULL,
    quantity_delta BIGINT NOT NULL CHECK (quantity_delta < 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (checkout_id, sku)
);

CREATE OR REPLACE FUNCTION flashcart_reject_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'inventory ledger rows are immutable';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS flashcart_inventory_ledger_immutable
    ON flashcart_inventory_ledger;
CREATE TRIGGER flashcart_inventory_ledger_immutable
    BEFORE UPDATE OR DELETE ON flashcart_inventory_ledger
    FOR EACH ROW EXECUTE FUNCTION flashcart_reject_ledger_mutation();

CREATE TABLE IF NOT EXISTS flashcart_outbox (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS flashcart_outbox_pending_idx
    ON flashcart_outbox (id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS flashcart_checkouts_pending_idx
    ON flashcart_checkouts (created_at)
    WHERE status = 'received' OR status = 'processing';

CREATE TABLE IF NOT EXISTS flashcart_operation_logs (
    id BIGSERIAL PRIMARY KEY,
    checkout_id UUID,
    level TEXT NOT NULL CHECK (level IN ('INFO', 'WARNING', 'ERROR')),
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS flashcart_rate_limits (
    client_key_hash TEXT PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL CHECK (request_count > 0)
);

ALTER TABLE flashcart_inventory
    ADD COLUMN IF NOT EXISTS initial_quantity BIGINT;
ALTER TABLE flashcart_inventory
    ADD COLUMN IF NOT EXISTS unit_price_cents BIGINT NOT NULL DEFAULT 1999;
ALTER TABLE flashcart_inventory
    ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'USD';
UPDATE flashcart_inventory
SET initial_quantity = available_quantity + COALESCE((
    SELECT ABS(SUM(quantity_delta))
    FROM flashcart_inventory_ledger
    WHERE flashcart_inventory_ledger.sku = flashcart_inventory.sku
), 0)
WHERE initial_quantity IS NULL;
ALTER TABLE flashcart_inventory
    ALTER COLUMN initial_quantity SET NOT NULL;
ALTER TABLE flashcart_inventory
    ALTER COLUMN initial_quantity SET DEFAULT 0;
ALTER TABLE flashcart_inventory
    DROP CONSTRAINT IF EXISTS flashcart_inventory_initial_quantity_check;
ALTER TABLE flashcart_inventory
    ADD CONSTRAINT flashcart_inventory_initial_quantity_check
    CHECK (initial_quantity >= available_quantity);

ALTER TABLE flashcart_checkouts
    ADD COLUMN IF NOT EXISTS total_cents BIGINT NOT NULL DEFAULT 0;
ALTER TABLE flashcart_checkouts
    ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'USD';
ALTER TABLE flashcart_checkouts
    ADD COLUMN IF NOT EXISTS failure_reason TEXT;
ALTER TABLE flashcart_checkout_items
    ADD COLUMN IF NOT EXISTS unit_price_cents BIGINT;
ALTER TABLE flashcart_checkout_items
    ADD COLUMN IF NOT EXISTS line_total_cents BIGINT;
UPDATE flashcart_checkout_items AS item
SET unit_price_cents = inventory.unit_price_cents,
    line_total_cents = item.quantity::BIGINT * inventory.unit_price_cents
FROM flashcart_inventory AS inventory
WHERE inventory.sku = item.sku
  AND (item.unit_price_cents IS NULL OR item.line_total_cents IS NULL);
UPDATE flashcart_checkout_items
SET unit_price_cents = 1999,
    line_total_cents = quantity::BIGINT * 1999
WHERE unit_price_cents IS NULL OR line_total_cents IS NULL;
ALTER TABLE flashcart_checkout_items
    ALTER COLUMN unit_price_cents SET NOT NULL;
ALTER TABLE flashcart_checkout_items
    ALTER COLUMN line_total_cents SET NOT NULL;
UPDATE flashcart_checkouts AS checkout
SET total_cents = COALESCE((
    SELECT SUM(item.line_total_cents)
    FROM flashcart_checkout_items AS item
    WHERE item.checkout_id = checkout.id
), 0)
WHERE checkout.total_cents = 0;
ALTER TABLE flashcart_checkouts
    DROP CONSTRAINT IF EXISTS flashcart_checkouts_failure_reason_check;
ALTER TABLE flashcart_checkouts
    ADD CONSTRAINT flashcart_checkouts_failure_reason_check
    CHECK (failure_reason IS NULL OR failure_reason IN ('insufficient_stock', 'processing_error'));
"""


class RedisStreams(Protocol):
    async def xadd(self, name: str, fields: dict[str, str]) -> Any: ...


class IdempotencyConflict(Exception):
    pass


class UnknownSkuError(Exception):
    pass


@dataclass(frozen=True)
class CheckoutRecord:
    checkout_id: str
    status: str
    created: bool
    total_cents: int = 0
    currency: str = "USD"


@dataclass(frozen=True)
class OperationEvent:
    id: int
    checkout_id: str | None
    level: str
    event_type: str
    message: str
    created_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "checkout_id": self.checkout_id[:8] if self.checkout_id else None,
            "level": self.level,
            "event_type": self.event_type,
            "message": self.message,
            "created_at": self.created_at,
        }


class PostgresStorage:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def ping(self) -> bool:
        async with self.pool.acquire() as connection:
            return await connection.fetchval("SELECT 1") == 1

    async def initialize(
        self, seed_inventory: dict[str, int | tuple[int, int] | tuple[int, int, str]]
    ) -> None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute("SELECT pg_advisory_xact_lock(9283746501)")
                await connection.execute(SCHEMA_SQL)
                if seed_inventory:
                    seed_rows = []
                    for sku, seed in seed_inventory.items():
                        if isinstance(seed, tuple):
                            if len(seed) == 2:
                                quantity, price_cents = seed
                                currency = "USD"
                            else:
                                quantity, price_cents, currency = seed
                        else:
                            quantity, price_cents, currency = (
                                seed,
                                DEFAULT_UNIT_PRICE_CENTS,
                                "USD",
                            )
                        if not 0 <= quantity <= MAX_BIGINT:
                            raise ValueError(f"Inventory quantity is out of range for {sku}")
                        if not 0 < price_cents <= MAX_BIGINT:
                            raise ValueError(f"Unit price is out of range for {sku}")
                        seed_rows.append((sku, quantity, quantity, price_cents, currency))
                    await connection.executemany(
                        """
                        INSERT INTO flashcart_inventory
                            (sku, available_quantity, initial_quantity,
                             unit_price_cents, currency)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (sku) DO NOTHING
                        """,
                        seed_rows,
                    )

    async def create_checkout(
        self,
        idempotency_key_hash: str,
        request_hash: str,
        email: str,
        items: list[dict[str, Any]],
    ) -> CheckoutRecord:
        checkout_id = uuid4()
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                skus = [item["sku"] for item in items]
                price_rows = await connection.fetch(
                    """
                    SELECT sku, unit_price_cents, currency
                    FROM flashcart_inventory
                    WHERE sku = ANY($1::TEXT[])
                    ORDER BY sku
                    FOR SHARE
                    """,
                    skus,
                )
                prices = {row["sku"]: row for row in price_rows}
                missing = sorted(set(skus) - prices.keys())
                if missing:
                    raise UnknownSkuError(missing[0])

                snapshot_items: list[dict[str, Any]] = []
                total_cents = 0
                currencies = {row["currency"] for row in price_rows}
                if len(currencies) != 1:
                    raise ValueError("A checkout cannot contain multiple currencies")
                currency = next(iter(currencies))
                for item in items:
                    unit_price_cents = int(prices[item["sku"]]["unit_price_cents"])
                    quantity = int(item["quantity"])
                    if quantity <= 0 or unit_price_cents <= 0:
                        raise ValueError("Quantity and unit price must be positive")
                    line_total_cents = quantity * unit_price_cents
                    if line_total_cents > MAX_BIGINT:
                        raise OverflowError("Checkout line total exceeds BIGINT range")
                    total_cents += line_total_cents
                    if total_cents > MAX_BIGINT:
                        raise OverflowError("Checkout total exceeds BIGINT range")
                    snapshot_items.append(
                        {
                            "sku": item["sku"],
                            "quantity": quantity,
                            "unit_price_cents": unit_price_cents,
                            "line_total_cents": line_total_cents,
                        }
                    )

                inserted_id = await connection.fetchval(
                    """
                    INSERT INTO flashcart_checkouts
                        (id, idempotency_key_hash, request_hash, email, status,
                         total_cents, currency)
                    VALUES ($1, $2, $3, $4, 'received', $5, $6)
                    ON CONFLICT (idempotency_key_hash) DO NOTHING
                    RETURNING id
                    """,
                    checkout_id,
                    idempotency_key_hash,
                    request_hash,
                    email,
                    total_cents,
                    currency,
                )
                if inserted_id is None:
                    existing = await connection.fetchrow(
                        """
                        SELECT id, request_hash, status, total_cents, currency
                        FROM flashcart_checkouts
                        WHERE idempotency_key_hash = $1
                        """,
                        idempotency_key_hash,
                    )
                    if existing is None:
                        raise RuntimeError("Idempotency record disappeared during lookup")
                    if existing["request_hash"] != request_hash:
                        raise IdempotencyConflict
                    return CheckoutRecord(
                        checkout_id=str(existing["id"]),
                        status=existing["status"],
                        created=False,
                        total_cents=int(existing["total_cents"]),
                        currency=existing["currency"],
                    )

                await connection.executemany(
                    """
                    INSERT INTO flashcart_checkout_items
                        (checkout_id, sku, quantity, unit_price_cents, line_total_cents)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    [
                        (
                            checkout_id,
                            item["sku"],
                            item["quantity"],
                            item["unit_price_cents"],
                            item["line_total_cents"],
                        )
                        for item in snapshot_items
                    ],
                )
                await self._insert_outbox(
                    connection,
                    "checkout.requested",
                    checkout_id,
                    {"checkout_id": str(checkout_id)},
                )
                await self._insert_log(
                    connection, checkout_id, "INFO", "checkout.received",
                    "Checkout accepted",
                )
        return CheckoutRecord(
            str(checkout_id), "received", True, total_cents, currency
        )

    async def get_status(self, checkout_id: str) -> dict[str, Any] | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT status, total_cents, currency, failure_reason
                FROM flashcart_checkouts
                WHERE id = $1
                """,
                UUID(checkout_id),
            )
        return dict(row) if row is not None else None

    async def get_inventory(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT sku, available_quantity, unit_price_cents, currency
                FROM flashcart_inventory
                ORDER BY sku
                """
            )
        return [
            {
                "sku": row["sku"],
                "available_quantity": row["available_quantity"],
                "unit_price_cents": row["unit_price_cents"],
                "currency": row["currency"],
            }
            for row in rows
        ]

    async def check_rate_limit(
        self, client_key_hash: str, window_seconds: int
    ) -> tuple[int, int]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                INSERT INTO flashcart_rate_limits
                    (client_key_hash, window_start, request_count)
                VALUES ($1, clock_timestamp(), 1)
                ON CONFLICT (client_key_hash) DO UPDATE SET
                    request_count = CASE
                        WHEN flashcart_rate_limits.window_start
                            <= clock_timestamp() - ($2 * interval '1 second')
                        THEN 1
                        ELSE flashcart_rate_limits.request_count + 1
                    END,
                    window_start = CASE
                        WHEN flashcart_rate_limits.window_start
                            <= clock_timestamp() - ($2 * interval '1 second')
                        THEN clock_timestamp()
                        ELSE flashcart_rate_limits.window_start
                    END
                RETURNING request_count,
                    GREATEST(
                        1,
                        CEIL(EXTRACT(EPOCH FROM (
                            window_start + ($2 * interval '1 second')
                            - clock_timestamp()
                        )))::INTEGER
                    ) AS retry_after
                """,
                client_key_hash,
                window_seconds,
                )
                cleanup_locked = await connection.fetchval(
                    "SELECT pg_try_advisory_xact_lock(9283746502)"
                )
                if cleanup_locked:
                    await connection.execute(
                        """
                        DELETE FROM flashcart_rate_limits
                        WHERE window_start < clock_timestamp() - interval '1 day'
                        """
                    )
        return row["request_count"], row["retry_after"]

    async def process_checkout(self, checkout_id: str) -> str | None:
        checkout_uuid = UUID(checkout_id)
        worker_token = uuid4()

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                claimed_id = await connection.fetchval(
                    """
                    UPDATE flashcart_checkouts
                    SET status = 'processing',
                        worker_token = $2,
                        lease_until = clock_timestamp() + interval '30 seconds',
                        updated_at = clock_timestamp()
                    WHERE id = $1
                      AND (
                        status = 'received'
                        OR (status = 'processing' AND lease_until < clock_timestamp())
                      )
                    RETURNING id
                    """,
                    checkout_uuid,
                    worker_token,
                )
                if claimed_id is None:
                    return await connection.fetchval(
                        "SELECT status FROM flashcart_checkouts WHERE id = $1",
                        checkout_uuid,
                    )
                await self._insert_status_event(
                    connection, checkout_uuid, "processing"
                )
                await self._insert_log(
                    connection, checkout_uuid, "INFO", "checkout.processing",
                    "Worker started checkout processing",
                )

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                checkout = await connection.fetchrow(
                    """
                    SELECT status, worker_token
                    FROM flashcart_checkouts
                    WHERE id = $1
                    FOR UPDATE
                    """,
                    checkout_uuid,
                )
                if (
                    checkout is None
                    or checkout["status"] != "processing"
                    or checkout["worker_token"] != worker_token
                ):
                    return checkout["status"] if checkout else None

                items = await connection.fetch(
                    """
                    SELECT sku, quantity
                    FROM flashcart_checkout_items
                    WHERE checkout_id = $1
                    ORDER BY sku
                    """,
                    checkout_uuid,
                )
                stock_rows = await connection.fetch(
                    """
                    SELECT sku, available_quantity
                    FROM flashcart_inventory
                    WHERE sku = ANY($1::TEXT[])
                    ORDER BY sku
                    FOR UPDATE
                    """,
                    [item["sku"] for item in items],
                )
                stock = {
                    row["sku"]: row["available_quantity"] for row in stock_rows
                }
                unavailable = [
                    item["sku"]
                    for item in items
                    if stock.get(item["sku"], 0) < item["quantity"]
                ]

                if unavailable:
                    await connection.execute(
                        """
                        UPDATE flashcart_checkouts
                            SET status = 'failed', failure_reason = 'insufficient_stock',
                                worker_token = NULL, lease_until = NULL,
                            updated_at = clock_timestamp()
                        WHERE id = $1
                        """,
                        checkout_uuid,
                    )
                    await self._insert_status_event(
                        connection,
                        checkout_uuid,
                        "failed",
                        failure_reason="insufficient_stock",
                    )
                    await self._insert_log(
                        connection,
                        checkout_uuid,
                        "WARNING",
                        "checkout.inventory_rejected",
                        "Checkout failed because inventory was unavailable",
                    )
                    return "failed"

                for item in items:
                    await connection.execute(
                        """
                        UPDATE flashcart_inventory
                        SET available_quantity = available_quantity - $2,
                            updated_at = clock_timestamp()
                        WHERE sku = $1
                        """,
                        item["sku"],
                        item["quantity"],
                    )
                    await connection.execute(
                        """
                        INSERT INTO flashcart_inventory_ledger
                            (checkout_id, sku, quantity_delta)
                        VALUES ($1, $2, $3)
                        """,
                        checkout_uuid,
                        item["sku"],
                        -item["quantity"],
                    )

                await connection.execute(
                    """
                    UPDATE flashcart_checkouts
                    SET status = 'completed', failure_reason = NULL,
                        worker_token = NULL, lease_until = NULL,
                        updated_at = clock_timestamp()
                    WHERE id = $1
                    """,
                    checkout_uuid,
                )
                await self._insert_status_event(connection, checkout_uuid, "completed")
                await self._insert_log(
                    connection,
                    checkout_uuid,
                    "INFO",
                    "checkout.completed",
                    "Inventory reserved and checkout completed",
                )
        return "completed"

    async def pending_checkouts(self, limit: int = 50) -> list[str]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT id
                FROM flashcart_checkouts
                WHERE status = 'received'
                   OR (status = 'processing' AND lease_until < clock_timestamp())
                ORDER BY created_at
                LIMIT $1
                """,
                limit,
            )
        return [str(row["id"]) for row in rows]

    async def fail_checkout_processing(self, checkout_id: str | None) -> None:
        checkout_uuid: UUID | None = None
        if checkout_id:
            try:
                checkout_uuid = UUID(checkout_id)
            except ValueError:
                checkout_uuid = None

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                if checkout_uuid is not None:
                    failed_id = await connection.fetchval(
                        """
                        UPDATE flashcart_checkouts
                        SET status = 'failed',
                            failure_reason = 'processing_error',
                            worker_token = NULL,
                            lease_until = NULL,
                            updated_at = clock_timestamp()
                        WHERE id = $1 AND status IN ('received', 'processing')
                        RETURNING id
                        """,
                        checkout_uuid,
                    )
                    if failed_id is not None:
                        await self._insert_status_event(
                            connection,
                            checkout_uuid,
                            "failed",
                            failure_reason="processing_error",
                        )
                        await self._insert_log(
                            connection,
                            checkout_uuid,
                            "ERROR",
                            "checkout.processing_error",
                            "Checkout failed after reaching the delivery retry limit",
                        )
                        return

                await connection.execute(
                    """
                    INSERT INTO flashcart_operation_logs
                        (checkout_id, level, event_type, message)
                    VALUES ($1, 'ERROR', 'worker.dlq',
                            'Invalid or missing checkout reference moved to DLQ')
                    """,
                    checkout_uuid,
                )

    async def publish_outbox(self, redis: RedisStreams, limit: int = 100) -> int:
        published = 0
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    SELECT id, event_type, aggregate_id, payload::text AS payload
                    FROM flashcart_outbox
                    WHERE published_at IS NULL
                    ORDER BY id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                    """,
                    limit,
                )
                for row in rows:
                    if row["event_type"] == "checkout.requested":
                        await redis.xadd(
                            ORDER_STREAM,
                            {
                                "event_id": str(row["id"]),
                                "event_type": row["event_type"],
                                "aggregate_id": str(row["aggregate_id"]),
                                "payload": row["payload"],
                            },
                        )
                    await connection.execute(
                        """
                        UPDATE flashcart_outbox
                        SET published_at = clock_timestamp()
                        WHERE id = $1
                        """,
                        row["id"],
                    )
                    published += 1
        return published

    async def operation_events(
        self, after_id: int, limit: int = 100
    ) -> list[OperationEvent]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT id, checkout_id, level, event_type, message, created_at
                FROM flashcart_operation_logs
                WHERE id > $1
                ORDER BY id
                LIMIT $2
                """,
                after_id,
                limit,
            )
        return [
            OperationEvent(
                id=row["id"],
                checkout_id=str(row["checkout_id"]) if row["checkout_id"] else None,
                level=row["level"],
                event_type=row["event_type"],
                message=row["message"],
                created_at=row["created_at"].isoformat(),
            )
            for row in rows
        ]

    async def record_operation(
        self, level: str, event_type: str, message: str
    ) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO flashcart_operation_logs (level, event_type, message)
                VALUES ($1, $2, $3)
                """,
                level,
                event_type,
                message,
            )

    async def cleanup_housekeeping(self) -> tuple[int, int]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                locked = await connection.fetchval(
                    "SELECT pg_try_advisory_xact_lock(9283746503)"
                )
                if not locked:
                    return 0, 0
                removed_logs = await connection.execute(
                    """
                    DELETE FROM flashcart_operation_logs
                    WHERE id IN (
                        SELECT id
                        FROM flashcart_operation_logs
                        ORDER BY id DESC
                        OFFSET $1
                    )
                    """,
                    MAX_OPERATION_LOG_ROWS,
                )
                removed_outbox = await connection.execute(
                    """
                    DELETE FROM flashcart_outbox
                    WHERE published_at < clock_timestamp() - interval '1 hour'
                    """
                )
        return (
            int(removed_logs.rsplit(" ", 1)[-1]),
            int(removed_outbox.rsplit(" ", 1)[-1]),
        )

    @staticmethod
    async def _insert_outbox(
        connection: asyncpg.Connection,
        event_type: str,
        aggregate_id: UUID,
        payload: dict[str, Any],
    ) -> None:
        await connection.execute(
            """
            INSERT INTO flashcart_outbox (event_type, aggregate_id, payload)
            VALUES ($1, $2, $3::JSONB)
            """,
            event_type,
            aggregate_id,
            json.dumps(payload),
        )

    @classmethod
    async def _insert_status_event(
        cls,
        connection: asyncpg.Connection,
        checkout_id: UUID,
        status: str,
        failure_reason: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "checkout_id": str(checkout_id),
            "status": status,
        }
        if failure_reason is not None:
            payload["failure_reason"] = failure_reason
        await cls._insert_outbox(
            connection,
            f"checkout.{status}",
            checkout_id,
            payload,
        )

    @staticmethod
    async def _insert_log(
        connection: asyncpg.Connection,
        checkout_id: UUID,
        level: str,
        event_type: str,
        message: str,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO flashcart_operation_logs
                (checkout_id, level, event_type, message)
            VALUES ($1, $2, $3, $4)
            """,
            checkout_id,
            level,
            event_type,
            message,
        )
