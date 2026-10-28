from __future__ import annotations

import asyncio
import logging
import os
import socket
from typing import Any
from uuid import uuid4

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from app.main import Settings, seed_inventory_mapping
from app.storage import (
    ORDER_CONSUMER_GROUP,
    ORDER_STREAM,
    PostgresStorage,
)

logger = logging.getLogger("flashcart.worker")
DLQ_STREAM = "flashcart:orders:dlq"
MAX_MESSAGE_DELIVERIES = 5
REDIS_RECONNECT_SECONDS = 5
STREAM_BLOCK_MILLISECONDS = 500
HOUSEKEEPING_INTERVAL_SECONDS = 60


async def _connect_redis(settings: Settings) -> Redis | None:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await redis.ping()
        try:
            await redis.xgroup_create(
                ORDER_STREAM, ORDER_CONSUMER_GROUP, id="0-0", mkstream=True
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise
    except RedisError:
        await redis.aclose()
        return None
    return redis


async def _claim_abandoned_messages(
    redis: Redis, consumer_name: str
) -> list[tuple[str, dict[str, Any]]]:
    result = await redis.xautoclaim(
        ORDER_STREAM,
        ORDER_CONSUMER_GROUP,
        consumer_name,
        min_idle_time=30_000,
        start_id="0-0",
        count=20,
    )
    return result[1]


async def _consume_messages(
    redis: Redis, consumer_name: str
) -> list[tuple[str, dict[str, Any]]]:
    messages = await _claim_abandoned_messages(redis, consumer_name)
    if messages:
        return messages
    results = await redis.xreadgroup(
        ORDER_CONSUMER_GROUP,
        consumer_name,
        {ORDER_STREAM: ">"},
        count=20,
        block=STREAM_BLOCK_MILLISECONDS,
    )
    return [message for _, entries in results for message in entries]


async def _handle_message(
    redis: Redis, storage: PostgresStorage, message: tuple[str, dict[str, Any]]
) -> None:
    message_id, fields = message
    pending = await redis.xpending_range(
        ORDER_STREAM,
        ORDER_CONSUMER_GROUP,
        min=message_id,
        max=message_id,
        count=1,
    )
    deliveries = (
        int(pending[0].get("times_delivered", pending[0].get("delivery_count", 1)))
        if pending
        else 1
    )
    if deliveries >= MAX_MESSAGE_DELIVERIES:
        dead_letter = {key: str(value) for key, value in fields.items()}
        dead_letter["source_message_id"] = message_id
        dead_letter["delivery_count"] = str(deliveries)
        dead_letter["failure_reason"] = "processing_error"
        await redis.xadd(DLQ_STREAM, dead_letter)
        await storage.fail_checkout_processing(fields.get("aggregate_id"))
        await redis.xack(ORDER_STREAM, ORDER_CONSUMER_GROUP, message_id)
        return

    if fields.get("event_type") == "checkout.requested":
        checkout_id = fields.get("aggregate_id")
        if not checkout_id:
            raise ValueError("Checkout event is missing its aggregate ID")
        await storage.process_checkout(str(checkout_id))
    await redis.xack(ORDER_STREAM, ORDER_CONSUMER_GROUP, message_id)


async def _handle_message_safely(
    redis: Redis, storage: PostgresStorage, message: tuple[str, dict[str, Any]]
) -> None:
    try:
        await _handle_message(redis, storage, message)
    except Exception:
        logger.exception("Order message processing failed; message remains pending")


async def _record_worker_event(
    storage: PostgresStorage,
    event_type: str,
    message: str,
    level: str = "WARNING",
) -> None:
    try:
        await storage.record_operation(level, event_type, message)
    except (asyncpg.PostgresError, OSError, TimeoutError):
        logger.exception("Could not persist worker lifecycle event")


async def run_worker() -> None:
    settings = Settings.from_environment()
    pool = await asyncpg.create_pool(
        settings.database_url, min_size=1, max_size=10, command_timeout=10
    )
    storage = PostgresStorage(pool)
    redis: Redis | None = None
    consumer_name = f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
    last_redis_attempt = 0.0
    last_housekeeping = 0.0
    redis_degraded = False
    try:
        await storage.initialize(seed_inventory_mapping(settings.seed_inventory))
        await storage.record_operation(
            "INFO", "worker.started", "Order processing worker started"
        )
        logger.info("Order worker started")

        while True:
            now = asyncio.get_running_loop().time()
            if now - last_housekeeping >= HOUSEKEEPING_INTERVAL_SECONDS:
                try:
                    await storage.cleanup_housekeeping()
                except (asyncpg.PostgresError, OSError, TimeoutError):
                    logger.exception("Periodic storage cleanup failed")
                last_housekeeping = now

            if redis is None and now - last_redis_attempt >= REDIS_RECONNECT_SECONDS:
                last_redis_attempt = now
                try:
                    redis = await _connect_redis(settings)
                except RedisError:
                    logger.exception("Redis stream initialization failed")
                if redis is None:
                    if not redis_degraded:
                        logger.warning(
                            "Redis Streams unavailable; processing from PostgreSQL"
                        )
                        await _record_worker_event(
                            storage,
                            "worker.redis_fallback",
                            "Redis unavailable; database polling is active",
                        )
                    redis_degraded = True
                elif redis_degraded:
                    await _record_worker_event(
                        storage,
                        "worker.redis_recovered",
                        "Redis Streams recovered; stream processing resumed",
                        level="INFO",
                    )
                    redis_degraded = False

            if redis is not None:
                try:
                    await storage.publish_outbox(redis)
                except RedisError:
                    logger.exception("Redis unavailable; switching to database polling")
                    await redis.aclose()
                    redis = None
                    last_redis_attempt = asyncio.get_running_loop().time()
                    redis_degraded = True
                    await _record_worker_event(
                        storage,
                        "worker.redis_fallback",
                        "Redis unavailable; database polling is active",
                    )
                except (asyncpg.PostgresError, OSError, TimeoutError):
                    logger.exception("Outbox publication failed")
                    await asyncio.sleep(1)

            if redis is not None:
                try:
                    messages = await _consume_messages(redis, consumer_name)
                except RedisError:
                    logger.exception("Redis stream read failed; using database polling")
                    await redis.aclose()
                    redis = None
                    last_redis_attempt = asyncio.get_running_loop().time()
                    redis_degraded = True
                    await _record_worker_event(
                        storage,
                        "worker.redis_fallback",
                        "Redis unavailable; database polling is active",
                    )
                else:
                    for message in messages:
                        await _handle_message_safely(redis, storage, message)

            try:
                for checkout_id in await storage.pending_checkouts(limit=50):
                    await storage.process_checkout(checkout_id)
            except (asyncpg.PostgresError, OSError, TimeoutError):
                logger.exception("PostgreSQL fallback polling failed")
                await asyncio.sleep(1)
            await asyncio.sleep(0.05)
    finally:
        if redis is not None:
            await redis.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run_worker())
