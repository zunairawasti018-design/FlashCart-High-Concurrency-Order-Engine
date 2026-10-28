from __future__ import annotations

import asyncio
import os
import unittest
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import httpx
from redis.exceptions import RedisError
from starlette.requests import Request

from app.main import RATE_LIMIT_SCRIPT, Settings, create_app
from app.storage import (
    DEFAULT_UNIT_PRICE_CENTS,
    CheckoutRecord,
    IdempotencyConflict,
    UnknownSkuError,
)


@dataclass
class FakeOperation:
    id: int
    checkout_id: str | None
    level: str
    event_type: str
    message: str
    created_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "checkout_id": self.checkout_id,
            "level": self.level,
            "event_type": self.event_type,
            "message": self.message,
            "created_at": self.created_at,
        }


class FakeStorage:
    def __init__(self) -> None:
        self.inventory: dict[str, int] = {}
        self.prices: dict[str, int] = {}
        self.checkouts: dict[str, dict[str, Any]] = {}
        self.idempotency: dict[str, tuple[str, str]] = {}
        self.ledger: list[tuple[str, str, int]] = []
        self.events: list[FakeOperation] = []
        self.rate_counts: dict[str, int] = {}
        self.lock = asyncio.Lock()

    async def initialize(
        self, seed_inventory: dict[str, int | tuple[int, int] | tuple[int, int, str]]
    ) -> None:
        for sku, seed in seed_inventory.items():
            quantity, price_cents = (
                (seed[0], seed[1])
                if isinstance(seed, tuple)
                else (seed, DEFAULT_UNIT_PRICE_CENTS)
            )
            self.inventory.setdefault(sku, quantity)
            self.prices.setdefault(sku, price_cents)

    async def ping(self) -> bool:
        return True

    async def check_rate_limit(
        self, client_key_hash: str, window_seconds: int
    ) -> tuple[int, int]:
        del window_seconds
        count = self.rate_counts.get(client_key_hash, 0) + 1
        self.rate_counts[client_key_hash] = count
        return count, 60

    async def create_checkout(
        self,
        idempotency_key_hash: str,
        request_hash: str,
        email: str,
        items: list[dict[str, Any]],
    ) -> Any:
        async with self.lock:
            existing = self.idempotency.get(idempotency_key_hash)
            if existing is not None:
                old_hash, checkout_id = existing
                if old_hash != request_hash:
                    raise IdempotencyConflict
                existing_checkout = self.checkouts[checkout_id]
                return CheckoutRecord(
                    checkout_id=checkout_id,
                    status=existing_checkout["status"],
                    created=False,
                    total_cents=existing_checkout["total_cents"],
                )

            snapshot_items = []
            total_cents = 0
            for item in items:
                sku = item["sku"]
                if sku not in self.prices:
                    raise UnknownSkuError(sku)
                price = self.prices[sku]
                line_total = price * item["quantity"]
                total_cents += line_total
                snapshot_items.append(
                    {
                        **item,
                        "unit_price_cents": price,
                        "line_total_cents": line_total,
                    }
                )
            checkout_id = str(uuid4())
            self.idempotency[idempotency_key_hash] = (request_hash, checkout_id)
            self.checkouts[checkout_id] = {
                "status": "received",
                "email": email,
                "items": snapshot_items,
                "total_cents": total_cents,
                "currency": "USD",
                "failure_reason": None,
                "worker_token": None,
            }
            self._log(checkout_id, "INFO", "checkout.received", "Checkout accepted")
            return CheckoutRecord(
                checkout_id=checkout_id,
                status="received",
                created=True,
                total_cents=total_cents,
            )

    async def get_status(self, checkout_id: str) -> dict[str, Any] | None:
        record = self.checkouts.get(checkout_id)
        if record is None:
            return None
        return {
            "status": record["status"],
            "total_cents": record["total_cents"],
            "currency": record["currency"],
            "failure_reason": record["failure_reason"],
        }

    async def get_inventory(self) -> list[dict[str, Any]]:
        return [
            {
                "sku": sku,
                "available_quantity": quantity,
                "unit_price_cents": self.prices[sku],
                "currency": "USD",
            }
            for sku, quantity in sorted(self.inventory.items())
        ]

    async def operation_events(
        self, after_id: int, limit: int = 100
    ) -> list[FakeOperation]:
        return [event for event in self.events if event.id > after_id][:limit]

    def _log(self, checkout_id: str, level: str, event_type: str, message: str) -> None:
        self.events.append(
            FakeOperation(
                id=len(self.events) + 1,
                checkout_id=checkout_id,
                level=level,
                event_type=event_type,
                message=message,
                created_at="2026-10-28T12:00:00+00:00",
            )
        )

    async def process_checkout(self, checkout_id: str) -> str | None:
        async with self.lock:
            record = self.checkouts.get(checkout_id)
            if record is None:
                return None
            if record["status"] != "received":
                return record["status"]

            record["status"] = "processing"
            self._log(
                checkout_id, "INFO", "checkout.processing", "Worker started checkout processing"
            )
            unavailable = [
                item["sku"]
                for item in record["items"]
                if self.inventory.get(item["sku"], 0) < item["quantity"]
            ]
            if unavailable:
                record["status"] = "failed"
                record["failure_reason"] = "insufficient_stock"
                self._log(
                    checkout_id,
                    "WARNING",
                    "checkout.inventory_rejected",
                    "Checkout failed because inventory was unavailable",
                )
                return "failed"

            for item in record["items"]:
                sku = item["sku"]
                quantity = item["quantity"]
                self.inventory[sku] -= quantity
                self.ledger.append((checkout_id, sku, -quantity))
            record["status"] = "completed"
            self._log(
                checkout_id,
                "INFO",
                "checkout.completed",
                "Inventory reserved and checkout completed",
            )
            return "completed"


class FakeRedis:
    def __init__(self, fail: bool = False) -> None:
        self.counts: dict[str, int] = {}
        self.fail = fail

    async def ping(self) -> bool:
        if self.fail:
            raise RedisError("redis offline")
        return True

    async def aclose(self) -> None:
        return None

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        if self.fail:
            raise RedisError("redis offline")
        if script != RATE_LIMIT_SCRIPT or numkeys != 1:
            raise AssertionError("Unexpected Redis operation")
        key = str(args[0])
        count = self.counts.get(key, 0) + 1
        self.counts[key] = count
        return [count, 60]


class OversizedChunkedBody(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b" " * 10_000
        yield b" " * 10_000

    async def aclose(self) -> None:
        return None


class CheckoutApiTests(unittest.IsolatedAsyncioTestCase):
    def test_inventory_seed_parser_supports_legacy_and_price_formats(self) -> None:
        with patch.dict(
            os.environ,
            {"FLASHCART_SEED_INVENTORY": "OLD_FORMAT:8,PRICED:4:325"},
            clear=True,
        ):
            settings = Settings.from_environment()
        self.assertEqual(
            settings.seed_inventory,
            (("OLD_FORMAT", 8), ("PRICED", 4, 325)),
        )

    async def asyncSetUp(self) -> None:
        self.redis = FakeRedis()
        self.storage = FakeStorage()
        self.settings = Settings(
            redis_url="redis://unused",
            allowed_origins=("https://shop.example",),
            allowed_hosts=("testserver",),
            rate_limit_requests=100,
            rate_limit_window_seconds=60,
            seed_inventory=(("ITEM_01", 20, 125), ("ITEM_02", 50, 275)),
            event_poll_interval_seconds=0.01,
            sse_connections_per_ip=1,
        )
        self.app = create_app(
            settings=self.settings,
            redis_client=self.redis,
            storage_client=self.storage,
        )
        self.lifespan = self.app.router.lifespan_context(self.app)
        await self.lifespan.__aenter__()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://testserver",
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        await self.lifespan.__aexit__(None, None, None)

    async def checkout(self, key: str, quantity: int = 1, sku: str = "ITEM_01"):
        return await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": key},
            json={
                "email": "SHOPPER@example.com",
                "items": [{"sku": sku, "quantity": quantity}],
            },
        )

    async def test_duplicate_checkout_returns_same_durable_order(self) -> None:
        first = await self.checkout("checkout-key-0001")
        repeated = await self.checkout("checkout-key-0001")

        self.assertEqual(first.status_code, 202)
        self.assertEqual(first.json(), repeated.json())
        self.assertEqual(first.json()["status"], "received")
        self.assertEqual(len(self.storage.checkouts), 1)

    async def test_same_idempotency_key_with_different_payload_is_rejected(self) -> None:
        first = await self.checkout("checkout-key-0002", quantity=1)
        changed = await self.checkout("checkout-key-0002", quantity=2)

        self.assertEqual(first.status_code, 202)
        self.assertEqual(changed.status_code, 409)

    async def test_duplicate_sku_lines_are_coalesced_with_integer_precision(self) -> None:
        response = await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": "checkout-key-0015"},
            json={
                "email": "shopper@example.com",
                "items": [
                    {"sku": "ITEM_01", "quantity": 2},
                    {"sku": "ITEM_01", "quantity": 3},
                    {"sku": "ITEM_02", "quantity": 1},
                ],
            },
        )
        checkout_id = response.json()["checkout_id"]
        order = self.storage.checkouts[checkout_id]
        self.assertEqual(response.status_code, 202)
        self.assertEqual(
            order["items"],
            [
                {
                    "sku": "ITEM_01",
                    "quantity": 5,
                    "unit_price_cents": 125,
                    "line_total_cents": 625,
                },
                {
                    "sku": "ITEM_02",
                    "quantity": 1,
                    "unit_price_cents": 275,
                    "line_total_cents": 275,
                },
            ],
        )
        self.assertEqual(order["total_cents"], 900)
        self.assertEqual(response.json()["total_cents"], order["total_cents"])

    async def test_fractional_quantity_and_injection_like_sku_are_rejected(self) -> None:
        fractional = await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": "checkout-key-0016"},
            json={
                "email": "shopper@example.com",
                "items": [{"sku": "ITEM_01", "quantity": 1.5}],
            },
        )
        injection_like = await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": "checkout-key-0017"},
            json={
                "email": "shopper@example.com",
                "items": [{"sku": "ITEM_01'; DROP TABLE flashcart_inventory;--", "quantity": 1}],
            },
        )
        self.assertEqual(fractional.status_code, 422)
        self.assertEqual(injection_like.status_code, 422)

    async def test_inventory_reservation_and_ledger_are_atomic_under_concurrency(self) -> None:
        submissions = await asyncio.gather(
            *(
                self.checkout(f"concurrent-checkout-key-{index:04}", quantity=10)
                for index in range(10)
            )
        )
        checkout_ids = [response.json()["checkout_id"] for response in submissions]

        results = await asyncio.gather(
            *(self.storage.process_checkout(checkout_id) for checkout_id in checkout_ids)
        )

        self.assertEqual(results.count("completed"), 2)
        self.assertEqual(results.count("failed"), 8)
        self.assertEqual(self.storage.inventory["ITEM_01"], 0)
        self.assertEqual(
            sum(delta for _, sku, delta in self.storage.ledger if sku == "ITEM_01"),
            -20,
        )
        self.assertEqual(len(self.storage.ledger), 2)

    async def test_failed_inventory_check_does_not_partially_decrement_stock(self) -> None:
        self.storage.inventory["ITEM_02"] = 0
        submitted = await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": "checkout-key-0012"},
            json={
                "email": "shopper@example.com",
                "items": [
                    {"sku": "ITEM_01", "quantity": 2},
                    {"sku": "ITEM_02", "quantity": 1},
                ],
            },
        )
        checkout_id = submitted.json()["checkout_id"]

        status = await self.storage.process_checkout(checkout_id)

        self.assertEqual(status, "failed")
        self.assertEqual(self.storage.inventory["ITEM_01"], 20)
        self.assertEqual(self.storage.ledger, [])
        status_response = await self.client.get(
            f"/api/checkouts/{checkout_id}/status"
        )
        self.assertEqual(status_response.json()["failure_reason"], "insufficient_stock")
        event_response = await self.client.get(
            f"/api/checkouts/{checkout_id}/events"
        )
        self.assertIn('"failure_reason":"insufficient_stock"', event_response.text)

    async def test_status_endpoint_and_live_status_sse(self) -> None:
        submitted = await self.checkout("checkout-key-0013")
        checkout_id = submitted.json()["checkout_id"]
        response = await self.client.get(
            f"/api/checkouts/{checkout_id}/status"
        )
        self.assertEqual(response.json()["status"], "received")

        route = next(
            route
            for route in self.app.routes
            if getattr(route, "path", "") == "/api/checkouts/{checkout_id}/events"
        )

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": b"", "more_body": False}

        request = Request(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": f"/api/checkouts/{checkout_id}/events",
                "raw_path": f"/api/checkouts/{checkout_id}/events".encode(),
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 12345),
                "server": ("testserver", 80),
                "app": self.app,
            },
            receive=receive,
        )
        stream_response = await route.endpoint(checkout_id, request)
        limited_response = await route.endpoint(checkout_id, request)
        self.assertEqual(limited_response.status_code, 429)
        stream = stream_response.body_iterator
        initial = await anext(stream)
        await self.storage.process_checkout(checkout_id)
        update = await anext(stream)
        await stream.aclose()
        initial_text = initial.decode() if isinstance(initial, bytes) else initial
        update_text = update.decode() if isinstance(update, bytes) else update
        self.assertIn('"status":"received"', initial_text)
        self.assertIn('"status":"completed"', update_text)

    async def test_unknown_checkout_status_returns_not_found(self) -> None:
        response = await self.client.get(
            "/api/checkouts/00000000-0000-4000-8000-000000000000/status"
        )
        self.assertEqual(response.status_code, 404)

    async def test_rate_limit_returns_retry_after(self) -> None:
        settings = Settings(
            redis_url="redis://unused",
            allowed_origins=("https://shop.example",),
            allowed_hosts=("testserver",),
            rate_limit_requests=1,
            rate_limit_window_seconds=60,
            seed_inventory=(("ITEM_01", 20),),
        )
        app = create_app(settings, self.redis, self.storage)
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                first = await client.post(
                    "/api/checkout",
                    headers={"Idempotency-Key": "rate-limit-key-0001"},
                    json={
                        "email": "shopper@example.com",
                        "items": [{"sku": "ITEM_01", "quantity": 1}],
                    },
                )
                limited = await client.post(
                    "/api/checkout",
                    headers={"Idempotency-Key": "rate-limit-key-0002"},
                    json={
                        "email": "shopper@example.com",
                        "items": [{"sku": "ITEM_01", "quantity": 1}],
                    },
                )
            self.assertEqual(first.status_code, 202)
            self.assertEqual(limited.status_code, 429)
            self.assertEqual(limited.headers["Retry-After"], "60")
        finally:
            await lifespan.__aexit__(None, None, None)

    async def test_forged_forwarded_for_does_not_bypass_rate_limit(self) -> None:
        redis = FakeRedis()
        settings = Settings(
            redis_url="redis://unused",
            allowed_origins=("https://shop.example",),
            allowed_hosts=("testserver",),
            rate_limit_requests=1,
            rate_limit_window_seconds=60,
            seed_inventory=(("ITEM_01", 20),),
        )
        app = create_app(settings, redis, self.storage)
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                body = {
                    "email": "shopper@example.com",
                    "items": [{"sku": "ITEM_01", "quantity": 1}],
                }
                first = await client.post(
                    "/api/checkout",
                    headers={
                        "Idempotency-Key": "spoof-test-key-0001",
                        "X-Forwarded-For": "203.0.113.1",
                    },
                    json=body,
                )
                second = await client.post(
                    "/api/checkout",
                    headers={
                        "Idempotency-Key": "spoof-test-key-0002",
                        "X-Forwarded-For": "198.51.100.8",
                    },
                    json=body,
                )
            self.assertEqual(first.status_code, 202)
            self.assertEqual(second.status_code, 429)
        finally:
            await lifespan.__aexit__(None, None, None)

    async def test_redis_rate_limit_failure_falls_back_to_database(self) -> None:
        redis = FakeRedis(fail=True)
        app = create_app(self.settings, redis, self.storage)
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/api/checkout",
                    headers={"Idempotency-Key": "fallback-key-0001"},
                    json={
                        "email": "shopper@example.com",
                        "items": [{"sku": "ITEM_01", "quantity": 1}],
                    },
                )
            self.assertEqual(response.status_code, 202)
            self.assertIsNone(app.state.redis)
        finally:
            await lifespan.__aexit__(None, None, None)

    async def test_inventory_endpoint_returns_seeded_stock(self) -> None:
        response = await self.client.get("/api/inventory")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["sku"], "ITEM_01")
        self.assertEqual(response.json()["items"][0]["available_quantity"], 20)

    async def test_invalid_fields_and_unknown_fields_are_rejected(self) -> None:
        response = await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": "checkout-key-0003"},
            json={
                "email": "not-an-email",
                "items": [{"sku": "../item", "quantity": 100}],
                "admin": True,
            },
        )
        self.assertEqual(response.status_code, 422)
        client_price = await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": "checkout-key-price-0001"},
            json={
                "email": "shopper@example.com",
                "items": [
                    {"sku": "ITEM_01", "quantity": 1, "unit_price_cents": 1}
                ],
                "total_cents": 1,
            },
        )
        self.assertEqual(client_price.status_code, 422)

    async def test_malformed_idempotency_key_is_rejected(self) -> None:
        response = await self.checkout("short")
        self.assertEqual(response.status_code, 400)

    async def test_oversized_content_length_and_chunked_bodies_are_rejected(self) -> None:
        sized = await self.client.post(
            "/api/checkout",
            headers={
                "Idempotency-Key": "checkout-key-0004",
                "Content-Type": "application/json",
            },
            content=b" " * (16 * 1024 + 1),
        )
        chunked = await self.client.post(
            "/api/checkout",
            headers={"Idempotency-Key": "checkout-key-0005"},
            content=OversizedChunkedBody(),
        )
        self.assertEqual(sized.status_code, 413)
        self.assertEqual(chunked.status_code, 413)

    async def test_host_and_origin_access_are_restricted(self) -> None:
        allowed = await self.client.get(
            "/", headers={"Origin": "https://shop.example"}
        )
        blocked = await self.client.get(
            "/", headers={"Origin": "https://attacker.example"}
        )
        invalid_host = await self.client.get(
            "http://attacker.example/",
            headers={"Host": "attacker.example"},
        )
        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(blocked.status_code, 403)
        self.assertEqual(invalid_host.status_code, 400)

    async def test_live_operations_console_sends_sanitized_events(self) -> None:
        submitted = await self.checkout("checkout-key-0014")
        checkout_id = submitted.json()["checkout_id"]
        route = next(
            route
            for route in self.app.routes
            if getattr(route, "path", "") == "/api/operations/events"
        )

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": b"", "more_body": False}

        request = Request(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/api/operations/events",
                "raw_path": b"/api/operations/events",
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 12345),
                "server": ("testserver", 80),
                "app": self.app,
            },
            receive=receive,
        )
        response = await route.endpoint(request, after=0)
        stream = response.body_iterator
        try:
            first = await anext(stream)
            await self.storage.process_checkout(checkout_id)
            processing = await anext(stream)
            completed = await anext(stream)
        finally:
            await stream.aclose()

        data = "\n".join(
            chunk.decode() if isinstance(chunk, bytes) else chunk
            for chunk in (first, processing, completed)
        )
        self.assertIn("checkout.received", data)
        self.assertIn("checkout.processing", data)
        self.assertIn("checkout.completed", data)
        self.assertNotIn("SHOPPER@example.com", data)

    async def test_security_headers_and_checkout_assets_are_set(self) -> None:
        response = await self.client.get("/")
        script = await self.client.get("/checkout.js")
        styles = await self.client.get("/checkout.css")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])
        self.assertIn("text/javascript", script.headers["Content-Type"])
        self.assertIn("text/css", styles.headers["Content-Type"])


if __name__ == "__main__":
    unittest.main()
