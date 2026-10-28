from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

import asyncpg
from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from app.storage import (
    DEFAULT_UNIT_PRICE_CENTS,
    MAX_BIGINT,
    IdempotencyConflict,
    PostgresStorage,
    UnknownSkuError,
)

logger = logging.getLogger("flashcart")
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
MAX_REQUEST_BYTES = 16 * 1024
TERMINAL_STATUSES = frozenset({"completed", "failed"})

RATE_LIMIT_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
local ttl = redis.call('TTL', KEYS[1])
if count == 1 or ttl < 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    ttl = tonumber(ARGV[1])
end
return {count, ttl}
"""


@dataclass(frozen=True)
class Settings:
    redis_url: str
    allowed_origins: tuple[str, ...]
    allowed_hosts: tuple[str, ...]
    rate_limit_requests: int
    rate_limit_window_seconds: int
    database_url: str = "postgresql://flashcart:flashcart@localhost:5432/flashcart"
    seed_inventory: tuple[tuple[str, int] | tuple[str, int, int], ...] = (
        ("ITEM_01", 100, DEFAULT_UNIT_PRICE_CENTS),
        ("ITEM_02", 50, DEFAULT_UNIT_PRICE_CENTS),
    )
    event_poll_interval_seconds: float = 0.5
    sse_connections_per_ip: int = 10
    redis_reconnect_seconds: float = 5

    @classmethod
    def from_environment(cls) -> Settings:
        origins = tuple(
            origin.strip()
            for origin in os.getenv("FLASHCART_ALLOWED_ORIGINS", "").split(",")
            if origin.strip()
        )
        hosts = tuple(
            host.strip()
            for host in os.getenv(
                "FLASHCART_ALLOWED_HOSTS", "localhost,127.0.0.1,testserver"
            ).split(",")
            if host.strip()
        )
        seed_inventory: list[tuple[str, int] | tuple[str, int, int]] = []
        for entry in os.getenv(
            "FLASHCART_SEED_INVENTORY", "ITEM_01:100,ITEM_02:50"
        ).split(","):
            if not entry.strip():
                continue
            parts = entry.strip().split(":")
            if len(parts) not in {2, 3} or not re.fullmatch(
                r"[A-Za-z0-9_-]{1,64}", parts[0]
            ):
                raise ValueError(f"Invalid inventory seed entry: {entry!r}")
            try:
                quantity = int(parts[1])
                price_cents = (
                    int(parts[2]) if len(parts) == 3 else DEFAULT_UNIT_PRICE_CENTS
                )
            except ValueError as error:
                raise ValueError(f"Invalid inventory quantity or price: {entry!r}") from error
            if not 0 <= quantity <= MAX_BIGINT:
                raise ValueError("Inventory seed quantities must be within BIGINT range")
            if not 0 < price_cents <= MAX_BIGINT:
                raise ValueError("Inventory seed prices must be positive BIGINT values")
            if len(parts) == 3:
                seed_inventory.append((parts[0], quantity, price_cents))
            else:
                seed_inventory.append((parts[0], quantity))

        settings = cls(
            redis_url=os.getenv("FLASHCART_REDIS_URL", "redis://localhost:6379/0"),
            allowed_origins=origins,
            allowed_hosts=hosts,
            rate_limit_requests=int(os.getenv("FLASHCART_RATE_LIMIT_REQUESTS", "30")),
            rate_limit_window_seconds=int(
                os.getenv("FLASHCART_RATE_LIMIT_WINDOW_SECONDS", "60")
            ),
            database_url=os.getenv(
                "FLASHCART_DATABASE_URL",
                "postgresql://flashcart:flashcart@localhost:5432/flashcart",
            ),
            seed_inventory=tuple(seed_inventory),
            event_poll_interval_seconds=float(
                os.getenv("FLASHCART_EVENT_POLL_SECONDS", "0.5")
            ),
            sse_connections_per_ip=int(
                os.getenv("FLASHCART_SSE_CONNECTIONS_PER_IP", "10")
            ),
            redis_reconnect_seconds=float(
                os.getenv("FLASHCART_REDIS_RECONNECT_SECONDS", "5")
            ),
        )
        if not settings.allowed_hosts:
            raise ValueError("FLASHCART_ALLOWED_HOSTS must contain at least one host")
        if settings.rate_limit_requests < 1 or settings.rate_limit_window_seconds < 1:
            raise ValueError("Rate-limit settings must be positive integers")
        if settings.event_poll_interval_seconds <= 0:
            raise ValueError("Event poll interval must be a positive number")
        if settings.sse_connections_per_ip < 1:
            raise ValueError("SSE connections per IP must be a positive integer")
        if settings.redis_reconnect_seconds <= 0:
            raise ValueError("Redis reconnect interval must be a positive number")
        if len({item[0] for item in settings.seed_inventory}) != len(settings.seed_inventory):
            raise ValueError("Inventory seed SKUs must be unique")
        return settings


def seed_inventory_mapping(
    seed_inventory: tuple[tuple[str, int] | tuple[str, int, int], ...]
) -> dict[str, int | tuple[int, int]]:
    return {
        entry[0]: entry[1] if len(entry) == 2 else (entry[1], entry[2])
        for entry in seed_inventory
    }


class RedisClient(Protocol):
    async def ping(self) -> Any: ...

    async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any: ...

    async def aclose(self) -> None: ...


class Storage(Protocol):
    async def initialize(
        self, seed_inventory: dict[str, int | tuple[int, int] | tuple[int, int, str]]
    ) -> None: ...

    async def ping(self) -> Any: ...

    async def check_rate_limit(
        self, client_key_hash: str, window_seconds: int
    ) -> tuple[int, int]: ...

    async def create_checkout(
        self,
        idempotency_key_hash: str,
        request_hash: str,
        email: str,
        items: list[dict[str, Any]],
    ) -> Any: ...

    async def get_status(self, checkout_id: str) -> dict[str, Any] | None: ...

    async def get_inventory(self) -> list[dict[str, Any]]: ...

    async def operation_events(self, after_id: int, limit: int = 100) -> list[Any]: ...


class SSEConnectionLimiter:
    def __init__(self, limit_per_ip: int) -> None:
        self.limit_per_ip = limit_per_ip
        self._connections: Counter[str] = Counter()
        self._lock = asyncio.Lock()

    async def reserve(self, ip_address: str) -> bool:
        async with self._lock:
            if self._connections[ip_address] >= self.limit_per_ip:
                return False
            self._connections[ip_address] += 1
            return True

    async def release(self, ip_address: str) -> None:
        async with self._lock:
            if self._connections[ip_address] <= 1:
                self._connections.pop(ip_address, None)
            else:
                self._connections[ip_address] -= 1


class OperationsEventBroadcaster:
    def __init__(self, storage: Storage, poll_interval: float) -> None:
        self.storage = storage
        self.poll_interval = poll_interval
        self.cursor = 0
        self.history: deque[dict[str, Any]] = deque(maxlen=5_000)
        self.subscribers: set[asyncio.Queue[dict[str, Any]]] = set()

    async def subscribe(self, after_id: int) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1_000)
        replay = [event for event in self.history if event["id"] > after_id]
        for event in replay[-queue.maxsize :]:
            queue.put_nowait(event)
        self.subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self.subscribers.discard(queue)

    async def run(self) -> None:
        while True:
            try:
                events = await self.storage.operation_events(self.cursor, limit=100)
            except (asyncpg.PostgresError, OSError, TimeoutError):
                logger.exception("Shared operations event poll failed")
                await asyncio.sleep(self.poll_interval)
                continue

            for event in events:
                raw_data = event.as_dict() if hasattr(event, "as_dict") else event
                data = dict(raw_data)
                checkout_id = data.get("checkout_id")
                if isinstance(checkout_id, str):
                    data["checkout_id"] = checkout_id[:8]
                self.cursor = int(data["id"])
                self.history.append(data)
                for queue in tuple(self.subscribers):
                    if queue.full():
                        try:
                            queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    queue.put_nowait(data)
            if not events:
                await asyncio.sleep(self.poll_interval)


class RequestSizeLimitMiddleware:
    def __init__(self, app: Any, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        raw_length = headers.get(b"content-length")
        if raw_length is not None:
            try:
                content_length = int(raw_length)
            except ValueError:
                await JSONResponse(
                    status_code=400, content={"detail": "Invalid Content-Length"}
                )(scope, receive, send)
                return
            if content_length < 0:
                await JSONResponse(
                    status_code=400, content={"detail": "Invalid Content-Length"}
                )(scope, receive, send)
                return
            if content_length > self.max_bytes:
                await JSONResponse(
                    status_code=413, content={"detail": "Request body too large"}
                )(scope, receive, send)
                return

        body_parts: list[bytes] = []
        received_bytes = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            if message["type"] != "http.request":
                continue
            body = message.get("body", b"")
            received_bytes += len(body)
            if received_bytes > self.max_bytes:
                await JSONResponse(
                    status_code=413, content={"detail": "Request body too large"}
                )(scope, receive, send)
                return
            body_parts.append(body)
            if not message.get("more_body", False):
                break

        request_body = b"".join(body_parts)
        body_sent = False

        async def replay_receive() -> dict[str, Any]:
            nonlocal body_sent
            if not body_sent:
                body_sent = True
                return {
                    "type": "http.request",
                    "body": request_body,
                    "more_body": False,
                }
            return await receive()

        await self.app(scope, replay_receive, send)


class CheckoutItem(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    sku: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    quantity: int = Field(ge=1, le=10)


class CheckoutRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    email: str = Field(min_length=3, max_length=254)
    items: list[CheckoutItem] = Field(min_length=1, max_length=10)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
            raise ValueError("Enter a valid email address")
        return value.lower()


def _client_address(request: Request) -> str:
    if request.client is None or not request.client.host:
        return "unknown"
    return request.client.host


def _redis_key(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"flashcart:{prefix}:{digest}"


async def _redis_reconnect_loop(application: FastAPI, settings: Settings) -> None:
    while True:
        await asyncio.sleep(settings.redis_reconnect_seconds)
        if application.state.redis is not None:
            continue
        client = Redis.from_url(settings.redis_url, decode_responses=True)
        try:
            await client.ping()
        except RedisError:
            await client.aclose()
            logger.warning("Redis is still unavailable; retrying connection later")
        else:
            application.state.redis = client
            logger.info("Redis connection recovered")


def create_app(
    settings: Settings | None = None,
    redis_client: RedisClient | None = None,
    storage_client: Storage | None = None,
) -> FastAPI:
    config = settings or Settings.from_environment()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        pool: asyncpg.Pool | None = None
        client: RedisClient | None = redis_client
        owns_redis = redis_client is None
        background_tasks: list[asyncio.Task[None]] = []
        try:
            if storage_client is None:
                pool = await asyncpg.create_pool(
                    config.database_url,
                    min_size=1,
                    max_size=10,
                    command_timeout=10,
                )
                storage = PostgresStorage(pool)
            else:
                storage = storage_client
            application.state.storage = storage
            await storage.initialize(seed_inventory_mapping(config.seed_inventory))

            if client is None:
                redis = Redis.from_url(config.redis_url, decode_responses=True)
                try:
                    await redis.ping()
                except RedisError:
                    logger.warning(
                        "Redis unavailable at startup; using PostgreSQL rate-limit and processing fallbacks"
                    )
                    await redis.aclose()
                else:
                    client = redis
            application.state.redis = client
            application.state.sse_limiter = SSEConnectionLimiter(
                config.sse_connections_per_ip
            )
            broadcaster = OperationsEventBroadcaster(
                storage, config.event_poll_interval_seconds
            )
            application.state.operations_broadcaster = broadcaster
            background_tasks.append(asyncio.create_task(broadcaster.run()))
            if owns_redis:
                background_tasks.append(
                    asyncio.create_task(_redis_reconnect_loop(application, config))
                )
            yield
        finally:
            for task in background_tasks:
                task.cancel()
            if background_tasks:
                await asyncio.gather(*background_tasks, return_exceptions=True)
            current_redis: RedisClient | None = application.state.redis
            if owns_redis and current_redis is not None:
                await current_redis.aclose()
            if pool is not None:
                await pool.close()

    application = FastAPI(
        title="FlashCart Checkout Engine",
        version="0.3.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    application.add_middleware(RequestSizeLimitMiddleware, max_bytes=MAX_REQUEST_BYTES)
    application.add_middleware(
        TrustedHostMiddleware, allowed_hosts=list(config.allowed_hosts)
    )
    if config.allowed_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=list(config.allowed_origins),
            allow_methods=["GET", "POST"],
            allow_headers=["Content-Type", "Idempotency-Key"],
            max_age=600,
        )

    @application.middleware("http")
    async def restrict_request_origins(request: Request, call_next: Any):
        origin = request.headers.get("origin")
        if origin and request.method != "OPTIONS":
            if config.allowed_origins:
                allowed = origin in config.allowed_origins
            else:
                request_origin = f"{request.url.scheme}://{request.url.netloc}".lower()
                allowed = origin.lower() == request_origin
            if not allowed:
                return JSONResponse(
                    status_code=403, content={"detail": "Origin is not allowed"}
                )
        return await call_next(request)

    @application.middleware("http")
    async def add_security_headers(request: Request, call_next: Any):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'self'; form-action 'self'; frame-ancestors 'none'"
        )
        return response

    @application.middleware("http")
    async def limit_checkout_traffic(request: Request, call_next: Any):
        if request.url.path != "/api/checkout" or request.method != "POST":
            return await call_next(request)

        client_hash = _redis_key("rate-client", _client_address(request))
        redis: RedisClient | None = request.app.state.redis
        if redis is not None:
            try:
                count, ttl = await redis.eval(
                    RATE_LIMIT_SCRIPT,
                    1,
                    _redis_key("rate", _client_address(request)),
                    config.rate_limit_window_seconds,
                )
            except RedisError:
                logger.warning("Redis rate limiter unavailable; using PostgreSQL")
                request.app.state.redis = None
                await redis.aclose()
                try:
                    count, ttl = await request.app.state.storage.check_rate_limit(
                        client_hash, config.rate_limit_window_seconds
                    )
                except (asyncpg.PostgresError, OSError, TimeoutError):
                    logger.exception("PostgreSQL rate-limit fallback failed")
                    return JSONResponse(
                        status_code=503,
                        content={"detail": "Checkout is temporarily unavailable"},
                        headers={"Retry-After": "1"},
                    )
        else:
            try:
                count, ttl = await request.app.state.storage.check_rate_limit(
                    client_hash, config.rate_limit_window_seconds
                )
            except (asyncpg.PostgresError, OSError, TimeoutError):
                logger.exception("PostgreSQL rate-limit check failed")
                return JSONResponse(
                    status_code=503,
                    content={"detail": "Checkout is temporarily unavailable"},
                    headers={"Retry-After": "1"},
                )

        if int(count) > config.rate_limit_requests:
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many checkout attempts"},
                headers={"Retry-After": str(max(1, int(ttl)))},
            )
        return await call_next(request)

    @application.get("/", include_in_schema=False)
    async def checkout_page() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @application.get("/checkout.js", include_in_schema=False)
    async def checkout_script() -> FileResponse:
        return FileResponse(STATIC_DIR / "checkout.js", media_type="text/javascript")

    @application.get("/checkout.css", include_in_schema=False)
    async def checkout_styles() -> FileResponse:
        return FileResponse(STATIC_DIR / "checkout.css", media_type="text/css")

    @application.get("/health/live", include_in_schema=False)
    async def liveness() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/health/ready", include_in_schema=False)
    async def readiness(request: Request) -> JSONResponse:
        try:
            await request.app.state.storage.ping()
        except (asyncpg.PostgresError, OSError, TimeoutError):
            logger.exception("PostgreSQL readiness check failed")
            return JSONResponse(status_code=503, content={"status": "unavailable"})
        return JSONResponse(
            status_code=200,
            content={
                "status": "ready",
                "queue": "redis-streams" if request.app.state.redis else "database-fallback",
            },
        )

    @application.get("/api/inventory")
    async def inventory(request: Request) -> JSONResponse:
        try:
            items = await request.app.state.storage.get_inventory()
        except (asyncpg.PostgresError, OSError, TimeoutError):
            logger.exception("Inventory lookup failed")
            return JSONResponse(
                status_code=503, content={"detail": "Inventory is unavailable"}
            )
        return JSONResponse(status_code=200, content={"items": items})

    @application.post("/api/checkout", status_code=202)
    async def submit_checkout(
        payload: CheckoutRequest,
        request: Request,
        idempotency_key: str = Header(alias="Idempotency-Key"),
    ) -> JSONResponse:
        if not re.fullmatch(r"[A-Za-z0-9._~-]{16,128}", idempotency_key):
            return JSONResponse(
                status_code=400,
                content={
                    "detail": "Idempotency-Key must be 16-128 safe ASCII characters"
                },
            )

        item_quantities: Counter[str] = Counter()
        for item in payload.items:
            item_quantities[item.sku] += item.quantity
        normalized_items = [
            {"sku": sku, "quantity": quantity}
            for sku, quantity in sorted(item_quantities.items())
        ]
        if any(quantity > MAX_BIGINT for quantity in item_quantities.values()):
            return JSONResponse(
                status_code=422, content={"detail": "Requested quantity is too large"}
            )
        canonical_payload = json.dumps(
            {"email": payload.email, "items": normalized_items},
            sort_keys=True,
            separators=(",", ":"),
        )
        request_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
        key_hash = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()

        try:
            record = await request.app.state.storage.create_checkout(
                key_hash, request_hash, payload.email, normalized_items
            )
        except IdempotencyConflict:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "Idempotency-Key was already used with a different request"
                },
            )
        except UnknownSkuError:
            return JSONResponse(
                status_code=422, content={"detail": "One or more SKUs are unavailable"}
            )
        except OverflowError:
            return JSONResponse(
                status_code=422, content={"detail": "Checkout total exceeds supported range"}
            )
        except ValueError:
            return JSONResponse(
                status_code=422, content={"detail": "Checkout currencies must match"}
            )
        except (asyncpg.PostgresError, OSError, TimeoutError):
            logger.exception("Durable checkout creation failed")
            return JSONResponse(
                status_code=503,
                content={"detail": "Checkout is temporarily unavailable"},
                headers={"Retry-After": "1"},
            )
        return JSONResponse(
            status_code=202,
            content={
                "checkout_id": record.checkout_id,
                "status": record.status,
                "total_cents": record.total_cents,
                "currency": record.currency,
            },
        )

    @application.get("/api/checkouts/{checkout_id}/status")
    async def checkout_status(checkout_id: str, request: Request) -> JSONResponse:
        try:
            canonical_id = str(UUID(checkout_id))
        except ValueError:
            return JSONResponse(status_code=404, content={"detail": "Checkout not found"})
        try:
            status = await request.app.state.storage.get_status(canonical_id)
        except (asyncpg.PostgresError, OSError, TimeoutError):
            logger.exception("Checkout status lookup failed")
            return JSONResponse(
                status_code=503, content={"detail": "Checkout status is unavailable"}
            )
        if status is None:
            return JSONResponse(status_code=404, content={"detail": "Checkout not found"})
        return JSONResponse(
            status_code=200,
            content={"checkout_id": canonical_id, **status},
        )

    @application.get("/api/checkouts/{checkout_id}/events")
    async def checkout_events(checkout_id: str, request: Request) -> Response:
        try:
            canonical_id = str(UUID(checkout_id))
            status = await request.app.state.storage.get_status(canonical_id)
        except ValueError:
            return JSONResponse(status_code=404, content={"detail": "Checkout not found"})
        except (asyncpg.PostgresError, OSError, TimeoutError):
            logger.exception("Checkout SSE status lookup failed")
            return JSONResponse(
                status_code=503, content={"detail": "Checkout status is unavailable"}
            )
        if status is None:
            return JSONResponse(status_code=404, content={"detail": "Checkout not found"})

        client_ip = _client_address(request)
        sse_limiter: SSEConnectionLimiter = request.app.state.sse_limiter
        if not await sse_limiter.reserve(client_ip):
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many live event connections"},
                headers={"Retry-After": "5"},
            )

        async def event_stream():
            try:
                current_status = status
                yield (
                    "event: status\ndata: "
                    + json.dumps(
                        {"checkout_id": canonical_id, **current_status},
                        separators=(",", ":"),
                    )
                    + "\n\n"
                )
                while current_status["status"] not in TERMINAL_STATUSES:
                    if await request.is_disconnected():
                        return
                    await asyncio.sleep(config.event_poll_interval_seconds)
                    try:
                        next_status = await request.app.state.storage.get_status(
                            canonical_id
                        )
                    except (asyncpg.PostgresError, OSError, TimeoutError):
                        logger.exception("Checkout SSE poll failed")
                        yield (
                            "event: error\ndata: "
                            '{"detail":"Checkout status temporarily unavailable"}\n\n'
                        )
                        return
                    if next_status is None:
                        yield "event: error\ndata: {\"detail\":\"Checkout not found\"}\n\n"
                        return
                    if next_status != current_status:
                        current_status = next_status
                        yield (
                            "event: status\ndata: "
                            + json.dumps(
                                {"checkout_id": canonical_id, **current_status},
                                separators=(",", ":"),
                            )
                            + "\n\n"
                        )
            finally:
                await sse_limiter.release(client_ip)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @application.get("/api/operations/events")
    async def operations_events(
        request: Request, after: int = Query(default=0, ge=0)
    ) -> Response:
        last_event_id = request.headers.get("last-event-id")
        cursor_start = (
            max(after, int(last_event_id))
            if last_event_id and last_event_id.isdigit()
            else after
        )

        client_ip = _client_address(request)
        sse_limiter: SSEConnectionLimiter = request.app.state.sse_limiter
        if not await sse_limiter.reserve(client_ip):
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many live event connections"},
                headers={"Retry-After": "5"},
            )
        broadcaster: OperationsEventBroadcaster = (
            request.app.state.operations_broadcaster
        )
        queue = await broadcaster.subscribe(cursor_start)

        async def event_stream():
            try:
                while not await request.is_disconnected():
                    try:
                        data = await asyncio.wait_for(
                            queue.get(),
                            timeout=config.event_poll_interval_seconds * 2,
                        )
                    except TimeoutError:
                        yield ": keep-alive\n\n"
                        continue
                    cursor = int(data["id"])
                    yield (
                        f"id: {cursor}\nevent: log\ndata: "
                        + json.dumps(data, separators=(",", ":"))
                        + "\n\n"
                    )
            finally:
                broadcaster.unsubscribe(queue)
                await sse_limiter.release(client_ip)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return application


app = create_app()
