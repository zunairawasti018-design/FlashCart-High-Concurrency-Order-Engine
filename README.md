# FlashCart Order Engine

FlashCart is a containerized checkout demo for sudden traffic surges and limited inventory. PostgreSQL is the source of truth; orders are simulated and no payment is collected.

## One-command local startup

Install and start Docker Desktop, then run this from the project directory:

```powershell
docker compose up --build
```

Open [http://localhost:8000](http://localhost:8000). If port 8000 is occupied, set `$env:FLASHCART_PORT = "8001"` before starting and open [http://localhost:8001](http://localhost:8001). Stop with `Ctrl+C` or `docker compose down`. The normal development PostgreSQL data is kept in a named volume; `docker compose down -v` removes it.

The local stack runs PostgreSQL, Redis, FastAPI, and an independent order worker. The default sample inventory is `ITEM_01:100,ITEM_02:50`. PostgreSQL's host port is loopback-only at `127.0.0.1:55432`. Sample credentials and values are for local development only. Optional settings are in [.env.example](./.env.example).

## User and system flow

```mermaid
flowchart TD
    U[Customer] -->|Choose SKU and quantity| UI[Checkout UI]
    UI -->|POST checkout + idempotency key| EDGE[Host and Origin checks]
    EDGE -->|Rejected| E403[403 forbidden]
    EDGE -->|Allowed| SIZE[Request size and schema validation]
    SIZE -->|Invalid or oversized| E422[422 validation error]
    SIZE -->|Valid request| RATE[Per-IP checkout rate limit]
    RATE -->|Over limit| E429[429 with Retry-After]
    RATE -->|Allowed| IDEM[Hash idempotency key and request]
    IDEM -->|Same key and payload| REPLAY[Return existing checkout and total]
    IDEM -->|Same key different payload| E409[409 idempotency conflict]
    IDEM -->|New key| TX[Transaction: price snapshot, items, outbox]
    TX --> DB[(PostgreSQL)]
    DB -->|202 with checkout ID and total cents| UI
    DB -->|Transactional outbox relay| RS[Redis Streams]
    DB -. Redis unavailable .-> FALLBACK[Worker polls PostgreSQL]
    RS --> W[Order worker]
    FALLBACK --> W
    W -->|Claim order and lock SKU rows in sorted order| STOCK{Enough stock?}
    STOCK -->|Yes: decrement and append immutable ledger| DONE[completed]
    STOCK -->|No partial decrement| SOLD[failed: insufficient_stock]
    W -->|Processing exception| RETRY{Delivery count below 5?}
    RETRY -->|Yes| RS
    RETRY -->|No| DLQ[DLQ stream; failed: processing_error]
    DONE --> DB
    SOLD --> DB
    DLQ --> DB
    UI -->|Checkout SSE and operations SSE| API[FastAPI]
    API -->|Persisted checkout state| DB
    API -->|One shared operations-log poller| DB
    DB -->|Status, total, failure reason| UI
```

## Architecture and grading criteria

- **Separation:** `app/main.py` owns HTTP validation, origin/host checks, rate limiting, Redis reconnection, and SSE transport; `app/storage.py` owns PostgreSQL transactions, idempotency, inventory, outbox, and ledger; `app/worker.py` owns Redis Streams, retries, PostgreSQL fallback, and housekeeping. `static/` is the browser UI; `tests/` separates in-memory API tests from real PostgreSQL integration tests.
- **System flow mapping:** the diagram includes origin rejection, 422 validation, 429 rate limiting, idempotency replay/conflict, stock rejection, retry, and DLQ.
- **Mathematical precision:** quantities and money are stored/processed as integers; price and line totals are snapshotted from server inventory, never provided by the client.
- **Testing isolation:** API tests use a fresh fake adapter; each integration test creates and drops its own random schema. Compose's test profile uses a separate temporary PostgreSQL with a tmpfs data directory.
- **Security:** body size and schema validation, Host/Origin checks, security headers, idempotency enforcement, rate limiting, no client-provided totals, and bounded SSE connections are covered by tests.
- **Load resistance:** parallel PostgreSQL tests and a local exhaustion load checker validate inventory/ledger conservation. Run the latter only against a disposable local stack.

## Phase 1 — ingress protection

- Request bodies are capped at 16 KiB; checkout fields are validated and unknown fields are rejected.
- Host allowlisting, exact Origin checks, security headers, and rate limiting are enabled.
- Idempotency-key hashes and request hashes are stored durably. A matching replay returns the same checkout; reusing a key with a different payload returns `409`.
- Redis provides the fast distributed rate-limit path. When Redis is unavailable, an atomic PostgreSQL rate limiter is used; the API periodically retries its Redis connection.

## Phase 2 — status synchronization and access controls

- Checkout status follows `received → processing → completed` or `failed`.
- `GET /api/checkouts/{checkout_id}/status` returns the persisted status, `total_cents`, `currency`, and `failure_reason` when present.
- `GET /api/checkouts/{checkout_id}/events` streams persisted checkout state. Operations SSE connections receive broadcasts from one shared PostgreSQL poller rather than polling once per client.
- Both SSE endpoints allow at most 10 concurrent connections per client IP by default. Configure `FLASHCART_SSE_CONNECTIONS_PER_IP` to change this cap.
- Configured hosts and exact browser origins are restricted. If `FLASHCART_ALLOWED_ORIGINS` is empty, same-origin browser requests are allowed.

## Phase 3 — storage and concurrency

- PostgreSQL is authoritative for checkout state, inventory, idempotency, rate-limit fallback, the immutable ledger, and the transactional outbox. Redis accelerates rate limiting and distributes work; it is not the ledger.
- Reservation locks all relevant inventory rows in SKU order, verifies every line, then decrements stock and appends ledger entries in the same transaction. If any SKU is unavailable, no inventory changes commit.
- Ledger rows have a database trigger rejecting update/delete operations. A completed checkout has at most one ledger row per SKU.
- The checkout transaction writes its `checkout.requested` outbox event with the order. The worker relays pending outbox rows to Redis Streams and consumes them through a consumer group.
- If Redis is unavailable, the worker polls PostgreSQL for unprocessed or lease-expired checkouts. PostgreSQL row locks and worker leases allow multiple workers to compete safely.
- Quantities use integral database types. For a SKU without restocking, `initial stock = remaining stock + absolute sum of ledger deltas`.
- If an order cannot be processed after five stream deliveries, the worker copies the message to `flashcart:orders:dlq`, records an `ERROR` log, marks the checkout `failed` with `failure_reason='processing_error'`, then acknowledges the original. Insufficient stock fails with `failure_reason='insufficient_stock'`.
- Worker housekeeping runs periodically: operation logs are trimmed to the newest 5,000 rows and outbox records published more than one hour ago are removed.

**Money:** `flashcart_inventory.unit_price_cents` is a positive `BIGINT`; checkout items retain `unit_price_cents` and `line_total_cents`; checkouts retain server-calculated `total_cents`. Multiplication and summation are checked against the PostgreSQL BIGINT maximum before persistence. Duplicate SKU lines are combined before totals are calculated. The browser formats cents for display only.

`FLASHCART_SEED_INVENTORY` accepts `SKU:QTY:PRICE_CENTS`, for example `ITEM_01:100:2499`. The legacy `SKU:QTY` format still works and uses the default price of 1999 cents. Currency defaults to USD. Existing seeded rows are not overwritten on restart; updating a SKU price affects new checkout snapshots only.

**Failure boundary:** the app and workers can share PostgreSQL across multiple nodes, and Redis loss falls back to PostgreSQL. A single PostgreSQL primary remains a failure boundary; automatic database failover requires a managed/high-availability PostgreSQL deployment and is not provided by local Compose.

## Phase 4 — operations UI and local environment

- The checkout UI shows inventory, unit price, calculated total, status, and a live operations console.
- Operations events omit emails, request bodies, and credentials; checkout IDs are reduced to an eight-character prefix before they are broadcast. Browser rendering uses text-only nodes.
- Containers run as a non-root `flashcart` user. The worker has a process healthcheck.
- `docker compose up --build` builds and starts the local environment.

## API endpoints

- `GET /api/inventory` — current inventory and unit price in cents
- `POST /api/checkout` — create/replay a checkout; requires an `Idempotency-Key` header
- `GET /api/checkouts/{checkout_id}/status` — status, total, currency, and optional failure reason
- `GET /api/checkouts/{checkout_id}/events` — SSE checkout state updates
- `GET /api/operations/events` — SSE sanitized operations log
- `GET /health/live` and `GET /health/ready` — liveness and PostgreSQL readiness

The server accepts no client price or total. Money values are returned as integer cents.

## Tests and isolated PostgreSQL

Run the full unit/API and PostgreSQL integration suites in one command:

```powershell
docker compose --profile test run --rm tests
```

The `test` profile creates an isolated PostgreSQL service using tmpfs (no persistent volume) and runs `python -m unittest discover -s tests -v`. The 250-way PostgreSQL concurrency test runs in that database. Integration tests create a unique schema per test and drop only that schema.

For non-containerized unit tests, install the exact versions pinned in `requirements.txt` and run `python -m unittest discover -s tests -v`. To run PostgreSQL integration tests directly, set `FLASHCART_TEST_DATABASE_URL` to a dedicated test database; do not use production.

## Ledger audit

Run the audit script against the database URL in `FLASHCART_DATABASE_URL`:

```powershell
$env:FLASHCART_DATABASE_URL = "postgresql://flashcart:flashcart@127.0.0.1:55432/flashcart"
python scripts\audit_ledger.py
```

It verifies every SKU's initial/remaining stock against ledger deltas, rejects negative stock, and checks completed checkout items against ledger entries. It prints a non-zero exit status on any mismatch. The load checker runs this audit automatically at the end.

## Bounded exhaustion load check

The local-only script refuses non-local HTTP targets, supports up to 2,000 requests and concurrency 100, verifies the stock delta against completed orders, and invokes the ledger audit. Load checks create orders and consume inventory; use a separate disposable Compose project. Configure the rate limit above the request count so the run is not truncated by `429`.

This example seeds 50 units and submits 300 concurrent quantity-one checkouts. Expected outcome: 50 completed, 250 failed, final stock 0, ledger sum `-50`.

```powershell
$env:FLASHCART_PORT = "8001"
$env:FLASHCART_POSTGRES_PORT = "55433"
$env:FLASHCART_SEED_INVENTORY = "LOAD_TEST:50:1999"
$env:FLASHCART_RATE_LIMIT_REQUESTS = "500"
docker compose -p flashcart-exhaust up --build -d
$env:FLASHCART_DATABASE_URL = "postgresql://flashcart:flashcart@127.0.0.1:55433/flashcart"
python scripts\load_check.py --base-url http://127.0.0.1:8001 --sku LOAD_TEST --requests 300 --concurrency 100 --exhaust
docker compose -p flashcart-exhaust down -v
```

Sample result (timing varies by machine):

```text
Submitted: 300
Accepted: 300
Rate-limited: 0
Completed: 50
Failed: 250
Inventory conservation: 50 - 0 = 50 (expected 50)
Exhaustion assertion: 50 completed, 250 failed, stock 0
Ledger: LOAD_TEST initial=50 remaining=0 ledger_delta=-50
Ledger audit passed: inventory, balances, and completed orders reconcile.
```

`FLASHCART_RATE_LIMIT_REQUESTS` defaults to 30 per `FLASHCART_RATE_LIMIT_WINDOW_SECONDS` (default 60 seconds); raise it for load tests and restore it afterward. `FLASHCART_SSE_CONNECTIONS_PER_IP` sets the SSE connection cap, and `FLASHCART_REDIS_RECONNECT_SECONDS` sets the API Redis retry interval. Local load numbers are not production capacity guarantees.
