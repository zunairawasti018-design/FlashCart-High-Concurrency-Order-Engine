from __future__ import annotations

import argparse
import asyncio
import math
import statistics
import sys
import time
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import httpx


MAX_REQUESTS = 2_000
MAX_CONCURRENCY = 100
TERMINAL_STATUSES = {"completed", "failed"}


def assert_exhaustion_results(
    requests: int,
    quantity: int,
    initial_stock: int,
    remaining_stock: int,
    completed: int,
    failed: int,
    rate_limited: int,
) -> None:
    expected_completed = min(requests, initial_stock // quantity)
    expected_failed = requests - expected_completed
    if rate_limited:
        raise AssertionError(
            "Exhaustion run was rate-limited; raise FLASHCART_RATE_LIMIT_REQUESTS"
        )
    if completed != expected_completed or failed != expected_failed:
        raise AssertionError(
            "Exhaustion outcomes mismatch: "
            f"expected {expected_completed} completed / {expected_failed} failed, "
            f"got {completed} completed / {failed} failed"
        )
    if remaining_stock != initial_stock - expected_completed * quantity:
        raise AssertionError("Exhaustion run ended with unexpected remaining stock")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a bounded checkout load check against a local FlashCart instance."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--sku", default="LOAD_TEST")
    parser.add_argument("--requests", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--wait-seconds", type=float, default=60)
    parser.add_argument(
        "--exhaust",
        action="store_true",
        help="Require requests to exceed the selected SKU's current stock and verify exact exhaustion",
    )
    arguments = parser.parse_args()

    parsed_url = urlparse(arguments.base_url)
    if parsed_url.scheme != "http" or parsed_url.hostname not in {
        "localhost",
        "127.0.0.1",
        "::1",
    }:
        parser.error("--base-url must target an HTTP localhost address")
    if not 1 <= arguments.requests <= MAX_REQUESTS:
        parser.error(f"--requests must be between 1 and {MAX_REQUESTS}")
    if not 1 <= arguments.concurrency <= MAX_CONCURRENCY:
        parser.error(f"--concurrency must be between 1 and {MAX_CONCURRENCY}")
    if not 1 <= arguments.quantity <= 10:
        parser.error("--quantity must be between 1 and 10")
    if not arguments.sku or len(arguments.sku) > 64:
        parser.error("--sku must contain between 1 and 64 characters")
    if arguments.wait_seconds <= 0:
        parser.error("--wait-seconds must be positive")
    return arguments


async def run_load_check(arguments: argparse.Namespace) -> None:
    base_url = arguments.base_url.rstrip("/")
    timeout = httpx.Timeout(10, connect=3)
    limits = httpx.Limits(
        max_connections=arguments.concurrency,
        max_keepalive_connections=arguments.concurrency,
    )
    async with httpx.AsyncClient(
        base_url=base_url, timeout=timeout, limits=limits
    ) as client:
        inventory_response = await client.get("/api/inventory")
        inventory_response.raise_for_status()
        inventory = {
            entry["sku"]: entry["available_quantity"]
            for entry in inventory_response.json()["items"]
        }
        if arguments.sku not in inventory:
            raise RuntimeError(
                f"SKU {arguments.sku!r} is not seeded. Start an isolated Compose "
                "stack with FLASHCART_SEED_INVENTORY including this SKU."
            )
        initial_stock = inventory[arguments.sku]
        if arguments.exhaust and arguments.requests * arguments.quantity <= initial_stock:
            raise ValueError(
                "--exhaust requires requests * quantity to exceed the selected SKU's stock"
            )

        semaphore = asyncio.Semaphore(arguments.concurrency)

        async def submit(index: int) -> tuple[float, str | None, int]:
            payload = {
                "email": f"load-{uuid4().hex}@example.test",
                "items": [
                    {"sku": arguments.sku, "quantity": arguments.quantity}
                ],
            }
            headers = {"Idempotency-Key": f"load-{uuid4().hex}-{index:04}"}
            started = time.perf_counter()
            async with semaphore:
                response = await client.post(
                    "/api/checkout", json=payload, headers=headers
                )
            elapsed = time.perf_counter() - started
            if response.status_code == 429:
                return elapsed, None, response.status_code
            if response.status_code != 202:
                raise RuntimeError(
                    f"Checkout request returned HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )
            return elapsed, response.json()["checkout_id"], response.status_code

        started = time.perf_counter()
        results = await asyncio.gather(
            *(submit(index) for index in range(arguments.requests))
        )
        submit_seconds = time.perf_counter() - started
        latencies = sorted(elapsed for elapsed, _, _ in results)
        accepted_ids = [
            checkout_id for _, checkout_id, _ in results if checkout_id is not None
        ]
        rejected_by_rate_limit = sum(
            status_code == 429 for _, _, status_code in results
        )
        completed = 0
        failed = 0
        deadline = time.monotonic() + arguments.wait_seconds
        remaining_ids = set(accepted_ids)

        while remaining_ids and time.monotonic() < deadline:
            for checkout_id in tuple(remaining_ids):
                status_response = await client.get(
                    f"/api/checkouts/{checkout_id}/status"
                )
                status_response.raise_for_status()
                status = status_response.json()["status"]
                if status in TERMINAL_STATUSES:
                    completed += status == "completed"
                    failed += status == "failed"
                    remaining_ids.remove(checkout_id)
            if remaining_ids:
                await asyncio.sleep(0.1)

        if remaining_ids:
            raise TimeoutError(
                f"{len(remaining_ids)} accepted checkouts did not reach a terminal state"
            )

        final_response = await client.get("/api/inventory")
        final_response.raise_for_status()
        final_inventory = {
            entry["sku"]: entry["available_quantity"]
            for entry in final_response.json()["items"]
        }
        remaining_stock = final_inventory[arguments.sku]
        consumed_stock = initial_stock - remaining_stock
        expected_consumption = completed * arguments.quantity

        print(f"Submitted: {arguments.requests}")
        print(f"Accepted: {len(accepted_ids)}")
        print(f"Rate-limited: {rejected_by_rate_limit}")
        print(f"Completed: {completed}")
        print(f"Failed: {failed}")
        print(f"Submission time: {submit_seconds:.3f}s")
        print(f"Throughput: {arguments.requests / submit_seconds:.2f} requests/s")
        print(f"Latency p50: {statistics.median(latencies) * 1000:.1f} ms")
        p95_index = max(0, math.ceil(len(latencies) * 0.95) - 1)
        print(f"Latency p95: {latencies[p95_index] * 1000:.1f} ms")
        print(
            f"Inventory conservation: {initial_stock} - {remaining_stock} "
            f"= {consumed_stock} (expected {expected_consumption})"
        )

        if remaining_stock < 0:
            raise AssertionError("Inventory became negative")
        if consumed_stock != expected_consumption:
            raise AssertionError(
                "Inventory delta does not match completed checkout quantities"
            )
        if arguments.exhaust:
            assert_exhaustion_results(
                arguments.requests,
                arguments.quantity,
                initial_stock,
                remaining_stock,
                completed,
                failed,
                rejected_by_rate_limit,
            )
            print(
                f"Exhaustion assertion: {completed} completed, {failed} failed, "
                f"stock {remaining_stock}"
            )

        import subprocess

        audit_script = Path(__file__).with_name("audit_ledger.py")
        sys.stdout.flush()
        subprocess.run(
            [
                sys.executable,
                str(audit_script),
                "--sku",
                arguments.sku,
                "--expected-ledger-delta",
                str(-consumed_stock),
            ],
            check=True,
            cwd=Path(__file__).resolve().parents[1],
        )


if __name__ == "__main__":
    asyncio.run(run_load_check(parse_arguments()))
