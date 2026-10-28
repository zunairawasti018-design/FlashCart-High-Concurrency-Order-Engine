from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg


async def audit_ledger(pool: asyncpg.Pool) -> list[str]:
    errors: list[str] = []
    async with pool.acquire() as connection:
        inventory_rows = await connection.fetch(
            """
            SELECT inventory.sku,
                   inventory.initial_quantity,
                   inventory.available_quantity,
                   COALESCE(SUM(ledger.quantity_delta), 0) AS ledger_delta
            FROM flashcart_inventory AS inventory
            LEFT JOIN flashcart_inventory_ledger AS ledger
                ON ledger.sku = inventory.sku
            GROUP BY inventory.sku, inventory.initial_quantity,
                     inventory.available_quantity
            HAVING inventory.available_quantity < 0
                OR inventory.initial_quantity::NUMERIC
                   <> inventory.available_quantity::NUMERIC
                      + ABS(COALESCE(SUM(ledger.quantity_delta), 0))
            ORDER BY inventory.sku
            """
        )
        errors.extend(
            f"inventory mismatch for {row['sku']}: initial={row['initial_quantity']}, "
            f"remaining={row['available_quantity']}, "
            f"ledger_delta={row['ledger_delta']}"
            for row in inventory_rows
        )

        orphan_ledger_rows = await connection.fetch(
            """
            SELECT ledger.sku, COALESCE(SUM(ledger.quantity_delta), 0) AS ledger_delta
            FROM flashcart_inventory_ledger AS ledger
            LEFT JOIN flashcart_inventory AS inventory
                ON inventory.sku = ledger.sku
            WHERE inventory.sku IS NULL
            GROUP BY ledger.sku
            ORDER BY ledger.sku
            """
        )
        errors.extend(
            f"ledger contains entries for missing inventory SKU {row['sku']}: "
            f"ledger_delta={row['ledger_delta']}"
            for row in orphan_ledger_rows
        )

        order_rows = await connection.fetch(
            """
            WITH expected AS (
                SELECT checkout.id AS checkout_id,
                       item.sku,
                       item.quantity,
                       checkout.status AS checkout_status
                FROM flashcart_checkouts AS checkout
                JOIN flashcart_checkout_items AS item
                    ON item.checkout_id = checkout.id
            )
            SELECT COALESCE(expected.checkout_id, ledger.checkout_id) AS checkout_id,
                   COALESCE(expected.sku, ledger.sku) AS sku,
                   expected.quantity,
                   expected.checkout_status,
                   ledger.quantity_delta
            FROM expected
            FULL OUTER JOIN flashcart_inventory_ledger AS ledger
                ON ledger.checkout_id = expected.checkout_id
               AND ledger.sku = expected.sku
            WHERE (
                expected.checkout_status = 'completed'
                AND (
                    ledger.id IS NULL
                    OR ledger.quantity_delta <> -expected.quantity::BIGINT
                )
            ) OR (
                ledger.id IS NOT NULL
                AND COALESCE(expected.checkout_status, '') <> 'completed'
            )
            ORDER BY checkout_id, sku
            """
        )
        errors.extend(
            f"completed-order ledger mismatch for checkout {row['checkout_id']} "
            f"SKU {row['sku']}: quantity={row['quantity']}, "
            f"ledger_delta={row['quantity_delta']}, "
            f"status={row['checkout_status']}"
            for row in order_rows
        )
    return errors


async def inventory_ledger_summary(
    pool: asyncpg.Pool, sku: str
) -> dict[str, int] | None:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT inventory.initial_quantity,
                   inventory.available_quantity,
                   COALESCE(SUM(ledger.quantity_delta), 0) AS ledger_delta
            FROM flashcart_inventory AS inventory
            LEFT JOIN flashcart_inventory_ledger AS ledger
                ON ledger.sku = inventory.sku
            WHERE inventory.sku = $1
            GROUP BY inventory.sku, inventory.initial_quantity,
                     inventory.available_quantity
            """,
            sku,
        )
    if row is None:
        return None
    return {
        "initial_quantity": int(row["initial_quantity"]),
        "available_quantity": int(row["available_quantity"]),
        "ledger_delta": int(row["ledger_delta"]),
    }


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify FlashCart inventory and checkout ledger invariants."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("FLASHCART_DATABASE_URL"),
        help="PostgreSQL URL (defaults to FLASHCART_DATABASE_URL)",
    )
    parser.add_argument("--sku", help="Also print and validate one SKU's ledger total")
    parser.add_argument(
        "--expected-ledger-delta",
        type=int,
        help="Expected sum of ledger quantity deltas for --sku",
    )
    arguments = parser.parse_args()
    if not arguments.database_url:
        parser.error("set FLASHCART_DATABASE_URL or pass --database-url")
    if arguments.expected_ledger_delta is not None and not arguments.sku:
        parser.error("--expected-ledger-delta requires --sku")

    pool = await asyncpg.create_pool(arguments.database_url, min_size=1, max_size=2)
    try:
        errors = await audit_ledger(pool)
        summary = (
            await inventory_ledger_summary(pool, arguments.sku)
            if arguments.sku
            else None
        )
    finally:
        await pool.close()

    if arguments.sku:
        if summary is None:
            errors.append(f"inventory SKU {arguments.sku!r} does not exist")
        else:
            print(
                f"Ledger: {arguments.sku} initial={summary['initial_quantity']} "
                f"remaining={summary['available_quantity']} "
                f"ledger_delta={summary['ledger_delta']}"
            )
            if (
                arguments.expected_ledger_delta is not None
                and summary["ledger_delta"] != arguments.expected_ledger_delta
            ):
                errors.append(
                    f"ledger delta for {arguments.sku} was "
                    f"{summary['ledger_delta']}, expected "
                    f"{arguments.expected_ledger_delta}"
                )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("Ledger audit passed: inventory, balances, and completed orders reconcile.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
