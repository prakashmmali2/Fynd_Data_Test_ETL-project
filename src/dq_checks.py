"""Quality checks for a loaded SQLite table."""

from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

REQUIRED_COLUMNS = {
    "sku",
    "order_id",
    "customer_id",
    "quantity",
    "unit_price",
    "order_timestamp",
    "updated_at",
}


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: str


def check_table(database: str | Path, table_name: str = "orders") -> list[CheckResult]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError("table_name must be a plain SQLite identifier")
    with sqlite3.connect(database) as connection:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)
        ).fetchone()
        if not exists:
            return [CheckResult("table_exists", False, f"Table {table_name!r} does not exist")]

        columns = {
            row[1] for row in connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        }
        schema_ok = REQUIRED_COLUMNS.issubset(columns)
        if not schema_ok:
            missing = sorted(REQUIRED_COLUMNS - columns)
            return [
                CheckResult("schema", False, "Missing columns: " + ", ".join(missing)),
            ]

        row_count = connection.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        null_count = connection.execute(
            f"""
            SELECT COUNT(*) FROM "{table_name}"
            WHERE sku IS NULL OR TRIM(sku) = ''
               OR order_id IS NULL OR TRIM(order_id) = ''
               OR customer_id IS NULL OR TRIM(customer_id) = ''
               OR quantity IS NULL OR unit_price IS NULL
               OR order_timestamp IS NULL OR updated_at IS NULL
            """
        ).fetchone()[0]
        duplicate_count = connection.execute(
            f'SELECT COUNT(*) FROM (SELECT sku FROM "{table_name}" GROUP BY sku HAVING COUNT(*) > 1)'
        ).fetchone()[0]
        invalid_count = connection.execute(
            f'SELECT COUNT(*) FROM "{table_name}" WHERE quantity <= 0 OR unit_price < 0'
        ).fetchone()[0]

    return [
        CheckResult("schema", schema_ok, "Required target columns are present"),
        CheckResult("row_count", row_count > 0, f"{row_count} rows loaded"),
        CheckResult("null_handling", null_count == 0, f"{null_count} rows contain required null/blank values"),
        CheckResult("duplicates", duplicate_count == 0, f"{duplicate_count} duplicate SKUs"),
        CheckResult("value_ranges", invalid_count == 0, f"{invalid_count} rows violate numeric constraints"),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/orders.sqlite"))
    parser.add_argument("--table", default="orders")
    args = parser.parse_args()
    checks = check_table(args.database, args.table)
    for check in checks:
        print(f"{'PASS' if check.passed else 'FAIL'} {check.name}: {check.details}")
    if not all(check.passed for check in checks):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
