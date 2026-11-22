"""Extract, validate, transform, and load order data into SQLite."""

from __future__ import annotations

import argparse
import re
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd

ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "sku",
    "quantity",
    "unit_price",
    "order_timestamp",
    "updated_at",
)


@dataclass(frozen=True)
class PipelineResult:
    rows_read: int
    rows_rejected: int
    duplicates_removed: int
    rows_loaded: int
    rejects_path: str

    @property
    def accounted_rows(self) -> int:
        return self.rows_rejected + self.duplicates_removed + self.rows_loaded


def _read_source(input_csv: Path) -> pd.DataFrame:
    frame = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    actual = set(frame.columns)
    expected = set(ORDER_COLUMNS)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing columns: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected columns: " + ", ".join(unexpected))
        raise ValueError("Source schema contract violation (" + "; ".join(details) + ")")
    return frame.loc[:, ORDER_COLUMNS].copy()


def _transform(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Return unique valid rows, rejected rows, and the duplicate count."""
    data = frame.copy()
    data.insert(0, "source_row", range(2, len(data) + 2))
    for column in ORDER_COLUMNS:
        data[column] = data[column].astype(str).str.strip()
    data["sku"] = data["sku"].str.upper()

    reasons = pd.Series("", index=data.index, dtype="object")

    def reject_where(mask: pd.Series, reason: str) -> None:
        eligible = reasons.eq("")
        reasons.loc[eligible & mask] = reason

    for column in ("order_id", "customer_id", "sku", "quantity", "unit_price", "order_timestamp", "updated_at"):
        reject_where(data[column].eq(""), f"missing_{column}")

    quantity = pd.to_numeric(data["quantity"], errors="coerce")
    reject_where(
        quantity.isna()
        | quantity.isin([float("inf"), float("-inf")])
        | (quantity <= 0)
        | (quantity % 1 != 0),
        "invalid_quantity",
    )

    price = pd.to_numeric(data["unit_price"], errors="coerce")
    reject_where(
        price.isna() | price.isin([float("inf"), float("-inf")]) | (price < 0),
        "invalid_unit_price",
    )

    order_time = pd.to_datetime(data["order_timestamp"], errors="coerce", utc=True)
    reject_where(order_time.isna(), "invalid_order_timestamp")

    update_time = pd.to_datetime(data["updated_at"], errors="coerce", utc=True)
    reject_where(update_time.isna(), "invalid_updated_at")

    rejected_mask = reasons.ne("")
    rejected = data.loc[rejected_mask, ["source_row", *ORDER_COLUMNS]].copy()
    rejected.insert(1, "reason", reasons.loc[rejected_mask])

    valid = data.loc[~rejected_mask].copy()
    valid["quantity"] = quantity.loc[~rejected_mask].astype(int)
    valid["unit_price"] = price.loc[~rejected_mask].astype(float)
    valid["order_timestamp"] = order_time.loc[~rejected_mask].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    valid["_updated_sort"] = update_time.loc[~rejected_mask]

    duplicate_count = len(valid) - valid["sku"].nunique()
    # Stable sorting makes the later source row win when timestamps are equal.
    valid = valid.sort_values(["_updated_sort", "source_row"], kind="mergesort")
    valid = valid.drop_duplicates(subset=["sku"], keep="last")
    valid["updated_at"] = valid["_updated_sort"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    valid = valid.loc[:, ORDER_COLUMNS]
    return valid, rejected, duplicate_count


def _validate_table_name(table_name: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError("table_name must be a plain SQLite identifier")


def _load(database: Path, table_name: str, rows: pd.DataFrame) -> None:
    _validate_table_name(table_name)
    with sqlite3.connect(database) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                sku TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price REAL NOT NULL CHECK (unit_price >= 0),
                order_timestamp TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        values = [tuple(row) for row in rows.loc[:, ORDER_COLUMNS].itertuples(index=False, name=None)]
        connection.executemany(
            f"""
            INSERT INTO "{table_name}"
                (order_id, customer_id, sku, quantity, unit_price, order_timestamp, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sku) DO UPDATE SET
                order_id = excluded.order_id,
                customer_id = excluded.customer_id,
                quantity = excluded.quantity,
                unit_price = excluded.unit_price,
                order_timestamp = excluded.order_timestamp,
                updated_at = excluded.updated_at
            WHERE excluded.updated_at >= "{table_name}".updated_at
            """,
            values,
        )


def run_pipeline(
    input_csv: str | Path,
    database: str | Path,
    rejects_csv: str | Path,
    table_name: str = "orders",
) -> PipelineResult:
    """Load a CSV, writing one reason per rejected row and upserting by SKU."""
    input_path = Path(input_csv)
    database_path = Path(database)
    rejects_path = Path(rejects_csv)
    source = _read_source(input_path)
    clean, rejected, duplicates_removed = _transform(source)

    result = PipelineResult(
        rows_read=len(source),
        rows_rejected=len(rejected),
        duplicates_removed=duplicates_removed,
        rows_loaded=len(clean),
        rejects_path=str(rejects_path),
    )
    if result.rows_read != result.accounted_rows:
        raise RuntimeError(
            f"Row accounting invariant failed: {result.rows_read} read, "
            f"{result.accounted_rows} accounted"
        )

    _load(database_path, table_name, clean)
    rejects_path.parent.mkdir(parents=True, exist_ok=True)
    rejected.to_csv(rejects_path, index=False)
    return result


def watch_pipeline(
    input_csv: str | Path,
    database: str | Path,
    rejects_csv: str | Path,
    table_name: str = "orders",
    poll_interval: float = 1.0,
) -> None:
    """Re-run the batch when an append-only source CSV changes."""
    if not 0 < poll_interval < float("inf"):
        raise ValueError("poll_interval must be a finite positive number")

    input_path = Path(input_csv)
    last_signature: tuple[int, int] | None = None
    while True:
        stat = input_path.stat()
        signature = (stat.st_mtime_ns, stat.st_size)
        if signature != last_signature:
            result = run_pipeline(input_path, database, rejects_csv, table_name)
            print(asdict(result), flush=True)
            last_signature = signature
        time.sleep(poll_interval)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--database", type=Path, default=Path("data/orders.sqlite"))
    parser.add_argument("--rejects", type=Path, default=Path("data/rejects.csv"))
    parser.add_argument("--table", default="orders")
    parser.add_argument(
        "--watch",
        action="store_true",
        help="poll an append-only CSV and rerun the batch when it changes",
    )
    parser.add_argument("--poll-interval", type=float, default=1.0, help="watch polling interval in seconds")
    args = parser.parse_args()
    if args.watch:
        watch_pipeline(args.input_csv, args.database, args.rejects, args.table, args.poll_interval)
        return
    result = run_pipeline(args.input_csv, args.database, args.rejects, args.table)
    print(asdict(result))


if __name__ == "__main__":
    main()
