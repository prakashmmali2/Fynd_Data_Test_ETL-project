import csv
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import pytest

from src.dq_checks import check_table
from src.etl_pipeline import ORDER_COLUMNS, run_pipeline
from src.generate_data import generate_data


def write_rows(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows, columns=ORDER_COLUMNS).to_csv(path, index=False)


def valid_row(sku: str = "SKU-1", **changes) -> dict:
    row = {
        "order_id": "ORD-1",
        "customer_id": "CUST-1",
        "sku": sku,
        "quantity": 2,
        "unit_price": 12.5,
        "order_timestamp": "2026-01-01T10:00:00Z",
        "updated_at": "2026-01-01T10:00:00Z",
    }
    row.update(changes)
    return row


def run(tmp_path: Path, rows: list[dict], **kwargs):
    source = tmp_path / "source.csv"
    write_rows(source, rows)
    return run_pipeline(
        source,
        tmp_path / "target.sqlite",
        tmp_path / "rejects.csv",
        **kwargs,
    )


def test_generated_fixture_has_expected_input_count(tmp_path):
    frame = generate_data(tmp_path / "orders.csv")
    assert len(frame) == 20015


def test_generated_fixture_schema_matches_contract(tmp_path):
    frame = generate_data(tmp_path / "orders.csv")
    assert tuple(frame.columns) == ORDER_COLUMNS


def test_generated_pipeline_has_expected_accounting(tmp_path):
    source = tmp_path / "orders.csv"
    generate_data(source)
    database = tmp_path / "target.sqlite"
    result = run_pipeline(source, database, tmp_path / "rejects.csv")
    assert (result.rows_read, result.rows_rejected, result.duplicates_removed, result.rows_loaded) == (
        20015,
        15,
        5,
        19995,
    )
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 19995


def test_accounting_invariant_is_reported(tmp_path):
    result = run(tmp_path, [valid_row()])
    assert result.rows_read == result.accounted_rows == 1


def test_sku_is_trimmed_and_uppercased(tmp_path):
    run(tmp_path, [valid_row(" sku-a ")])
    with sqlite3.connect(tmp_path / "target.sqlite") as db:
        assert db.execute("SELECT sku FROM orders").fetchone()[0] == "SKU-A"


def test_string_fields_are_trimmed(tmp_path):
    run(tmp_path, [valid_row("SKU-A", order_id=" ORD-9 ", customer_id=" C-9 ")])
    with sqlite3.connect(tmp_path / "target.sqlite") as db:
        assert db.execute("SELECT order_id, customer_id FROM orders").fetchone() == ("ORD-9", "C-9")


def test_latest_update_wins_even_when_source_order_is_oldest_last(tmp_path):
    older = valid_row("SKU-A", order_id="OLD", updated_at="2026-01-01T00:00:00Z")
    newer = valid_row("SKU-A", order_id="NEW", updated_at="2026-01-03T00:00:00Z")
    result = run(tmp_path, [newer, older])
    with sqlite3.connect(tmp_path / "target.sqlite") as db:
        assert db.execute("SELECT order_id FROM orders").fetchone()[0] == "NEW"
    assert result.duplicates_removed == 1


def test_later_source_row_breaks_equal_timestamp_tie(tmp_path):
    result = run(tmp_path, [valid_row("SKU-A", order_id="FIRST"), valid_row("SKU-A", order_id="LAST")])
    with sqlite3.connect(tmp_path / "target.sqlite") as db:
        assert db.execute("SELECT order_id FROM orders").fetchone()[0] == "LAST"
    assert result.duplicates_removed == 1


def test_rejected_rows_have_one_reason_each(tmp_path):
    run(tmp_path, [valid_row(quantity="bad"), valid_row("SKU-B", unit_price=-1)])
    with (tmp_path / "rejects.csv").open(newline="") as stream:
        rejected = list(csv.DictReader(stream))
    assert len(rejected) == 2
    assert all(row["reason"] for row in rejected)


def test_rejection_reason_uses_validation_priority(tmp_path):
    run(tmp_path, [valid_row(order_id="", quantity="bad")])
    with (tmp_path / "rejects.csv").open(newline="") as stream:
        rejected = list(csv.DictReader(stream))
    assert rejected[0]["reason"] == "missing_order_id"


def test_blank_required_value_is_rejected(tmp_path):
    result = run(tmp_path, [valid_row(customer_id=" ")])
    assert result.rows_rejected == 1
    assert result.rows_loaded == 0


def test_non_integral_quantity_is_rejected(tmp_path):
    result = run(tmp_path, [valid_row(quantity="1.5")])
    assert result.rows_rejected == 1


def test_non_numeric_quantity_is_rejected(tmp_path):
    result = run(tmp_path, [valid_row(quantity="many")])
    assert result.rows_rejected == 1


def test_negative_price_is_rejected(tmp_path):
    result = run(tmp_path, [valid_row(unit_price="-0.01")])
    assert result.rows_rejected == 1


def test_infinite_price_is_rejected(tmp_path):
    result = run(tmp_path, [valid_row(unit_price="inf")])
    assert result.rows_rejected == 1


def test_invalid_timestamp_is_rejected(tmp_path):
    result = run(tmp_path, [valid_row(order_timestamp="not-a-time")])
    assert result.rows_rejected == 1


def test_rejects_csv_is_written_with_header_for_no_rejects(tmp_path):
    run(tmp_path, [valid_row()])
    with (tmp_path / "rejects.csv").open(newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames[:2] == ["source_row", "reason"]
        assert list(reader) == []


def test_same_batch_rerun_is_idempotent(tmp_path):
    source = tmp_path / "source.csv"
    write_rows(source, [valid_row("SKU-A"), valid_row("SKU-B")])
    database = tmp_path / "target.sqlite"
    rejects = tmp_path / "rejects.csv"
    run_pipeline(source, database, rejects)
    run_pipeline(source, database, rejects)
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 2


def test_upsert_updates_existing_sku(tmp_path):
    source = tmp_path / "source.csv"
    database = tmp_path / "target.sqlite"
    rejects = tmp_path / "rejects.csv"
    write_rows(source, [valid_row("SKU-A", order_id="OLD")])
    run_pipeline(source, database, rejects)
    write_rows(source, [valid_row("SKU-A", order_id="NEW", updated_at="2026-02-01T00:00:00Z")])
    run_pipeline(source, database, rejects)
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT order_id FROM orders").fetchone()[0] == "NEW"


def test_late_older_batch_does_not_regress_existing_row(tmp_path):
    source = tmp_path / "source.csv"
    database = tmp_path / "target.sqlite"
    rejects = tmp_path / "rejects.csv"
    write_rows(source, [valid_row("SKU-A", order_id="NEW", updated_at="2026-02-01T00:00:00Z")])
    run_pipeline(source, database, rejects)
    write_rows(source, [valid_row("SKU-A", order_id="OLD", updated_at="2026-01-01T00:00:00Z")])
    run_pipeline(source, database, rejects)
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT order_id FROM orders").fetchone()[0] == "NEW"


def test_target_schema_has_sku_primary_key(tmp_path):
    run(tmp_path, [valid_row()])
    with sqlite3.connect(tmp_path / "target.sqlite") as db:
        primary_keys = [row[1] for row in db.execute("PRAGMA table_info(orders)") if row[5]]
    assert primary_keys == ["sku"]


def test_source_contract_rejects_missing_column(tmp_path):
    source = tmp_path / "source.csv"
    frame = pd.DataFrame([valid_row()]).drop(columns=["updated_at"])
    frame.to_csv(source, index=False)
    with pytest.raises(ValueError, match="missing columns: updated_at"):
        run_pipeline(source, tmp_path / "target.sqlite", tmp_path / "rejects.csv")


def test_source_contract_rejects_unexpected_column(tmp_path):
    source = tmp_path / "source.csv"
    frame = pd.DataFrame([valid_row()])
    frame["new_source_field"] = "value"
    frame.to_csv(source, index=False)
    with pytest.raises(ValueError, match="unexpected columns: new_source_field"):
        run_pipeline(source, tmp_path / "target.sqlite", tmp_path / "rejects.csv")


def test_column_order_does_not_break_contract(tmp_path):
    row = valid_row()
    source = tmp_path / "source.csv"
    pd.DataFrame([row], columns=list(reversed(ORDER_COLUMNS))).to_csv(source, index=False)
    result = run_pipeline(source, tmp_path / "target.sqlite", tmp_path / "rejects.csv")
    assert result.rows_loaded == 1


def test_quality_checks_pass_for_pipeline_output(tmp_path):
    run(tmp_path, [valid_row()])
    checks = check_table(tmp_path / "target.sqlite")
    assert all(check.passed for check in checks)


@pytest.mark.parametrize(
    ("rows", "expected_failure"),
    [
        ([valid_row("SKU-A"), valid_row("SKU-A")], "duplicates"),
        ([valid_row(customer_id=" ")], "null_handling"),
        ([valid_row(quantity=0)], "value_ranges"),
    ],
)
def test_quality_gate_catches_deliberately_corrupted_targets(tmp_path, rows, expected_failure):
    database = tmp_path / "corrupted.sqlite"
    with sqlite3.connect(database) as db:
        db.execute(
            """
            CREATE TABLE orders (
                sku TEXT,
                order_id TEXT,
                customer_id TEXT,
                quantity INTEGER,
                unit_price REAL,
                order_timestamp TEXT,
                updated_at TEXT
            )
            """
        )
        db.executemany(
            """
            INSERT INTO orders
                (sku, order_id, customer_id, quantity, unit_price, order_timestamp, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                tuple(row[column] for column in ORDER_COLUMNS)
                for row in rows
            ],
        )

    checks = {check.name: check for check in check_table(database)}
    assert not checks[expected_failure].passed


def test_table_name_is_validated(tmp_path):
    with pytest.raises(ValueError, match="plain SQLite identifier"):
        run(tmp_path, [valid_row()], table_name="orders; DROP TABLE orders")


def test_release_gate_cli_returns_nonzero_for_missing_table(tmp_path):
    completed = subprocess.run(
        [sys.executable, "-m", "src.report", "--database", str(tmp_path / "missing.sqlite")],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 1
    assert "FAIL table_exists" in completed.stdout


def test_watch_mode_loads_appended_csv_row(tmp_path):
    source = tmp_path / "source.csv"
    database = tmp_path / "target.sqlite"
    rejects = tmp_path / "rejects.csv"
    write_rows(source, [valid_row("SKU-A")])

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "src.etl_pipeline",
            str(source),
            "--database",
            str(database),
            "--rejects",
            str(rejects),
            "--watch",
            "--poll-interval",
            "0.1",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if database.exists():
                with sqlite3.connect(database) as db:
                    table_exists = db.execute(
                        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'orders'"
                    ).fetchone()
                    if table_exists and db.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1:
                        break
            time.sleep(0.05)
        else:
            pytest.fail("watcher did not load the initial source row")

        with source.open("a", newline="") as stream:
            appended = valid_row("SKU-B")
            csv.writer(stream).writerow([appended[column] for column in ORDER_COLUMNS])

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with sqlite3.connect(database) as db:
                row_count = db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            if row_count == 2:
                break
            time.sleep(0.05)
        assert row_count == 2, "watcher did not load the appended source row"
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)
