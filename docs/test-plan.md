# ETL workflow test plan

## Contract

The source CSV must contain exactly these columns (column order may vary):
`order_id`, `customer_id`, `sku`, `quantity`, `unit_price`,
`order_timestamp`, and `updated_at`. A missing or added column fails the
contract before the target is modified.

## Expected data behavior

- Trim surrounding whitespace and normalize SKU values to uppercase.
- Reject blank required fields, non-positive/non-integral quantities,
  negative or non-numeric prices, and invalid timestamps.
- Write rejected records to a CSV with exactly one rejection reason per row.
- For repeated SKUs, retain the record with the latest `updated_at`; if times
  tie, retain the later source record.
- Upsert by SKU so running the same batch more than once does not grow the
  target or change the resulting records.
- Preserve the row-accounting invariant:
  `rows_read = rows_rejected + duplicates_removed + rows_loaded`.

## Generated fixture

`python -m src.generate_data --output data/orders.csv` creates 20,015 rows:
15 rejected records, 5 duplicate records removed, and 19,995 unique records
loaded. The duplicate updates are deliberately out of source order.

Run tests with `python -m pytest -q --junitxml=test-results\pytest-results.xml`.
The JUnit XML report is saved at `test-results\pytest-results.xml`.

## Business context and harness scope

Fynd's public overview describes unified commerce, including connecting orders
and inventory across stores and sales channels:
[Fynd company overview](https://www.fynd.com/about-us). That public description
is context, not a production integration contract. This repository is a
synthetic CSV-to-SQLite harness, not Fynd's real system. Its deliberate
corruption tests verify that the release gate rejects duplicate SKUs, blank
required values, and invalid quantity ranges. The current source contract does
not model channel IDs, store/location IDs, inventory movements, returns, or a
streaming event feed; no production compatibility is claimed. The optional
CSV watcher is only a local near-real-time demo: it reruns the full batch when
the file changes and has no defined latency SLA or durable event checkpoint.

The watcher integration test appends a valid record and verifies it becomes
queryable in SQLite. This proves the demo detects an append; it does not certify
Fynd compatibility or prove behavior under production load.

## Release gate

Run `python -m src.report --database data/orders.sqlite` after loading. The
command exits non-zero if the table is missing, empty, has missing required
columns, contains blank/null required values, has duplicate SKUs, or violates
numeric value constraints.
