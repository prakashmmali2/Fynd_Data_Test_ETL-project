# ETL and workflow test harness

A small, repeatable orders pipeline demonstrating source contracts, pandas
transformations, reject handling, SQLite upserts, data-quality gates, and CI.

## Quick start

```powershell
python -m pip install -r requirements.txt
python -m src.generate_data --output data/orders.csv
python -m src.etl_pipeline data/orders.csv --database data/orders.sqlite --rejects data/rejects.csv
python -m src.report --database data/orders.sqlite
python -m pytest -q --junitxml=test-results\pytest-results.xml
```

The generated fixture contains exactly 20,015 source rows. The expected pipeline
summary is 15 rejected, 5 duplicate rows removed, and 19,995 rows loaded. Each
run enforces `rows_read = rows_rejected + duplicates_removed + rows_loaded`.
The test command saves a machine-readable JUnit report to
`test-results\pytest-results.xml`.

For a local near-real-time append demo, start
`python -m src.etl_pipeline data/orders.csv --database data/orders.sqlite --rejects data/rejects.csv --watch`.
Append complete records to the CSV; the watcher checks for changes every
second by default (`--poll-interval` changes the interval). It reruns the full
batch, so this is not an incremental stream processor or a production latency
guarantee. Stop it with Ctrl+C.

## Project layout

- `src/generate_data.py` creates deterministic sample data and known defects.
- `src/etl_pipeline.py` checks the CSV contract, cleans and deduplicates rows,
  writes rejects, and upserts records into SQLite by SKU.
- `src/dq_checks.py` provides target table checks.
- `src/report.py` is the release gate (exit code 1 on failure).
- `tests/` covers the pipeline contract and edge cases.
- `.github/workflows/ci.yml` runs tests and the end-to-end gate on pushes and
  pull requests.

See [the test plan](docs/test-plan.md) and [defect-log template](docs/defect-log-template.md).
