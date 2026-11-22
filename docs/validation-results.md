# Validation results

Validated on 2026-11-22 with Python 3.11.

## Dataset

- Source records: 20,015
- Rejected records: 15
- Duplicate SKUs removed: 5
- Rows loaded into SQLite: 19,995
- Duplicate SKUs in target: 0
- Blank/null required values: 0
- Numeric value violations: 0

## Test report

- Test suite: 31 passed
- JUnit report: `test-results/pytest-results.xml`
- Release gate: passed

The test suite includes deliberate-corruption checks and a local CSV append
watcher check. It also demonstrates that the fixture accounting invariant holds.
