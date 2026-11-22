"""Command-line release gate for the loaded pipeline output."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.dq_checks import check_table


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/orders.sqlite"))
    parser.add_argument("--table", default="orders")
    args = parser.parse_args()
    checks = check_table(args.database, args.table)
    failed = [check for check in checks if not check.passed]
    for check in checks:
        print(f"{'PASS' if check.passed else 'FAIL'} {check.name}: {check.details}")
    if failed:
        raise SystemExit(1)
    print("Release gate passed")


if __name__ == "__main__":
    main()
