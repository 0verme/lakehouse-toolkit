# !/bin/python
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shared.lineage.mapping_sqlite import (  # noqa: E402
    MAPPING_DB_PATH,
    MAPPING_XLSX_PATH,
    recreate_mapping_sqlite,
)


def main():
    parser = argparse.ArgumentParser(
        description="Import mapping.xlsx into SQLite for lineage lookup."
    )
    parser.add_argument(
        "--xlsx", default=str(MAPPING_XLSX_PATH), help="Path to source mapping.xlsx"
    )
    parser.add_argument(
        "--db", default=str(MAPPING_DB_PATH), help="Path to target sqlite db"
    )
    args = parser.parse_args()

    result = recreate_mapping_sqlite(args.xlsx, args.db)
    print(f"导入完成: {result['edge_count']} 条血缘关系")
    print(f"Excel: {result['xlsx_path']}")
    print(f"SQLite: {result['db_path']}")


if __name__ == "__main__":
    main()
