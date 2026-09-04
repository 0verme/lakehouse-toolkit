from collections.abc import Iterable, Mapping

from shared.db.gaussdb import select_sql_with_profile

DEFAULT_PROFILE = "demo"
PLAN_SQL = "select plan_name, table_name from demo_meta.result_receipts"

# 公开 demo 只展示通用数据资产分类，不包含任何真实业务系统映射。
PLAN_LABELS = {
    "DEMO_PLAN_INGEST_DAY": "demo_ingest",
    "DEMO_PLAN_EXPORT_DAY": "demo_export",
}


class AssetMappingLoadError(RuntimeError):
    """元数据映射查询失败。"""


def build_asset_plan_map(rows: Iterable[tuple[object, object]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        if not row or len(row) < 2:
            continue
        plan_name = str(row[0] or "").strip().upper()
        table_name = str(row[1] or "").strip().upper()
        if plan_name and table_name:
            result[table_name] = plan_name
    return result


def load_asset_plan_map(profile: str = DEFAULT_PROFILE) -> dict[str, str]:
    rows = select_sql_with_profile(profile, PLAN_SQL)
    if rows is None:
        raise AssetMappingLoadError("公开 demo 资产映射加载失败")
    return build_asset_plan_map(rows)


def classify_asset_tables(
    table_names: Iterable[str],
    plan_by_table: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    plan_by_table = plan_by_table or {}
    labels = set()
    for table_name in table_names:
        normalized = str(table_name or "").strip().upper()
        plan_name = plan_by_table.get(normalized, "")
        label = PLAN_LABELS.get(plan_name)
        if label:
            labels.add(label)
    return tuple(sorted(labels))
