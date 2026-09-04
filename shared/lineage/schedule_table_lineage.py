from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from shared.config.metadata import table as metadata_table
from shared.db.gaussdb import select_sql_with_profile
from shared.graph.dependency import parse_job_dependencies

UPSTREAM = "upstream"
DOWNSTREAM = "downstream"
DIRECTIONS = (UPSTREAM, DOWNSTREAM)


def _metadata_table(key: str, default_table: str) -> str:
    return metadata_table(key, default_table)


def _render_metadata_sql(template: str, replacements: dict[str, str]) -> str:
    rendered = template
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    return rendered


def build_job_sql() -> str:
    return _render_metadata_sql(
        """
select plan_name, job_name, dependency_text
from __JOBS_TABLE__
where job_name is not null
""",
        {"__JOBS_TABLE__": _metadata_table("jobs", "jobs")},
    )


def build_table_job_sql() -> str:
    return _render_metadata_sql(
        """
SELECT DISTINCT p.target_table AS table_name,
       j.job_name
FROM __JOBS_TABLE__ j
INNER JOIN __PROGRAMS_TABLE__ p
    ON j.program_name = p.program_name
WHERE p.target_table IS NOT NULL
""",
        {
            "__JOBS_TABLE__": _metadata_table("jobs", "jobs"),
            "__PROGRAMS_TABLE__": _metadata_table("programs", "programs"),
        },
    )


def normalize_name(value) -> str:
    return "" if value is None else str(value).strip().upper()


def normalize_table_name(value) -> str:
    return normalize_name(value).replace("`", "").replace('"', "")


def is_dwf_table(table_name: str) -> bool:
    normalized = normalize_table_name(table_name)
    return normalized.startswith(("DWF.", "DWS_DWF."))


@dataclass
class JobInfo:
    job_name: str
    plan_names: set[str] = field(default_factory=set)
    dependencies: list[str] = field(default_factory=list)
    table_names: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class TableEdge:
    """A displayed table relation whose direction is always data flow."""

    source_table: str
    target_table: str
    depth: int
    path: tuple[str, ...]

    @property
    def upstream_table(self) -> str:
        """Backward-compatible alias for callers of the former upstream-only API."""
        return self.source_table

    @property
    def current_table(self) -> str:
        """Backward-compatible alias for callers of the former upstream-only API."""
        return self.target_table


@dataclass
class TableTrace:
    root_table: str
    direction: str
    nodes: set[str]
    node_depths: dict[str, int]
    edges: list[TableEdge]
    dwf_tables: set[str]
    terminal_tables: set[str]
    cycles: list[list[str]]
    missing_jobs: set[str]
    unmapped_jobs: set[str]
    max_depth: int
    truncated: bool = False

    def to_graph_dict(self) -> dict:
        deepest_level = max(self.node_depths.values(), default=0)
        direct_sources: dict[str, set[str]] = {}
        direct_targets: dict[str, set[str]] = {}
        for edge in self.edges:
            direct_sources.setdefault(edge.target_table, set()).add(edge.source_table)
            direct_targets.setdefault(edge.source_table, set()).add(edge.target_table)

        nodes = []
        for table_name in sorted(self.nodes):
            if table_name == self.root_table:
                node_type = "root_table"
            elif self.direction == UPSTREAM and table_name in self.dwf_tables:
                node_type = "dwf_table"
            elif self.direction == DOWNSTREAM and table_name in self.terminal_tables:
                node_type = "terminal_table"
            else:
                node_type = "table"
            depth = self.node_depths.get(table_name, 0)
            nodes.append(
                {
                    "id": f"table:{table_name}",
                    "label": table_name,
                    "type": node_type,
                    "col": deepest_level - depth
                    if self.direction == UPSTREAM
                    else depth,
                    "detail": {
                        "表名": table_name,
                        "直接上游表": "、".join(
                            sorted(direct_sources.get(table_name, set()))
                        )
                        or "-",
                        "直接下游表": "、".join(
                            sorted(direct_targets.get(table_name, set()))
                        )
                        or "-",
                        (
                            "是否 DWF 截止表"
                            if self.direction == UPSTREAM
                            else "是否末端表"
                        ): (
                            "是"
                            if (
                                table_name in self.dwf_tables
                                if self.direction == UPSTREAM
                                else table_name in self.terminal_tables
                            )
                            else "否"
                        ),
                    },
                }
            )

        warnings = []
        if self.missing_jobs:
            relation_label = "前置" if self.direction == UPSTREAM else "关联"
            warnings.append(
                f"有 {len(self.missing_jobs)} 个 {relation_label}作业未出现在 demo_meta.jobs.job_name 中，对应分支无法继续。"
            )
        if self.unmapped_jobs:
            direction_label = "上游" if self.direction == UPSTREAM else "下游"
            warnings.append(
                f"有 {len(self.unmapped_jobs)} 个中间作业没有关联表名，"
                f"已跳过作业节点并继续查找更{direction_label}表。"
            )
        if self.cycles:
            warnings.append(f"检测到 {len(self.cycles)} 条作业依赖环，已停止对应分支。")
        if self.truncated:
            warnings.append(
                f"递归已达到最大深度 {self.max_depth}，部分分支未完全展开。"
            )
        return {
            "root_table": self.root_table,
            "direction": self.direction,
            "nodes": nodes,
            "edges": sorted(
                {
                    (f"table:{edge.source_table}", f"table:{edge.target_table}")
                    for edge in self.edges
                }
            ),
            "warnings": warnings,
            "max_depth": self.max_depth,
            "truncated": self.truncated,
        }


def build_job_index(
    job_rows: Iterable[tuple], table_job_rows: Iterable[tuple] = ()
) -> dict[str, JobInfo]:
    result: dict[str, JobInfo] = {}
    for plan_name, job_name, dependency_text in job_rows:
        normalized_job = normalize_name(job_name)
        if not normalized_job:
            continue
        info = result.setdefault(normalized_job, JobInfo(job_name=normalized_job))
        normalized_plan = normalize_name(plan_name)
        if normalized_plan:
            info.plan_names.add(normalized_plan)
        for dependency in parse_job_dependencies(dependency_text):
            normalized_dependency = normalize_name(dependency)
            if normalized_dependency and normalized_dependency not in info.dependencies:
                info.dependencies.append(normalized_dependency)

    for table_name, job_name in table_job_rows:
        normalized_job = normalize_name(job_name)
        normalized_table = normalize_table_name(table_name)
        if normalized_job in result and normalized_table and "." in normalized_table:
            result[normalized_job].table_names.add(normalized_table)
    return result


def load_job_index(profile: str = "demo") -> dict[str, JobInfo]:
    job_rows = select_sql_with_profile(profile, build_job_sql()) or []
    table_job_rows = select_sql_with_profile(profile, build_table_job_sql()) or []
    return build_job_index(job_rows, table_job_rows)


def build_table_job_map(job_index: dict[str, JobInfo]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for job_name, info in job_index.items():
        for table_name in info.table_names:
            result.setdefault(table_name, []).append(job_name)
    for table_name in result:
        result[table_name].sort()
    return result


def build_downstream_job_index(job_index: dict[str, JobInfo]) -> dict[str, list[str]]:
    result: dict[str, set[str]] = {}
    for downstream_job, info in job_index.items():
        for upstream_job in info.dependencies:
            result.setdefault(upstream_job, set()).add(downstream_job)
    return {job_name: sorted(dependents) for job_name, dependents in result.items()}


def find_table_candidates(
    input_name: str, table_job_map: dict[str, list[str]]
) -> list[str]:
    keyword = normalize_table_name(input_name)
    if not keyword:
        return []
    if keyword in table_job_map:
        return [keyword]
    return sorted(table_name for table_name in table_job_map if keyword in table_name)


def trace_table_lineage(
    root_table: str,
    job_index: dict[str, JobInfo],
    table_job_map: dict[str, list[str]] | None = None,
    max_depth: int = 30,
    direction: str = UPSTREAM,
) -> TableTrace:
    if direction not in DIRECTIONS:
        raise ValueError(f"不支持的检查方向: {direction}")

    normalized_root = normalize_table_name(root_table)
    table_job_map = table_job_map or build_table_job_map(job_index)
    root_jobs = table_job_map.get(normalized_root, [])
    if not root_jobs:
        raise KeyError(f"未找到表对应的作业: {normalized_root or root_table}")

    try:
        max_depth = max(1, int(max_depth))
    except (TypeError, ValueError) as exc:
        raise ValueError("max_depth 必须是整数") from exc
    downstream_index = (
        build_downstream_job_index(job_index) if direction == DOWNSTREAM else {}
    )
    nodes = {normalized_root}
    node_depths = {normalized_root: 0}
    edges: list[TableEdge] = []
    dwf_tables: set[str] = (
        {normalized_root}
        if direction == UPSTREAM and is_dwf_table(normalized_root)
        else set()
    )
    natural_terminal_tables: set[str] = set()
    cycles: list[list[str]] = []
    missing_jobs: set[str] = set()
    unmapped_jobs: set[str] = set()
    seen_edges: set[tuple[str, str]] = set()
    expanded_depth: dict[tuple[str, str], int] = {}
    truncated = False

    def walk(
        current_job: str,
        current_table: str,
        job_path: list[str],
        table_path: list[str],
        job_depth: int,
        table_depth: int,
    ):
        nonlocal truncated
        if direction == UPSTREAM and is_dwf_table(current_table):
            dwf_tables.add(current_table)
            return

        state_key = (current_job, current_table)
        previous_depth = expanded_depth.get(state_key)
        if previous_depth is not None and previous_depth <= job_depth:
            return
        expanded_depth[state_key] = job_depth

        if direction == UPSTREAM:
            next_jobs = job_index[current_job].dependencies
        else:
            next_jobs = downstream_index.get(current_job, [])

        if not next_jobs:
            if direction == DOWNSTREAM:
                natural_terminal_tables.add(current_table)
            return
        if job_depth >= max_depth:
            truncated = True
            return

        for next_job in next_jobs:
            if next_job in job_path:
                cycle_start = job_path.index(next_job)
                cycle = job_path[cycle_start:] + [next_job]
                if cycle not in cycles:
                    cycles.append(cycle)
                continue
            next_info = job_index.get(next_job)
            if next_info is None:
                missing_jobs.add(next_job)
                continue

            next_tables = sorted(next_info.table_names)
            if not next_tables:
                unmapped_jobs.add(next_job)
                walk(
                    next_job,
                    current_table,
                    job_path + [next_job],
                    table_path,
                    job_depth + 1,
                    table_depth,
                )
                continue

            for next_table in next_tables:
                next_table_depth = table_depth + 1
                nodes.add(next_table)
                node_depths[next_table] = min(
                    node_depths.get(next_table, next_table_depth),
                    next_table_depth,
                )
                source_table, target_table = (
                    (next_table, current_table)
                    if direction == UPSTREAM
                    else (current_table, next_table)
                )
                edge_key = (source_table, target_table)
                if source_table != target_table and edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append(
                        TableEdge(
                            source_table=source_table,
                            target_table=target_table,
                            depth=next_table_depth,
                            path=tuple(table_path + [next_table]),
                        )
                    )
                if direction == UPSTREAM and is_dwf_table(next_table):
                    dwf_tables.add(next_table)
                    continue
                walk(
                    next_job,
                    next_table,
                    job_path + [next_job],
                    table_path + [next_table],
                    job_depth + 1,
                    next_table_depth,
                )

    if not (direction == UPSTREAM and is_dwf_table(normalized_root)):
        for root_job in root_jobs:
            walk(root_job, normalized_root, [root_job], [normalized_root], 0, 0)

    tables_with_downstream = {edge.source_table for edge in edges}
    terminal_tables = (
        natural_terminal_tables - tables_with_downstream
        if direction == DOWNSTREAM
        else set()
    )
    return TableTrace(
        root_table=normalized_root,
        direction=direction,
        nodes=nodes,
        node_depths=node_depths,
        edges=edges,
        dwf_tables=dwf_tables,
        terminal_tables=terminal_tables,
        cycles=cycles,
        missing_jobs=missing_jobs,
        unmapped_jobs=unmapped_jobs,
        max_depth=max_depth,
        truncated=truncated,
    )


def trace_upstream_tables(
    root_table: str,
    job_index: dict[str, JobInfo],
    table_job_map: dict[str, list[str]] | None = None,
    max_depth: int = 30,
) -> TableTrace:
    return trace_table_lineage(
        root_table,
        job_index,
        table_job_map=table_job_map,
        max_depth=max_depth,
        direction=UPSTREAM,
    )


def trace_downstream_tables(
    root_table: str,
    job_index: dict[str, JobInfo],
    table_job_map: dict[str, list[str]] | None = None,
    max_depth: int = 30,
) -> TableTrace:
    return trace_table_lineage(
        root_table,
        job_index,
        table_job_map=table_job_map,
        max_depth=max_depth,
        direction=DOWNSTREAM,
    )
