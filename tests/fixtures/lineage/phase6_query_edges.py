"""Phase 6 query fixtures；只使用公开 demo 资产名。"""

from __future__ import annotations

from shared.lineage.domain import LineageEdge


def demo_edge(
    source: str,
    target: str,
    *,
    environment: str = "DEV",
    source_profile: str = "fixture",
    program_name: str = "DEMO_PROGRAM_PHASE6",
    job_key: str | None = None,
) -> LineageEdge:
    """创建不含真实 metadata 的 materialized edge fixture。"""

    return LineageEdge(
        environment=environment,
        source_profile=source_profile,
        source_table=source,
        target_table=target,
        program_name=program_name,
        job_key=job_key,
        evidence_type="fixture",
        evidence={"fixture": "phase6_query"},
    )


SIMPLE_CHAIN_EDGES = (
    demo_edge("ODS.DEMO_A", "DWM.DEMO_B"),
    demo_edge("DWM.DEMO_B", "DWA.DEMO_C"),
    demo_edge("DWA.DEMO_C", "DM.DEMO_D"),
)

BRANCH_EDGES = (
    demo_edge("ODS.DEMO_A", "DWM.DEMO_B"),
    demo_edge("ODS.DEMO_A", "DWM.DEMO_C"),
    demo_edge("DWM.DEMO_B", "DWA.DEMO_D"),
    demo_edge("DWM.DEMO_C", "DWA.DEMO_D"),
)

CYCLE_EDGES = (
    demo_edge("DWM.DEMO_A", "DWM.DEMO_B"),
    demo_edge("DWM.DEMO_B", "DWM.DEMO_C"),
    demo_edge("DWM.DEMO_C", "DWM.DEMO_A"),
)

SELF_REFERENCE_EDGES = (demo_edge("DWM.DEMO_SELF", "DWM.DEMO_SELF"),)

MULTIPLE_PROGRAM_SAME_EDGE = (
    demo_edge(
        "ODS.DEMO_A",
        "DWM.DEMO_B",
        program_name="DEMO_PROGRAM_ONE",
        job_key="DEMO_JOB_ONE",
    ),
    demo_edge(
        "ODS.DEMO_A",
        "DWM.DEMO_B",
        program_name="DEMO_PROGRAM_TWO",
        job_key="DEMO_JOB_TWO",
    ),
)

ENVIRONMENT_EDGES = (
    demo_edge("ODS.DEMO_A", "DWM.DEMO_B", environment="DEV"),
    demo_edge("ODS.DEMO_A", "DWA.DEMO_C", environment="PROD"),
)

MULTIPLE_SOURCE_PROFILE_EDGES = (
    demo_edge(
        "ODS.DEMO_A",
        "DWM.DEMO_B",
        source_profile="mysql_dev_a",
        program_name="DEMO_PROGRAM_PROFILE_A",
    ),
    demo_edge(
        "DWM.DEMO_B",
        "DWA.DEMO_C",
        source_profile="mysql_dev_b",
        program_name="DEMO_PROGRAM_PROFILE_B",
    ),
)


def fanout_edges(count: int = 100) -> tuple[LineageEdge, ...]:
    """生成 deterministic fan-out，用于 max_nodes 安全限制测试。"""

    if count < 0:
        raise ValueError("count must be non-negative")
    return tuple(
        demo_edge(
            "ODS.DEMO_ROOT",
            f"DWM.DEMO_FANOUT_{index:03d}",
            program_name=f"DEMO_PROGRAM_FANOUT_{index:03d}",
        )
        for index in range(1, count + 1)
    )


__all__ = [
    "BRANCH_EDGES",
    "CYCLE_EDGES",
    "ENVIRONMENT_EDGES",
    "MULTIPLE_PROGRAM_SAME_EDGE",
    "MULTIPLE_SOURCE_PROFILE_EDGES",
    "SELF_REFERENCE_EDGES",
    "SIMPLE_CHAIN_EDGES",
    "demo_edge",
    "fanout_edges",
]
