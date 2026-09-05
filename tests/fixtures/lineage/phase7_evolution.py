"""Phase 7 public incremental/history fixtures."""

from __future__ import annotations

from shared.lineage.domain import ProgramSource, compute_source_hash

VALID_PROGRAM = """
execute("INSERT INTO TMP_VALID SELECT * FROM ODS.DEMO_A")
execute("INSERT INTO DWA.DEMO_TARGET SELECT * FROM TMP_VALID")
"""

BROKEN_BRANCH_PROGRAM = """
execute("INSERT INTO TMP_VALID SELECT * FROM ODS.DEMO_A")
execute("INSERT INTO DWA.DEMO_TARGET SELECT * FROM TMP_VALID")
execute("INSERT INTO TMP_BROKEN SELECT * FROM ODS.DEMO_X")
"""

RESOLVED_PROGRAM = VALID_PROGRAM

DEV_ONLY_PROGRAM = """
execute("INSERT INTO DWA.DEMO_DEV_ONLY SELECT * FROM ODS.DEMO_A")
"""

PROD_ONLY_PROGRAM = """
execute("INSERT INTO DWA.DEMO_PROD_ONLY SELECT * FROM ODS.DEMO_A")
"""


def source(
    program_name: str,
    script_code: str = VALID_PROGRAM,
    *,
    environment: str = "DEV",
    source_profile: str = "fixture",
    expected_target: str | None = "DWA.DEMO_TARGET",
    source_hash: str | None = None,
) -> ProgramSource:
    resolved_hash = (
        compute_source_hash(program_name, script_code, expected_target)
        if source_hash is None
        else source_hash
    )
    return ProgramSource(
        environment=environment,
        source_profile=source_profile,
        program_name=program_name,
        script_code=script_code,
        expected_target=expected_target,
        source_hash=resolved_hash,
    )


__all__ = [
    "BROKEN_BRANCH_PROGRAM",
    "DEV_ONLY_PROGRAM",
    "PROD_ONLY_PROGRAM",
    "RESOLVED_PROGRAM",
    "VALID_PROGRAM",
    "source",
]
