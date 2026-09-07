"""Synthetic multi-profile inputs for the lineage coverage funnel tests."""

from __future__ import annotations

from shared.lineage.domain import ProgramSource


COVERAGE_PROFILE_SOURCES = (
    ProgramSource(
        environment="ENV_A",
        source_profile="profile_a",
        program_name="DEMO_KNOWN_SQL",
        script_code='execute("INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A")',
        expected_target="DWA.DEMO_RESULT",
    ),
    ProgramSource(
        environment="ENV_A",
        source_profile="profile_b",
        program_name="DEMO_DYNAMIC_SQL",
        script_code=(
            "def run(target):\n"
            "    sql = f\"INSERT INTO {target} SELECT * FROM ODS.DEMO_A\"\n"
            "    execute(sql)\n"
        ),
        expected_target="DWA.DEMO_RESULT",
    ),
    ProgramSource(
        environment="ENV_B",
        source_profile="profile_a",
        program_name="DEMO_UNKNOWN_WRAPPER",
        script_code=(
            'execute_with_retry('
            '"INSERT INTO DWA.DEMO_RESULT SELECT * FROM ODS.DEMO_A"'
            ")"
        ),
        expected_target="DWA.DEMO_RESULT",
    ),
    ProgramSource(
        environment="ENV_B",
        source_profile="profile_b",
        program_name="DEMO_READ_ONLY_SQL",
        script_code='execute("SELECT * FROM ODS.DEMO_A")',
        expected_target="DWA.DEMO_RESULT",
    ),
)
