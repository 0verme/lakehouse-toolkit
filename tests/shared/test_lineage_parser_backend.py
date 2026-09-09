from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timezone

from shared.lineage.audit import audit_program_physical_dag
from shared.lineage.domain import ProgramSource
from shared.lineage.materialization import materialize_program
from shared.lineage.parser_backend import (
    DEFAULT_PARSER_BACKEND,
    LegacyParserBackend,
    SqlAnalysis,
    SqlParseConfidence,
    SqlParseStatus,
    analyze_sql,
)
from shared.lineage.physical_dag import build_program_physical_dag, extract_sql_steps


SCRIPT = '''
execute("""
WITH base AS (
    SELECT * FROM ODS.DEMO_A
)
INSERT INTO DWA.DEMO_RESULT
SELECT * FROM base
""")
'''
OBSERVED_AT = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)


class FakeBackend:
    backend = "fake"
    backend_version = "fake-v1"

    def __init__(self, result: SqlAnalysis):
        self._result = result

    def analyze(self, script_code: str) -> SqlAnalysis:
        del script_code
        return replace(
            self._result,
            backend=self.backend,
            backend_version=self.backend_version,
        )


def source(*, expected_target: str | None = "DWA.DEMO_RESULT") -> ProgramSource:
    return ProgramSource(
        environment="DEV",
        source_profile="fixture",
        program_name="DEMO_PARSER_BACKEND",
        script_code=SCRIPT,
        expected_target=expected_target,
    )


def empty_analysis(
    status: SqlParseStatus,
    reason: str,
) -> SqlAnalysis:
    return SqlAnalysis(
        steps=(),
        ctes=(),
        candidate_count=0,
        parse_status=status,
        extraction_reason=reason,
        evidence={"reason": reason},
        confidence=SqlParseConfidence.NONE,
        backend="fixture",
        backend_version="fixture-v0",
    )


class ParserBackendContractTests(unittest.TestCase):
    def test_legacy_backend_is_the_default_and_exposes_minimal_contract(self):
        self.assertIsInstance(DEFAULT_PARSER_BACKEND, LegacyParserBackend)

        result = analyze_sql(SCRIPT)
        self.assertEqual(result.backend, "legacy")
        self.assertEqual(result.backend_version, "legacy-parser-v1")
        self.assertEqual(result.parse_status, SqlParseStatus.SUCCESS)
        self.assertEqual(result.confidence, SqlParseConfidence.HIGH)
        self.assertEqual(result.candidate_count, 1)
        self.assertEqual(result.ctes, (("BASE",),))
        self.assertEqual(result.evidence["candidate_count"], 1)
        self.assertEqual(result.steps[0].statement_type, "insert")
        self.assertEqual(result.steps[0].target, "DWA.DEMO_RESULT")
        self.assertEqual(result.steps[0].sources, ("ODS.DEMO_A",))

        self.assertEqual(result, analyze_sql(SCRIPT))
        self.assertEqual(
            result.to_compare_dict(), analyze_sql(SCRIPT).to_compare_dict()
        )

    def test_fake_backend_is_replaceable_without_downstream_backend_knowledge(self):
        legacy_result = analyze_sql(SCRIPT)
        fake = FakeBackend(legacy_result)
        fake_result = analyze_sql(SCRIPT, backend=fake)

        self.assertEqual(fake_result.backend, "fake")
        self.assertEqual(fake_result.backend_version, "fake-v1")
        self.assertEqual(fake_result.steps, legacy_result.steps)
        self.assertEqual(
            extract_sql_steps(SCRIPT, backend=fake),
            extract_sql_steps(SCRIPT),
        )
        self.assertEqual(
            fake_result.to_compare_dict()["steps"],
            legacy_result.to_compare_dict()["steps"],
        )

        legacy_dag = build_program_physical_dag(source())
        fake_dag = build_program_physical_dag(source(), backend=fake)
        self.assertEqual(fake_dag.steps, legacy_dag.steps)
        self.assertEqual(fake_dag.nodes, legacy_dag.nodes)
        self.assertEqual(fake_dag.edges, legacy_dag.edges)
        self.assertEqual(fake_dag.sinks, legacy_dag.sinks)
        self.assertEqual(fake_dag.sql_candidate_count, legacy_dag.sql_candidate_count)
        self.assertEqual(
            fake_dag.sql_extraction_reason,
            legacy_dag.sql_extraction_reason,
        )

        legacy_audit = audit_program_physical_dag(
            legacy_dag,
            observed_at=OBSERVED_AT,
            batch_id="batch-parser-backend",
        )
        fake_audit = audit_program_physical_dag(
            fake_dag,
            observed_at=OBSERVED_AT,
            batch_id="batch-parser-backend",
        )
        self.assertEqual(fake_audit, legacy_audit)
        self.assertEqual(fake_audit.issues, legacy_audit.issues)

        legacy_materialization = materialize_program(
            legacy_dag,
            legacy_audit,
            batch_id="batch-parser-backend",
            observed_at=OBSERVED_AT,
        )
        fake_materialization = materialize_program(
            fake_dag,
            fake_audit,
            batch_id="batch-parser-backend",
            observed_at=OBSERVED_AT,
        )
        self.assertEqual(fake_materialization.edges, legacy_materialization.edges)
        self.assertEqual(fake_materialization.issues, legacy_materialization.issues)

    def test_unresolved_and_failed_backend_results_are_explicit_and_isolated(self):
        for status, reason in (
            (SqlParseStatus.UNRESOLVED, "FAKE_DYNAMIC_SQL"),
            (SqlParseStatus.FAILED, "FAKE_PARSE_FAILED"),
        ):
            with self.subTest(status=status):
                fake = FakeBackend(empty_analysis(status, reason))
                result = analyze_sql(SCRIPT, backend=fake)
                self.assertEqual(result.parse_status, status)
                self.assertEqual(result.steps, ())
                self.assertEqual(result.backend, "fake")
                self.assertEqual(result.evidence, {"reason": reason})

                dag = build_program_physical_dag(
                    source(expected_target=None),
                    backend=fake,
                )
                self.assertEqual(dag.steps, ())
                self.assertEqual(dag.nodes, ())
                self.assertEqual(dag.edges, ())
                self.assertEqual(dag.sql_candidate_count, 0)
                self.assertEqual(dag.sql_extraction_reason, reason)

        # A failed experiment does not alter the production default or its output.
        production_dag = build_program_physical_dag(source())
        self.assertEqual(len(production_dag.edges), 1)
        self.assertEqual(analyze_sql(SCRIPT).backend, "legacy")


if __name__ == "__main__":
    unittest.main()
