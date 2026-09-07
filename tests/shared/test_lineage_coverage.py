from __future__ import annotations

import json
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from jobs.crontab import imp_lineage_edge
from shared.lineage.audit import audit_program_physical_dag
from shared.lineage.coverage import (
    CoverageReason,
    LineageCoverageAccumulator,
    write_json_report,
)
from shared.lineage.evolution import SnapshotScope
from shared.lineage.materialization import materialize_program
from shared.lineage.physical_dag import build_program_physical_dag
from tests.fixtures.lineage.phase8_coverage_profiles import COVERAGE_PROFILE_SOURCES


OBSERVED_AT = datetime(2026, 1, 5, 10, 11, 12, tzinfo=timezone.utc)


class LineageCoverageTests(unittest.TestCase):
    def test_multi_profile_funnel_counts_each_environment_profile(self):
        coverage = LineageCoverageAccumulator()
        coverage.observe_sources(COVERAGE_PROFILE_SOURCES)
        formal_edges = []
        for source in COVERAGE_PROFILE_SOURCES:
            dag = build_program_physical_dag(source)
            coverage.observe_dag(dag, count_program=False)
            audit = audit_program_physical_dag(
                dag,
                observed_at=OBSERVED_AT,
                batch_id="coverage-batch",
            )
            formal_edges.extend(
                materialize_program(
                    dag,
                    audit,
                    batch_id="coverage-batch",
                    observed_at=OBSERVED_AT,
                ).edges
            )
        coverage.observe_materialized_edges(formal_edges)

        report = coverage.report(generated_at="2026-01-05T10:11:12+00:00")
        self.assertEqual(report.coverage_scope, "ALL_PROGRAMS")
        self.assertEqual(
            [(item.environment, item.source_profile) for item in report.profiles],
            [
                ("ENV_A", "profile_a"),
                ("ENV_A", "profile_b"),
                ("ENV_B", "profile_a"),
                ("ENV_B", "profile_b"),
            ],
        )

        known = report.profiles[0]
        self.assertEqual(known.total_programs, 1)
        self.assertEqual(known.sql_candidate_count, 1)
        self.assertEqual(known.sql_step_count, 1)
        self.assertEqual(known.write_target_count, 1)
        self.assertEqual(known.physical_node_count, 2)
        self.assertEqual(known.physical_edge_count, 1)
        self.assertEqual(known.lineage_edge_count, 1)
        self.assertEqual(known.programs_with_lineage_edges, 1)
        self.assertEqual(
            known.failure_reasons,
            {
                reason.value: 0
                for reason in CoverageReason
                if reason
                not in {
                    CoverageReason.CANDIDATE_FOUND,
                    CoverageReason.RAW_SQL,
                    CoverageReason.NO_LINEAGE_EDGE,
                }
            },
        )

        dynamic = report.profiles[1]
        self.assertEqual(
            dynamic.failure_reasons[CoverageReason.SQL_ARGUMENT_DYNAMIC.value],
            1,
        )
        unknown = report.profiles[2]
        self.assertEqual(
            unknown.failure_reasons[CoverageReason.SQL_CALL_NOT_RECOGNIZED.value],
            1,
        )
        read_only = report.profiles[3]
        self.assertEqual(read_only.sql_candidate_count, 1)
        self.assertEqual(read_only.sql_step_count, 1)
        self.assertEqual(
            read_only.failure_reasons[CoverageReason.READ_ONLY_SQL.value],
            1,
        )

    def test_incremental_scope_does_not_double_count_sources_and_dags(self):
        sources = COVERAGE_PROFILE_SOURCES[:2]
        stable_sources = tuple(
            replace(source, source_hash=f"stable-{index}")
            for index, source in enumerate(sources)
        )
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            imp_lineage_edge.materialize_sources(
                stable_sources,
                db_path=db_path,
                batch_id="coverage-initial",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
                snapshot_scopes=[
                    SnapshotScope("ENV_A", "profile_a"),
                    SnapshotScope("ENV_A", "profile_b"),
                ],
            )
            coverage = LineageCoverageAccumulator()
            imp_lineage_edge.materialize_sources(
                stable_sources,
                db_path=db_path,
                batch_id="coverage-unchanged",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
                snapshot_scopes=[
                    SnapshotScope("ENV_A", "profile_a"),
                    SnapshotScope("ENV_A", "profile_b"),
                ],
                coverage=coverage,
            )

        report = coverage.report(generated_at="2026-01-05T10:11:12+00:00")
        self.assertEqual(report.coverage_scope, "REBUILT_PROGRAMS")
        self.assertEqual(sum(profile.total_programs for profile in report.profiles), 2)
        self.assertEqual(sum(profile.parsed_programs for profile in report.profiles), 0)
        self.assertEqual(
            sum(profile.lineage_edge_count for profile in report.profiles),
            1,
        )

    def test_job_writes_the_sanitized_coverage_report(self):
        class FixtureProvider:
            def iter_program_sources(self):
                yield COVERAGE_PROFILE_SOURCES[0]

        output_path = Path("artifacts/lineage_coverage/_test_job_report.json")
        try:
            with TemporaryDirectory() as directory:
                result = imp_lineage_edge.main(
                    [FixtureProvider()],
                    db_path=Path(directory) / "lineage.db",
                    batch_id="batch-coverage-report",
                    observed_at=OBSERVED_AT,
                    coverage_report_path=output_path,
                )
            self.assertEqual(result, 0)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["profiles"][0]["program_count"], 1)
            self.assertNotIn("DEMO_KNOWN_SQL", json.dumps(payload))
        finally:
            if output_path.exists():
                output_path.unlink()

    def test_report_is_aggregate_only_and_writes_under_artifact_root(self):
        coverage = LineageCoverageAccumulator()
        coverage.observe_sources(COVERAGE_PROFILE_SOURCES[:1])
        coverage.observe_dag(
            build_program_physical_dag(COVERAGE_PROFILE_SOURCES[0]),
            count_program=False,
        )
        report = coverage.report(generated_at="2026-01-05T10:11:12+00:00")
        serialized = json.dumps(report.to_dict(), ensure_ascii=False)

        self.assertNotIn("INSERT INTO", serialized)
        self.assertNotIn("ODS.DEMO_A", serialized)
        self.assertNotIn("DEMO_KNOWN_SQL", serialized)
        self.assertNotIn("password", serialized.lower())

        output_path = Path("artifacts/lineage_coverage/_test_coverage_report.json")
        try:
            written = write_json_report(report, output_path)
            self.assertEqual(written, (Path.cwd() / output_path).resolve())
            payload = json.loads(written.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema_version"], 1)
            self.assertEqual(payload["profiles"][0]["program_count"], 1)
        finally:
            if output_path.exists():
                output_path.unlink()

        with self.assertRaises(ValueError):
            write_json_report(report, Path("artifacts/other/report.json"))


if __name__ == "__main__":
    unittest.main()
