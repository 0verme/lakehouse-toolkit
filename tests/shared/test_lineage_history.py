from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from jobs.crontab.imp_lineage_edge import materialize_sources
from shared.lineage import (
    LineageEdge,
    MaterializationBatch,
    SQLiteMaterializationStore,
)
from shared.lineage.domain import IssueType, LineageIssue
from shared.lineage.history import (  # pyright: ignore[reportMissingImports]
    BusinessLineageEdge,
    IssueLifecycleStatus,
    diff_environments,
    diff_lineage_batches,
    reconcile_issue_lifecycle,
)
from tests.fixtures.lineage.phase7_evolution import (  # pyright: ignore[reportMissingImports]
    BROKEN_BRANCH_PROGRAM,
    source,
)

OBSERVED_AT = datetime(2026, 4, 1, 9, 0, tzinfo=timezone.utc)


def edge(
    source_table: str,
    target_table: str,
    *,
    environment: str = "DEV",
    source_profile: str = "fixture",
    program_name: str = "PROGRAM_DEMO_HISTORY",
    source_hash: str | None = None,
) -> LineageEdge:
    return LineageEdge(
        environment=environment,
        source_profile=source_profile,
        source_table=source_table,
        target_table=target_table,
        program_name=program_name,
        source_hash=source_hash,
        evidence_type="fixture",
        evidence={"phase": 7},
    )


class IssueHistoryTests(unittest.TestCase):
    def test_issue_new_persists_and_resolves_without_mutating_old_batch(self):
        issue = source("PROGRAM_DEMO_ISSUE", BROKEN_BRANCH_PROGRAM)
        first = issue
        second = issue
        third = source("PROGRAM_DEMO_ISSUE")

        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            materialize_sources(
                [first],
                db_path=db_path,
                batch_id="batch-history-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            materialize_sources(
                [second],
                db_path=db_path,
                batch_id="batch-history-2",
                observed_at=OBSERVED_AT.replace(day=2),
                complete_snapshot=True,
            )
            materialize_sources(
                [third],
                db_path=db_path,
                batch_id="batch-history-3",
                observed_at=OBSERVED_AT.replace(day=3),
                complete_snapshot=True,
            )
            store = SQLiteMaterializationStore(db_path)
            lifecycle = store.reconcile_issue_lifecycle(
                "batch-history-1", "batch-history-2"
            )
            resolved = store.reconcile_issue_lifecycle(
                "batch-history-2", "batch-history-3"
            )
            old_issues = store.read_issues(batch_id="batch-history-1")
            self.assertEqual(old_issues, store.read_issues(batch_id="batch-history-1"))
            active_issues = store.read_issues(active_only=True)

        self.assertTrue(lifecycle.newly_detected == ())
        self.assertTrue(
            all(
                record.status is IssueLifecycleStatus.PERSISTING
                for record in lifecycle.persisting
            )
        )
        self.assertTrue(resolved.resolved)
        self.assertTrue(
            all(
                record.resolved_at == OBSERVED_AT.replace(day=3)
                for record in resolved.resolved
            )
        )
        self.assertTrue(old_issues)
        self.assertEqual(active_issues, ())

    def test_branch_broken_requires_previous_valid_target(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "lineage.db"
            valid = source("PROGRAM_DEMO_BRANCH")
            broken = source("PROGRAM_DEMO_BRANCH", BROKEN_BRANCH_PROGRAM)
            materialize_sources(
                [valid],
                db_path=db_path,
                batch_id="batch-branch-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            materialize_sources(
                [broken],
                db_path=db_path,
                batch_id="batch-branch-2",
                observed_at=OBSERVED_AT.replace(day=2),
                complete_snapshot=True,
            )
            store = SQLiteMaterializationStore(db_path)
            issue_types = {
                IssueType(issue.issue_type).value
                for issue in store.read_issues(active_only=True)
            }

        self.assertIn("ORPHAN_BRANCH", issue_types)
        self.assertIn("LINEAGE_BRANCH_BROKEN", issue_types)


class LineageDiffTests(unittest.TestCase):
    def test_batch_diff_uses_business_identity_not_provenance(self):
        previous = (
            edge("ODS.DEMO_A", "DWM.DEMO_B", program_name="PROGRAM_DEMO_OLD"),
            edge("DWM.DEMO_B", "DWA.DEMO_C"),
        )
        current = (
            edge(
                "ODS.DEMO_A",
                "DWM.DEMO_B",
                program_name="PROGRAM_DEMO_NEW",
                source_profile="other_fixture",
            ),
            edge("DWM.DEMO_B", "DWA.DEMO_D"),
        )

        result = diff_lineage_batches(previous, current)

        self.assertEqual(
            result.added_edges,
            (BusinessLineageEdge("DEV", "DWM.DEMO_B", "DWA.DEMO_D"),),
        )
        self.assertEqual(
            result.removed_edges,
            (BusinessLineageEdge("DEV", "DWM.DEMO_B", "DWA.DEMO_C"),),
        )
        self.assertEqual(
            result.unchanged_edges,
            (BusinessLineageEdge("DEV", "ODS.DEMO_A", "DWM.DEMO_B"),),
        )

    def test_environment_diff_collapses_multiple_dev_profiles_and_normalizes_assets(
        self,
    ):
        dev_edges = (
            edge(
                "DWA.DEMO_X",
                "DM.DEMO_COMMON",
                source_profile="mysql_dev_a",
            ),
            edge(
                "DWA.DEMO_X",
                "DM.DEMO_COMMON",
                source_profile="mysql_dev_b",
            ),
            edge(
                "DWA.DEMO_X",
                "DM.DEMO_DEV_ONLY",
                program_name="PROGRAM_DEMO_DEV",
            ),
        )
        prod_edges = (
            edge(
                "DWA.DEMO_X",
                "DM.DEMO_COMMON",
                environment="PROD",
                source_profile="production_metadata",
                program_name="PROGRAM_DEMO_PROD",
            ),
            edge(
                "DWA.DEMO_X",
                "DM.DEMO_PROD_ONLY",
                environment="PROD",
                source_profile="production_metadata",
                program_name="PROGRAM_DEMO_PROD_ONLY",
            ),
        )

        result = diff_environments(dev_edges, prod_edges)

        self.assertEqual(
            {(item.source_table, item.target_table) for item in result.unchanged_edges},
            {("DWS_DWA.DEMO_X", "DWS_DM.DEMO_COMMON")},
        )
        self.assertEqual(
            {(item.source_table, item.target_table) for item in result.only_in_dev},
            {("DWS_DWA.DEMO_X", "DWS_DM.DEMO_DEV_ONLY")},
        )
        self.assertEqual(
            {(item.source_table, item.target_table) for item in result.only_in_prod},
            {("DWS_DWA.DEMO_X", "DWS_DM.DEMO_PROD_ONLY")},
        )

    def test_store_history_api_reads_old_batch_and_compares_batches(self):
        first = edge("ODS.DEMO_A", "DWM.DEMO_B", program_name="PROGRAM_DEMO_A")
        second = edge("DWM.DEMO_B", "DWA.DEMO_C", program_name="PROGRAM_DEMO_B")
        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(
                MaterializationBatch(
                    batch_id="batch-diff-1",
                    observed_at=OBSERVED_AT,
                    edges=(first,),
                )
            )
            store.publish(
                MaterializationBatch(
                    batch_id="batch-diff-2",
                    observed_at=OBSERVED_AT.replace(day=2),
                    edges=(first, second),
                )
            )
            result = store.diff_lineage_batches("batch-diff-1", "batch-diff-2")
            batches = store.list_batch_metadata()
            old_edges = store.read_edges(batch_id="batch-diff-1")

        self.assertEqual(
            result.added_edges,
            (BusinessLineageEdge("DEV", "DWS_DWM.DEMO_B", "DWS_DWA.DEMO_C"),),
        )
        self.assertEqual(len(old_edges), 1)
        self.assertEqual(
            [item.batch_id for item in batches], ["batch-diff-1", "batch-diff-2"]
        )
        self.assertFalse(batches[0].is_active)
        self.assertTrue(batches[1].is_active)

    def test_history_and_diff_are_deterministic(self):
        first = (edge("ODS.DEMO_B", "DWM.DEMO_C"), edge("ODS.DEMO_A", "DWM.DEMO_B"))
        second = tuple(reversed(first))

        self.assertEqual(
            diff_lineage_batches(first, ()), diff_lineage_batches(second, ())
        )

    def test_pure_issue_reconciliation_reports_new_persisting_resolved(self):
        # Store-backed tests above exercise real audit issues; this test keeps the
        # lifecycle primitive explicit and independent from parser behavior.
        first = LineageIssue(
            environment="DEV",
            source_profile="fixture",
            program_name="PROGRAM_DEMO_LIFECYCLE",
            issue_type=IssueType.ORPHAN_BRANCH,
            severity="MEDIUM",
            message="demo orphan",
            branch_sink="TMP_DEMO_ORPHAN",
            stable_key="stable-demo-orphan",
            first_seen_at=OBSERVED_AT,
            last_seen_at=OBSERVED_AT,
        )
        persisting = reconcile_issue_lifecycle(
            [first],
            [first],
            observed_at=OBSERVED_AT.replace(day=2),
        )
        resolved = reconcile_issue_lifecycle(
            [first],
            [],
            observed_at=OBSERVED_AT.replace(day=3),
        )

        self.assertEqual(
            persisting.persisting[0].status, IssueLifecycleStatus.PERSISTING
        )
        self.assertEqual(persisting.current_issues[0].first_seen_at, OBSERVED_AT)
        self.assertEqual(resolved.resolved[0].status, IssueLifecycleStatus.RESOLVED)
        self.assertEqual(resolved.resolved[0].resolved_at, OBSERVED_AT.replace(day=3))


if __name__ == "__main__":
    unittest.main()
