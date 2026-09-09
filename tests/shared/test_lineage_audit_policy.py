from __future__ import annotations

import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from jobs.crontab import imp_lineage_edge
from shared.lineage import (
    AUDIT_POLICY_VERSION,
    AUDIT_RULE_VERSION,
    AuditConfidence,
    AuditFact,
    AuditPolicy,
    AuditPolicyResult,
    IssueDisposition,
    IssueType,
    LineageIssue,
    MaterializationBatch,
    SQLiteMaterializationStore,
    audit_program_physical_dag,
    candidate_from_fact,
    detect_audit_facts,
    materialize_batch,
    replay_audit_policy,
)
from shared.lineage.audit import compute_lineage_issue_stable_key
from shared.lineage.evolution import reconcile_issue_lifecycle
from shared.lineage.materialization_sqlite import SCHEMA_SQL
from shared.lineage.domain import ProgramSource
from shared.lineage.physical_dag import ProgramPhysicalDAG, build_program_physical_dag
from tests.fixtures.lineage.phase4_audit_programs import (
    CYCLE_PROGRAM,
    ORPHAN_BRANCH_PROGRAM,
)
from tests.fixtures.lineage.phase7_evolution import BROKEN_BRANCH_PROGRAM, source

OBSERVED_AT = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)


def build_orphan_dag() -> ProgramPhysicalDAG:
    return build_program_physical_dag(
        ProgramSource(
            environment="DEV",
            source_profile="fixture",
            program_name="PROGRAM_POLICY",
            script_code=ORPHAN_BRANCH_PROGRAM,
            expected_target="DWA.DEMO_RESULT",
        )
    )


class AuditFactPolicyBoundaryTests(unittest.TestCase):
    def test_detector_returns_facts_without_risk_or_disposition(self):
        facts = detect_audit_facts(build_orphan_dag(), observed_at=OBSERVED_AT)

        self.assertTrue(facts)
        self.assertTrue(all(isinstance(fact, AuditFact) for fact in facts))
        self.assertTrue(all(not hasattr(fact, "severity") for fact in facts))
        self.assertTrue(all(not hasattr(fact, "disposition") for fact in facts))
        self.assertTrue(all(fact.rule_version == AUDIT_RULE_VERSION for fact in facts))
        self.assertTrue(all(fact.confidence is AuditConfidence.HIGH for fact in facts))

    def test_severity_and_disposition_policy_replay_keeps_fact_set_and_identity(self):
        dag = build_orphan_dag()
        default = audit_program_physical_dag(
            dag,
            observed_at=OBSERVED_AT,
            batch_id="batch-default",
        )
        custom_policy = AuditPolicy(
            severity_by_issue_type={
                IssueType.ORPHAN_BRANCH: "LOW",
                IssueType.MULTI_SINK_CANDIDATE: "LOW",
            },
            default_disposition=IssueDisposition.ACCEPTED,
            policy_version="audit-policy-test-v2",
        )
        custom = audit_program_physical_dag(
            dag,
            observed_at=OBSERVED_AT,
            batch_id="batch-custom",
            policy=custom_policy,
        )

        self.assertEqual(default.facts, custom.facts)
        self.assertEqual(
            {fact.stable_issue_identity for fact in default.facts},
            {fact.stable_issue_identity for fact in custom.facts},
        )
        self.assertEqual(
            {issue.stable_key for issue in default.issues},
            {issue.stable_key for issue in custom.issues},
        )
        self.assertEqual(
            {issue.severity for issue in custom.issues},
            {"LOW"},
        )
        self.assertTrue(
            all(issue.disposition is IssueDisposition.ACCEPTED for issue in custom.issues)
        )
        self.assertTrue(
            all(issue.policy_version == "audit-policy-test-v2" for issue in custom.issues)
        )

    def test_policy_result_is_a_projection_and_can_replay_without_dag(self):
        fact = next(
            fact
            for fact in detect_audit_facts(build_orphan_dag())
            if fact.issue_type is IssueType.ORPHAN_BRANCH
        )
        policy = AuditPolicy(
            severity_by_issue_type={IssueType.ORPHAN_BRANCH: "LOW"},
            policy_version="audit-policy-replay-v1",
        )
        result = policy.evaluate(fact)

        self.assertIsInstance(result, AuditPolicyResult)
        self.assertIs(result.fact, fact)
        self.assertEqual(result.stable_key, fact.stable_issue_identity)
        replayed = replay_audit_policy(
            (fact,), policy, batch_id="batch-replay", observed_at=OBSERVED_AT
        )
        self.assertEqual(replayed[0].stable_key, fact.stable_issue_identity)
        self.assertEqual(replayed[0].severity, "LOW")
        self.assertEqual(replayed[0].policy_version, "audit-policy-replay-v1")

    def test_message_and_evidence_changes_do_not_change_identity(self):
        common = {
            "environment": "DEV",
            "source_profile": "fixture",
            "program_name": "PROGRAM_POLICY",
            "issue_type": IssueType.ORPHAN_BRANCH,
            "branch_sink": "TMP_ORPHAN",
            "rule_version": AUDIT_RULE_VERSION,
        }
        first = AuditFact(
            **common,
            message="first explanation",
            evidence={"edge_count": 1},
        )
        second = AuditFact(
            **common,
            message="reworded explanation",
            evidence={"edge_count": 2, "new": True},
        )

        self.assertEqual(first.stable_issue_identity, second.stable_issue_identity)
        self.assertEqual(
            compute_lineage_issue_stable_key(
                "DEV",
                "fixture",
                "PROGRAM_POLICY",
                IssueType.ORPHAN_BRANCH,
                branch_sink="TMP_ORPHAN",
            ),
            first.stable_issue_identity,
        )

    def test_branch_node_and_cycle_semantics_change_identity(self):
        branch_a = compute_lineage_issue_stable_key(
            "DEV", "fixture", "PROGRAM_POLICY", IssueType.ORPHAN_BRANCH, branch_sink="TMP_A"
        )
        branch_b = compute_lineage_issue_stable_key(
            "DEV", "fixture", "PROGRAM_POLICY", IssueType.ORPHAN_BRANCH, branch_sink="TMP_B"
        )
        node_a = compute_lineage_issue_stable_key(
            "DEV", "fixture", "PROGRAM_POLICY", IssueType.SELF_REFERENCE, node_key="TMP_A"
        )
        node_b = compute_lineage_issue_stable_key(
            "DEV", "fixture", "PROGRAM_POLICY", IssueType.SELF_REFERENCE, node_key="TMP_B"
        )
        cycle_a = compute_lineage_issue_stable_key(
            "DEV",
            "fixture",
            "PROGRAM_POLICY",
            IssueType.CYCLE_DETECTED,
            cycle_nodes=("TMP_A", "TMP_B"),
        )
        cycle_b = compute_lineage_issue_stable_key(
            "DEV",
            "fixture",
            "PROGRAM_POLICY",
            IssueType.CYCLE_DETECTED,
            cycle_nodes=("TMP_A", "TMP_C"),
        )

        self.assertNotEqual(branch_a, branch_b)
        self.assertNotEqual(node_a, node_b)
        self.assertNotEqual(cycle_a, cycle_b)
        self.assertEqual(
            detect_audit_facts(
                build_program_physical_dag(
                    ProgramSource(
                        environment="DEV",
                        source_profile="fixture",
                        program_name="PROGRAM_CYCLE",
                        script_code=CYCLE_PROGRAM,
                    )
                )
            )[0].issue_type,
            IssueType.CYCLE_DETECTED,
        )

    def test_golden_candidate_ignores_policy_projection_fields(self):
        result = audit_program_physical_dag(
            build_orphan_dag(), observed_at=OBSERVED_AT, batch_id="batch-golden"
        )
        fact = next(
            fact for fact in result.facts if fact.issue_type is IssueType.ORPHAN_BRANCH
        )
        issue = next(
            issue for issue in result.issues if issue.issue_type is IssueType.ORPHAN_BRANCH
        )
        accepted = issue.with_disposition(
            IssueDisposition.ACCEPTED,
            updated_at=OBSERVED_AT,
            updated_by="reviewer",
        )

        self.assertEqual(candidate_from_fact(fact), candidate_from_fact(AuditFact.from_issue(accepted)))


class IssueDispositionHistoryTests(unittest.TestCase):
    def test_manual_disposition_survives_policy_change_and_missing_fact_resolves(self):
        issue = LineageIssue(
            environment="DEV",
            source_profile="fixture",
            program_name="PROGRAM_HISTORY",
            issue_type=IssueType.ORPHAN_BRANCH,
            severity="MEDIUM",
            message="initial",
            branch_sink="TMP_ORPHAN",
            evidence={"edge_count": 1},
            stable_key="stable-history",
            confidence=AuditConfidence.HIGH,
            rule_version=AUDIT_RULE_VERSION,
            policy_version=AUDIT_POLICY_VERSION,
        )
        accepted = issue.with_disposition(
            IssueDisposition.ACCEPTED,
            updated_at=OBSERVED_AT,
            updated_by="reviewer",
        )
        changed = replace(
            accepted,
            severity="LOW",
            message="updated",
            evidence={"edge_count": 2},
            policy_version="audit-policy-v2",
        )
        persisting = reconcile_issue_lifecycle(
            (accepted,), (changed,), observed_at=OBSERVED_AT.replace(day=2)
        )
        resolved = reconcile_issue_lifecycle(
            (accepted,), (), observed_at=OBSERVED_AT.replace(day=3)
        )
        false_positive = issue.with_disposition(
            IssueDisposition.FALSE_POSITIVE,
            updated_at=OBSERVED_AT,
            updated_by="reviewer",
        )
        false_positive_replay = reconcile_issue_lifecycle(
            (false_positive,), (changed,), observed_at=OBSERVED_AT.replace(day=4)
        )

        current = persisting.current_issues[0]
        self.assertEqual(current.stable_key, accepted.stable_key)
        self.assertEqual(current.severity, "LOW")
        self.assertEqual(current.disposition, IssueDisposition.ACCEPTED)
        self.assertEqual(current.disposition_updated_by, "reviewer")
        self.assertEqual(
            resolved.resolved[0].issue.disposition, IssueDisposition.RESOLVED
        )
        self.assertFalse(resolved.resolved[0].issue.is_active)
        self.assertEqual(
            false_positive_replay.current_issues[0].disposition,
            IssueDisposition.FALSE_POSITIVE,
        )


class SQLitePolicyCompatibilityTests(unittest.TestCase):
    def test_policy_fields_and_manual_disposition_are_historical_rows(self):
        result = audit_program_physical_dag(
            build_orphan_dag(), observed_at=OBSERVED_AT, batch_id="batch-policy-1"
        )
        batch = materialize_batch(
            [result], batch_id="batch-policy-1", observed_at=OBSERVED_AT
        )
        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(batch)
            issue = next(
                issue
                for issue in store.read_issues(active_only=True)
                if issue.issue_type is IssueType.ORPHAN_BRANCH
            )
            store.set_issue_disposition(
                issue.stable_key or "",
                IssueDisposition.ACCEPTED,
                batch_id="batch-policy-manual",
                observed_at=OBSERVED_AT.replace(day=2),
                updated_by="reviewer",
            )
            old_issue = next(
                issue
                for issue in store.read_issues(batch_id="batch-policy-1")
                if issue.issue_type is IssueType.ORPHAN_BRANCH
            )
            current_issue = next(
                issue
                for issue in store.read_issues(active_only=True)
                if issue.issue_type is IssueType.ORPHAN_BRANCH
            )
            policy = AuditPolicy(
                severity_by_issue_type={IssueType.ORPHAN_BRANCH: "LOW"},
                policy_version="audit-policy-replay-v2",
            )
            store.replay_issue_policy(
                policy,
                source_batch_id="batch-policy-manual",
                batch_id="batch-policy-replayed",
                observed_at=OBSERVED_AT.replace(day=3),
            )
            replayed = next(
                issue
                for issue in store.read_issues(active_only=True)
                if issue.issue_type is IssueType.ORPHAN_BRANCH
            )

        self.assertEqual(old_issue.disposition, IssueDisposition.OPEN)
        self.assertEqual(current_issue.disposition, IssueDisposition.ACCEPTED)
        self.assertEqual(current_issue.disposition_updated_by, "reviewer")
        self.assertEqual(replayed.severity, "LOW")
        self.assertEqual(replayed.policy_version, "audit-policy-replay-v2")
        self.assertEqual(replayed.disposition, IssueDisposition.ACCEPTED)
        self.assertEqual(replayed.stable_key, issue.stable_key)

    def test_missing_current_fact_is_persisted_as_resolved_history_projection(self):
        result = audit_program_physical_dag(
            build_orphan_dag(), observed_at=OBSERVED_AT, batch_id="batch-resolve-1"
        )
        initial = materialize_batch(
            [result], batch_id="batch-resolve-1", observed_at=OBSERVED_AT
        )
        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "resolved.db")
            store.publish(initial)
            store.publish(
                MaterializationBatch(
                    batch_id="batch-resolve-2",
                    observed_at=OBSERVED_AT.replace(day=2),
                )
            )
            active_issues = store.read_issues(active_only=True)
            resolved_issues = store.read_issues(batch_id="batch-resolve-2")

        self.assertEqual(active_issues, ())
        self.assertTrue(resolved_issues)
        self.assertTrue(
            all(issue.disposition is IssueDisposition.RESOLVED for issue in resolved_issues)
        )
        self.assertTrue(all(not issue.is_active for issue in resolved_issues))

    def test_legacy_issue_schema_gets_safe_defaults_without_rewriting_old_values(self):
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "legacy.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.executescript(
                    """
                    CREATE TABLE lineage_batch (
                        batch_id TEXT PRIMARY KEY,
                        observed_at TEXT NOT NULL,
                        published_at TEXT,
                        edge_count INTEGER NOT NULL,
                        issue_count INTEGER NOT NULL,
                        is_active INTEGER NOT NULL
                    );
                    CREATE TABLE lineage_issue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        environment TEXT NOT NULL,
                        source_profile TEXT NOT NULL,
                        program_name TEXT NOT NULL,
                        issue_type TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        stable_key TEXT,
                        node_key TEXT,
                        branch_sink TEXT,
                        message TEXT NOT NULL,
                        evidence TEXT NOT NULL,
                        batch_id TEXT NOT NULL,
                        first_seen_at TEXT NOT NULL,
                        last_seen_at TEXT NOT NULL,
                        is_active INTEGER NOT NULL
                    );
                    INSERT INTO lineage_batch VALUES (
                        'batch-legacy-policy', '2026-05-01T10:00:00+00:00',
                        '2026-05-01T10:00:00+00:00', 0, 1, 1
                    );
                    INSERT INTO lineage_issue VALUES (
                        1, 'DEV', 'fixture', 'PROGRAM_LEGACY', 'ORPHAN_BRANCH',
                        'MEDIUM', 'legacy-stable', NULL, 'TMP_LEGACY',
                        'legacy message', '{"edge_count":1}', 'batch-legacy-policy',
                        '2026-05-01T10:00:00+00:00', '2026-05-01T10:00:00+00:00', 1
                    );
                    """
                )
            store = SQLiteMaterializationStore(db_path)
            [issue] = store.read_issues(active_only=True)
            with closing(sqlite3.connect(db_path)) as connection:
                columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(lineage_issue)")
                }
                version = connection.execute("PRAGMA user_version").fetchone()[0]

        self.assertEqual(issue.stable_key, "legacy-stable")
        self.assertEqual(issue.confidence, AuditConfidence.UNKNOWN)
        self.assertEqual(issue.rule_version, "audit-rule-legacy")
        self.assertEqual(issue.disposition, IssueDisposition.OPEN)
        self.assertEqual(issue.policy_version, "audit-policy-legacy")
        self.assertTrue(
            {
                "confidence",
                "rule_version",
                "disposition",
                "policy_version",
                "disposition_updated_at",
                "disposition_updated_by",
            }.issubset(columns)
        )
        self.assertEqual(version, 3)

    def test_schema_sql_still_has_no_lineage_edge_semantic_change(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS lineage_edge", SCHEMA_SQL)
        self.assertNotIn("lineage_closure", SCHEMA_SQL)


class IncrementalPolicyReplayTests(unittest.TestCase):
    def test_policy_change_replays_unchanged_program_without_rebuilding_dag(self):
        program = source("PROGRAM_POLICY_REPLAY", BROKEN_BRANCH_PROGRAM)
        custom_policy = AuditPolicy(
            severity_by_issue_type={
                IssueType.ORPHAN_BRANCH: "LOW",
                IssueType.MULTI_SINK_CANDIDATE: "LOW",
            },
            default_disposition=IssueDisposition.ACCEPTED,
            policy_version="audit-policy-incremental-v2",
        )
        with TemporaryDirectory() as directory:
            db_path = Path(directory) / "incremental.db"
            imp_lineage_edge.materialize_sources(
                [program],
                db_path=db_path,
                batch_id="batch-incremental-1",
                observed_at=OBSERVED_AT,
                complete_snapshot=True,
            )
            with patch(
                "jobs.crontab.imp_lineage_edge.build_program_physical_dag",
                side_effect=AssertionError("policy-only replay rebuilt DAG"),
            ):
                imp_lineage_edge.materialize_sources(
                    [program],
                    db_path=db_path,
                    batch_id="batch-incremental-2",
                    observed_at=OBSERVED_AT.replace(day=2),
                    complete_snapshot=True,
                    policy=custom_policy,
                )
            store = SQLiteMaterializationStore(db_path)
            issues = store.read_issues(active_only=True)

        self.assertTrue(issues)
        self.assertTrue(all(issue.severity == "LOW" for issue in issues))
        self.assertTrue(
            all(issue.disposition is IssueDisposition.ACCEPTED for issue in issues)
        )
        self.assertTrue(
            all(issue.policy_version == "audit-policy-incremental-v2" for issue in issues)
        )


if __name__ == "__main__":
    unittest.main()
