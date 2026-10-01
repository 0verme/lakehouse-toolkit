from __future__ import annotations

import json
import unittest
from dataclasses import replace
from datetime import datetime, timezone

from shared.lineage.dap_contract import (
    DAPLineageCapacityLimits,
    build_dap_lineage_contract,
    preflight_dap_lineage_contract,
    serialize_dap_lineage_contract,
)
from shared.lineage.domain import LineageEdge, ProgramIdentity
from shared.lineage.materialization_dws import (
    DWSActiveSnapshotMetadata,
    DWSBusinessEdgeRow,
    business_edge_key,
    program_key,
)


OBSERVED_AT = datetime(2026, 9, 30, 8, 9, 10, tzinfo=timezone.utc)
ENVIRONMENT = "DEV214"
SOURCE_PROFILE = "fixture-sql"
BATCH_ID = "batch-dap-fixture-1"


def active_snapshot(
    *,
    batch_id: str = BATCH_ID,
    environment: str = ENVIRONMENT,
    source_profile: str = SOURCE_PROFILE,
    complete: bool = True,
) -> DWSActiveSnapshotMetadata:
    return DWSActiveSnapshotMetadata(
        batch_id=batch_id,
        snapshot_scope=((environment, source_profile),),
        observed_at=OBSERVED_AT,
        complete_snapshot=complete,
        snapshot_mode="FULL" if complete else "PARTIAL",
    )


def business_row(
    source_table: str,
    target_table: str,
    *,
    program_name: str = "005:DWM.RESULT:1:fixture",
    environment: str = ENVIRONMENT,
    source_profile: str = SOURCE_PROFILE,
    batch_id: str = BATCH_ID,
    collapse_depth: int = 2,
    source_hash: str | None = "sha256:synthetic",
    physical_derivation_hash: str = "a" * 64,
) -> DWSBusinessEdgeRow:
    edge = LineageEdge(
        environment=environment,
        source_profile=source_profile,
        source_table=source_table,
        target_table=target_table,
        program_name=program_name,
    )
    return DWSBusinessEdgeRow(
        row_key=f"row:{business_edge_key(edge)}",
        business_edge_key=business_edge_key(edge),
        environment=environment,
        source_profile=source_profile,
        program_key=program_key(
            ProgramIdentity(environment, source_profile, program_name)
        ),
        program_name=program_name,
        source_dataset_key="dataset-source",
        source_table=edge.source_table,
        target_dataset_key="dataset-target",
        target_table=edge.target_table,
        collapse_depth=collapse_depth,
        physical_derivation_hash=physical_derivation_hash,
        source_hash=source_hash,
        pipeline_version="lineage-pipeline-v12-fixture",
        batch_id=batch_id,
        observed_at=OBSERVED_AT,
        first_seen_at=OBSERVED_AT,
        last_seen_at=OBSERVED_AT,
        last_changed_at=OBSERVED_AT,
        is_active=True,
        created_at=OBSERVED_AT,
        updated_at=OBSERVED_AT,
    )


def make_contract(*rows: DWSBusinessEdgeRow) -> dict[str, object]:
    return build_dap_lineage_contract(
        rows,
        active_snapshot=active_snapshot(),
        environment=ENVIRONMENT,
        source_profile=SOURCE_PROFILE,
    )


class DAPLineageContractAdapterTests(unittest.TestCase):
    def test_single_business_edge_projects_table_task_table(self) -> None:
        contract = make_contract(business_row("DWF.A", "DWM.B"))
        nodes = contract["nodes"]
        edges = contract["edges"]
        assert isinstance(nodes, list)
        assert isinstance(edges, list)

        node_by_id = {node["externalId"]: node for node in nodes}
        table_nodes = [node for node in nodes if node["type"] == "table"]
        task_nodes = [node for node in nodes if node["type"] == "task"]
        self.assertEqual(len(table_nodes), 2)
        self.assertEqual(len(task_nodes), 1)
        self.assertEqual(task_nodes[0]["name"], "005:DWM.RESULT:1:fixture")
        self.assertEqual(
            task_nodes[0]["attributes"]["programKey"],
            business_row("DWF.A", "DWM.B").program_key,
        )
        self.assertEqual(len(edges), 2)
        self.assertEqual(
            {(edge["type"], edge["sourceId"], edge["targetId"]) for edge in edges},
            {
                (
                    "table_to_task",
                    "table:DEV214:DWF.A",
                    task_nodes[0]["externalId"],
                ),
                (
                    "task_to_table",
                    task_nodes[0]["externalId"],
                    "table:DEV214:DWM.B",
                ),
            },
        )
        for edge in edges:
            self.assertIn(edge["sourceId"], node_by_id)
            self.assertIn(edge["targetId"], node_by_id)
            self.assertEqual(edge["confidence"], "unknown")
            expected_key = business_row("DWF.A", "DWM.B").business_edge_key
            self.assertEqual(edge["evidence"]["sourceRecordId"], expected_key)
            self.assertEqual(edge["diagnostics"][0]["collapseDepth"], 2)

    def test_multiple_edges_share_one_task_and_reuse_table_nodes(self) -> None:
        rows = (
            business_row("DWF.A", "DWM.SHARED"),
            business_row("DWF.B", "DWM.SHARED"),
        )
        contract = make_contract(*rows)
        nodes = contract["nodes"]
        edges = contract["edges"]
        assert isinstance(nodes, list)
        assert isinstance(edges, list)
        self.assertEqual(sum(node["type"] == "task" for node in nodes), 1)
        self.assertEqual(sum(node["type"] == "table" for node in nodes), 3)
        self.assertEqual(len(edges), 4)
        task_id = next(node["externalId"] for node in nodes if node["type"] == "task")
        self.assertEqual(
            sum(edge["targetId"] == task_id for edge in edges),
            2,
        )
        shared_id = "table:DEV214:DWM.SHARED"
        self.assertEqual(
            sum(edge["targetId"] == shared_id for edge in edges),
            2,
        )

    def test_same_table_can_be_reused_by_multiple_programs(self) -> None:
        rows = (
            business_row("DWF.A", "DWM.SHARED", program_name="program-one"),
            business_row("DWF.B", "DWM.SHARED", program_name="program-two"),
        )
        contract = make_contract(*rows)
        nodes = contract["nodes"]
        edges = contract["edges"]
        assert isinstance(nodes, list)
        assert isinstance(edges, list)
        self.assertEqual(sum(node["type"] == "task" for node in nodes), 2)
        self.assertEqual(sum(node["type"] == "table" for node in nodes), 3)
        self.assertEqual(
            sum(edge["targetId"] == "table:DEV214:DWM.SHARED" for edge in edges),
            2,
        )

    def test_node_edge_snapshot_identity_and_json_are_stable(self) -> None:
        rows = (
            business_row("DWF.B", "DWM.Z", program_name="program-two"),
            business_row("DWF.A", "DWM.Y", program_name="program-one"),
        )
        first = make_contract(*rows)
        second = make_contract(*rows)
        self.assertEqual(first, second)
        self.assertEqual(
            serialize_dap_lineage_contract(first),
            serialize_dap_lineage_contract(second),
        )
        self.assertEqual(
            first["snapshot"]["externalSnapshotId"],
            second["snapshot"]["externalSnapshotId"],
        )
        self.assertEqual(
            [node["externalId"] for node in first["nodes"]],
            sorted(node["externalId"] for node in first["nodes"]),
        )
        self.assertEqual(
            [edge["externalId"] for edge in first["edges"]],
            sorted(edge["externalId"] for edge in first["edges"]),
        )

    def test_snapshot_identity_changes_with_batch_and_scope(self) -> None:
        baseline = make_contract()["snapshot"]["externalSnapshotId"]
        another_batch = build_dap_lineage_contract(
            (),
            active_snapshot=active_snapshot(batch_id="batch-dap-fixture-2"),
            environment=ENVIRONMENT,
            source_profile=SOURCE_PROFILE,
        )["snapshot"]["externalSnapshotId"]
        another_profile = build_dap_lineage_contract(
            (),
            active_snapshot=active_snapshot(source_profile="fixture-other"),
            environment=ENVIRONMENT,
            source_profile="fixture-other",
        )["snapshot"]["externalSnapshotId"]
        expected_length = 64 + len("lakehouse-toolkit:dap-lineage-adapter-v1:")
        self.assertEqual(len(baseline), expected_length)
        self.assertNotEqual(baseline, another_batch)
        self.assertNotEqual(baseline, another_profile)

    def test_input_order_does_not_change_canonical_contract(self) -> None:
        rows = (
            business_row("DWF.A", "DWM.Y", program_name="program-one"),
            business_row("DWF.B", "DWM.Z", program_name="program-two"),
        )
        forward = make_contract(*rows)
        reversed_order = make_contract(*reversed(rows))
        self.assertEqual(
            serialize_dap_lineage_contract(forward),
            serialize_dap_lineage_contract(reversed_order),
        )

    def test_duplicate_canonical_business_edge_is_deduplicated(self) -> None:
        row = business_row("DWF.A", "DWM.B")
        contract = make_contract(row, row)
        self.assertEqual(len(contract["edges"]), 2)
        with self.assertRaisesRegex(ValueError, "conflicting row data"):
            make_contract(row, replace(row, collapse_depth=3))

    def test_other_environment_or_source_profile_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside the requested"):
            make_contract(
                business_row("DWF.A", "DWM.B", environment="PROD")
            )
        with self.assertRaisesRegex(ValueError, "outside the requested"):
            make_contract(
                business_row("DWF.A", "DWM.B", source_profile="other-profile")
            )

    def test_projection_does_not_copy_sql_or_raw_sensitive_metadata(self) -> None:
        row = business_row(
            "DWF.A",
            "DWM.B",
            source_hash="SELECT * FROM confidential_table WHERE password='secret'",
            physical_derivation_hash="token=do-not-export",
        )
        serialized = serialize_dap_lineage_contract(make_contract(row))
        self.assertNotIn("SELECT *", serialized)
        self.assertNotIn("password", serialized)
        self.assertNotIn("secret", serialized)
        self.assertNotIn("token=", serialized)
        self.assertIn(row.business_edge_key, serialized)
        self.assertIn("collapseDepth", serialized)
        self.assertIn("pipelineVersion", serialized)

    def test_empty_complete_snapshot_is_a_valid_empty_contract(self) -> None:
        contract = make_contract()
        self.assertEqual(contract["nodes"], [])
        self.assertEqual(contract["edges"], [])
        preflight = preflight_dap_lineage_contract(
            contract,
            environment=ENVIRONMENT,
            source_profile=SOURCE_PROFILE,
            toolkit_batch_id=BATCH_ID,
        )
        self.assertTrue(preflight.ready, preflight.errors)
        self.assertEqual(preflight.total_nodes, 0)
        self.assertEqual(preflight.dap_edges, 0)
        self.assertEqual(preflight.business_edges, 0)

    def test_partial_batch_is_not_exportable_as_replace_snapshot(self) -> None:
        with self.assertRaisesRegex(ValueError, "complete FULL toolkit snapshot"):
            build_dap_lineage_contract(
                (),
                active_snapshot=active_snapshot(complete=False),
                environment=ENVIRONMENT,
                source_profile=SOURCE_PROFILE,
            )

    def test_preflight_validates_endpoint_integrity(self) -> None:
        contract = make_contract(business_row("DWF.A", "DWM.B"))
        contract["edges"][0]["sourceId"] = "missing-node"
        preflight = preflight_dap_lineage_contract(
            contract,
            environment=ENVIRONMENT,
            source_profile=SOURCE_PROFILE,
            toolkit_batch_id=BATCH_ID,
        )
        self.assertFalse(preflight.ready)
        self.assertTrue(
            any("does not resolve" in error for error in preflight.errors),
            preflight.errors,
        )

    def test_preflight_reports_node_edge_and_payload_capacity_overflow(self) -> None:
        contract = make_contract(business_row("DWF.A", "DWM.B"))
        preflight = preflight_dap_lineage_contract(
            contract,
            environment=ENVIRONMENT,
            source_profile=SOURCE_PROFILE,
            toolkit_batch_id=BATCH_ID,
            limits=DAPLineageCapacityLimits(
                max_nodes=2,
                max_edges=1,
                max_payload_bytes=1,
            ),
        )
        self.assertFalse(preflight.ready)
        self.assertEqual(preflight.total_nodes, 3)
        self.assertEqual(preflight.dap_edges, 2)
        self.assertGreater(preflight.payload_bytes, 1)
        self.assertTrue(
            any("node capacity exceeded" in error for error in preflight.errors)
        )
        self.assertTrue(
            any("edge capacity exceeded" in error for error in preflight.errors)
        )
        self.assertTrue(
            any("body capacity exceeded" in error for error in preflight.errors)
        )
        self.assertEqual(
            preflight.payload_bytes,
            len(serialize_dap_lineage_contract(contract).encode("utf-8")),
        )

    def test_capacity_limits_reject_invalid_values(self) -> None:
        with self.assertRaises(ValueError):
            DAPLineageCapacityLimits(max_nodes=-1)

    def test_wire_json_uses_dap_camel_case_contract_names(self) -> None:
        serialized = serialize_dap_lineage_contract(
            make_contract(business_row("DWF.A", "DWM.B"))
        )
        decoded = json.loads(serialized)
        self.assertEqual(decoded["contractVersion"], "1.0")
        self.assertIn("externalSnapshotId", decoded["snapshot"])
        self.assertIn("sourceId", decoded["edges"][0])
        self.assertIn("sourceRecordId", decoded["edges"][0]["evidence"])
        self.assertTrue(serialized.endswith("\n"))


if __name__ == "__main__":
    unittest.main()
