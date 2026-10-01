from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from types import SimpleNamespace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from shared.lineage.dap_contract import (
    DAPLineageCapacityLimits,
    DAPLineagePreflight,
)
from shared.lineage.domain import LineageEdge, ProgramIdentity
from shared.lineage.materialization_dws import (
    DWSActiveSnapshotMetadata,
    DWSBusinessEdgeRow,
    business_edge_key,
    program_key,
)
from tools.integrations import export_dap_lineage


STAMP = datetime(2026, 9, 30, 8, 9, 10, tzinfo=timezone.utc)
ENVIRONMENT = "DEV214"
SOURCE_PROFILE = "fixture-sql"
BATCH_ID = "batch-cli-fixture"


def make_active(
    *,
    batch_id: str = BATCH_ID,
    complete: bool = True,
) -> DWSActiveSnapshotMetadata:
    return DWSActiveSnapshotMetadata(
        batch_id=batch_id,
        snapshot_scope=((ENVIRONMENT, SOURCE_PROFILE),),
        observed_at=STAMP,
        complete_snapshot=complete,
        snapshot_mode="FULL" if complete else "PARTIAL",
    )


def make_row() -> DWSBusinessEdgeRow:
    edge = LineageEdge(
        environment=ENVIRONMENT,
        source_profile=SOURCE_PROFILE,
        source_table="DWF.A",
        target_table="DWM.B",
        program_name="005:DWM.B:1:fixture",
    )
    return DWSBusinessEdgeRow(
        row_key="row-cli-fixture",
        business_edge_key=business_edge_key(edge),
        environment=ENVIRONMENT,
        source_profile=SOURCE_PROFILE,
        program_key=program_key(
            ProgramIdentity(ENVIRONMENT, SOURCE_PROFILE, "005:DWM.B:1:fixture")
        ),
        program_name="005:DWM.B:1:fixture",
        source_dataset_key="source-dataset",
        source_table=edge.source_table,
        target_dataset_key="target-dataset",
        target_table=edge.target_table,
        collapse_depth=2,
        physical_derivation_hash="b" * 64,
        source_hash="sha256:fixture",
        pipeline_version="lineage-pipeline-v12-fixture",
        batch_id=BATCH_ID,
        observed_at=STAMP,
        first_seen_at=STAMP,
        last_seen_at=STAMP,
        last_changed_at=STAMP,
        is_active=True,
        created_at=STAMP,
        updated_at=STAMP,
    )


class FakeReader:
    def __init__(
        self,
        active_snapshots: tuple[DWSActiveSnapshotMetadata | None, ...] = (
            make_active(),
        ),
        rows: tuple[DWSBusinessEdgeRow, ...] = (make_row(),),
    ) -> None:
        self.active_snapshots = active_snapshots
        self.rows = rows
        self.metadata_reads = 0
        self.business_read: dict[str, object] | None = None

    def get_active_snapshot_metadata(self) -> DWSActiveSnapshotMetadata | None:
        index = min(self.metadata_reads, len(self.active_snapshots) - 1)
        self.metadata_reads += 1
        return self.active_snapshots[index]

    def read_business_rows(
        self,
        *,
        batch_id: str,
        active_only: bool,
        environment: str,
        source_profile: str,
    ) -> tuple[DWSBusinessEdgeRow, ...]:
        self.business_read = {
            "batch_id": batch_id,
            "active_only": active_only,
            "environment": environment,
            "source_profile": source_profile,
        }
        return self.rows


class ExportDapLineageTests(unittest.TestCase):
    def test_cli_requires_explicit_environment_and_source_profile(self) -> None:
        with self.assertRaises(SystemExit):
            export_dap_lineage.build_parser().parse_args(["--dry-run"])
        args = export_dap_lineage.build_parser().parse_args(
            [
                "--environment",
                ENVIRONMENT,
                "--source-profile",
                SOURCE_PROFILE,
                "--dry-run",
            ]
        )
        self.assertEqual(args.environment, ENVIRONMENT)
        self.assertEqual(args.source_profile, SOURCE_PROFILE)
        self.assertTrue(args.dry_run)

    def test_dws_profile_resolution_is_exact_and_explicit(self) -> None:
        resolver = SimpleNamespace(
            resolve=lambda environment: SimpleNamespace(
                dws_profile=f"dws-{environment}"
            )
        )
        with patch.object(
            export_dap_lineage,
            "load_lineage_environment_scope_resolver",
            return_value=resolver,
        ) as load_resolver:
            selected = export_dap_lineage._resolve_dws_profile(
                environment=ENVIRONMENT,
                explicit_profile=None,
                scope_config=None,
            )
        self.assertEqual(selected, f"dws-{ENVIRONMENT}")
        load_resolver.assert_called_once_with(config_path=None)

        with patch.object(
            export_dap_lineage,
            "load_lineage_environment_scope_resolver",
        ) as load_resolver:
            selected = export_dap_lineage._resolve_dws_profile(
                environment="OTHER",
                explicit_profile="explicit-dws",
                scope_config=None,
            )
        self.assertEqual(selected, "explicit-dws")
        load_resolver.assert_not_called()

    def test_exports_only_explicit_scope_from_one_complete_active_batch(self) -> None:
        reader = FakeReader()
        exported = export_dap_lineage.export_active_business_lineage(
            reader=reader,
            environment=ENVIRONMENT,
            source_profile=SOURCE_PROFILE,
            limits=DAPLineageCapacityLimits(),
        )
        self.assertTrue(exported.preflight.ready, exported.preflight.errors)
        self.assertEqual(reader.metadata_reads, 2)
        self.assertEqual(
            reader.business_read,
            {
                "batch_id": BATCH_ID,
                "active_only": True,
                "environment": ENVIRONMENT,
                "source_profile": SOURCE_PROFILE,
            },
        )
        self.assertEqual(exported.preflight.business_edges, 1)
        self.assertEqual(exported.preflight.dap_edges, 2)

    def test_no_batch_partial_batch_and_scope_mismatch_are_non_ready(self) -> None:
        cases = (
            (FakeReader(active_snapshots=(None,)), "no active toolkit"),
            (
                FakeReader(active_snapshots=(make_active(complete=False),)),
                "not a complete FULL snapshot",
            ),
            (
                FakeReader(
                    active_snapshots=(
                        DWSActiveSnapshotMetadata(
                            batch_id=BATCH_ID,
                            snapshot_scope=(("OTHER", SOURCE_PROFILE),),
                            observed_at=STAMP,
                            complete_snapshot=True,
                            snapshot_mode="FULL",
                        ),
                    )
                ),
                "not declared",
            ),
        )
        for reader, expected in cases:
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(
                    export_dap_lineage.DAPExportNotReadyError, expected
                ):
                    export_dap_lineage.export_active_business_lineage(
                        reader=reader,
                        environment=ENVIRONMENT,
                        source_profile=SOURCE_PROFILE,
                        limits=DAPLineageCapacityLimits(),
                    )
                self.assertIsNone(reader.business_read)

    def test_export_aborts_if_active_batch_changes_during_read(self) -> None:
        reader = FakeReader(
            active_snapshots=(make_active(), make_active(batch_id="batch-new"))
        )
        with self.assertRaisesRegex(
            export_dap_lineage.DAPExportNotReadyError, "changed while reading"
        ):
            export_dap_lineage.export_active_business_lineage(
                reader=reader,
                environment=ENVIRONMENT,
                source_profile=SOURCE_PROFILE,
                limits=DAPLineageCapacityLimits(),
            )

    def test_dry_run_prints_stats_without_writing_a_payload_file(self) -> None:
        stdout = io.StringIO()
        with (
            patch.object(
                export_dap_lineage,
                "DWSMaterializationStore",
                return_value=FakeReader(),
            ) as store,
            contextlib.redirect_stdout(stdout),
        ):
            status = export_dap_lineage.cli(
                [
                    "--environment",
                    ENVIRONMENT,
                    "--source-profile",
                    SOURCE_PROFILE,
                    "--dws-profile",
                    "fixture-dws",
                    "--dry-run",
                ]
            )
        self.assertEqual(status, 0)
        self.assertIn("READY", stdout.getvalue())
        self.assertIn("toolkit_batch_id: batch-cli-fixture", stdout.getvalue())
        self.assertIn("business_edges: 1", stdout.getvalue())
        self.assertIn("dap_edges: 2", stdout.getvalue())
        self.assertIn("capacity_nodes: 3/10000 PASS", stdout.getvalue())
        store.assert_called_once_with(profile="fixture-dws")

    def test_export_writes_deterministic_json_without_overwriting(self) -> None:
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as directory:
            output = Path(directory) / "lineage.json"
            stdout = io.StringIO()
            with (
                patch.object(
                    export_dap_lineage,
                    "DWSMaterializationStore",
                    return_value=FakeReader(),
                ),
                contextlib.redirect_stdout(stdout),
            ):
                status = export_dap_lineage.cli(
                    [
                        "--environment",
                        ENVIRONMENT,
                        "--source-profile",
                        SOURCE_PROFILE,
                        "--dws-profile",
                        "fixture-dws",
                        "--output",
                        str(output),
                    ]
                )
            self.assertEqual(status, 0)
            content = output.read_bytes()
            decoded = json.loads(content)
            self.assertEqual(decoded["contractVersion"], "1.0")
            self.assertEqual(decoded["snapshot"]["mode"], "replace")
            self.assertEqual(len(decoded["edges"]), 2)
            self.assertIn(f"output: {output}", stdout.getvalue())

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                repeated_status = export_dap_lineage.cli(
                    [
                        "--environment",
                        ENVIRONMENT,
                        "--source-profile",
                        SOURCE_PROFILE,
                        "--dws-profile",
                        "fixture-dws",
                        "--output",
                        str(output),
                    ]
                )
            self.assertEqual(repeated_status, 1)
            self.assertIn("FileExistsError", stderr.getvalue())
            self.assertEqual(output.read_bytes(), content)

    def test_non_ready_preflight_returns_nonzero(self) -> None:
        limits = DAPLineageCapacityLimits(
            max_nodes=0,
            max_edges=0,
            max_payload_bytes=0,
        )
        preflight = DAPLineagePreflight(
            environment=ENVIRONMENT,
            source_profile=SOURCE_PROFILE,
            toolkit_batch_id=BATCH_ID,
            table_nodes=1,
            task_nodes=1,
            total_nodes=2,
            business_edges=1,
            dap_edges=2,
            diagnostic_count=2,
            payload_bytes=512,
            payload_megabytes=512 / (1024 * 1024),
            contract_version="1.0",
            limits=limits,
            errors=("capacity exceeded",),
        )
        exported = export_dap_lineage.DAPLineageExport(
            contract={}, serialized="{}\n", preflight=preflight
        )
        stdout = io.StringIO()
        with (
            patch.object(
                export_dap_lineage,
                "DWSMaterializationStore",
                return_value=FakeReader(),
            ),
            patch.object(
                export_dap_lineage,
                "export_active_business_lineage",
                return_value=exported,
            ),
            contextlib.redirect_stdout(stdout),
        ):
            status = export_dap_lineage.cli(
                [
                    "--environment",
                    ENVIRONMENT,
                    "--source-profile",
                    SOURCE_PROFILE,
                    "--dws-profile",
                    "fixture-dws",
                    "--dry-run",
                ]
            )
        self.assertEqual(status, 2)
        self.assertIn("NON-READY", stdout.getvalue())
        self.assertIn("capacity_nodes: 2/0 OVER", stdout.getvalue())

    def test_output_path_must_stay_under_current_directory(self) -> None:
        with self.assertRaisesRegex(ValueError, "inside the current working directory"):
            export_dap_lineage._safe_output_path(Path.cwd().parent / "outside.json")


if __name__ == "__main__":
    unittest.main()
