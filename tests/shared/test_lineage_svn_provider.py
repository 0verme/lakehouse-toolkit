from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import yaml  # pyright: ignore[reportMissingModuleSource]

from jobs.crontab import imp_lineage_edge
from shared.lineage import svn_inventory
from shared.lineage.lineage_builder import normalize_table_name
from shared.lineage.materialization import materialize_program
from shared.lineage.physical_dag import build_program_physical_dag
from shared.lineage.providers import (
    load_program_source_providers,
    load_svn_program_source_profiles,
)
from shared.lineage.svn_inventory import (
    DECODE_ERROR,
    DWF_LAYOUT,
    INVALID_PROGRAM_DIRECTORY,
    NOT_ATTEMPTED,
    OUT_OF_SCOPE,
    PROCESSING_LAYOUT,
    READ_ERROR,
    SourceReadResult,
    SVNFileInventory,
    SVNProfile,
    SVNScanResult,
    UNRESOLVED_REASON_ORDER,
)
from shared.lineage.svn_provider import (
    IDENTITY_COLLISION,
    SNAPSHOT_COMPLETE,
    SVNProgramSourceProvider,
    canonicalize_svn_relative_locator,
    svn_program_name_from_relative_path,
)


class SVNProgramSourceProviderTests(unittest.TestCase):
    @staticmethod
    def _write_program(
        root: Path,
        *,
        layout: str,
        target: str = "DWM.DEMO_RESULT",
        filename: str = "DEMO_PROGRAM.py",
        source: str | None = None,
    ) -> Path:
        schema, table = target.split(".", 1)
        if layout == PROCESSING_LAYOUT:
            path = (
                root
                / "DIDP_PROJECT_WORKSPACE"
                / schema
                / "1.0"
                / f"DWS_{schema}"
                / f"DWS_{schema}.{table}"
                / filename
            )
        else:
            path = (
                root
                / "DIDP_PROJECT_WORKSPACE"
                / "DW_PROJECT"
                / "1.0"
                / "DWS_DWF"
                / f"DWS_DWF.{table}"
                / filename
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            source
            or f'execute("INSERT INTO {target} SELECT * FROM ODS.DEMO_SOURCE")\n',
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _provider(root: Path, name: str, layout: str) -> SVNProgramSourceProvider:
        return SVNProgramSourceProvider(
            SVNProfile(
                name=name,
                environment="PROD",
                root_path=root,
                layout=layout,
            )
        )

    def test_production_svn_provider_enforces_prod_environment(self):
        profile = SVNProfile(
            name="DEMO_SVN_PROFILE",
            environment="DEV",
            root_path=Path("DEMO_ROOT"),
            layout=PROCESSING_LAYOUT,
        )
        with self.assertRaisesRegex(ValueError, "environment=PROD"):
            SVNProgramSourceProvider(profile)

    def test_processing_and_dwf_map_only_valid_files_and_preserve_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "DEMO_SVN_ROOT"
            processing_path = self._write_program(
                root,
                layout=PROCESSING_LAYOUT,
                target="DWM.DEMO_PROCESSING",
            )
            dwf_path = self._write_program(
                root,
                layout=DWF_LAYOUT,
                target="DWF.DEMO_DWF",
                filename="DEMO_DWF_PROGRAM.py",
            )
            (root / "DIDP_PROJECT_WORKSPACE" / "DEMO_SIBLING").mkdir(parents=True)
            (
                root / "DIDP_PROJECT_WORKSPACE" / "DEMO_SIBLING" / "DEMO_OTHER.py"
            ).write_text("print('out of scope')\n", encoding="utf-8")

            processing_provider = self._provider(
                root, "prod_svn_processing", PROCESSING_LAYOUT
            )
            dwf_provider = self._provider(root, "prod_svn_dwf", DWF_LAYOUT)
            processing_sources = list(processing_provider.iter_program_sources())
            dwf_sources = list(dwf_provider.iter_program_sources())

        self.assertEqual(len(processing_sources), 1)
        self.assertEqual(len(dwf_sources), 1)
        self.assertEqual(processing_sources[0].environment, "PROD")
        self.assertEqual(processing_sources[0].source_profile, "prod_svn_processing")
        self.assertEqual(processing_sources[0].expected_target, "DWM.DEMO_PROCESSING")
        self.assertEqual(dwf_sources[0].source_profile, "prod_svn_dwf")
        self.assertEqual(dwf_sources[0].expected_target, "DWF.DEMO_DWF")
        self.assertEqual(processing_provider.snapshot_status, SNAPSHOT_COMPLETE)
        self.assertEqual(dwf_provider.snapshot_status, SNAPSHOT_COMPLETE)
        self.assertEqual(
            processing_provider.accounting.out_of_scope_python_files,
            2,
        )
        self.assertEqual(
            processing_provider.accounting.diagnostic_counts[OUT_OF_SCOPE],
            2,
        )
        accounting_text = json.dumps(processing_provider.accounting.to_dict())
        self.assertNotIn(str(processing_path), accounting_text)
        self.assertNotIn(processing_path.name, accounting_text)
        self.assertNotIn("INSERT INTO", accounting_text)
        self.assertEqual(processing_path.suffix, ".py")
        self.assertEqual(dwf_path.suffix, ".py")

    def test_expected_target_comes_from_validated_directory_not_sql_guessing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "DEMO_SVN_ROOT"
            self._write_program(
                root,
                layout=PROCESSING_LAYOUT,
                target="DWM.DEMO_PATH_TARGET",
                source=(
                    'execute("INSERT INTO DWM.DEMO_SQL_TARGET '
                    'SELECT * FROM ODS.DEMO_SOURCE")\n'
                ),
            )
            provider = self._provider(root, "prod_svn_processing", PROCESSING_LAYOUT)
            source = list(provider.iter_program_sources())[0]

        self.assertEqual(source.expected_target, "DWM.DEMO_PATH_TARGET")
        dag = build_program_physical_dag(source)
        self.assertEqual(
            dag.expected_target, normalize_table_name("DWM.DEMO_PATH_TARGET")
        )
        self.assertEqual(
            {edge.target for edge in dag.edges},
            {normalize_table_name("DWM.DEMO_SQL_TARGET")},
        )

    def test_malformed_candidate_is_rejected_and_marks_snapshot_partial(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "DEMO_SVN_ROOT"
            self._write_program(
                root,
                layout=PROCESSING_LAYOUT,
                target="DWM.DEMO_VALID",
            )
            malformed = (
                root
                / "DIDP_PROJECT_WORKSPACE"
                / "DWM"
                / "1.0"
                / "DWS_DWM"
                / "DEMO_NOT_PROGRAM"
                / "DEMO_BAD.py"
            )
            malformed.parent.mkdir(parents=True, exist_ok=True)
            malformed.write_text("print('malformed')\n", encoding="utf-8")

            provider = self._provider(root, "prod_svn_processing", PROCESSING_LAYOUT)
            sources = list(provider.iter_program_sources())

        self.assertEqual(len(sources), 1)
        self.assertFalse(provider.snapshot_complete)
        self.assertEqual(
            provider.accounting.diagnostic_counts[INVALID_PROGRAM_DIRECTORY], 1
        )
        self.assertEqual(provider.accounting.rejected_program_files, 1)
        self.assertIn(
            INVALID_PROGRAM_DIRECTORY,
            {item.reason for item in provider.diagnostics},
        )

    def test_read_and_decode_failures_are_diagnostics_not_program_sources(self):
        for read_status, expected_reason, field_name in (
            (READ_ERROR, READ_ERROR, "read_errors"),
            (DECODE_ERROR, DECODE_ERROR, "decode_errors"),
        ):
            with (
                self.subTest(read_status=read_status),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory) / "DEMO_SVN_ROOT"
                self._write_program(
                    root,
                    layout=PROCESSING_LAYOUT,
                    target="DWM.DEMO_READ_RESULT",
                )
                provider = self._provider(
                    root, "prod_svn_processing", PROCESSING_LAYOUT
                )
                reader_result = SourceReadResult(None, read_status, read_status)
                with patch.object(
                    svn_inventory,
                    "read_python_source",
                    return_value=reader_result,
                ) as reader:
                    sources = list(provider.iter_program_sources())

            self.assertEqual(sources, [])
            self.assertFalse(provider.snapshot_complete)
            self.assertEqual(getattr(provider.accounting, field_name), 1)
            self.assertEqual(provider.accounting.diagnostic_counts[expected_reason], 1)
            reader.assert_called_once()

    def test_identity_is_root_independent_and_source_hash_is_stable(self):
        relative_path = (
            "DIDP_PROJECT_WORKSPACE/DWM/1.0/DWS_DWM/DWS_DWM.DEMO_RESULT/DEMO_PROGRAM.py"
        )
        with (
            tempfile.TemporaryDirectory() as first_directory,
            tempfile.TemporaryDirectory() as second_directory,
        ):
            first_root = Path(first_directory) / "DEMO_ROOT_A"
            second_root = Path(second_directory) / "DEMO_ROOT_B"
            for root in (first_root, second_root):
                path = root / Path(*relative_path.split("/"))
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    'execute("INSERT INTO DWM.DEMO_RESULT SELECT * FROM ODS.DEMO_SOURCE")\n',
                    encoding="utf-8",
                )

            first = list(
                self._provider(
                    first_root, "prod_svn_processing", PROCESSING_LAYOUT
                ).iter_program_sources()
            )
            second = list(
                self._provider(
                    second_root, "prod_svn_processing", PROCESSING_LAYOUT
                ).iter_program_sources()
            )

        self.assertEqual(first, second)
        self.assertEqual(first[0].source_hash, second[0].source_hash)
        self.assertEqual(
            first[0].program_name, svn_program_name_from_relative_path(relative_path)
        )
        self.assertEqual(
            canonicalize_svn_relative_locator(relative_path),
            canonicalize_svn_relative_locator(relative_path.replace("/", "\\")),
        )
        self.assertNotIn(str(first_root), first[0].program_name)
        self.assertNotIn("DEMO_ROOT_A", first[0].program_name)

    def test_identity_collision_is_rejected_without_silent_overwrite(self):
        profile = SVNProfile(
            name="prod_svn_processing",
            environment="PROD",
            root_path=Path("DEMO_ROOT"),
            layout=PROCESSING_LAYOUT,
        )
        records = tuple(
            SVNFileInventory(
                profile_name=profile.name,
                environment=profile.environment,
                relative_path=relative_path,
                filename=relative_path.rsplit("/", 1)[-1],
                layout=PROCESSING_LAYOUT,
                layer="DWM",
                declared_primary_target="DWM.DEMO_RESULT",
                file_size=1,
                read_status=NOT_ATTEMPTED,
                matched_program_file=True,
                directory_pattern_valid=True,
                candidate=True,
            )
            for relative_path in (
                "DIDP_PROJECT_WORKSPACE/DWM/1.0/DWS_DWM/"
                "DWS_DWM.DEMO_RESULT/DEMO_CASE.py",
                "DIDP_PROJECT_WORKSPACE/DWM/1.0/DWS_DWM/"
                "DWS_DWM.DEMO_RESULT/demo_case.py",
            )
        )
        scan = SVNScanResult(
            profile_name=profile.name,
            environment=profile.environment,
            layout=PROCESSING_LAYOUT,
            status=svn_inventory.SUCCESS,
            scanned_python_files=2,
            matched_program_files=2,
            unmatched_python_files=0,
            primary_target_resolved=2,
            primary_target_unresolved=0,
            readable_files=0,
            read_errors=0,
            decode_errors=0,
            layer_counts={"DWM": 2},
            unresolved_reasons=dict.fromkeys(UNRESOLVED_REASON_ORDER, 0),
            elapsed_ms=0.0,
            records=records,
            candidate_program_files=2,
        )
        provider = SVNProgramSourceProvider(profile)
        with patch.object(svn_inventory, "scan_svn_profile", return_value=scan):
            sources = list(provider.iter_program_sources())

        self.assertEqual(sources, [])
        self.assertFalse(provider.snapshot_complete)
        self.assertEqual(provider.accounting.diagnostic_counts[IDENTITY_COLLISION], 2)
        self.assertEqual(provider.accounting.rejected_program_files, 2)

    def test_unexpected_inventory_failure_marks_snapshot_failed_without_masking_error(
        self,
    ):
        provider = self._provider(
            Path("DEMO_ROOT"), "prod_svn_processing", PROCESSING_LAYOUT
        )
        with patch.object(
            svn_inventory,
            "scan_svn_profile",
            side_effect=RuntimeError("DEMO_INTERNAL_FAILURE"),
        ):
            with self.assertRaisesRegex(RuntimeError, "DEMO_INTERNAL_FAILURE"):
                list(provider.iter_program_sources())

        self.assertFalse(provider.snapshot_complete)
        self.assertEqual(provider.snapshot_status, "FAILED")
        self.assertEqual(provider.accounting.scanned_python_files, 0)

    def test_same_snapshot_iteration_is_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "DEMO_SVN_ROOT"
            self._write_program(
                root,
                layout=PROCESSING_LAYOUT,
                target="DWM.DEMO_Z",
                filename="DEMO_Z.py",
            )
            self._write_program(
                root,
                layout=PROCESSING_LAYOUT,
                target="DWM.DEMO_A",
                filename="DEMO_A.py",
            )
            provider = self._provider(root, "prod_svn_processing", PROCESSING_LAYOUT)
            first = list(provider.iter_program_sources())
            second = list(provider.iter_program_sources())

        self.assertEqual(first, second)
        self.assertEqual(
            [item.program_name for item in first],
            sorted(item.program_name for item in first),
        )
        self.assertEqual(
            [item.source_hash for item in first],
            [item.source_hash for item in second],
        )


class SVNProgramSourceIntegrationTests(unittest.TestCase):
    def test_svn_config_rejects_network_and_credential_fields(self):
        profiles = (
            {
                "name": "prod_svn_processing",
                "environment": "PROD",
                "root_path": "svn://DEMO_REPOSITORY",
                "layout": PROCESSING_LAYOUT,
            },
            {
                "name": "prod_svn_processing",
                "environment": "PROD",
                "root_path": "DEMO_ROOT",
                "connection": {"password": "DEMO_SECRET"},
                "layout": PROCESSING_LAYOUT,
            },
        )
        with tempfile.TemporaryDirectory() as directory:
            for index, profile in enumerate(profiles):
                with self.subTest(index=index):
                    config_path = Path(directory) / f"DEMO_PROVIDERS_{index}.yaml"
                    config_path.write_text(
                        yaml.safe_dump({"svn_profiles": [profile]}),
                        encoding="utf-8",
                    )
                    with self.assertRaises(svn_inventory.SVNInventoryConfigError):
                        load_svn_program_source_profiles(config_path)

    def test_provider_loader_keeps_two_production_profiles_independent(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "DEMO_PROVIDERS.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "svn_profiles": [
                            {
                                "name": "prod_svn_processing",
                                "environment": "PROD",
                                "root_path": "DEMO_ROOT",
                                "layout": PROCESSING_LAYOUT,
                            },
                            {
                                "name": "prod_svn_dwf",
                                "environment": "PROD",
                                "root_path": "DEMO_ROOT",
                                "layout": DWF_LAYOUT,
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            profiles = load_svn_program_source_profiles(config_path)
            providers = load_program_source_providers(config_path)

        self.assertEqual(
            [
                (profile.name, profile.environment, profile.layout)
                for profile in profiles
            ],
            [
                ("prod_svn_processing", "PROD", PROCESSING_LAYOUT),
                ("prod_svn_dwf", "PROD", DWF_LAYOUT),
            ],
        )
        self.assertEqual(
            [
                (provider.source_profile, provider.profile.layout)
                for provider in providers
            ],
            [
                ("prod_svn_processing", PROCESSING_LAYOUT),
                ("prod_svn_dwf", DWF_LAYOUT),
            ],
        )

    def test_svn_program_source_reuses_physical_dag_and_materialization(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "DEMO_SVN_ROOT"
            SVNProgramSourceProviderTests._write_program(
                root,
                layout=PROCESSING_LAYOUT,
            )
            provider = SVNProgramSourceProvider(
                SVNProfile(
                    name="prod_svn_processing",
                    environment="PROD",
                    root_path=root,
                    layout=PROCESSING_LAYOUT,
                )
            )
            sources = list(provider.iter_program_sources())
            source = sources[0]
            dag = build_program_physical_dag(source)
            materialized = materialize_program(
                dag,
                batch_id="batch-demo-svn",
                observed_at=datetime.now(timezone.utc),
            )

        self.assertEqual(dag.program_source, source)
        self.assertEqual(
            dag.expected_target,
            normalize_table_name("DWM.DEMO_RESULT"),
        )
        self.assertEqual(
            {(edge.source_table, edge.target_table) for edge in materialized.edges},
            {("ODS.DEMO_SOURCE", normalize_table_name("DWM.DEMO_RESULT"))},
        )
        self.assertEqual(materialized.edges[0].source_profile, "prod_svn_processing")
        self.assertTrue(provider.snapshot_complete)

    def test_provider_failure_downgrades_snapshot_and_retains_previous_facts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "DEMO_SVN_ROOT"
            SVNProgramSourceProviderTests._write_program(
                root,
                layout=PROCESSING_LAYOUT,
            )
            first_provider = SVNProgramSourceProvider(
                SVNProfile(
                    name="prod_svn_processing",
                    environment="PROD",
                    root_path=root,
                    layout=PROCESSING_LAYOUT,
                )
            )
            db_path = Path(directory) / "DEMO_LINEAGE.db"
            self.assertEqual(
                imp_lineage_edge.main(
                    [first_provider],
                    db_path=db_path,
                    batch_id="batch-demo-svn-initial",
                    coverage_report_path=None,
                ),
                0,
            )

            failed_provider = SVNProgramSourceProvider(
                SVNProfile(
                    name="prod_svn_processing",
                    environment="PROD",
                    root_path=root,
                    layout=PROCESSING_LAYOUT,
                )
            )
            output = io.StringIO()
            with (
                patch.object(
                    svn_inventory,
                    "read_python_source",
                    return_value=SourceReadResult(None, READ_ERROR, READ_ERROR),
                ),
                redirect_stdout(output),
            ):
                result = imp_lineage_edge.main(
                    [failed_provider],
                    db_path=db_path,
                    batch_id="batch-demo-svn-partial",
                    coverage_report_path=None,
                )
            store = imp_lineage_edge.SQLiteMaterializationStore(db_path)
            active_edges = store.read_edges(active_only=True)

        self.assertEqual(result, 0)
        self.assertEqual(len(active_edges), 1)
        self.assertEqual(active_edges[0].batch_id, "batch-demo-svn-partial")
        self.assertIn("stage=source_provider status=INCOMPLETE", output.getvalue())
        self.assertIn("partial_snapshot=True", output.getvalue())

    def test_profile_only_full_replay_uses_scoped_complete_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "DEMO_SVN_ROOT"
            SVNProgramSourceProviderTests._write_program(
                root,
                layout=PROCESSING_LAYOUT,
            )
            provider = SVNProgramSourceProvider(
                SVNProfile(
                    name="prod_svn_processing",
                    environment="PROD",
                    root_path=root,
                    layout=PROCESSING_LAYOUT,
                )
            )
            output = io.StringIO()
            with redirect_stdout(output):
                result = imp_lineage_edge.main(
                    [provider],
                    db_path=Path(directory) / "DEMO_LINEAGE.db",
                    batch_id="batch-demo-svn-job",
                    selected_profiles=("prod_svn_processing",),
                    coverage_report_path=None,
                )

        self.assertEqual(result, 0)
        self.assertIn("replay_mode=controlled_profile", output.getvalue())
        self.assertIn("partial_snapshot=False", output.getvalue())


if __name__ == "__main__":
    unittest.main()
