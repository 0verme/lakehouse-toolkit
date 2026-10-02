from __future__ import annotations

import ast
import contextlib
import io
import json
import tempfile
import unittest
from collections import Counter, defaultdict
from pathlib import Path
from unittest.mock import patch

from jobs.crontab import sync_ods_dwf_field_mappings as entry
from tools.field_mapping import benchmark_resolver, metadata_resolver
from tools.field_mapping.collector import collect_workspace
from tools.field_mapping.metadata_resolver import MetadataResolver
from tools.field_mapping.models import RecvDwfRecord, SchemaConfigRecord


class ReferenceMetadataResolver:
    """Test-only copy of the pre-index resolver algorithm for equivalence checks."""

    def __init__(self, recv_dwf, schema_configs, upstream_payload):
        self.recv_dwf = tuple(recv_dwf)
        self.schema_configs = tuple(schema_configs)
        self.upstream_systems, self.upstream_system_count = (
            metadata_resolver.load_upstream_systems(upstream_payload)
        )
        self.schemas_by_key = defaultdict(set)
        for config in self.schema_configs:
            self.schemas_by_key[config.schema_key.casefold()].add(
                config.db_schema.upper()
            )
        self.systems_by_identity = defaultdict(set)
        for system in self.upstream_systems:
            self.systems_by_identity[system.identity.casefold()].add(
                system.upstream_system_id
            )

    def resolve(self, *, target, program_names, physical_source):
        logical_target = metadata_resolver.normalize_logical_target(target)
        target_rows = [
            row
            for row in self.recv_dwf
            if metadata_resolver.normalize_logical_target(row.table_name)
            == logical_target
        ]
        if not target_rows:
            return metadata_resolver.Resolution(
                status="UNRESOLVED", reason="no_recv_dwf"
            )

        program_keys = {
            metadata_resolver.normalize_program_name(name) for name in program_names
        }
        program_rows = [
            row
            for row in self.recv_dwf
            if metadata_resolver.normalize_program_name(row.ods_job_name)
            and metadata_resolver.normalize_program_name(row.ods_job_name)
            in program_keys
        ]
        target_program_rows = [
            row
            for row in program_rows
            if metadata_resolver.normalize_logical_target(row.table_name)
            == logical_target
        ]
        if program_rows and not target_program_rows:
            return metadata_resolver.Resolution(
                status="CONFLICT", reason="program_metadata_conflict"
            )
        if target_program_rows:
            target_identities = {
                (row.recv_plan.casefold(), row.data_source.casefold())
                for row in target_rows
            }
            matched_identities = {
                (row.recv_plan.casefold(), row.data_source.casefold())
                for row in target_program_rows
            }
            if not target_identities.intersection(matched_identities):
                return metadata_resolver.Resolution(
                    status="CONFLICT", reason="program_metadata_conflict"
                )
            candidates = target_program_rows
            evidence = ["ods_job_name", "table_name"]
        else:
            candidates = target_rows
            evidence = ["table_name"]

        source, source_error = metadata_resolver.parse_dwo_physical_table(
            physical_source, self.schema_configs
        )
        if source_error:
            return metadata_resolver.Resolution(
                status="UNRESOLVED", reason=source_error, evidence=tuple(evidence)
            )

        schema_compatible = []
        missing_schema = []
        for row in candidates:
            configured = self.schemas_by_key.get(row.data_source.casefold(), set())
            if not configured:
                missing_schema.append(row)
            elif source and source.db_schema.casefold() in {
                schema.casefold() for schema in configured
            }:
                schema_compatible.append(row)
        if not schema_compatible:
            if missing_schema:
                return metadata_resolver.Resolution(
                    status="UNRESOLVED",
                    reason="no_schema_config",
                    source=source,
                    evidence=tuple(evidence),
                )
            return metadata_resolver.Resolution(
                status="CONFLICT",
                reason="schema_match_conflict",
                source=source,
                evidence=tuple(evidence + ["db_schema"]),
            )
        candidates = schema_compatible
        evidence.append("db_schema")

        identities = {
            (row.recv_plan.casefold(), row.data_source.casefold())
            for row in candidates
        }
        recv_plans = {identity[0] for identity in identities}
        data_sources = {identity[1] for identity in identities}
        if len(recv_plans) > 1:
            return metadata_resolver.Resolution(
                status="CONFLICT",
                reason="multiple_recv_plan_conflict",
                source=source,
                evidence=tuple(evidence),
            )
        if len(data_sources) > 1:
            return metadata_resolver.Resolution(
                status="CONFLICT",
                reason="multiple_data_source_conflict",
                source=source,
                evidence=tuple(evidence),
            )

        record = min(
            candidates,
            key=lambda row: (
                row.recv_plan.casefold(),
                row.data_source.casefold(),
                metadata_resolver.normalize_program_name(row.ods_job_name),
                row.ods_job_name.casefold(),
            ),
        )
        matching_systems = self.systems_by_identity.get(
            record.recv_plan.casefold(), set()
        )
        if not matching_systems:
            return metadata_resolver.Resolution(
                status="UNRESOLVED",
                reason="unknown_upstream_system",
                record=record,
                source=source,
                evidence=tuple(evidence),
            )
        if len(matching_systems) != 1:
            return metadata_resolver.Resolution(
                status="CONFLICT",
                reason="upstream_system_conflict",
                record=record,
                source=source,
                evidence=tuple(evidence),
            )
        return metadata_resolver.Resolution(
            status="RESOLVED",
            record=record,
            source=source,
            upstream_system_id=next(iter(matching_systems)),
            evidence=tuple(evidence + ["dap_upstream_system"]),
        )


def _signature(result):
    return (
        result.status,
        result.reason,
        result.record,
        result.source,
        result.upstream_system_id,
        result.evidence,
    )


def _systems(*items):
    return {"items": [{"id": name, "upstreamSystemId": value} for name, value in items]}


class MetadataResolverEquivalenceTests(unittest.TestCase):
    def test_reference_and_indexed_resolver_match_all_resolution_fields(self):
        cases = [
            (
                "table_name_only",
                [RecvDwfRecord("PLAN", "DWF.DWF_TARGET", "SRC", ods_job_name="OTHER")],
                [SchemaConfigRecord("SRC", "MY_SCHEMA")],
                _systems(("PLAN", 10)),
                "DWF.F_TARGET",
                ("UNMATCHED.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("RESOLVED", None),
            ),
            (
                "program_and_table_name",
                [RecvDwfRecord("PLAN", "DWF.DWF_TARGET", "SRC", ods_job_name="JOB_DEMO_DAY")],
                [SchemaConfigRecord("SRC", "MY_SCHEMA")],
                _systems(("PLAN", 10)),
                "DWF.F_TARGET",
                ("005_DEMO.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("RESOLVED", None),
            ),
            (
                "program_metadata_conflict",
                [
                    RecvDwfRecord("P1", "DWF.F_TARGET", "S1", ods_job_name="OTHER"),
                    RecvDwfRecord("P2", "DWF.F_OTHER", "S2", ods_job_name="JOB_DEMO_DAY"),
                ],
                [SchemaConfigRecord("S1", "MY_SCHEMA"), SchemaConfigRecord("S2", "MY_SCHEMA")],
                _systems(("P1", 10), ("P2", 20)),
                "DWF.DWF_TARGET",
                ("005_DEMO.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("CONFLICT", "program_metadata_conflict"),
            ),
            (
                "db_schema_hit_with_underscores",
                [RecvDwfRecord("PLAN", "DWF.F_TARGET", "SRC", ods_job_name="")],
                [SchemaConfigRecord("SRC", "MY_SCHEMA_A_B")],
                _systems(("PLAN", 10)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_A_B_SOURCE",
                ("RESOLVED", None),
            ),
            (
                "no_schema_config",
                [RecvDwfRecord("PLAN", "DWF.F_TARGET", "MISSING", ods_job_name="")],
                [SchemaConfigRecord("PARSER_ONLY", "MY_SCHEMA")],
                _systems(("PLAN", 10)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("UNRESOLVED", "no_schema_config"),
            ),
            (
                "schema_match_conflict",
                [RecvDwfRecord("PLAN", "DWF.F_TARGET", "SRC", ods_job_name="")],
                [SchemaConfigRecord("SRC", "OTHER_SCHEMA"), SchemaConfigRecord("PARSER", "MY_SCHEMA")],
                _systems(("PLAN", 10)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("CONFLICT", "schema_match_conflict"),
            ),
            (
                "multiple_recv_plan_conflict",
                [
                    RecvDwfRecord("P1", "DWF.F_TARGET", "S1", ods_job_name=""),
                    RecvDwfRecord("P2", "DWF.DWF_TARGET", "S2", ods_job_name=""),
                ],
                [SchemaConfigRecord("S1", "MY_SCHEMA"), SchemaConfigRecord("S2", "MY_SCHEMA")],
                _systems(("P1", 10), ("P2", 20)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("CONFLICT", "multiple_recv_plan_conflict"),
            ),
            (
                "multiple_data_source_conflict",
                [
                    RecvDwfRecord("P1", "DWF.F_TARGET", "S1", ods_job_name=""),
                    RecvDwfRecord("P1", "DWF.DWF_TARGET", "S2", ods_job_name=""),
                ],
                [SchemaConfigRecord("S1", "MY_SCHEMA"), SchemaConfigRecord("S2", "MY_SCHEMA")],
                _systems(("P1", 10)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("CONFLICT", "multiple_data_source_conflict"),
            ),
            (
                "unknown_upstream_system",
                [RecvDwfRecord("UNKNOWN", "DWF.F_TARGET", "SRC", ods_job_name="")],
                [SchemaConfigRecord("SRC", "MY_SCHEMA")],
                _systems(),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("UNRESOLVED", "unknown_upstream_system"),
            ),
            (
                "upstream_system_conflict",
                [RecvDwfRecord("PLAN", "DWF.F_TARGET", "SRC", ods_job_name="")],
                [SchemaConfigRecord("SRC", "MY_SCHEMA")],
                _systems(("PLAN", 10), ("plan", 11)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("CONFLICT", "upstream_system_conflict"),
            ),
            (
                "equivalent_duplicate_metadata",
                [
                    RecvDwfRecord("PLAN", "DWF.F_TARGET", "SRC", ods_job_name="JOB_Z_DAY"),
                    RecvDwfRecord("plan", "DWF.DWF_TARGET", "src", ods_job_name="JOB_A_DAY"),
                ],
                [SchemaConfigRecord("SRC", "MY_SCHEMA")],
                _systems(("PLAN", 10)),
                "DWF.F_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("RESOLVED", None),
            ),
            (
                "same_target_multiple_source_systems",
                [
                    RecvDwfRecord("PLAN_A", "DWF.F_TARGET", "SRC_A", ods_job_name=""),
                    RecvDwfRecord("PLAN_B", "DWF.DWF_TARGET", "SRC_B", ods_job_name=""),
                ],
                [SchemaConfigRecord("SRC_A", "MY_SCHEMA"), SchemaConfigRecord("SRC_B", "OTHER_SCHEMA"), SchemaConfigRecord("PARSER", "MY_SCHEMA")],
                _systems(("PLAN_A", 10), ("PLAN_B", 20)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("RESOLVED", None),
            ),
            (
                "no_recv_dwf",
                [RecvDwfRecord("PLAN", "DWF.F_OTHER", "SRC", ods_job_name="")],
                [SchemaConfigRecord("SRC", "MY_SCHEMA")],
                _systems(("PLAN", 10)),
                "DWF.DWF_TARGET",
                ("NO_MATCH.py",),
                "DWO.DWO_MY_SCHEMA_SOURCE",
                ("UNRESOLVED", "no_recv_dwf"),
            ),
        ]

        for name, recv, schemas, systems, target, programs, physical_source, wanted in cases:
            with self.subTest(case=name):
                indexed = MetadataResolver(recv, schemas, systems)
                reference = ReferenceMetadataResolver(recv, schemas, systems)
                args = {
                    "target": target,
                    "program_names": programs,
                    "physical_source": physical_source,
                }
                actual = indexed.resolve(**args)
                expected = reference.resolve(**args)
                self.assertEqual(wanted, (actual.status, actual.reason))
                self.assertEqual(_signature(expected), _signature(actual))

    def test_cache_key_normalizes_and_deduplicates_inputs_without_cross_merging(self):
        recv = [
            RecvDwfRecord("PLAN_A", "DWF.F_TARGET", "SRC_A", ods_job_name="JOB_DEMO_DAY"),
            RecvDwfRecord("PLAN_B", "DWF.DWF_OTHER", "SRC_B", ods_job_name="JOB_OTHER_DAY"),
        ]
        resolver = MetadataResolver(
            recv,
            [
                SchemaConfigRecord("SRC_A", "SCHEMA_A"),
                SchemaConfigRecord("SRC_B", "SCHEMA_B"),
            ],
            _systems(("PLAN_A", 10), ("PLAN_B", 20)),
        )
        first = resolver.resolve(
            target="DWF.F_TARGET",
            program_names=("005_DEMO.py", "JOB_DEMO_DAY", "005_DEMO.py"),
            physical_source='"DWO"."DWO_SCHEMA_A_SOURCE"',
        )
        equivalent = resolver.resolve(
            target="DWF.DWF_TARGET",
            program_names=("JOB_DEMO_DAY", "005_DEMO.py"),
            physical_source="DWO.DWO_SCHEMA_A_SOURCE",
        )
        table_only = resolver.resolve(
            target="DWF.DWF_TARGET",
            program_names=("NO_MATCH.py",),
            physical_source="DWO.DWO_SCHEMA_A_SOURCE",
        )
        other_program = resolver.resolve(
            target="DWF.DWF_TARGET",
            program_names=("JOB_OTHER_DAY",),
            physical_source="DWO.DWO_SCHEMA_A_SOURCE",
        )
        other_source = resolver.resolve(
            target="DWF.DWF_TARGET",
            program_names=("005_DEMO.py",),
            physical_source="DWO.DWO_SCHEMA_B_SOURCE",
        )

        self.assertEqual(_signature(first), _signature(equivalent))
        self.assertIs(first, equivalent)
        self.assertEqual(("RESOLVED", None), (table_only.status, table_only.reason))
        self.assertEqual(
            ("CONFLICT", "program_metadata_conflict"),
            (other_program.status, other_program.reason),
        )
        self.assertEqual(
            ("CONFLICT", "schema_match_conflict"),
            (other_source.status, other_source.reason),
        )
        self.assertEqual(4, len(resolver._resolution_cache))

    def test_collector_audit_result_matches_reference_with_repeated_field_resolves(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "DWS_DWF.DWF_DEMO_TARGET"
            project.mkdir()
            (project / "JOB_DEMO_DAY.sql").write_text(
                "INSERT INTO DWF.F_DEMO_TARGET (ID, NAME, CODE) "
                "SELECT s.ID, s.NAME, s.CODE "
                "FROM DWO.DWO_DEMO_SCHEMA_A_B_SOURCE s",
                encoding="utf-8",
            )
            recv = [
                RecvDwfRecord(
                    "PLAN", "DWF.DWF_DEMO_TARGET", "SRC", ods_job_name="JOB_DEMO_DAY"
                )
            ]
            schemas = [SchemaConfigRecord("SRC", "DEMO_SCHEMA_A_B")]
            systems = _systems(("PLAN", 101))
            indexed = MetadataResolver(recv, schemas, systems)
            reference = ReferenceMetadataResolver(recv, schemas, systems)
            # The collector consumes the resolver's precomputed logical-target set;
            # share only this lookup view, not the reference resolve implementation.
            reference.recv_by_logical_target = indexed.recv_by_logical_target

            with contextlib.redirect_stderr(io.StringIO()):
                actual = collect_workspace(root, indexed)
                expected = collect_workspace(root, reference)

        self.assertEqual(expected, actual)
        self.assertEqual(3, actual.summary["resolution"]["resolved_field_mappings"])
        self.assertEqual(1, len(indexed._resolution_cache))

    def test_collector_audit_result_matches_reference_for_unresolved_baselines(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "project-no-program").mkdir()
            no_final = root / "project-no-final"
            no_final.mkdir()
            (no_final / "select.sql").write_text("SELECT 1", encoding="utf-8")

            def add_mapping(project_name, target):
                project = root / project_name
                project.mkdir()
                (project / "mapping.sql").write_text(
                    f"INSERT INTO {target} (ID) SELECT s.ID "
                    "FROM DWO.DWO_DEMO_SCHEMA_A_B_SOURCE s",
                    encoding="utf-8",
                )

            add_mapping("project-no-recv", "DWF.DWF_NOT_IN_METADATA")
            add_mapping("project-no-schema", "DWF.DWF_NO_SCHEMA")
            add_mapping("project-schema-conflict", "DWF.DWF_SCHEMA_CONFLICT")
            add_mapping("project-unknown-system", "DWF.DWF_UNKNOWN_SYSTEM")
            add_mapping("project-resolved", "DWF.DWF_DEMO_TARGET")

            recv = [
                RecvDwfRecord("PLAN", "DWF.F_DEMO_TARGET", "SRC"),
                RecvDwfRecord("PLAN", "DWF.F_NO_SCHEMA", "MISSING"),
                RecvDwfRecord("PLAN", "DWF.F_SCHEMA_CONFLICT", "SRC_BAD"),
                RecvDwfRecord("UNKNOWN", "DWF.F_UNKNOWN_SYSTEM", "SRC_UNKNOWN"),
            ]
            schemas = [
                SchemaConfigRecord("SRC", "DEMO_SCHEMA_A_B"),
                SchemaConfigRecord("PARSER", "DEMO_SCHEMA_A_B"),
                SchemaConfigRecord("SRC_BAD", "OTHER_SCHEMA"),
                SchemaConfigRecord("SRC_UNKNOWN", "DEMO_SCHEMA_A_B"),
            ]
            systems = _systems(("PLAN", 101))
            indexed = MetadataResolver(recv, schemas, systems)
            reference = ReferenceMetadataResolver(recv, schemas, systems)
            reference.recv_by_logical_target = indexed.recv_by_logical_target

            with contextlib.redirect_stderr(io.StringIO()):
                actual = collect_workspace(root, indexed)
                expected = collect_workspace(root, reference)

        self.assertEqual(expected, actual)
        self.assertEqual(1, actual.summary["unresolved"]["no_program"])
        self.assertEqual(1, actual.summary["unresolved"]["no_final_target"])
        self.assertEqual(1, actual.summary["unresolved"]["no_recv_dwf"])
        self.assertEqual(1, actual.summary["unresolved"]["no_schema_config"])
        self.assertEqual(1, actual.summary["unresolved"]["unknown_upstream_system"])
        self.assertEqual(1, actual.summary["conflict"]["schema_match_conflict"])


class MetadataResolverComplexityTests(unittest.TestCase):
    def test_3762_rows_and_1000_repeated_lookups_do_not_rescan_metadata(self):
        metadata_rows = 3762
        resolve_calls = 1000
        recv = [
            RecvDwfRecord(
                "PLAN_0" if index == 0 else f"PLAN_{index}",
                "DWF.F_TARGET" if index == 0 else f"DWF.DWF_TARGET_{index}",
                f"SOURCE_{index}",
                ods_job_name=(
                    "JOB_DEMO_DAY" if index == 0 else f"JOB_OTHER_{index}_DAY"
                ),
            )
            for index in range(metadata_rows)
        ]
        schemas = [SchemaConfigRecord("SOURCE_0", "DEMO_SCHEMA_A")]
        systems = _systems(("PLAN_0", 123))
        args = {
            "target": "DWF.DWF_TARGET",
            "program_names": ("005_DEMO.py", "JOB_DEMO_DAY"),
            "physical_source": "DWO.DWO_DEMO_SCHEMA_A_SOURCE",
        }

        original_program_normalizer = metadata_resolver.normalize_program_name
        original_target_normalizer = metadata_resolver.normalize_logical_target
        program_results = {
            value: original_program_normalizer(value)
            for value in [*(row.ods_job_name for row in recv), *args["program_names"]]
        }
        target_results = {
            value: original_target_normalizer(value)
            for value in [*(row.table_name for row in recv), args["target"]]
        }
        counts = Counter()

        def counted_program_name(value):
            counts["normalize_program_name"] += 1
            return program_results[value]

        def counted_logical_target(value):
            counts["normalize_logical_target"] += 1
            return target_results[value]

        legacy = ReferenceMetadataResolver(recv, schemas, systems)
        with (
            patch.object(metadata_resolver, "normalize_program_name", counted_program_name),
            patch.object(metadata_resolver, "normalize_logical_target", counted_logical_target),
        ):
            expected = None
            for _ in range(resolve_calls):
                expected = legacy.resolve(**args)
            before = counts.copy()

            counts.clear()
            indexed = MetadataResolver(recv, schemas, systems)
            with patch.object(
                indexed, "_resolve_indexed", wraps=indexed._resolve_indexed
            ) as indexed_resolve:
                actual = None
                for _ in range(resolve_calls):
                    actual = indexed.resolve(**args)
            after = counts.copy()

        self.assertEqual(_signature(expected), _signature(actual))
        self.assertEqual(1, indexed_resolve.call_count)
        self.assertEqual(1, len(indexed._resolution_cache))
        self.assertEqual(7_527_000, before["normalize_program_name"])
        self.assertEqual(3_764_000, before["normalize_logical_target"])
        self.assertEqual(5_762, after["normalize_program_name"])
        self.assertEqual(4_762, after["normalize_logical_target"])
        self.assertLess(after["normalize_program_name"], metadata_rows * 2)
        self.assertLess(after["normalize_logical_target"], metadata_rows * 2)


class FieldMappingCliCompatibilityAndProgressTests(unittest.TestCase):
    def test_dap_client_does_not_import_or_annotate_typing_self(self):
        source_path = (
            Path(__file__).resolve().parents[2]
            / "tools"
            / "field_mapping"
            / "dap_client.py"
        )
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        self.assertTrue(
            any(
                isinstance(node, ast.ImportFrom)
                and node.module == "__future__"
                and any(alias.name == "annotations" for alias in node.names)
                for node in tree.body
            )
        )
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "typing":
                self.assertNotIn("Self", {alias.name for alias in node.names})
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "Self"
                and isinstance(node.value, ast.Name)
                and node.value.id == "typing"
            ):
                self.fail("typing.Self is not available in Python 3.10")
        enter = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "__enter__"
        )
        self.assertIsInstance(enter.returns, ast.Name)
        self.assertEqual("FieldMappingApiClient", enter.returns.id)

    def test_progress_every_defaults_to_100_and_is_configurable(self):
        required = ["--directory", "src", "--metadata-json", "metadata.json"]
        self.assertEqual(
            100, entry.build_argument_parser().parse_args(required).progress_every
        )
        self.assertEqual(
            25,
            entry.build_argument_parser()
            .parse_args([*required, "--progress-every", "25"])
            .progress_every,
        )

    def test_benchmark_helper_reports_counters_summary_and_audit_digest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "DWS_DWF.DWF_DEMO_TARGET"
            project.mkdir()
            (project / "mapping.sql").write_text(
                "INSERT INTO DWF.F_DEMO_TARGET (ID) "
                "SELECT s.ID FROM DWO.DWO_SCHEMA_A_SOURCE s",
                encoding="utf-8",
            )
            metadata_path = root / "metadata.json"
            metadata_path.write_text(
                '{"recv_dwf":[{"recv_plan":"PLAN","table_name":"DWF.DWF_DEMO_TARGET",'
                '"data_source":"SRC","ods_job_name":""}],'
                '"schema_config":[{"schema_key":"SRC","db_schema":"SCHEMA_A"}]}',
                encoding="utf-8",
            )
            upstreams_path = root / "upstreams.json"
            upstreams_path.write_text(
                '{"items":[{"id":"PLAN","upstreamSystemId":101}]}',
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                code = benchmark_resolver.main(
                    [
                        "--repository-root",
                        str(Path(__file__).resolve().parents[2]),
                        "--directory",
                        str(root),
                        "--metadata-json",
                        str(metadata_path),
                        "--upstreams-json",
                        str(upstreams_path),
                    ]
                )

        result = json.loads(stdout.getvalue())
        self.assertEqual(0, code)
        self.assertEqual(1, result["resolve_calls"])
        self.assertEqual(1, result["audit_summary"]["resolution"]["resolved_field_mappings"])
        self.assertEqual(64, len(result["audit_result_sha256"]))
        self.assertIn("[collector] projects=1/1", stderr.getvalue())

    def test_collector_emits_only_bounded_project_progress(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in ("project-a", "project-b", "project-c"):
                (root / name).mkdir()
            resolver = MetadataResolver([], [], _systems())
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                audit = collect_workspace(root, resolver, progress_every=2)

        lines = stderr.getvalue().splitlines()
        self.assertEqual(2, len(lines))
        self.assertRegex(lines[0], r"^\[collector\] projects=2/3 elapsed=\d+\.\d+s$")
        self.assertRegex(lines[1], r"^\[collector\] projects=3/3 elapsed=\d+\.\d+s$")
        self.assertEqual(3, audit.summary["unresolved"]["no_program"])


if __name__ == "__main__":
    unittest.main()
