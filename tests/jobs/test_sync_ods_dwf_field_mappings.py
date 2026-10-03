from __future__ import annotations

import contextlib
import io
import json
import secrets
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from jobs.crontab import sync_ods_dwf_field_mappings as entry
from tools.field_mapping.collector import collect_workspace
from tools.field_mapping.dap_client import (
    FieldMappingApiClient,
    FieldMappingApiError,
    build_import_payload,
    is_local_write_url,
    split_batches,
    submit_batches,
)
from tools.field_mapping.metadata_resolver import (
    MetadataResolver,
    canonical_dwf_target,
    derive_recv_namespace,
    match_dwo_source,
    normalize_logical_target,
    normalize_program_name,
)
from tools.field_mapping.models import (
    MappingField,
    MappingItem,
    RecvDwfRecord,
    SchemaConfigRecord,
)

PROGRAM = "005_DWS_DWF_DWF_DEMO_SOURCE_TABLE_00.py"
JOB = "JOB_DWS_DWS_DWF_DWF_DEMO_SOURCE_TABLE_00_DAY"
TARGET = "DWF.DWF_DEMO_SOURCE_TABLE"
DEMO_RECV_PLAN = "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY"


def _resolver(
    recv: list[RecvDwfRecord] | None = None,
    schemas: list[SchemaConfigRecord] | None = None,
    systems: list[dict[str, object]] | None = None,
) -> MetadataResolver:
    return MetadataResolver(
        recv
        if recv is not None
        else [RecvDwfRecord(DEMO_RECV_PLAN, TARGET, "DEMO_SYSTEM_A", ods_job_name=JOB)],
        schemas
        if schemas is not None
        else [SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A")],
        {
            "items": systems
            if systems is not None
            else [{"id": DEMO_RECV_PLAN, "upstreamSystemId": 106}]
        },
    )


def _write_project(root: Path, files: dict[str, str]) -> Path:
    project = root / "DWS_DWF.DWF_DEMO_SOURCE_TABLE"
    project.mkdir(parents=True)
    for filename, content in files.items():
        (project / filename).write_text(content, encoding="utf-8")
    return project


def _item(
    source: str = "DEMO_SOURCE_TABLE",
    *,
    target: str = "DWF_DEMO_SOURCE_TABLE",
    system: int = 106,
) -> MappingItem:
    return MappingItem(
        source_system_identity="DEMO_SYSTEM_A",
        source_system_id=system,
        source_table=source,
        target_table=target,
        fields=(
            MappingField(
                source_field="ID",
                target_field="ID",
                mapping_rule="DIRECT",
                field_order=1,
                physical_source_table="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
                db_schema="DEMO_SCHEMA_A",
                program=PROGRAM,
            ),
        ),
    )


def _import_response(
    items: list[MappingItem], action: str = "created", *, dry_run: bool = False
) -> dict[str, object]:
    fields = sum(len(item.fields) for item in items)
    return {
        "mode": "upsert",
        "dryRun": dry_run,
        "summary": {
            "received": len(items),
            "created": len(items) if action == "created" else 0,
            "updated": len(items) if action == "updated" else 0,
            "unchanged": len(items) if action == "unchanged" else 0,
            "failed": len(items) if action == "failed" else 0,
            "fieldCount": fields,
        },
        "items": [
            {
                "index": index,
                "identity": {
                    "sourceSystemId": item.source_system_id,
                    "sourceTable": item.source_table,
                    "targetLayer": "DWF",
                    "targetTable": item.target_table,
                },
                "action": action,
                "fieldCount": len(item.fields),
                **(
                    {"error": {"code": "INVALID_FIELD", "message": "invalid mapping"}}
                    if action == "failed"
                    else {}
                ),
            }
            for index, item in enumerate(items)
        ],
    }


def _metadata_json(
    path: Path,
    recv: list[dict[str, str]] | None = None,
    schemas: list[dict[str, str]] | None = None,
) -> Path:
    path.write_text(
        json.dumps(
            {
                "recv_dwf": recv
                or [
                    {
                        "recv_plan": DEMO_RECV_PLAN,
                        "table_name": TARGET,
                        "data_source": "DEMO_SYSTEM_A",
                        "ods_job_name": JOB,
                    }
                ],
                "schema_config": schemas
                or [{"schema_key": "DEMO_SYSTEM_A", "db_schema": "DEMO_SCHEMA_A"}],
            }
        ),
        encoding="utf-8",
    )
    return path


class MetadataResolutionTests(unittest.TestCase):
    def test_historical_dwf_names_normalize_to_one_logical_target(self):
        self.assertEqual(
            "DEMO_SOURCE_TABLE", normalize_logical_target("DWF.F_DEMO_SOURCE_TABLE")
        )
        self.assertEqual(
            "DEMO_SOURCE_TABLE", normalize_logical_target("DWF.DWF_DEMO_SOURCE_TABLE")
        )
        self.assertEqual(
            "DWF_DEMO_SOURCE_TABLE", canonical_dwf_target("DWF.F_DEMO_SOURCE_TABLE")
        )
        self.assertEqual(
            "DWF_DEMO_SOURCE_TABLE", canonical_dwf_target("DWF.DWF_DEMO_SOURCE_TABLE")
        )

    def test_python_filename_and_ods_job_normalize_to_same_identity(self):
        self.assertEqual(normalize_program_name(PROGRAM), normalize_program_name(JOB))

    def test_recv_plan_resolves_to_current_dap_system_id_not_a_constant(self):
        resolver = _resolver(
            systems=[{"id": DEMO_RECV_PLAN, "upstreamSystemId": 999}]
        )
        result = resolver.resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual("RESOLVED", result.status)
        self.assertEqual(DEMO_RECV_PLAN, result.record.recv_plan)
        self.assertEqual(999, result.upstream_system_id)
        mapping = _item(system=result.upstream_system_id)
        self.assertEqual(
            ("demo_system_a", "demo_source_table", "dwf_demo_source_table"),
            mapping.identity,
        )

    def test_recv_namespace_identity_and_schema_config_audit_metadata(self):
        result = _resolver().resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual("DEMO_SCHEMA_A", result.source.recv_namespace)
        self.assertEqual("DEMO_SOURCE_TABLE", result.source.source_table)
        # p_schema_config.db_schema is source-side metadata, not DWO identity.
        self.assertEqual("DEMO_SCHEMA_A", result.source.db_schema)

    def test_missing_schema_config_does_not_block_recv_namespace_identity(self):
        result = _resolver(schemas=[]).resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual("RESOLVED", result.status)
        self.assertEqual("DEMO_SCHEMA_A", result.source.recv_namespace)
        self.assertIsNone(result.source.db_schema)

    def test_ambiguous_db_schema_config_is_never_picked_arbitrarily(self):
        result = _resolver(
            schemas=[
                SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A"),
                SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_OTHER"),
            ]
        ).resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual("RESOLVED", result.status)
        self.assertIsNone(result.source.db_schema)

    def test_recv_namespace_derivation_matches_full_data_source(self):
        self.assertEqual(
            "DEMO_SCHEMA_A",
            derive_recv_namespace(DEMO_RECV_PLAN, "DEMO_SYSTEM_A"),
        )
        self.assertIsNone(derive_recv_namespace("DEMO_SYSTEM_A", "DEMO_SYSTEM_A"))

    def test_match_dwo_source_requires_namespace_token_boundary(self):
        source = match_dwo_source(
            "DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE", "DEMO_SCHEMA_A"
        )
        self.assertEqual("DEMO_SOURCE_TABLE", source.source_table)
        self.assertIsNone(
            match_dwo_source("DWO.DWO_DEMO_SCHEMA_AX_DEMO_SOURCE_TABLE", "DEMO_SCHEMA_A")
        )

    def test_one_dwf_target_can_resolve_multiple_dwo_sources_and_systems(self):
        recv = [
            RecvDwfRecord(
                DEMO_RECV_PLAN, TARGET, "DEMO_SYSTEM_A", ods_job_name=JOB
            ),
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_B_DEMO_SYSTEM_B_DAY",
                "DWF.F_DEMO_SOURCE_TABLE",
                "DEMO_SYSTEM_B",
                ods_job_name=JOB,
            ),
        ]
        resolver = _resolver(
            recv,
            [
                SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A"),
                SchemaConfigRecord("DEMO_SYSTEM_B", "DEMO_SCHEMA_B"),
            ],
            [
                {"id": DEMO_RECV_PLAN, "upstreamSystemId": 106},
                {
                    "id": "PLAN_SA_RECV_DEMO_SCHEMA_B_DEMO_SYSTEM_B_DAY",
                    "upstreamSystemId": 244,
                },
            ],
        )
        first = resolver.resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        second = resolver.resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_B_DEMO_SOURCE_TABLE_B",
        )
        self.assertEqual(
            ("RESOLVED", 106, "DEMO_SOURCE_TABLE", "DEMO_SCHEMA_A"),
            (
                first.status,
                first.upstream_system_id,
                first.source.source_table,
                first.source.recv_namespace,
            ),
        )
        self.assertEqual(
            ("RESOLVED", 244, "DEMO_SOURCE_TABLE_B", "DEMO_SCHEMA_B"),
            (
                second.status,
                second.upstream_system_id,
                second.source.source_table,
                second.source.recv_namespace,
            ),
        )

    def test_program_mismatch_does_not_block_recv_namespace_identity(self):
        resolver = _resolver(
            [
                RecvDwfRecord(
                    DEMO_RECV_PLAN,
                    TARGET,
                    "DEMO_SYSTEM_A",
                    ods_job_name="JOB_FOR_OTHER_TABLE",
                )
            ],
            [SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A")],
            [{"id": DEMO_RECV_PLAN, "upstreamSystemId": 1}],
        )
        result = resolver.resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual(
            ("RESOLVED", None), (result.status, result.reason)
        )
        self.assertNotIn("ods_job_name", result.evidence)

    def test_multiple_recv_namespace_identities_are_conflicts(self):
        rows = [
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_A_SOURCE_A_DAY",
                TARGET,
                "SOURCE_A",
                ods_job_name="OTHER",
            ),
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_A_SOURCE_B_DAY",
                TARGET,
                "SOURCE_B",
                ods_job_name="OTHER",
            ),
        ]
        resolver = _resolver(rows, [], [])
        result = resolver.resolve(
            target=TARGET,
            program_names=["unmatched.py"],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual("multiple_recv_namespace_conflict", result.reason)

        same_plan = [
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY",
                TARGET,
                "DEMO_SYSTEM_A",
                ods_job_name="OTHER",
            ),
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_PRO",
                TARGET,
                "DEMO_SYSTEM_A",
                ods_job_name="OTHER",
            ),
        ]
        resolver = _resolver(same_plan, [], [])
        result = resolver.resolve(
            target=TARGET,
            program_names=["unmatched.py"],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual("multiple_recv_namespace_conflict", result.reason)

    def test_no_recv_namespace_match_and_unknown_system_are_unresolved(self):
        resolver = _resolver(
            [
                RecvDwfRecord(
                    "PLAN_SA_RECV_OTHER_DEMO_SYSTEM_A_DAY",
                    TARGET,
                    "DEMO_SYSTEM_A",
                    ods_job_name=JOB,
                )
            ],
            [SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A")],
            [{"id": "PLAN_SA_RECV_OTHER_DEMO_SYSTEM_A_DAY", "upstreamSystemId": 106}],
        )
        result = resolver.resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual(
            ("UNRESOLVED", "no_recv_namespace_match"),
            (result.status, result.reason),
        )
        unknown = _resolver(systems=[]).resolve(
            target=TARGET,
            program_names=[PROGRAM],
            physical_source="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
        )
        self.assertEqual(
            ("UNRESOLVED", "unknown_upstream_system"), (unknown.status, unknown.reason)
        )


class SqlProjectionTests(unittest.TestCase):
    def _collect(self, files: dict[str, str], resolver: MetadataResolver | None = None):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        _write_project(Path(temp.name), files)
        return collect_workspace(temp.name, resolver or _resolver())

    def test_multi_python_project_and_tmp_chain_project_to_final_dwf(self):
        audit = self._collect(
            {
                "stage.py": (
                    "def run(execute):\n"
                    '    execute("INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE_TMP (ID) '
                    'SELECT s.ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE s")\n'
                ),
                PROGRAM: (
                    "def run(execute):\n"
                    '    execute("INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID) '
                    'SELECT t.ID FROM DWF.DWF_DEMO_SOURCE_TABLE_TMP t")\n'
                ),
            }
        )
        self.assertEqual(1, audit.summary["project_count"])
        self.assertEqual(2, audit.summary["python_file_count"])
        self.assertEqual(1, audit.summary["dwo_physical_table_count"])
        self.assertEqual(1, len(audit.items))
        item = audit.items[0]
        self.assertEqual(
            ("DEMO_SOURCE_TABLE", "DWF_DEMO_SOURCE_TABLE"),
            (item.source_table, item.target_table),
        )
        self.assertEqual(
            "DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
            audit.resolved[0]["physicalSourceTable"],
        )

    def test_alias_cast_case_and_coalesce_keep_all_upstream_fields(self):
        audit = self._collect(
            {
                "program.sql": (
                    "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE "
                    "(ID, CAST_ID, FULL_NAME, LABEL) SELECT "
                    "s.ID AS alias_id, CAST(s.ID AS CHAR), "
                    "COALESCE(s.FIRST_NAME, s.LAST_NAME), "
                    "CASE WHEN s.FLAG = 1 THEN s.ID ELSE s.OTHER_ID END "
                    "FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE s"
                )
            }
        )
        self.assertEqual(1, len(audit.items))
        mappings = {
            (item.source_field, item.target_field): item
            for item in audit.items[0].fields
        }
        self.assertEqual("DIRECT", mappings[("ID", "ID")].mapping_rule)
        self.assertEqual("待补充", mappings[("ID", "CAST_ID")].mapping_rule)
        self.assertIn(("FIRST_NAME", "FULL_NAME"), mappings)
        self.assertIn(("LAST_NAME", "FULL_NAME"), mappings)
        self.assertEqual(
            {"FLAG", "ID", "OTHER_ID"},
            {source for source, target in mappings if target == "LABEL"},
        )
        self.assertEqual(
            0, audit.summary["unresolved"]["multi_source_field_unsupported"]
        )
        build_import_payload(audit.items)

    def test_join_and_cte_are_resolved_to_dwo_leaf_sources(self):
        recv = [
            RecvDwfRecord(
                DEMO_RECV_PLAN, TARGET, "DEMO_SYSTEM_A", ods_job_name=JOB
            ),
            RecvDwfRecord(
                "PLAN_SA_RECV_DEMO_SYSTEM_B_DEMO_SYSTEM_B_DAY",
                TARGET,
                "DEMO_SYSTEM_B",
                ods_job_name=JOB,
            ),
        ]
        resolver = _resolver(
            recv,
            [
                SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A"),
                SchemaConfigRecord("DEMO_SYSTEM_B", "DEMO_SYSTEM_B"),
            ],
            [
                {"id": DEMO_RECV_PLAN, "upstreamSystemId": 106},
                {
                    "id": "PLAN_SA_RECV_DEMO_SYSTEM_B_DEMO_SYSTEM_B_DAY",
                    "upstreamSystemId": 207,
                },
            ],
        )
        audit = self._collect(
            {
                PROGRAM: (
                    "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID, DEMO_CONTACT_NAME) "
                    "WITH customers AS (SELECT c.ID, c.DEMO_CONTACT_ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE c) "
                    "SELECT customers.ID, r.NAME FROM customers "
                    "JOIN DWO.DWO_DEMO_SYSTEM_B_DEMO_CONTACT r ON customers.DEMO_CONTACT_ID = r.ID"
                )
            },
            resolver,
        )
        by_system = {item.source_system_id: item for item in audit.items}
        self.assertEqual({106, 207}, set(by_system))
        self.assertEqual("DEMO_SOURCE_TABLE", by_system[106].source_table)
        self.assertEqual("DEMO_CONTACT", by_system[207].source_table)
        self.assertEqual("DEMO_CONTACT_NAME", by_system[207].fields[0].target_field)

    def test_quoted_identifiers_and_case_are_normalized(self):
        audit = self._collect(
            {
                "quoted.sql": (
                    "INSERT INTO `dwf`.`DWF_DEMO_SOURCE_TABLE` (`TargetId`) "
                    "SELECT `s`.`Id` FROM `dwo`.`DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE` AS `s`"
                )
            }
        )
        self.assertEqual("ID", audit.items[0].fields[0].source_field)
        self.assertEqual("TARGETID", audit.items[0].fields[0].target_field)

    def test_merge_and_ambiguous_expression_are_unsupported_not_guessed(self):
        merge = self._collect(
            {
                PROGRAM: (
                    "MERGE INTO DWF.DWF_DEMO_SOURCE_TABLE t USING DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE s "
                    "ON t.ID=s.ID WHEN MATCHED THEN UPDATE SET t.ID=s.ID"
                )
            }
        )
        self.assertEqual((), merge.items)
        self.assertTrue(
            any(row["reason"] == "unsupported_sql" for row in merge.unresolved)
        )
        ambiguous = self._collect(
            {
                "expr.sql": (
                    "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID) "
                    "SELECT x.ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE x "
                    "JOIN DWO.DWO_DEMO_SCHEMA_A_OTHER_TABLE y ON x.ID=y.ID"
                )
            }
        )
        # The qualified expression resolves. An unqualified joined field does not.
        self.assertEqual(1, len(ambiguous.items))
        unqualified = self._collect(
            {
                "expr.sql": (
                    "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID) "
                    "SELECT ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE x "
                    "JOIN DWO.DWO_DEMO_SCHEMA_A_OTHER_TABLE y ON x.ID=y.ID"
                )
            }
        )
        self.assertEqual((), unqualified.items)
        self.assertTrue(
            any("ambiguous" in row["detail"] for row in unqualified.unresolved)
        )

    def test_resolved_audit_lists_only_evidence_that_was_actually_matched(self):
        resolver = _resolver(
            [RecvDwfRecord(DEMO_RECV_PLAN, TARGET, "DEMO_SYSTEM_A")],
            [SchemaConfigRecord("DEMO_SYSTEM_A", "DEMO_SCHEMA_A")],
            [{"id": DEMO_RECV_PLAN, "upstreamSystemId": 106}],
        )
        audit = self._collect(
            {
                "mapping.sql": (
                    "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID) "
                    "SELECT s.ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE s"
                )
            },
            resolver,
        )

        self.assertEqual(1, len(audit.items))
        self.assertEqual(
            "table_name;recv_namespace;dap_upstream_system",
            audit.resolved[0]["evidence"],
        )

    def test_conflicting_field_order_is_not_silently_selected(self):
        audit = self._collect(
            {
                "first.sql": (
                    "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID, NAME) "
                    "SELECT s.ID, s.NAME FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE s"
                ),
                "second.sql": (
                    "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (NAME, ID) "
                    "SELECT s.NAME, s.ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE s"
                ),
            }
        )

        self.assertEqual((), audit.items)
        self.assertTrue(
            any(row["reason"] == "field_mapping_conflict" for row in audit.conflicts)
        )

    def test_metadata_conflict_never_enters_resolved_payload(self):
        resolver = _resolver(
            [
                RecvDwfRecord(
                    "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY",
                    TARGET,
                    "DEMO_SYSTEM_A",
                    ods_job_name="OTHER",
                ),
                RecvDwfRecord(
                    "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_PRO",
                    TARGET,
                    "DEMO_SYSTEM_A",
                    ods_job_name="OTHER",
                ),
            ],
            [],
            [
                {"id": "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_DAY", "upstreamSystemId": 1},
                {"id": "PLAN_SA_RECV_DEMO_SCHEMA_A_DEMO_SYSTEM_A_PRO", "upstreamSystemId": 2},
            ],
        )
        audit = self._collect(
            {
                "x.sql": "INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID) SELECT x.ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE x"
            },
            resolver,
        )
        self.assertEqual((), audit.items)
        self.assertTrue(
            any(
                row["reason"] == "multiple_recv_namespace_conflict"
                for row in audit.conflicts
            )
        )


class BatchAndApiTests(unittest.TestCase):
    class Response:
        def __init__(self, status: int, body: dict[str, object]):
            self.status_code = status
            self._body = body

        def json(self):
            return self._body

    class Session:
        def __init__(self, responses):
            self.responses = list(responses)
            self.calls = []
            self.headers = {}

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            result = self.responses.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result

        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            result = self.responses.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result

        def close(self):
            pass

    def test_batch_size_100_and_contract_allows_multiple_sources_per_target(self):
        items = [_item(f"SRC_{index}") for index in range(201)]
        self.assertEqual(
            [100, 100, 1], [len(batch) for batch in split_batches(items, 100)]
        )
        two_sources = _item("SOURCE_A")
        second_field = MappingField(
            source_field="OTHER_ID",
            target_field="ID",
            mapping_rule="待补充",
            field_order=1,
            physical_source_table="DWO.DWO_DEMO_SCHEMA_A_OTHER",
            db_schema="DEMO_SCHEMA_A",
            program=PROGRAM,
        )
        from tools.field_mapping.models import MappingItem as TableMapping

        second_table = TableMapping(
            "DEMO_SCHEMA_A_B", 244, "SOURCE_B", "DWF_DEMO_SOURCE_TABLE", (second_field,)
        )
        payload = build_import_payload([two_sources, second_table])
        self.assertEqual(2, len(payload["items"]))
        self.assertEqual("upsert", payload["mode"])

    def test_default_100_batch_splits_4000_mappings_into_40_serial_batches(self):
        batches = split_batches([_item(f"T{index}") for index in range(4_000)], 100)
        self.assertEqual(40, len(batches))
        self.assertTrue(all(len(batch) == 100 for batch in batches))

    def test_retryable_502_503_504_and_connection_timeout_are_bounded(self):
        items = [_item()]
        success = self.Response(200, _import_response(items, dry_run=True))
        session = self.Session(
            [
                self.Response(503, {}),
                self.Response(502, {}),
                self.Response(504, {}),
                success,
            ]
        )
        sleep = Mock()
        client = FieldMappingApiClient(
            "http://dap.example.test",
            session_cookie="sid=secret",
            session=session,
            max_retries=3,
            retry_backoff=0.25,
            sleep=sleep,
        )
        payload = build_import_payload(items, dry_run=True)
        self.assertEqual(
            "created", client.import_mappings(payload)["items"][0]["action"]
        )
        self.assertEqual(4, len(session.calls))
        self.assertEqual(
            [0.25, 0.5, 1.0], [call.args[0] for call in sleep.call_args_list]
        )

        session = self.Session([requests.exceptions.Timeout(), success])
        client = FieldMappingApiClient(
            "http://dap.example.test",
            session_cookie="sid=x",
            session=session,
            max_retries=1,
            sleep=Mock(),
        )
        self.assertEqual(
            "created", client.import_mappings(payload)["items"][0]["action"]
        )
        self.assertEqual(2, len(session.calls))

    def test_upstream_systems_pagination_uses_server_effective_page_size(self):
        session = self.Session(
            [
                self.Response(
                    200,
                    {
                        "items": [
                            {"id": "A", "upstreamSystemId": 11},
                            {"id": "B", "upstreamSystemId": 12},
                        ],
                        "page": 1,
                        "pageSize": 2,
                        "total": 3,
                    },
                ),
                self.Response(
                    200,
                    {
                        "items": [{"id": "C", "upstreamSystemId": 13}],
                        "page": 2,
                        "pageSize": 2,
                        "total": 3,
                    },
                ),
            ]
        )
        client = FieldMappingApiClient("http://dap.example.test", session=session)
        systems = client.get_upstream_systems()
        self.assertEqual(3, len(systems["items"]))
        self.assertEqual(2, len(session.calls))

    def test_login_accepts_runtime_password_without_echoing_it(self):
        session = self.Session(
            [
                self.Response(200, {"message": "ok"}),
                self.Response(
                    200,
                    {
                        "data": {
                            "user": "uat-user",
                            "permissions": ["field_mapping:write"],
                        }
                    },
                ),
            ]
        )
        client = FieldMappingApiClient("http://dap.example.test", session=session)
        password = secrets.token_urlsafe(24)
        self.assertEqual("uat-user", client.login("uat-user", password)["data"]["user"])
        self.assertEqual(password, session.calls[0][1]["json"]["password"])

    def test_400_401_403_are_not_retried(self):
        for status in (400, 401, 403):
            session = self.Session(
                [
                    self.Response(
                        status,
                        {"error": {"code": f"HTTP_{status}", "message": "rejected"}},
                    )
                ]
            )
            client = FieldMappingApiClient(
                "http://dap.example.test",
                session_cookie="sid=sensitive",
                session=session,
                max_retries=4,
            )
            with self.subTest(status=status), self.assertRaises(FieldMappingApiError):
                client.import_mappings(build_import_payload([_item()], dry_run=True))
            self.assertEqual(1, len(session.calls))

    def test_replay_reports_unchanged_and_localhost_guard_blocks_remote_real_write(
        self,
    ):
        item = _item()
        unchanged = self.Response(
            200, _import_response([item], "unchanged", dry_run=True)
        )
        session = self.Session([unchanged, unchanged])
        client = FieldMappingApiClient(
            "http://dap.example.test", session_cookie="sid=x", session=session
        )
        first = submit_batches([item], client, batch_size=100, dry_run=True)
        second = submit_batches([item], client, batch_size=100, dry_run=True)
        self.assertEqual((1, 0), (first["unchanged"], first["failed"]))
        self.assertEqual((1, 0), (second["unchanged"], second["failed"]))
        self.assertEqual(2, len(session.calls))

        changed_field = MappingField(
            source_field="ID",
            target_field="ID",
            mapping_rule="待补充",
            field_order=1,
            physical_source_table="DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE",
            db_schema="DEMO_SCHEMA_A",
            program=PROGRAM,
        )
        changed_item = MappingItem(
            item.source_system_identity,
            item.source_system_id,
            item.source_table,
            item.target_table,
            (changed_field,),
        )
        self.assertEqual(item.dap_identity, changed_item.dap_identity)
        self.assertNotEqual(
            build_import_payload([item]), build_import_payload([changed_item])
        )
        updated_response = self.Response(
            200, _import_response([changed_item], "updated", dry_run=True)
        )
        updated_client = FieldMappingApiClient(
            "http://dap.example.test",
            session_cookie="sid=x",
            session=self.Session([updated_response]),
        )
        updated = submit_batches(
            [changed_item], updated_client, batch_size=100, dry_run=True
        )
        self.assertEqual((1, 0), (updated["updated"], updated["failed"]))
        remote = FieldMappingApiClient(
            "https://dap.example.test", session_cookie="sid=x", session=self.Session([])
        )
        with self.assertRaisesRegex(ValueError, "localhost"):
            remote.import_mappings(build_import_payload([item], dry_run=False))
        self.assertTrue(is_local_write_url("http://localhost:15099"))
        self.assertTrue(is_local_write_url("http://127.0.0.1:15099"))
        self.assertFalse(is_local_write_url("http://0.0.0.0:15099"))
        self.assertFalse(is_local_write_url("https://dap.example.test"))
        self.assertFalse(is_local_write_url("http://[invalid"))

    def test_local_real_write_bypasses_environment_proxies_and_redirects(self):
        item = _item()
        session = self.Session([self.Response(200, _import_response([item]))])
        session.trust_env = True
        client = FieldMappingApiClient(
            "http://localhost:15099", session_cookie="sid=x", session=session
        )

        client.import_mappings(build_import_payload([item], dry_run=False))

        self.assertFalse(session.trust_env)
        self.assertFalse(session.calls[0][1]["allow_redirects"])

    def test_item_level_api_failure_is_recorded_without_retrying_other_items(self):
        item = _item()
        response = self.Response(200, _import_response([item], "failed", dry_run=True))
        client = FieldMappingApiClient(
            "http://dap.example.test",
            session_cookie="sid=x",
            session=self.Session([response]),
        )
        result = submit_batches([item], client, batch_size=100, dry_run=True)
        self.assertEqual(1, result["failed"])
        self.assertEqual("INVALID_FIELD", result["failedItems"][0]["errorCode"])

    def test_partial_batch_failure_is_recorded_without_losing_retry_context(self):
        item = _item()

        class BrokenClient:
            def import_mappings(self, payload):
                raise FieldMappingApiError("HTTP_503", "temporary", retryable=True)

            @staticmethod
            def redact(value):
                return value

        result = submit_batches([item], BrokenClient(), batch_size=100, dry_run=True)
        self.assertEqual(1, result["failed"])
        self.assertEqual("DEMO_SOURCE_TABLE", result["failedItems"][0]["sourceTable"])


class RunModeTests(unittest.TestCase):
    class FakeClient:
        def __init__(self, action: str = "created", fail: bool = False):
            self.action = action
            self.fail = fail
            self.payloads = []
            self.closed = False

        def get_upstream_systems(self):
            return {"items": [{"id": DEMO_RECV_PLAN, "upstreamSystemId": 106}]}

        def import_mappings(self, payload):
            self.payloads.append(payload)
            items = []
            for index, raw in enumerate(payload["items"]):
                items.append(
                    {
                        "index": index,
                        "identity": {
                            "sourceSystemId": raw["sourceSystemId"],
                            "sourceTable": raw["sourceTable"],
                            "targetLayer": raw["targetLayer"],
                            "targetTable": raw["targetTable"],
                        },
                        "action": self.action,
                        "fieldCount": len(raw["fields"]),
                    }
                )
            return {
                "mode": "upsert",
                "dryRun": payload["dryRun"],
                "summary": {
                    "received": len(items),
                    "created": int(self.action == "created") * len(items),
                    "updated": int(self.action == "updated") * len(items),
                    "unchanged": int(self.action == "unchanged") * len(items),
                    "failed": 0,
                    "fieldCount": sum(item["fieldCount"] for item in items),
                },
                "items": items,
            }

        def get_mapping_stats(self):
            return {"data": {"sourceTableCount": 1, "fieldCount": 1}}

        def get_mapping_tables(self):
            return [{"srcTable": "DEMO_SOURCE_TABLE"}]

        def get_mapping_fields(self):
            return [{"srcField": "ID"}]

        def close(self):
            self.closed = True

    def _inputs(self, directory: Path) -> tuple[Path, Path]:
        source_root = directory / "source"
        _write_project(
            source_root,
            {
                PROGRAM: (
                    "def run(execute):\n"
                    '    execute("INSERT INTO DWF.DWF_DEMO_SOURCE_TABLE (ID) '
                    'SELECT s.ID FROM DWO.DWO_DEMO_SCHEMA_A_DEMO_SOURCE_TABLE s")\n'
                )
            },
        )
        metadata = _metadata_json(directory / "metadata.json")
        upstreams = directory / "upstreams.json"
        upstreams.write_text(
            json.dumps({"items": [{"id": DEMO_RECV_PLAN, "upstreamSystemId": 106}]}),
            encoding="utf-8",
        )
        return source_root, metadata

    def test_local_dry_run_writes_audit_bundle_and_never_creates_api_client(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, metadata = self._inputs(root)
            upstreams = root / "upstreams.json"
            report_root = root / "reports"
            stdout = io.StringIO()
            with (
                patch.object(
                    entry,
                    "_runtime_client",
                    side_effect=AssertionError("must not create DAP client"),
                ),
                contextlib.redirect_stdout(stdout),
            ):
                code = entry.main(
                    [
                        "--directory",
                        str(source),
                        "--metadata-json",
                        str(metadata),
                        "--upstreams-json",
                        str(upstreams),
                        "--mode",
                        "local-dry-run",
                        "--report-root",
                        str(report_root),
                        "--include-payload",
                    ]
                )
            self.assertEqual(0, code)
            report = next(report_root.iterdir())
            self.assertTrue((report / "summary.json").is_file())
            self.assertTrue((report / "resolved.csv").is_file())
            self.assertTrue((report / "unresolved.csv").is_file())
            self.assertTrue((report / "conflicts.csv").is_file())
            self.assertTrue((report / "payload.json").is_file())
            self.assertIn('"resolved_field_mappings": 1', stdout.getvalue())

    def test_server_dry_run_submits_only_dryrun_true(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, metadata = self._inputs(root)
            fake = self.FakeClient()
            with patch.object(entry, "_runtime_client", return_value=fake):
                code = entry.main(
                    [
                        "--directory",
                        str(source),
                        "--metadata-json",
                        str(metadata),
                        "--api-base-url",
                        "http://dap.example.test",
                        "--mode",
                        "server-dry-run",
                        "--report-root",
                        str(root / "reports"),
                    ]
                )
            self.assertEqual(0, code)
            self.assertEqual([True], [payload["dryRun"] for payload in fake.payloads])
            self.assertTrue(fake.closed)

    def test_real_sync_on_localhost_upserts_and_reconciles_dap_stats(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source, metadata = self._inputs(root)
            fake = self.FakeClient()
            with patch.object(entry, "_runtime_client", return_value=fake):
                code = entry.main(
                    [
                        "--directory",
                        str(source),
                        "--metadata-json",
                        str(metadata),
                        "--api-base-url",
                        "http://127.0.0.1:15099",
                        "--mode",
                        "real-sync",
                        "--report-root",
                        str(root / "reports"),
                    ]
                )
            self.assertEqual(0, code)
            self.assertEqual([False], [payload["dryRun"] for payload in fake.payloads])
            self.assertTrue(fake.closed)
            summary = json.loads(
                next((root / "reports").iterdir()).joinpath("summary.json").read_text()
            )
            self.assertTrue(summary["dap_reconciliation"]["fields_match_stats"])

    def test_non_local_real_sync_is_rejected_before_api_client_or_scan(self):
        with patch.object(
            entry, "_runtime_client", side_effect=AssertionError("must be blocked")
        ):
            code = entry.main(
                [
                    "--directory",
                    "/missing",
                    "--metadata-json",
                    "/missing.json",
                    "--api-base-url",
                    "https://dap.example.test",
                    "--mode",
                    "real-sync",
                ]
            )
        self.assertEqual(2, code)


if __name__ == "__main__":
    unittest.main()
