from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from jobs.crontab import sync_ods_dwf_field_mappings as collector


def _item(source_table: str, source_system_id: int = 101) -> collector.MappingItem:
    return collector.MappingItem(
        source_system_id=source_system_id,
        source_table=source_table,
        target_table="DWF.DEMO_TARGET",
        fields=(
            collector.MappingField(
                source_field="ID",
                target_field="ID",
                mapping_rule="DIRECT",
                field_order=1,
            ),
        ),
    )


def _response(actions: list[str]) -> dict[str, object]:
    return {
        "mode": "upsert",
        "dryRun": False,
        "summary": {
            "received": len(actions),
            "created": actions.count("created"),
            "updated": actions.count("updated"),
            "unchanged": actions.count("unchanged"),
            "failed": actions.count("failed"),
            "fieldCount": len(actions),
        },
        "items": [
            {
                "index": index,
                "identity": {"sourceSystemId": 101, "sourceTable": f"ODS.T{index}"},
                "action": action,
                "fieldCount": 1,
                **(
                    {"error": {"code": "INVALID_FIELD", "message": "invalid mapping"}}
                    if action == "failed"
                    else {}
                ),
            }
            for index, action in enumerate(actions)
        ],
    }


class MappingContractTests(unittest.TestCase):
    def test_payload_matches_dap_import_contract_and_is_upsert_only(self):
        item = collector.MappingItem(
            source_system_id=103,
            source_table="ODS.CUSTOMER",
            target_table="DWF.CUSTOMER",
            fields=(
                collector.MappingField(
                    source_field="CUST_ID",
                    source_type="VARCHAR(32)",
                    source_comment="customer key",
                    target_field="CUSTOMER_ID",
                    mapping_rule="RENAME",
                    field_order=1,
                ),
            ),
        )

        payload = collector.build_import_payload([item])

        self.assertEqual("upsert", payload["mode"])
        self.assertFalse(payload["dryRun"])
        self.assertEqual(
            {
                "sourceSystemId": 103,
                "sourceTable": "ODS.CUSTOMER",
                "targetLayer": "DWF",
                "targetTable": "DWF.CUSTOMER",
                "fields": [
                    {
                        "sourceField": "CUST_ID",
                        "sourceType": "VARCHAR(32)",
                        "sourceComment": "customer key",
                        "targetField": "CUSTOMER_ID",
                        "mappingRule": "RENAME",
                        "fieldOrder": 1,
                    }
                ],
            },
            payload["items"][0],
        )
        self.assertNotIn("dataSourceId", payload["items"][0])
        self.assertFalse(
            any(key in payload for key in ("delete", "replace", "truncate"))
        )

    def test_payload_validation_rejects_bad_identity_or_duplicate_field_key(self):
        item = _item("ODS.CUSTOMER")
        duplicate = collector.MappingItem(
            source_system_id=item.source_system_id,
            source_table=item.source_table,
            target_table=item.target_table,
            fields=(item.fields[0], item.fields[0]),
        )
        with self.assertRaisesRegex(ValueError, "duplicates"):
            collector.build_import_payload([duplicate])
        with self.assertRaisesRegex(ValueError, "500"):
            collector.build_import_payload([_item(f"ODS.T{n}") for n in range(501)])

    def test_idempotent_replay_keeps_the_same_dap_identity_and_payload(self):
        item = _item("ODS.CUSTOMER", 103)

        first = collector.build_import_payload([item])
        second = collector.build_import_payload([item])

        self.assertEqual(first, second)
        self.assertEqual((103, "ods.customer"), item.identity)


class SourceSystemIdentityTests(unittest.TestCase):
    def test_source_system_id_must_be_explicit_and_is_not_guessed(self):
        self.assertIsNone(collector.resolve_source_system_id(None, {}))
        self.assertEqual(
            103,
            collector.resolve_source_system_id(
                None, {"PYTOOLS_DAP_SOURCE_SYSTEM_ID": "103"}
            ),
        )
        with self.assertRaisesRegex(ValueError, "positive integer"):
            collector.resolve_source_system_id(
                None, {"PYTOOLS_DAP_SOURCE_SYSTEM_ID": "CORE"}
            )

    def test_same_source_table_in_two_systems_has_two_distinct_identities(self):
        first = _item("ODS.CUSTOMER", source_system_id=103)
        second = _item("ODS.CUSTOMER", source_system_id=204)

        self.assertNotEqual(first.identity, second.identity)
        self.assertNotEqual(
            collector.build_import_payload([first]),
            collector.build_import_payload([second]),
        )

    def test_missing_identity_skips_tables_without_sending_them(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "load.sql").write_text(
                "INSERT INTO DWF.CUSTOMER (CUSTOMER_ID) "
                "SELECT c.ID FROM ODS.CUSTOMER c",
                encoding="utf-8",
            )

            result = collector.collect_workspace(
                directory, source_system_id=None, dialect="mysql"
            )

        self.assertEqual((), result.items)
        self.assertEqual(1, result.stats.candidate_programs)
        self.assertEqual(1, result.stats.skipped_tables)
        self.assertTrue(
            any(
                "missing explicit DAP sourceSystemId" in item
                for item in result.stats.diagnostics
            )
        )


class SqlFieldMappingParseTests(unittest.TestCase):
    def _collect(
        self, sql: str, *, source_system_id: int = 103
    ) -> collector.Collection:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        Path(directory.name, "program.sql").write_text(sql, encoding="utf-8")
        return collector.collect_workspace(
            directory.name, source_system_id=source_system_id, dialect="mysql"
        )

    def test_python_program_uses_existing_static_sql_candidate_extractor(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        program = (
            "def run(execute):\n"
            "    sql = 'INSERT INTO DWF.CUSTOMER (ID) SELECT c.ID FROM ODS.CUSTOMER c'\n"
            "    execute(sql)\n"
        )
        Path(directory.name, "program.py").write_text(program, encoding="utf-8")

        result = collector.collect_workspace(
            directory.name, source_system_id=103, dialect="mysql"
        )

        self.assertEqual(1, result.stats.candidate_programs)
        self.assertEqual("ODS.CUSTOMER", result.items[0].source_table)

    def test_insert_select_field_order_alias_and_mapping_rule(self):
        result = self._collect(
            "INSERT INTO DWF.CUSTOMER (CUSTOMER_ID) "
            "SELECT c.ID AS customer_alias FROM ODS.CUSTOMER c"
        )

        self.assertEqual(1, result.stats.parsed_tables)
        mapping = result.items[0]
        self.assertEqual("ODS.CUSTOMER", mapping.source_table)
        self.assertEqual("DWF.CUSTOMER", mapping.target_table)
        self.assertEqual(
            ("ID", "CUSTOMER_ID", "RENAME", 1),
            (
                mapping.fields[0].source_field,
                mapping.fields[0].target_field,
                mapping.fields[0].mapping_rule,
                mapping.fields[0].field_order,
            ),
        )

    def test_function_commas_are_parsed_as_ast_and_multicolumn_coalesce_is_skipped(
        self,
    ):
        result = self._collect(
            "INSERT INTO DWF.CUSTOMER (ID_ALIAS, NAME_UPPER, FULL_NAME, COUNTRY) "
            "SELECT c.ID AS alias, UPPER(c.NAME), "
            "COALESCE(c.FIRST_NAME, c.LAST_NAME), CONCAT(c.COUNTRY, ',') "
            "FROM ODS.CUSTOMER c"
        )

        mapping = result.items[0]
        fields = {item.target_field: item for item in mapping.fields}
        self.assertEqual({"ID_ALIAS", "NAME_UPPER", "COUNTRY"}, set(fields))
        self.assertEqual("待补充", fields["NAME_UPPER"].mapping_rule)
        self.assertEqual("COUNTRY", fields["COUNTRY"].source_field)
        self.assertEqual(4, fields["COUNTRY"].field_order)
        self.assertTrue(
            any("FULL_NAME" in diagnostic for diagnostic in result.stats.diagnostics)
        )

    def test_case_when_and_cast_are_never_guessed(self):
        result = self._collect(
            "INSERT INTO DWF.CUSTOMER (CAST_ID, CONDITIONAL_ID) "
            "SELECT CAST(c.ID AS CHAR), "
            "CASE WHEN c.FLAG = 1 THEN c.ID ELSE c.OTHER_ID END "
            "FROM ODS.CUSTOMER c"
        )

        fields = {item.target_field: item for item in result.items[0].fields}
        self.assertEqual({"CAST_ID"}, set(fields))
        self.assertEqual("待补充", fields["CAST_ID"].mapping_rule)
        self.assertTrue(
            any("CONDITIONAL_ID" in item for item in result.stats.diagnostics)
        )

    def test_quoted_schema_identifiers_and_case_normalization(self):
        result = self._collect(
            "INSERT INTO `dwf`.`Customer` (`TargetId`) "
            "SELECT `c`.`Id` FROM `ods`.`Customer` AS `c`"
        )

        mapping = result.items[0]
        self.assertEqual("ODS.CUSTOMER", mapping.source_table)
        self.assertEqual("DWF.CUSTOMER", mapping.target_table)
        self.assertEqual("ID", mapping.fields[0].source_field)
        self.assertEqual("TARGETID", mapping.fields[0].target_field)

    def test_join_fields_are_grouped_by_physical_source_table(self):
        result = self._collect(
            "INSERT INTO DWF.CUSTOMER (CUSTOMER_ID, CRM_NAME) "
            "SELECT c.ID, r.NAME FROM ODS.CUSTOMER c "
            "JOIN CRM.CONTACT r ON c.ID = r.CUSTOMER_ID"
        )

        self.assertEqual(2, len(result.items))
        by_source = {item.source_table: item for item in result.items}
        self.assertEqual({"ODS.CUSTOMER", "CRM.CONTACT"}, set(by_source))
        self.assertEqual(
            "CUSTOMER_ID", by_source["ODS.CUSTOMER"].fields[0].target_field
        )
        self.assertEqual("CRM_NAME", by_source["CRM.CONTACT"].fields[0].target_field)

    def test_cte_is_explicitly_unsupported(self):
        result = self._collect(
            "INSERT INTO DWF.CUSTOMER (CUSTOMER_ID) "
            "WITH source_rows AS (SELECT ID FROM ODS.CUSTOMER) "
            "SELECT ID FROM source_rows"
        )

        self.assertEqual((), result.items)
        self.assertGreaterEqual(result.stats.skipped_tables, 1)
        self.assertTrue(
            any(
                "CTE source is unsupported" in item for item in result.stats.diagnostics
            )
        )

    def test_source_table_feeding_multiple_targets_is_skipped_for_dap_identity(self):
        result = self._collect(
            "INSERT INTO DWF.CUSTOMER_A (ID) SELECT c.ID FROM ODS.CUSTOMER c; "
            "INSERT INTO DWF.CUSTOMER_B (ID) SELECT c.ID FROM ODS.CUSTOMER c"
        )

        self.assertEqual((), result.items)
        self.assertEqual(1, result.stats.skipped_tables)
        self.assertTrue(
            any("multiple DWF targets" in item for item in result.stats.diagnostics)
        )


class BatchAndPartialFailureTests(unittest.TestCase):
    def test_failed_table_can_be_rescanned_and_filtered_for_single_item_retry(self):
        stats = collector.CollectorStats(parsed_tables=2, parsed_fields=2)
        collection = collector.Collection(
            (_item("ODS.CUSTOMER"), _item("ODS.ACCOUNT")), stats
        )

        retry = collector.filter_collection_by_source_tables(
            collection, ["ods.customer"]
        )

        self.assertEqual(["ODS.CUSTOMER"], [item.source_table for item in retry.items])
        self.assertEqual(1, retry.stats.parsed_tables)
        self.assertEqual(1, retry.stats.parsed_fields)

    def test_default_batch_size_100_splits_4000_tables_as_expected(self):
        items = [_item(f"ODS.T{index:04d}") for index in range(4_000)]

        batches = collector.split_batches(items, 100)

        self.assertEqual(40, len(batches))
        self.assertTrue(all(len(batch) == 100 for batch in batches))

    def test_100_tables_fit_one_batch_and_101_split(self):
        items = [_item(f"ODS.T{index:03d}") for index in range(101)]

        self.assertEqual(
            [100], [len(batch) for batch in collector.split_batches(items[:100], 100)]
        )
        self.assertEqual(
            [100, 1], [len(batch) for batch in collector.split_batches(items, 100)]
        )
        with self.assertRaisesRegex(ValueError, "between 1 and 500"):
            collector.split_batches(items, 501)

    def test_partial_failure_preserves_item_level_results(self):
        client = Mock()
        client.import_mappings.return_value = _response(
            ["created", "failed", "unchanged"]
        )
        stats = collector.CollectorStats()
        items = [_item(f"ODS.T{index}") for index in range(3)]

        collector.submit_batches(
            items,
            client,
            batch_size=100,
            server_dry_run=False,
            stats=stats,
        )

        self.assertEqual(
            (1, 0, 1, 1), (stats.created, stats.updated, stats.unchanged, stats.failed)
        )
        self.assertEqual("ODS.T1", stats.failed_tables[0]["sourceTable"])
        self.assertEqual("INVALID_FIELD", stats.failed_tables[0]["errorCode"])
        self.assertEqual(3, stats.field_count)


class DryRunAndApiRetryTests(unittest.TestCase):
    class FakeResponse:
        def __init__(self, status_code: int, body: dict[str, object]) -> None:
            self.status_code = status_code
            self._body = body

        def json(self) -> dict[str, object]:
            return self._body

    class FakeSession:
        def __init__(self, responses: list[object]) -> None:
            self.responses = responses
            self.calls: list[dict[str, object]] = []

        def post(
            self, url: str, **kwargs: object
        ) -> DryRunAndApiRetryTests.FakeResponse:
            self.calls.append({"url": url, **kwargs})
            result = self.responses.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result

        def close(self) -> None:
            return None

    def test_local_dry_run_writes_preview_and_never_constructs_api_client(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "load.sql").write_text(
                "INSERT INTO DWF.CUSTOMER (ID) SELECT c.ID FROM ODS.CUSTOMER c",
                encoding="utf-8",
            )
            output = Path(directory, "preview.json")
            stdout = io.StringIO()
            with (
                patch.object(
                    collector,
                    "FieldMappingApiClient",
                    side_effect=AssertionError("HTTP client must not be created"),
                ) as api_client,
                contextlib.redirect_stdout(stdout),
            ):
                exit_code = collector.main(
                    [
                        "--directory",
                        directory,
                        "--source-system-id",
                        "103",
                        "--dry-run",
                        "--payload-output",
                        str(output),
                    ]
                )

            preview = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(0, exit_code)
        self.assertEqual("local-dry-run", preview["executionMode"])
        self.assertTrue(preview["batches"][0]["dryRun"])
        self.assertIn("dry_run=true", stdout.getvalue())
        api_client.assert_not_called()

    def test_502_503_timeout_policy_retries_only_retryable_http_errors(self):
        good = self.FakeResponse(200, _response(["created"]))
        session = self.FakeSession(
            [self.FakeResponse(503, {}), self.FakeResponse(502, {}), good]
        )
        sleeper = Mock()
        client = collector.FieldMappingApiClient(
            "https://dap.example.test",
            session_cookie="session=redacted-cookie",
            max_retries=2,
            retry_backoff=0.5,
            session=session,
            sleep=sleeper,
        )

        response = client.import_mappings({"mode": "upsert", "items": [{}]})

        self.assertEqual("created", response["items"][0]["action"])
        self.assertEqual(3, len(session.calls))
        self.assertEqual((5.0, 30.0), session.calls[0]["timeout"])
        self.assertEqual(
            "session=redacted-cookie", session.calls[0]["headers"]["Cookie"]
        )
        self.assertEqual([0.5, 1.0], [call.args[0] for call in sleeper.call_args_list])
        self.assertEqual(
            "https://dap.example.test/api/field-mappings/import",
            session.calls[0]["url"],
        )

    def test_server_dry_run_is_sent_as_dry_run_and_validated_in_response(self):
        response = _response(["created"])
        response["dryRun"] = True
        session = self.FakeSession([self.FakeResponse(200, response)])
        client = collector.FieldMappingApiClient(
            "https://dap.example.test",
            session_cookie="session=secret",
            max_retries=0,
            session=session,
        )

        result = client.import_mappings(
            {"mode": "upsert", "dryRun": True, "items": [{}]}
        )

        self.assertTrue(result["dryRun"])
        self.assertTrue(session.calls[0]["json"]["dryRun"])

    def test_connection_timeout_retries_a_bounded_number_of_times(self):
        session = self.FakeSession(
            [
                collector.requests.exceptions.Timeout(),
                self.FakeResponse(200, _response(["created"])),
            ]
        )
        sleeper = Mock()
        client = collector.FieldMappingApiClient(
            "https://dap.example.test",
            session_cookie="session=secret",
            max_retries=1,
            retry_backoff=0.25,
            session=session,
            sleep=sleeper,
        )

        result = client.import_mappings({"mode": "upsert", "items": [{}]})

        self.assertEqual("created", result["items"][0]["action"])
        self.assertEqual(2, len(session.calls))
        sleeper.assert_called_once_with(0.25)

    def test_non_retryable_400_401_403_are_not_retried(self):
        for status, code in (
            (400, "VALIDATION_ERROR"),
            (401, "UNAUTHORIZED"),
            (403, "FORBIDDEN"),
        ):
            with self.subTest(status=status):
                session = self.FakeSession(
                    [
                        self.FakeResponse(
                            status,
                            {"error": {"code": code, "message": "rejected"}},
                        ),
                        self.FakeResponse(200, _response(["created"])),
                    ]
                )
                client = collector.FieldMappingApiClient(
                    "https://dap.example.test",
                    session_cookie="session=secret",
                    max_retries=2,
                    retry_backoff=0,
                    session=session,
                    sleep=Mock(),
                )
                with self.assertRaises(collector.FieldMappingApiError) as raised:
                    client.import_mappings({"mode": "upsert", "items": [{}]})
                self.assertEqual(code, raised.exception.code)
                self.assertEqual(1, len(session.calls))
                self.assertNotIn("secret", str(raised.exception))

    def test_item_error_message_redacts_session_cookie(self):
        response = _response(["failed"])
        response["items"][0]["error"] = {
            "code": "ITEM_FAILED",
            "message": "rejected session=private-value",
        }
        session = self.FakeSession([self.FakeResponse(200, response)])
        client = collector.FieldMappingApiClient(
            "https://dap.example.test",
            session_cookie="session=private-value",
            max_retries=0,
            session=session,
        )
        stats = collector.CollectorStats()
        stderr = io.StringIO()

        with contextlib.redirect_stderr(stderr):
            collector.submit_batches(
                [_item("ODS.T0")],
                client,
                batch_size=100,
                server_dry_run=False,
                stats=stats,
            )

        self.assertEqual(1, stats.failed)
        self.assertNotIn("private-value", stderr.getvalue())
        self.assertNotIn("private-value", str(stats.failed_tables))


if __name__ == "__main__":
    unittest.main()
