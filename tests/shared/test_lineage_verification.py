from __future__ import annotations

import json
import os
import unittest
from collections.abc import Sequence
from types import SimpleNamespace
from unittest.mock import patch

from shared.lineage.providers import MySQLProcessProfile
from shared.lineage.verification import (  # pyright: ignore[reportMissingImports]
    SnapshotStatus,
    TargetEvidenceProbe,
    VerificationErrorCode,
    build_verification_report,
    verify_mysql_profile,
    verify_production_provider,
)


class FakeCursor:
    def __init__(
        self,
        rows: Sequence[object],
        *,
        execute_error: Exception | None = None,
        fetch_error_after: int | None = None,
    ) -> None:
        self.rows = list(rows)
        self.execute_error = execute_error
        self.fetch_error_after = fetch_error_after
        self.offset = 0
        self.closed = False
        self.execute_calls: list[tuple[object, ...]] = []
        self.fetchmany_calls: list[int] = []

    def execute(self, *args: object) -> None:
        self.execute_calls.append(args)
        if self.execute_error is not None:
            raise self.execute_error

    def fetchmany(self, size: int) -> list[object]:
        self.fetchmany_calls.append(size)
        if self.fetch_error_after is not None and self.offset >= self.fetch_error_after:
            raise OSError("fetch failed for VERY_SECRET_TEST_VALUE at 10.0.0.1")
        batch = self.rows[self.offset : self.offset + size]
        self.offset += len(batch)
        return batch

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_instance = cursor
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


class DriverLikeClob:
    def __init__(self, value: str) -> None:
        self.value = value

    def read(self) -> str:
        return self.value

    def __str__(self) -> str:
        return "RAW_SCRIPT_CONTENT"


class UnsupportedDriverValue:
    def __str__(self) -> str:
        return "RAW_SCRIPT_CONTENT"


def make_profile(
    name: str = "dev_a",
    *,
    batch_size: int = 2,
    expected_target_column: str | None = "expected_target",
) -> MySQLProcessProfile:
    prefix = name.upper()
    return MySQLProcessProfile(
        name=name,
        environment="DEV",
        host_env=f"{prefix}_HOST",
        port_env=f"{prefix}_PORT",
        user_env=f"{prefix}_USER",
        password_env=f"{prefix}_PASSWORD",
        database_env=f"{prefix}_DATABASE",
        process_table="demo_meta.processes",
        program_name_column="process_name",
        script_code_column="script_code",
        expected_target_column=expected_target_column,
        batch_size=batch_size,
    )


def env_for(profile: MySQLProcessProfile) -> dict[str, str]:
    return {
        profile.host_env: "127.0.0.1",
        profile.port_env: "3306",
        profile.user_env: "DEMO_USER",
        profile.password_env: "fake-password",
        profile.database_env: "internal-db",
    }


class LineageVerificationTests(unittest.TestCase):
    def verify_with_rows(
        self,
        rows: Sequence[object],
        *,
        profile: MySQLProcessProfile | None = None,
        sample_limit: int = 20,
        sample_only: bool = False,
        target_probe: TargetEvidenceProbe | None = None,
        cursor_kwargs: dict[str, object] | None = None,
    ):
        profile = profile or make_profile()
        kwargs = cursor_kwargs or {}
        execute_error = kwargs.get("execute_error")
        fetch_error_after = kwargs.get("fetch_error_after")
        cursor = FakeCursor(
            rows,
            execute_error=(
                execute_error if isinstance(execute_error, Exception) else None
            ),
            fetch_error_after=(
                fetch_error_after if isinstance(fetch_error_after, int) else None
            ),
        )
        connection = FakeConnection(cursor)
        with patch.dict(os.environ, env_for(profile), clear=False):
            result = verify_mysql_profile(
                profile,
                connection_factory=lambda _settings: connection,
                sample_limit=sample_limit,
                sample_only=sample_only,
                target_probe=target_probe,
            )
        self.assertTrue(connection.closed)
        self.assertTrue(cursor.closed)
        return result, cursor

    def test_runtime_shapes_and_driver_read_are_sanitized(self):
        profile = make_profile(batch_size=10)
        result, _cursor = self.verify_with_rows(
            [
                ("PROGRAM_STR", "select 1", "DWM.TARGET"),
                ("PROGRAM_BYTES", b"select 2", "DWM.TARGET"),
                ("PROGRAM_BYTEARRAY", bytearray(b"select 3"), None),
                ("PROGRAM_MEMORYVIEW", memoryview(b"select 4"), None),
                ("PROGRAM_NONE", None, None),
                (
                    "PROGRAM_CLOB",
                    DriverLikeClob("select RAW_SCRIPT_CONTENT"),
                    None,
                ),
            ],
            profile=profile,
        )

        self.assertEqual(result.error_code, VerificationErrorCode.SUCCESS.value)
        self.assertEqual(result.snapshot_status, SnapshotStatus.COMPLETE.value)
        self.assertEqual(result.row_count, 6)
        self.assertEqual(
            result.script_type_counts,
            {
                "None": 1,
                "bytearray": 1,
                "bytes": 1,
                "driver_specific_object": 1,
                "memoryview": 1,
                "str": 1,
            },
        )
        self.assertEqual(result.script_null_count, 1)
        self.assertEqual(result.script_decode_successes, 5)
        self.assertEqual(result.script_decode_failures, 0)
        self.assertEqual(result.explicit_target_present_count, 2)
        self.assertEqual(result.explicit_target_null_count, 4)

    def test_connection_and_query_errors_are_distinguished(self):
        profile = make_profile("connection_failure")
        with patch.dict(os.environ, env_for(profile), clear=False):
            connection_result = verify_mysql_profile(
                profile,
                connection_factory=lambda _settings: (_ for _ in ()).throw(
                    OSError("fake-password 10.0.0.1")
                ),
            )
        self.assertEqual(
            connection_result.error_code,
            VerificationErrorCode.CONNECT_ERROR.value,
        )
        self.assertEqual(connection_result.query_status, "NOT_RUN")
        self.assertEqual(connection_result.snapshot_status, SnapshotStatus.FAILED.value)

        profile = make_profile("query_failure")
        result, _cursor = self.verify_with_rows(
            [],
            profile=profile,
            cursor_kwargs={
                "execute_error": RuntimeError(
                    "SELECT ... from internal-db for fake-password"
                )
            },
        )
        self.assertEqual(result.error_code, VerificationErrorCode.QUERY_ERROR.value)
        self.assertEqual(result.connection_status, "SUCCESS")
        self.assertEqual(result.query_status, "FAILED")
        self.assertEqual(result.snapshot_status, SnapshotStatus.FAILED.value)

    def test_unknown_driver_shape_requires_thin_adapter(self):
        result, _cursor = self.verify_with_rows(
            [("PROGRAM_UNKNOWN_DRIVER", UnsupportedDriverValue(), None)],
            profile=make_profile("unknown_driver"),
        )
        self.assertEqual(result.error_code, VerificationErrorCode.DECODE_ERROR.value)
        self.assertEqual(result.script_thin_adapter_required, 1)
        self.assertEqual(result.snapshot_status, SnapshotStatus.FAILED.value)

    def test_row_mapping_and_decode_errors_never_complete_snapshot(self):
        mapping_result, _cursor = self.verify_with_rows(
            [("PROGRAM_ONLY",)],
            profile=make_profile("mapping_failure"),
        )
        self.assertEqual(
            mapping_result.error_code,
            VerificationErrorCode.ROW_MAPPING_ERROR.value,
        )
        self.assertEqual(mapping_result.snapshot_status, SnapshotStatus.FAILED.value)

        decode_result, _cursor = self.verify_with_rows(
            [("PROGRAM_BAD_BYTES", b"\xff\xfe", None)],
            profile=make_profile("decode_failure"),
        )
        self.assertEqual(
            decode_result.error_code,
            VerificationErrorCode.DECODE_ERROR.value,
        )
        self.assertEqual(decode_result.script_decode_failures, 1)
        self.assertEqual(decode_result.snapshot_status, SnapshotStatus.FAILED.value)

    def test_target_conflict_and_derived_only_are_diagnostic_only(self):
        profile = make_profile("target_evidence")
        result, _cursor = self.verify_with_rows(
            [
                ("PROGRAM_CONFLICT", "select 1", "DWM.EXPLICIT_TARGET"),
                ("PROGRAM_DERIVED", "select 2", None),
            ],
            profile=profile,
            target_probe=TargetEvidenceProbe(
                metadata_join=lambda item: (
                    "DWM.LEGACY_TARGET"
                    if item.program_name == "PROGRAM_CONFLICT"
                    else None
                ),
                derived=lambda item: (
                    "DWM.DERIVED_TARGET"
                    if item.program_name == "PROGRAM_DERIVED"
                    else None
                ),
            ),
        )

        self.assertEqual(result.target_conflict_count, 1)
        self.assertEqual(result.derived_only_count, 1)
        self.assertEqual(result.target_match_count, 0)
        self.assertEqual(result.target_evidence_counts["CONFLICT"], 1)
        self.assertEqual(result.target_evidence_counts["DERIVED_ONLY"], 1)

    def test_duplicate_identity_is_only_short_hash(self):
        result, _cursor = self.verify_with_rows(
            [
                ("DUPLICATE_PROGRAM", "select 1", None),
                ("DUPLICATE_PROGRAM", "select 1", None),
            ],
            profile=make_profile("duplicates"),
        )
        self.assertEqual(result.duplicate_identity_count, 1)
        self.assertEqual(len(result.duplicate_identity_samples), 1)
        self.assertEqual(len(result.duplicate_identity_samples[0]), 12)
        self.assertNotIn("DUPLICATE_PROGRAM", result.duplicate_identity_samples)

    def test_sample_only_is_partial_and_full_stream_is_complete(self):
        rows = [
            ("PROGRAM_1", "select 1", None),
            ("PROGRAM_2", "select 2", None),
            ("PROGRAM_3", "select 3", None),
        ]
        sample_result, sample_cursor = self.verify_with_rows(
            rows,
            profile=make_profile("sample", batch_size=10),
            sample_limit=2,
            sample_only=True,
        )
        self.assertEqual(sample_result.row_count, 2)
        self.assertEqual(sample_result.sample_count, 2)
        self.assertEqual(sample_result.snapshot_status, SnapshotStatus.PARTIAL.value)
        self.assertEqual(sample_cursor.fetchmany_calls, [2])

        full_result, full_cursor = self.verify_with_rows(
            rows,
            profile=make_profile("full", batch_size=2),
        )
        self.assertEqual(full_result.row_count, 3)
        self.assertEqual(full_result.snapshot_status, SnapshotStatus.COMPLETE.value)
        self.assertEqual(full_cursor.fetchmany_calls, [2, 2, 2])

    def test_mid_stream_failure_is_not_complete(self):
        result, _cursor = self.verify_with_rows(
            [
                ("PROGRAM_1", "select 1", None),
                ("PROGRAM_2", "select 2", None),
            ],
            profile=make_profile("mid_stream", batch_size=1),
            cursor_kwargs={"fetch_error_after": 1},
        )
        self.assertEqual(result.error_code, VerificationErrorCode.QUERY_ERROR.value)
        self.assertEqual(result.snapshot_status, SnapshotStatus.FAILED.value)
        self.assertNotEqual(result.snapshot_status, SnapshotStatus.COMPLETE.value)

    def test_production_provider_backend_is_injectable_and_lazy(self):
        calls: list[str] = []

        def loader():
            calls.append("called")
            return [
                SimpleNamespace(
                    process_name="PROD_PROGRAM",
                    script_code="RAW_SCRIPT_CONTENT",
                )
            ]

        result = verify_production_provider(loader, sample_limit=20)
        self.assertEqual(result.import_status, "PASS")
        self.assertTrue(result.legacy_loader_callable)
        self.assertTrue(result.loader_invocation_attempted)
        self.assertTrue(result.rows_readable)
        self.assertEqual(result.row_count, 1)
        self.assertEqual(result.snapshot_status, SnapshotStatus.COMPLETE.value)
        self.assertEqual(calls, ["called"])

        partial = verify_production_provider(loader, sample_limit=1, sample_only=True)
        self.assertEqual(partial.snapshot_status, SnapshotStatus.PARTIAL.value)

        def failing_loader():
            raise OSError("internal-db fake-password 10.0.0.1")

        failed = verify_production_provider(failing_loader)
        self.assertEqual(failed.error_code, VerificationErrorCode.QUERY_ERROR.value)
        self.assertFalse(failed.rows_readable)
        self.assertEqual(failed.snapshot_status, SnapshotStatus.FAILED.value)

        bad_rows = verify_production_provider(
            lambda: [{"program_name": "PROGRAM_WITHOUT_CODE"}]
        )
        self.assertEqual(
            bad_rows.error_code,
            VerificationErrorCode.ROW_MAPPING_ERROR.value,
        )

    def test_report_is_sanitized(self):
        profile = make_profile("safe_report")
        result, _cursor = self.verify_with_rows(
            [("PROGRAM_SECRET", "RAW_SCRIPT_CONTENT", "DWM.REAL_TARGET")],
            profile=profile,
        )
        report = build_verification_report(
            [result],
            production_provider=verify_production_provider(
                lambda: (_ for _ in ()).throw(
                    OSError(
                        "fake-password 10.0.0.1 internal-db "
                        "svn://internal SELECT ... VERY_SECRET_TEST_VALUE"
                    )
                )
            ),
            generated_at="2026-09-05T00:00:00Z",
        )
        serialized = json.dumps(report.to_dict(), ensure_ascii=False)
        for secret in (
            "fake-password",
            "10.0.0.1",
            "internal-db",
            "svn://internal",
            "SELECT ...",
            "RAW_SCRIPT_CONTENT",
            "VERY_SECRET_TEST_VALUE",
            "PROGRAM_SECRET",
            "DWM.REAL_TARGET",
        ):
            self.assertNotIn(secret, serialized)
        self.assertIn('"script_type_counts"', serialized)
        self.assertIn('"error_message_safe": "metadata query failed"', serialized)

    def test_missing_credentials_are_config_error_without_factory_call(self):
        profile = make_profile("missing_credentials")
        called = False

        def factory(_settings):
            nonlocal called
            called = True
            return FakeConnection(FakeCursor([]))

        with patch.dict(os.environ, {}, clear=True):
            result = verify_mysql_profile(profile, connection_factory=factory)
        self.assertEqual(result.error_code, VerificationErrorCode.CONFIG_ERROR.value)
        self.assertFalse(called)
        self.assertNotIn("fake-password", result.error_message_safe or "")


if __name__ == "__main__":
    unittest.main()
