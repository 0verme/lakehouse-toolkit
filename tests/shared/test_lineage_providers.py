from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from shared.lineage.domain import ProgramSource, compute_source_hash
from shared.lineage.lineage_builder import ProcessInfo
from shared.lineage.providers import (  # pyright: ignore[reportMissingImports]
    MySQLProcessProfile,
    MySQLProcessProvider,
    ProductionProvider,
    ProgramSourceProvider,
    ProviderError,
    iter_program_sources,
    load_mysql_process_profiles,
)


class FakeCursor:
    def __init__(self, rows: Sequence[object]):
        self.rows = list(rows)
        self.offset = 0
        self.execute_calls: list[tuple[object, ...]] = []
        self.fetchmany_calls: list[int] = []
        self.closed = False
        self.fetchall_called = False

    def execute(self, *args):
        self.execute_calls.append(args)

    def fetchmany(self, size: int):
        self.fetchmany_calls.append(size)
        batch = self.rows[self.offset : self.offset + size]
        self.offset += len(batch)
        return batch

    def fetchall(self):
        self.fetchall_called = True
        raise AssertionError("the provider must use fetchmany, not fetchall")

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self.cursor_instance = cursor
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def make_profile(
    name: str = "mysql_dev_a",
    *,
    batch_size: int = 200,
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


def environment_for(profile: MySQLProcessProfile) -> dict[str, str]:
    return {
        profile.host_env: "127.0.0.1",
        profile.port_env: "3306",
        profile.user_env: "DEMO_USER",
        profile.password_env: "DEMO_PASSWORD_VALUE",
        profile.database_env: "demo_meta",
    }


class LineageProviderTests(unittest.TestCase):
    def test_mysql_provider_contract_and_repeated_fetchmany(self):
        profile = make_profile(batch_size=2)
        rows = [
            ("PROGRAM_1", "select 1", "DWM.TARGET_1"),
            ("PROGRAM_2", "select 2", "DWM.TARGET_2"),
            ("PROGRAM_3", "select 3", "DWM.TARGET_3"),
            ("PROGRAM_4", "select 4", "DWM.TARGET_4"),
            ("PROGRAM_5", "select 5", "DWM.TARGET_5"),
        ]
        cursor = FakeCursor(rows)
        connection = FakeConnection(cursor)

        with patch.dict(os.environ, environment_for(profile), clear=False):
            provider = MySQLProcessProvider(
                profile, connection_factory=lambda settings: connection
            )
            self.assertIsInstance(provider, ProgramSourceProvider)
            sources = list(provider.iter_program_sources())

        self.assertEqual(len(sources), 5)
        self.assertTrue(all(isinstance(item, ProgramSource) for item in sources))
        self.assertEqual(
            cursor.fetchmany_calls,
            [2, 2, 2, 2],
            "5 rows with batch_size=2 must fetch until an empty batch",
        )
        self.assertFalse(cursor.fetchall_called)
        self.assertEqual(cursor.execute_calls[0][0], provider.query)
        self.assertEqual(sources[0].environment, "DEV")
        self.assertEqual(sources[0].source_profile, "mysql_dev_a")
        self.assertTrue(cursor.closed)
        self.assertTrue(connection.closed)

    def test_one_and_many_profiles_keep_environment_and_source_profile_separate(self):
        profiles = [
            make_profile("mysql_dev_a", expected_target_column=None),
            make_profile("mysql_dev_b", expected_target_column=None),
            make_profile("mysql_dev_c", expected_target_column=None),
            make_profile("mysql_dev_d", expected_target_column=None),
        ]
        connections: dict[str, FakeConnection] = {}

        def factory(settings):
            profile_name = settings.database.removeprefix("demo_")
            cursor = FakeCursor(
                [(f"PROGRAM_{profile_name}", f"select '{profile_name}'")]
            )
            connection = FakeConnection(cursor)
            connections[profile_name] = connection
            return connection

        values = {}
        for profile in profiles:
            values.update(environment_for(profile))
            values[profile.database_env] = f"demo_{profile.name}"

        with patch.dict(os.environ, values, clear=False):
            all_sources = list(
                iter_program_sources(
                    [
                        MySQLProcessProvider(profile, connection_factory=factory)
                        for profile in profiles
                    ]
                )
            )

        self.assertEqual(
            [(item.environment, item.source_profile) for item in all_sources],
            [("DEV", profile.name) for profile in profiles],
        )
        self.assertEqual(
            [item.program_name for item in all_sources],
            [f"PROGRAM_{profile.name}" for profile in profiles],
        )
        self.assertTrue(all(item.expected_target is None for item in all_sources))
        self.assertEqual(len(connections), 4)

        single_profile = profiles[0]
        with patch.dict(os.environ, environment_for(single_profile), clear=False):
            single_connection = FakeConnection(
                FakeCursor([("PROGRAM_SINGLE", "select 1")])
            )
            single_source = list(
                MySQLProcessProvider(
                    single_profile,
                    connection_factory=lambda settings: single_connection,
                ).iter_program_sources()
            )
        self.assertEqual(len(single_source), 1)
        self.assertEqual(single_source[0].source_profile, "mysql_dev_a")

    def test_decode_bytes_bytearray_none_and_empty_expected_target(self):
        profile = make_profile(batch_size=10)
        rows = [
            (b"PROGRAM_BYTES", b"select 1", bytearray(b"DWM.TARGET")),
            ("PROGRAM_BYTES", "select 1", "DWM.TARGET"),
            ("PROGRAM_NONE", None, ""),
        ]
        cursor = FakeCursor(rows)
        connection = FakeConnection(cursor)

        with patch.dict(os.environ, environment_for(profile), clear=False):
            sources = list(
                MySQLProcessProvider(
                    profile,
                    connection_factory=lambda settings: connection,
                ).iter_program_sources()
            )

        self.assertTrue(all(isinstance(item.script_code, str) for item in sources))
        self.assertEqual(sources[0].program_name, "PROGRAM_BYTES")
        self.assertEqual(sources[0].script_code, "select 1")
        self.assertEqual(sources[0].expected_target, "DWM.TARGET")
        self.assertEqual(sources[1].expected_target, "DWM.TARGET")
        self.assertEqual(sources[2].script_code, "")
        self.assertIsNone(sources[2].expected_target)
        self.assertEqual(sources[0].source_hash, sources[1].source_hash)

    def test_source_hash_is_stable_and_changes_for_code_or_target(self):
        expected_payload = json.dumps(
            {
                "expected_target": "DWM.TARGET",
                "program_name": "PROGRAM",
                "script_code": "select 1",
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        expected_hash = hashlib.sha256(expected_payload).hexdigest()

        self.assertEqual(
            compute_source_hash("PROGRAM", "select 1", "DWM.TARGET"), expected_hash
        )
        self.assertEqual(
            compute_source_hash("PROGRAM", "select 1", "DWM.TARGET"),
            compute_source_hash("PROGRAM", "select 1", "DWM.TARGET"),
        )
        self.assertNotEqual(
            compute_source_hash("PROGRAM", "select 1", "DWM.TARGET"),
            compute_source_hash("PROGRAM", "select 2", "DWM.TARGET"),
        )
        self.assertNotEqual(
            compute_source_hash("PROGRAM", "select 1", "DWM.TARGET"),
            compute_source_hash("PROGRAM", "select 1", "DWM.OTHER"),
        )

    def test_expected_target_is_not_guessed_when_column_is_not_configured(self):
        profile = make_profile(expected_target_column=None)
        cursor = FakeCursor([("PROGRAM_DWM_TARGET", "select * from DWM.TARGET")])
        connection = FakeConnection(cursor)

        with patch.dict(os.environ, environment_for(profile), clear=False):
            source = next(
                MySQLProcessProvider(
                    profile,
                    connection_factory=lambda settings: connection,
                ).iter_program_sources()
            )

        self.assertIsNone(source.expected_target)

    def test_invalid_identifier_is_rejected_before_query(self):
        placeholder_key = "DEMO_AUTH_ENV"
        with self.assertRaises(ValueError):
            MySQLProcessProfile(
                name="mysql_dev_a",
                environment="DEV",
                host_env="HOST",
                port_env="PORT",
                user_env="USER",
                password_env=placeholder_key,
                database_env="DATABASE",
                process_table="demo_meta.processes; drop table x",
            )

    def test_missing_credential_fails_fast_with_profile_context(self):
        profile = make_profile("mysql_dev_missing")
        factory_called = False

        def factory(settings):
            nonlocal factory_called
            factory_called = True
            return FakeConnection(FakeCursor([]))

        with (
            patch.dict(os.environ, {}, clear=True),
            self.assertRaisesRegex(
                ProviderError,
                r"environment=DEV source_profile=mysql_dev_missing.*HOST",
            ),
        ):
            MySQLProcessProvider(
                profile, connection_factory=factory
            ).iter_program_sources()

        self.assertFalse(factory_called)

    def test_connection_failure_has_profile_context_without_password(self):
        profile = make_profile("mysql_dev_failure")

        def factory(settings):
            raise OSError(f"connection failed for {settings.password}")

        with (
            patch.dict(os.environ, environment_for(profile), clear=False),
            self.assertRaisesRegex(
                ProviderError,
                r"environment=DEV source_profile=mysql_dev_failure",
            ) as raised,
        ):
            list(
                MySQLProcessProvider(
                    profile, connection_factory=factory
                ).iter_program_sources()
            )

        self.assertNotIn("DEMO_PASSWORD_VALUE", str(raised.exception))

    def test_generator_close_and_row_validation_cleanup_resources(self):
        profile = make_profile()
        cursor = FakeCursor([("PROGRAM_1", "select 1", "DWM.TARGET")])
        connection = FakeConnection(cursor)
        with patch.dict(os.environ, environment_for(profile), clear=False):
            iterator = MySQLProcessProvider(
                profile,
                connection_factory=lambda settings: connection,
            ).iter_program_sources()
            next(iterator)
            iterator.close()

        self.assertTrue(cursor.closed)
        self.assertTrue(connection.closed)

        invalid_cursor = FakeCursor([(None, "select 1", "DWM.TARGET")])
        invalid_connection = FakeConnection(invalid_cursor)
        with (
            patch.dict(os.environ, environment_for(profile), clear=False),
            self.assertRaises(ProviderError),
        ):
            list(
                MySQLProcessProvider(
                    profile,
                    connection_factory=lambda settings: invalid_connection,
                ).iter_program_sources()
            )
        self.assertTrue(invalid_cursor.closed)
        self.assertTrue(invalid_connection.closed)

    def test_production_adapter_maps_legacy_process_info_and_injected_target(self):
        legacy_rows = [
            ProcessInfo(
                source_table="process_registry",
                process_name="DEMO_JOB:DWM.TARGET",
                script_code="select 1",
            ),
            {"program_name": "DEMO_PROGRAM_2", "script_code": "select 2"},
        ]
        provider = ProductionProvider(lambda: legacy_rows)
        sources = list(provider.iter_program_sources())

        self.assertTrue(all(isinstance(item, ProgramSource) for item in sources))
        self.assertEqual(
            [(item.environment, item.source_profile) for item in sources],
            [("PROD", "production_metadata"), ("PROD", "production_metadata")],
        )
        self.assertEqual(sources[0].program_name, "DEMO_JOB:DWM.TARGET")
        self.assertEqual(sources[0].script_code, "select 1")
        self.assertIsNone(sources[0].expected_target)

        row = SimpleNamespace(program_name="DEMO_PROGRAM_3", script_code="select 3")
        target_provider = ProductionProvider(
            lambda: [row],
            expected_target_getter=lambda legacy_row: "DWM.EXPLICIT_TARGET",
        )
        target_source = next(target_provider.iter_program_sources())
        self.assertEqual(target_source.expected_target, "DWM.EXPLICIT_TARGET")

    def test_production_loader_error_has_context(self):
        def loader():
            raise OSError("metadata unavailable")

        with self.assertRaisesRegex(
            ProviderError,
            r"environment=PROD source_profile=production_metadata",
        ):
            ProductionProvider(loader).iter_program_sources()

    def test_provider_aggregator_is_streaming(self):
        events: list[str] = []

        class TrackingProvider:
            def __init__(self, name: str):
                self.name = name

            def iter_program_sources(self):
                events.append(f"start:{self.name}")
                yield ProgramSource(
                    environment="DEV",
                    source_profile=self.name,
                    program_name=f"PROGRAM_{self.name}",
                    script_code="select 1",
                )
                events.append(f"end:{self.name}")

        sources = iter_program_sources([TrackingProvider("a"), TrackingProvider("b")])
        first = next(sources)
        self.assertEqual(first.source_profile, "a")
        self.assertEqual(events, ["start:a"])
        second = next(sources)
        self.assertEqual(second.source_profile, "b")
        self.assertEqual(events, ["start:a", "end:a", "start:b"])

    def test_load_mysql_process_profiles_from_yaml(self):
        config = """\
mysql_process_profiles:
  - name: mysql_dev_demo
    environment: DEV
    host_env: DEMO_HOST
    port_env: DEMO_PORT
    user_env: DEMO_USER
    password_env: DEMO_PASSWORD
    database_env: DEMO_DATABASE
    table: demo_meta.processes
    program_name_column: process_name
    script_code_column: script_code
    expected_target_column: expected_target
    batch_size: 7
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # pi-lens-ignore: python-path-traversal (TemporaryDirectory is test-controlled)
            path = Path(temp_dir) / "providers.yaml"
            path.write_text(config, encoding="utf-8")
            profiles = load_mysql_process_profiles(path)

        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles[0].name, "mysql_dev_demo")
        self.assertEqual(profiles[0].batch_size, 7)
        self.assertEqual(profiles[0].expected_target_column, "expected_target")


if __name__ == "__main__":
    unittest.main()
