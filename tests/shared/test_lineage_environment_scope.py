from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import shared.lineage.environment_scope as environment_scope
from shared.lineage.environment_scope import (
    CONFIG_PATH,
    DISABLED_LINEAGE_ENVIRONMENT,
    EXAMPLE_CONFIG_PATH,
    LINEAGE_SCOPE_CONFIG_INVALID,
    LINEAGE_SCOPE_CONFIG_NOT_FOUND,
    UNKNOWN_LINEAGE_ENVIRONMENT,
    LineageEnvironmentScope,
    LineageEnvironmentScopeError,
    LineageEnvironmentScopeResolver,
    load_lineage_environment_scopes,
)
from shared.lineage.providers import load_mysql_process_profiles


class LineageEnvironmentScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.enabled = LineageEnvironmentScope(
            name="demo_dev",
            environment="DEMO_DEV",
            sql_source_profile="DEMO_SQL_PROFILE",
            schedule_source_profile="DEMO_SCHEDULE_PROFILE",
            label="示例开发环境",
            dws_profile="demo",
        )
        self.disabled = LineageEnvironmentScope(
            name="demo_disabled",
            environment="DEMO_DISABLED",
            sql_source_profile="DEMO_SQL_DISABLED",
            schedule_source_profile="DEMO_SCHEDULE_DISABLED",
            label="停用示例环境",
            dws_profile="demo",
            enabled=False,
        )

    def test_resolves_environment_to_split_profiles_and_dws_profile(self):
        resolver = LineageEnvironmentScopeResolver((self.enabled, self.disabled))

        resolved = resolver.resolve(" DEMO_DEV ")

        self.assertEqual(resolved, self.enabled)
        self.assertEqual(resolved.sql_source_profile, "DEMO_SQL_PROFILE")
        self.assertEqual(resolved.schedule_source_profile, "DEMO_SCHEDULE_PROFILE")
        self.assertEqual(resolved.dws_profile, "demo")
        self.assertEqual(resolver.enabled_scopes(), (self.enabled,))

    def test_unknown_environment_fails_with_code(self):
        resolver = LineageEnvironmentScopeResolver((self.enabled,))

        with self.assertRaisesRegex(
            LineageEnvironmentScopeError, UNKNOWN_LINEAGE_ENVIRONMENT
        ) as context:
            resolver.resolve("DEMO_UNKNOWN")

        self.assertEqual(context.exception.code, UNKNOWN_LINEAGE_ENVIRONMENT)

    def test_disabled_environment_fails_with_code(self):
        resolver = LineageEnvironmentScopeResolver((self.enabled, self.disabled))

        with self.assertRaisesRegex(
            LineageEnvironmentScopeError, DISABLED_LINEAGE_ENVIRONMENT
        ) as context:
            resolver.resolve("DEMO_DISABLED")

        self.assertEqual(context.exception.code, DISABLED_LINEAGE_ENVIRONMENT)

    def test_loads_scope_from_combined_provider_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lineage_providers.local.yaml"
            path.write_text(
                """
mysql_process_profiles:
  - name: mysql_dev_a
    environment: DEV
scopes:
  - name: demo_dev
    environment: DEMO_DEV
    sql_source_profile: mysql_dev_a
    schedule_source_profile: mysql_dev_a
    label: 示例开发环境
    dws_profile: demo
    enabled: true
""",
                encoding="utf-8",
            )

            scopes = load_lineage_environment_scopes(path)

        self.assertEqual(
            scopes,
            (
                LineageEnvironmentScope(
                    name="demo_dev",
                    environment="DEMO_DEV",
                    sql_source_profile="mysql_dev_a",
                    schedule_source_profile="mysql_dev_a",
                    label="示例开发环境",
                    dws_profile="demo",
                ),
            ),
        )

    def test_missing_local_config_falls_back_to_provider_example(self):
        with tempfile.TemporaryDirectory() as directory:
            missing_local = Path(directory) / "lineage_providers.local.yaml"
            example = Path(directory) / "lineage_providers.example.yaml"
            example.write_text(
                """
scopes:
  - name: demo_dev
    environment: DEMO_DEV
    sql_source_profile: mysql_dev_a
    schedule_source_profile: mysql_dev_a
    label: 示例开发环境
    dws_profile: demo
""",
                encoding="utf-8",
            )

            with patch.object(
                environment_scope, "CONFIG_PATH", missing_local
            ), patch.object(environment_scope, "EXAMPLE_CONFIG_PATH", example):
                scopes = load_lineage_environment_scopes()

        self.assertEqual(scopes[0].sql_source_profile, "mysql_dev_a")
        self.assertEqual(scopes[0].schedule_source_profile, "mysql_dev_a")

    def test_public_example_scopes_reference_profiles_in_same_config(self):
        scopes = load_lineage_environment_scopes(EXAMPLE_CONFIG_PATH)
        profile_names = {
            profile.name for profile in load_mysql_process_profiles(EXAMPLE_CONFIG_PATH)
        }

        self.assertTrue(scopes)
        for scope in scopes:
            self.assertIn(scope.sql_source_profile, profile_names)
            self.assertIn(scope.schedule_source_profile, profile_names)

    def test_missing_both_provider_configs_has_formal_not_found_code(self):
        with tempfile.TemporaryDirectory() as directory:
            missing_local = Path(directory) / "lineage_providers.local.yaml"
            missing_example = Path(directory) / "lineage_providers.example.yaml"
            with patch.object(
                environment_scope, "CONFIG_PATH", missing_local
            ), patch.object(environment_scope, "EXAMPLE_CONFIG_PATH", missing_example):
                with self.assertRaises(LineageEnvironmentScopeError) as context:
                    load_lineage_environment_scopes()

        self.assertEqual(context.exception.code, LINEAGE_SCOPE_CONFIG_NOT_FOUND)

    def test_invalid_scopes_structure_has_formal_invalid_code(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lineage_providers.yaml"
            path.write_text(
                "mysql_process_profiles: []\nscopes: invalid\n",
                encoding="utf-8",
            )

            with self.assertRaises(LineageEnvironmentScopeError) as context:
                load_lineage_environment_scopes(path)

        self.assertEqual(context.exception.code, LINEAGE_SCOPE_CONFIG_INVALID)

    def test_duplicate_environment_is_rejected(self):
        duplicate = LineageEnvironmentScope(
            name="another_name",
            environment="DEMO_DEV",
            sql_source_profile="SQL_B",
            schedule_source_profile="SCHEDULE_B",
            label="另一个名称",
            dws_profile="demo",
        )

        with self.assertRaisesRegex(ValueError, "unique"):
            LineageEnvironmentScopeResolver((self.enabled, duplicate))

    def test_duplicate_name_is_rejected(self):
        duplicate = LineageEnvironmentScope(
            name="demo_dev",
            environment="DEMO_OTHER",
            sql_source_profile="SQL_B",
            schedule_source_profile="SCHEDULE_B",
            label="另一个名称",
            dws_profile="demo",
        )

        with self.assertRaisesRegex(ValueError, "unique"):
            LineageEnvironmentScopeResolver((self.enabled, duplicate))

    def test_default_paths_use_combined_provider_config(self):
        self.assertEqual(CONFIG_PATH.name, "lineage_providers.local.yaml")
        self.assertEqual(EXAMPLE_CONFIG_PATH.name, "lineage_providers.example.yaml")


if __name__ == "__main__":
    unittest.main()
