from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from shared.lineage.environment_scope import (
    DISABLED_LINEAGE_ENVIRONMENT,
    UNKNOWN_LINEAGE_ENVIRONMENT,
    LineageEnvironmentScope,
    LineageEnvironmentScopeError,
    LineageEnvironmentScopeResolver,
    load_lineage_environment_scopes,
)


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

    def test_loads_public_scope_contract_from_yaml(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lineage_scopes.yaml"
            path.write_text(
                """
scopes:
  - name: demo_dev
    environment: DEMO_DEV
    sql_source_profile: DEMO_SQL_PROFILE
    schedule_source_profile: DEMO_SCHEDULE_PROFILE
    label: 示例开发环境
    dws_profile: demo
    enabled: true
""",
                encoding="utf-8",
            )

            scopes = load_lineage_environment_scopes(path)

        self.assertEqual(scopes, (self.enabled,))

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


if __name__ == "__main__":
    unittest.main()
