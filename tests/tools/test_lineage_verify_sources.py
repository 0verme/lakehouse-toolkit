from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from shared.lineage.providers import MySQLProcessProfile
from shared.lineage.verification import SnapshotStatus, VerificationErrorCode
from tools.lineage import verify_sources


class LineageVerifySourcesCliTests(unittest.TestCase):
    def write_config(self, directory: str, content: str) -> Path:
        path = Path(directory) / "providers.yaml"
        path.write_text(content, encoding="utf-8")
        return path

    def run_cli(self, config: Path) -> tuple[int, str]:
        with self.assertLogs(verify_sources.LOGGER, level="ERROR") as captured:
            result = verify_sources.main(["--config", str(config)])
        return result, "\n".join(record.getMessage() for record in captured.records)

    def test_missing_pyyaml_is_reported_as_dependency_error(self):
        config_text = "mysql_process_profiles: []\n"
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, config_text)
            real_import = __import__

            def import_without_yaml(name, *args, **kwargs):
                if name == "yaml":
                    raise ModuleNotFoundError("No module named 'yaml'", name="yaml")
                return real_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=import_without_yaml):
                result, output = self.run_cli(config)

        self.assertEqual(result, 2)
        self.assertIn("stage=dependency status=FAILED dependency=PyYAML", output)
        self.assertIn(
            'hint="run: python -m pip install -r requirements.txt"',
            output,
        )
        self.assertNotIn("stage=config status=FAILED", output)

    def test_missing_config_is_classified(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "missing.yaml"
            result, output = self.run_cli(config)

        self.assertEqual(result, 2)
        self.assertIn("stage=config status=FAILED error=CONFIG_NOT_FOUND", output)

    def test_malformed_yaml_reports_location_without_content(self):
        secret = "VERY_SECRET_YAML_VALUE"
        config_text = (
            "mysql_process_profiles:\n"
            "  - name: demo\n"
            f"    password: {secret}\n"
            "    invalid: [unterminated\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, config_text)
            result, output = self.run_cli(config)

        self.assertEqual(result, 2)
        self.assertRegex(output, r"error=YAML_INVALID line=\d+ column=\d+")
        self.assertNotIn(secret, output)
        self.assertNotIn("unterminated", output)

    def test_yaml_root_must_be_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, "- VERY_SECRET_ROOT_VALUE\n")
            result, output = self.run_cli(config)

        self.assertEqual(result, 2)
        self.assertIn("error=CONFIG_INVALID", output)
        self.assertIn("reason=root must be a mapping", output)
        self.assertNotIn("VERY_SECRET_ROOT_VALUE", output)

    def test_mysql_process_profiles_must_be_list(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(
                directory,
                "mysql_process_profiles: VERY_SECRET_PROFILES_VALUE\n",
            )
            result, output = self.run_cli(config)

        self.assertEqual(result, 2)
        self.assertIn(
            "reason=mysql_process_profiles must be a list",
            output,
        )
        self.assertNotIn("VERY_SECRET_PROFILES_VALUE", output)

    def test_invalid_profile_reports_safe_field_level_reason(self):
        config_text = """\
mysql_process_profiles:
  - environment: DEV
    connection:
      host: 127.0.0.1
      port: 3306
      user: DEMO_USER
      password: VERY_SECRET_PASSWORD
      database: demo_meta
"""
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, config_text)
            result, output = self.run_cli(config)

        self.assertEqual(result, 2)
        self.assertIn(
            "reason=profile[0] missing required field: name",
            output,
        )
        self.assertNotIn("VERY_SECRET_PASSWORD", output)

    def test_unexpected_config_exception_only_reports_exception_class(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, "mysql_process_profiles: []\n")
            with patch.object(
                verify_sources,
                "load_mysql_process_profiles",
                side_effect=RuntimeError("password=VERY_SECRET_EXCEPTION_VALUE"),
            ):
                result, output = self.run_cli(config)

        self.assertEqual(result, 2)
        self.assertIn(
            "stage=config status=FAILED error=UNEXPECTED exception=RuntimeError",
            output,
        )
        self.assertNotIn("VERY_SECRET_EXCEPTION_VALUE", output)

    def test_valid_configuration_keeps_normal_execution_path(self):
        config_text = """\
mysql_process_profiles:
  - name: mysql_dev_demo
    environment: DEV
    connection_env:
      host: DEMO_HOST
      port: DEMO_PORT
      user: DEMO_USER
      password: DEMO_PASSWORD
      database: DEMO_DATABASE
"""
        profile = MySQLProcessProfile.from_mapping(
            {
                "name": "mysql_dev_demo",
                "environment": "DEV",
                "connection_env": {
                    "host": "DEMO_HOST",
                    "port": "DEMO_PORT",
                    "user": "DEMO_USER",
                    "password": "DEMO_PASSWORD",
                    "database": "DEMO_DATABASE",
                },
            }
        )
        report = SimpleNamespace(
            profiles=[],
            production_provider=SimpleNamespace(
                error_code=VerificationErrorCode.NOT_RUN.value,
                snapshot_status=SnapshotStatus.NOT_RUN.value,
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, config_text)
            with (
                patch.object(
                    verify_sources,
                    "verify_mysql_profiles",
                    return_value=[],
                ) as verify_profiles,
                patch.object(
                    verify_sources,
                    "build_verification_report",
                    return_value=report,
                ),
                patch.object(verify_sources, "write_json_report") as write_report,
            ):
                result = verify_sources.main(["--config", str(config)])

        self.assertEqual(result, 0)
        verify_profiles.assert_called_once_with(
            [profile], sample_limit=20, sample_only=False
        )
        write_report.assert_called_once()


if __name__ == "__main__":
    unittest.main()
