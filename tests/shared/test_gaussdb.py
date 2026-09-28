import importlib
import json
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

gaussdb = importlib.import_module("shared.db.gaussdb")
WORK_TMP = Path("runtime/temp/tests_gaussdb")
WORK_TMP.mkdir(parents=True, exist_ok=True)


def make_temp_dir():
    path = WORK_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


class GaussDbConfigTests(unittest.TestCase):
    def test_load_db_profiles_merges_defaults(self):
        tmp = make_temp_dir()
        try:
            config_path = tmp / "database.yaml"
            config_path.write_text(
                json.dumps(
                    {
                        "defaults": {
                            "driver": "demo.Driver",
                            "jar_path": "C:/jdbc/demo.jar",
                        },
                        "profiles": {
                            "demo": {
                                "jdbc_url": "jdbc:demo://127.0.0.1:5432/demo",
                                "user": "demo_user",
                                "password_env": "PYTOOLS_DEMO_DB_PASSWORD",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(gaussdb, "CONFIG_PATH", config_path):
                profiles = gaussdb.load_db_profiles()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertIn("demo", profiles)
        self.assertEqual(profiles["demo"]["driver"], "demo.Driver")
        self.assertEqual(profiles["demo"]["jar_path"], "C:/jdbc/demo.jar")
        self.assertEqual(profiles["demo"]["user"], "demo_user")

    def test_get_db_profile_applies_defaults(self):
        tmp = make_temp_dir()
        try:
            config_path = tmp / "database.yaml"
            config_path.write_text(
                json.dumps(
                    {
                        "profiles": {
                            "demo": {
                                "jdbc_url": "jdbc:demo://127.0.0.1:5432/demo",
                                "user": "demo_user",
                                "password_env": "PYTOOLS_DEMO_DB_PASSWORD",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(gaussdb, "CONFIG_PATH", config_path):
                profile = gaussdb.get_db_profile("demo")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(profile["driver"], "com.huawei.gauss200.jdbc.Driver")
        self.assertTrue(
            profile["jar_path"]
            .replace("\\", "/")
            .endswith("resources/jars/gaussdb200.jar")
        )
        self.assertEqual(profile["jdbc_url"], "jdbc:demo://127.0.0.1:5432/demo")

    def test_database_yaml_legacy_profile_and_relative_jar_path(self):
        tmp = make_temp_dir()
        try:
            config_path = tmp / "database.yaml"
            config_path.write_text(
                json.dumps(
                    {
                        "defaults": {"jar_path": "resources/jars/gaussdb200.jar"},
                        "profiles": {
                            "legacy": {
                                "jdbc_url": "jdbc:gaussdb://db.example.invalid:5432/demo",
                                "user": "demo_user",
                                "password": "placeholder-only",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.object(gaussdb, "CONFIG_PATH", tmp / "database.local.yaml"),
                patch.object(gaussdb, "GENERIC_CONFIG_PATH", config_path),
                patch.object(gaussdb, "EXAMPLE_CONFIG_PATH", tmp / "missing.yaml"),
                patch.object(gaussdb, "ROOT_DIR", tmp),
            ):
                profile = gaussdb.get_db_profile("legacy")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(profile["password"], "placeholder-only")
        self.assertEqual(profile["driver"], "com.huawei.gauss200.jdbc.Driver")
        self.assertEqual(
            Path(profile["jar_path"]), tmp / "resources/jars/gaussdb200.jar"
        )

    def test_connect_uses_the_legacy_jdbc_profile_boundary(self):
        tmp = make_temp_dir()
        try:
            jar_path = tmp / "gaussdb200.jar"
            jar_path.write_bytes(b"test-only placeholder")
            connect = Mock(return_value=object())
            jdbc = type("JayDeBeApiStub", (), {"connect": connect})()
            profile = {
                "driver": "com.huawei.gauss200.jdbc.Driver",
                "jdbc_url": "jdbc:gaussdb://db.example.invalid:5432/demo",
                "user": "demo_user",
                "password": "placeholder-only",
                "jar_path": str(jar_path),
            }
            with (
                patch.object(gaussdb, "get_db_profile", return_value=profile),
                patch.object(gaussdb.importlib, "import_module", return_value=jdbc),
            ):
                connection = gaussdb.connect_with_profile("legacy")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertIsNotNone(connection)
        connect.assert_called_once_with(
            "com.huawei.gauss200.jdbc.Driver",
            "jdbc:gaussdb://db.example.invalid:5432/demo",
            ["demo_user", "placeholder-only"],
            str(jar_path),
        )

    def test_local_database_config_has_precedence(self):
        tmp = make_temp_dir()
        try:
            local_path = tmp / "database.local.yaml"
            generic_path = tmp / "database.yaml"
            local_path.write_text(
                json.dumps({"profiles": {"selected": {"user": "local"}}}),
                encoding="utf-8",
            )
            generic_path.write_text(
                json.dumps({"profiles": {"selected": {"user": "generic"}}}),
                encoding="utf-8",
            )
            with (
                patch.object(gaussdb, "CONFIG_PATH", local_path),
                patch.object(gaussdb, "GENERIC_CONFIG_PATH", generic_path),
            ):
                profiles = gaussdb.load_db_profiles()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(profiles["selected"]["user"], "local")


if __name__ == "__main__":
    unittest.main()
