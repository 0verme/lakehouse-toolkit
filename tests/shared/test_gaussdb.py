import importlib
import json
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

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

        self.assertEqual(profile["driver"], gaussdb.DEFAULT_DRIVER)
        self.assertTrue(
            profile["jar_path"]
            .replace("\\", "/")
            .endswith("resources/jars/jdbc-driver.jar")
        )
        self.assertEqual(profile["jdbc_url"], "jdbc:demo://127.0.0.1:5432/demo")


if __name__ == "__main__":
    unittest.main()
