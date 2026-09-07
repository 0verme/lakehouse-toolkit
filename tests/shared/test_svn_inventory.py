from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from shared.lineage import svn_inventory
from shared.lineage.svn_inventory import (
    DECODE_ERROR,
    DWF_LAYOUT,
    GRANDPARENT_MISMATCH,
    INVALID_PROGRAM_DIRECTORY,
    NO_MATCHED_FILES,
    NOT_ATTEMPTED,
    PATH_LAYOUT_ERROR,
    PROCESSING_LAYOUT,
    READ_ERROR,
    READABLE,
    SUCCESS,
    SVNProfile,
    SourceReadResult,
    UNSUPPORTED_LAYER,
    build_svn_verification_report,
    classify_svn_program_path,
    derive_primary_target_from_program_path,
    load_svn_profiles,
    read_python_source,
    scan_svn_profile,
)
from tools.lineage import verify_svn_sources

FIXTURE_ROOT = (
    Path(__file__).resolve().parents[1] / "fixtures" / "svn_inventory" / "production"
)


class SVNInventoryPathTests(unittest.TestCase):
    def test_parent_and_grandparent_are_authoritative(self):
        valid = (
            "/demo/DIDP_PROJECT_WORKSPACE/DWM/1.0/DWS_DWM/"
            "DWS_DWM.RESULT_A/005_DWS_DWM_RESULT_A_00.py"
        )
        mismatch = (
            "/demo/DIDP_PROJECT_WORKSPACE/DWM/1.0/DWS_DWM/DWS_DWP.RESULT_A/program.py"
        )
        malformed = (
            "/demo/DIDP_PROJECT_WORKSPACE/DWM/1.0/DWS_DWM/not_a_program/program.py"
        )

        self.assertEqual(derive_primary_target_from_program_path(valid), "DWM.RESULT_A")
        self.assertIsNone(derive_primary_target_from_program_path(mismatch))
        self.assertIsNone(derive_primary_target_from_program_path(malformed))
        self.assertIsNone(
            derive_primary_target_from_program_path(valid.removesuffix(".py"))
        )

    def test_table_prefix_is_not_removed(self):
        path = (
            "/demo/DIDP_PROJECT_WORKSPACE/DWM/1.0/DWS_DWM/"
            "DWS_DWM.DWS_RESULT_A/program.py"
        )
        self.assertEqual(
            derive_primary_target_from_program_path(path), "DWM.DWS_RESULT_A"
        )

    def test_windows_and_linux_paths_have_the_same_result(self):
        linux_path = (
            "/home/demo/DIDP_PROJECT_WORKSPACE/DWF/1.0/DWS_DWF/"
            "DWS_DWF.RESULT_C/program.py"
        )
        windows_path = (
            r"E:\demo\DIDP_PROJECT_WORKSPACE\DW_PROJECT\1.0\DWS_DWF"
            r"\DWS_DWF.RESULT_C\program.py"
        )
        self.assertEqual(
            derive_primary_target_from_program_path(linux_path), "DWF.RESULT_C"
        )
        self.assertEqual(
            derive_primary_target_from_program_path(windows_path), "DWF.RESULT_C"
        )

    def test_layout_classifier_reports_specific_mismatch(self):
        path = (
            FIXTURE_ROOT
            / "DIDP_PROJECT_WORKSPACE"
            / "DWM"
            / "1.0"
            / "DWS_DWM"
            / "DWS_DWP.RESULT_MISMATCH"
            / "mismatch.py"
        )
        result = classify_svn_program_path(path, PROCESSING_LAYOUT)
        self.assertFalse(result.matched_program_file)
        self.assertFalse(result.directory_pattern_valid)
        self.assertEqual(result.unresolved_reason, GRANDPARENT_MISMATCH)

        malformed = (
            FIXTURE_ROOT
            / "DIDP_PROJECT_WORKSPACE"
            / "DWM"
            / "1.0"
            / "DWS_DWM"
            / "not_a_program"
            / "malformed.py"
        )
        malformed_result = classify_svn_program_path(malformed, PROCESSING_LAYOUT)
        self.assertEqual(malformed_result.unresolved_reason, INVALID_PROGRAM_DIRECTORY)

        unsupported = (
            FIXTURE_ROOT
            / "DIDP_PROJECT_WORKSPACE"
            / "DX"
            / "1.0"
            / "DWS_DX"
            / "DWS_DX.UNSUPPORTED"
            / "unsupported.py"
        )
        unsupported_result = classify_svn_program_path(unsupported, PROCESSING_LAYOUT)
        self.assertFalse(unsupported_result.matched_program_file)
        self.assertEqual(unsupported_result.unresolved_reason, UNSUPPORTED_LAYER)
        self.assertEqual(
            classify_svn_program_path(
                "/tmp/not-a-python.txt", PROCESSING_LAYOUT
            ).unresolved_reason,
            svn_inventory.NOT_PYTHON,
        )


class SVNInventoryScanTests(unittest.TestCase):
    def profile(self, layout: str) -> SVNProfile:
        return SVNProfile(
            name=f"demo_{layout}",
            environment="PROD",
            root_path=FIXTURE_ROOT,
            layout=layout,
        )

    def test_processing_scan_uses_strict_layout_and_layer_counts(self):
        progress: list[tuple[str, int, int, int]] = []
        result = scan_svn_profile(
            self.profile(PROCESSING_LAYOUT),
            progress_callback=lambda *values: progress.append(values),
            progress_interval=1,
        )

        self.assertEqual(result.status, SUCCESS)
        self.assertEqual(result.matched_program_files, 2)
        self.assertEqual(result.layer_counts["DWM"], 1)
        self.assertEqual(result.layer_counts["DWP"], 1)
        self.assertGreater(result.unmatched_python_files, 0)
        self.assertGreater(result.primary_target_unresolved, 0)
        self.assertEqual(result.readable_files, 2)
        self.assertEqual(result.read_errors, 0)
        self.assertEqual(result.decode_errors, 0)
        self.assertTrue(progress)
        self.assertNotIn(
            ".svn/ignored.py", " ".join(item.relative_path for item in result.records)
        )
        self.assertNotIn(
            "__pycache__/ignored.py",
            " ".join(item.relative_path for item in result.records),
        )

        targets = {
            item.declared_primary_target
            for item in result.records
            if item.matched_program_file
        }
        self.assertEqual(
            targets,
            {"DWM.RESULT_A", "DWP.RESULT_B"},
        )
        unrelated = next(
            item for item in result.records if item.filename == "unrelated.py"
        )
        self.assertFalse(unrelated.matched_program_file)
        self.assertEqual(unrelated.read_status, NOT_ATTEMPTED)
        self.assertGreater(result.unresolved_reasons[UNSUPPORTED_LAYER], 0)

    def test_dwf_scan_is_separate_from_processing(self):
        result = scan_svn_profile(self.profile(DWF_LAYOUT))
        self.assertEqual(result.status, SUCCESS)
        self.assertEqual(result.matched_program_files, 1)
        self.assertEqual(result.layer_counts, {"DWF": 1})
        record = next(item for item in result.records if item.matched_program_file)
        self.assertEqual(record.layer, "DWF")
        self.assertEqual(record.declared_primary_target, "DWF.RESULT_C")

    def test_sample_limit_bounds_the_files_visited(self):
        result = scan_svn_profile(
            self.profile(PROCESSING_LAYOUT), sample_only=True, sample_limit=2
        )
        self.assertEqual(result.scanned_python_files, 2)
        self.assertEqual(len(result.records), 2)

    def test_empty_and_invalid_roots_have_distinct_scan_statuses(self):
        with tempfile.TemporaryDirectory() as directory:
            empty_root = Path(directory) / "empty"
            empty_root.mkdir()
            empty_profile = SVNProfile(
                name="demo_empty",
                environment="PROD",
                root_path=empty_root,
                layout=PROCESSING_LAYOUT,
            )
            self.assertEqual(scan_svn_profile(empty_profile).status, NO_MATCHED_FILES)

            invalid_root = Path(directory) / "invalid"
            invalid_root.mkdir()
            (invalid_root / "script.py").write_text(
                "print('not a program')", encoding="utf-8"
            )
            invalid_profile = SVNProfile(
                name="demo_invalid",
                environment="PROD",
                root_path=invalid_root,
                layout=PROCESSING_LAYOUT,
            )
            self.assertEqual(
                scan_svn_profile(invalid_profile).status, PATH_LAYOUT_ERROR
            )

    def test_coding_cookie_is_read_with_tokenize_open(self):
        with tempfile.TemporaryDirectory() as directory:
            path = (
                Path(directory)
                / "DIDP_PROJECT_WORKSPACE"
                / "DWM"
                / "1.0"
                / "DWS_DWM"
                / "DWS_DWM.RESULT_ENCODING"
                / "coding_header.py"
            )
            path.parent.mkdir(parents=True)
            path.write_bytes(b"# -*- coding: latin-1 -*-\nlabel = 'caf\xe9'\n")
            result = read_python_source(path)
        self.assertEqual(result.read_status, READABLE)
        self.assertIsNotNone(result.script_code)
        self.assertIn("latin-1", result.script_code or "")
        self.assertTrue((result.script_code or "").endswith("caf\xe9'\n"))

    def test_read_error_does_not_abort_the_profile(self):
        real_reader = svn_inventory.read_python_source

        def failing_reader(path):
            if Path(path).name == "005_DWS_DWM_RESULT_A_00.py":
                return SourceReadResult(None, READ_ERROR, READ_ERROR)
            return real_reader(path)

        with patch.object(
            svn_inventory, "read_python_source", side_effect=failing_reader
        ):
            result = scan_svn_profile(self.profile(PROCESSING_LAYOUT))

        self.assertEqual(result.status, READ_ERROR)
        self.assertEqual(result.read_errors, 1)
        self.assertEqual(result.decode_errors, 0)
        self.assertEqual(result.matched_program_files, 2)
        self.assertEqual(result.readable_files, 1)

    def test_decode_error_is_counted_separately(self):
        real_reader = svn_inventory.read_python_source

        def failing_reader(path):
            if Path(path).name == "005_DWS_DWP_RESULT_B_1_00.py":
                return SourceReadResult(None, DECODE_ERROR, DECODE_ERROR)
            return real_reader(path)

        with patch.object(
            svn_inventory, "read_python_source", side_effect=failing_reader
        ):
            result = scan_svn_profile(self.profile(PROCESSING_LAYOUT))

        self.assertEqual(result.status, DECODE_ERROR)
        self.assertEqual(result.decode_errors, 1)
        self.assertEqual(result.read_errors, 0)
        self.assertEqual(result.readable_files, 1)


class SVNInventoryConfigAndReportTests(unittest.TestCase):
    def test_loads_only_local_profile_fields(self):
        config = """
svn_profiles:
  - name: prod_svn_processing
    environment: PROD
    root_path: E:/demo/svn/production
    layout: processing
  - name: prod_svn_dwf
    environment: PROD
    root_path: /home/demo/svn/production
    layout: dwf
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "providers.yaml"
            path.write_text(config, encoding="utf-8")
            profiles = load_svn_profiles(path)

        self.assertEqual(
            [profile.name for profile in profiles],
            [
                "prod_svn_processing",
                "prod_svn_dwf",
            ],
        )
        self.assertEqual(profiles[0].layout, PROCESSING_LAYOUT)
        self.assertEqual(profiles[1].layout, DWF_LAYOUT)

    def test_report_contains_no_path_filename_target_or_source(self):
        result = scan_svn_profile(self._profile(PROCESSING_LAYOUT))
        report = build_svn_verification_report([result], sample_only=True)
        serialized = json.dumps(report, ensure_ascii=False)

        self.assertIn("scanned_python_files", serialized)
        self.assertIn("directory_pattern_valid", serialized)
        self.assertNotIn("DWM.RESULT_A", serialized)
        self.assertNotIn("005_DWS_DWM_RESULT_A_00.py", serialized)
        self.assertNotIn(str(FIXTURE_ROOT), serialized)
        self.assertNotIn("Fictional DWM fixture", serialized)

    def _profile(self, layout: str) -> SVNProfile:
        return SVNProfile(
            name="demo_report",
            environment="PROD",
            root_path=FIXTURE_ROOT,
            layout=layout,
        )


class VerifySVNSourcesCLITests(unittest.TestCase):
    def write_config(self, directory: str, root: str) -> Path:
        config = (
            "svn_profiles:\n"
            "  - name: prod_svn_processing\n"
            "    environment: PROD\n"
            f"    root_path: {root}\n"
            "    layout: processing\n"
            "  - name: prod_svn_dwf\n"
            "    environment: PROD\n"
            f"    root_path: {root}\n"
            "    layout: dwf\n"
        )
        path = Path(directory) / "providers.yaml"
        path.write_text(config, encoding="utf-8")
        return path

    def test_sample_cli_emits_observable_success_and_safe_report(self):
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(directory, str(FIXTURE_ROOT))
            output = Path(directory) / "report.json"
            console = io.StringIO()
            with redirect_stdout(console):
                exit_code = verify_svn_sources.main(
                    [
                        "--config",
                        str(config),
                        "--sample-only",
                        "--sample-limit",
                        "20",
                        "--output",
                        str(output),
                    ]
                )

            text = console.getvalue()
            report_text = output.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 0)
        self.assertIn(
            "stage=svn_scan profile=prod_svn_processing environment=PROD status=SUCCESS",
            text,
        )
        self.assertIn(
            "stage=svn_scan profile=prod_svn_dwf environment=PROD status=SUCCESS",
            text,
        )
        self.assertNotIn("RESULT_A", report_text)
        self.assertNotIn("005_DWS", report_text)
        self.assertNotIn(str(FIXTURE_ROOT), report_text)
        report = json.loads(report_text)
        self.assertEqual(report["sample_only"], True)
        self.assertEqual(len(report["profiles"]), 2)

    def test_missing_root_has_a_specific_status(self):
        with tempfile.TemporaryDirectory() as directory:
            missing_root = str(Path(directory) / "does-not-exist")
            config = self.write_config(directory, missing_root)
            console = io.StringIO()
            with redirect_stdout(console):
                exit_code = verify_svn_sources.main(
                    [
                        "--config",
                        str(config),
                        "--output",
                        str(Path(directory) / "report.json"),
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertIn("status=ROOT_NOT_FOUND", console.getvalue())
        self.assertNotIn(missing_root, console.getvalue())

    def test_invalid_config_is_classified_without_values(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "providers.yaml"
            config.write_text("svn_profiles: []\n", encoding="utf-8")
            console = io.StringIO()
            with redirect_stdout(console):
                exit_code = verify_svn_sources.main(["--config", str(config)])

        self.assertEqual(exit_code, 2)
        self.assertIn(
            "stage=config status=FAILED error=CONFIG_ERROR", console.getvalue()
        )


if __name__ == "__main__":
    unittest.main()
