from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from typing import Any, cast

from tools.research.sqlglot_compare import (
    _load_jsonl,
    build_report,
    compare_sample,
    validate_privacy,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
CORPUS_PATH = ROOT_DIR / "tests" / "fixtures" / "research" / "sqlglot_corpus.jsonl"
SQLGLOT_AVAILABLE = importlib.util.find_spec("sqlglot") is not None


@unittest.skipUnless(SQLGLOT_AVAILABLE, "install requirements-research.txt")
class SQLGlotCompareTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.samples = _load_jsonl(CORPUS_PATH)

    def test_corpus_is_synthetic_and_privacy_safe(self):
        self.assertEqual(len(self.samples), 29)
        self.assertEqual(validate_privacy(self.samples), ())
        self.assertTrue(
            all(
                sample.origin in {"synthetic_reconstruction", "sanitized_shape"}
                for sample in self.samples
            )
        )

    def test_corpus_covers_required_shapes(self):
        shapes = {shape for sample in self.samples for shape in sample.shape}
        self.assertTrue(
            {
                "normal_sql",
                "complex_join",
                "cte",
                "nested_subquery",
                "alias",
                "merge",
                "update",
                "existing_parser_failure",
                "dialect_edge",
                "dynamic_sql_recovered",
            }.issubset(shapes)
        )

    def test_reviewed_disagreements_have_truth(self):
        by_id = {sample.sample_id: sample for sample in self.samples}

        insert_ignore = compare_sample(by_id["mysql_insert_ignore"])
        self.assertEqual(insert_ignore.truth_class, "LEGACY_MORE_CONSERVATIVE")
        self.assertEqual(
            insert_ignore.legacy_truth.mismatches, ("statement_type", "target")
        )

        insert_set = compare_sample(by_id["mysql_insert_set"])
        self.assertEqual(insert_set.truth_class, "SQLGLOT_MORE_CONSERVATIVE")
        self.assertEqual(insert_set.availability_class, "LEGACY_ONLY")
        self.assertEqual(insert_set.failure_classes, ("SQLGLOT_FAILED",))

        invalid = compare_sample(by_id["invalid_unclosed_comment"])
        self.assertEqual(invalid.truth_class, "LEGACY_MORE_AGGRESSIVE")
        self.assertEqual(invalid.failure_classes, ("SQLGLOT_FAILED",))
        self.assertGreater(invalid.legacy_truth.silent_wrong_target, 0)
        self.assertGreater(invalid.legacy_truth.silent_wrong_source, 0)

        dynamic = compare_sample(by_id["dynamic_sql_unresolved"])
        self.assertEqual(dynamic.availability_class, "BOTH_UNRESOLVED")
        self.assertEqual(dynamic.truth_class, "BOUNDARY_NOT_COMPARABLE")

        both_failed = compare_sample(by_id["replace_both_failed"])
        self.assertEqual(both_failed.availability_class, "BOTH_FAILED")
        self.assertEqual(
            both_failed.failure_classes,
            ("LEGACY_FAILED", "SQLGLOT_FAILED"),
        )

    def test_report_is_deterministic_without_benchmark_timings(self):
        first = build_report(self.samples, run_benchmark=False)
        second = build_report(self.samples, run_benchmark=False)
        self.assertEqual(first["corpus"], second["corpus"])
        self.assertEqual(first["summary"], second["summary"])
        self.assertEqual(first["comparisons"], second["comparisons"])
        self.assertEqual(first["truth_review"], second["truth_review"])

    def test_public_report_does_not_include_sql_or_asset_text(self):
        report = build_report(self.samples, run_benchmark=False)
        rendered = json.dumps(report, ensure_ascii=False)
        self.assertNotIn("INSERT INTO", rendered)
        self.assertNotIn("DEMO_", rendered)
        self.assertNotIn("executor.do", rendered)
        corpus = cast(dict[str, Any], report["corpus"])
        sanitization = cast(dict[str, Any], corpus["sanitization"])
        self.assertFalse(sanitization["report_contains_sql_text"])

    def test_benchmark_records_quantiles_and_relative_cost(self):
        report = build_report(
            self.samples,
            run_benchmark=True,
            benchmark_repeats=2,
            benchmark_warmup=0,
        )
        benchmark = cast(dict[str, Any], report["benchmark"])
        self.assertIsNotNone(benchmark)
        self.assertEqual(benchmark["sample_count"], 26)
        for backend in ("legacy", "legacy_sql_surface", "sqlglot"):
            backend_summary = cast(dict[str, Any], benchmark[backend])
            self.assertEqual(backend_summary["operation_count"], 52)
            self.assertGreaterEqual(backend_summary["p50_ms_per_sample"], 0)
            self.assertGreaterEqual(backend_summary["p95_ms_per_sample"], 0)
            self.assertGreaterEqual(backend_summary["max_ms_per_sample"], 0)
        relative_cost = cast(dict[str, Any], benchmark["relative_cost"])
        self.assertIn(
            "total_runtime_multiple_vs_legacy_sql_surface",
            relative_cost,
        )


if __name__ == "__main__":
    unittest.main()
