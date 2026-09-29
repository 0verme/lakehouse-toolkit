import unittest
from typing import ClassVar

from shared.graph.dependency import parse_job_dependencies
from shared.graph.dependency_splitter import (
    DependencyClosureTooLargeError,
    DuplicateIdentifierError,
    split_rows_preserving_dependencies,
)


class DependencySplitterTests(unittest.TestCase):
    columns: ClassVar[list[str]] = ["job_name", "dependencies"]

    def split(self, rows, max_rows=500):
        return split_rows_preserving_dependencies(
            self.columns,
            rows,
            id_column="job_name",
            dependency_column="dependencies",
            max_rows_per_chunk=max_rows,
        )

    def assert_chunk_invariant(self, rows, result):
        source_ids = {str(row["job_name"]).strip() for row in rows if row.get("job_name")}
        for chunk in result.chunks:
            chunk_ids = {str(row["job_name"]).strip() for row in chunk}
            for row in chunk:
                for dependency in parse_job_dependencies(row.get("dependencies")):
                    if dependency in source_ids:
                        self.assertIn(
                            dependency,
                            chunk_ids,
                            msg=f"{row['job_name']} missing {dependency} in chunk",
                        )

    def test_chain_under_limit_remains_one_chunk(self):
        rows = [
            {"job_name": "A", "dependencies": ""},
            {"job_name": "B", "dependencies": "33:A"},
            {"job_name": "C", "dependencies": "33:B"},
        ]

        result = self.split(rows)

        self.assertEqual(len(result.chunks), 1)
        self.assertEqual([row["job_name"] for row in result.chunks[0]], ["A", "B", "C"])
        self.assert_chunk_invariant(rows, result)

    def test_ordinary_split_covers_every_job_and_all_chunks_fit(self):
        rows = [{"job_name": "A", "dependencies": ""}]
        rows.extend(
            {"job_name": f"JOB_{index}", "dependencies": "33:A"}
            for index in range(1, 7)
        )

        result = self.split(rows, max_rows=3)

        self.assertGreater(len(result.chunks), 1)
        self.assertTrue(all(len(chunk) <= 3 for chunk in result.chunks))
        self.assertEqual(
            {row["job_name"] for chunk in result.chunks for row in chunk},
            {row["job_name"] for row in rows},
        )
        self.assertGreater(
            sum(len(chunk) for chunk in result.chunks), result.diagnostics.valid_id_count
        )
        self.assert_chunk_invariant(rows, result)

    def test_cross_chunk_dependency_closure_is_copied(self):
        rows = [
            {"job_name": "A", "dependencies": ""},
            {"job_name": "B", "dependencies": "33:A"},
            {"job_name": "C", "dependencies": "33:B"},
            {"job_name": "X", "dependencies": ""},
            {"job_name": "D", "dependencies": "33:C"},
        ]

        result = self.split(rows, max_rows=4)

        self.assertEqual(len(result.chunks), 2)
        self.assertEqual(
            {row["job_name"] for row in result.chunks[1]}, {"A", "B", "C", "D"}
        )
        self.assert_chunk_invariant(rows, result)

    def test_duplicate_identifier_is_rejected_with_data_rows(self):
        rows = [
            {"job_name": "JOB_A", "dependencies": ""},
            {"job_name": "JOB_B", "dependencies": ""},
            {"job_name": "JOB_A", "dependencies": ""},
        ]

        with self.assertRaisesRegex(
            DuplicateIdentifierError, r"JOB_A.*数据行 1、3.*2 次"
        ):
            self.split(rows)

    def test_empty_identifiers_are_excluded_and_reported(self):
        rows = [
            {"job_name": "A", "dependencies": ""},
            {"job_name": "  ", "dependencies": "33:A"},
            {"job_name": None, "dependencies": ""},
        ]

        result = self.split(rows)

        self.assertEqual(result.diagnostics.original_data_rows, 3)
        self.assertEqual(result.diagnostics.valid_id_count, 1)
        self.assertEqual(result.diagnostics.empty_id_rows, (2, 3))
        self.assertEqual([[row["job_name"] for row in chunk] for chunk in result.chunks], [["A"]])

    def test_missing_dependencies_are_reported_without_fabricated_rows(self):
        rows = [
            {"job_name": "B", "dependencies": "33:MISSING|33:MISSING"},
            {"job_name": "A", "dependencies": ""},
        ]

        result = self.split(rows, max_rows=1)

        self.assertEqual(result.diagnostics.missing_dependencies, ("MISSING",))
        self.assertEqual(result.diagnostics.missing_dependency_count, 1)
        self.assertEqual(result.diagnostics.missing_dependency_references, 1)
        self.assertNotIn(
            "MISSING", {row["job_name"] for chunk in result.chunks for row in chunk}
        )
        self.assert_chunk_invariant(rows, result)

    def test_cycle_is_reported_and_splits_without_recursion_or_duplicates(self):
        rows = [
            {"job_name": "A", "dependencies": "33:B"},
            {"job_name": "B", "dependencies": "33:A"},
            {"job_name": "X", "dependencies": ""},
        ]

        result = self.split(rows, max_rows=2)

        self.assertTrue(result.diagnostics.cycles)
        self.assertTrue(
            any(set(cycle[:-1]) == {"A", "B"} for cycle in result.diagnostics.cycles)
        )
        self.assertEqual(
            {row["job_name"] for row in result.chunks[0]}, {"A", "B"}
        )
        self.assertTrue(all(len(chunk) <= 2 for chunk in result.chunks))
        self.assert_chunk_invariant(rows, result)

    def test_single_closure_larger_than_limit_is_rejected(self):
        rows = [
            {"job_name": "A", "dependencies": ""},
            {"job_name": "B", "dependencies": "33:A"},
            {"job_name": "C", "dependencies": "33:B"},
        ]

        with self.assertRaisesRegex(DependencyClosureTooLargeError, "超过上限 2"):
            self.split(rows, max_rows=2)

    def test_non_positive_limit_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "必须大于 0"):
            self.split([], max_rows=0)


if __name__ == "__main__":
    unittest.main()
