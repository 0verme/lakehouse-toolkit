from __future__ import annotations

import unittest
from dataclasses import fields
from datetime import datetime, timezone

from shared.lineage.domain import DatasetIdentity
from tests.fixtures.lineage.column_lineage_research import (
    ColumnAvailability,
    ColumnLineageStatus,
    ColumnMetadata,
    FakeMetadataProvider,
    MetadataSnapshot,
    MetadataStatus,
    infer_column_lineage,
    resolved_snapshot,
    stale_snapshot,
    synthetic_identity,
)


OBSERVED_AT = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)


def provider_with_core_schemas() -> FakeMetadataProvider:
    snapshots = {
        synthetic_identity("ODS.A"): resolved_snapshot(
            "ODS.A", ["ID", "A", "B", "X", "Y", "VALUE", "NAME"]
        ),
        synthetic_identity("DWF.B"): resolved_snapshot(
            "DWF.B", ["ID", "B_VALUE", "KEY"]
        ),
        synthetic_identity("ODS.SOURCE"): resolved_snapshot(
            "ODS.SOURCE", ["ID", "NAME", "VALUE"]
        ),
        synthetic_identity("DWM.TARGET"): resolved_snapshot(
            "DWM.TARGET", ["ID", "LABEL", "VALUE"]
        ),
        synthetic_identity("DWM.MERGED"): resolved_snapshot(
            "DWM.MERGED", ["ID", "VALUE"]
        ),
    }
    return FakeMetadataProvider(snapshots)


class MetadataProviderContractTests(unittest.TestCase):
    def test_contract_contains_identity_and_required_column_provenance(self):
        snapshot = resolved_snapshot(
            "ODS.A",
            ["ID", "VALUE"],
            observed_at=OBSERVED_AT,
            snapshot_version="catalog-17",
            source="dws-catalog",
        )

        self.assertEqual(snapshot.status, MetadataStatus.RESOLVED)
        self.assertEqual(snapshot.dataset, synthetic_identity("ODS.A"))
        self.assertEqual(snapshot.snapshot_version, "catalog-17")
        self.assertEqual(snapshot.observed_at, OBSERVED_AT)
        self.assertEqual(snapshot.source, "dws-catalog")
        self.assertEqual(
            {field.name for field in fields(ColumnMetadata)},
            {
                "name",
                "ordinal",
                "data_type",
                "snapshot_version",
                "observed_at",
                "availability",
                "source",
            },
        )
        self.assertEqual(snapshot.columns[0].ordinal, 1)
        self.assertEqual(snapshot.columns[0].availability, ColumnAvailability.AVAILABLE)

    def test_unknown_dataset_is_not_guessed(self):
        provider = FakeMetadataProvider({})

        snapshot = provider.get_columns(synthetic_identity("ODS.UNKNOWN"))

        self.assertEqual(snapshot.status, MetadataStatus.NOT_AVAILABLE)
        self.assertEqual(snapshot.columns, ())
        self.assertEqual(snapshot.availability, ColumnAvailability.NOT_AVAILABLE)

    def test_stale_snapshot_is_explicit_and_not_resolved(self):
        snapshot = stale_snapshot("ODS.A", ["ID", "VALUE"])
        provider = FakeMetadataProvider({snapshot.dataset: snapshot})

        result = infer_column_lineage("SELECT ID FROM ODS.A", provider)

        self.assertEqual(snapshot.status, MetadataStatus.STALE)
        self.assertEqual(result.status, ColumnLineageStatus.UNRESOLVED)
        self.assertEqual(result.dependencies, ())

    def test_provider_calls_are_cached_per_dataset_during_one_evaluation(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage(
            "SELECT A.ID, A.VALUE FROM ODS.A A JOIN ODS.A A2 ON A.ID = A2.ID",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(result.metadata_lookups, 1)
        self.assertEqual(provider.calls, [synthetic_identity("ODS.A")])


class SyntheticColumnLineageTests(unittest.TestCase):
    def test_select_star_requires_source_columns_and_expands_in_ordinal_order(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage("SELECT * FROM ODS.A", provider)

        self.assertEqual(result.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(
            result.output_columns,
            ("ID", "A", "B", "X", "Y", "VALUE", "NAME"),
        )
        self.assertEqual(
            {dependency.source_column for dependency in result.dependencies},
            {"ID", "A", "B", "X", "Y", "VALUE", "NAME"},
        )

    def test_select_star_without_metadata_is_unresolved_and_emits_no_fact(self):
        provider = FakeMetadataProvider({})

        result = infer_column_lineage("SELECT * FROM ODS.UNKNOWN", provider)

        self.assertEqual(result.status, ColumnLineageStatus.UNRESOLVED)
        self.assertEqual(result.dependencies, ())
        self.assertIn("source columns", result.reason)

    def test_explicit_alias_and_expression_dependencies_are_resolved(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage(
            "SELECT A.ID AS IDENTIFIER, A.A + A.B AS TOTAL FROM ODS.A A",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(
            {
                (dependency.output_column, dependency.source_column)
                for dependency in result.dependencies
            },
            {
                ("IDENTIFIER", "ID"),
                ("TOTAL", "A"),
                ("TOTAL", "B"),
            },
        )

    def test_case_expression_preserves_all_source_dependencies(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage(
            "SELECT CASE WHEN X THEN Y ELSE VALUE END AS RESULT FROM ODS.A",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(
            {dependency.source_column for dependency in result.dependencies},
            {"X", "Y", "VALUE"},
        )

    def test_join_with_unqualified_duplicate_column_is_ambiguous(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage(
            "SELECT ID FROM ODS.A A JOIN DWF.B B ON A.KEY = B.KEY",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.AMBIGUOUS)
        self.assertEqual(result.dependencies, ())

    def test_union_merges_ordinal_dependencies_without_guessing(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage(
            "SELECT ID FROM ODS.A UNION SELECT ID FROM DWF.B",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(
            {
                (dependency.source_dataset.canonical_name, dependency.source_column)
                for dependency in result.dependencies
            },
            {("ODS.A", "ID"), ("DWF.B", "ID")},
        )

    def test_cte_and_subquery_trace_back_to_physical_source_columns(self):
        provider = provider_with_core_schemas()

        cte = infer_column_lineage(
            "WITH BASE AS (SELECT ID, VALUE FROM ODS.A) "
            "SELECT VALUE FROM BASE",
            provider,
        )
        subquery = infer_column_lineage(
            "SELECT X.ID FROM (SELECT ID FROM ODS.A) X",
            provider,
        )

        self.assertEqual(cte.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(subquery.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(
            {dependency.source_column for dependency in cte.dependencies},
            {"VALUE"},
        )
        self.assertEqual(
            {dependency.source_column for dependency in subquery.dependencies},
            {"ID"},
        )

    def test_insert_select_uses_explicit_target_mapping(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage(
            "INSERT INTO DWM.TARGET (ID, LABEL) "
            "SELECT ID, NAME AS LABEL FROM ODS.SOURCE",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.RESOLVED)
        self.assertEqual(result.output_columns, ("ID", "LABEL"))
        self.assertEqual(
            {
                (dependency.output_column, dependency.source_column)
                for dependency in result.dependencies
            },
            {("ID", "ID"), ("LABEL", "NAME")},
        )

    def test_insert_star_without_target_metadata_is_only_partial(self):
        source = resolved_snapshot("ODS.SOURCE", ["ID", "NAME"])
        provider = FakeMetadataProvider({source.dataset: source})

        result = infer_column_lineage(
            "INSERT INTO DWM.UNKNOWN SELECT * FROM ODS.SOURCE",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.PARTIALLY_RESOLVED)
        self.assertEqual(result.dependencies, ())
        self.assertIn("target metadata", result.reason)

    def test_merge_is_explicitly_partial_even_when_assignments_are_known(self):
        provider = provider_with_core_schemas()

        result = infer_column_lineage(
            "MERGE INTO DWM.MERGED T USING ODS.SOURCE S ON T.ID = S.ID "
            "WHEN MATCHED THEN UPDATE SET T.VALUE = S.VALUE "
            "WHEN NOT MATCHED THEN INSERT (ID, VALUE) VALUES (S.ID, S.VALUE)",
            provider,
        )

        self.assertEqual(result.status, ColumnLineageStatus.PARTIALLY_RESOLVED)
        self.assertEqual(
            {
                (dependency.output_column, dependency.source_column)
                for dependency in result.dependencies
            },
            {("VALUE", "VALUE"), ("ID", "ID")},
        )
        self.assertIn("MERGE action predicates", result.reason)


if __name__ == "__main__":
    unittest.main()
