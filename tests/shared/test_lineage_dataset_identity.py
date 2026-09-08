from __future__ import annotations

import unittest
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from shared.lineage import (
    DatasetIdentity,
    LineageEdge,
    MaterializationBatch,
    ProgramSource,
    SQLiteMaterializationStore,
    audit_program_physical_dag,
    build_program_physical_dag,
    materialize_batch,
    materialize_program,
    query_downstream,
)

OBSERVED_AT = datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc)


class DatasetIdentityContractTests(unittest.TestCase):
    def test_environment_is_a_hard_boundary(self):
        dev = DatasetIdentity.from_name("DEV200", "DWM.TABLE_A")
        other_dev = DatasetIdentity.from_name("DEV214", "DWM.TABLE_A")

        self.assertIsNotNone(dev)
        self.assertIsNotNone(other_dev)
        if dev is None or other_dev is None:
            self.fail("expected both environment-scoped identities")
        self.assertNotEqual(dev, other_dev)
        self.assertEqual(dev.key, ("DEV200", "DWM", "TABLE_A"))

    def test_schema_and_table_are_case_and_whitespace_insensitive(self):
        canonical = DatasetIdentity.from_name("DEV200", "DWM.TABLE_A")
        variants = (
            DatasetIdentity.from_name("DEV200", "dwm.table_a"),
            DatasetIdentity.from_name("DEV200", " Dwm . TABLE_A "),
        )

        self.assertTrue(all(value == canonical for value in variants))
        if canonical is None:
            self.fail("expected a canonical DatasetIdentity")
        self.assertEqual(canonical.canonical_name, "DWM.TABLE_A")
        self.assertEqual(canonical.to_dict()["canonical_schema"], "DWM")
        self.assertEqual(canonical.to_dict()["canonical_table"], "TABLE_A")

    def test_source_profile_is_provenance_not_dataset_identity(self):
        first = LineageEdge(
            environment="DEV200",
            source_profile="profile_a",
            source_table="dwm.table_a",
            target_table="DWM.TABLE_B",
            program_name="PROGRAM_A",
            evidence={"raw_source": "dwm.table_a"},
        )
        second = LineageEdge(
            environment="DEV200",
            source_profile="profile_b",
            source_table="DWM.TABLE_A",
            target_table="dwm.table_b",
            program_name="PROGRAM_A",
            evidence={"raw_source": "DWM.TABLE_A"},
        )

        self.assertEqual(
            first.source_dataset_identity,
            second.source_dataset_identity,
        )
        self.assertEqual(
            first.target_dataset_identity,
            second.target_dataset_identity,
        )
        self.assertNotEqual(first.source_profile, second.source_profile)
        self.assertNotEqual(first, second)
        self.assertEqual(first.evidence, {"raw_source": "dwm.table_a"})

    def test_identity_has_no_platform_or_catalog_and_rejects_extra_namespace(self):
        identity = DatasetIdentity("DEV200", "DWM", "TABLE_A")

        self.assertEqual(
            {field.name for field in fields(DatasetIdentity)},
            {"environment", "canonical_schema", "canonical_table"},
        )
        self.assertIsNone(DatasetIdentity.from_name("DEV200", "PLATFORM.DWM.TABLE_A"))
        self.assertIsNone(DatasetIdentity.from_name("DEV200", "CAT.DWM.TABLE_A"))
        self.assertEqual(identity.key, ("DEV200", "DWM", "TABLE_A"))

    def test_missing_schema_is_unresolved_and_does_not_create_formal_edge(self):
        self.assertIsNone(DatasetIdentity.from_name("DEV200", "TABLE_A"))

        source = ProgramSource(
            environment="DEV200",
            source_profile="fixture",
            program_name="PROGRAM_MISSING_SCHEMA",
            script_code="INSERT INTO ODS.TABLE_B SELECT * FROM TABLE_A",
            expected_target="ODS.TABLE_B",
        )
        dag = build_program_physical_dag(source)
        result = materialize_program(
            dag,
            batch_id="batch-missing-schema",
            observed_at=OBSERVED_AT,
        )

        self.assertIn(("TABLE_A", "ODS.TABLE_B"), dag.edge_pairs)
        self.assertEqual(result.edges, ())
        with self.assertRaisesRegex(ValueError, "qualified schema.table"):
            LineageEdge(
                environment="DEV200",
                source_profile="fixture",
                source_table="TABLE_A",
                target_table="DWM.TABLE_B",
            )

    def test_tmp_stays_in_physical_dag_and_is_not_a_dataset(self):
        source = ProgramSource(
            environment="DEV200",
            source_profile="fixture",
            program_name="PROGRAM_TMP_BOUNDARY",
            script_code=(
                "CREATE TEMP TABLE TMP_STAGE AS SELECT * FROM ODS.TABLE_A;"
                " INSERT INTO ODS.TABLE_B SELECT * FROM TMP_STAGE"
            ),
            expected_target="ODS.TABLE_B",
        )
        dag = build_program_physical_dag(source)
        result = materialize_program(
            dag,
            batch_id="batch-tmp-boundary",
            observed_at=OBSERVED_AT,
        )

        self.assertTrue(dag.node_map["TMP_STAGE"].is_temporary)
        self.assertIsNone(DatasetIdentity.from_name("DEV200", "DWM.TMP_STAGE"))
        self.assertEqual(len(result.edges), 1)
        self.assertEqual(
            result.edges[0].source_dataset_identity.canonical_table, "TABLE_A"
        )
        self.assertEqual(
            result.edges[0].target_dataset_identity.canonical_table, "TABLE_B"
        )
        self.assertNotIn(
            "TMP_STAGE",
            {
                result.edges[0].source_table,
                result.edges[0].target_table,
            },
        )

    def test_same_named_edges_from_multiple_environments_are_not_collapsed(self):
        audits = []
        for environment in ("DEV200", "DEV214"):
            source = ProgramSource(
                environment=environment,
                source_profile="fixture",
                program_name="PROGRAM_SAME_DATASET_NAMES",
                script_code="INSERT INTO ODS.TABLE_B SELECT * FROM ODS.TABLE_A",
                expected_target="ODS.TABLE_B",
            )
            audits.append(
                audit_program_physical_dag(
                    build_program_physical_dag(source),
                    batch_id="batch-environments",
                    observed_at=OBSERVED_AT,
                )
            )

        batch = materialize_batch(
            audits,
            batch_id="batch-environments",
            observed_at=OBSERVED_AT,
        )

        self.assertEqual(len(batch.edges), 2)
        self.assertEqual(
            {edge.source_dataset_identity.environment for edge in batch.edges},
            {"DEV200", "DEV214"},
        )
        self.assertEqual(
            {edge.source_dataset_identity.key[1:] for edge in batch.edges},
            {("ODS", "TABLE_A")},
        )

    def test_sqlite_query_canonicalizes_table_and_keeps_environment_boundary(self):
        edges = (
            LineageEdge(
                environment="DEV200",
                source_profile="fixture",
                source_table="dwm.table_a",
                target_table="dwm.table_b",
                program_name="PROGRAM_DEV200",
            ),
            LineageEdge(
                environment="DEV214",
                source_profile="fixture",
                source_table="DWM.TABLE_A",
                target_table="DWM.TABLE_C",
                program_name="PROGRAM_DEV214",
            ),
        )

        with TemporaryDirectory() as directory:
            store = SQLiteMaterializationStore(Path(directory) / "lineage.db")
            store.publish(
                MaterializationBatch(
                    batch_id="batch-query-boundary",
                    observed_at=OBSERVED_AT,
                    edges=edges,
                )
            )
            dev200 = query_downstream(store, " dwm.table_a ", "DEV200")
            dev214 = query_downstream(store, "DWM.TABLE_A", "DEV214")

        self.assertEqual(
            [node.table for node in dev200.nodes], ["DWM.TABLE_A", "DWM.TABLE_B"]
        )
        self.assertEqual(
            [node.table for node in dev214.nodes], ["DWM.TABLE_A", "DWM.TABLE_C"]
        )


if __name__ == "__main__":
    unittest.main()
