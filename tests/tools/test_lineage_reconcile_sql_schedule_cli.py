from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from shared.lineage.environment_scope import LineageEnvironmentScope
from tools.lineage import reconcile_sql_schedule


class ReconciliationCliScopeResolutionTests(unittest.TestCase):
    def test_cli_resolves_dws_and_source_profiles_from_environment_scope(self):
        scope = LineageEnvironmentScope(
            name="demo_dev",
            environment="DEMO_DEV",
            sql_source_profile="DEMO_SQL_PROFILE",
            schedule_source_profile="DEMO_SCHEDULE_PROFILE",
            label="synthetic scope",
            dws_profile="DEMO_DWS_PROFILE",
        )
        resolver = SimpleNamespace(resolve=Mock(return_value=scope))
        output = io.StringIO()
        with (
            patch.object(
                reconcile_sql_schedule,
                "load_lineage_environment_scope_resolver",
                return_value=resolver,
            ) as load_resolver,
            patch.object(
                reconcile_sql_schedule,
                "run",
                return_value=object(),
            ) as run_reconciliation,
            patch.object(reconcile_sql_schedule, "_render", return_value="report\n"),
            redirect_stdout(output),
        ):
            code = reconcile_sql_schedule.cli(
                [
                    "--environment",
                    "DEMO_DEV",
                    "--target",
                    "DWM.RESULT_A",
                ]
            )

        self.assertEqual(code, 0)
        load_resolver.assert_called_once_with()
        resolver.resolve.assert_called_once_with("DEMO_DEV")
        self.assertEqual(
            run_reconciliation.call_args.kwargs,
            {
                "dws_profile": "DEMO_DWS_PROFILE",
                "environment": "DEMO_DEV",
                "sql_source_profile": "DEMO_SQL_PROFILE",
                "schedule_source_profile": "DEMO_SCHEDULE_PROFILE",
                "source_profile": None,
                "target_table": "DWM.RESULT_A",
                "target_tables": None,
                "timing": run_reconciliation.call_args.kwargs["timing"],
            },
        )
        self.assertEqual(output.getvalue(), "report\n")


if __name__ == "__main__":
    unittest.main()
