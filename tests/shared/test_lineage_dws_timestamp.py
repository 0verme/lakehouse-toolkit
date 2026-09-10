from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from shared.lineage.dws_timestamp import (
    TIMESTAMPTZ_PARAM_SQL,
    dws_timestamp_param,
    dws_timestamp_projection,
    normalize_dws_timestamp_text,
    parse_dws_timestamp,
)


class DWSTimestampContractTests(unittest.TestCase):
    def test_write_and_projection_contract_are_explicit(self) -> None:
        value = datetime(
            2026,
            9,
            10,
            18,
            56,
            48,
            581400,
            tzinfo=timezone(timedelta(hours=8)),
        )

        self.assertEqual(
            dws_timestamp_param(value, "observed_at"),
            "2026-09-10T18:56:48.581400+08:00",
        )
        self.assertIsNone(dws_timestamp_param(None, "published_at"))
        self.assertEqual(
            TIMESTAMPTZ_PARAM_SQL,
            "CAST(? AS TIMESTAMP WITH TIME ZONE)",
        )
        self.assertEqual(
            dws_timestamp_projection("e.observed_at"),
            "CAST(e.observed_at AS VARCHAR(128)) AS observed_at",
        )

        with self.assertRaisesRegex(ValueError, "timezone offset"):
            dws_timestamp_param(
                datetime(2026, 9, 10, 18, 56, 48),
                "observed_at",
            )

    def test_offsets_are_normalized_for_python_310(self) -> None:
        expected = datetime(
            2026,
            1,
            15,
            3,
            4,
            5,
            123456,
            tzinfo=timezone.utc,
        )
        values = {
            "2026-01-15 11:04:05.123456+08": (
                expected,
                "2026-01-15 11:04:05.123456+08:00",
            ),
            "2026-01-15 03:04:05.123456+00": (
                expected,
                "2026-01-15 03:04:05.123456+00:00",
            ),
            "2026-01-15 11:04:05.123456+08:00": (
                expected,
                "2026-01-15 11:04:05.123456+08:00",
            ),
            "2026-01-15 11:04:05.123456+0800": (
                expected,
                "2026-01-15 11:04:05.123456+08:00",
            ),
            "2026-01-15 08:04:05.123456+05": (
                expected,
                "2026-01-15 08:04:05.123456+05:00",
            ),
            "2026-01-15 08:34:05.123456+0530": (
                expected,
                "2026-01-15 08:34:05.123456+05:30",
            ),
            "2026-01-15T03:04:05.123456Z": (
                expected,
                "2026-01-15T03:04:05.123456+00:00",
            ),
        }
        for text, (expected_value, normalized) in values.items():
            with self.subTest(text=text):
                parsed = parse_dws_timestamp(text, "observed_at")
                self.assertEqual(parsed, expected_value)
                self.assertEqual(normalize_dws_timestamp_text(text), normalized)

    def test_fractional_seconds_are_padded_from_one_to_six_digits(self) -> None:
        for digits in ("1", "12", "123", "1234", "12345", "123456"):
            with self.subTest(digits=digits):
                parsed = parse_dws_timestamp(
                    f"2026-09-10 18:56:48.{digits}+08:00",
                    "observed_at",
                )
                self.assertEqual(parsed.microsecond, int(digits.ljust(6, "0")))
                self.assertEqual(parsed.utcoffset(), timedelta(hours=8))

    def test_real_dws_failure_fixture_round_trips_as_aware_datetime(self) -> None:
        parsed = parse_dws_timestamp(
            "2026-09-10 18:56:48.5814+08:00",
            "observed_at",
        )

        self.assertTrue(parsed.tzinfo is not None)
        self.assertEqual(parsed.microsecond, 581400)
        self.assertEqual(parsed.utcoffset(), timedelta(hours=8))

    def test_naive_timestamps_are_rejected_and_instants_compare_correctly(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone offset"):
            parse_dws_timestamp("2026-09-10 18:56:48.581400", "observed_at")
        with self.assertRaisesRegex(ValueError, "timezone offset"):
            parse_dws_timestamp(
                datetime(2026, 9, 10, 18, 56, 48, 581400),
                "observed_at",
            )

        utc = parse_dws_timestamp("2026-09-10 10:56:48.581400+00", "observed_at")
        local = parse_dws_timestamp("2026-09-10 18:56:48.5814+08:00", "observed_at")
        later = parse_dws_timestamp("2026-09-10 10:56:49+00:00", "observed_at")
        self.assertEqual(utc, local)
        self.assertNotEqual(utc, later)


if __name__ == "__main__":
    unittest.main()
