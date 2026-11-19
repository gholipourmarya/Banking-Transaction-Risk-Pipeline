import unittest
from datetime import datetime, timezone

from streaming.generate import generate_rows


class GenerateTests(unittest.TestCase):
    def test_reproducible_transactions_include_risk_and_quality_cases(self):
        start = datetime(2025, 1, 1, 12, tzinfo=timezone.utc)
        rows = list(generate_rows(1001, 42, start, "TEST"))
        self.assertEqual(rows, list(generate_rows(1001, 42, start, "TEST")))
        self.assertEqual(len({row["transaction_id"] for row in rows}), 1001)
        self.assertEqual(len({row["account_id"] for row in rows[:8]}), 1)
        self.assertGreaterEqual(float(rows[8]["amount"]), 10_000)
        self.assertEqual(rows[9]["amount"], "-1")
        self.assertEqual(rows[0]["event_at"], "2025-01-01T12:00:00.000Z")
        self.assertEqual(rows[-1]["event_at"], "2025-01-01T12:00:20.000Z")
