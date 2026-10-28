from __future__ import annotations

import unittest

from scripts.load_check import assert_exhaustion_results


class ExhaustionAssertionTests(unittest.TestCase):
    def test_exact_exhaustion_matches_reserved_stock(self) -> None:
        assert_exhaustion_results(
            requests=300,
            quantity=1,
            initial_stock=50,
            remaining_stock=0,
            completed=50,
            failed=250,
            rate_limited=0,
        )

    def test_exhaustion_rejects_rate_limited_runs(self) -> None:
        with self.assertRaisesRegex(AssertionError, "rate-limited"):
            assert_exhaustion_results(300, 1, 50, 0, 50, 249, 1)

    def test_exhaustion_rejects_incorrect_final_stock(self) -> None:
        with self.assertRaisesRegex(AssertionError, "remaining stock"):
            assert_exhaustion_results(300, 1, 50, 1, 50, 250, 0)


if __name__ == "__main__":
    unittest.main()
