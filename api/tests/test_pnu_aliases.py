from __future__ import annotations

import unittest

from app.pnu_aliases import pnu_query_candidates


class PnuQueryCandidatesTest(unittest.TestCase):
    def test_empty_and_short_values_keep_existing_behavior(self) -> None:
        self.assertEqual(pnu_query_candidates(""), [])
        self.assertEqual(pnu_query_candidates("  abc-123  "), ["abc-123"])

    def test_unrelated_pnu_only_adds_land_flag_fallback(self) -> None:
        self.assertEqual(
            pnu_query_candidates("1168010100112340001"),
            ["1168010100112340001", "1168010100012340001"],
        )

    def test_gwangju_current_code_falls_back_to_legacy_code(self) -> None:
        self.assertEqual(
            pnu_query_candidates("1224012000113160009"),
            [
                "1224012000113160009",
                "1224012000013160009",
                "2914012000113160009",
                "2914012000013160009",
            ],
        )

    def test_gwangju_legacy_code_falls_back_to_current_code(self) -> None:
        self.assertEqual(
            pnu_query_candidates("2914012000113160009"),
            [
                "2914012000113160009",
                "2914012000013160009",
                "1224012000113160009",
                "1224012000013160009",
            ],
        )

    def test_mountain_flag_can_fall_back_to_hub_flag(self) -> None:
        self.assertEqual(
            pnu_query_candidates("1224012000213160009"),
            [
                "1224012000213160009",
                "1224012000113160009",
                "2914012000213160009",
                "2914012000113160009",
            ],
        )

    def test_incheon_current_code_falls_back_to_exact_legacy_code(self) -> None:
        self.assertEqual(
            pnu_query_candidates("2827510100101230004"),
            [
                "2827510100101230004",
                "2827510100001230004",
                "2826010300101230004",
                "2826010300001230004",
            ],
        )

    def test_incheon_legacy_code_falls_back_to_exact_current_code(self) -> None:
        self.assertEqual(
            pnu_query_candidates("2826010300101230004"),
            [
                "2826010300101230004",
                "2826010300001230004",
                "2827510100101230004",
                "2827510100001230004",
            ],
        )

    def test_formatted_value_is_normalized_before_aliasing(self) -> None:
        self.assertEqual(
            pnu_query_candidates("PNU: 12240-12000-1-1316-0009"),
            [
                "1224012000113160009",
                "1224012000013160009",
                "2914012000113160009",
                "2914012000013160009",
            ],
        )


if __name__ == "__main__":
    unittest.main()
