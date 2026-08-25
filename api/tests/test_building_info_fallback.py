from __future__ import annotations

import unittest
from unittest.mock import patch

from app import main


class _EmptyCursor:
    def __init__(self, queries: list[str]) -> None:
        self._queries = queries

    def __enter__(self) -> "_EmptyCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self._queries.append(query)

    def fetchall(self) -> list[object]:
        return []


class _EmptyConnection:
    def __init__(self, queries: list[str]) -> None:
        self._queries = queries

    def __enter__(self) -> "_EmptyConnection":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def cursor(self) -> _EmptyCursor:
        return _EmptyCursor(self._queries)


class BuildingInfoFallbackTest(unittest.TestCase):
    def test_empty_current_stores_do_not_query_removed_legacy_views(self) -> None:
        queries: list[str] = []

        with (
            patch.object(main, "_pnu_query_candidates", return_value=["4127110900200510003"]),
            patch.object(main, "_fetch_dataset_pnu_kv_payload", return_value=None),
            patch.object(main, "_active_release", return_value={"id": 23}),
            patch.object(main, "_db_url", return_value="postgresql://test"),
            patch.object(main.psycopg, "connect", side_effect=lambda *_args, **_kwargs: _EmptyConnection(queries)),
        ):
            result = main._fetch_building_info_line("4127110900200510003")

        self.assertIsNone(result)
        self.assertEqual(len(queries), 2)
        self.assertIn("FROM building_info_line", queries[0])
        self.assertIn("FROM dataset_record", queries[1])
        self.assertFalse(any("_line_v" in query for query in queries))


if __name__ == "__main__":
    unittest.main()
