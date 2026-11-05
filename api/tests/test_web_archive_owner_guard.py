"""Exercise the route's owner guard without a running database or real session."""
from __future__ import annotations

import ast
import asyncio
from pathlib import Path
import unittest


class FakeHttpException(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code


class FakeRequest:
    def __init__(self, expected: str | None):
        self.headers = {} if expected is None else {"x-expected-web-user-id": expected}

    async def json(self):
        return {"requestId": "web-archive:test", "snapshot": {"title": "test", "pages": ["<p>test</p>"]}, "html": "<p>test</p>"}


class FakeResponse:
    def __init__(self):
        self.headers = {}


class FakeArchives:
    def __init__(self):
        self.created = []

    def require_enabled(self, _settings):
        pass

    def create_web_snapshot(self, _settings, owner, payload):
        self.created.append((owner, payload))
        return {"ok": True, "id": "fixture"}


class WebArchiveOwnerGuardTest(unittest.TestCase):
    def test_cookie_principal_must_match_expected_web_owner(self):
        source = Path(__file__).parents[1] / "app" / "platform" / "archive_routes.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        function = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "archive_create")
        function.decorator_list = []
        archives = FakeArchives()
        namespace = {
            "Request": FakeRequest,
            "Response": FakeResponse,
            "HTTPException": FakeHttpException,
            "get_settings": lambda: object(),
            "archives": archives,
            "shared_session": lambda _settings, _request: {"auth_type": "cookie", "user_id": "platform-a", "external_id": "1"},
            "PRIVATE": {},
        }
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
        create = namespace["archive_create"]

        with self.assertRaises(FakeHttpException) as mismatch:
            asyncio.run(create(FakeRequest("2"), FakeResponse()))
        self.assertEqual(mismatch.exception.status_code, 409)
        self.assertEqual(archives.created, [])
        self.assertEqual(asyncio.run(create(FakeRequest("1"), FakeResponse()))["id"], "fixture")
        self.assertEqual(len(archives.created), 1)
        self.assertEqual(asyncio.run(create(FakeRequest(None), FakeResponse()))["id"], "fixture")


if __name__ == "__main__":
    unittest.main()
