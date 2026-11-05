"""Isolated HTTP contract for web-session ownership of shared snapshots."""
from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
import sqlite3
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.platform import archive_routes, archives
import importlib


session_module = importlib.import_module("app.platform.shared_session")
OWNER_A = "11111111-1111-4111-8111-111111111111"
OWNER_B = "22222222-2222-4222-8222-222222222222"


class SqliteCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    @staticmethod
    def convert(row):
        if row is None:
            return None
        result = dict(row)
        for key in ("content_formats", "included_items", "json_data"):
            if key in result and isinstance(result[key], str):
                result[key] = json.loads(result[key])
        if isinstance(result.get("saved_at"), str):
            result["saved_at"] = datetime.fromisoformat(result["saved_at"]).replace(tzinfo=timezone.utc)
        return result

    def fetchone(self):
        return self.convert(self.cursor.fetchone())

    def fetchall(self):
        return [self.convert(row) for row in self.cursor.fetchall()]


class SqliteConnection:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=5)
        self.connection.row_factory = sqlite3.Row

    def execute(self, sql, values=()):
        if "pg_advisory_xact_lock" in sql:
            sql, values = "SELECT 1", ()
        else:
            sql = re.sub(r"\bFOR UPDATE\b", "", sql, flags=re.IGNORECASE)
            sql = re.sub(r"\bNOW\(\)", "CURRENT_TIMESTAMP", sql, flags=re.IGNORECASE)
            sql = sql.replace("%s", "?")
            values = tuple(json.dumps(value.obj) if hasattr(value, "obj") else value for value in values)
        return SqliteCursor(self.connection.execute(sql, values))

    def close(self, failed=False):
        if failed:
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()


class IntrospectionHandler(BaseHTTPRequestHandler):
    sessions = {}

    def do_POST(self):
        if self.path != "/introspect" or self.headers.get("X-Internal-Service-Token") != "isolated-test-token":
            self.send_error(403)
            return
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        session = self.sessions.get(body.get("sessionId"))
        if session is None:
            self.send_error(401)
            return
        data = json.dumps(session).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args):
        pass


class WebArchiveOwnerHttpTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="shared-owner-http-")
        self.database = Path(self.temporary.name) / "test.sqlite3"
        connection = sqlite3.connect(self.database)
        connection.executescript("""
            CREATE TABLE platform_users(id TEXT PRIMARY KEY,status TEXT,auth_version INTEGER);
            CREATE TABLE platform_external_accounts(namespace TEXT,external_id TEXT,user_id TEXT);
            CREATE TABLE platform_archive_imports(
                id TEXT PRIMARY KEY,user_id TEXT,source_kind TEXT,source_key TEXT,title TEXT,address TEXT,
                provenance TEXT,artifact_sha256 TEXT,byte_size INTEGER,html_data TEXT,json_data TEXT,
                saved_at TEXT DEFAULT CURRENT_TIMESTAMP,deleted_at TEXT);
            CREATE TABLE platform_archive_uploads(id TEXT,user_id TEXT,archive_id TEXT,byte_size INTEGER,expires_at TEXT);
            CREATE TABLE platform_archive_events(id TEXT,user_id TEXT,archive_id TEXT,event_type TEXT,operation_id TEXT,detail TEXT);
            CREATE TABLE platform_report_archives(id TEXT,user_id TEXT,status TEXT);
            CREATE TABLE platform_credit_ledger(id TEXT,user_id TEXT);
            CREATE VIEW platform_archive_catalog AS SELECT id,user_id,title,address,saved_at,
                '[]' AS included_items,source_kind,provenance,'["html","json"]' AS content_formats,
                artifact_sha256 AS content_hash,artifact_sha256,byte_size,1 AS page_count,
                'web' AS renderer_profile,'test' AS renderer_version
                FROM platform_archive_imports WHERE deleted_at IS NULL;
        """)
        connection.executemany("INSERT INTO platform_users VALUES (?,'active',0)", [(OWNER_A,), (OWNER_B,)])
        connection.executemany("INSERT INTO platform_external_accounts VALUES ('web',?,?)", [("101", OWNER_A), ("202", OWNER_B)])
        connection.commit()
        connection.close()

        IntrospectionHandler.sessions = {
            "session-a": {"externalId": "101", "authVersion": 0, "authenticatedAt": 100},
            "session-b": {"externalId": "202", "authVersion": 0, "authenticatedAt": 100},
        }
        self.introspection = ThreadingHTTPServer(("127.0.0.1", 0), IntrospectionHandler)
        self.worker = threading.Thread(target=self.introspection.serve_forever, daemon=True)
        self.worker.start()
        settings = SimpleNamespace(
            shared_archives_enabled=True, archive_uploads_enabled=True,
            web_session_cookie_name="bl_session", shared_web_origins=("https://building-land.com",),
            web_session_introspection_url=f"http://127.0.0.1:{self.introspection.server_port}/introspect",
            report_renderer_allowed_hosts=("127.0.0.1",), internal_service_token="isolated-test-token",
        )

        @contextmanager
        def isolated_connect(_settings):
            connection = SqliteConnection(self.database)
            try:
                yield connection
            except Exception:
                connection.close(failed=True)
                raise
            else:
                connection.close()

        self.patches = [
            mock.patch.object(archive_routes, "get_settings", return_value=settings),
            mock.patch.object(session_module, "connect", isolated_connect),
            mock.patch.object(archives, "connect", isolated_connect),
            mock.patch.object(archive_routes, "connect", isolated_connect),
        ]
        for patcher in self.patches:
            patcher.start()
        app = FastAPI()
        app.include_router(archive_routes.router)
        self.client = TestClient(app, follow_redirects=False)

    def tearDown(self):
        self.client.close()
        for patcher in reversed(self.patches):
            patcher.stop()
        self.introspection.shutdown()
        self.introspection.server_close()
        self.worker.join(timeout=3)
        self.temporary.cleanup()

    def headers(self, session="session-a", expected=None, origin="https://building-land.com"):
        result = {"Cookie": f"bl_session={session}", "Origin": origin, "X-Archive-Request": "1"}
        if expected is not None:
            result["X-Expected-Web-User-Id"] = expected
        return result

    @staticmethod
    def snapshot(request_id="web-owner-test-1", html="<p>private report</p>"):
        return {"requestId": request_id,
                "snapshot": {"title": "Private report", "address": "Synthetic address", "pages": [html]},
                "html": f"<html><body>{html}</body></html>"}

    def count(self, table):
        connection = sqlite3.connect(self.database)
        try:
            return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        finally:
            connection.close()

    def test_mismatched_web_owner_rejected_before_writes_and_normal_contract_preserved(self):
        # The browser header is a local web user ID; the authenticated principal is a different central UUID.
        self.assertNotEqual("101", OWNER_A)
        with ThreadPoolExecutor(max_workers=3) as executor:
            mismatches = list(executor.map(lambda _: self.client.post("/api/v1/report-archives",
                headers=self.headers("session-b", "101"), json=self.snapshot()), range(3)))
        self.assertTrue(all(result.status_code == 409 for result in mismatches))
        self.assertEqual(self.client.post("/api/v1/report-archives", headers=self.headers("session-a", "202"),
            json=self.snapshot()).status_code, 409)
        self.assertEqual(self.count("platform_archive_imports"), 0)
        self.assertEqual(self.count("platform_archive_events"), 0)
        self.assertEqual(self.count("platform_credit_ledger"), 0)

        owner_headers = self.headers("session-a", "101")
        first = self.client.post("/api/v1/report-archives", headers=owner_headers, json=self.snapshot())
        self.assertEqual(first.status_code, 200, first.text)
        archive_id = first.json()["id"]
        retried = self.client.post("/api/v1/report-archives", headers=owner_headers, json=self.snapshot())
        self.assertEqual(retried.status_code, 200, retried.text)
        self.assertEqual(retried.json()["id"], archive_id)
        self.assertEqual(self.client.post("/api/v1/report-archives", headers=owner_headers,
            json=self.snapshot(html="<p>changed</p>")).status_code, 409)
        self.assertEqual(self.count("platform_archive_imports"), 1)
        self.assertEqual(self.count("platform_archive_events"), 1)

        # Older web requests remain compatible, while bearer uploads do not enter this cookie-only branch.
        legacy_web = self.client.post("/api/v1/report-archives", headers=self.headers(),
            json=self.snapshot("web-owner-test-2"))
        self.assertEqual(legacy_web.status_code, 200, legacy_web.text)
        other_web = self.client.post("/api/v1/report-archives", headers=self.headers("session-b"),
            json=self.snapshot("web-owner-test-3"))
        self.assertEqual(other_web.status_code, 200, other_web.text)
        self.assertEqual(self.count("platform_archive_imports"), 3)
        with mock.patch.object(session_module, "authenticate_access_token", return_value={"user_id": OWNER_A}), \
                mock.patch.object(archives, "commit_upload", return_value={"ok": True, "id": "mobile-fixture"}) as commit:
            mobile = self.client.post("/api/v1/report-archives", json={"uploadId": "synthetic-upload"},
                headers={"Authorization": "Bearer " + "x" * 40})
        self.assertEqual(mobile.status_code, 200, mobile.text)
        commit.assert_called_once()
        self.assertEqual(self.client.get(f"/api/v1/report-archives/{archive_id}",
            headers=self.headers("session-b")).status_code, 404)
        self.assertEqual(self.client.delete(f"/api/v1/report-archives/{archive_id}",
            headers=self.headers("session-b")).status_code, 404)
        self.assertEqual(self.client.get(f"/api/v1/report-archives/{archive_id}",
            headers=self.headers("session-a")).status_code, 200)
        self.assertEqual(self.count("platform_credit_ledger"), 0)

    def test_missing_tampered_expired_or_wrong_origin_auth_never_writes(self):
        request = self.snapshot("web-owner-auth-1")
        cases = [
            ({"Origin": "https://building-land.com", "X-Archive-Request": "1"}, 401),
            (self.headers("invalid-session", "101"), 401),
            (self.headers("session-a", "101", "https://evil.test"), 403),
            ({"Cookie": "bl_session=session-a", "Origin": "https://building-land.com"}, 403),
            ({**self.headers("session-a", "101"), "Authorization": "Bearer invalid"}, 401),
        ]
        for headers, expected_status in cases:
            with self.subTest(headers=headers):
                self.assertEqual(self.client.post("/api/v1/report-archives", headers=headers,
                    json=request).status_code, expected_status)
        IntrospectionHandler.sessions.pop("session-a")
        self.assertEqual(self.client.post("/api/v1/report-archives", headers=self.headers("session-a", "101"),
            json=request).status_code, 401)
        self.assertEqual(self.count("platform_archive_imports"), 0)
        self.assertEqual(self.count("platform_archive_events"), 0)
        self.assertEqual(self.count("platform_credit_ledger"), 0)


if __name__ == "__main__":
    unittest.main()
