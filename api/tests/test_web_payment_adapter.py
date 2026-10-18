from __future__ import annotations

import json
import threading
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fastapi import HTTPException

from app.platform.config import PlatformSettings
from app.platform.web_payments import read_web_credit_payments


class _WebHistoryHandler(BaseHTTPRequestHandler):
    received: list[dict] = []

    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.received.append({
            "path": self.path,
            "token": self.headers.get("X-Internal-Service-Token"),
            "body": body,
        })
        payload = json.dumps({"items": [], "nextCursor": None}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_: object) -> None:
        pass


class WebPaymentAdapterTest(unittest.TestCase):
    def test_internal_web_read_uses_configured_host_and_service_token(self) -> None:
        _WebHistoryHandler.received = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), _WebHistoryHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            settings = replace(
                PlatformSettings.from_env(),
                web_session_introspection_url=(
                    f"http://127.0.0.1:{server.server_port}/api/internal/platform-session"
                ),
                report_renderer_allowed_hosts=("127.0.0.1",),
                internal_service_token="test-service-token",
            )
            result = read_web_credit_payments(
                settings, external_id="42", platform_user_id="12345678-1234-4234-8234-123456789abc",
                limit=2, cursor="2026-10-17T01:05:00|7",
            )
            self.assertEqual(result, {"items": [], "nextCursor": None})
            self.assertEqual(len(_WebHistoryHandler.received), 1)
            sent = _WebHistoryHandler.received[0]
            self.assertEqual(sent["path"], "/api/internal/mobile-credit-payments")
            self.assertEqual(sent["token"], "test-service-token")
            self.assertEqual(sent["body"]["externalId"], "42")
            self.assertEqual(sent["body"]["limit"], 2)

            unsafe = replace(settings, web_session_introspection_url="http://example.com/api/internal/platform-session")
            with self.assertRaises(HTTPException) as blocked:
                read_web_credit_payments(
                    unsafe, external_id="42", platform_user_id="12345678-1234-4234-8234-123456789abc",
                    limit=2, cursor=None,
                )
            self.assertEqual(blocked.exception.status_code, 503)
            self.assertEqual(len(_WebHistoryHandler.received), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
