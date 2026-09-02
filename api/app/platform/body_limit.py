from __future__ import annotations

from typing import Any

from .config import get_settings


class RequestBodyLimitMiddleware:
    """Enforce body limits before JSON parsing, including chunked requests."""

    def __init__(self, app: Any) -> None:
        self.app = app

    @staticmethod
    def _limit(path: str) -> int | None:
        if path.startswith("/api/mobile/v1/reports/"):
            return get_settings().report_max_body_bytes
        if path.startswith("/api/mobile/v1/store/"):
            return 3 * 1024 * 1024
        if path.startswith("/api/mobile/v1/auth/"):
            return 64 * 1024
        if path == "/api/v1/environment-analysis":
            return 256 * 1024
        if path.startswith("/api/internal/v1/web/"):
            return 256 * 1024
        return None

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        limit = self._limit(str(scope.get("path") or ""))
        if limit is None:
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        raw_length = headers.get(b"content-length")
        try:
            content_length = int(raw_length) if raw_length is not None else None
        except ValueError:
            content_length = None
        if content_length is not None and content_length > limit:
            await self._reject(send)
            return

        consumed = 0
        rejected = False

        async def limited_receive() -> dict[str, Any]:
            nonlocal consumed, rejected
            message = await receive()
            if message.get("type") == "http.request":
                consumed += len(message.get("body", b""))
                if consumed > limit:
                    rejected = True
                    return {"type": "http.disconnect"}
            return message

        try:
            await self.app(scope, limited_receive, send)
        except Exception:
            if rejected:
                await self._reject(send)
                return
            raise

    @staticmethod
    async def _reject(send: Any) -> None:
        body = b'{"detail":"request body is too large"}'
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                    (b"cache-control", b"no-store"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
