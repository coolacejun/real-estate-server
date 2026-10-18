"""Read web payment receipts through the existing internal web service channel."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlparse, urlunparse

from fastapi import HTTPException

from .config import PlatformSettings


def read_web_credit_payments(
    settings: PlatformSettings,
    *,
    external_id: str,
    platform_user_id: str,
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    parsed = urlparse(settings.web_session_introspection_url)
    if (
        parsed.scheme not in ("http", "https")
        or parsed.hostname not in settings.report_renderer_allowed_hosts
        or parsed.path != "/api/internal/platform-session"
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or not settings.internal_service_token
    ):
        raise HTTPException(503, "web payment history adapter is not configured")
    endpoint = urlunparse(parsed._replace(path="/api/internal/mobile-credit-payments"))
    request = urllib.request.Request(
        endpoint,
        method="POST",
        data=json.dumps({
            "externalId": external_id,
            "platformUserId": platform_user_id,
            "limit": limit,
            "cursor": cursor,
        }).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Internal-Service-Token": settings.internal_service_token,
        },
    )

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args: Any, **kwargs: Any) -> None:
            return None

    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=5) as response:
            raw = response.read(131073)
        if len(raw) > 131072:
            raise ValueError("oversized payment history response")
        payload = json.loads(raw)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise ValueError("invalid payment history response")
        return payload
    except urllib.error.HTTPError as exc:
        if exc.code == 403:
            raise HTTPException(409, "web account connection requires review") from exc
        raise HTTPException(503, "web payment history is unavailable") from exc
    except (urllib.error.URLError, TimeoutError, ValueError, TypeError) as exc:
        raise HTTPException(503, "web payment history is unavailable") from exc
