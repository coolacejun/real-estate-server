"""Resolve both clients to one central principal. Never create accounts on reads."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from http.cookies import CookieError, SimpleCookie
from urllib.parse import urlparse

from fastapi import HTTPException, Request

from .config import PlatformSettings
from .repository import authenticate_access_token, connect
from .security import sha256_text


def shared_session(settings: PlatformSettings, request: Request) -> dict:
    authorization = request.headers.get('authorization')
    if authorization is not None:
        if not authorization.startswith('Bearer ') or not 32 <= len(authorization[7:]) <= 256:
            raise HTTPException(401, 'invalid bearer token')
        with connect(settings) as connection:
            session = authenticate_access_token(connection, authorization[7:])
        return {**session, 'auth_type': 'bearer'}

    cookie = SimpleCookie()
    try:
        cookie.load(request.headers.get('cookie', ''))
        raw = cookie[settings.web_session_cookie_name].value
    except (CookieError, KeyError, ValueError):
        raise HTTPException(401, 'login required')
    if not raw or len(raw) > 256:
        raise HTTPException(401, 'invalid web session')
    if request.method not in ('GET', 'HEAD', 'OPTIONS'):
        # Strict Origin plus a non-simple, same-origin header protects cookie APIs.
        # Bearer clients never fall back to a cookie after failed authentication.
        if (request.headers.get('origin') not in settings.shared_web_origins
                or request.headers.get('x-archive-request') != '1'):
            raise HTTPException(403, 'same-origin request required')
    endpoint = settings.web_session_introspection_url
    parsed = urlparse(endpoint)
    if (parsed.scheme not in ('http', 'https') or parsed.hostname not in settings.report_renderer_allowed_hosts
            or parsed.username or parsed.password or not settings.internal_service_token):
        raise HTTPException(503, 'web session adapter is not configured')
    internal = urllib.request.Request(endpoint, method='POST',
        data=json.dumps({'sessionId': raw}).encode(), headers={
            'Content-Type': 'application/json',
            'X-Internal-Service-Token': settings.internal_service_token,
        })
    # Redirects must never forward an internal credential to another endpoint.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    try:
        with urllib.request.build_opener(NoRedirect).open(internal, timeout=5) as response:
            body = response.read(8193)
        if len(body) > 8192:
            raise ValueError('oversized session result')
        payload = json.loads(body)
        external_id = str(payload['externalId'])
        authenticated_at = int(payload['authenticatedAt'])
        auth_version = int(payload['authVersion'])
    except urllib.error.HTTPError as exc:
        raise HTTPException(401 if exc.code == 401 else 503, 'web session verification failed') from exc
    except (urllib.error.URLError, TimeoutError, ValueError, KeyError, TypeError) as exc:
        raise HTTPException(503, 'web session verification unavailable') from exc
    with connect(settings) as connection:
        row = connection.execute('''SELECT a.user_id FROM platform_external_accounts a
            JOIN platform_users u ON u.id = a.user_id
            WHERE a.namespace = 'web' AND a.external_id = %s AND u.status = 'active'
              AND u.auth_version = %s''', (external_id, auth_version)).fetchone()
    if row is None:
        raise HTTPException(409, 'web account connection required')
    return {'user_id': str(row['user_id']), 'external_id': external_id,
            'auth_type': 'cookie', 'authenticated_at': authenticated_at, 'session_hash': sha256_text(raw)}
