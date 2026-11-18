"""Native Apple authentication: server nonce, PKCE, verified JWT and one-use code.

Apple refresh tokens are encrypted at rest and used only for authorization
revocation. Never log credentials, identity tokens or provider response bodies.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import jwt
from cryptography.fernet import Fernet, InvalidToken
from fastapi import HTTPException

from .config import PlatformSettings
from .identity_connections import auth_binding, require_recent_auth
from .repository import (
    assert_schema, connect, issue_token_pair, profile_payload, record_auth_event,
    resolve_oauth_identity, utcnow,
)
from .security import (
    constant_time_equal, random_token, sha256_text, validate_pkce_challenge,
    validate_pkce_verifier,
)

APPLE_ISSUER = 'https://appleid.apple.com'


def _token_cipher(settings: PlatformSettings) -> Fernet:
    try:
        return Fernet(settings.apple_signin_token_key.encode('ascii'))
    except (ValueError, UnicodeError):
        raise HTTPException(503, 'Apple login is not configured') from None


def _require_configured(settings: PlatformSettings) -> None:
    if not (settings.apple_bundle_id and settings.apple_signin_team_id
            and settings.apple_signin_key_id and settings.apple_signin_private_key_file
            and Path(settings.apple_signin_private_key_file).is_file()):
        raise HTTPException(503, 'Apple login is not configured')
    _token_cipher(settings)


@lru_cache(maxsize=1)
def _apple_jwks() -> jwt.PyJWKClient:
    return jwt.PyJWKClient(APPLE_ISSUER + '/auth/keys', cache_keys=True, lifespan=3600, timeout=5)


def verify_identity_token(settings: PlatformSettings, token: str, nonce_hash: str | None) -> dict[str, Any]:
    """Trust only Apple's RSA signature and this app's exact audience and nonce."""
    if not isinstance(token, str) or not 100 <= len(token) <= 16384:
        raise HTTPException(401, 'Apple identity verification failed')
    try:
        header = jwt.get_unverified_header(token)
        if header.get('alg') != 'RS256' or not isinstance(header.get('kid'), str) or not 1 <= len(header['kid']) <= 128:
            raise ValueError('invalid header')
        key = _apple_jwks().get_signing_key_from_jwt(token).key
        claims = jwt.decode(
            token, key, algorithms=['RS256'], audience=settings.apple_bundle_id,
            issuer=APPLE_ISSUER, leeway=30,
            options={'require': ['iss', 'aud', 'exp', 'iat', 'sub'] + (['nonce'] if nonce_hash else []), 'strict_aud': True},
        )
        subject = claims['sub']
        nonce = claims.get('nonce')
        if (not isinstance(subject, str) or not 1 <= len(subject) <= 255
                or (nonce_hash is not None and (not isinstance(nonce, str) or not constant_time_equal(sha256_text(nonce), nonce_hash)))
                or not isinstance(claims['iat'], (int, float)) or time.time() - claims['iat'] > 600):
            raise ValueError('invalid claims')
        if claims.get('email') is not None and (
                not isinstance(claims['email'], str) or len(claims['email']) > 320):
            raise ValueError('invalid email')
        return claims
    except jwt.PyJWKClientConnectionError:
        raise HTTPException(503, 'Apple login verification is temporarily unavailable') from None
    except (jwt.PyJWTError, ValueError, TypeError, KeyError):
        raise HTTPException(401, 'Apple identity verification failed') from None


def _client_secret(settings: PlatformSettings) -> str:
    _require_configured(settings)
    now = int(time.time())
    try:
        return jwt.encode(
            {'iss': settings.apple_signin_team_id, 'iat': now, 'exp': now + 300,
             'aud': APPLE_ISSUER, 'sub': settings.apple_bundle_id},
            Path(settings.apple_signin_private_key_file).read_bytes(), algorithm='ES256',
            headers={'kid': settings.apple_signin_key_id},
        )
    except (OSError, ValueError, jwt.PyJWTError):
        raise HTTPException(503, 'Apple login is not configured') from None


def _apple_post(settings: PlatformSettings, endpoint: str, fields: dict[str, str]) -> dict[str, Any]:
    form = {'client_id': settings.apple_bundle_id, 'client_secret': _client_secret(settings), **fields}
    request = urllib.request.Request(
        APPLE_ISSUER + endpoint, data=urllib.parse.urlencode(form).encode('utf-8'),
        headers={'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}, method='POST',
    )
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=8) as response:
            body = response.read(65537)
        if len(body) > 65536:
            raise ValueError('oversized Apple response')
        payload = json.loads(body) if body else {}
        if not isinstance(payload, dict) or payload.get('error'):
            raise ValueError('invalid Apple response')
        return payload
    except urllib.error.HTTPError as error:
        try:
            provider_error = json.loads(error.read(4096)).get('error')
        except (ValueError, AttributeError):
            provider_error = None
        code = 401 if endpoint == '/auth/token' and provider_error == 'invalid_grant' else 503
        raise HTTPException(code, 'Apple authorization could not be verified') from None
    except (urllib.error.URLError, TimeoutError, ValueError):
        raise HTTPException(503, 'Apple login is temporarily unavailable') from None


def begin_apple_signin(settings: PlatformSettings, *, code_challenge: str,
                       link_session: dict | None = None) -> dict[str, str]:
    _require_configured(settings)
    challenge = validate_pkce_challenge(code_challenge)
    if link_session:
        require_recent_auth(settings, link_session)
    state = random_token('as_', 32)
    nonce = random_token('an_', 32)
    with connect(settings) as connection:
        assert_schema(connection)
        connection.execute(
            '''INSERT INTO mobile_oauth_flows
               (state_hash,provider,code_challenge,redirect_uri,link_user_id,nonce_hash,expires_at,link_auth_binding)
               VALUES (%s,'apple',%s,'native-apple',%s,%s,%s,%s)''',
            (sha256_text(state), challenge, link_session['user_id'] if link_session else None,
             sha256_text(nonce), utcnow() + timedelta(seconds=settings.oauth_state_ttl_seconds),
             auth_binding(link_session) if link_session else None),
        )
    return {'state': state, 'nonce': nonce}


def complete_apple_signin(settings: PlatformSettings, *, state: str, code_verifier: str,
                          device_id: str, identity_token: str, authorization_code: str,
                          full_name: str = '', link_session: dict | None = None) -> dict[str, Any]:
    _require_configured(settings)
    if not 32 <= len(state) <= 160 or not 1 <= len(authorization_code) <= 4096 or len(full_name) > 120:
        raise HTTPException(422, 'Apple login request is invalid')
    state_hash = sha256_text(state)
    with connect(settings) as connection:
        assert_schema(connection)
        flow = connection.execute('SELECT * FROM mobile_oauth_flows WHERE state_hash=%s FOR UPDATE', (state_hash,)).fetchone()
        if (not flow or flow['provider'] != 'apple' or flow['status'] != 'pending' or flow['expires_at'] <= utcnow()):
            raise HTTPException(401, 'Apple login request is invalid or expired')
        validate_pkce_verifier(code_verifier, str(flow['code_challenge']))
        if flow['link_user_id']:
            if (not link_session or link_session['user_id'] != str(flow['link_user_id'])
                    or not constant_time_equal(auth_binding(link_session), flow['link_auth_binding'] or '')):
                raise HTTPException(403, 'original account connection session required')
            require_recent_auth(settings, link_session)
        connection.execute("UPDATE mobile_oauth_flows SET status='processing',consumed_at=NOW() WHERE state_hash=%s", (state_hash,))
    try:
        native_claims = verify_identity_token(settings, identity_token, str(flow['nonce_hash']))
        provider_tokens = _apple_post(settings, '/auth/token', {'grant_type': 'authorization_code', 'code': authorization_code})
        claims = verify_identity_token(settings, str(provider_tokens.get('id_token') or ''), str(flow['nonce_hash']))
        if not constant_time_equal(claims['sub'], native_claims['sub']):
            raise HTTPException(401, 'Apple identity verification failed')
        refresh_token = provider_tokens.get('refresh_token')
        if not isinstance(refresh_token, str) or not 20 <= len(refresh_token) <= 8192:
            raise HTTPException(503, 'Apple authorization could not be verified')
        ciphertext = _token_cipher(settings).encrypt(refresh_token.encode()).decode('ascii')
        verified_email = claims.get('email_verified') in (True, 'true')
        email = claims.get('email') if verified_email else None
        with connect(settings) as connection:
            current = connection.execute("SELECT status,link_user_id FROM mobile_oauth_flows WHERE state_hash=%s FOR UPDATE", (state_hash,)).fetchone()
            if not current or current['status'] != 'processing':
                raise HTTPException(401, 'Apple login request is no longer active')
            # First-login-only name fields must survive later Apple logins.
            name = full_name.strip() or None
            if not name:
                previous = connection.execute("SELECT provider_display_name FROM platform_identities WHERE provider='apple' AND provider_subject=%s", (claims['sub'],)).fetchone()
                name = previous['provider_display_name'] if previous else None
            user_id = resolve_oauth_identity(
                connection, provider='apple', subject=claims['sub'], email=email,
                display_name=name, email_verified=verified_email,
                link_user_id=str(current['link_user_id']) if current['link_user_id'] else None,
            )
            connection.execute(
                '''INSERT INTO mobile_apple_credentials (provider_subject,user_id,refresh_token_ciphertext)
                   VALUES (%s,%s,%s) ON CONFLICT(provider_subject) DO UPDATE
                   SET refresh_token_ciphertext=EXCLUDED.refresh_token_ciphertext,updated_at=NOW(),revoked_at=NULL''',
                (claims['sub'], user_id, ciphertext),
            )
            pair = issue_token_pair(connection, settings, user_id=user_id, device_id=device_id)
            connection.execute("UPDATE mobile_oauth_flows SET status='completed' WHERE state_hash=%s", (state_hash,))
            record_auth_event(connection, event_type='apple_identity_verified', user_id=user_id,
                              provider='apple', family_id=str(pair['familyId']), device_id=device_id)
            profile = profile_payload(connection, user_id)
        return {**profile, **{k: pair[k] for k in ['accessToken','refreshToken','tokenType','accessTokenExpiresIn','refreshTokenExpiresIn']}, 'loginProvider': 'apple'}
    except Exception:
        with connect(settings) as connection:
            connection.execute("UPDATE mobile_oauth_flows SET status='failed' WHERE state_hash=%s AND status='processing'", (state_hash,))
            record_auth_event(connection, event_type='apple_login_failed', provider='apple')
        raise


def revoke_apple_credentials(settings: PlatformSettings, connection, user_id: str) -> None:
    """Revoke at Apple before disconnecting or withdrawing the local identity."""
    rows = connection.execute('''SELECT provider_subject,refresh_token_ciphertext
        FROM mobile_apple_credentials WHERE user_id=%s AND revoked_at IS NULL FOR UPDATE''', (user_id,)).fetchall()
    for row in rows:
        try:
            token = _token_cipher(settings).decrypt(row['refresh_token_ciphertext'].encode('ascii')).decode('utf-8')
        except (InvalidToken, ValueError, UnicodeError):
            raise HTTPException(503, 'Apple authorization revocation is unavailable') from None
        _apple_post(settings, '/auth/revoke', {'token': token, 'token_type_hint': 'refresh_token'})
        connection.execute('''UPDATE mobile_apple_credentials
            SET revoked_at=NOW(),refresh_token_ciphertext='',updated_at=NOW() WHERE provider_subject=%s''', (row['provider_subject'],))


def validate_saved_apple_authorization(settings: PlatformSettings, user_id: str) -> None:
    """Check authorizations at most daily; provider outages never erase sessions."""
    with connect(settings) as connection:
        rows = connection.execute('''SELECT provider_subject,refresh_token_ciphertext FROM mobile_apple_credentials
            WHERE user_id=%s AND revoked_at IS NULL AND updated_at < NOW() - INTERVAL '1 day' ''', (user_id,)).fetchall()
    for row in rows:
        try:
            refresh_token = _token_cipher(settings).decrypt(row['refresh_token_ciphertext'].encode()).decode()
        except (InvalidToken, ValueError, UnicodeError):
            raise HTTPException(503, 'Apple authorization verification is unavailable') from None
        try:
            result = _apple_post(settings, '/auth/token', {'grant_type': 'refresh_token', 'refresh_token': refresh_token})
            claims = verify_identity_token(settings, str(result.get('id_token') or ''), None)
            if not constant_time_equal(claims['sub'], row['provider_subject']):
                raise HTTPException(401, 'Apple authorization verification failed')
        except HTTPException as error:
            if error.status_code != 401:
                raise
            from .identity_connections import _revoke_account_sessions
            with connect(settings) as connection:
                connection.execute("UPDATE mobile_apple_credentials SET revoked_at=NOW(),refresh_token_ciphertext='' WHERE provider_subject=%s", (row['provider_subject'],))
                connection.execute("UPDATE platform_identities SET is_active=FALSE,disconnected_at=NOW() WHERE provider='apple' AND provider_subject=%s", (row['provider_subject'],))
                _revoke_account_sessions(connection, user_id, 'apple_authorization_revoked')
                record_auth_event(connection, event_type='apple_authorization_revoked', user_id=user_id, provider='apple')
            raise HTTPException(401, 'Apple authorization was revoked; sign in again') from None
        replacement = result.get('refresh_token') or refresh_token
        if not isinstance(replacement, str) or not 20 <= len(replacement) <= 8192:
            raise HTTPException(503, 'Apple authorization verification is unavailable')
        encrypted = _token_cipher(settings).encrypt(replacement.encode()).decode()
        with connect(settings) as connection:
            connection.execute('''UPDATE mobile_apple_credentials SET updated_at=NOW(),refresh_token_ciphertext=%s
                WHERE provider_subject=%s AND revoked_at IS NULL''', (encrypted, row['provider_subject']))
