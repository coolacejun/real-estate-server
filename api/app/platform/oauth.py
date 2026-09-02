from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from fastapi import HTTPException

from .config import PlatformSettings
from .repository import (
    assert_schema,
    connect,
    issue_token_pair,
    new_id,
    profile_payload,
    resolve_oauth_identity,
    utcnow,
)
from .security import constant_time_equal, random_token, sha256_text, validate_pkce_challenge


SUPPORTED_PROVIDERS = {"kakao", "naver", "google"}


@dataclass(frozen=True)
class ProviderIdentity:
    subject: str
    email: str | None
    display_name: str | None


def _callback_url(settings: PlatformSettings, provider: str) -> str:
    return f"{settings.oauth_callback_base_url}/api/mobile/v1/auth/oauth/callback/{provider}"


def begin_oauth(
    settings: PlatformSettings,
    *,
    provider: str,
    code_challenge: str,
    redirect_uri: str,
    link_user_id: str | None,
) -> dict[str, str]:
    if provider not in SUPPORTED_PROVIDERS:
        raise HTTPException(status_code=422, detail="unsupported OAuth provider")
    challenge = validate_pkce_challenge(code_challenge)
    if redirect_uri not in settings.oauth_redirect_allowlist:
        raise HTTPException(status_code=422, detail="redirectUri is not allowed")
    provider_settings = settings.provider(provider)
    if not provider_settings.configured:
        raise HTTPException(status_code=503, detail=f"{provider} OAuth is not configured")

    state = random_token("os_", 32)
    state_hash = sha256_text(state)
    with connect(settings) as connection:
        assert_schema(connection)
        connection.execute(
            """
            INSERT INTO mobile_oauth_flows
              (state_hash, provider, code_challenge, redirect_uri, link_user_id, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                state_hash,
                provider,
                challenge,
                redirect_uri,
                link_user_id,
                utcnow() + timedelta(seconds=settings.oauth_state_ttl_seconds),
            ),
        )

    query: dict[str, str] = {
        "client_id": provider_settings.client_id,
        "redirect_uri": _callback_url(settings, provider),
        "response_type": "code",
        "state": state,
    }
    if provider_settings.scope:
        query["scope"] = provider_settings.scope
    if provider == "google":
        query.update({"access_type": "offline", "prompt": "select_account"})
    authorization_url = f"{provider_settings.authorization_url}?{urllib.parse.urlencode(query)}"
    return {"state": state, "authorizationUrl": authorization_url}


def _form_post_json(url: str, form: dict[str, str], headers: dict[str, str] | None = None) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(form).encode("utf-8"),
        headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded", **(headers or {})},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="OAuth provider token exchange failed") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="OAuth provider returned an invalid token response")
    return payload


def _get_json(url: str, access_token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "Authorization": f"Bearer {access_token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="OAuth provider profile lookup failed") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="OAuth provider returned an invalid profile")
    return payload


def fetch_provider_identity(
    settings: PlatformSettings, *, provider: str, authorization_code: str, state: str
) -> ProviderIdentity:
    provider_settings = settings.provider(provider)
    token_form = {
        "grant_type": "authorization_code",
        "client_id": provider_settings.client_id,
        "client_secret": provider_settings.client_secret,
        "redirect_uri": _callback_url(settings, provider),
        "code": authorization_code,
    }
    if provider == "naver":
        token_form["state"] = state
    token_payload = _form_post_json(provider_settings.token_url, token_form)
    access_token = str(token_payload.get("access_token") or "")
    if not access_token:
        raise HTTPException(status_code=502, detail="OAuth provider did not return an access token")
    raw_profile = _get_json(provider_settings.userinfo_url, access_token)

    if provider == "naver":
        profile = raw_profile.get("response") if isinstance(raw_profile.get("response"), dict) else {}
        subject = str(profile.get("id") or "")
        email = str(profile.get("email") or "").strip() or None
        name = str(profile.get("name") or profile.get("nickname") or "").strip() or None
    elif provider == "kakao":
        account = raw_profile.get("kakao_account") if isinstance(raw_profile.get("kakao_account"), dict) else {}
        kakao_profile = account.get("profile") if isinstance(account.get("profile"), dict) else {}
        subject = str(raw_profile.get("id") or "")
        email = str(account.get("email") or "").strip() or None
        name = str(kakao_profile.get("nickname") or "").strip() or None
    else:
        subject = str(raw_profile.get("sub") or "")
        email = str(raw_profile.get("email") or "").strip() or None
        name = str(raw_profile.get("name") or "").strip() or None
    if not subject or len(subject) > 255:
        raise HTTPException(status_code=502, detail="OAuth provider profile has no stable subject")
    return ProviderIdentity(subject=subject, email=email, display_name=name)


def complete_provider_callback(
    settings: PlatformSettings,
    *,
    provider: str,
    state: str,
    authorization_code: str,
) -> str:
    if provider not in SUPPORTED_PROVIDERS or not state or not authorization_code:
        raise HTTPException(status_code=400, detail="invalid OAuth callback")
    state_hash = sha256_text(state)
    with connect(settings) as connection:
        assert_schema(connection)
        flow = connection.execute(
            "SELECT * FROM mobile_oauth_flows WHERE state_hash = %s FOR UPDATE",
            (state_hash,),
        ).fetchone()
        if (
            flow is None
            or flow["provider"] != provider
            or flow["status"] != "pending"
            or flow["expires_at"] <= utcnow()
        ):
            raise HTTPException(status_code=400, detail="OAuth state is invalid or expired")
        connection.execute(
            "UPDATE mobile_oauth_flows SET status = 'processing', consumed_at = NOW() WHERE state_hash = %s",
            (state_hash,),
        )

    try:
        identity = fetch_provider_identity(
            settings, provider=provider, authorization_code=authorization_code, state=state
        )
        exchange_code = random_token("oc_", 36)
        with connect(settings) as connection:
            assert_schema(connection)
            flow = connection.execute(
                "SELECT * FROM mobile_oauth_flows WHERE state_hash = %s AND status = 'processing' FOR UPDATE",
                (state_hash,),
            ).fetchone()
            if flow is None:
                raise HTTPException(status_code=400, detail="OAuth state cannot be completed")
            user_id = resolve_oauth_identity(
                connection,
                provider=provider,
                subject=identity.subject,
                email=identity.email,
                display_name=identity.display_name,
                link_user_id=str(flow["link_user_id"]) if flow["link_user_id"] else None,
            )
            connection.execute(
                """
                INSERT INTO mobile_auth_codes
                  (code_hash, state_hash, user_id, provider, code_challenge, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    sha256_text(exchange_code), state_hash, user_id, provider,
                    flow["code_challenge"], utcnow() + timedelta(seconds=settings.oauth_code_ttl_seconds),
                ),
            )
            connection.execute(
                "UPDATE mobile_oauth_flows SET status = 'completed' WHERE state_hash = %s",
                (state_hash,),
            )
            redirect_uri = str(flow["redirect_uri"])
    except Exception:
        with connect(settings) as connection:
            connection.execute(
                "UPDATE mobile_oauth_flows SET status = 'failed' WHERE state_hash = %s AND status = 'processing'",
                (state_hash,),
            )
        raise

    query = urllib.parse.urlencode({"state": state, "provider": provider, "code": exchange_code})
    separator = "&" if "?" in redirect_uri else "?"
    return f"{redirect_uri}{separator}{query}"


def exchange_auth_code(
    settings: PlatformSettings, *, code: str, code_verifier: str, device_id: str
) -> dict[str, Any]:
    if not code or not code_verifier:
        raise HTTPException(status_code=422, detail="code and codeVerifier are required")
    if len(code_verifier) < 43 or len(code_verifier) > 128:
        raise HTTPException(status_code=422, detail="invalid codeVerifier")
    digest = base64.urlsafe_b64encode(__import__("hashlib").sha256(code_verifier.encode("ascii")).digest()).decode("ascii").rstrip("=")
    with connect(settings) as connection:
        assert_schema(connection)
        row = connection.execute(
            "SELECT * FROM mobile_auth_codes WHERE code_hash = %s FOR UPDATE",
            (sha256_text(code),),
        ).fetchone()
        if row is None or row["consumed_at"] is not None or row["expires_at"] <= utcnow():
            raise HTTPException(status_code=401, detail="authorization code is invalid or expired")
        if not constant_time_equal(digest, str(row["code_challenge"])):
            connection.execute(
                "UPDATE mobile_auth_codes SET consumed_at = NOW() WHERE code_hash = %s",
                (sha256_text(code),),
            )
            connection.commit()
            raise HTTPException(status_code=401, detail="PKCE verification failed")
        connection.execute(
            "UPDATE mobile_auth_codes SET consumed_at = NOW() WHERE code_hash = %s",
            (sha256_text(code),),
        )
        pair = issue_token_pair(
            connection,
            settings,
            user_id=str(row["user_id"]),
            device_id=device_id,
        )
        payload = profile_payload(connection, str(row["user_id"]))
    payload.update({"accessToken": pair["accessToken"], "refreshToken": pair["refreshToken"]})
    return payload
