from __future__ import annotations

import logging
import re
import time
from collections import defaultdict, deque
from threading import Lock
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse

from .config import PlatformSettings, get_settings
from .environment import analyze_environment
from .oauth import begin_oauth, complete_provider_callback, exchange_auth_code
from .reports import (
    AssetRecord,
    CanonicalReport,
    begin_final_usage,
    complete_final_usage,
    fail_final_usage,
    list_archives,
    load_archive,
    materialize_assets,
    render_html,
    render_pdf,
    validate_canonical_report,
)
from .repository import (
    assert_schema,
    authenticate_access_token,
    connect,
    get_or_create_store_account,
    grant_paid_credits,
    profile_payload,
    complete_paid_credit_reversal,
    prepare_paid_credit_reversal,
    resolve_external_web_account,
    rollback_paid_credit_reversal,
    revoke_paid_credits,
    revoke_token_family,
    rotate_refresh_token,
)
from .security import DEVICE_ID_RE, client_key, constant_time_equal, sha256_bytes
from .store import catalog, process_purchase


router = APIRouter()
logger = logging.getLogger("building_land.platform")
_RATE_LOCK = Lock()
_RATE_BUCKETS: dict[str, deque[float]] = defaultdict(deque)


def _settings() -> PlatformSettings:
    return get_settings()


def _body(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="request body must be an object")
    return payload


def _rate_limit(request: Request, group: str, limit: int, window_seconds: int) -> None:
    now = time.monotonic()
    key = f"{group}:{client_key(request)}"
    with _RATE_LOCK:
        bucket = _RATE_BUCKETS[key]
        while bucket and bucket[0] <= now - window_seconds:
            bucket.popleft()
        if len(bucket) >= limit:
            raise HTTPException(status_code=429, detail="too many requests", headers={"Retry-After": str(window_seconds)})
        bucket.append(now)


def _raw_bearer(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="bearer token required")
    raw = authorization[7:].strip()
    if len(raw) < 32 or len(raw) > 256:
        raise HTTPException(status_code=401, detail="invalid access token")
    return raw


def _session(authorization: str | None) -> dict[str, str]:
    settings = _settings()
    with connect(settings) as connection:
        assert_schema(connection)
        return authenticate_access_token(connection, _raw_bearer(authorization))


def _optional_session(authorization: str | None) -> dict[str, str] | None:
    if not authorization:
        return None
    return _session(authorization)


def _internal_auth(token: str | None) -> None:
    expected = _settings().internal_service_token
    if not expected:
        raise HTTPException(status_code=503, detail="internal account adapter is not configured")
    if not token or not constant_time_equal(token, expected):
        raise HTTPException(status_code=403, detail="internal service authentication failed")


def _pdf_response(
    data: bytes,
    canonical: CanonicalReport,
    *,
    archive_id: str | None = None,
    usage_id: str | None = None,
) -> Response:
    headers = {
        "X-Report-Schema-Version": str(canonical.report["schemaVersion"]),
        "X-Report-Renderer-Version": canonical.response_renderer_version,
        "X-Report-Renderer-Profile": canonical.renderer_profile,
        "X-Report-Content-Hash": canonical.content_hash,
        "X-Report-Artifact-Sha256": sha256_bytes(data),
        "Cache-Control": "private, no-store",
        "Content-Disposition": 'attachment; filename="building-land-report.pdf"',
    }
    if archive_id:
        headers["X-Report-Archive-Id"] = archive_id
    if usage_id:
        headers["X-Report-Usage-Id"] = usage_id
    return Response(data, media_type="application/pdf", headers=headers)


@router.post("/api/mobile/v1/auth/oauth/start")
async def mobile_oauth_start(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    _rate_limit(request, "oauth-start", 12, 60)
    payload = _body(await request.json())
    link_requested = payload.get("linkAccount") is True
    session = _session(authorization) if link_requested else None
    return begin_oauth(
        _settings(),
        provider=str(payload.get("provider") or "").lower(),
        code_challenge=str(payload.get("codeChallenge") or ""),
        redirect_uri=str(payload.get("redirectUri") or ""),
        link_user_id=session["user_id"] if session else None,
    )


@router.get("/api/mobile/v1/auth/oauth/callback/{provider}")
def mobile_oauth_callback(
    provider: str,
    state: str = Query(""),
    code: str = Query(""),
    error: str = Query(""),
) -> RedirectResponse:
    if error:
        raise HTTPException(status_code=400, detail="OAuth authorization was cancelled")
    destination = complete_provider_callback(
        _settings(), provider=provider.lower(), state=state, authorization_code=code
    )
    return RedirectResponse(destination, status_code=302, headers={"Cache-Control": "no-store"})


@router.post("/api/mobile/v1/auth/token")
async def mobile_auth_token(request: Request) -> dict[str, Any]:
    _rate_limit(request, "oauth-token", 20, 60)
    payload = _body(await request.json())
    device_id = str(payload.get("deviceId") or "")
    if not DEVICE_ID_RE.fullmatch(device_id):
        raise HTTPException(status_code=422, detail="deviceId is invalid")
    return exchange_auth_code(
        _settings(),
        code=str(payload.get("code") or ""),
        code_verifier=str(payload.get("codeVerifier") or ""),
        device_id=device_id,
    )


@router.post("/api/mobile/v1/auth/refresh")
async def mobile_auth_refresh(request: Request) -> dict[str, Any]:
    _rate_limit(request, "oauth-refresh", 30, 60)
    payload = _body(await request.json())
    refresh_token = str(payload.get("refreshToken") or "")
    device_id = str(payload.get("deviceId") or "")
    if not refresh_token or not DEVICE_ID_RE.fullmatch(device_id):
        raise HTTPException(status_code=422, detail="refreshToken and valid deviceId are required")
    settings = _settings()
    with connect(settings) as connection:
        assert_schema(connection)
        pair, user_id = rotate_refresh_token(
            connection, settings, raw_token=refresh_token, device_id=device_id
        )
        profile = profile_payload(connection, user_id)
    profile.update({"accessToken": pair["accessToken"], "refreshToken": pair["refreshToken"]})
    return profile


@router.get("/api/mobile/v1/me")
def mobile_me(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    settings = _settings()
    session = _session(authorization)
    with connect(settings) as connection:
        return profile_payload(connection, session["user_id"])


@router.post("/api/mobile/v1/auth/logout", status_code=204)
def mobile_logout(authorization: str | None = Header(default=None)) -> Response:
    settings = _settings()
    session = _session(authorization)
    with connect(settings) as connection:
        revoke_token_family(connection, session["family_id"], "logout")
    return Response(status_code=204)


@router.get("/api/mobile/v1/store/catalog")
def mobile_store_catalog(
    platform: str = Query(...), authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    normalized = platform.lower()
    if normalized not in {"ios", "android"}:
        raise HTTPException(status_code=422, detail="platform must be ios or android")
    settings = _settings()
    session = _session(authorization)
    with connect(settings) as connection:
        account_token = get_or_create_store_account(
            connection,
            user_id=session["user_id"],
            platform=normalized,
            device_id=session["device_id"],
        )
    return {"accountToken": account_token, "products": catalog(normalized)}


async def _store_request(
    request: Request, authorization: str | None, *, restored: bool
) -> dict[str, Any]:
    _rate_limit(request, "store-verify", 30, 60)
    payload = _body(await request.json())
    session = _session(authorization)
    if bool(payload.get("restored")) != restored:
        raise HTTPException(status_code=422, detail="restored flag does not match endpoint")
    return process_purchase(
        _settings(),
        user_id=session["user_id"],
        platform=str(payload.get("platform") or "").lower(),
        product_id=str(payload.get("productId") or ""),
        verification_data=str(payload.get("verificationData") or ""),
        transaction_id=str(payload.get("transactionId") or "").strip() or None,
        restored=restored,
    )


@router.post("/api/mobile/v1/store/verify")
async def mobile_store_verify(
    request: Request, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    return await _store_request(request, authorization, restored=False)


@router.post("/api/mobile/v1/store/restore")
async def mobile_store_restore(
    request: Request, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    return await _store_request(request, authorization, restored=True)


@router.post("/api/mobile/v1/reports/preview")
async def mobile_report_preview(
    request: Request, authorization: str | None = Header(default=None)
) -> Response:
    _rate_limit(request, "report-preview", 20, 60)
    _session(authorization)
    payload = _body(await request.json())
    canonical = validate_canonical_report(
        payload.get("report"), requested_profile=payload.get("rendererProfile")
    )
    data = render_pdf(_settings(), canonical)
    return _pdf_response(data, canonical)


@router.post("/api/mobile/v1/reports/final")
async def mobile_report_final(
    request: Request, authorization: str | None = Header(default=None)
) -> Response:
    _rate_limit(request, "report-final", 12, 60)
    session = _session(authorization)
    payload = _body(await request.json())
    canonical = validate_canonical_report(
        payload.get("report"),
        requested_profile=payload.get("rendererProfile"),
        expected_content_hash=payload.get("contentHash"),
    )
    request_id = str(payload.get("requestId") or "")
    settings = _settings()
    operation = begin_final_usage(
        settings, user_id=session["user_id"], request_id=request_id, canonical=canonical
    )
    usage = operation["usage"]
    if operation["action"] == "completed":
        archive, stored = load_archive(settings, user_id=session["user_id"], archive_id=str(usage["archive_id"]))
        data = render_pdf(settings, stored, asset_manifest=archive["asset_manifest"])
        return _pdf_response(
            data, stored, archive_id=str(archive["id"]), usage_id=str(usage["id"])
        )
    try:
        stored_report, assets = materialize_assets(
            settings, user_id=session["user_id"], report=canonical.report
        )
        stored = CanonicalReport(
            stored_report,
            canonical.content_hash,
            canonical.renderer_profile,
            canonical.response_renderer_version,
        )
        data = render_pdf(settings, stored, asset_manifest=[item.as_json() for item in assets])
        archive_id = complete_final_usage(
            settings, usage=usage, canonical=canonical, stored_report=stored_report, assets=assets
        )
    except HTTPException as exc:
        fail_final_usage(settings, str(usage["id"]), f"http_{exc.status_code}")
        raise
    except Exception as exc:
        logger.exception("canonical report generation failed usage_id=%s", usage["id"])
        fail_final_usage(settings, str(usage["id"]), "renderer_failure")
        raise HTTPException(status_code=500, detail="report generation failed") from exc
    return _pdf_response(
        data, canonical, archive_id=archive_id, usage_id=str(usage["id"])
    )


@router.get("/api/report-archive")
def mobile_archive_list(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    session = _session(authorization)
    return {"items": list_archives(_settings(), user_id=session["user_id"])}


@router.get("/api/report-archive/content")
def mobile_archive_content(
    id: str = Query(...),
    format: str = Query("pdf"),
    authorization: str | None = Header(default=None),
) -> Response:
    session = _session(authorization)
    settings = _settings()
    archive, canonical = load_archive(settings, user_id=session["user_id"], archive_id=id)
    manifest = archive["asset_manifest"]
    if format == "pdf":
        data = render_pdf(settings, canonical, asset_manifest=manifest)
        return _pdf_response(
            data, canonical, archive_id=str(archive["id"]), usage_id=str(archive["usage_id"])
        )
    if format == "html":
        data = render_html(settings, canonical, asset_manifest=manifest)
        return Response(
            data,
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "private, no-store", "X-Report-Content-Hash": canonical.content_hash},
        )
    raise HTTPException(status_code=422, detail="format must be pdf or html")


@router.post("/api/v1/environment-analysis")
async def environment_analysis(request: Request) -> dict[str, Any]:
    _rate_limit(request, "environment", 60, 60)
    return analyze_environment(_settings(), await request.json())


@router.post("/api/internal/v1/web/accounts/resolve")
async def internal_web_account_resolve(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _internal_auth(x_internal_service_token)
    payload = _body(await request.json())
    external_id = str(payload.get("externalId") or "").strip()
    if not external_id or len(external_id) > 160:
        raise HTTPException(status_code=422, detail="externalId is required")
    settings = _settings()
    with connect(settings) as connection:
        assert_schema(connection)
        user_id = resolve_external_web_account(
            connection,
            external_id=external_id,
            email=str(payload.get("email") or "").strip() or None,
            display_name=str(payload.get("displayName") or "").strip() or None,
            provider=str(payload.get("provider") or "").lower() or None,
            provider_subject=str(payload.get("providerSubject") or "").strip() or None,
        )
        legacy_paid_raw = payload.get("legacyPaidRemaining", 0)
        try:
            legacy_paid = int(legacy_paid_raw or 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="legacyPaidRemaining must be an integer") from exc
        if legacy_paid < 0 or legacy_paid > 1_000_000:
            raise HTTPException(status_code=422, detail="legacyPaidRemaining is outside the allowed range")
        if legacy_paid:
            grant_paid_credits(
                connection,
                user_id=user_id,
                credits=legacy_paid,
                reason="web_balance_migration",
                idempotency_key=f"web-balance-migration:{external_id}",
                reference_type="web_account",
                reference_id=external_id,
                metadata={"source": "legacy_web_sqlite"},
            )
        profile = profile_payload(connection, user_id)
    return {"userId": user_id, **profile}


@router.get("/api/internal/v1/web/accounts/{external_id}/credits")
def internal_web_credit_summary(
    external_id: str,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _internal_auth(x_internal_service_token)
    settings = _settings()
    with connect(settings) as connection:
        link = connection.execute(
            "SELECT user_id FROM platform_external_accounts WHERE namespace = 'web' AND external_id = %s",
            (external_id,),
        ).fetchone()
        if link is None:
            raise HTTPException(status_code=404, detail="web account is not linked")
        return profile_payload(connection, str(link["user_id"]))["creditSummary"]


@router.post("/api/internal/v1/web/credits/grant")
async def internal_web_credit_grant(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _internal_auth(x_internal_service_token)
    payload = _body(await request.json())
    external_id = str(payload.get("externalId") or "").strip()
    idempotency_key = str(payload.get("idempotencyKey") or "").strip()
    try:
        credits = int(payload.get("credits"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="credits must be an integer") from exc
    if not external_id or not idempotency_key or len(idempotency_key) > 200 or not (1 <= credits <= 1000):
        raise HTTPException(status_code=422, detail="credit grant input is invalid")
    settings = _settings()
    with connect(settings) as connection:
        link = connection.execute(
            "SELECT user_id FROM platform_external_accounts WHERE namespace = 'web' AND external_id = %s",
            (external_id,),
        ).fetchone()
        if link is None:
            raise HTTPException(status_code=404, detail="web account is not linked")
        user_id = str(link["user_id"])
        granted, balance = grant_paid_credits(
            connection,
            user_id=user_id,
            credits=credits,
            reason="web_payment",
            idempotency_key=f"web:{idempotency_key}",
            reference_type="web_payment",
            reference_id=idempotency_key,
            metadata={"source": "web"},
        )
        summary = profile_payload(connection, user_id)["creditSummary"]
    return {"granted": granted, "paidBalance": balance, "creditSummary": summary}


@router.post("/api/internal/v1/web/credits/revoke")
async def internal_web_credit_revoke(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _internal_auth(x_internal_service_token)
    payload = _body(await request.json())
    external_id = str(payload.get("externalId") or "").strip()
    idempotency_key = str(payload.get("idempotencyKey") or "").strip()
    try:
        credits = int(payload.get("credits"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="credits must be an integer") from exc
    if not external_id or not idempotency_key or len(idempotency_key) > 200 or not (1 <= credits <= 1000):
        raise HTTPException(status_code=422, detail="credit reversal input is invalid")
    settings = _settings()
    with connect(settings) as connection:
        user_id = _web_user_id(connection, external_id)
        revoked, balance = revoke_paid_credits(
            connection,
            user_id=user_id,
            credits=credits,
            idempotency_key=f"web-reversal:{idempotency_key}",
            reference_type="web_payment",
            reference_id=idempotency_key,
        )
        summary = profile_payload(connection, user_id)["creditSummary"]
    return {"revoked": revoked, "paidBalance": balance, "creditSummary": summary}


async def _credit_reversal_request(
    request: Request,
    token: str | None,
    action: str,
) -> dict[str, Any]:
    _internal_auth(token)
    payload = _body(await request.json())
    external_id = str(payload.get("externalId") or "").strip()
    external_key = str(payload.get("idempotencyKey") or "").strip()
    if not external_id or not external_key or len(external_key) > 200:
        raise HTTPException(status_code=422, detail="credit reversal input is invalid")
    settings = _settings()
    with connect(settings) as connection:
        user_id = _web_user_id(connection, external_id)
        if action == "prepare":
            try:
                credits = int(payload.get("credits"))
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=422, detail="credits must be an integer") from exc
            if not 1 <= credits <= 1000:
                raise HTTPException(status_code=422, detail="credits are outside the allowed range")
            reversal = prepare_paid_credit_reversal(
                connection, user_id=user_id, credits=credits, external_key=external_key
            )
        elif action == "complete":
            reversal = complete_paid_credit_reversal(
                connection, user_id=user_id, external_key=external_key
            )
        else:
            reversal = rollback_paid_credit_reversal(
                connection, user_id=user_id, external_key=external_key
            )
        summary = profile_payload(connection, user_id)["creditSummary"]
    return {
        "reversalId": str(reversal["id"]),
        "status": reversal["status"],
        "creditSummary": summary,
    }


@router.post("/api/internal/v1/web/credits/reversal/prepare")
async def internal_web_credit_reversal_prepare(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _credit_reversal_request(request, x_internal_service_token, "prepare")


@router.post("/api/internal/v1/web/credits/reversal/complete")
async def internal_web_credit_reversal_complete(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _credit_reversal_request(request, x_internal_service_token, "complete")


@router.post("/api/internal/v1/web/credits/reversal/rollback")
async def internal_web_credit_reversal_rollback(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    return await _credit_reversal_request(request, x_internal_service_token, "rollback")


def _web_user_id(connection: Any, external_id: str) -> str:
    link = connection.execute(
        "SELECT user_id FROM platform_external_accounts WHERE namespace = 'web' AND external_id = %s",
        (external_id,),
    ).fetchone()
    if link is None:
        raise HTTPException(status_code=404, detail="web account is not linked")
    return str(link["user_id"])


@router.post("/api/internal/v1/web/reports/reserve")
async def internal_web_report_reserve(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _internal_auth(x_internal_service_token)
    payload = _body(await request.json())
    external_id = str(payload.get("externalId") or "").strip()
    request_id = str(payload.get("requestId") or "").strip()
    content_hash = str(payload.get("contentHash") or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", content_hash):
        raise HTTPException(status_code=422, detail="contentHash must be a SHA-256 digest")
    settings = _settings()
    with connect(settings) as connection:
        assert_schema(connection)
        user_id = _web_user_id(connection, external_id)
    canonical = CanonicalReport(
        report={"schemaVersion": 1},
        content_hash=content_hash,
        renderer_profile="web-a4-v1",
        response_renderer_version="web-a4-v1",
    )
    operation = begin_final_usage(
        settings, user_id=user_id, request_id=request_id, canonical=canonical
    )
    with connect(settings) as connection:
        summary = profile_payload(connection, user_id)["creditSummary"]
    return {
        "usageId": str(operation["usage"]["id"]),
        "status": operation["usage"]["status"],
        "action": operation["action"],
        "creditSummary": summary,
    }


@router.post("/api/internal/v1/web/reports/complete")
async def internal_web_report_complete(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _internal_auth(x_internal_service_token)
    payload = _body(await request.json())
    external_id = str(payload.get("externalId") or "").strip()
    usage_id = str(payload.get("usageId") or "").strip()
    settings = _settings()
    with connect(settings) as connection:
        assert_schema(connection)
        user_id = _web_user_id(connection, external_id)
        usage = connection.execute(
            "SELECT * FROM platform_report_usages WHERE id = %s AND user_id = %s FOR UPDATE",
            (usage_id, user_id),
        ).fetchone()
        if usage is None:
            raise HTTPException(status_code=404, detail="report usage not found")
        if usage["status"] == "failed":
            raise HTTPException(status_code=409, detail="failed report usage cannot be completed")
        if usage["status"] == "pending":
            connection.execute(
                """
                UPDATE platform_report_usages
                SET status = 'completed', completed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (usage_id,),
            )
        summary = profile_payload(connection, user_id)["creditSummary"]
    return {"usageId": usage_id, "status": "completed", "creditSummary": summary}


@router.post("/api/internal/v1/web/reports/fail")
async def internal_web_report_fail(
    request: Request,
    x_internal_service_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _internal_auth(x_internal_service_token)
    payload = _body(await request.json())
    external_id = str(payload.get("externalId") or "").strip()
    usage_id = str(payload.get("usageId") or "").strip()
    settings = _settings()
    with connect(settings) as connection:
        assert_schema(connection)
        user_id = _web_user_id(connection, external_id)
        usage = connection.execute(
            "SELECT id, status FROM platform_report_usages WHERE id = %s AND user_id = %s",
            (usage_id, user_id),
        ).fetchone()
        if usage is None:
            raise HTTPException(status_code=404, detail="report usage not found")
        if usage["status"] == "completed":
            raise HTTPException(status_code=409, detail="completed report usage cannot be failed")
    fail_final_usage(settings, usage_id, "web_render_failure")
    with connect(settings) as connection:
        summary = profile_payload(connection, user_id)["creditSummary"]
    return {"usageId": usage_id, "status": "failed", "creditSummary": summary}
