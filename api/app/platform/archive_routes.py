from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query, Request, Response

from . import archives
from .config import get_settings
from .repository import connect
from .reports import load_archive, render_html, render_pdf
from .security import sha256_bytes
from .shared_session import shared_session

router = APIRouter()
PRIVATE = {'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff'}


@router.get('/api/v1/account/me')
def account_me(request: Request, response: Response):
    from .repository import profile_payload
    settings, user_id = _principal(request)
    with connect(settings) as connection:
        profile = profile_payload(connection, user_id)
    response.headers.update(PRIVATE)
    return profile


@router.post('/api/v1/account/identities/start')
async def identity_start(request: Request, response: Response):
    from .identity_connections import auth_binding, require_recent_auth
    from .oauth import begin_oauth
    settings = get_settings()
    principal = shared_session(settings, request)
    if principal['auth_type'] != 'bearer':
        raise HTTPException(422, 'web account connections use /api/auth/{provider}/start?link=1')
    require_recent_auth(settings, principal)
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(422, 'object required')
    response.headers.update(PRIVATE)
    return begin_oauth(settings, provider=str(payload.get('provider', '')),
        code_challenge=str(payload.get('codeChallenge', '')), redirect_uri=str(payload.get('redirectUri', '')),
        link_user_id=principal['user_id'], link_auth_binding=auth_binding(principal))


@router.post('/api/v1/account/identities/{provider}/disconnect')
def identity_disconnect(provider: str, request: Request, response: Response):
    from .identity_connections import disconnect_identity
    settings = get_settings()
    response.headers.update(PRIVATE)
    return disconnect_identity(settings, shared_session(settings, request), provider)


@router.post('/api/v1/account/withdraw')
async def account_withdraw(request: Request, response: Response):
    from .identity_connections import withdraw_account
    settings = get_settings()
    principal = shared_session(settings, request)
    payload = await request.json()
    response.headers.update(PRIVATE)
    return withdraw_account(settings, principal, isinstance(payload, dict) and payload.get('confirm') == 'withdraw-account')


def _principal(request: Request, *, upload: bool = False):
    settings = get_settings()
    archives.require_enabled(settings, upload=upload)
    return settings, shared_session(settings, request)['user_id']


@router.get('/api/v1/report-archives')
def archive_list(request: Request, response: Response, cursor: str | None = None, limit: int = Query(50, ge=1, le=100)):
    settings, user_id = _principal(request)
    response.headers.update(PRIVATE)
    return archives.list_items(settings, user_id, cursor, limit)


@router.post('/api/v1/report-archives/uploads')
async def upload_start(request: Request, response: Response):
    settings, user_id = _principal(request, upload=True)
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(422, 'object required')
    response.headers.update(PRIVATE)
    return archives.begin_upload(settings, user_id, payload)


@router.put('/api/v1/report-archives/uploads/{upload_id}/content')
async def upload_content(upload_id: str, request: Request, response: Response):
    from starlette.concurrency import run_in_threadpool
    settings, user_id = _principal(request, upload=True)
    response.headers.update(PRIVATE)
    return await run_in_threadpool(archives.put_upload, settings, user_id, upload_id,
                                  await request.body(), request.headers.get('content-type', ''))


@router.post('/api/v1/report-archives')
async def archive_create(request: Request, response: Response):
    settings = get_settings()
    archives.require_enabled(settings)
    principal = shared_session(settings, request)
    user_id = principal['user_id']
    payload = await request.json()
    if isinstance(payload, dict) and 'snapshot' in payload:
        if principal['auth_type'] != 'cookie':
            raise HTTPException(403, 'web session required for HTML snapshots')
        response.headers.update(PRIVATE)
        return archives.create_web_snapshot(settings, user_id, payload)
    if not isinstance(payload, dict) or set(payload) != {'uploadId'}:
        raise HTTPException(422, 'only a verified uploadId may create an imported archive')
    response.headers.update(PRIVATE)
    return archives.commit_upload(settings, user_id, str(payload['uploadId']))


@router.delete('/api/v1/report-archives/{archive_id}')
def archive_delete(archive_id: str, request: Request, response: Response):
    settings, user_id = _principal(request)
    response.headers.update(PRIVATE)
    return archives.delete_item(settings, user_id, archive_id)


@router.get('/api/v1/report-archives/{archive_id}')
def archive_detail(archive_id: str, request: Request, response: Response):
    settings, user_id = _principal(request)
    response.headers.update(PRIVATE)
    return {'ok': True, **archives.get_item(settings, user_id, archive_id)}


@router.get('/api/v1/report-archives/{archive_id}/content')
def archive_content(archive_id: str, request: Request, format: str = Query('pdf')):
    settings, user_id = _principal(request)
    item = archives.get_item(settings, user_id, archive_id)
    if format not in item['contentFormats']:
        raise HTTPException(422, 'archive format is unavailable')
    headers = {**PRIVATE, 'X-Report-Archive-Id': item['id'],
               'Content-Disposition': f'attachment; filename="building-land-report.{format}"'}
    if item['sourceKind'] == 'canonical':
        row, canonical = load_archive(settings, user_id=user_id, archive_id=item['id'])
        if format == 'pdf':
            from .routes import _pdf_response
            return _pdf_response(render_pdf(settings, canonical, asset_manifest=row['asset_manifest']),
                                 canonical, archive_id=item['id'], usage_id=str(row['usage_id']), owner_user_id=user_id)
        data = (render_html(settings, canonical, asset_manifest=row['asset_manifest']) if format == 'html'
                else json.dumps(row['canonical_report'], ensure_ascii=False).encode())
        headers['X-Report-Content-Hash'] = canonical.content_hash
    else:
        with connect(settings) as connection:
            row = connection.execute('''SELECT pdf_data,html_data,json_data,artifact_sha256 FROM platform_archive_imports
                WHERE user_id=%s AND id=%s AND deleted_at IS NULL''', (user_id, item['id'])).fetchone()
        if row is None:
            raise HTTPException(404, 'archive not found')
        data = (bytes(row['pdf_data']) if format == 'pdf' else row['html_data'].encode() if format == 'html'
                else json.dumps(row['json_data'], ensure_ascii=False).encode())
        if format == 'pdf' and sha256_bytes(data) != row['artifact_sha256']:
            raise HTTPException(503, 'archive integrity check failed')
    if isinstance(data, str):
        data = data.encode()
    headers['X-Report-Artifact-Sha256'] = sha256_bytes(data)
    if format == 'html':
        headers['Content-Security-Policy'] = "sandbox; default-src 'none'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    return Response(data, headers=headers, media_type={'pdf': 'application/pdf', 'html': 'text/html', 'json': 'application/json'}[format])
