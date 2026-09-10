"""Shared archive read model and explicit, uncharged PDF imports."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore
from typing import Any

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from .config import PlatformSettings
from .repository import connect, new_id, utcnow

MAX_BYTES = 16 * 1024 * 1024
OWNER_QUOTA = 512 * 1024 * 1024
_CHECK_SLOTS = BoundedSemaphore(2)
_REQUEST_RE = re.compile(r'[A-Za-z0-9_.:-]{8,160}')
_SHA_RE = re.compile(r'[0-9a-f]{64}')


def require_enabled(settings: PlatformSettings, *, upload: bool = False) -> None:
    if not settings.shared_archives_enabled or (upload and not settings.archive_uploads_enabled):
        raise HTTPException(503, 'archive uploads are disabled' if upload else 'shared archives are disabled')


def archive_uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        raise HTTPException(404, 'archive not found')


def event(connection, user_id: str, kind: str, operation_id: str, archive_id: str | None = None, **detail):
    connection.execute('''INSERT INTO platform_archive_events
        (id,user_id,archive_id,event_type,operation_id,detail) VALUES (%s,%s,%s,%s,%s,%s)''',
        (new_id(), user_id, archive_id, kind, operation_id, Jsonb(detail)))


def lock_owner(connection, user_id: str) -> None:
    connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', (f'archive-owner:{user_id}',))
    if connection.execute("SELECT id FROM platform_users WHERE id=%s AND status='active' FOR UPDATE", (user_id,)).fetchone() is None:
        raise HTTPException(401, 'active account required')


def item_payload(row: dict) -> dict:
    archive_id = str(row['id'])
    formats = list(row['content_formats'])
    root = f'/api/v1/report-archives/{archive_id}'
    return {'id': archive_id, 'archiveId': archive_id, 'name': archive_id, 'folderName': archive_id,
            'title': row['title'] or '', 'address': row['address'] or '', 'status': 'ready',
            'savedAt': row['saved_at'].isoformat(), 'includedItems': row['included_items'],
            'sourceKind': row['source_kind'], 'provenance': row['provenance'],
            'contentFormats': formats, 'contentHash': row['content_hash'],
            'artifactSha256': row['artifact_sha256'], 'byteSize': row['byte_size'],
            'pageCount': row['page_count'], 'rendererProfile': row['renderer_profile'],
            'rendererVersion': row['renderer_version'],
            **{f'{fmt}Url': f'{root}/content?format={fmt}' for fmt in formats}}


def list_items(settings: PlatformSettings, user_id: str, cursor: str | None = None, limit: int = 50) -> dict:
    require_enabled(settings)
    values: list[Any] = [user_id]
    where = 'user_id = %s'
    if cursor:
        try:
            if len(cursor) > 256:
                raise ValueError()
            saved_at, archive_id = json.loads(base64.urlsafe_b64decode(cursor + '=' * (-len(cursor) % 4)))
            timestamp = datetime.fromisoformat(saved_at)
            if timestamp.tzinfo is None:
                raise ValueError()
            archive_id = str(uuid.UUID(archive_id))
        except (ValueError, TypeError, UnicodeError):
            raise HTTPException(422, 'invalid archive cursor')
        where += ' AND (saved_at, id) < (%s, %s)'
        values.extend([timestamp, archive_id])
    limit = max(1, min(100, limit))
    values.append(limit + 1)
    with connect(settings) as connection:
        rows = connection.execute(f'SELECT * FROM platform_archive_catalog WHERE {where} ORDER BY saved_at DESC, id DESC LIMIT %s', values).fetchall()
    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        next_cursor = base64.urlsafe_b64encode(json.dumps([last['saved_at'].isoformat(), str(last['id'])]).encode()).decode().rstrip('=')
    return {'ok': True, 'items': [item_payload(row) for row in rows[:limit]], 'nextCursor': next_cursor}


def get_item(settings: PlatformSettings, user_id: str, archive_id: str) -> dict:
    require_enabled(settings)
    with connect(settings) as connection:
        row = connection.execute('SELECT * FROM platform_archive_catalog WHERE user_id = %s AND id = %s',
                                 (user_id, archive_uuid(archive_id))).fetchone()
    if row is None:
        raise HTTPException(404, 'archive not found')
    return item_payload(row)


def validate_pdf(data: bytes) -> dict:
    if not 0 < len(data) <= MAX_BYTES:
        raise HTTPException(413, 'PDF exceeds upload limit')
    if not _CHECK_SLOTS.acquire(blocking=False):
        raise HTTPException(429, 'PDF verification is busy', headers={'Retry-After': '3'})
    try:
        result = subprocess.run([sys.executable, str(Path(__file__).with_name('pdf_upload_check.py'))],
            input=data, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        if result.returncode != 0 or len(result.stdout) > 1024:
            raise ValueError()
        checked = json.loads(result.stdout)
        if checked['sha256'] != hashlib.sha256(data).hexdigest() or checked['byteSize'] != len(data):
            raise ValueError()
        return checked
    except (subprocess.TimeoutExpired, ValueError, KeyError) as exc:
        raise HTTPException(422, 'PDF is invalid, encrypted, too complex, or contains active content') from exc
    finally:
        _CHECK_SLOTS.release()


def _upload_result(row: dict) -> dict:
    return {'uploadId': str(row['id']), 'archiveId': str(row['archive_id']) if row['archive_id'] else None,
            'status': 'completed' if row['archive_id'] else ('uploaded' if row['page_count'] else 'pending'),
            'expiresAt': row['expires_at'].isoformat()}


def begin_upload(settings: PlatformSettings, user_id: str, payload: dict) -> dict:
    require_enabled(settings, upload=True)
    request_id = str(payload.get('requestId') or '')
    sha = str(payload.get('sha256') or '')
    size = payload.get('byteSize')
    title = payload.get('title', '')
    address = payload.get('address', '')
    provenance = payload.get('provenance', 'legacy-local')
    if (not _REQUEST_RE.fullmatch(request_id) or not _SHA_RE.fullmatch(sha)
            or type(size) is not int or not 0 < size <= MAX_BYTES or payload.get('contentType') != 'application/pdf'
            or not isinstance(title, str) or not 1 <= len(title) <= 240
            or not isinstance(address, str) or len(address) > 300
            or provenance not in ('legacy-local', 'local-generated-pdf')):
        raise HTTPException(422, 'invalid PDF upload metadata')
    metadata = {'sha256': sha, 'byteSize': size, 'title': title, 'address': address, 'provenance': provenance}
    fingerprint = hashlib.sha256(json.dumps(metadata, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    with connect(settings) as connection:
        lock_owner(connection, user_id)
        connection.execute('''UPDATE platform_archive_uploads SET pdf_data=NULL,page_count=NULL
            WHERE user_id=%s AND archive_id IS NULL AND expires_at<=NOW() AND pdf_data IS NOT NULL''', (user_id,))
        existing = connection.execute('SELECT * FROM platform_archive_uploads WHERE user_id = %s AND request_id = %s FOR UPDATE',
                                      (user_id, request_id)).fetchone()
        if existing:
            if existing['fingerprint'] != fingerprint:
                raise HTTPException(409, 'requestId was used with different PDF or metadata')
            if existing['archive_id'] or existing['expires_at'] > utcnow():
                return _upload_result(existing)
        quota = connection.execute('''SELECT
            (SELECT COALESCE(SUM(byte_size),0) FROM platform_archive_imports WHERE user_id=%s) AS stored,
            (SELECT COALESCE(SUM(byte_size),0) FROM platform_archive_uploads WHERE user_id=%s AND archive_id IS NULL AND expires_at>NOW()) AS pending,
            (SELECT COUNT(*) FROM platform_archive_uploads WHERE user_id=%s AND archive_id IS NULL AND expires_at>NOW()) AS count''',
            (user_id, user_id, user_id)).fetchone()
        if quota['stored'] + quota['pending'] + size > OWNER_QUOTA or quota['count'] >= 10:
            raise HTTPException(413, 'archive storage quota exceeded')
        if existing:
            row = connection.execute('''UPDATE platform_archive_uploads SET expires_at=NOW()+INTERVAL '24 hours',
                pdf_data=NULL,page_count=NULL WHERE id=%s RETURNING *''', (existing['id'],)).fetchone()
        else:
            row = connection.execute('''INSERT INTO platform_archive_uploads
                (id,user_id,request_id,fingerprint,expected_sha256,byte_size,title,address,provenance)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
                (new_id(), user_id, request_id, fingerprint, sha, size, title, address, provenance)).fetchone()
        event(connection, user_id, 'upload_started', str(row['id']))
        return _upload_result(row)


def put_upload(settings: PlatformSettings, user_id: str, upload_id: str, data: bytes, content_type: str) -> dict:
    require_enabled(settings, upload=True)
    upload_id = archive_uuid(upload_id)
    with connect(settings) as connection:
        row = connection.execute('SELECT * FROM platform_archive_uploads WHERE user_id=%s AND id=%s', (user_id, upload_id)).fetchone()
    if row is None:
        raise HTTPException(404, 'upload not found')
    if content_type != 'application/pdf' or len(data) != row['byte_size'] or hashlib.sha256(data).hexdigest() != row['expected_sha256']:
        raise HTTPException(422, 'PDF type, size or hash mismatch')
    if row['archive_id']:
        return _upload_result(row)
    if row['expires_at'] <= utcnow():
        raise HTTPException(410, 'upload expired; restart with the same requestId')
    checked = validate_pdf(data)
    with connect(settings) as connection:
        lock_owner(connection, user_id)
        row = connection.execute('SELECT * FROM platform_archive_uploads WHERE user_id=%s AND id=%s FOR UPDATE', (user_id, upload_id)).fetchone()
        if row['archive_id']:
            return _upload_result(row)
        if row['expires_at'] <= utcnow():
            raise HTTPException(410, 'upload expired')
        row = connection.execute('UPDATE platform_archive_uploads SET pdf_data=%s,page_count=%s WHERE id=%s RETURNING *',
                                 (data, checked['pageCount'], upload_id)).fetchone()
        event(connection, user_id, 'upload_verified', upload_id)
        return _upload_result(row)


def commit_upload(settings: PlatformSettings, user_id: str, upload_id: str) -> dict:
    require_enabled(settings, upload=True)
    upload_id = archive_uuid(upload_id)
    with connect(settings) as connection:
        lock_owner(connection, user_id)
        upload = connection.execute('SELECT * FROM platform_archive_uploads WHERE user_id=%s AND id=%s FOR UPDATE', (user_id, upload_id)).fetchone()
        if upload is None:
            raise HTTPException(404, 'upload not found')
        if upload['archive_id']:
            archive_id = str(upload['archive_id'])
        else:
            if upload['expires_at'] <= utcnow():
                raise HTTPException(410, 'upload expired')
            if not upload['pdf_data'] or not upload['page_count']:
                raise HTTPException(409, 'PDF upload must be verified first')
            existing = connection.execute('''SELECT id FROM platform_archive_imports WHERE user_id=%s
                AND source_kind='uploaded-pdf' AND artifact_sha256=%s AND deleted_at IS NULL''',
                (user_id, upload['expected_sha256'])).fetchone()
            archive_id = str(existing['id']) if existing else new_id()
            if existing is None:
                connection.execute('''INSERT INTO platform_archive_imports
                    (id,user_id,source_kind,title,address,provenance,artifact_sha256,byte_size,page_count,pdf_data)
                    VALUES (%s,%s,'uploaded-pdf',%s,%s,%s,%s,%s,%s,%s)''',
                    (archive_id,user_id,upload['title'],upload['address'],upload['provenance'],upload['expected_sha256'],
                     upload['byte_size'],upload['page_count'],upload['pdf_data']))
            connection.execute('UPDATE platform_archive_uploads SET archive_id=%s,pdf_data=NULL WHERE id=%s', (archive_id, upload_id))
            event(connection, user_id, 'upload_deduplicated' if existing else 'archive_imported', upload_id, archive_id)
    return {'ok': True, **get_item(settings, user_id, archive_id)}


def create_web_snapshot(settings: PlatformSettings, user_id: str, payload: dict) -> dict:
    """Preserve the existing web HTML format without impersonating a paid final."""
    require_enabled(settings)
    request_id = str(payload.get('requestId') or '')
    snapshot = payload.get('snapshot')
    html = payload.get('html')
    if (set(payload) != {'requestId', 'snapshot', 'html'} or not _REQUEST_RE.fullmatch(request_id)
            or not isinstance(snapshot, dict) or not isinstance(html, str) or not html):
        raise HTTPException(422, 'invalid web archive')
    title, address = snapshot.get('title', ''), snapshot.get('address', '')
    pages = snapshot.get('pages')
    if (not isinstance(title, str) or not 1 <= len(title) <= 240 or not isinstance(address, str)
            or len(address) > 300 or not isinstance(pages, list) or not 1 <= len(pages) <= 200
            or not all(isinstance(page, str) for page in pages)):
        raise HTTPException(422, 'invalid snapshot metadata')
    serialized = json.dumps({'snapshot': snapshot, 'html': html}, sort_keys=True, ensure_ascii=False).encode()
    if len(serialized) > MAX_BYTES:
        raise HTTPException(413, 'web archive exceeds size limit')
    sha = hashlib.sha256(serialized).hexdigest()
    with connect(settings) as connection:
        lock_owner(connection, user_id)
        existing = connection.execute('''SELECT id,artifact_sha256 FROM platform_archive_imports
            WHERE user_id=%s AND source_kind='legacy-web' AND source_key=%s''', (user_id, request_id)).fetchone()
        if existing:
            if existing['artifact_sha256'] != sha:
                raise HTTPException(409, 'requestId was used with different content')
            archive_id = str(existing['id'])
        else:
            stored = connection.execute('''SELECT
                (SELECT COALESCE(SUM(byte_size),0) FROM platform_archive_imports WHERE user_id=%s) +
                (SELECT COALESCE(SUM(byte_size),0) FROM platform_archive_uploads WHERE user_id=%s AND archive_id IS NULL AND expires_at>NOW()) AS size''', (user_id,user_id)).fetchone()['size']
            if stored + len(serialized) > OWNER_QUOTA:
                raise HTTPException(413, 'archive storage quota exceeded')
            archive_id = new_id()
            connection.execute('''INSERT INTO platform_archive_imports
                (id,user_id,source_kind,source_key,title,address,provenance,artifact_sha256,byte_size,html_data,json_data)
                VALUES (%s,%s,'legacy-web',%s,%s,%s,'web-html-snapshot',%s,%s,%s,%s)''',
                (archive_id,user_id,request_id,title,address,sha,len(serialized),html,Jsonb(snapshot)))
            event(connection, user_id, 'web_snapshot_created', request_id, archive_id)
    return {'ok': True, **get_item(settings, user_id, archive_id)}


def delete_item(settings: PlatformSettings, user_id: str, archive_id: str) -> dict:
    require_enabled(settings)
    archive_id = archive_uuid(archive_id)
    with connect(settings) as connection:
        lock_owner(connection, user_id)
        canonical = connection.execute('SELECT id FROM platform_report_archives WHERE user_id=%s AND id=%s FOR UPDATE', (user_id, archive_id)).fetchone()
        imported = connection.execute('SELECT id FROM platform_archive_imports WHERE user_id=%s AND id=%s FOR UPDATE', (user_id, archive_id)).fetchone()
        if canonical:
            connection.execute("UPDATE platform_report_archives SET status='deleted',deleted_at=COALESCE(deleted_at,NOW()),updated_at=NOW() WHERE user_id=%s AND id=%s", (user_id, archive_id))
        elif imported:
            connection.execute('UPDATE platform_archive_imports SET deleted_at=COALESCE(deleted_at,NOW()) WHERE user_id=%s AND id=%s', (user_id, archive_id))
        else:
            raise HTTPException(404, 'archive not found')
        event(connection, user_id, 'archive_deleted', archive_id, archive_id)
    return {'ok': True, 'archiveId': archive_id}
