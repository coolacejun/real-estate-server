"""Reviewable, resumable legacy archive import. Default mode is read-only.

This tool never moves identities, balances, ledger entries or payment ownership.
Unmapped/conflicting accounts go to review, with no email-based matching.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'api'))
from psycopg.types.json import Jsonb
from app.platform.config import get_settings
from app.platform.repository import connect, new_id


def source_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def read_sources(path: Path):
    # Require a consistent SQLite backup, not a live WAL database file copy.
    if Path(str(path) + '-wal').exists():
        raise ValueError('Use a SQLite backup snapshot; a WAL sidecar is present.')
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute('BEGIN')
        for row in connection.execute('''SELECT a.*,l.platform_user_id AS mapped_user_id,u.withdrawn_at
            FROM report_archives a JOIN users u ON u.id=a.user_id
            LEFT JOIN platform_account_links l ON l.local_user_id=a.user_id
            WHERE a.deleted_at IS NULL ORDER BY a.user_id,a.id'''):
            yield dict(row)
    finally:
        connection.close()


def prepare_plan(path: Path, connection):
    ready, review = [], []
    for source in read_sources(path):
        key = f"legacy-web:{source['user_id']}:{source['id']}"
        owner = connection.execute('''SELECT a.user_id FROM platform_external_accounts a
            JOIN platform_users u ON u.id=a.user_id WHERE a.namespace='web' AND a.external_id=%s AND u.status='active' ''',
            (str(source['user_id']),)).fetchone()
        reason = None
        if source['withdrawn_at']:
            reason = 'withdrawn_source_account'
        elif not owner or not source['mapped_user_id']:
            reason = 'unmapped_account_requires_authenticated_connection_review'
        elif str(owner['user_id']) != source['mapped_user_id']:
            reason = 'conflicting_central_user_ids'
        elif not source['html_content'] or not source['json_content']:
            reason = 'missing_sql_snapshot_content'
        try:
            snapshot = json.loads(source['json_content'] or '{}')
            serialized = json.dumps({'snapshot': snapshot, 'html': source['html_content']}, sort_keys=True, ensure_ascii=False).encode()
            if not isinstance(snapshot, dict) or not isinstance(snapshot.get('pages'), list) or not 1 <= len(snapshot['pages']) <= 200:
                reason = reason or 'invalid_snapshot_pages'
            if len(serialized) > 16 * 1024 * 1024:
                reason = reason or 'archive_exceeds_import_limit'
        except (ValueError, TypeError):
            reason = reason or 'invalid_snapshot_json'
        if reason:
            review.append({'sourceKey': key, 'reason': reason})
            continue
        ready.append({'sourceKey': key, 'archiveId': str(uuid.uuid5(uuid.NAMESPACE_URL, f'building-land:{key}')),
            'ownerUserId': str(owner['user_id']), 'sha256': hashlib.sha256(serialized).hexdigest(),
            'byteSize': len(serialized), 'sourceId': source['id'], 'externalId': str(source['user_id'])})
    return {'version': 1, 'sourceSha256': source_digest(path), 'ready': ready, 'review': review}


def apply_plan(path: Path, connection, plan: dict, migration_id: str):
    # Recompute ownership and source content under the same central transaction.
    # A stale or edited plan cannot assign an archive to a different owner.
    if prepare_plan(path, connection) != plan:
        raise ValueError('Plan/source/ownership changed; generate and review a new dry-run plan.')
    sources = {f"legacy-web:{s['user_id']}:{s['id']}": s for s in read_sources(path)}
    inserted = 0
    for item in plan['ready']:
        source = sources[item['sourceKey']]
        serialized = json.dumps({'snapshot': json.loads(source['json_content']), 'html': source['html_content']}, sort_keys=True, ensure_ascii=False).encode()
        if hashlib.sha256(serialized).hexdigest() != item['sha256']:
            raise ValueError('Source snapshot changed while reading; no imports committed.')
        owner = connection.execute('''SELECT a.user_id FROM platform_external_accounts a
            JOIN platform_users u ON u.id=a.user_id WHERE a.namespace='web' AND a.external_id=%s
            AND u.status='active' FOR SHARE OF a,u''', (item['externalId'],)).fetchone()
        if not owner or str(owner['user_id']) != item['ownerUserId']:
            raise ValueError('Account mapping changed during migration.')
        existing = connection.execute('SELECT user_id,artifact_sha256 FROM platform_archive_imports WHERE id=%s', (item['archiveId'],)).fetchone()
        if existing:
            if str(existing['user_id']) != item['ownerUserId'] or existing['artifact_sha256'] != item['sha256']:
                raise ValueError('Existing import differs; never overwrite it.')
            continue
        connection.execute('''INSERT INTO platform_archive_imports
            (id,user_id,source_kind,source_key,title,address,provenance,artifact_sha256,byte_size,html_data,json_data,migration_id,saved_at)
            VALUES (%s,%s,'legacy-web',%s,%s,%s,'legacy-web-import',%s,%s,%s,%s,%s,%s)''',
            (item['archiveId'],item['ownerUserId'],item['sourceKey'],str(source['title'] or '')[:240],
             str(source['address'] or '')[:300],item['sha256'],item['byteSize'],source['html_content'],
             Jsonb(json.loads(source['json_content'])),migration_id,source['saved_at']))
        connection.execute('''INSERT INTO platform_archive_events(id,user_id,archive_id,event_type,operation_id,detail)
            VALUES (%s,%s,%s,'legacy_imported',%s,%s)''',
            (new_id(),item['ownerUserId'],item['archiveId'],migration_id,Jsonb({'sourceKey': item['sourceKey']})))
        inserted += 1
    for item in plan['review']:
        connection.execute('''INSERT INTO platform_account_review_queue(id,source_key,reason,evidence)
            VALUES (%s,%s,%s,%s) ON CONFLICT(source_key) DO NOTHING''',
            (new_id(),item['sourceKey'],item['reason'],Jsonb({'migrationId': migration_id})))
    return {'inserted': inserted, 'alreadyImported': len(plan['ready']) - inserted, 'review': len(plan['review'])}


def rollback(connection, migration_id: str, *, apply: bool):
    rows = connection.execute('SELECT id,user_id FROM platform_archive_imports WHERE migration_id=%s AND deleted_at IS NULL' + (' FOR UPDATE' if apply else ''), (migration_id,)).fetchall()
    if apply:
        for row in rows:
            connection.execute('UPDATE platform_archive_imports SET deleted_at=NOW() WHERE id=%s', (row['id'],))
            connection.execute('''INSERT INTO platform_archive_events(id,user_id,archive_id,event_type,operation_id)
                VALUES (%s,%s,%s,'legacy_import_hidden',%s)''', (new_id(),row['user_id'],row['id'],migration_id))
    return {'importsToHide': len(rows), 'applied': apply, 'originalsAndOwnershipRetained': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--web-snapshot', type=Path)
    parser.add_argument('--plan', type=Path)
    parser.add_argument('--plan-sha256')
    parser.add_argument('--migration-id')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--rollback', action='store_true')
    args = parser.parse_args()
    if not os.environ.get('DATABASE_URL'):
        parser.error('DATABASE_URL must be supplied by the operator; environment files are not read.')
    if args.apply and not args.migration_id:
        parser.error('--apply requires an audited --migration-id')
    with connect(get_settings()) as connection:
        if args.rollback:
            if not args.migration_id:
                parser.error('--rollback requires --migration-id')
            if not args.apply:
                connection.execute('SET TRANSACTION READ ONLY')
            print(json.dumps(rollback(connection, args.migration_id, apply=args.apply)))
            return
        if not args.web_snapshot or not args.plan:
            parser.error('--web-snapshot and --plan are required')
        if args.apply:
            content = args.plan.read_bytes()
            if not args.plan_sha256 or hashlib.sha256(content).hexdigest() != args.plan_sha256:
                parser.error('Provide the SHA-256 of the reviewed plan with --plan-sha256.')
            result = apply_plan(args.web_snapshot, connection, json.loads(content), args.migration_id)
        else:
            connection.execute('SET TRANSACTION READ ONLY')
            plan = prepare_plan(args.web_snapshot, connection)
            content = json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2).encode()
            args.plan.write_bytes(content)
            result = {'ready': len(plan['ready']), 'review': len(plan['review']),
                      'planSha256': hashlib.sha256(content).hexdigest(), 'applied': False}
        print(json.dumps(result))


if __name__ == '__main__':
    main()
