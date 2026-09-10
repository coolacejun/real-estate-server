from __future__ import annotations

import time

from fastapi import HTTPException

from .repository import connect, record_auth_event
from .security import sha256_text


def auth_binding(session: dict) -> str:
    value = session.get('session_hash') if session.get('auth_type') == 'cookie' else session.get('family_id')
    if not value:
        raise HTTPException(401, 'authenticated session required')
    return sha256_text(f"{session.get('auth_type', 'bearer')}:{value}")


def require_recent_auth(settings, session: dict) -> None:
    if session.get('auth_type') == 'cookie':
        authenticated_at = session.get('authenticated_at', 0)
    else:
        with connect(settings) as connection:
            row = connection.execute('''SELECT MIN(created_at) AS authenticated_at FROM (
                SELECT created_at FROM mobile_refresh_tokens WHERE family_id=%s
                UNION ALL SELECT created_at FROM mobile_access_tokens WHERE family_id=%s
                ) original_auth''', (session['family_id'], session['family_id'])).fetchone()
        authenticated_at = row['authenticated_at'].timestamp() if row['authenticated_at'] else 0
    age = time.time() - authenticated_at
    if not 0 <= age <= 600:
        raise HTTPException(403, 'recent login required; sign in again before changing account connections')


def disconnect_identity(settings, session: dict, provider: str) -> dict:
    require_recent_auth(settings, session)
    if provider not in ('kakao', 'naver', 'google'):
        raise HTTPException(422, 'unsupported identity')
    user_id = session['user_id']
    with connect(settings) as connection:
        user = connection.execute("SELECT id FROM platform_users WHERE id=%s AND status='active' FOR UPDATE", (user_id,)).fetchone()
        if user is None:
            raise HTTPException(401, 'active account required')
        rows = connection.execute('SELECT provider FROM platform_identities WHERE user_id=%s AND is_active FOR UPDATE', (user_id,)).fetchall()
        if not any(row['provider'] == provider for row in rows):
            raise HTTPException(404, 'identity not connected')
        if not any(row['provider'] != provider for row in rows):
            raise HTTPException(409, 'cannot disconnect the last login method')
        connection.execute('UPDATE platform_identities SET is_active=FALSE,disconnected_at=NOW(),updated_at=NOW() WHERE user_id=%s AND provider=%s', (user_id, provider))
        _revoke_account_sessions(connection, user_id, 'identity_disconnected')
        record_auth_event(connection, event_type='identity_disconnected', user_id=user_id, provider=provider)
    return {'ok': True, 'reauthenticationRequired': True}


def _revoke_account_sessions(connection, user_id: str, reason: str):
    connection.execute('UPDATE platform_users SET auth_version=auth_version+1,updated_at=NOW() WHERE id=%s', (user_id,))
    connection.execute('UPDATE mobile_access_tokens SET revoked_at=COALESCE(revoked_at,NOW()) WHERE user_id=%s', (user_id,))
    connection.execute('UPDATE mobile_refresh_tokens SET revoked_at=COALESCE(revoked_at,NOW()),revoke_reason=COALESCE(revoke_reason,%s) WHERE user_id=%s', (reason, user_id))
    connection.execute("UPDATE mobile_oauth_flows SET status='failed' WHERE link_user_id=%s AND status IN ('pending','processing')", (user_id,))
    connection.execute('UPDATE mobile_auth_codes SET consumed_at=COALESCE(consumed_at,NOW()) WHERE user_id=%s', (user_id,))


def withdraw_account(settings, session: dict, confirmed: bool) -> dict:
    require_recent_auth(settings, session)
    if not confirmed:
        raise HTTPException(422, 'explicit withdrawal confirmation required')
    user_id = session['user_id']
    with connect(settings) as connection:
        user = connection.execute('SELECT * FROM platform_users WHERE id=%s FOR UPDATE', (user_id,)).fetchone()
        if user is None or user['status'] != 'active':
            raise HTTPException(401, 'active account required')
        pending = connection.execute('''SELECT
            EXISTS(SELECT 1 FROM platform_report_usages WHERE user_id=%s AND status='pending') OR
            EXISTS(SELECT 1 FROM platform_credit_reversals WHERE user_id=%s AND status='pending') OR
            EXISTS(SELECT 1 FROM mobile_store_transactions WHERE user_id=%s AND post_commit_status IN ('pending','failed')) AS busy''',
            (user_id,user_id,user_id)).fetchone()['busy']
        if user['paid_remaining'] or pending:
            raise HTTPException(409, 'remaining purchased credits or pending transactions require review before withdrawal')
        connection.execute("UPDATE platform_users SET status='withdrawn',withdrawn_at=NOW() WHERE id=%s", (user_id,))
        _revoke_account_sessions(connection, user_id, 'withdrawn')
        record_auth_event(connection, event_type='account_withdrawn', user_id=user_id,
                          detail={'ownershipRetained': True, 'ledgerRetained': True})
    return {'ok': True, 'reauthenticationRequired': True}
