"""Version 1: additive ten-credit grant; ad-removal entitlement is preserved."""
from __future__ import annotations

import re
from fastapi import HTTPException
from psycopg.types.json import Jsonb

from .repository import connect, new_id, grant_paid_credits, profile_payload
from .security import constant_time_equal
from .legacy_verifier import rule_for, verify_legacy

VERSION = 1
CREDITS = 10


def _schema(connection):
    if connection.execute("SELECT version FROM schema_migrations WHERE version='013_legacy_store_migration'").fetchone() is None:
        raise HTTPException(503, 'legacy migration schema is required')


def _lock_and_find(connection, purchase):
    _schema(connection)
    # Low-volume historical migration: serialize each provider, including
    # notifications. This also covers overlapping alias sets and new tombstones.
    connection.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                       ('legacy-migration-v1:' + purchase.platform,))
    rows = connection.execute('''SELECT DISTINCT p.* FROM legacy_store_purchases p
        LEFT JOIN legacy_store_aliases a ON a.purchase_id=p.id
        WHERE p.platform=%s AND (p.identity_digest=%s OR a.alias_digest=ANY(%s))''',
        (purchase.platform, purchase.identity, list(purchase.aliases))).fetchall()
    if len(rows) > 1:
        raise HTTPException(409, 'legacy identity conflict requires review')
    row = rows[0] if rows else None
    if row and (row['product_id'] != purchase.product_id or row['identity_digest'] != purchase.identity):
        raise HTTPException(409, 'legacy identity conflict requires review')
    return row


def _aliases(connection, purchase, purchase_id):
    for alias in purchase.aliases:
        connection.execute('''INSERT INTO legacy_store_aliases(platform,alias_digest,purchase_id)
            VALUES (%s,%s,%s) ON CONFLICT DO NOTHING''', (purchase.platform, alias, purchase_id))


def _audit(connection, purchase_id, event_type, detail):
    connection.execute('INSERT INTO legacy_store_audit(id,purchase_id,event_type,detail) VALUES (%s,%s,%s,%s)',
                       (new_id(), purchase_id, event_type, Jsonb(detail)))


def _old_ownership(connection, purchase, user_id):
    rows = connection.execute('''SELECT user_id,product_id,status FROM mobile_store_transactions
        WHERE platform=%s AND (transaction_key=ANY(%s) OR verification_digest=%s)''',
        (purchase.platform, list(purchase.old_keys), purchase.receipt_digest)).fetchall()
    if any(str(r['user_id']) != user_id or r['product_id'] != purchase.product_id for r in rows):
        raise HTTPException(409, 'legacy purchase belongs to another account or product')
    if any(r['status'] == 'revoked' for r in rows):
        raise HTTPException(409, 'legacy purchase was revoked')


def _account(connection, purchase, user_id, row, *, approval=False):
    if row and row['user_id'] and str(row['user_id']) != user_id:
        raise HTTPException(409, 'legacy purchase belongs to another account')
    _old_ownership(connection, purchase, user_id)
    if connection.execute("SELECT id FROM platform_users WHERE id=%s AND status='active'", (user_id,)).fetchone() is None:
        raise HTTPException(403, 'legacy account is not active')
    bound = connection.execute('''SELECT user_id FROM legacy_store_bindings
        WHERE platform=%s AND identity_digest=%s''', (purchase.platform, purchase.identity)).fetchone()
    if bound and str(bound['user_id']) != user_id:
        raise HTTPException(409, 'legacy reviewed account mismatch')
    if purchase.account_token:
        account = connection.execute('SELECT account_token FROM mobile_store_accounts WHERE user_id=%s AND platform=%s',
                                     (user_id, purchase.platform)).fetchone()
        if account is None or not constant_time_equal(str(account['account_token']).lower(), purchase.account_token.lower()):
            raise HTTPException(409, 'legacy store account mismatch')
    elif not bound and not approval:
        raise HTTPException(409, 'legacy account binding review required')


def _eligible(rule, purchase):
    if purchase.state != 'purchased':
        raise HTTPException(422, 'legacy purchase is not currently eligible')
    if not 0 < purchase.purchased_ms < rule.purchase_before_ms:
        raise HTTPException(422, 'purchase is outside the legacy sale period')


def approve_binding(settings, *, user_id, evidence_ref, apply=False, **verification):
    """Operator CLI only. Review evidence out of band; never first-claim a receipt."""
    if not re.fullmatch(r'[A-Za-z0-9_./:-]{1,200}', evidence_ref):
        raise HTTPException(422, 'binding evidence reference is required')
    purchase = verify_legacy(settings, **verification)
    _eligible(rule_for(settings, purchase.platform, purchase.product_id), purchase)
    with connect(settings) as c:
        row = _lock_and_find(c, purchase)
        _account(c, purchase, user_id, row, approval=True)
        if row and row['state'] == 'revoked':
            raise HTTPException(409, 'legacy purchase was revoked')
        if apply:
            c.execute('''INSERT INTO legacy_store_bindings(platform,identity_digest,user_id,evidence_ref)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                (purchase.platform, purchase.identity, user_id, evidence_ref))
            _audit(c, row['id'] if row else None, 'binding_review', {'identity': purchase.identity, 'evidenceRef': evidence_ref})
    return {'version': VERSION, 'status': 'binding_confirmed' if apply else 'would_bind', 'identity': purchase.identity}


def _process_legacy(settings, *, user_id, apply=True, **verification):
    if apply and (not settings.legacy_grant_enabled or not settings.legacy_notifications_enabled):
        raise HTTPException(503, 'legacy credit migration is disabled')
    purchase = verify_legacy(settings, **verification)
    rule = rule_for(settings, purchase.platform, purchase.product_id)
    # Provider revocation may arrive first through a restore or backfill.
    if purchase.state == 'revoked':
        with connect(settings) as c:
            row = _lock_and_find(c, purchase)
            if row and row['user_id'] and str(row['user_id']) != user_id:
                raise HTTPException(409, 'legacy purchase belongs to another account')
        reconcile(settings, purchase=purchase, apply=apply)
        return {'version': VERSION, 'status': 'revoked', 'creditsGranted': 0, 'alreadyProcessed': False}
    _eligible(rule, purchase)
    with connect(settings) as c:
        row = _lock_and_find(c, purchase)
        _account(c, purchase, user_id, row)
        if row and row['state'] == 'revoked':
            return {'version': VERSION, 'status': 'revoked', 'creditsGranted': 0, 'alreadyProcessed': True}
        if row:
            if apply:
                _aliases(c, purchase, row['id'])
            result = {'version': VERSION, 'status': 'already_granted', 'creditsGranted': 0, 'alreadyProcessed': True}
        elif not apply:
            result = {'version': VERSION, 'status': 'would_grant', 'creditsGranted': CREDITS, 'alreadyProcessed': False}
        else:
            purchase_id = new_id()
            key = 'legacy-store:v1:' + purchase.platform + ':' + purchase.identity
            grant_paid_credits(c, user_id=user_id, credits=CREDITS, reason='legacy_store_migration_v1',
                idempotency_key=key, reference_type='legacy_store_purchase', reference_id=purchase_id,
                metadata={'version': VERSION, 'platform': purchase.platform, 'productId': purchase.product_id,
                          'evidenceRef': rule.evidence_ref, 'credits': CREDITS})
            ledger = c.execute('SELECT id FROM platform_credit_ledger WHERE idempotency_key=%s', (key,)).fetchone()
            c.execute('''INSERT INTO legacy_store_purchases
                (id,platform,identity_digest,product_id,user_id,state,credits_granted,grant_ledger_id,evidence_ref)
                VALUES (%s,%s,%s,%s,%s,'granted',10,%s,%s)''',
                (purchase_id, purchase.platform, purchase.identity, purchase.product_id, user_id, ledger['id'], rule.evidence_ref))
            _aliases(c, purchase, purchase_id)
            # Additional benefit. Existing source transaction and entitlement are
            # preserved; migration never revokes or expires ad removal.
            c.execute('''INSERT INTO platform_entitlements(id,user_id,store,product_id,status,pricing_policy)
                VALUES (%s,%s,%s,%s,'active','legacy') ON CONFLICT (user_id,store,product_id) DO NOTHING''',
                (new_id(), user_id, purchase.platform, purchase.product_id))
            _audit(c, purchase_id, 'granted', {'version': VERSION, 'credits': CREDITS})
            result = {'version': VERSION, 'status': 'granted', 'creditsGranted': CREDITS, 'alreadyProcessed': False}
        result['creditSummary'] = profile_payload(c, user_id)['creditSummary']
        result['identity'] = purchase.identity
        return result


def process_legacy(settings, *, user_id, apply=True, **verification):
    result = _process_legacy(settings, user_id=user_id, apply=apply, **verification)
    if (apply and verification['platform'] == 'android'
            and result['status'] in ('granted', 'already_granted')):
        from .legacy_verifier import acknowledge_legacy
        acknowledge_legacy(settings, verification['product_id'], verification['verification_data'])
    return result


def reconcile(settings, *, purchase, apply=False):
    """Verified revocations win permanently. Never debit below zero or revoke ads."""
    rule = rule_for(settings, purchase.platform, purchase.product_id)
    if purchase.state != 'revoked':
        return {'version': VERSION, 'status': 'unchanged'}
    with connect(settings) as c:
        row = _lock_and_find(c, purchase)
        if row and row['state'] == 'revoked':
            return {'version': VERSION, 'status': 'already_revoked'}
        if not apply:
            return {'version': VERSION, 'status': 'would_revoke', 'identity': purchase.identity}
        purchase_id = str(row['id']) if row else new_id()
        debit, unpaid, ledger_id = 0, 0, None
        if row:
            user = c.execute('SELECT paid_remaining FROM platform_users WHERE id=%s FOR UPDATE', (row['user_id'],)).fetchone()
            debit = min(int(row['credits_granted']), int(user['paid_remaining']))
            unpaid = int(row['credits_granted']) - debit
            if debit:
                ledger_id = new_id()
                balance = int(user['paid_remaining']) - debit
                c.execute('UPDATE platform_users SET paid_remaining=%s,updated_at=NOW() WHERE id=%s', (balance, row['user_id']))
                c.execute('''INSERT INTO platform_credit_ledger
                    (id,user_id,bucket,delta,reason,idempotency_key,reference_type,reference_id,balance_after,metadata)
                    VALUES (%s,%s,'paid',%s,'legacy_store_reversal_v1',%s,'legacy_store_purchase',%s,%s,%s)''',
                    (ledger_id, row['user_id'], -debit, 'legacy-revoke:v1:' + purchase_id, purchase_id, balance,
                     Jsonb({'creditsGranted': int(row['credits_granted']), 'reconciliationCredits': unpaid})))
            c.execute('''UPDATE legacy_store_purchases SET state='revoked',revoked_at=NOW(),verified_at=NOW(),
                reversal_ledger_id=%s,reconciliation_credits=%s WHERE id=%s''', (ledger_id, unpaid, purchase_id))
        else:
            c.execute('''INSERT INTO legacy_store_purchases(id,platform,identity_digest,product_id,state,credits_granted,evidence_ref,revoked_at)
                VALUES (%s,%s,%s,%s,'revoked',0,%s,NOW())''',
                (purchase_id, purchase.platform, purchase.identity, purchase.product_id, rule.evidence_ref))
        _aliases(c, purchase, purchase_id)
        _audit(c, purchase_id, 'revoked', {'debited': debit, 'reconciliationCredits': unpaid})
        return {'version': VERSION, 'status': 'revoked', 'debited': debit, 'reconciliationCredits': unpaid}


def history(settings, user_id):
    with connect(settings) as c:
        _schema(c)
        return [{'version': 1, 'productId': r['product_id'], 'status': r['state'],
                 'creditsGranted': r['credits_granted'], 'reconciliationCredits': r['reconciliation_credits']}
                for r in c.execute('SELECT * FROM legacy_store_purchases WHERE user_id=%s ORDER BY created_at', (user_id,))]
