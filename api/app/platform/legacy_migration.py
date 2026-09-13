"""Migration v1: once per account and verified Google subscription lineage."""
import re
from fastapi import HTTPException
from psycopg.types.json import Jsonb
from .repository import connect, new_id, grant_paid_credits, profile_payload, assert_schema, utcnow
from .security import constant_time_equal, sha256_text
from .legacy_verifier import PRODUCT, verify_legacy, token_digest, get_order, order_is_void, acknowledge_legacy

VERSION, CREDITS = 1, 10

def _lock(c):
    assert_schema(c)
    # Deliberately serialize this low-volume migration INCLUDING provider reads:
    # stale concurrent restores cannot overwrite a later RTDN observation.
    c.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', ('google-subscription-migration-v1',))

def _audit(c, lineage, event, detail):
    c.execute('INSERT INTO legacy_subscription_audit(id,lineage_id,event_type,detail) VALUES (%s,%s,%s,%s)',
        (new_id(), lineage, event, Jsonb(detail)))

def _verify(c, settings, verification):
    return verify_legacy(settings, **verification, known_anchor=lambda digest:
        c.execute('''SELECT 1 FROM legacy_subscription_tokens t JOIN legacy_subscription_lineages l ON l.id=t.lineage_id
            WHERE t.token_digest=%s AND l.user_id IS NOT NULL''', (digest,)).fetchone() is not None)

def _lineage(c, purchase):
    digests = [p.digest for p in purchase.chain] + ([purchase.anchor_digest] if purchase.anchor_digest else [])
    rows = c.execute('''SELECT DISTINCT l.* FROM legacy_subscription_lineages l
        JOIN legacy_subscription_tokens t ON t.lineage_id=l.id WHERE t.token_digest=ANY(%s)''', (digests,)).fetchall()
    if len(rows) > 1:
        raise HTTPException(409, 'subscription lineage conflict requires review')
    return rows[0] if rows else {'id': new_id(), 'root_digest': purchase.chain[-1].digest,
                               'user_id': None, 'binding_kind': None, 'evidence_ref': None}

def _account(c, purchase, lineage, user_id, reviewed=False):
    if lineage['user_id'] and str(lineage['user_id']) != user_id:
        raise HTTPException(409, 'subscription belongs to another account')
    if not c.execute("SELECT id FROM platform_users WHERE id=%s AND status='active' FOR UPDATE", (user_id,)).fetchone():
        raise HTTPException(403, 'subscription account is not active')
    # Honor old transaction ownership without using those records as eligibility.
    for item in purchase.chain:
        rows = c.execute('''SELECT user_id FROM mobile_store_transactions WHERE platform='android'
            AND (verification_digest=%s OR transaction_key=%s)''',
            (sha256_text(item.token), item.latest_order_id or '')).fetchall()
        if any(str(r['user_id']) != user_id for r in rows):
            raise HTTPException(409, 'historical subscription account mismatch')
    accounts = [p.account_token for p in purchase.chain if p.account_token]
    expected = c.execute("SELECT account_token FROM mobile_store_accounts WHERE user_id=%s AND platform='android'", (user_id,)).fetchone()
    if accounts and (not expected or any(not constant_time_equal(str(expected['account_token']).lower(), a.lower()) for a in accounts)):
        raise HTTPException(409, 'Google subscription account mismatch')
    if not accounts and not lineage['user_id'] and not reviewed:
        raise HTTPException(409, 'subscription account binding review required')
    lineage['user_id'] = user_id
    lineage['binding_kind'] = lineage['binding_kind'] or ('provider' if accounts else 'reviewed')

def _persist(c, purchase, lineage):
    c.execute('''INSERT INTO legacy_subscription_lineages(id,root_digest,user_id,binding_kind,evidence_ref)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET user_id=EXCLUDED.user_id,
        binding_kind=EXCLUDED.binding_kind,evidence_ref=EXCLUDED.evidence_ref''',
        (lineage['id'], lineage['root_digest'], lineage['user_id'], lineage['binding_kind'], lineage['evidence_ref']))
    for index, item in enumerate(purchase.chain):
        c.execute('''INSERT INTO legacy_subscription_tokens(token_digest,lineage_id,subscription_state,
            expires_at,entitlement_active,latest_order_id,verified_at) VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(token_digest) DO UPDATE SET subscription_state=EXCLUDED.subscription_state,
            expires_at=EXCLUDED.expires_at,entitlement_active=EXCLUDED.entitlement_active,
            latest_order_id=EXCLUDED.latest_order_id,verified_at=EXCLUDED.verified_at''',
            (item.digest, lineage['id'], item.state, item.expires_at,
             index == 0 and purchase.eligible and item.entitled, item.latest_order_id, purchase.observed_at))
    for item in purchase.chain:
        if item.linked_token:
            ancestor = token_digest(item.linked_token)
            existing = c.execute('SELECT superseded_by FROM legacy_subscription_tokens WHERE token_digest=%s', (ancestor,)).fetchone()
            if existing and existing['superseded_by'] not in (None, item.digest):
                raise HTTPException(409, 'branched subscription lineage requires review')
            c.execute('UPDATE legacy_subscription_tokens SET superseded_by=%s WHERE token_digest=%s', (item.digest, ancestor))

def _refresh_entitlement(c, user_id):
    if not user_id:
        return
    row = c.execute('''SELECT MAX(t.expires_at) AS expiry FROM legacy_subscription_tokens t
        JOIN legacy_subscription_lineages l ON l.id=t.lineage_id
        WHERE l.user_id=%s AND t.entitlement_active AND t.expires_at>clock_timestamp()
          AND t.superseded_by IS NULL AND t.revoked_at IS NULL
          AND NOT EXISTS(SELECT 1 FROM legacy_subscription_token_revocations r WHERE r.token_digest=t.token_digest)
          AND NOT EXISTS(SELECT 1 FROM legacy_subscription_order_voids v WHERE v.order_id=t.latest_order_id)''', (user_id,)).fetchone()
    expiry = row['expiry']
    c.execute('''INSERT INTO platform_entitlements(id,user_id,store,product_id,status,pricing_policy,expires_at,verified_at)
        VALUES (%s,%s,'android',%s,%s,'legacy',%s,NOW()) ON CONFLICT(user_id,store,product_id)
        DO UPDATE SET status=EXCLUDED.status,expires_at=EXCLUDED.expires_at,verified_at=NOW(),updated_at=NOW()''',
        (new_id(), user_id, PRODUCT, 'active' if expiry else 'revoked', expiry))

def _reverse(c, order_id, reason):
    c.execute('INSERT INTO legacy_subscription_order_voids(order_id,reason) VALUES (%s,%s) ON CONFLICT DO NOTHING', (order_id, reason))
    row = c.execute('SELECT * FROM legacy_subscription_grants WHERE source_order_id=%s FOR UPDATE', (order_id,)).fetchone()
    if not row or row['state'] == 'revoked':
        return {'status': 'already_revoked' if row else 'void_recorded', 'debited': 0, 'reconciliationCredits': 0}
    user = c.execute('SELECT paid_remaining FROM platform_users WHERE id=%s FOR UPDATE', (row['user_id'],)).fetchone()
    debit = min(CREDITS, int(user['paid_remaining']))
    unpaid, ledger = CREDITS - debit, None
    if debit:
        ledger, balance = new_id(), int(user['paid_remaining']) - debit
        c.execute('UPDATE platform_users SET paid_remaining=%s,updated_at=NOW() WHERE id=%s', (balance, row['user_id']))
        c.execute('''INSERT INTO platform_credit_ledger(id,user_id,bucket,delta,reason,idempotency_key,
            reference_type,reference_id,balance_after,metadata)
            VALUES (%s,%s,'paid',%s,'legacy_subscription_reversal_v1',%s,'legacy_subscription_grant',%s,%s,%s)''',
            (ledger, row['user_id'], -debit, 'legacy-sub-revoke:v1:' + str(row['id']), str(row['id']), balance,
             Jsonb({'reconciliationCredits': unpaid})))
    c.execute('''UPDATE legacy_subscription_grants SET state='revoked',revoked_at=NOW(),
        reversal_ledger_id=%s,reconciliation_credits=%s WHERE id=%s''', (ledger, unpaid, row['id']))
    _audit(c, row['lineage_id'], 'source_payment_reversed', {'debited': debit, 'reconciliationCredits': unpaid, 'reason': reason})
    _refresh_entitlement(c, row['user_id'])
    return {'status': 'revoked', 'debited': debit, 'reconciliationCredits': unpaid}

def _reconcile_source(c, settings, lineage):
    grant = c.execute("SELECT * FROM legacy_subscription_grants WHERE lineage_id=%s AND state='granted'", (lineage['id'],)).fetchone()
    if grant and order_is_void(get_order(settings, grant['source_order_id'])):
        return _reverse(c, grant['source_order_id'], 'refunded')
    return {'status': 'unchanged'}

def approve_binding(settings, *, user_id, evidence_ref, apply=False, **verification):
    if not re.fullmatch(r'[A-Za-z0-9_./:-]{1,200}', evidence_ref):
        raise HTTPException(422, 'binding evidence reference is required')
    with connect(settings) as c:
        _lock(c)
        purchase = _verify(c, settings, verification)
        if not purchase.eligible:
            raise HTTPException(422, 'currently entitled paid subscription required')
        lineage = _lineage(c, purchase)
        _account(c, purchase, lineage, user_id, reviewed=True)
        lineage['evidence_ref'] = evidence_ref
        _persist(c, purchase, lineage)
        _audit(c, lineage['id'], 'binding_review', {'evidenceRef': evidence_ref})
        if not apply:
            c.rollback()
    return {'version': VERSION, 'status': 'binding_confirmed' if apply else 'would_bind'}

def process_legacy(settings, *, user_id, apply=True, **verification):
    with connect(settings) as c:
        _lock(c)
        purchase = _verify(c, settings, verification)
        lineage = _lineage(c, purchase)
        _account(c, purchase, lineage, user_id)
        _persist(c, purchase, lineage)
        _reconcile_source(c, settings, lineage)
        if purchase.order and order_is_void(purchase.order):
            _reverse(c, purchase.current.latest_order_id, 'refunded')
        current = c.execute('SELECT * FROM legacy_subscription_tokens WHERE token_digest=%s', (purchase.current.digest,)).fetchone()
        blocked_order = c.execute('SELECT 1 FROM legacy_subscription_order_voids WHERE order_id=%s', (purchase.current.latest_order_id,)).fetchone()
        revoked = c.execute('SELECT 1 FROM legacy_subscription_token_revocations WHERE token_digest=%s', (purchase.current.digest,)).fetchone()
        eligible = (purchase.eligible and purchase.current.expires_at > utcnow()
            and not current['superseded_by'] and not current['revoked_at'] and not blocked_order and not revoked)
        previous = c.execute('SELECT state FROM legacy_subscription_grants WHERE user_id=%s OR lineage_id=%s', (user_id, lineage['id'])).fetchone()
        status, credits = 'ineligible', 0
        if previous:
            status = 'already_granted' if previous['state'] == 'granted' else 'revoked'
        elif eligible:
            if not settings.legacy_grant_enabled or not settings.legacy_notifications_enabled:
                status = 'grant_disabled'
            elif c.execute('SELECT 1 FROM legacy_store_purchases WHERE credits_granted>0 LIMIT 1').fetchone():
                raise HTTPException(409, 'superseded migration grants require reconciliation before activation')
            else:
                grant_id = new_id()
                key = 'legacy-subscription:v1:' + user_id
                grant_paid_credits(c, user_id=user_id, credits=CREDITS, reason='legacy_subscription_migration_v1',
                    idempotency_key=key, reference_type='legacy_subscription_grant', reference_id=grant_id,
                    metadata={'version': VERSION, 'platform': 'android', 'productId': PRODUCT})
                ledger = c.execute('SELECT id FROM platform_credit_ledger WHERE idempotency_key=%s', (key,)).fetchone()
                c.execute('''INSERT INTO legacy_subscription_grants(id,user_id,lineage_id,source_token_digest,
                    source_order_id,state,grant_ledger_id) VALUES (%s,%s,%s,%s,%s,'granted',%s)''',
                    (grant_id, user_id, lineage['id'], purchase.current.digest, purchase.current.latest_order_id, ledger['id']))
                _audit(c, lineage['id'], 'granted', {'credits': CREDITS, 'version': VERSION})
                status, credits = 'granted' if apply else 'would_grant', CREDITS
        _refresh_entitlement(c, user_id)
        profile = profile_payload(c, user_id)
        entitlement = next((e for e in profile['storeEntitlements'] if e['store'] == 'android' and e['productId'] == PRODUCT), None)
        result = {'version': VERSION, 'status': status, 'creditsGranted': credits,
            'alreadyProcessed': previous is not None, 'subscriptionState': purchase.current.state,
            'entitlementActive': entitlement is not None, 'expiresAt': entitlement['expiresAt'] if entitlement else None,
            'creditSummary': profile['creditSummary']}
        if not apply:
            c.rollback()
    if apply and eligible and not purchase.current.acknowledged:
        acknowledge_legacy(settings, purchase.current.token)
    return result

def reconcile(settings, *, apply=False, **verification):
    with connect(settings) as c:
        _lock(c)
        purchase = _verify(c, settings, verification)
        lineage = _lineage(c, purchase)
        _persist(c, purchase, lineage)
        result = _reconcile_source(c, settings, lineage)
        if purchase.order and order_is_void(purchase.order):
            current_result = _reverse(c, purchase.current.latest_order_id, 'refunded')
            if result['status'] != 'revoked':
                result = current_result
        _refresh_entitlement(c, lineage['user_id'])
        if not apply:
            c.rollback()
            if result['status'] == 'revoked':
                result['status'] = 'would_revoke'
        return {'version': VERSION, **result}

def history(settings, user_id):
    with connect(settings) as c:
        assert_schema(c)
        return [{'version': 1, 'productId': PRODUCT, 'status': r['state'], 'creditsGranted': r['credits'],
            'reconciliationCredits': r['reconciliation_credits']} for r in c.execute(
            'SELECT * FROM legacy_subscription_grants WHERE user_id=%s ORDER BY created_at', (user_id,))]


def reconcile_order(settings, *, order_id, apply=False):
    """Recheck an already recorded grant source even after its token expires."""
    with connect(settings) as c:
        _lock(c)
        grant = c.execute('SELECT 1 FROM legacy_subscription_grants WHERE source_order_id=%s', (order_id,)).fetchone()
        if not grant:
            raise HTTPException(422, 'recorded grant source order required')
        order = get_order(settings, order_id)
        result = _reverse(c, order_id, 'refunded') if order_is_void(order) else {'status': 'unchanged'}
        if not apply:
            c.rollback()
            if result['status'] == 'revoked':
                result['status'] = 'would_revoke'
        return {'version': VERSION, **result}
