"""Authenticated Google RTDN; credit reversal matches the original grant order."""
import base64
import json
from fastapi import HTTPException
from .legacy_verifier import PRODUCT, token_digest
from .legacy_migration import (_lock, _verify, _lineage, _persist, _reverse,
    _reconcile_source, _refresh_entitlement, _audit)
from .repository import connect

def apple_notification(settings, payload):
    raise HTTPException(410, 'Apple legacy migration is not supported')

def verify_google_push(settings, authorization):
    if not settings.google_rtdn_audience or not settings.google_rtdn_email:
        raise HTTPException(503, 'Google push authentication is not configured')
    try:
        from google.oauth2 import id_token
        from google.auth.transport.requests import Request
        if not authorization or not authorization.startswith('Bearer '):
            raise ValueError()
        claims = id_token.verify_oauth2_token(authorization[7:], Request(), audience=settings.google_rtdn_audience)
        if claims.get('email') != settings.google_rtdn_email or claims.get('email_verified') is not True:
            raise ValueError()
    except Exception:
        raise HTTPException(403, 'Google push authentication failed') from None

def google_notification(settings, payload, authorization):
    if not settings.legacy_notifications_enabled:
        raise HTTPException(503, 'legacy notifications are disabled')
    verify_google_push(settings, authorization)
    try:
        encoded = payload['message']['data']
        if not isinstance(encoded, str) or len(encoded) > 2*1024*1024:
            raise ValueError()
        event = json.loads(base64.b64decode(encoded, validate=True))
        if event['packageName'] != settings.google_play_package_name:
            raise ValueError()
        notice, void = event.get('subscriptionNotification'), event.get('voidedPurchaseNotification')
        if void and void.get('productType') == 1:
            token, order_id, kind = void['purchaseToken'], void['orderId'], 'void'
            if not isinstance(order_id, str) or not 0 < len(order_id) <= 250:
                raise ValueError()
        elif notice and notice.get('subscriptionId') == PRODUCT:
            token, order_id, kind = notice['purchaseToken'], None, notice['notificationType']
        else:
            return {'status': 'ignored'}
        digest = token_digest(token)
    except (KeyError, ValueError, TypeError):
        raise HTTPException(422, 'Google notification rejected') from None
    # Commit authenticated tombstones first. Provider unavailability must not
    # reopen a revoked token or lose an order void that preceded a claim.
    if kind in ('void', 12):
        with connect(settings) as c:
            _lock(c)
            if kind == 'void':
                result = _reverse(c, order_id, 'refunded')
            else:
                c.execute('INSERT INTO legacy_subscription_token_revocations(token_digest) VALUES (%s) ON CONFLICT DO NOTHING', (digest,))
                result = {'status': 'revoked_token'}
            owner = c.execute('''SELECT l.user_id FROM legacy_subscription_lineages l
                JOIN legacy_subscription_tokens t ON t.lineage_id=l.id WHERE t.token_digest=%s''', (digest,)).fetchone()
            if owner:
                _refresh_entitlement(c, owner['user_id'])
        if kind == 'void':
            return result
    with connect(settings) as c:
        _lock(c)
        purchase = _verify(c, settings, dict(platform='android', product_id=PRODUCT, verification_data=token))
        lineage = _lineage(c, purchase)
        _persist(c, purchase, lineage)
        result = _reconcile_source(c, settings, lineage)
        # REVOKED applies to current entitlement; only reverse the grant when
        # the provider's current order is the grant source. Other renewals do not.
        if kind == 12:
            grant = c.execute('SELECT source_order_id FROM legacy_subscription_grants WHERE lineage_id=%s', (lineage['id'],)).fetchone()
            if grant and grant['source_order_id'] == purchase.current.latest_order_id and not purchase.current.entitled:
                result = _reverse(c, grant['source_order_id'], 'revoked')
        _refresh_entitlement(c, lineage['user_id'])
        _audit(c, lineage['id'], 'subscription_sync', {'subscriptionState': purchase.current.state, 'notificationType': kind})
        return result
