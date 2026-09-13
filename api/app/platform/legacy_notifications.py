"""Authenticated provider notifications, independent of the grant kill switch."""
import base64
import json
from dataclasses import replace

from fastapi import HTTPException
from .legacy_verifier import (apple_verifier, identity_digest, LegacyPurchase, rule_for, rules,
                              verify_legacy, _enum)
from .legacy_migration import reconcile
from .repository import connect
from .security import sha256_text


def _enabled(settings):
    if not settings.legacy_notifications_enabled:
        raise HTTPException(503, 'legacy notifications are disabled')


def apple_notification(settings, payload):
    _enabled(settings)
    try:
        signed = payload['signedPayload']
        if not isinstance(signed, str) or len(signed) > 2*1024*1024:
            raise ValueError()
        verifier = apple_verifier(settings)
        event = verifier.verify_and_decode_notification(signed)
        if _enum(event.notificationType) not in ('REFUND', 'REVOKE'):
            return {'status': 'ignored'}
        item = verifier.verify_and_decode_signed_transaction(event.data.signedTransactionInfo)
        if (item.revocationDate is None or _enum(item.type) != 'Non-Consumable'
                or _enum(item.inAppOwnershipType) != 'PURCHASED'):
            raise ValueError()
        if not any(r.platform == 'ios' and r.product_id == item.productId for r in rules(settings)):
            return {'status': 'ignored'}
        canonical = identity_digest('apple', item.originalTransactionId)
        purchase = LegacyPurchase('ios', item.productId, canonical,
            tuple(sorted({canonical, identity_digest('apple', item.transactionId)})),
            (item.transactionId, item.originalTransactionId), str(item.appAccountToken or ''),
            int(item.originalPurchaseDate or item.purchaseDate), 'revoked', sha256_text(signed))
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(422, 'Apple notification rejected') from None
    return reconcile(settings, purchase=purchase, apply=True)


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
    _enabled(settings)
    verify_google_push(settings, authorization)
    try:
        encoded = payload['message']['data']
        if not isinstance(encoded, str) or len(encoded) > 2*1024*1024:
            raise ValueError()
        event = json.loads(base64.b64decode(encoded, validate=True))
        if event['packageName'] != settings.google_play_package_name:
            raise ValueError()
        notice = event.get('oneTimeProductNotification')
        voided = event.get('voidedPurchaseNotification')
        if notice and notice.get('notificationType') == 2:
            token = notice['purchaseToken']
            products = [notice['sku']]
        elif voided and voided.get('productType') == 2:
            token = voided['purchaseToken']
            # Voided notifications omit SKU; resolve only our persisted identity
            # or a provider-verified allowlist match, never caller-supplied status.
            with connect(settings) as c:
                row = c.execute("SELECT product_id FROM legacy_store_purchases WHERE platform='android' AND identity_digest=%s",
                                (identity_digest('google-token', token),)).fetchone()
            products = [row['product_id']] if row else [r.product_id for r in rules(settings) if r.platform == 'android']
        else:
            return {'status': 'ignored'}
    except Exception:
        raise HTTPException(422, 'Google notification rejected') from None
    allowed = {r.product_id for r in rules(settings) if r.platform == 'android'}
    for product_id in products:
        if product_id not in allowed:
            continue
        # The authenticated cancel/void notification is itself authoritative for
        # a known token. This works even when a refunded token later returns 410.
        canonical = identity_digest('google-token', token)
        with connect(settings) as c:
            known = c.execute("SELECT product_id FROM legacy_store_purchases WHERE platform='android' AND identity_digest=%s",
                              (canonical,)).fetchone()
        if known:
            if known['product_id'] != product_id:
                raise HTTPException(409, 'Google notification product mismatch')
            purchase = LegacyPurchase('android', product_id, canonical, (canonical,), (), '', 0, 'revoked', '')
        else:
            try:
                purchase = verify_legacy(settings, platform='android', product_id=product_id, verification_data=token)
            except HTTPException:
                continue
            purchase = replace(purchase, state='revoked')
        return reconcile(settings, purchase=purchase, apply=True)
    # Retry rather than acknowledge a refund whose identity could not be checked.
    if any(p in allowed for p in products):
        raise HTTPException(503, 'Google notification identity needs retry')
    return {'status': 'ignored'}
