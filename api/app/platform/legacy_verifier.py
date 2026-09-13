"""Fresh provider verification for migration v1. Never log provider payloads."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException

from .config import PlatformSettings
from .security import sha256_text
from .store import AppleStoreVerifier, GooglePlayVerifier, PRODUCTS


@dataclass(frozen=True)
class LegacyRule:
    platform: str
    product_id: str
    purchase_before_ms: int
    evidence_ref: str


def rules(settings: PlatformSettings) -> list[LegacyRule]:
    if not settings.legacy_policy_file:
        return []
    try:
        data = json.loads(Path(settings.legacy_policy_file).read_text(encoding='utf-8'))
        if data['version'] != 1 or not isinstance(data['products'], list):
            raise ValueError()
        result = []
        for row in data['products']:
            if (row['platform'] not in ('ios', 'android') or row['kind'] != 'non_consumable'
                    or row['productId'] in PRODUCTS
                    or not re.fullmatch(r'[A-Za-z0-9_.-]{1,200}', row['productId'])
                    or not re.fullmatch(r'[A-Za-z0-9_./:-]{1,200}', row['evidenceRef'])):
                raise ValueError()
            cutoff = datetime.fromisoformat(row['purchaseBefore'].replace('Z', '+00:00'))
            if cutoff.tzinfo is None or cutoff > datetime.now(timezone.utc):
                raise ValueError()
            result.append(LegacyRule(row['platform'], row['productId'], int(cutoff.timestamp()*1000), row['evidenceRef']))
        if len({(r.platform, r.product_id) for r in result}) != len(result):
            raise ValueError()
        return result
    except (OSError, ValueError, KeyError, TypeError):
        raise HTTPException(503, 'legacy policy configuration is invalid') from None


def rule_for(settings, platform, product_id):
    rule = next((r for r in rules(settings) if r.platform == platform and r.product_id == product_id), None)
    if rule is None:
        raise HTTPException(422, 'legacy product is not allowlisted')
    return rule


@dataclass(frozen=True)
class LegacyPurchase:
    platform: str
    product_id: str
    identity: str  # digest of provider originalTransactionId or purchaseToken
    aliases: tuple[str, ...]
    old_keys: tuple[str, ...]
    account_token: str
    purchased_ms: int
    state: str
    receipt_digest: str


def identity_digest(kind: str, value: str) -> str:
    if not value or len(value) > 16384:
        raise HTTPException(422, 'store identity is invalid')
    return sha256_text(kind + ':' + value)


def apple_verifier(settings):
    try:
        from appstoreserverlibrary.signed_data_verifier import SignedDataVerifier
        from appstoreserverlibrary.models.Environment import Environment
        from cryptography import x509
        from cryptography.hazmat.primitives.serialization import Encoding
        root = Path(settings.apple_root_ca_file).read_bytes()
        if root.startswith(b'-----BEGIN'):
            root = x509.load_pem_x509_certificate(root).public_bytes(Encoding.DER)
        return SignedDataVerifier([root], True, Environment.PRODUCTION,
                                  settings.apple_bundle_id, int(settings.apple_app_id))
    except Exception:
        raise HTTPException(503, 'Apple migration verifier is not configured') from None


def _apple_current(settings, transaction_id):
    # SDK verifies certificate purpose, chain, app/environment and online OCSP.
    # The submitted signed receipt alone can be stale after a refund.
    try:
        from appstoreserverlibrary.models.Environment import Environment
        from appstoreserverlibrary.api_client import AppStoreServerAPIClient
        client = AppStoreServerAPIClient(Path(settings.apple_iap_key_file).read_bytes(),
            settings.apple_iap_key_id, settings.apple_iap_issuer_id,
            settings.apple_bundle_id, Environment.PRODUCTION)
        reply = client.get_transaction_info(transaction_id)
        return apple_verifier(settings).verify_and_decode_signed_transaction(reply.signedTransactionInfo)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502, 'Apple current transaction verification unavailable') from None


def _enum(value):
    return getattr(value, 'value', value)


def apple_purchase(settings, product_id, data, transaction_id):
    try:
        wrapper = json.loads(data)
        signed = wrapper.get('signedTransactionInfo', data) if isinstance(wrapper, dict) else data
    except ValueError:
        signed = data
    try:
        if signed.count('.') == 2:
            submitted = apple_verifier(settings).verify_and_decode_signed_transaction(signed)
            key = submitted.transactionId
            if submitted.productId != product_id:
                raise ValueError()
        else:
            # Legacy receipt endpoint checks the app receipt; current Server API
            # below remains mandatory, including for historical app receipts.
            receipt = AppleStoreVerifier(settings)._legacy_receipt(data)
            body = receipt.get('receipt', {})
            if body.get('bundle_id') != settings.apple_bundle_id or receipt.get('environment') != 'Production':
                raise ValueError()
            items = [r for r in body.get('in_app', []) if r.get('product_id') == product_id
                     and (not transaction_id or r.get('transaction_id') == transaction_id)]
            key = max(items, key=lambda r: int(r.get('purchase_date_ms', 0)))['transaction_id']
        if transaction_id and key != transaction_id:
            raise ValueError()
        item = _apple_current(settings, key)
        if (item.transactionId != key or item.productId != product_id
                or _enum(item.type) != 'Non-Consumable'
                or _enum(item.inAppOwnershipType) != 'PURCHASED'):
            raise ValueError()
        original = item.originalTransactionId
        canonical = identity_digest('apple', original)
        state = 'revoked' if item.revocationDate is not None else 'purchased'
        # Missing or zero-price evidence cannot establish an actual paid sale.
        if state == 'purchased' and (getattr(item, 'price', None) is None or item.price <= 0):
            raise ValueError()
        return LegacyPurchase('ios', product_id, canonical,
            tuple(sorted({canonical, identity_digest('apple', key)})), (key, original),
            str(item.appAccountToken or ''), int(item.originalPurchaseDate or item.purchaseDate),
            state, sha256_text(data))
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(422, 'Apple legacy purchase evidence rejected') from None


def google_purchase(settings, product_id, data, transaction_id):
    verifier = GooglePlayVerifier(settings)
    token = data.strip()
    try:
        response = verifier._session().get(verifier._url(product_id, token), timeout=15)
        if response.status_code != 200:
            raise HTTPException(502 if response.status_code >= 500 else 422, 'Google legacy verification rejected')
        item = response.json()
        if (item.get('productId', product_id) != product_id
                or item.get('purchaseToken', token) != token
                or int(item.get('quantity', 1)) != 1):
            raise ValueError()
        order = str(item.get('orderId') or '')
        if transaction_id and transaction_id != order:
            raise ValueError()
        state = {0: 'purchased', 1: 'revoked', 2: 'pending'}.get(item.get('purchaseState'), 'unverifiable')
        if state == 'purchased' and (not order or 'purchaseType' in item
                                   or item.get('consumptionState') != 0
                                   or item.get('refundableQuantity', 1) != 1):
            raise ValueError()
        canonical = identity_digest('google-token', token)
        aliases = {canonical}
        if order:
            aliases.add(identity_digest('google-order', order))
        return LegacyPurchase('android', product_id, canonical, tuple(sorted(aliases)),
            tuple(k for k in (order, sha256_text(token)) if k),
            str(item.get('obfuscatedExternalAccountId') or ''),
            int(item['purchaseTimeMillis']), state, sha256_text(data))
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(422, 'Google legacy purchase evidence rejected') from None


def verify_legacy(settings, *, platform, product_id, verification_data, transaction_id=None):
    rule_for(settings, platform, product_id)
    if not verification_data or len(verification_data) > 2*1024*1024:
        raise HTTPException(422, 'legacy verification data is invalid')
    # Fixtures are impossible outside an explicitly isolated test environment.
    if settings.store_verifier_mode == 'fake':
        if settings.app_env != 'test':
            raise HTTPException(503, 'fake legacy verifier is forbidden')
        try:
            p = json.loads(verification_data)
            if not p.get('valid') or p['productId'] != product_id or p['platform'] != platform:
                raise ValueError()
            key = p['originalTransactionId'] if platform == 'ios' else p['purchaseToken']
            ident = identity_digest('apple' if platform == 'ios' else 'google-token', key)
            alias = identity_digest('apple' if platform == 'ios' else 'google-order', p['transactionId'])
            return LegacyPurchase(platform, product_id, ident, tuple(sorted({ident, alias})),
                (key, p['transactionId']), p.get('accountToken', ''), p['purchasedMs'], p['state'], sha256_text(verification_data))
        except Exception:
            raise HTTPException(422, 'legacy fixture rejected') from None
    if settings.store_verifier_mode != 'production':
        raise HTTPException(503, 'legacy verifier mode is invalid')
    return (apple_purchase if platform == 'ios' else google_purchase)(settings, product_id, verification_data, transaction_id)


def acknowledge_legacy(settings, product_id, verification_data):
    if settings.app_env == 'test' and settings.store_verifier_mode == 'fake':
        return
    verifier = GooglePlayVerifier(settings)
    try:
        session = verifier._session()
        url = verifier._url(product_id, verification_data.strip())
        current = session.get(url, timeout=15)
        if current.status_code != 200 or current.json().get('purchaseState') != 0:
            raise ValueError()
        if current.json().get('acknowledgementState') == 1:
            return
        reply = session.post(url + ':acknowledge', json={}, timeout=15)
        if reply.status_code not in (200, 204):
            raise ValueError()
    except Exception:
        raise HTTPException(503, 'legacy acknowledgement must be retried') from None
