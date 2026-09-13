"""Google monthly subscription evidence; never persist raw purchase tokens."""
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import quote
from fastapi import HTTPException
from .security import sha256_text

PRODUCT = 'remove_ads_monthly'
ACCESS_STATES = {'SUBSCRIPTION_STATE_ACTIVE', 'SUBSCRIPTION_STATE_CANCELED', 'SUBSCRIPTION_STATE_IN_GRACE_PERIOD'}
REFUND_STATES = {'REFUNDED', 'PARTIALLY_REFUNDED'}

def now():
    return datetime.now(timezone.utc)

def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if result.tzinfo is None:
            raise ValueError()
        return result
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(422, 'invalid Google subscription timestamp') from None

def token_digest(token):
    if not isinstance(token, str) or not token or len(token) > 16384:
        raise HTTPException(422, 'invalid Google subscription token')
    return sha256_text('google-subscription:' + token)

def require_product(platform, product_id):
    if platform != 'android' or product_id != PRODUCT:
        raise HTTPException(410, 'legacy restoration is Google monthly subscription only')

def _session(settings):
    if settings.store_verifier_mode != 'production':
        raise HTTPException(503, 'Google subscription verifier requires production transport')
    try:
        from google.oauth2 import service_account
        from google.auth.transport.requests import AuthorizedSession
        credentials = service_account.Credentials.from_service_account_file(settings.google_play_service_account_file,
            scopes=['https://www.googleapis.com/auth/androidpublisher'])
        if not settings.google_play_package_name:
            raise ValueError()
        return AuthorizedSession(credentials)
    except Exception:
        raise HTTPException(503, 'Google subscription verifier is not configured') from None

def _url(settings, resource):
    return 'https://androidpublisher.googleapis.com/androidpublisher/v3/applications/' + quote(settings.google_play_package_name, safe='') + '/' + resource

def _get_json(settings, resource):
    try:
        reply = _session(settings).get(_url(settings, resource), timeout=15, allow_redirects=False)
        if reply.status_code != 200:
            raise HTTPException(503 if reply.status_code >= 500 or reply.status_code in (401, 403, 429) else 422, 'Google subscription evidence unavailable')
        result = reply.json()
        if not isinstance(result, dict):
            raise ValueError()
        return result
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(503, 'Google subscription verification unavailable') from None

def get_subscription(settings, token):
    token_digest(token)
    return _get_json(settings, 'purchases/subscriptionsv2/tokens/' + quote(token, safe=''))

def get_order(settings, order_id):
    if not isinstance(order_id, str) or not 0 < len(order_id) <= 250:
        raise HTTPException(422, 'invalid Google order identity')
    result = _get_json(settings, 'orders/' + quote(order_id, safe=''))
    if result.get('orderId') != order_id:
        raise HTTPException(422, 'Google order identity mismatch')
    return result

def order_is_void(order):
    return order.get('state') in REFUND_STATES

def validate_order(order, tokens, base_plan):
    lines = order.get('lineItems', [])
    if len(lines) != 1 or lines[0].get('productId') != PRODUCT or order.get('purchaseToken') not in tokens:
        raise HTTPException(422, 'Google payment product or token mismatch')
    line = lines[0]
    details = line.get('subscriptionDetails', {})
    if details.get('basePlanId') != base_plan:
        raise HTTPException(422, 'Google payment base plan mismatch')
    phase = details.get('offerPhaseDetails', {})
    paid_phase = ('baseDetails' in phase or 'introductoryPriceDetails' in phase
        or details.get('offerPhase') in ('BASE', 'INTRODUCTORY')
        or phase.get('prorationPeriodDetails', {}).get('originalOfferPhase') in ('BASE', 'INTRODUCTORY'))
    try:
        money = line['total']
        positive = int(money.get('units', 0)) * 1000000000 + int(money.get('nanos', 0)) > 0
        currency = bool(money['currencyCode'])
    except (KeyError, TypeError, ValueError):
        positive = currency = False
    return order.get('state') == 'PROCESSED' and paid_phase and positive and currency

@dataclass(frozen=True)
class Subscription:
    token: str
    digest: str
    linked_token: str | None
    account_token: str
    state: str
    expires_at: datetime | None
    entitled: bool
    latest_order_id: str | None
    base_plan: str
    acknowledged: bool
    anchor_only: bool = False

@dataclass(frozen=True)
class VerifiedSubscription:
    chain: tuple[Subscription, ...]
    anchor_digest: str | None
    eligible: bool
    observed_at: datetime
    order: dict | None
    @property
    def current(self):
        return self.chain[0]

def _parse(token, data, observed):
    lines = data.get('lineItems', [])
    if (data.get('kind') != 'androidpublisher#subscriptionPurchaseV2' or 'testPurchase' in data
            or len(lines) != 1 or lines[0].get('productId') != PRODUCT):
        raise HTTPException(422, 'Google subscription product evidence rejected')
    line = lines[0]
    if ('autoRenewingPlan' not in line or 'prepaidPlan' in line or 'installmentDetails' in line['autoRenewingPlan']):
        raise HTTPException(422, 'Google monthly auto-renewing subscription required')
    expiry = timestamp(line['expiryTime']) if line.get('expiryTime') else None
    state = data.get('subscriptionState', '')
    replaced = 'replacementCancellation' in data.get('canceledStateContext', {})
    linked = data.get('linkedPurchaseToken')
    expired = data.get('outOfAppPurchaseContext', {}).get('expiredPurchaseToken')
    return Subscription(token, token_digest(token), linked or expired,
        data.get('externalAccountIdentifiers', {}).get('obfuscatedExternalAccountId', ''),
        state, expiry, state in ACCESS_STATES and expiry is not None and expiry > observed and not replaced,
        line.get('latestSuccessfulOrderId'), line.get('offerDetails', {}).get('basePlanId', ''),
        data.get('acknowledgementState') == 'ACKNOWLEDGEMENT_STATE_ACKNOWLEDGED', bool(expired and not linked))

def verify_legacy(settings, *, platform, product_id, verification_data, transaction_id=None, known_anchor=None):
    require_product(platform, product_id)
    token_digest(verification_data)
    observed, token, chain, seen, anchor = now(), verification_data, [], set(), None
    for depth in range(32):
        digest = token_digest(token)
        if digest in seen:
            raise HTTPException(409, 'Google subscription lineage cycle')
        seen.add(digest)
        if depth and known_anchor and known_anchor(digest):
            anchor = digest
            break
        item = _parse(token, get_subscription(settings, token), observed)
        chain.append(item)
        if item.anchor_only:
            # Google permits this expired token only for a stored user mapping,
            # never speculative historical API lookup or first-claim binding.
            anchor = token_digest(item.linked_token)
            if anchor in seen or not known_anchor or not known_anchor(anchor):
                raise HTTPException(409, 'out-of-app subscription owner requires stored mapping')
            break
        if not item.linked_token:
            break
        token = item.linked_token
    else:
        raise HTTPException(409, 'Google subscription lineage requires review')
    current, order, eligible = chain[0], None, False
    if current.entitled:
        catalog = _get_json(settings, 'subscriptions/' + PRODUCT)
        plans = [p for p in catalog.get('basePlans', []) if p.get('basePlanId') == current.base_plan]
        if (catalog.get('productId') != PRODUCT or len(plans) != 1
                or plans[0].get('autoRenewingBasePlanType', {}).get('billingPeriodDuration') != 'P1M'):
            raise HTTPException(422, 'Google monthly billing period evidence required')
        if current.latest_order_id:
            order = get_order(settings, current.latest_order_id)
            eligible = validate_order(order, [p.token for p in chain], current.base_plan)
    return VerifiedSubscription(tuple(chain), anchor, eligible, observed, order)

def acknowledge_legacy(settings, token):
    current = _parse(token, get_subscription(settings, token), now())
    if current.acknowledged or not current.entitled:
        return
    try:
        reply = _session(settings).post(_url(settings, 'purchases/subscriptions/' + PRODUCT
            + '/tokens/' + quote(token, safe='') + ':acknowledge'), json={}, timeout=15, allow_redirects=False)
        if reply.status_code not in (200, 204):
            raise ValueError()
    except Exception:
        raise HTTPException(503, 'subscription acknowledgement must be retried') from None
