from __future__ import annotations

import base64
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from .config import PlatformSettings
from .repository import (
    assert_schema,
    connect,
    grant_paid_credits,
    new_id,
    profile_payload,
    store_account_token,
)
from .security import constant_time_equal, sha256_text


CATALOG_VERSION = 2
PRODUCTS: dict[str, dict[str, Any]] = {
    "buildingland.report_credits_10": {"credits": 10, "enabled": True, "version": 2, "pricing": "current"},
    "buildingland.report_credits_30": {"credits": 30, "enabled": True, "version": 2, "pricing": "current"},
    "buildingland.report_credits_50": {"credits": 50, "enabled": True, "version": 2, "pricing": "current"},
    "buildingland.report_credits_60": {"credits": 60, "enabled": False, "version": 1, "pricing": "retired"},
    "buildingland.report_credits_90": {"credits": 90, "enabled": False, "version": 1, "pricing": "retired"},
}
LEGACY_PRODUCT = "remove_ads_monthly"


def catalog(platform: str) -> list[dict[str, Any]]:
    return [
        {
            "platform": platform,
            "productId": product_id,
            "kind": "consumable",
            "credits": int(config["credits"]),
            "enabled": bool(config["enabled"]),
            "version": int(config["version"]),
        }
        for product_id, config in PRODUCTS.items()
    ]


@dataclass(frozen=True)
class VerifiedPurchase:
    platform: str
    transaction_key: str
    product_id: str
    account_token: str
    environment: str
    requires_post_commit: bool = False


class StoreVerifier(Protocol):
    def verify(
        self,
        *,
        product_id: str,
        verification_data: str,
        transaction_id: str | None,
        account_token: str,
    ) -> VerifiedPurchase: ...

    def post_commit(self, *, product_id: str, verification_data: str) -> None: ...


class FakeStoreVerifier:
    def __init__(self, platform: str) -> None:
        self.platform = platform

    def verify(
        self,
        *,
        product_id: str,
        verification_data: str,
        transaction_id: str | None,
        account_token: str,
    ) -> VerifiedPurchase:
        try:
            payload = json.loads(verification_data)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="test receipt is invalid") from exc
        if not isinstance(payload, dict) or payload.get("valid") is not True:
            raise HTTPException(status_code=422, detail="test receipt was rejected")
        if payload.get("platform") != self.platform or payload.get("productId") != product_id:
            raise HTTPException(status_code=422, detail="receipt product or platform mismatch")
        received_account = str(payload.get("accountToken") or "")
        if not constant_time_equal(received_account, account_token):
            raise HTTPException(status_code=422, detail="receipt account mismatch")
        key = str(payload.get("transactionId") or transaction_id or "")
        if not key:
            raise HTTPException(status_code=422, detail="receipt transaction is missing")
        return VerifiedPurchase(
            platform=self.platform,
            transaction_key=key,
            product_id=product_id,
            account_token=received_account,
            environment="test",
            requires_post_commit=self.platform == "android",
        )

    def post_commit(self, *, product_id: str, verification_data: str) -> None:
        payload = json.loads(verification_data)
        if payload.get("postCommitFails") is True:
            raise HTTPException(status_code=503, detail="store acknowledgement is temporarily unavailable")


def _json_request(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        raise HTTPException(status_code=502, detail="store verification service is unavailable") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="store verification response is invalid")
    return payload


class AppleStoreVerifier:
    def __init__(self, settings: PlatformSettings) -> None:
        self.settings = settings
        if not settings.apple_bundle_id:
            raise HTTPException(status_code=503, detail="Apple store verification is not configured")

    def _legacy_receipt(self, verification_data: str) -> dict[str, Any]:
        body: dict[str, Any] = {"receipt-data": verification_data, "exclude-old-transactions": False}
        if self.settings.apple_shared_secret:
            body["password"] = self.settings.apple_shared_secret
        encoded = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            "https://buy.itunes.apple.com/verifyReceipt",
            data=encoded,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        payload = _json_request(request)
        if payload.get("status") == 21007:
            request = urllib.request.Request(
                "https://sandbox.itunes.apple.com/verifyReceipt",
                data=encoded,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                method="POST",
            )
            payload = _json_request(request)
        if payload.get("status") != 0:
            raise HTTPException(status_code=422, detail="Apple receipt was rejected")
        return payload

    def _verified_jws_payload(self, value: str) -> dict[str, Any]:
        trust_path = Path(self.settings.apple_root_ca_file)
        if not self.settings.apple_root_ca_file or not trust_path.is_file() or trust_path.stat().st_size == 0:
            raise HTTPException(status_code=503, detail="Apple JWS trust root is not configured")
        try:
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
            from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

            header_part, payload_part, signature_part = value.split(".")
            decode = lambda part: base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))
            header = json.loads(decode(header_part))
            payload = json.loads(decode(payload_part))
            if header.get("alg") != "ES256":
                raise ValueError("unexpected JWS algorithm")
            chain = [x509.load_der_x509_certificate(base64.b64decode(item)) for item in header.get("x5c", [])]
            if len(chain) < 2:
                raise ValueError("certificate chain missing")
            trusted = x509.load_pem_x509_certificate(trust_path.read_bytes())
            now = datetime.now(timezone.utc)
            for certificate in chain:
                if certificate.not_valid_before_utc > now or certificate.not_valid_after_utc < now:
                    raise ValueError("certificate expired")
            for child, issuer in zip(chain, chain[1:]):
                key = issuer.public_key()
                if isinstance(key, rsa.RSAPublicKey):
                    key.verify(child.signature, child.tbs_certificate_bytes, padding.PKCS1v15(), child.signature_hash_algorithm)
                else:
                    key.verify(child.signature, child.tbs_certificate_bytes, ec.ECDSA(child.signature_hash_algorithm))
            last = chain[-1]
            if last.fingerprint(hashes.SHA256()) != trusted.fingerprint(hashes.SHA256()):
                trust_key = trusted.public_key()
                if isinstance(trust_key, rsa.RSAPublicKey):
                    trust_key.verify(last.signature, last.tbs_certificate_bytes, padding.PKCS1v15(), last.signature_hash_algorithm)
                else:
                    trust_key.verify(last.signature, last.tbs_certificate_bytes, ec.ECDSA(last.signature_hash_algorithm))
            raw_signature = decode(signature_part)
            if len(raw_signature) != 64:
                raise ValueError("unexpected ES256 signature length")
            signature = encode_dss_signature(
                int.from_bytes(raw_signature[:32], "big"), int.from_bytes(raw_signature[32:], "big")
            )
            leaf_key = chain[0].public_key()
            leaf_key.verify(signature, f"{header_part}.{payload_part}".encode("ascii"), ec.ECDSA(hashes.SHA256()))
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=422, detail="Apple signed transaction was rejected") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Apple signed transaction payload is invalid")
        return payload

    def verify(
        self,
        *,
        product_id: str,
        verification_data: str,
        transaction_id: str | None,
        account_token: str,
    ) -> VerifiedPurchase:
        signed_value = verification_data
        try:
            wrapper = json.loads(verification_data)
            if isinstance(wrapper, dict) and wrapper.get("signedTransactionInfo"):
                signed_value = str(wrapper["signedTransactionInfo"])
        except ValueError:
            pass
        if signed_value.count(".") == 2:
            item = self._verified_jws_payload(signed_value)
            receipt_environment = str(item.get("environment") or "Production").lower()
        else:
            receipt = self._legacy_receipt(verification_data)
            receipt_body = receipt.get("receipt") if isinstance(receipt.get("receipt"), dict) else {}
            if receipt_body.get("bundle_id") != self.settings.apple_bundle_id:
                raise HTTPException(status_code=422, detail="Apple receipt application mismatch")
            candidates = [
                row for row in receipt_body.get("in_app", [])
                if isinstance(row, dict) and row.get("product_id") == product_id
            ]
            if transaction_id:
                candidates = [row for row in candidates if row.get("transaction_id") == transaction_id]
            if not candidates:
                raise HTTPException(status_code=422, detail="Apple receipt product was not found")
            item = max(candidates, key=lambda row: int(row.get("purchase_date_ms") or 0))
            receipt_environment = str(receipt.get("environment") or "Production").lower()

        if str(item.get("bundleId") or item.get("bundle_id") or self.settings.apple_bundle_id) != self.settings.apple_bundle_id:
            raise HTTPException(status_code=422, detail="Apple receipt application mismatch")
        if str(item.get("productId") or item.get("product_id") or "") != product_id:
            raise HTTPException(status_code=422, detail="Apple receipt product mismatch")
        if item.get("revocationDate") or item.get("cancellation_date"):
            raise HTTPException(status_code=422, detail="Apple transaction was revoked")
        received_account = str(item.get("appAccountToken") or item.get("app_account_token") or "")
        if received_account and not constant_time_equal(received_account.lower(), account_token.lower()):
            raise HTTPException(status_code=422, detail="Apple receipt account mismatch")
        if not received_account and product_id != LEGACY_PRODUCT:
            raise HTTPException(status_code=422, detail="Apple receipt account binding is missing")
        key = str(item.get("transactionId") or item.get("transaction_id") or transaction_id or "")
        if not key:
            raise HTTPException(status_code=422, detail="Apple receipt transaction is missing")
        if transaction_id and not constant_time_equal(key, transaction_id):
            raise HTTPException(status_code=422, detail="Apple receipt transaction mismatch")
        return VerifiedPurchase("ios", key, product_id, received_account, receipt_environment)

    def post_commit(self, *, product_id: str, verification_data: str) -> None:
        return None


class GooglePlayVerifier:
    def __init__(self, settings: PlatformSettings) -> None:
        self.settings = settings
        if not settings.google_play_package_name or not settings.google_play_service_account_file:
            raise HTTPException(status_code=503, detail="Google Play verification is not configured")
        if not Path(settings.google_play_service_account_file).is_file():
            raise HTTPException(status_code=503, detail="Google Play credential file is unavailable")

    def _session(self):
        try:
            from google.oauth2 import service_account
            from google.auth.transport.requests import AuthorizedSession

            credentials = service_account.Credentials.from_service_account_file(
                self.settings.google_play_service_account_file,
                scopes=["https://www.googleapis.com/auth/androidpublisher"],
            )
            return AuthorizedSession(credentials)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Google Play verifier could not be initialized") from exc

    def _url(self, product_id: str, token: str) -> str:
        return (
            "https://androidpublisher.googleapis.com/androidpublisher/v3/applications/"
            f"{urllib.parse.quote(self.settings.google_play_package_name, safe='')}/purchases/products/"
            f"{urllib.parse.quote(product_id, safe='')}/tokens/{urllib.parse.quote(token, safe='')}"
        )

    def verify(
        self,
        *,
        product_id: str,
        verification_data: str,
        transaction_id: str | None,
        account_token: str,
    ) -> VerifiedPurchase:
        token = verification_data.strip()
        if not token:
            raise HTTPException(status_code=422, detail="Google Play purchase token is missing")
        try:
            response = self._session().get(self._url(product_id, token), timeout=15)
            payload = response.json()
        except Exception as exc:
            raise HTTPException(status_code=502, detail="Google Play verification service is unavailable") from exc
        if response.status_code != 200 or not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail="Google Play purchase was rejected")
        if int(payload.get("purchaseState", -1)) != 0:
            raise HTTPException(status_code=422, detail="Google Play purchase is not completed")
        received_account = str(
            payload.get("obfuscatedExternalAccountId")
            or (payload.get("externalAccountIdentifiers") or {}).get("obfuscatedExternalAccountId")
            or ""
        )
        if received_account and not constant_time_equal(received_account.lower(), account_token.lower()):
            raise HTTPException(status_code=422, detail="Google Play purchase account mismatch")
        if not received_account and product_id != LEGACY_PRODUCT:
            raise HTTPException(status_code=422, detail="Google Play purchase account binding is missing")
        key = str(payload.get("orderId") or transaction_id or sha256_text(token))
        return VerifiedPurchase("android", key, product_id, received_account, "production", True)

    def post_commit(self, *, product_id: str, verification_data: str) -> None:
        action = "acknowledge" if product_id == LEGACY_PRODUCT else "consume"
        try:
            response = self._session().post(
                f"{self._url(product_id, verification_data.strip())}:{action}",
                json={},
                timeout=15,
            )
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Google Play post-verification action is temporarily unavailable") from exc
        if response.status_code not in {200, 204, 409}:
            raise HTTPException(status_code=503, detail="Google Play post-verification action is temporarily unavailable")


def verifier_for(settings: PlatformSettings, platform: str) -> StoreVerifier:
    if settings.store_verifier_mode == "fake":
        if settings.app_env != "test":
            raise HTTPException(status_code=503, detail="fake store verification is disabled outside tests")
        return FakeStoreVerifier(platform)
    if settings.store_verifier_mode != "production":
        raise HTTPException(status_code=503, detail="store verifier mode is invalid")
    return AppleStoreVerifier(settings) if platform == "ios" else GooglePlayVerifier(settings)


def process_purchase(
    settings: PlatformSettings,
    *,
    user_id: str,
    platform: str,
    product_id: str,
    verification_data: str,
    transaction_id: str | None,
    restored: bool,
) -> dict[str, Any]:
    if platform not in {"ios", "android"}:
        raise HTTPException(status_code=422, detail="platform must be ios or android")
    if not verification_data or len(verification_data) > 2 * 1024 * 1024:
        raise HTTPException(status_code=422, detail="verificationData is missing or too large")
    if restored and product_id != LEGACY_PRODUCT:
        raise HTTPException(status_code=422, detail="consumable credit products cannot be restored")
    if not restored and product_id not in PRODUCTS:
        raise HTTPException(status_code=422, detail="unknown store product")
    if not restored and not bool(PRODUCTS[product_id]["enabled"]):
        raise HTTPException(status_code=410, detail="store product is retired")
    digest = sha256_text(verification_data)
    verifier = verifier_for(settings, platform)

    already_processed = False
    with connect(settings) as connection:
        assert_schema(connection)
        existing = connection.execute(
            "SELECT * FROM mobile_store_transactions WHERE platform = %s AND verification_digest = %s",
            (platform, digest),
        ).fetchone()
        if existing is not None:
            if str(existing["user_id"]) != user_id or existing["product_id"] != product_id:
                raise HTTPException(status_code=409, detail="store transaction belongs to another account or product")
            result = dict(existing)
            already_processed = True
        else:
            result = None

    if result is None:
        with connect(settings) as connection:
            expected_account_token = store_account_token(connection, user_id, platform)
        verified = verifier.verify(
            product_id=product_id,
            verification_data=verification_data,
            transaction_id=transaction_id,
            account_token=expected_account_token,
        )
        if verified.platform != platform or verified.product_id != product_id:
            raise HTTPException(status_code=422, detail="verified store transaction does not match request")
        config = PRODUCTS.get(product_id)
        credits = 0 if restored else int(config["credits"] if config else 0)
        pricing_policy = "legacy" if restored else str(config["pricing"])
        store_transaction_id = new_id()
        with connect(settings) as connection:
            assert_schema(connection)
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"store:{platform}:{verified.transaction_key}",),
            )
            collision = connection.execute(
                """
                SELECT * FROM mobile_store_transactions
                WHERE platform = %s AND (transaction_key = %s OR verification_digest = %s)
                FOR UPDATE
                """,
                (platform, verified.transaction_key, digest),
            ).fetchone()
            if collision is not None:
                if str(collision["user_id"]) != user_id or collision["product_id"] != product_id:
                    raise HTTPException(status_code=409, detail="store transaction belongs to another account or product")
                result = dict(collision)
                already_processed = True
            else:
                post_status = "pending" if verified.requires_post_commit else "not_required"
                connection.execute(
                    """
                    INSERT INTO mobile_store_transactions
                      (id, platform, transaction_key, verification_digest, user_id, product_id,
                       store_environment, status, pricing_policy, credits_granted, post_commit_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        store_transaction_id, platform, verified.transaction_key, digest, user_id, product_id,
                        verified.environment, "entitled" if restored else "granted", pricing_policy, credits, post_status,
                    ),
                )
                if credits:
                    grant_paid_credits(
                        connection,
                        user_id=user_id,
                        credits=credits,
                        reason="store_purchase",
                        idempotency_key=f"store:{platform}:{verified.transaction_key}",
                        reference_type="store_transaction",
                        reference_id=store_transaction_id,
                        metadata={"platform": platform, "productId": product_id, "catalogVersion": int(config["version"])},
                    )
                if restored:
                    connection.execute(
                        """
                        INSERT INTO platform_entitlements
                          (id, user_id, store, product_id, status, pricing_policy, source_transaction_id)
                        VALUES (%s, %s, %s, %s, 'active', 'legacy', %s)
                        ON CONFLICT (user_id, store, product_id) DO UPDATE
                        SET status = 'active', source_transaction_id = EXCLUDED.source_transaction_id, updated_at = NOW()
                        """,
                        (new_id(), user_id, platform, product_id, store_transaction_id),
                    )
                connection.execute(
                    """
                    INSERT INTO mobile_store_events (id, transaction_id, user_id, event_type, detail)
                    VALUES (%s, %s, %s, 'verified_and_committed', %s)
                    """,
                    (new_id(), store_transaction_id, user_id, Jsonb({"productId": product_id, "credits": credits})),
                )
                result = {
                    "id": store_transaction_id,
                    "product_id": product_id,
                    "pricing_policy": pricing_policy,
                    "credits_granted": credits,
                    "post_commit_status": post_status,
                }

    if platform == "android" and result["post_commit_status"] != "completed":
        try:
            verifier.post_commit(product_id=product_id, verification_data=verification_data)
        except HTTPException:
            with connect(settings) as connection:
                connection.execute(
                    """
                    UPDATE mobile_store_transactions SET post_commit_status = 'failed', updated_at = NOW()
                    WHERE id = %s
                    """,
                    (result["id"],),
                )
            raise
        with connect(settings) as connection:
            connection.execute(
                """
                UPDATE mobile_store_transactions SET post_commit_status = 'completed', updated_at = NOW()
                WHERE id = %s
                """,
                (result["id"],),
            )

    with connect(settings) as connection:
        summary = profile_payload(connection, user_id)["creditSummary"]
    return {
        "transactionId": str(result["id"]),
        "productId": product_id,
        "status": "active" if restored else "verified",
        "pricingPolicy": result["pricing_policy"],
        "creditsGranted": int(result["credits_granted"]),
        "catalogVersion": 1 if result["pricing_policy"] in {"retired", "legacy"} else CATALOG_VERSION,
        "alreadyProcessed": already_processed,
        "creditSummary": summary,
    }
