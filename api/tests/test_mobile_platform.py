from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import unittest
import uuid
import re
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import psycopg
from fastapi import HTTPException
from fastapi.testclient import TestClient


os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("STORE_VERIFIER_MODE", "fake")
os.environ.setdefault("KAKAO_OAUTH_CLIENT_ID", "test-client")
os.environ.setdefault("KAKAO_OAUTH_CLIENT_SECRET", "test-secret")
os.environ.setdefault("NAVER_OAUTH_CLIENT_ID", "test-client")
os.environ.setdefault("NAVER_OAUTH_CLIENT_SECRET", "test-secret")
os.environ.setdefault("GOOGLE_OAUTH_CLIENT_ID", "test-client")
os.environ.setdefault("GOOGLE_OAUTH_CLIENT_SECRET", "test-secret")
os.environ.setdefault("PLATFORM_INTERNAL_SERVICE_TOKEN", "test-internal-token-with-sufficient-length")
os.environ.setdefault("MOBILE_OAUTH_CALLBACK_BASE_URL", "https://building-land.test")

from app.main import app
from app.platform.config import get_settings
from app.platform.oauth import ProviderIdentity
from app.platform.reports import begin_final_usage, render_pdf, validate_canonical_report
from app.platform.repository import new_id
from app.platform.security import sha256_text
from app.platform.store import FakeStoreVerifier, verifier_for


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


class MobilePlatformContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        root = Path(cls.temp_dir.name)
        cls.assets = root / "assets"
        cls.environment = root / "environment"
        cls._write_environment_fixture(cls.environment)
        os.environ["REPORT_ASSET_DIR"] = str(cls.assets)
        os.environ["ENVIRONMENT_DATA_DIR"] = str(cls.environment)
        get_settings.cache_clear()
        cls.database_url = os.environ["DATABASE_URL"]
        cls.client = TestClient(app, follow_redirects=False)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        cls.temp_dir.cleanup()

    def setUp(self) -> None:
        with psycopg.connect(self.database_url) as connection:
            connection.execute("TRUNCATE platform_users, mobile_oauth_flows CASCADE")

    @staticmethod
    def _write_environment_fixture(root: Path) -> None:
        (root / "bus-stops").mkdir(parents=True)
        (root / "amenities").mkdir()
        (root / "parks").mkdir()
        common = "id,name,type,address,lat,lng,dataDate\n1,시설,일반,서울,37.5001,127.0001,2026-01-01\n"
        (root / "rail-stations.csv").write_text(common, encoding="utf-8")
        (root / "subway-stations.csv").write_text(common, encoding="utf-8")
        (root / "bus-stops" / "seoul.csv").write_text(common, encoding="utf-8")
        (root / "amenities" / "seoul.csv").write_text(
            "id,name,type,address,lat,lng,category\n1,약국,약국,서울,37.5001,127.0001,약국\n",
            encoding="utf-8",
        )
        (root / "parks" / "seoul.csv").write_text(common, encoding="utf-8")
        (root / "schools.csv").write_text(
            "학교ID,학교명,학교급구분,위도,경도,데이터기준일자\n1,테스트초,초등학교,37.5001,127.0001,2026-01-01\n",
            encoding="utf-8",
        )
        compact = {"source": "fixture", "items": [[37.5001, 127.0001, 2, 0]], "purposes": ["생활방범"]}
        (root / "cctv.json").write_text(json.dumps(compact), encoding="utf-8")
        (root / "security-lights.json").write_text(json.dumps(compact), encoding="utf-8")

    def _create_user(self, *, free: int = 3, paid: int = 0) -> tuple[str, str, dict[str, str]]:
        user_id = str(uuid.uuid4())
        family_id = str(uuid.uuid4())
        access = f"ma_{uuid.uuid4().hex}{uuid.uuid4().hex}"
        with psycopg.connect(self.database_url) as connection:
            connection.execute(
                """
                INSERT INTO platform_users (id, email, display_name, free_remaining, paid_remaining)
                VALUES (%s, %s, 'Tester', %s, %s)
                """,
                (user_id, f"{user_id}@example.test", free, paid),
            )
            connection.execute(
                """
                INSERT INTO mobile_access_tokens
                  (token_hash, family_id, user_id, device_id, expires_at)
                VALUES (%s, %s, %s, 'device-contract-test-0001', NOW() + INTERVAL '1 hour')
                """,
                (sha256_text(access), family_id, user_id),
            )
        return user_id, access, {"Authorization": f"Bearer {access}"}

    @staticmethod
    def _report(title: str = "Contract report", *, with_image: bool = False) -> dict[str, object]:
        page: dict[str, object] = {
            "pageKey": "cover",
            "layout": "cover",
            "title": title,
            "footerTitle": title,
            "address": "Seoul",
        }
        if with_image:
            page["mapImage"] = f"data:image/png;base64,{base64.b64encode(PNG_1X1).decode('ascii')}"
        return {
            "schemaVersion": 1,
            "rendererVersion": "web-a4-canonical-v1",
            "mappingVersion": "mobile-v1",
            "reportId": "report-contract-1",
            "title": title,
            "address": "Seoul",
            "includedItems": ["cover"],
            "officeInfo": {},
            "reportTheme": "navy",
            "pages": [page],
        }

    def _credit_summary(self, headers: dict[str, str]) -> dict[str, int]:
        response = self.client.get("/api/mobile/v1/me", headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["creditSummary"]

    def _oauth_login(self, provider: str, subject: str, email: str) -> dict[str, object]:
        verifier = "p" * 64
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        start = self.client.post(
            "/api/mobile/v1/auth/oauth/start",
            json={"provider": provider, "codeChallenge": challenge, "redirectUri": "buildingland://oauth/callback"},
        )
        self.assertEqual(start.status_code, 200, start.text)
        state = start.json()["state"]
        with patch(
            "app.platform.oauth.fetch_provider_identity",
            return_value=ProviderIdentity(subject, email, "Same Email"),
        ):
            callback = self.client.get(
                f"/api/mobile/v1/auth/oauth/callback/{provider}?state={state}&code=provider-code"
            )
        self.assertEqual(callback.status_code, 302, callback.text)
        code = parse_qs(urlparse(callback.headers["location"]).query)["code"][0]
        token = self.client.post(
            "/api/mobile/v1/auth/token",
            json={"code": code, "codeVerifier": verifier, "deviceId": f"device-{provider}-{subject}"[:128]},
        )
        self.assertEqual(token.status_code, 200, token.text)
        return token.json()

    def test_oauth_pkce_code_is_one_time_and_refresh_replay_revokes_family(self) -> None:
        verifier = "v" * 64
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        start = self.client.post(
            "/api/mobile/v1/auth/oauth/start",
            json={"provider": "kakao", "codeChallenge": challenge, "redirectUri": "buildingland://oauth/callback"},
        )
        self.assertEqual(start.status_code, 200, start.text)
        state = start.json()["state"]
        with patch(
            "app.platform.oauth.fetch_provider_identity",
            return_value=ProviderIdentity("provider-subject-1", "person@example.test", "Person"),
        ):
            callback = self.client.get(
                f"/api/mobile/v1/auth/oauth/callback/kakao?state={state}&code=provider-code"
            )
        self.assertEqual(callback.status_code, 302, callback.text)
        callback_query = parse_qs(urlparse(callback.headers["location"]).query)
        code = callback_query["code"][0]
        token = self.client.post(
            "/api/mobile/v1/auth/token",
            json={"code": code, "codeVerifier": verifier, "deviceId": "device-contract-test-0001"},
        )
        self.assertEqual(token.status_code, 200, token.text)
        self.assertEqual(token.json()["creditSummary"], {"freeRemaining": 3, "paidRemaining": 0, "availableCredits": 3})
        replay = self.client.post(
            "/api/mobile/v1/auth/token",
            json={"code": code, "codeVerifier": verifier, "deviceId": "device-contract-test-0001"},
        )
        self.assertEqual(replay.status_code, 401)

        old_refresh = token.json()["refreshToken"]
        rotated = self.client.post(
            "/api/mobile/v1/auth/refresh",
            json={"refreshToken": old_refresh, "deviceId": "device-contract-test-0001"},
        )
        self.assertEqual(rotated.status_code, 200, rotated.text)
        replay_refresh = self.client.post(
            "/api/mobile/v1/auth/refresh",
            json={"refreshToken": old_refresh, "deviceId": "device-contract-test-0001"},
        )
        self.assertEqual(replay_refresh.status_code, 401)
        revoked_access = self.client.get(
            "/api/mobile/v1/me",
            headers={"Authorization": f"Bearer {rotated.json()['accessToken']}"},
        )
        self.assertEqual(revoked_access.status_code, 401)

    def test_identity_is_subject_based_free_grant_is_once_and_linking_is_explicit(self) -> None:
        first = self._oauth_login("kakao", "same-subject", "shared@example.test")
        repeated = self._oauth_login("kakao", "same-subject", "changed@example.test")
        self.assertEqual(first["user"]["id"], repeated["user"]["id"])
        self.assertEqual(repeated["creditSummary"]["freeRemaining"], 3)

        same_email_other_provider = self._oauth_login("naver", "different-subject", "shared@example.test")
        self.assertNotEqual(first["user"]["id"], same_email_other_provider["user"]["id"])

        verifier = "l" * 64
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
        unauthenticated_link = self.client.post(
            "/api/mobile/v1/auth/oauth/start",
            json={
                "provider": "google",
                "codeChallenge": challenge,
                "redirectUri": "buildingland://oauth/callback",
                "linkAccount": True,
            },
        )
        self.assertEqual(unauthenticated_link.status_code, 401)
        start = self.client.post(
            "/api/mobile/v1/auth/oauth/start",
            headers={"Authorization": f"Bearer {first['accessToken']}"},
            json={
                "provider": "google",
                "codeChallenge": challenge,
                "redirectUri": "buildingland://oauth/callback",
                "linkAccount": True,
            },
        )
        self.assertEqual(start.status_code, 200, start.text)
        state = start.json()["state"]
        with patch(
            "app.platform.oauth.fetch_provider_identity",
            return_value=ProviderIdentity("linked-google", "shared@example.test", "Linked"),
        ):
            callback = self.client.get(
                f"/api/mobile/v1/auth/oauth/callback/google?state={state}&code=provider-code"
            )
        code = parse_qs(urlparse(callback.headers["location"]).query)["code"][0]
        linked = self.client.post(
            "/api/mobile/v1/auth/token",
            json={"code": code, "codeVerifier": verifier, "deviceId": "device-explicit-link-0001"},
        )
        self.assertEqual(linked.status_code, 200, linked.text)
        self.assertEqual(first["user"]["id"], linked.json()["user"]["id"])
        with psycopg.connect(self.database_url) as connection:
            grant_count = connection.execute(
                "SELECT COUNT(*) FROM platform_credit_ledger WHERE user_id = %s AND reason = 'initial_account_grant'",
                (first["user"]["id"],),
            ).fetchone()[0]
        self.assertEqual(grant_count, 1)

    def test_preview_rejects_invalid_inline_image_content(self) -> None:
        _, _, headers = self._create_user()
        report = self._report()
        report["pages"][0]["mapImage"] = (
            "data:image/png;base64," + base64.b64encode(b"not-an-image").decode("ascii")
        )
        response = self.client.post(
            "/api/mobile/v1/reports/preview",
            headers=headers,
            json={"report": report, "rendererProfile": "web-a4-v1"},
        )
        self.assertEqual(response.status_code, 422, response.text)

    def test_internal_web_ledger_bridge_idempotency_reversal_and_report_saga(self) -> None:
        internal = {"X-Internal-Service-Token": "test-internal-token-with-sufficient-length"}
        account = self.client.post(
            "/api/internal/v1/web/accounts/resolve",
            headers=internal,
            json={"externalId": "101", "email": "same@example.test", "displayName": "Web One"},
        )
        self.assertEqual(account.status_code, 200, account.text)
        other = self.client.post(
            "/api/internal/v1/web/accounts/resolve",
            headers=internal,
            json={"externalId": "102", "email": "same@example.test", "displayName": "Web Two"},
        )
        self.assertEqual(other.status_code, 200, other.text)
        self.assertNotEqual(account.json()["userId"], other.json()["userId"])
        linked = self.client.post(
            "/api/internal/v1/web/accounts/resolve",
            headers=internal,
            json={
                "externalId": "101",
                "email": "same@example.test",
                "displayName": "Web One",
                "provider": "google",
                "providerSubject": "web-explicit-google",
            },
        )
        self.assertEqual(linked.status_code, 200, linked.text)
        self.assertEqual(linked.json()["userId"], account.json()["userId"])
        with psycopg.connect(self.database_url) as connection:
            identity_owner = connection.execute(
                "SELECT user_id FROM platform_identities WHERE provider = 'google' AND provider_subject = 'web-explicit-google'"
            ).fetchone()[0]
        self.assertEqual(str(identity_owner), account.json()["userId"])

        grant_body = {"externalId": "101", "credits": 10, "idempotencyKey": "payment-order-101"}
        grant = self.client.post("/api/internal/v1/web/credits/grant", headers=internal, json=grant_body)
        repeated = self.client.post("/api/internal/v1/web/credits/grant", headers=internal, json=grant_body)
        self.assertTrue(grant.json()["granted"])
        self.assertFalse(repeated.json()["granted"])
        self.assertEqual(repeated.json()["paidBalance"], 10)

        reversal_body = {"externalId": "101", "credits": 10, "idempotencyKey": "cancel-order-101"}
        prepared = self.client.post(
            "/api/internal/v1/web/credits/reversal/prepare", headers=internal, json=reversal_body
        )
        self.assertEqual(prepared.status_code, 200, prepared.text)
        self.assertEqual(prepared.json()["creditSummary"]["paidRemaining"], 0)
        rolled_back = self.client.post(
            "/api/internal/v1/web/credits/reversal/rollback",
            headers=internal,
            json={"externalId": "101", "idempotencyKey": "cancel-order-101"},
        )
        self.assertEqual(rolled_back.json()["creditSummary"]["paidRemaining"], 10)
        prepared_again = self.client.post(
            "/api/internal/v1/web/credits/reversal/prepare", headers=internal, json=reversal_body
        )
        self.assertEqual(prepared_again.json()["creditSummary"]["paidRemaining"], 0)
        completed = self.client.post(
            "/api/internal/v1/web/credits/reversal/complete",
            headers=internal,
            json={"externalId": "101", "idempotencyKey": "cancel-order-101"},
        )
        self.assertEqual(completed.json()["status"], "completed")

        reservation = {"externalId": "101", "requestId": "web-report-request-0001", "contentHash": "a" * 64}
        reserved = self.client.post("/api/internal/v1/web/reports/reserve", headers=internal, json=reservation)
        self.assertEqual(reserved.status_code, 200, reserved.text)
        self.assertEqual(reserved.json()["creditSummary"]["freeRemaining"], 2)
        failed = self.client.post(
            "/api/internal/v1/web/reports/fail",
            headers=internal,
            json={"externalId": "101", "usageId": reserved.json()["usageId"]},
        )
        self.assertEqual(failed.json()["creditSummary"]["freeRemaining"], 3)
        retried = self.client.post("/api/internal/v1/web/reports/reserve", headers=internal, json=reservation)
        self.assertEqual(retried.json()["creditSummary"]["freeRemaining"], 2)
        done = self.client.post(
            "/api/internal/v1/web/reports/complete",
            headers=internal,
            json={"externalId": "101", "usageId": retried.json()["usageId"]},
        )
        self.assertEqual(done.json()["status"], "completed")
        no_second_debit = self.client.post(
            "/api/internal/v1/web/reports/reserve", headers=internal, json=reservation
        )
        self.assertEqual(no_second_debit.json()["action"], "completed")
        self.assertEqual(no_second_debit.json()["creditSummary"]["freeRemaining"], 2)
        denied = self.client.get(
            "/api/internal/v1/web/accounts/101/credits",
            headers={"X-Internal-Service-Token": "wrong"},
        )
        self.assertEqual(denied.status_code, 403)

    def test_store_catalog_and_concurrent_receipt_grant_are_exactly_once(self) -> None:
        user_id, _, headers = self._create_user()
        catalog_response = self.client.get("/api/mobile/v1/store/catalog?platform=ios", headers=headers)
        self.assertEqual(catalog_response.status_code, 200, catalog_response.text)
        catalog_payload = catalog_response.json()
        self.assertEqual(len(catalog_payload["products"]), 5)
        enabled = {item["productId"]: item["enabled"] for item in catalog_payload["products"]}
        self.assertEqual(
            enabled,
            {
                "buildingland.report_credits_10": True,
                "buildingland.report_credits_30": True,
                "buildingland.report_credits_50": True,
                "buildingland.report_credits_60": False,
                "buildingland.report_credits_90": False,
            },
        )
        receipt = json.dumps(
            {
                "valid": True,
                "platform": "ios",
                "productId": "buildingland.report_credits_10",
                "accountToken": catalog_payload["accountToken"],
                "transactionId": "ios-transaction-exactly-once",
            },
            sort_keys=True,
        )
        body = {
            "platform": "ios",
            "productId": "buildingland.report_credits_10",
            "verificationData": receipt,
            "transactionId": "ios-transaction-exactly-once",
            "restored": False,
        }
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(lambda _: self.client.post("/api/mobile/v1/store/verify", headers=headers, json=body), range(2)))
        self.assertEqual([item.status_code for item in responses], [200, 200])
        self.assertEqual({item.json()["alreadyProcessed"] for item in responses}, {False, True})
        self.assertEqual({item.json()["transactionId"] for item in responses}, {responses[0].json()["transactionId"]})
        self.assertEqual(responses[0].json()["creditSummary"]["paidRemaining"], 10)
        self.assertEqual(self._credit_summary(headers)["paidRemaining"], 10)
        with psycopg.connect(self.database_url) as connection:
            grants = connection.execute(
                "SELECT COUNT(*) FROM platform_credit_ledger WHERE user_id = %s AND reason = 'store_purchase'",
                (user_id,),
            ).fetchone()[0]
        self.assertEqual(grants, 1)

        retired = self.client.post(
            "/api/mobile/v1/store/verify",
            headers=headers,
            json={
                "platform": "ios",
                "productId": "buildingland.report_credits_60",
                "verificationData": receipt,
                "transactionId": "retired-product",
                "restored": False,
            },
        )
        self.assertEqual(retired.status_code, 410)

        _, _, other_headers = self._create_user()
        cross_account = self.client.post("/api/mobile/v1/store/verify", headers=other_headers, json=body)
        self.assertEqual(cross_account.status_code, 409)

    def test_store_verification_modes_fail_closed(self) -> None:
        settings = get_settings()
        with self.assertRaises(HTTPException) as fake_outside_test:
            verifier_for(replace(settings, app_env="production", store_verifier_mode="fake"), "ios")
        self.assertEqual(fake_outside_test.exception.status_code, 503)
        with self.assertRaises(HTTPException) as missing_apple:
            verifier_for(
                replace(settings, app_env="production", store_verifier_mode="production", apple_bundle_id=""),
                "ios",
            )
        self.assertEqual(missing_apple.exception.status_code, 503)
        with self.assertRaises(HTTPException) as missing_google:
            verifier_for(
                replace(
                    settings,
                    app_env="production",
                    store_verifier_mode="production",
                    google_play_package_name="",
                    google_play_service_account_file="",
                ),
                "android",
            )
        self.assertEqual(missing_google.exception.status_code, 503)

    def test_android_post_commit_failure_redelivers_without_second_grant(self) -> None:
        _, _, headers = self._create_user()
        catalog_response = self.client.get("/api/mobile/v1/store/catalog?platform=android", headers=headers)
        token = catalog_response.json()["accountToken"]
        receipt = json.dumps(
            {
                "valid": True,
                "platform": "android",
                "productId": "buildingland.report_credits_30",
                "accountToken": token,
                "transactionId": "android-redelivery-1",
            },
            sort_keys=True,
        )
        body = {
            "platform": "android",
            "productId": "buildingland.report_credits_30",
            "verificationData": receipt,
            "transactionId": "android-redelivery-1",
            "restored": False,
        }
        with patch.object(
            FakeStoreVerifier,
            "post_commit",
            side_effect=[HTTPException(status_code=503, detail="temporary"), None],
        ):
            first = self.client.post("/api/mobile/v1/store/verify", headers=headers, json=body)
            second = self.client.post("/api/mobile/v1/store/verify", headers=headers, json=body)
        self.assertEqual(first.status_code, 503)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(self._credit_summary(headers)["paidRemaining"], 30)

    def test_legacy_store_restore_creates_entitlement_without_credit_grant(self) -> None:
        _, _, headers = self._create_user()
        account = self.client.get(
            "/api/mobile/v1/store/catalog?platform=ios", headers=headers
        ).json()["accountToken"]
        verification = json.dumps(
            {
                "valid": True,
                "platform": "ios",
                "productId": "remove_ads_monthly",
                "accountToken": account,
                "transactionId": "legacy-entitlement-restore-1",
            },
            sort_keys=True,
        )
        body = {
            "platform": "ios",
            "productId": "remove_ads_monthly",
            "verificationData": verification,
            "transactionId": "legacy-entitlement-restore-1",
            "restored": True,
        }
        restored = self.client.post("/api/mobile/v1/store/restore", headers=headers, json=body)
        repeated = self.client.post("/api/mobile/v1/store/restore", headers=headers, json=body)
        self.assertEqual(restored.status_code, 200, restored.text)
        self.assertEqual(restored.json()["creditsGranted"], 0)
        self.assertEqual(restored.json()["pricingPolicy"], "legacy")
        self.assertFalse(restored.json()["alreadyProcessed"])
        self.assertTrue(repeated.json()["alreadyProcessed"])
        profile = self.client.get("/api/mobile/v1/me", headers=headers).json()
        self.assertEqual(profile["creditSummary"], {"freeRemaining": 3, "paidRemaining": 0, "availableCredits": 3})
        self.assertEqual(
            profile["storeEntitlements"],
            [{"store": "ios", "productId": "remove_ads_monthly", "status": "active", "pricingPolicy": "legacy"}],
        )

    def test_preview_final_archive_idempotency_ownership_and_regeneration(self) -> None:
        _, _, headers = self._create_user()
        report = self._report(with_image=True)
        preview = self.client.post("/api/mobile/v1/reports/preview", headers=headers, json={"report": report})
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertTrue(preview.content.startswith(b"%PDF-"))
        self.assertEqual(self._credit_summary(headers)["freeRemaining"], 3)

        request_id = "request-contract-final-0001"
        final = self.client.post(
            "/api/mobile/v1/reports/final",
            headers=headers,
            json={"requestId": request_id, "report": report},
        )
        self.assertEqual(final.status_code, 200, final.text)
        self.assertEqual(final.headers["x-report-renderer-profile"], "web-a4-v1")
        self.assertEqual(final.headers["x-report-renderer-version"], "web-a4-canonical-v1")
        self.assertEqual(hashlib.sha256(final.content).hexdigest(), final.headers["x-report-artifact-sha256"])
        self.assertEqual(self._credit_summary(headers)["freeRemaining"], 2)
        archive_id = final.headers["x-report-archive-id"]

        retry = self.client.post(
            "/api/mobile/v1/reports/final", headers=headers, json={"requestId": request_id, "report": report}
        )
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertEqual(retry.headers["x-report-archive-id"], archive_id)
        self.assertEqual(self._credit_summary(headers)["freeRemaining"], 2)
        conflict = self.client.post(
            "/api/mobile/v1/reports/final",
            headers=headers,
            json={"requestId": request_id, "report": self._report("Different")},
        )
        self.assertEqual(conflict.status_code, 409)

        listing = self.client.get("/api/report-archive", headers=headers)
        self.assertEqual(listing.status_code, 200)
        self.assertEqual(listing.json()["items"][0]["contentFormats"], ["pdf", "html"])
        regenerated = self.client.get(f"/api/report-archive/content?id={archive_id}&format=pdf", headers=headers)
        self.assertEqual(regenerated.status_code, 200, regenerated.text)
        self.assertTrue(regenerated.content.startswith(b"%PDF-"))
        with psycopg.connect(self.database_url) as connection:
            archive = connection.execute(
                "SELECT canonical_report FROM platform_report_archives WHERE id = %s", (archive_id,)
            ).fetchone()[0]
        self.assertNotIn("base64", json.dumps(archive))

        _, _, other_headers = self._create_user()
        denied = self.client.get(f"/api/report-archive/content?id={archive_id}&format=pdf", headers=other_headers)
        self.assertEqual(denied.status_code, 404)

    def test_final_uses_free_then_paid_and_renderer_failure_refunds(self) -> None:
        _, _, headers = self._create_user(free=1, paid=1)
        first = self.client.post(
            "/api/mobile/v1/reports/final",
            headers=headers,
            json={"requestId": "request-free-first-0001", "report": self._report()},
        )
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(self._credit_summary(headers), {"freeRemaining": 0, "paidRemaining": 1, "availableCredits": 1})
        with patch("app.platform.routes.render_pdf", side_effect=HTTPException(status_code=500, detail="render failed")):
            failed = self.client.post(
                "/api/mobile/v1/reports/final",
                headers=headers,
                json={"requestId": "request-refund-paid-0002", "report": self._report("Failure")},
            )
        self.assertEqual(failed.status_code, 500)
        self.assertEqual(self._credit_summary(headers), {"freeRemaining": 0, "paidRemaining": 1, "availableCredits": 1})

    def test_stale_final_reservation_is_refunded_then_retried_once(self) -> None:
        user_id, _, headers = self._create_user(free=1)
        report = self._report("Stale recovery")
        canonical = validate_canonical_report(report)
        operation = begin_final_usage(
            get_settings(),
            user_id=user_id,
            request_id="request-stale-final-0001",
            canonical=canonical,
        )
        self.assertEqual(operation["action"], "render")
        with psycopg.connect(self.database_url) as connection:
            connection.execute(
                "UPDATE platform_report_usages SET reserved_at = NOW() - INTERVAL '16 minutes' WHERE id = %s",
                (operation["usage"]["id"],),
            )
        recovered = self.client.post(
            "/api/mobile/v1/reports/final",
            headers=headers,
            json={"requestId": "request-stale-final-0001", "report": report},
        )
        self.assertEqual(recovered.status_code, 200, recovered.text)
        self.assertEqual(self._credit_summary(headers)["freeRemaining"], 0)
        with psycopg.connect(self.database_url) as connection:
            ledger = connection.execute(
                "SELECT reason, delta FROM platform_credit_ledger WHERE user_id = %s ORDER BY created_at",
                (user_id,),
            ).fetchall()
        self.assertEqual([row[0] for row in ledger].count("report_final"), 2)
        self.assertEqual([row[0] for row in ledger].count("report_failure_refund"), 1)

    def test_auth_credit_validation_and_body_limit_statuses(self) -> None:
        self.assertEqual(self.client.get("/api/mobile/v1/me").status_code, 401)
        _, _, empty_headers = self._create_user(free=0, paid=0)
        no_credit = self.client.post(
            "/api/mobile/v1/reports/final",
            headers=empty_headers,
            json={"requestId": "request-no-credit-0001", "report": self._report()},
        )
        self.assertEqual(no_credit.status_code, 402)
        invalid = self._report()
        invalid["pages"][0]["layout"] = "unknown-layout"
        rejected = self.client.post(
            "/api/mobile/v1/reports/preview", headers=empty_headers, json={"report": invalid}
        )
        self.assertEqual(rejected.status_code, 422)

        previous_limit = os.environ.get("MOBILE_REPORT_MAX_BODY_BYTES")
        os.environ["MOBILE_REPORT_MAX_BODY_BYTES"] = str(1024 * 1024)
        get_settings.cache_clear()
        try:
            too_large = self.client.post(
                "/api/mobile/v1/reports/preview",
                headers=empty_headers,
                json={"report": {"padding": "x" * (1024 * 1024 + 128)}},
            )
        finally:
            if previous_limit is None:
                os.environ.pop("MOBILE_REPORT_MAX_BODY_BYTES", None)
            else:
                os.environ["MOBILE_REPORT_MAX_BODY_BYTES"] = previous_limit
            get_settings.cache_clear()
        self.assertEqual(too_large.status_code, 413)

    def test_environment_contract_uses_authoritative_fixture_and_partial_errors(self) -> None:
        body = {
            "location": {"lat": 37.5, "lng": 127.0, "crs": "EPSG:4326"},
            "address": {"parcel": "서울특별시 테스트동 1", "road": "서울특별시 테스트로 1"},
            "parcelId": "1111010100100010000",
            "radiusProfile": "web-v1",
            "calculationVersion": "environment-web-v1",
        }
        response = self.client.post("/api/v1/environment-analysis", json=body)
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        self.assertFalse(result["partial"])
        self.assertEqual(
            [row["label"] for row in result["reportRows"]],
            ["대상 위치", "도로명주소", "교통", "생활편의", "공원", "학교", "보안등", "CCTV"],
        )
        (self.environment / "parks" / "seoul.csv").unlink()
        partial = self.client.post("/api/v1/environment-analysis", json=body)
        self.assertEqual(partial.status_code, 200, partial.text)
        self.assertTrue(partial.json()["partial"])
        self.assertEqual(partial.json()["categories"]["park"]["status"], "error")

    def test_canonical_schema_and_profile_golden_fixtures(self) -> None:
        contracts = Path(__file__).resolve().parents[1] / "contracts"
        schema = json.loads((contracts / "canonical-report-v1.schema.json").read_text(encoding="utf-8"))
        profiles = json.loads((contracts / "renderer-profiles-v1.json").read_text(encoding="utf-8"))
        self.assertEqual(schema["properties"]["schemaVersion"]["const"], 1)
        self.assertEqual(set(profiles["profiles"]), {"web-a4-v1", "ios-a4-v1", "android-a4-v1"})
        for fixture_path in sorted((contracts / "golden").glob("*.json")):
            with self.subTest(fixture=fixture_path.name):
                fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
                canonical = validate_canonical_report(
                    fixture["report"], requested_profile=fixture["rendererProfile"]
                )
                self.assertEqual(canonical.content_hash, fixture["expected"]["contentHash"])
                pdf = render_pdf(get_settings(), canonical)
                self.assertGreaterEqual(len(pdf), fixture["expected"]["minimumPdfBytes"])
                page_count = len(re.findall(rb"/Type\s*/Page\b", pdf))
                self.assertGreaterEqual(page_count, fixture["expected"]["minimumPages"])


if __name__ == "__main__":
    unittest.main()
