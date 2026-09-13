from __future__ import annotations
import base64
import io
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import psycopg
from fastapi import HTTPException
from fastapi.testclient import TestClient
import test_mobile_platform as base
from app.platform.config import get_settings
from app.platform.legacy_migration import process_legacy, approve_binding, reconcile
from app.platform.legacy_verifier import verify_legacy, rules, google_purchase, apple_purchase, apple_verifier
from app.platform.legacy_notifications import google_notification, apple_notification, verify_google_push
from app.platform.repository import connect
from app.main import app


class LegacyMigrationTest(unittest.TestCase):
    _create_user = base.MobilePlatformContractTest._create_user
    _credit_summary = base.MobilePlatformContractTest._credit_summary

    @classmethod
    def setUpClass(cls):
        cls.database_url = os.environ['DATABASE_URL']
        cls.temp = tempfile.TemporaryDirectory()
        cls.policy = Path(cls.temp.name) / 'policy.json'
        cls.policy.write_text(json.dumps({'version': 1, 'products': [
            {'platform': p, 'productId': 'remove_ads_monthly', 'kind': 'non_consumable',
             'purchaseBefore': '2026-08-01T00:00:00Z', 'evidenceRef': 'synthetic-test-evidence'}
            for p in ('ios', 'android')]}), encoding='utf-8')
        cls.settings = replace(get_settings(), legacy_policy_file=str(cls.policy),
            legacy_grant_enabled=True, legacy_notifications_enabled=True,
            google_play_package_name='test.package', google_rtdn_audience='https://test/push',
            google_rtdn_email='push@example.test')
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.temp.cleanup()

    def setUp(self):
        with psycopg.connect(self.database_url) as c:
            c.execute('TRUNCATE platform_users,mobile_oauth_flows CASCADE')
        self.user, _, self.headers = self._create_user(paid=7)
        self.token = self.client.get('/api/mobile/v1/store/catalog?platform=android', headers=self.headers).json()['accountToken']
        self.ios_token = self.client.get('/api/mobile/v1/store/catalog?platform=ios', headers=self.headers).json()['accountToken']

    def evidence(self, platform='android', **changes):
        p = dict(valid=True, platform=platform, productId='remove_ads_monthly', transactionId='order-one',
                 originalTransactionId='original-one', purchaseToken='sensitive-purchase-token',
                 purchasedMs=1700000000000, accountToken=self.token if platform == 'android' else self.ios_token,
                 state='purchased')
        p.update(changes)
        return dict(platform=platform, product_id=p['productId'], verification_data=json.dumps(p))

    def grant(self, *, apply=True, user=None, settings=None, **changes):
        return process_legacy(settings or self.settings, user_id=user or self.user, apply=apply, **self.evidence(**changes))

    def scalar(self, sql):
        with psycopg.connect(self.database_url) as c:
            return c.execute(sql).fetchone()[0]

    def test_disabled_default_and_empty_allowlist(self):
        self.assertFalse(replace(get_settings(), legacy_policy_file='').legacy_grant_enabled)
        self.assertEqual(rules(replace(self.settings, legacy_policy_file='')), [])
        for change in ({'legacy_grant_enabled': False}, {'legacy_notifications_enabled': False}, {'legacy_policy_file': ''}):
            with self.assertRaises(HTTPException):
                self.grant(settings=replace(self.settings, **change))
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_store_purchases'), 0)

    def test_concurrent_android_replay_one_grant_and_existing_balance_added(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            result = list(pool.map(lambda _: self.grant(), range(16)))
        self.assertEqual(sum(r['creditsGranted'] for r in result), 10)
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 17)
        self.assertEqual(self.scalar("SELECT count(*) FROM platform_credit_ledger WHERE reason='legacy_store_migration_v1'"), 1)
        self.assertEqual(self.scalar('SELECT credits_granted FROM legacy_store_purchases'), 10)

    def test_apple_original_transaction_replay_new_receipt_no_extra_grant(self):
        self.grant(platform='ios')
        result = self.grant(platform='ios', transactionId='restored-transaction')
        self.assertEqual(result['status'], 'already_granted')
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_store_aliases'), 3)
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 17)

    def test_dry_run_is_read_only_and_apply_retry_safe(self):
        first = self.grant(apply=False)
        self.assertEqual(first['status'], 'would_grant')
        for table in ('legacy_store_purchases', 'legacy_store_aliases', 'legacy_store_audit', 'platform_credit_ledger', 'platform_entitlements'):
            self.assertEqual(self.scalar('SELECT count(*) FROM ' + table), 0)
        self.grant()
        self.assertEqual(self.grant(apply=False)['status'], 'already_granted')

    def test_rejects_pending_invalid_mismatched_and_nonhistorical(self):
        for changes in ({'state': 'pending'}, {'state': 'unverifiable'}, {'state': 'canceled'},
                        {'state': 'chargeback'}, {'valid': False}, {'accountToken': 'other'},
                        {'productId': 'buildingland.report_credits_10'}, {'purchasedMs': 1900000000000}):
            with self.subTest(changes=changes), self.assertRaises(HTTPException):
                self.grant(**changes)
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 7)

    def test_unbound_requires_review_and_cannot_transfer(self):
        proof = self.evidence(accountToken='')
        with self.assertRaises(HTTPException):
            process_legacy(self.settings, user_id=self.user, **proof)
        approve_binding(self.settings, user_id=self.user, evidence_ref='support/case-1', **proof)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_store_bindings'), 0)
        approve_binding(self.settings, user_id=self.user, evidence_ref='support/case-1', apply=True, **proof)
        process_legacy(self.settings, user_id=self.user, **proof)
        other, _, _ = self._create_user()
        with self.assertRaises(HTTPException):
            approve_binding(self.settings, user_id=other, evidence_ref='case-2', apply=True, **proof)
        with self.assertRaises(HTTPException):
            process_legacy(self.settings, user_id=other, **proof)

    def test_existing_store_owner_conflict_is_not_auto_migrated(self):
        proof = self.evidence()
        other, _, _ = self._create_user()
        from app.platform.security import sha256_text
        from app.platform.repository import new_id
        with psycopg.connect(self.database_url) as c:
            c.execute('''INSERT INTO mobile_store_transactions
                (id,platform,transaction_key,verification_digest,user_id,product_id,store_environment,status,pricing_policy)
                VALUES (%s,'android','order-one',%s,%s,'remove_ads_monthly','production','entitled','legacy')''',
                (new_id(), sha256_text(proof['verification_data']), other))
        with self.assertRaises(HTTPException):
            process_legacy(self.settings, user_id=self.user, **proof)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_store_purchases'), 0)

    def test_ack_failure_is_after_commit_and_retry_does_not_grant_twice(self):
        with patch('app.platform.legacy_verifier.acknowledge_legacy', side_effect=HTTPException(503, 'retry')):
            with self.assertRaises(HTTPException):
                self.grant()
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 17)
        self.assertEqual(self.grant()['status'], 'already_granted')
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 17)

    def test_refund_dry_run_and_zero_balance_reconciliation(self):
        revoked = verify_legacy(self.settings, **self.evidence(state='revoked'))
        self.assertEqual(reconcile(self.settings, purchase=revoked)['status'], 'would_revoke')
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_store_purchases'), 0)
        self.grant()
        with psycopg.connect(self.database_url) as c:
            c.execute('UPDATE platform_users SET paid_remaining=0 WHERE id=%s', (self.user,))
        reconcile(self.settings, purchase=revoked)
        self.assertEqual(self.scalar('SELECT state FROM legacy_store_purchases'), 'granted')
        result = reconcile(self.settings, purchase=revoked, apply=True)
        self.assertEqual(result['debited'], 0)
        self.assertEqual(result['reconciliationCredits'], 10)
        self.assertEqual(self.scalar("SELECT count(*) FROM platform_credit_ledger WHERE delta<0"), 0)

    def test_apple_signed_refund_handler_preserves_entitlement_and_rejects_bad_envelope(self):
        self.grant(platform='ios')
        item = SimpleNamespace(transactionId='restored-id', originalTransactionId='original-one',
            productId='remove_ads_monthly', type='Non-Consumable', inAppOwnershipType='PURCHASED',
            revocationDate=1700000000100, originalPurchaseDate=1700000000000,
            purchaseDate=1700000000000, appAccountToken=self.ios_token)
        verifier = Mock()
        verifier.verify_and_decode_notification.return_value = SimpleNamespace(notificationType='REFUND',
            data=SimpleNamespace(signedTransactionInfo='nested'))
        verifier.verify_and_decode_signed_transaction.return_value = item
        with patch('app.platform.legacy_notifications.apple_verifier', return_value=verifier):
            self.assertEqual(apple_notification(self.settings, {'signedPayload': 'synthetic'})['status'], 'revoked')
            self.assertEqual(apple_notification(self.settings, {'signedPayload': 'synthetic'})['status'], 'already_revoked')
            verifier.verify_and_decode_notification.side_effect = ValueError('invalid signature')
            with self.assertRaises(HTTPException):
                apple_notification(self.settings, {'signedPayload': 'tampered'})
        self.assertEqual(self.scalar("SELECT count(*) FROM platform_entitlements WHERE status='active'"), 1)

    def test_schema_rerun_does_not_remove_grant_or_enable_second_grant(self):
        self.grant()
        migration = Path(__file__).resolve().parents[2] / 'db/013_legacy_store_migration.sql'
        with psycopg.connect(self.database_url, autocommit=True) as c:
            c.execute(migration.read_text(encoding='utf-8'))
            c.execute(migration.read_text(encoding='utf-8'))
        self.assertEqual(self.grant()['status'], 'already_granted')
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 17)

    def test_transaction_rolls_back_ledger_on_failure(self):
        with patch('app.platform.legacy_migration._audit', side_effect=RuntimeError('synthetic failure')):
            with self.assertRaises(RuntimeError):
                self.grant()
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 7)
        self.assertEqual(self.scalar('SELECT count(*) FROM platform_credit_ledger'), 0)
        self.grant()
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 17)

    def test_refund_once_no_negative_balance_and_preserves_ad_removal(self):
        self.grant()
        with psycopg.connect(self.database_url) as c:
            c.execute('UPDATE platform_users SET paid_remaining=3 WHERE id=%s', (self.user,))
        revoked = verify_legacy(self.settings, **self.evidence(state='revoked'))
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: reconcile(self.settings, purchase=revoked, apply=True), range(12)))
        self.assertEqual(sum(r.get('debited', 0) for r in results), 3)
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 0)
        self.assertEqual(self.scalar('SELECT reconciliation_credits FROM legacy_store_purchases'), 7)
        self.assertEqual(self.scalar("SELECT count(*) FROM platform_entitlements WHERE status='active'"), 1)
        self.assertEqual(self.grant()['status'], 'revoked')

    def test_refund_before_grant_wins_and_kill_switch_keeps_reconciliation(self):
        proof = verify_legacy(self.settings, **self.evidence(state='revoked'))
        disabled = replace(self.settings, legacy_grant_enabled=False)
        reconcile(disabled, purchase=proof, apply=True)
        self.assertEqual(self.grant()['status'], 'revoked')
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 7)

    def test_concurrent_grant_refund_no_resurrection(self):
        revoked = verify_legacy(self.settings, **self.evidence(state='revoked'))
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(self.grant)
            b = pool.submit(reconcile, self.settings, purchase=revoked, apply=True)
            a.result(); b.result()
        self.assertEqual(self.scalar('SELECT state FROM legacy_store_purchases'), 'revoked')
        self.assertEqual(self._credit_summary(self.headers)['paidRemaining'], 7)

    def test_restore_and_history_are_authorized_and_server_amount_fixed(self):
        evidence = self.evidence()
        request = {'platform': 'android', 'productId': 'remove_ads_monthly', 'restored': True,
                   'verificationData': evidence['verification_data'], 'credits': 99999}
        with patch('app.platform.routes._settings', return_value=self.settings):
            self.assertEqual(self.client.post('/api/mobile/v1/store/restore', json=request).status_code, 401)
            first = self.client.post('/api/mobile/v1/store/restore', headers=self.headers, json=request)
            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual(first.json()['legacyMigration']['creditsGranted'], 10)
            again = self.client.post('/api/mobile/v1/store/restore', headers=self.headers, json=request)
            self.assertEqual(again.json()['legacyMigration']['status'], 'already_granted')
            history = self.client.get('/api/mobile/v1/store/legacy-migration/v1', headers=self.headers).json()
            self.assertEqual(history['purchases'][0]['creditsGranted'], 10)
            other, _, headers = self._create_user()
            self.assertEqual(self.client.get('/api/mobile/v1/store/legacy-migration/v1', headers=headers).json()['purchases'], [])

    def test_google_authenticated_void_notification_handles_provider_410(self):
        self.grant()
        data = {'packageName': 'test.package', 'voidedPurchaseNotification': {'productType': 2, 'purchaseToken': 'sensitive-purchase-token'}}
        payload = {'message': {'data': base64.b64encode(json.dumps(data).encode()).decode()}}
        with self.assertRaises(HTTPException):
            google_notification(self.settings, payload, None)
        with patch('app.platform.legacy_notifications.verify_google_push'):
            result = google_notification(self.settings, payload, 'Bearer synthetic')
            self.assertEqual(result['status'], 'revoked')
            self.assertEqual(google_notification(self.settings, payload, 'Bearer synthetic')['status'], 'already_revoked')

    def test_fake_verifier_cannot_be_used_in_production(self):
        with self.assertRaises(HTTPException):
            self.grant(settings=replace(self.settings, app_env='production'))

    def test_batch_report_redaction_and_retry(self):
        import importlib.util
        path = Path(__file__).resolve().parents[2] / 'scripts/legacy_store_migration.py'
        spec = importlib.util.spec_from_file_location('legacy_cli', path)
        cli = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cli)
        evidence = self.evidence()
        row = {'userId': self.user, 'platform': 'android', 'productId': evidence['product_id'],
               'verificationData': evidence['verification_data']}
        result = list(cli.run_batch(self.settings, ['invalid-secret-token', json.dumps(row)]))
        self.assertEqual([r['status'] for r in result], ['invalid_input', 'would_grant'])
        text = json.dumps(result)
        for secret in ('invalid-secret-token', 'sensitive-purchase-token', self.user, self.token):
            self.assertNotIn(secret, text)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_store_purchases'), 0)
        self.assertEqual(list(cli.run_batch(self.settings, [json.dumps(row)], apply=True))[0]['status'], 'granted')
        self.assertEqual(list(cli.run_batch(self.settings, [json.dumps(row)], apply=True))[0]['status'], 'already_granted')


class ProviderVerificationTest(unittest.TestCase):
    def test_google_pending_refund_promo_reward_sandbox_product_and_caller_key(self):
        settings = get_settings()
        verifier = Mock()
        response = Mock(status_code=200)
        verifier._session.return_value.get.return_value = response
        good = dict(productId='legacy', orderId='order', purchaseState=0, consumptionState=0,
                    purchaseTimeMillis='1700000000000', acknowledgementState=1)
        with patch('app.platform.legacy_verifier.GooglePlayVerifier', return_value=verifier):
            response.json.return_value = good
            accepted = google_purchase(settings, 'legacy', 'raw-token', 'order')
            self.assertNotIn('raw-token', accepted.identity)
            for change in ({'purchaseType': 0}, {'purchaseType': 1}, {'purchaseType': 2},
                           {'productId': 'different'}, {'quantity': 2}, {'consumptionState': 1}, {'refundableQuantity': 0}):
                response.json.return_value = {**good, **change}
                with self.subTest(change=change), self.assertRaises(HTTPException):
                    google_purchase(settings, 'legacy', 'raw-token', None)
            for state, expected in ((1, 'revoked'), (2, 'pending')):
                response.json.return_value = {**good, 'purchaseState': state}
                self.assertEqual(google_purchase(settings, 'legacy', 'raw-token', None).state, expected)
            response.json.return_value = good
            with self.assertRaises(HTTPException):
                google_purchase(settings, 'legacy', 'raw-token', 'client-invented-order')

    def test_apple_requires_current_api_nonconsumable_paid_owned_purchase(self):
        good = dict(transactionId='tx', originalTransactionId='original', productId='legacy',
                    type='Non-Consumable', inAppOwnershipType='PURCHASED', revocationDate=None,
                    appAccountToken='account', price=4900000, originalPurchaseDate=1700000000000, purchaseDate=1700000000000)
        verifier = Mock()
        verifier.verify_and_decode_signed_transaction.return_value = SimpleNamespace(transactionId='tx', productId='legacy')
        with patch('app.platform.legacy_verifier.apple_verifier', return_value=verifier), patch('app.platform.legacy_verifier._apple_current') as current:
            current.return_value = SimpleNamespace(**good)
            self.assertEqual(apple_purchase(get_settings(), 'legacy', 'signed.payload.value', 'tx').state, 'purchased')
            current.assert_called_once()
            for change in ({'price': 0}, {'price': None}, {'type': 'Auto-Renewable Subscription'},
                           {'inAppOwnershipType': 'FAMILY_SHARED'}, {'transactionId': 'other'}, {'productId': 'other'}):
                current.return_value = SimpleNamespace(**{**good, **change})
                with self.subTest(change=change), self.assertRaises(HTTPException):
                    apple_purchase(get_settings(), 'legacy', 'signed.payload.value', 'tx')
            current.return_value = SimpleNamespace(**{**good, 'revocationDate': 1700000000001})
            self.assertEqual(apple_purchase(get_settings(), 'legacy', 'signed.payload.value', 'tx').state, 'revoked')

    def test_official_apple_sdk_rejects_unsigned_receipt_and_invalid_notification(self):
        from appstoreserverlibrary.signed_data_verifier import SignedDataVerifier, VerificationException
        from appstoreserverlibrary.models.Environment import Environment
        verifier = SignedDataVerifier([], True, Environment.PRODUCTION, 'test.bundle', 123)
        with self.assertRaises(VerificationException):
            verifier.verify_and_decode_signed_transaction('unsigned.invalid.data')
        with self.assertRaises(VerificationException):
            verifier.verify_and_decode_notification('unsigned.invalid.data')

    def test_google_push_rejects_wrong_audience_email_or_unverified_identity(self):
        settings = replace(get_settings(), google_rtdn_audience='https://test', google_rtdn_email='verified@example.test')
        for claim in ({'email': 'other', 'email_verified': True}, {'email': 'verified@example.test', 'email_verified': False}):
            with patch('google.oauth2.id_token.verify_oauth2_token', return_value=claim), self.assertRaises(HTTPException):
                verify_google_push(settings, 'Bearer token')
        with patch('google.oauth2.id_token.verify_oauth2_token', side_effect=ValueError('bad audience')), self.assertRaises(HTTPException):
            verify_google_push(settings, 'Bearer token')
