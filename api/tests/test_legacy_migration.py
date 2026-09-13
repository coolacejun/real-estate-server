from __future__ import annotations
import base64
import copy
import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from unittest.mock import patch, Mock
from urllib.parse import unquote
import psycopg
from fastapi import HTTPException
from fastapi.testclient import TestClient
import test_mobile_platform as base
from app.main import app
from app.platform.config import get_settings
from app.platform.legacy_verifier import PRODUCT, now, token_digest, verify_legacy, _session
from app.platform.legacy_migration import process_legacy, approve_binding, reconcile
from app.platform.legacy_notifications import google_notification, apple_notification, verify_google_push

class LegacySubscriptionTest(unittest.TestCase):
    _create_user = base.MobilePlatformContractTest._create_user
    _credit_summary = base.MobilePlatformContractTest._credit_summary

    @classmethod
    def setUpClass(cls):
        cls.database_url = os.environ['DATABASE_URL']
        cls.client = TestClient(app)
        cls.settings = replace(get_settings(), legacy_grant_enabled=True, legacy_notifications_enabled=True,
            google_play_package_name='test.package', google_rtdn_audience='https://test/push', google_rtdn_email='push@example.test')

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        with psycopg.connect(self.database_url) as c:
            c.execute('TRUNCATE platform_users,mobile_oauth_flows,legacy_subscription_lineages,legacy_subscription_order_voids,legacy_subscription_token_revocations CASCADE')
        self.user, _, self.headers = self._create_user(paid=7)
        self.account = self.client.get('/api/mobile/v1/store/catalog?platform=android', headers=self.headers).json()['accountToken']
        self.subs, self.orders = {}, {}
        self.catalog = {'productId': PRODUCT, 'basePlans': [{'basePlanId': 'monthly', 'autoRenewingBasePlanType': {'billingPeriodDuration': 'P1M'}}]}
        self.add('token-a', 'order-a')
        self.transport = patch('app.platform.legacy_verifier._get_json', side_effect=self.provider)
        self.transport.start()
        self.addCleanup(self.transport.stop)

    def add(self, token, order, linked=None, state='ACTIVE', days=30, account=None):
        self.subs[token] = {'kind': 'androidpublisher#subscriptionPurchaseV2',
            'subscriptionState': 'SUBSCRIPTION_STATE_' + state,
            'acknowledgementState': 'ACKNOWLEDGEMENT_STATE_ACKNOWLEDGED',
            'externalAccountIdentifiers': {'obfuscatedExternalAccountId': self.account if account is None else account},
            'lineItems': [{'productId': PRODUCT, 'autoRenewingPlan': {'autoRenewEnabled': state != 'CANCELED'},
                'offerDetails': {'basePlanId': 'monthly'}, 'latestSuccessfulOrderId': order,
                'expiryTime': (now() + timedelta(days=days)).isoformat()}]}
        if linked:
            self.subs[token]['linkedPurchaseToken'] = linked
        self.orders[order] = {'orderId': order, 'purchaseToken': token, 'state': 'PROCESSED', 'lineItems': [
            {'productId': PRODUCT, 'total': {'units': '4900', 'currencyCode': 'KRW'},
             'subscriptionDetails': {'basePlanId': 'monthly', 'offerPhaseDetails': {'baseDetails': {}}}}]}

    def provider(self, settings, resource):
        if resource.startswith('purchases/subscriptionsv2/tokens/'):
            row = self.subs.get(unquote(resource.split('/')[-1]))
        elif resource.startswith('orders/'):
            row = self.orders.get(unquote(resource[7:]))
        elif resource == 'subscriptions/' + PRODUCT:
            row = self.catalog
        else:
            raise AssertionError('unexpected provider API')
        if row is None:
            raise HTTPException(422, 'synthetic unavailable')
        return copy.deepcopy(row)

    def verification(self, token='token-a'):
        return dict(platform='android', product_id=PRODUCT, verification_data=token)

    def grant(self, token='token-a', **kwargs):
        return process_legacy(kwargs.pop('settings', self.settings), user_id=kwargs.pop('user_id', self.user),
            **self.verification(token), **kwargs)

    def scalar(self, sql, params=()):
        with psycopg.connect(self.database_url) as c:
            return c.execute(sql, params).fetchone()[0]

    def profile(self):
        return self.client.get('/api/mobile/v1/me', headers=self.headers).json()

    def push(self, body, settings=None):
        payload = {'message': {'data': base64.b64encode(json.dumps({'packageName': 'test.package', **body}).encode()).decode()}}
        with patch('app.platform.legacy_notifications.verify_google_push'):
            return google_notification(settings or self.settings, payload, 'Bearer synthetic')

    def notice(self, kind, token='token-a'):
        return self.push({'subscriptionNotification': {'subscriptionId': PRODUCT, 'purchaseToken': token, 'notificationType': kind}})

    def void(self, order='order-a', token='token-a'):
        return self.push({'voidedPurchaseNotification': {'productType': 1, 'purchaseToken': token, 'orderId': order}})

    def test_concurrent_replay_once_per_account(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: self.grant(), range(12)))
        self.assertEqual(sum(r['creditsGranted'] for r in results), 10)
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 17)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_grants'), 1)

    def test_valid_states_and_expiry(self):
        for state in ('ACTIVE', 'CANCELED', 'IN_GRACE_PERIOD'):
            with self.subTest(state=state):
                self.subs['token-a']['subscriptionState'] = 'SUBSCRIPTION_STATE_' + state
                result = self.grant(apply=False)
                self.assertEqual(result['status'], 'would_grant')
                self.assertTrue(result['entitlementActive'])
        for state in ('EXPIRED', 'PENDING', 'PAUSED', 'ON_HOLD', 'PENDING_PURCHASE_CANCELED', 'UNSPECIFIED'):
            with self.subTest(state=state):
                self.subs['token-a']['subscriptionState'] = 'SUBSCRIPTION_STATE_' + state
                result = self.grant(apply=False)
                self.assertEqual(result['creditsGranted'], 0)
                self.assertFalse(result['entitlementActive'])
        self.add('token-a', 'order-a', days=-1)
        self.assertEqual(self.grant()['creditsGranted'], 0)

    def test_normal_expiry_does_not_reverse_credits(self):
        self.grant()
        self.add('token-a', 'order-a', state='EXPIRED', days=-1)
        self.notice(13)
        self.assertEqual(self.profile()['storeEntitlements'], [])
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 17)
        self.assertEqual(self.scalar("SELECT state FROM legacy_subscription_grants"), 'granted')

    def test_profile_expires_without_another_provider_event(self):
        self.grant()
        with psycopg.connect(self.database_url) as c:
            c.execute("UPDATE platform_entitlements SET expires_at=NOW()-INTERVAL '1 second'")
        self.assertEqual(self.profile()['storeEntitlements'], [])

    def test_hold_then_recovery_resyncs_without_new_grant(self):
        self.grant()
        self.subs['token-a']['subscriptionState'] = 'SUBSCRIPTION_STATE_ON_HOLD'
        self.notice(5)
        self.assertEqual(self.profile()['storeEntitlements'], [])
        self.subs['token-a']['subscriptionState'] = 'SUBSCRIPTION_STATE_ACTIVE'
        self.notice(1)
        self.assertEqual(len(self.profile()['storeEntitlements']), 1)
        self.assertEqual(self.grant()['creditsGranted'], 0)

    def test_monthly_plan_and_paid_provider_evidence_required(self):
        self.catalog['basePlans'][0]['autoRenewingBasePlanType']['billingPeriodDuration'] = 'P1Y'
        with self.assertRaises(HTTPException):
            self.grant()
        self.catalog['basePlans'][0]['autoRenewingBasePlanType']['billingPeriodDuration'] = 'P1M'
        line = self.orders['order-a']['lineItems'][0]
        line['total']['units'] = '0'
        self.assertEqual(self.grant(apply=False)['creditsGranted'], 0)
        line['total']['units'] = '1000'  # Actual paid amount need not equal 4900.
        self.assertEqual(self.grant(apply=False)['creditsGranted'], 10)
        line['subscriptionDetails']['offerPhaseDetails'] = {'freeTrialDetails': {}}
        self.assertEqual(self.grant(apply=False)['creditsGranted'], 0)

    def test_invalid_payment_states_excluded(self):
        for state in ('PENDING', 'CANCELED', 'PENDING_REFUND', 'REFUNDED', 'PARTIALLY_REFUNDED'):
            self.orders['order-a']['state'] = state
            self.assertEqual(self.grant(apply=False)['creditsGranted'], 0)

    def test_wrong_product_prepaid_installment_test_purchase_fail_closed(self):
        original = copy.deepcopy(self.subs['token-a'])
        for mutation in ('product', 'prepaid', 'installment', 'test'):
            self.subs['token-a'] = copy.deepcopy(original)
            p = self.subs['token-a']
            if mutation == 'product': p['lineItems'][0]['productId'] = 'different'
            if mutation == 'prepaid': p['lineItems'][0]['prepaidPlan'] = {}
            if mutation == 'installment': p['lineItems'][0]['autoRenewingPlan']['installmentDetails'] = {}
            if mutation == 'test': p['testPurchase'] = {}
            with self.subTest(mutation=mutation), self.assertRaises(HTTPException): self.grant()

    def test_apple_and_other_legacy_ids_fail_closed(self):
        for platform, product in (('ios', PRODUCT), ('android', 'retired.other')):
            with self.assertRaises(HTTPException) as failure:
                process_legacy(self.settings, user_id=self.user, platform=platform, product_id=product, verification_data='x')
            self.assertEqual(failure.exception.status_code, 410)
        with self.assertRaises(HTTPException): apple_notification(self.settings, {})
        self.assertEqual(self.client.get('/api/mobile/v1/store/catalog?platform=ios', headers=self.headers).json()['legacyRestoreProductIds'], [])

    def test_missing_identity_requires_review_and_cannot_transfer(self):
        self.subs['token-a']['externalAccountIdentifiers'] = {}
        with self.assertRaises(HTTPException): self.grant()
        approve_binding(self.settings, user_id=self.user, evidence_ref='support/case1', **self.verification())
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_lineages'), 0)
        approve_binding(self.settings, user_id=self.user, evidence_ref='support/case1', apply=True, **self.verification())
        self.grant()
        other, _, _ = self._create_user()
        with self.assertRaises(HTTPException): self.grant(user_id=other)

    def test_provider_account_mismatch_cannot_be_manually_overridden(self):
        self.subs['token-a']['externalAccountIdentifiers']['obfuscatedExternalAccountId'] = 'other'
        with self.assertRaises(HTTPException): self.grant()
        with self.assertRaises(HTTPException):
            approve_binding(self.settings, user_id=self.user, evidence_ref='support/case1', apply=True, **self.verification())

    def test_renewal_linked_tokens_and_separate_lineage_never_double_grant(self):
        self.grant()
        self.add('token-a', 'order-renewal')
        self.assertEqual(self.grant()['creditsGranted'], 0)
        self.add('token-b', 'order-b', linked='token-a')
        self.assertEqual(self.grant('token-b')['creditsGranted'], 0)
        self.add('token-c', 'order-c')
        self.assertEqual(self.grant('token-c')['creditsGranted'], 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_lineages'), 2)
        other, _, _ = self._create_user()
        with self.assertRaises(HTTPException): self.grant('token-b', user_id=other)

    def test_unknown_link_chain_resolves_root_and_conflicting_owner_rejected(self):
        self.add('token-b', 'order-b', linked='token-a')
        self.grant('token-b')
        self.assertEqual(self.scalar('SELECT root_digest FROM legacy_subscription_lineages'), token_digest('token-a'))
        self.assertEqual(self.grant()['creditsGranted'], 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_grants'), 1)

    def test_missing_ancestor_or_cycle_fails_closed(self):
        self.subs['token-a']['linkedPurchaseToken'] = 'missing'
        with self.assertRaises(HTTPException): self.grant()
        self.subs['token-a']['linkedPurchaseToken'] = 'token-a'
        with self.assertRaises(HTTPException): self.grant()
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_grants'), 0)

    def test_persisted_ancestor_can_anchor_after_provider_token_expiry(self):
        self.grant()
        self.add('token-b', 'order-b', linked='token-a')
        del self.subs['token-a']
        self.assertEqual(self.grant('token-b')['creditsGranted'], 0)

    def test_old_replaced_token_cannot_reactivate_or_clear_current_entitlement(self):
        self.grant()
        self.add('token-b', 'order-b', linked='token-a')
        self.grant('token-b')
        expiry = self.profile()['storeEntitlements'][0]['expiresAt']
        self.add('token-a', 'order-a', state='EXPIRED', days=-1)
        self.notice(13)
        self.assertEqual(self.profile()['storeEntitlements'][0]['expiresAt'], expiry)
        self.subs['token-b']['subscriptionState'] = 'SUBSCRIPTION_STATE_ON_HOLD'
        self.notice(5, 'token-b')
        self.add('token-a', 'order-a')
        self.grant()
        self.assertEqual(self.profile()['storeEntitlements'], [])

    def test_source_refund_reversal_nonnegative_and_idempotent(self):
        self.grant()
        with psycopg.connect(self.database_url) as c:
            c.execute('UPDATE platform_users SET paid_remaining=3 WHERE id=%s', (self.user,))
        result = self.void()
        self.assertEqual((result['debited'], result['reconciliationCredits']), (3, 7))
        self.assertEqual(self.void()['status'], 'already_revoked')
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 0)
        self.assertEqual(self.profile()['storeEntitlements'], [])
        self.assertEqual(self.grant()['creditsGranted'], 0)

    def test_other_renewal_refund_never_reverses_source_grant(self):
        self.grant()
        self.add('token-a', 'order-renewal')
        self.grant()
        self.void('order-renewal')
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 17)
        self.assertEqual(self.scalar('SELECT state FROM legacy_subscription_grants'), 'granted')
        self.assertEqual(self.profile()['storeEntitlements'], [])

    def test_source_order_refund_discovered_during_reconcile_after_renewal(self):
        self.grant()
        self.add('token-a', 'order-renewal')
        self.orders['order-a']['state'] = 'REFUNDED'
        result = reconcile(self.settings, apply=True, **self.verification())
        self.assertEqual(result['status'], 'revoked')
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 7)
        self.assertEqual(len(self.profile()['storeEntitlements']), 1)

    def test_void_before_claim_wins_even_against_stale_processed_order(self):
        self.void()
        self.assertEqual(self.grant()['creditsGranted'], 0)
        self.assertEqual(self.profile()['storeEntitlements'], [])

    def test_revoked_token_before_claim_never_reactivates(self):
        self.subs['token-a']['subscriptionState'] = 'SUBSCRIPTION_STATE_EXPIRED'
        self.notice(12)
        self.add('token-a', 'order-a')
        self.assertEqual(self.grant()['creditsGranted'], 0)
        self.assertEqual(self.profile()['storeEntitlements'], [])

    def test_source_revocation_and_other_renewal_revocation(self):
        self.grant()
        self.subs['token-a']['subscriptionState'] = 'SUBSCRIPTION_STATE_EXPIRED'
        self.notice(12)
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 7)

    def test_dry_run_rolls_back_every_write_including_bindings_and_reversals(self):
        self.assertEqual(self.grant(apply=False)['status'], 'would_grant')
        for table in ('legacy_subscription_lineages', 'legacy_subscription_tokens', 'legacy_subscription_grants', 'legacy_subscription_audit', 'platform_entitlements', 'platform_credit_ledger'):
            self.assertEqual(self.scalar('SELECT count(*) FROM ' + table), 0)
        self.grant()
        self.orders['order-a']['state'] = 'REFUNDED'
        self.assertEqual(reconcile(self.settings, apply=False, **self.verification())['status'], 'would_revoke')
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 17)

    def test_grant_switch_does_not_disable_entitlement_sync_or_refunds(self):
        self.assertFalse(get_settings().legacy_grant_enabled)
        off = replace(self.settings, legacy_grant_enabled=False)
        self.assertEqual(self.grant(settings=off)['status'], 'grant_disabled')
        self.assertEqual(len(self.profile()['storeEntitlements']), 1)
        self.grant()
        self.push({'voidedPurchaseNotification': {'productType': 1, 'purchaseToken': 'token-a', 'orderId': 'order-a'}}, settings=off)
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 7)

    def test_authenticated_rtdn_required(self):
        with self.assertRaises(HTTPException) as failure:
            google_notification(self.settings, {}, None)
        self.assertEqual(failure.exception.status_code, 403)
        with patch('google.oauth2.id_token.verify_oauth2_token', return_value={'email': 'wrong', 'email_verified': True}):
            with self.assertRaises(HTTPException): verify_google_push(self.settings, 'Bearer synthetic')

    def test_restore_api_returns_expiry_and_no_raw_receipt(self):
        with patch('app.platform.routes._settings', return_value=self.settings):
            reply = self.client.post('/api/mobile/v1/store/restore', headers=self.headers, json={
                'platform': 'android', 'productId': PRODUCT, 'verificationData': 'token-a', 'restored': True})
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(reply.json()['legacyMigration']['creditsGranted'], 10)
        self.assertTrue(reply.json()['expiresAt'])
        self.assertNotIn('token-a', reply.text)
        self.assertNotIn('order-a', reply.text)

    def test_fake_transport_cannot_be_used_in_production(self):
        with self.assertRaises(HTTPException): _session(replace(self.settings, app_env='production', store_verifier_mode='fake'))

    def test_acknowledgement_is_after_commit_and_retry_does_not_regrant(self):
        self.subs['token-a']['acknowledgementState'] = 'ACKNOWLEDGEMENT_STATE_PENDING'
        with patch('app.platform.legacy_migration.acknowledge_legacy', side_effect=HTTPException(503, 'synthetic')):
            with self.assertRaises(HTTPException): self.grant()
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_grants'), 1)
        with patch('app.platform.legacy_migration.acknowledge_legacy') as ack:
            self.assertEqual(self.grant()['creditsGranted'], 0)
            ack.assert_called_once()

    def test_concurrent_void_and_claim_never_leave_the_grant_available(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            claim, refund = pool.submit(self.grant), pool.submit(self.void)
            claim.result(); refund.result()
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 7)
        self.assertEqual(self.profile()['storeEntitlements'], [])

    def test_other_renewal_revocation_does_not_reverse_source(self):
        self.grant()
        self.add('token-a', 'order-renewal', state='EXPIRED')
        self.notice(12)
        self.assertEqual(self.scalar('SELECT state FROM legacy_subscription_grants'), 'granted')
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 17)
        self.assertEqual(self.profile()['storeEntitlements'], [])

    def test_source_chargeback_reconciles_from_google_order(self):
        self.grant()
        self.orders['order-a']['state'] = 'REFUNDED'
        self.orders['order-a']['orderHistory'] = {'refundEvent': {'refundReason': 'CHARGEBACK'}}
        reconcile(self.settings, apply=True, **self.verification())
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 7)

    def test_database_failure_rolls_back_ledger_and_binding_together(self):
        with patch('app.platform.legacy_migration._audit', side_effect=RuntimeError('synthetic failure')):
            with self.assertRaises(RuntimeError): self.grant()
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 7)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_lineages'), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM platform_credit_ledger'), 0)

    def test_cli_redaction_dry_run_and_apply_are_retry_safe(self):
        import importlib.util
        from pathlib import Path
        spec = importlib.util.spec_from_file_location('legacy_cli', Path(__file__).resolve().parents[2] / 'scripts' / 'legacy_store_migration.py')
        cli = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cli)
        rows = [json.dumps({'userId': self.user, 'platform': 'android', 'productId': PRODUCT, 'verificationData': 'token-a'})]
        dry = list(cli.run_batch(self.settings, rows))[0]
        self.assertEqual(dry['status'], 'would_grant')
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_grants'), 0)
        first = list(cli.run_batch(self.settings, rows, apply=True))[0]
        second = list(cli.run_batch(self.settings, rows, apply=True))[0]
        self.assertEqual((first['creditsGranted'], second['creditsGranted']), (10, 0))
        report = json.dumps([dry, first, second])
        for private in ('token-a', 'order-a', self.user, self.account): self.assertNotIn(private, report)

    def test_out_of_app_repurchase_requires_stored_owner_and_keeps_lineage(self):
        self.add('token-b', 'order-b')
        self.subs['token-b']['outOfAppPurchaseContext'] = {'expiredPurchaseToken': 'token-a'}
        with self.assertRaises(HTTPException): self.grant('token-b')
        self.grant()
        del self.subs['token-a']
        self.assertEqual(self.grant('token-b')['creditsGranted'], 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_lineages'), 1)

    def test_order_only_reconciliation_works_after_token_inventory_is_unavailable(self):
        from app.platform.legacy_migration import reconcile_order
        self.grant()
        self.subs.clear()
        self.orders['order-a']['state'] = 'REFUNDED'
        self.assertEqual(reconcile_order(self.settings, order_id='order-a')['status'], 'would_revoke')
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 17)
        self.assertEqual(reconcile_order(self.settings, order_id='order-a', apply=True)['status'], 'revoked')
        self.assertEqual(self.profile()['creditSummary']['paidRemaining'], 7)
        with self.assertRaises(HTTPException): reconcile_order(self.settings, order_id='unknown', apply=True)

    def test_superseded_issued_grants_block_activation(self):
        self.grant()
        with psycopg.connect(self.database_url) as c:
            c.execute("""INSERT INTO legacy_store_purchases(id,platform,identity_digest,product_id,user_id,state,
                credits_granted,grant_ledger_id,evidence_ref)
                SELECT id,'android',repeat('a',64),'remove_ads_monthly',user_id,'granted',10,grant_ledger_id,'superseded-test'
                FROM legacy_subscription_grants""")
        other, _, headers = self._create_user()
        account = self.client.get('/api/mobile/v1/store/catalog?platform=android', headers=headers).json()['accountToken']
        self.add('token-other', 'order-other', account=account)
        with self.assertRaises(HTTPException): self.grant('token-other', user_id=other)
        self.assertEqual(self.scalar('SELECT count(*) FROM legacy_subscription_grants'), 1)

    def test_expiry_during_provider_lookup_prevents_grant_and_ack(self):
        self.subs['token-a']['acknowledgementState'] = 'ACKNOWLEDGEMENT_STATE_PENDING'
        with patch('app.platform.legacy_migration.utcnow', return_value=now()+timedelta(days=31)), patch('app.platform.legacy_migration.acknowledge_legacy') as ack:
            self.assertEqual(self.grant()['creditsGranted'], 0)
            ack.assert_not_called()

    def test_ineligible_paid_state_does_not_acknowledge(self):
        self.subs['token-a']['acknowledgementState'] = 'ACKNOWLEDGEMENT_STATE_PENDING'
        self.orders['order-a']['state'] = 'REFUNDED'
        with patch('app.platform.legacy_migration.acknowledge_legacy') as ack:
            self.grant()
            ack.assert_not_called()

    def test_old_one_time_verifiers_cannot_process_any_legacy_product(self):
        from app.platform.store import AppleStoreVerifier, GooglePlayVerifier, FakeStoreVerifier
        for verifier in (object.__new__(AppleStoreVerifier), object.__new__(GooglePlayVerifier), FakeStoreVerifier('android')):
            with self.assertRaises(HTTPException) as failure:
                verifier.verify(product_id=PRODUCT, verification_data='synthetic', transaction_id=None, account_token=self.account)
            self.assertEqual(failure.exception.status_code, 410)
        with self.assertRaises(HTTPException):
            object.__new__(GooglePlayVerifier).post_commit(product_id=PRODUCT, verification_data='synthetic')
