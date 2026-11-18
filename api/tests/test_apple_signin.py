from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlparse

import jwt
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from fastapi import HTTPException
from fastapi.testclient import TestClient

os.environ.setdefault('APP_ENV', 'test')
os.environ.setdefault('STORE_VERIFIER_MODE', 'fake')
os.environ.setdefault('SERVER_LOG_FILE', str(Path(tempfile.gettempdir()) / 'apple-signin-contract-api.log'))

from app.main import app
from app.platform.apple_signin import verify_identity_token
from app.platform.config import get_settings
from app.platform.repository import connect, issue_token_pair, resolve_oauth_identity
from app.platform.routes import _RATE_BUCKETS, _RATE_LOCK
from app.platform.security import sha256_text


class AppleSignInContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parsed = urlparse(os.environ['DATABASE_URL'])
        if parsed.hostname != '127.0.0.1' or parsed.username != 'archive_contract':
            raise RuntimeError('Apple contracts require the disposable loopback test database')
        cls.directory = tempfile.TemporaryDirectory()
        cls.private_file = Path(cls.directory.name) / 'synthetic-apple.p8'
        cls.private_file.write_bytes(ec.generate_private_key(ec.SECP256R1()).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        cls.private_file.chmod(0o600)
        cls.previous = {k: os.environ.get(k) for k in ['APPLE_BUNDLE_ID', 'APPLE_SIGNIN_TEAM_ID',
            'APPLE_SIGNIN_KEY_ID', 'APPLE_SIGNIN_PRIVATE_KEY_FILE', 'APPLE_SIGNIN_TOKEN_KEY']}
        os.environ.update(APPLE_BUNDLE_ID='ace.jun.realEstate', APPLE_SIGNIN_TEAM_ID='TESTTEAM00',
            APPLE_SIGNIN_KEY_ID='TESTKEY000', APPLE_SIGNIN_PRIVATE_KEY_FILE=str(cls.private_file),
            APPLE_SIGNIN_TOKEN_KEY=Fernet.generate_key().decode())
        get_settings.cache_clear()
        cls.settings = get_settings()
        cls.rsa_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.directory.cleanup()
        for k, v in cls.previous.items():
            if v is None: os.environ.pop(k, None)
            else: os.environ[k] = v
        get_settings.cache_clear()

    def setUp(self):
        with connect(self.settings) as connection:
            connection.execute('TRUNCATE platform_users,mobile_oauth_flows CASCADE')
        with _RATE_LOCK:
            _RATE_BUCKETS.clear()
        self.verifier = 'v' * 64
        self.challenge = base64.urlsafe_b64encode(hashlib.sha256(self.verifier.encode()).digest()).decode().rstrip('=')
        self.jwks = patch('app.platform.apple_signin._apple_jwks', return_value=SimpleNamespace(
            get_signing_key_from_jwt=lambda _: SimpleNamespace(key=self.rsa_key.public_key())))
        self.jwks.start()
        self.addCleanup(self.jwks.stop)

    def _start(self, headers=None, link=False):
        response = self.client.post('/api/mobile/v1/auth/apple/start', json={
            'codeChallenge': self.challenge, 'linkAccount': link}, headers=headers or {})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        return response.json()

    def _jwt(self, flow, subject='apple-user', **overrides):
        claims = {'iss': 'https://appleid.apple.com', 'aud': 'ace.jun.realEstate',
            'iat': int(time.time()), 'exp': int(time.time()) + 300, 'sub': subject,
            'nonce': flow['nonce'], 'email': 'hidden@privaterelay.appleid.com', 'email_verified': 'true'}
        claims.update(overrides)
        return jwt.encode(claims, self.rsa_key, algorithm='RS256', headers={'kid': 'test-rsa-key'})

    def _finish(self, flow, subject='apple-user', *, headers=None, full_name='Apple Tester', token=None, returned_subject=None):
        token = token or self._jwt(flow, subject)
        provider_tokens = {'id_token': self._jwt(flow, returned_subject or subject),
            'refresh_token': 'synthetic-provider-refresh-' + subject}
        with patch('app.platform.apple_signin._apple_post', return_value=provider_tokens) as provider:
            result = self.client.post('/api/mobile/v1/auth/apple/token', json={
                'state': flow['state'], 'codeVerifier': self.verifier,
                'deviceId': 'apple-contract-device-0001', 'identityToken': token,
                'authorizationCode': 'synthetic-one-use-code', 'fullName': full_name,
            }, headers=headers or {})
        return result, provider

    def _login(self, subject='apple-user'):
        response, _ = self._finish(self._start(), subject)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_private_relay_login_and_repeat_login_grant_only_once(self):
        first = self._login()
        self.assertEqual(first['loginProvider'], 'apple')
        self.assertEqual(first['user']['email'], 'hidden@privaterelay.appleid.com')
        self.assertEqual(first['creditSummary']['availableCredits'], 3)
        second, _ = self._finish(self._start(), full_name='')
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(second.json()['user']['id'], first['user']['id'])
        self.assertEqual(second.json()['socialAccounts'][0]['displayName'], 'Apple Tester')
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) AS n FROM platform_credit_ledger').fetchone()['n'], 1)

    def test_state_and_provider_code_exchange_are_one_time(self):
        flow = self._start()
        first, provider = self._finish(flow)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(provider.call_count, 1)
        replay, provider = self._finish(flow)
        self.assertEqual(replay.status_code, 401)
        self.assertEqual(provider.call_count, 0)

    def test_wrong_pkce_cannot_exchange(self):
        flow = self._start()
        self.verifier = 'x' * 64
        response, provider = self._finish(flow)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(provider.call_count, 0)

    def test_invalid_jwt_claims_fail_before_provider_exchange(self):
        for changes in [{'aud': 'other.app'}, {'iss': 'https://attacker.test'},
                        {'nonce': 'wrong'}, {'exp': int(time.time()) - 100},
                        {'iat': int(time.time()) + 100}, {'sub': ''}, {'iat': int(time.time()) - 1000}]:
            with self.subTest(changes=changes):
                flow = self._start()
                result, provider = self._finish(flow, token=self._jwt(flow, **changes))
                self.assertEqual(result.status_code, 401, result.text)
                self.assertEqual(provider.call_count, 0)

    def test_wrong_signature_is_rejected(self):
        flow = self._start()
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        token = jwt.encode({'iss': 'https://appleid.apple.com', 'aud': 'ace.jun.realEstate',
            'sub': 'attacker', 'nonce': flow['nonce'], 'iat': int(time.time()), 'exp': int(time.time()) + 300},
            other, algorithm='RS256', headers={'kid': 'test-rsa-key'})
        response, provider = self._finish(flow, token=token)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(provider.call_count, 0)

    def test_client_and_code_exchange_subject_must_match(self):
        result, _ = self._finish(self._start(), returned_subject='another-user')
        self.assertEqual(result.status_code, 401)
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) AS n FROM platform_users').fetchone()['n'], 0)

    def test_provider_refresh_token_is_encrypted_and_audit_is_credential_free(self):
        flow = self._start()
        result, _ = self._finish(flow)
        self.assertEqual(result.status_code, 200, result.text)
        with connect(self.settings) as connection:
            row = connection.execute('SELECT refresh_token_ciphertext FROM mobile_apple_credentials').fetchone()
            self.assertNotIn('synthetic-provider-refresh', row['refresh_token_ciphertext'])
            decoded = Fernet(self.settings.apple_signin_token_key.encode()).decrypt(row['refresh_token_ciphertext'].encode()).decode()
            self.assertEqual(decoded, 'synthetic-provider-refresh-apple-user')
            audit = json.dumps([dict(r) for r in connection.execute('SELECT detail FROM mobile_auth_events').fetchall()])
            self.assertNotIn('synthetic', audit)
            self.assertNotIn(flow['nonce'], audit)
            stored = connection.execute('SELECT nonce_hash FROM mobile_oauth_flows').fetchone()['nonce_hash']
            self.assertEqual(stored, sha256_text(flow['nonce']))

    def test_same_email_never_silently_merges_existing_social_account(self):
        with connect(self.settings) as connection:
            other = resolve_oauth_identity(connection, provider='google', subject='google-user',
                email='hidden@privaterelay.appleid.com', display_name='Other', link_user_id=None)
        current = self._login()
        self.assertNotEqual(current['user']['id'], other)

    def test_expired_challenge_is_rejected(self):
        flow = self._start()
        with connect(self.settings) as connection:
            connection.execute("UPDATE mobile_oauth_flows SET expires_at=NOW()-INTERVAL '1 minute'")
        result, provider = self._finish(flow)
        self.assertEqual(result.status_code, 401)
        self.assertEqual(provider.call_count, 0)

    def _google_session(self, subject):
        with connect(self.settings) as connection:
            user_id = resolve_oauth_identity(connection, provider='google', subject=subject,
                email=subject + '@example.test', display_name='Existing user', link_user_id=None)
            pair = issue_token_pair(connection, self.settings, user_id=user_id, device_id='google-contract-device-0001')
        return user_id, {'Authorization': 'Bearer ' + pair['accessToken']}

    def test_linking_requires_original_recent_session_and_preserves_balance(self):
        owner, original = self._google_session('owner')
        _, attacker = self._google_session('attacker')
        flow = self._start(original, link=True)
        failed, provider = self._finish(flow, headers=attacker)
        self.assertEqual(failed.status_code, 403, failed.text)
        provider.assert_not_called()
        linked, _ = self._finish(flow, headers=original)
        self.assertEqual(linked.status_code, 200, linked.text)
        self.assertEqual(linked.json()['user']['id'], owner)
        self.assertEqual(linked.json()['creditSummary']['availableCredits'], 3)
        self.assertEqual({r['provider'] for r in linked.json()['socialAccounts']}, {'google', 'apple'})

    def test_client_secret_uses_only_app_scoped_apple_claims(self):
        from app.platform.apple_signin import _client_secret
        key = serialization.load_pem_private_key(self.private_file.read_bytes(), password=None)
        token = _client_secret(self.settings)
        claims = jwt.decode(token, key.public_key(), algorithms=['ES256'],
            audience='https://appleid.apple.com', issuer='TESTTEAM00')
        self.assertEqual(claims['sub'], 'ace.jun.realEstate')
        self.assertLessEqual(claims['exp'] - claims['iat'], 300)
        self.assertEqual(jwt.get_unverified_header(token)['kid'], 'TESTKEY000')

    def test_apple_withdrawal_revokes_before_closing_session(self):
        login = self._login()
        headers = {'Authorization': 'Bearer ' + login['accessToken']}
        with patch('app.platform.apple_signin._apple_post', return_value={}) as provider:
            response = self.client.post('/api/v1/account/withdraw', headers=headers, json={'confirm': 'withdraw-account'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(provider.call_args.args[1], '/auth/revoke')
        self.assertEqual(provider.call_args.args[2]['token_type_hint'], 'refresh_token')
        self.assertEqual(self.client.get('/api/mobile/v1/me', headers=headers).status_code, 401)
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT status FROM platform_users').fetchone()['status'], 'withdrawn')
            self.assertEqual(connection.execute('SELECT refresh_token_ciphertext FROM mobile_apple_credentials').fetchone()['refresh_token_ciphertext'], '')

    def test_provider_revocation_outage_keeps_account_active(self):
        login = self._login()
        headers = {'Authorization': 'Bearer ' + login['accessToken']}
        with patch('app.platform.apple_signin._apple_post', side_effect=HTTPException(503, 'provider unavailable')):
            result = self.client.post('/api/v1/account/withdraw', headers=headers, json={'confirm': 'withdraw-account'})
        self.assertEqual(result.status_code, 503)
        with connect(self.settings) as connection:
            self.assertEqual(connection.execute('SELECT status FROM platform_users').fetchone()['status'], 'active')
            self.assertIsNone(connection.execute('SELECT revoked_at FROM mobile_apple_credentials').fetchone()['revoked_at'])

    def test_last_login_method_cannot_be_disconnected(self):
        login = self._login()
        headers = {'Authorization': 'Bearer ' + login['accessToken']}
        with patch('app.platform.apple_signin._apple_post') as provider:
            response = self.client.post('/api/v1/account/identities/apple/disconnect', headers=headers)
        self.assertEqual(response.status_code, 409)
        provider.assert_not_called()

    def test_daily_authorization_check_is_cached(self):
        login = self._login()
        headers = {'Authorization': 'Bearer ' + login['accessToken']}
        with connect(self.settings) as connection:
            connection.execute("UPDATE mobile_apple_credentials SET updated_at=NOW()-INTERVAL '2 days'")
        flow = {'nonce': 'not-needed-for-refresh'}
        with patch('app.platform.apple_signin._apple_post', return_value={'id_token': self._jwt(flow)}) as provider:
            first = self.client.get('/api/mobile/v1/me', headers=headers)
            second = self.client.get('/api/mobile/v1/me', headers=headers)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(provider.call_args.args[2]['grant_type'], 'refresh_token')

    def test_apple_revocation_invalidates_sessions_but_outage_does_not(self):
        login = self._login()
        headers = {'Authorization': 'Bearer ' + login['accessToken']}
        with connect(self.settings) as connection:
            connection.execute("UPDATE mobile_apple_credentials SET updated_at=NOW()-INTERVAL '2 days'")
        with patch('app.platform.apple_signin._apple_post', side_effect=HTTPException(503, 'provider unavailable')):
            self.assertEqual(self.client.get('/api/mobile/v1/me', headers=headers).status_code, 503)
        with connect(self.settings) as connection:
            self.assertIsNone(connection.execute('SELECT revoked_at FROM mobile_access_tokens LIMIT 1').fetchone()['revoked_at'])
        with patch('app.platform.apple_signin._apple_post', side_effect=HTTPException(401, 'invalid grant')):
            self.assertEqual(self.client.get('/api/mobile/v1/me', headers=headers).status_code, 401)
        with connect(self.settings) as connection:
            self.assertIsNotNone(connection.execute('SELECT revoked_at FROM mobile_access_tokens LIMIT 1').fetchone()['revoked_at'])


if __name__ == '__main__':
    unittest.main()
