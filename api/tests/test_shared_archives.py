from __future__ import annotations

import base64
import asyncio
import hashlib
import io
import json
import os
import importlib.util
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import psycopg
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pypdf import PdfWriter

from app.platform import router
from app.platform.body_limit import RequestBodyLimitMiddleware
from app.platform.config import get_settings
from app.platform.oauth import ProviderIdentity
from app.platform.repository import connect, issue_token_pair, resolve_oauth_identity


class SharedArchiveContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.update(APP_ENV='test', SHARED_ARCHIVES_ENABLED='true', ARCHIVE_UPLOADS_ENABLED='true',
            STORE_VERIFIER_MODE='fake', KAKAO_OAUTH_CLIENT_ID='test-client', KAKAO_OAUTH_CLIENT_SECRET='test-secret',
            NAVER_OAUTH_CLIENT_ID='test-client', NAVER_OAUTH_CLIENT_SECRET='test-secret',
            GOOGLE_OAUTH_CLIENT_ID='test-client', GOOGLE_OAUTH_CLIENT_SECRET='test-secret',
            PLATFORM_INTERNAL_SERVICE_TOKEN='test-internal-token-with-sufficient-length')
        get_settings.cache_clear()
        cls.settings = get_settings()
        app = FastAPI()
        app.add_middleware(RequestBodyLimitMiddleware)
        app.include_router(router)
        cls.client = TestClient(app, follow_redirects=False)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        os.environ['SHARED_ARCHIVES_ENABLED'] = 'false'
        os.environ['ARCHIVE_UPLOADS_ENABLED'] = 'false'
        get_settings.cache_clear()

    def setUp(self):
        with psycopg.connect(self.settings.database_url) as connection:
            connection.execute('TRUNCATE platform_users,mobile_oauth_flows,platform_account_review_queue CASCADE')
        self.owner, self.headers = self.user('owner')
        self.other, self.other_headers = self.user('other')
        self.pdf = self.make_pdf()
        self.metadata = {'requestId': 'local-upload:test-report', 'sha256': hashlib.sha256(self.pdf).hexdigest(),
            'byteSize': len(self.pdf), 'title': 'Saved local report', 'address': 'Synthetic address',
            'provenance': 'legacy-local', 'contentType': 'application/pdf'}

    def user(self, subject, provider='kakao'):
        with connect(self.settings) as connection:
            user_id = resolve_oauth_identity(connection, provider=provider, subject=subject,
                email='same@example.test', display_name=subject, link_user_id=None)
            token = issue_token_pair(connection, self.settings, user_id=user_id, device_id=f'device-{subject}-00000000')
        return user_id, {'Authorization': f"Bearer {token['accessToken']}"}

    @staticmethod
    def make_pdf(pages=1, active=False):
        writer = PdfWriter()
        for _ in range(pages):
            writer.add_blank_page(width=595, height=842)
        if active:
            writer.add_js('app.alert("test")')
        output = io.BytesIO()
        writer.write(output)
        return output.getvalue()

    def sql(self, query, args=()):
        with connect(self.settings) as connection:
            return connection.execute(query, args).fetchall()

    def start(self, metadata=None, headers=None):
        response = self.client.post('/api/v1/report-archives/uploads', json=metadata or self.metadata, headers=headers or self.headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()['uploadId']

    def upload(self):
        upload_id = self.start()
        response = self.client.put(f'/api/v1/report-archives/uploads/{upload_id}/content', content=self.pdf,
            headers={**self.headers, 'Content-Type': 'application/pdf'})
        self.assertEqual(response.status_code, 200, response.text)
        return upload_id

    def finish(self, upload_id, headers=None):
        return self.client.post('/api/v1/report-archives', json={'uploadId': upload_id}, headers=headers or self.headers)

    def test_upload_retry_and_same_owner_hash_dedup_preserve_ledger_and_bytes(self):
        ledger_before = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
        usages_before = self.sql('SELECT * FROM platform_report_usages')
        upload_id = self.upload()
        with ThreadPoolExecutor(max_workers=6) as pool:
            responses = list(pool.map(lambda _: self.finish(upload_id), range(6)))
        self.assertTrue(all(item.status_code == 200 for item in responses), [item.text for item in responses])
        ids = {item.json()['id'] for item in responses}
        self.assertEqual(len(ids), 1)
        archive_id = ids.pop()
        duplicate_id = self.start({**self.metadata, 'requestId': 'local-upload:second-key'})
        self.client.put(f'/api/v1/report-archives/uploads/{duplicate_id}/content', content=self.pdf,
            headers={**self.headers, 'Content-Type': 'application/pdf'})
        self.assertEqual(self.finish(duplicate_id).json()['id'], archive_id)
        data = self.client.get(f'/api/v1/report-archives/{archive_id}/content', headers=self.headers)
        self.assertEqual(data.content, self.pdf)
        self.assertEqual(data.headers['x-report-artifact-sha256'], self.metadata['sha256'])
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger_before)
        self.assertEqual(self.sql('SELECT * FROM platform_report_usages'), usages_before)
        self.assertEqual(len(self.sql('SELECT * FROM platform_archive_imports')), 1)

    def test_idor_at_start_put_commit_detail_download_and_owner_override(self):
        upload_id = self.upload()
        self.assertEqual(self.finish(upload_id, self.other_headers).status_code, 404)
        self.assertEqual(self.client.put(f'/api/v1/report-archives/uploads/{upload_id}/content', content=self.pdf,
            headers={**self.other_headers, 'Content-Type': 'application/pdf'}).status_code, 404)
        archive_id = self.finish(upload_id).json()['id']
        for suffix in ('', '/content'):
            self.assertEqual(self.client.get(f'/api/v1/report-archives/{archive_id}{suffix}', headers=self.other_headers).status_code, 404)
        self.assertEqual(self.client.get('/api/v1/report-archives', headers=self.other_headers).json()['items'], [])
        self.assertEqual(self.client.post('/api/v1/report-archives', json={'uploadId': upload_id, 'userId': self.owner}, headers=self.other_headers).status_code, 422)

    def test_reused_request_id_with_changed_metadata_or_pdf_is_conflict(self):
        self.start()
        response = self.client.post('/api/v1/report-archives/uploads', json={**self.metadata, 'title': 'Changed'}, headers=self.headers)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(len(self.sql('SELECT * FROM platform_archive_uploads')), 1)

    def test_type_hash_size_page_and_active_content_validation(self):
        upload_id = self.start()
        path = f'/api/v1/report-archives/uploads/{upload_id}/content'
        for data, mime in ((self.pdf, 'text/html'), (self.pdf + b'x', 'application/pdf'), (b'not a pdf', 'application/pdf')):
            self.assertEqual(self.client.put(path, content=data, headers={**self.headers, 'Content-Type': mime}).status_code, 422)
        for name, pdf in (('active', self.make_pdf(active=True)), ('too-many-pages', self.make_pdf(pages=201))):
            metadata = {**self.metadata, 'requestId': f'local-upload:{name}', 'sha256': hashlib.sha256(pdf).hexdigest(), 'byteSize': len(pdf)}
            identifier = self.start(metadata)
            result = self.client.put(f'/api/v1/report-archives/uploads/{identifier}/content', content=pdf,
                headers={**self.headers, 'Content-Type': 'application/pdf'})
            self.assertEqual(result.status_code, 422, result.text)
        self.assertEqual(self.finish(upload_id).status_code, 409)

    def test_expired_partial_upload_restarts_without_new_key(self):
        upload_id = self.start()
        self.sql("UPDATE platform_archive_uploads SET expires_at=NOW()-INTERVAL '1 second' WHERE id=%s RETURNING id", (upload_id,))
        self.assertEqual(self.finish(upload_id).status_code, 410)
        self.assertEqual(self.start(), upload_id)
        self.assertEqual(self.finish(upload_id).status_code, 409)

    def test_owner_quota_covers_partial_reservations(self):
        for index in range(10):
            self.start({**self.metadata, 'requestId': f'local-upload:queued-{index}'})
        response = self.client.post('/api/v1/report-archives/uploads', json={**self.metadata, 'requestId': 'local-upload:over-quota'}, headers=self.headers)
        self.assertEqual(response.status_code, 413)

    def test_bad_bearer_never_falls_back_to_cookie(self):
        response = self.client.get('/api/v1/report-archives', headers={'Authorization': 'Bearer wrong', 'Cookie': 'bl_session=anything'})
        self.assertEqual(response.status_code, 401)

    def test_oauth_callback_query_is_private_to_handler_not_access_log_scope(self):
        outer = {'type': 'http', 'path': '/api/mobile/v1/auth/oauth/callback/kakao',
                 'query_string': b'code=synthetic-secret&state=synthetic-state', 'headers': []}
        seen = []
        async def app(scope, receive, send):
            seen.append(scope['query_string'])
        asyncio.run(RequestBodyLimitMiddleware(app)(outer, None, None))
        self.assertEqual(outer['query_string'], b'')
        self.assertEqual(seen, [b'code=synthetic-secret&state=synthetic-state'])

    def test_cookie_writes_require_allowed_origin_and_non_simple_header(self):
        for headers in ({'Cookie': 'bl_session=test-cookie-session'},
                        {'Cookie': 'bl_session=test-cookie-session', 'Origin': 'https://evil.test', 'X-Archive-Request': '1'},
                        {'Cookie': 'bl_session=test-cookie-session', 'Origin': 'https://building-land.com'}):
            response = self.client.post('/api/v1/report-archives/uploads', headers=headers, json=self.metadata)
            self.assertEqual(response.status_code, 403)

    def test_cookie_and_bearer_share_same_archive_id_and_auth_version_revokes_cookie(self):
        archive_id = self.finish(self.upload()).json()['id']
        self.sql("INSERT INTO platform_external_accounts(namespace,external_id,user_id) VALUES ('web','123',%s) RETURNING user_id", (self.owner,))
        class FakeResponse:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size): return json.dumps({'externalId': '123', 'authenticatedAt': 1, 'authVersion': 0}).encode()
        with patch('app.platform.shared_session.urllib.request.build_opener') as opener:
            opener.return_value.open.return_value = FakeResponse()
            response = self.client.get('/api/v1/report-archives', headers={'Cookie': 'bl_session=test-cookie-session'})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['items'][0]['id'], archive_id)
            self.sql('UPDATE platform_users SET auth_version=1 WHERE id=%s RETURNING id', (self.owner,))
            self.assertEqual(self.client.get('/api/v1/report-archives', headers={'Cookie': 'bl_session=test-cookie-session'}).status_code, 409)

    def test_web_snapshot_retries_use_common_catalog_without_debit_and_remain_inert(self):
        from app.platform import archives
        principal = {'user_id': self.owner, 'auth_type': 'cookie'}
        payload = {'requestId': 'web-snapshot:test', 'snapshot': {'title': 'Legacy', 'address': '', 'pages': ['<p>Saved</p>']},
                   'html': '<html><script>window.top.location="https://example.test"</script><p>Saved</p></html>'}
        before = self.sql('SELECT id,delta,user_id FROM platform_credit_ledger ORDER BY id')
        with patch('app.platform.archive_routes.shared_session', return_value=principal):
            first = self.client.post('/api/v1/report-archives', json=payload)
            second = self.client.post('/api/v1/report-archives', json=payload)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()['id'], second.json()['id'])
        archive_id = first.json()['id']
        content = self.client.get(f'/api/v1/report-archives/{archive_id}/content?format=html', headers=self.headers)
        self.assertEqual(content.text, payload['html'])
        self.assertIn('sandbox', content.headers['content-security-policy'])
        self.assertIn('attachment', content.headers['content-disposition'])
        self.assertEqual(self.client.get(f'/api/v1/report-archives/{archive_id}', headers=self.other_headers).status_code, 404)
        self.assertEqual(before, self.sql('SELECT id,delta,user_id FROM platform_credit_ledger ORDER BY id'))
        with patch.object(archives, 'OWNER_QUOTA', len(self.pdf) + 1):
            response = self.client.post('/api/v1/report-archives/uploads', json=self.metadata, headers=self.headers)
            self.assertEqual(response.status_code, 413)

    def test_disconnected_identity_cannot_register_another_web_account(self):
        self.sql('UPDATE platform_identities SET is_active=FALSE WHERE user_id=%s RETURNING id', (self.owner,))
        response = self.client.post('/api/internal/v1/web/accounts/resolve',
            headers={'X-Internal-Service-Token': self.settings.internal_service_token},
            json={'externalId': 'new-web', 'provider': 'kakao', 'providerSubject': 'owner',
                  'providerClientId': 'test-client', 'registrationConfirmed': True})
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.sql("SELECT * FROM platform_external_accounts WHERE external_id='new-web'"), [])
        self.assertEqual(len(self.sql('SELECT * FROM platform_account_review_queue')), 1)

    def email_registration(self, external_id='email-new', **changes):
        payload = {'externalId': external_id, 'provider': 'web_email', 'providerSubject': f'web:{external_id}',
                   'email': 'same@example.test', 'emailVerified': True, 'registrationConfirmed': True,
                   'legacyPaidRemaining': 0, **changes}
        return self.client.post('/api/internal/v1/web/accounts/resolve',
            headers={'X-Internal-Service-Token': self.settings.internal_service_token}, json=payload)

    def test_web_email_registration_is_concurrent_idempotent_and_never_merges_by_email(self):
        original = self.sql('SELECT * FROM platform_credit_ledger WHERE user_id=%s ORDER BY id', (self.owner,))
        with ThreadPoolExecutor(max_workers=5) as pool:
            responses = list(pool.map(lambda _: self.email_registration(), range(5)))
        for response in responses:
            self.assertEqual(response.status_code, 200, response.text)
        owners = {response.json()['userId'] for response in responses}
        self.assertEqual(len(owners), 1)
        owner = owners.pop()
        self.assertNotIn(owner, (self.owner, self.other))
        self.assertEqual(self.sql('SELECT delta FROM platform_credit_ledger WHERE user_id=%s', (owner,)), [{'delta': 3}])
        identity = self.sql('SELECT provider,provider_subject,provider_email_verified FROM platform_identities WHERE user_id=%s', (owner,))[0]
        self.assertEqual(identity, {'provider': 'web_email', 'provider_subject': 'web:email-new', 'provider_email_verified': True})
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger WHERE user_id=%s ORDER BY id', (self.owner,)), original)
        other = self.email_registration('email-second')
        self.assertEqual(other.status_code, 200, other.text)
        self.assertNotEqual(other.json()['userId'], owner)

    def test_email_proof_subject_and_legacy_value_fail_closed(self):
        ledger = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
        for changes in ({'emailVerified': False}, {'providerSubject': 'web:another-owner'}, {'legacyPaidRemaining': 4}):
            response = self.email_registration(**changes)
            self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger)
        self.assertEqual(self.sql("SELECT * FROM platform_external_accounts WHERE external_id='email-new'"), [])

    def test_legacy_email_cannot_claim_a_second_initial_grant_in_either_flag_mode(self):
        ledger = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
        with patch('app.platform.routes._settings', return_value=replace(self.settings, shared_archives_enabled=False)):
            off = self.email_registration('legacy-off', registrationConfirmed=False, legacyRegistration=True)
        self.assertEqual(off.status_code, 409, off.text)
        on = self.email_registration('legacy-on', registrationConfirmed=False, legacyRegistration=True)
        self.assertEqual(on.status_code, 409, on.text)
        self.assertEqual(self.sql("SELECT * FROM platform_external_accounts WHERE external_id IN ('legacy-on','legacy-off')"), [])
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger)

    def test_registered_email_does_not_move_existing_provider_or_archive_ownership(self):
        email_owner = self.email_registration().json()['userId']
        archive_id = self.finish(self.upload()).json()['id']
        ledger = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
        response = self.client.post('/api/internal/v1/web/accounts/resolve',
            headers={'X-Internal-Service-Token': self.settings.internal_service_token},
            json={'externalId': 'email-new', 'provider': 'kakao', 'providerSubject': 'owner',
                  'providerClientId': 'test-client', 'explicitLink': True})
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(str(self.sql("SELECT user_id FROM platform_external_accounts WHERE external_id='email-new'")[0]['user_id']), email_owner)
        self.assertEqual(self.client.get(f'/api/v1/report-archives/{archive_id}', headers=self.headers).status_code, 200)
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger)

    def test_actual_web_email_registration_payload_reaches_central_identity_and_replays(self):
        web_root = Path(os.environ['SHARED_WEB_REPO'])
        sys.path.insert(0, str(web_root))
        spec = importlib.util.spec_from_file_location('email_web_contract', web_root / 'server.py')
        web = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(web)
        def resolve(payload):
            response = self.client.post('/api/internal/v1/web/accounts/resolve',
                headers={'X-Internal-Service-Token': self.settings.internal_service_token}, json=payload)
            if response.status_code != 200:
                raise web.platform_ledger.PlatformAccountError(response.json()['detail'], response.status_code)
            return response.json()
        try:
            with tempfile.TemporaryDirectory() as temporary, \
                 patch.object(web.platform_ledger, 'enabled', return_value=True), \
                 patch.object(web.platform_ledger, 'resolve_account', side_effect=resolve), \
                 patch.object(web.platform_ledger, 'credit_summary', return_value={'freeRemaining': 3, 'paidRemaining': 0}):
                web.DB_PATH = Path(temporary) / 'email.sqlite3'
                web.init_auth_db()
                user = web.create_user('same@example.test', 'Email', 'email', web.hash_password('Synthetic-password-123'), email_verified_at=web.now_iso())
                for flag in (False, True):
                    with patch.object(web, 'SHARED_ARCHIVES_ENABLED', flag), \
                         patch.object(web.platform_ledger, 'account_profile', return_value={'socialAccounts': [{'provider': 'web_email'}]}):
                        result = web.build_me_payload(user['id'])
                        owner = result['user']['platformUserId']
                        self.assertNotIn(owner, (self.owner, self.other))
                        self.assertEqual(str(self.sql("SELECT user_id FROM platform_external_accounts WHERE external_id=%s", (str(user['id']),))[0]['user_id']), owner)
                self.assertEqual(self.sql('SELECT delta FROM platform_credit_ledger WHERE user_id=%s', (owner,)), [{'delta': 3}])
        finally:
            sys.path.remove(str(web_root))

    def test_normal_final_is_one_debit_and_visible_through_legacy_and_common_routes(self):
        report = {'schemaVersion': 1, 'rendererVersion': 'web-a4-canonical-v1', 'mappingVersion': 'mobile-v1',
            'title': 'Final', 'address': 'Synthetic', 'includedItems': ['cover'], 'officeInfo': {}, 'reportTheme': 'navy',
            'pages': [{'pageKey': 'cover', 'layout': 'cover', 'title': 'Final', 'footerTitle': 'Final', 'address': 'Synthetic'}]}
        payload = {'requestId': 'final-shared-test-1', 'report': report}
        with patch('app.platform.routes.render_pdf', return_value=self.pdf):
            first = self.client.post('/api/mobile/v1/reports/final', headers=self.headers, json=payload)
            second = self.client.post('/api/v1/reports/final', headers=self.headers, json=payload)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        archive_id = first.headers['x-report-archive-id']
        self.assertEqual(first.headers['x-report-owner-id'], self.owner)
        self.assertEqual(second.headers['x-report-archive-id'], archive_id)
        for path in ('/api/report-archive', '/api/v1/report-archives'):
            self.assertEqual(self.client.get(path, headers=self.headers).json()['items'][0]['id'], archive_id)
        self.assertEqual(len(self.sql('SELECT * FROM platform_credit_ledger WHERE user_id=%s AND delta<0', (self.owner,))), 1)

    def test_link_waits_for_pkce_and_original_session_and_never_grants(self):
        verifier = 'v' * 64
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
        started = self.client.post('/api/mobile/v1/auth/oauth/start', headers=self.headers,
            json={'provider': 'naver', 'codeChallenge': challenge, 'redirectUri': 'buildingland://oauth/callback', 'linkAccount': True})
        self.assertEqual(started.status_code, 200, started.text)
        with patch('app.platform.oauth.fetch_provider_identity', return_value=ProviderIdentity('naver-subject', 'same@example.test', 'Name')):
            callback = self.client.get(f"/api/mobile/v1/auth/oauth/callback/naver?state={started.json()['state']}&code=synthetic")
        self.assertEqual(callback.status_code, 302, callback.text)
        self.assertEqual(self.sql("SELECT * FROM platform_identities WHERE provider='naver'"), [])
        code = parse_qs(urlparse(callback.headers['location']).query)['code'][0]
        payload = {'code': code, 'codeVerifier': verifier, 'deviceId': 'device-link-test-00000'}
        self.assertEqual(self.client.post('/api/mobile/v1/auth/token', headers=self.other_headers, json=payload).status_code, 403)
        linked = self.client.post('/api/mobile/v1/auth/token', headers=self.headers, json=payload)
        self.assertEqual(linked.status_code, 200, linked.text)
        self.assertEqual(linked.json()['user']['id'], self.owner)
        self.assertEqual(len(self.sql('SELECT * FROM platform_credit_ledger WHERE user_id=%s', (self.owner,))), 1)
        self.assertEqual(self.client.post('/api/mobile/v1/auth/token', headers=self.headers, json=payload).status_code, 401)

    def test_last_identity_disconnect_and_paid_withdrawal_are_blocked(self):
        self.assertEqual(self.client.post('/api/v1/account/identities/kakao/disconnect', headers=self.headers).status_code, 409)
        self.sql('UPDATE platform_users SET paid_remaining=1 WHERE id=%s RETURNING id', (self.owner,))
        response = self.client.post('/api/v1/account/withdraw', headers=self.headers, json={'confirm': 'withdraw-account'})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.sql('SELECT status FROM platform_users WHERE id=%s', (self.owner,))[0]['status'], 'active')

    def test_withdrawal_keeps_archive_and_ledger_and_revokes_all_auth(self):
        self.finish(self.upload())
        before = self.sql('SELECT * FROM platform_archive_imports ORDER BY id')
        ledger = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
        response = self.client.post('/api/v1/account/withdraw', headers=self.headers, json={'confirm': 'withdraw-account'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.client.get('/api/v1/report-archives', headers=self.headers).status_code, 401)
        self.assertEqual(self.sql('SELECT * FROM platform_archive_imports ORDER BY id'), before)
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger)

    def test_ambiguous_web_mapping_goes_to_review_without_balance_migration(self):
        headers = {'X-Internal-Service-Token': self.settings.internal_service_token}
        result = self.client.post('/api/internal/v1/web/accounts/resolve', headers=headers, json={
            'externalId': 'old-web-user', 'provider': 'kakao', 'providerSubject': 'owner',
            'providerClientId': 'test-client', 'legacyPaidRemaining': 30})
        self.assertEqual(result.status_code, 409, result.text)
        self.assertEqual(self.sql('SELECT * FROM platform_external_accounts'), [])
        self.assertEqual(len(self.sql('SELECT * FROM platform_account_review_queue')), 1)
        self.assertEqual(len(self.sql('SELECT * FROM platform_credit_ledger')), 2)

    def test_migration_dry_run_replay_and_rollback_preserve_source_owners_and_ledger(self):
        spec = importlib.util.spec_from_file_location('archive_migration_contract', Path(__file__).resolve().parents[2] / 'scripts/archive_migration.py')
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'web-backup.sqlite3'
            with sqlite3.connect(source) as sqlite:
                sqlite.executescript('''CREATE TABLE users(id INTEGER PRIMARY KEY, withdrawn_at TEXT);
                    CREATE TABLE platform_account_links(local_user_id INTEGER,platform_user_id TEXT);
                    CREATE TABLE report_archives(id TEXT,user_id INTEGER,title TEXT,address TEXT,
                        json_content TEXT,html_content TEXT,saved_at TEXT,deleted_at TEXT);''')
                sqlite.executemany('INSERT INTO users VALUES (?,NULL)', [(1,), (2,)])
                sqlite.execute('INSERT INTO platform_account_links VALUES (1,?)', (self.owner,))
                for index in (1,2):
                    sqlite.execute('INSERT INTO report_archives VALUES (?,?,?,?,?,?,?,NULL)',
                        (f'legacy-{index}',index,'Legacy','Synthetic',json.dumps({'title':'Legacy','pages':['<p>report</p>']}),'<p>report</p>','2026-09-01T00:00:00Z'))
            sqlite.close()
            self.sql("INSERT INTO platform_external_accounts(namespace,external_id,user_id) VALUES ('web','1',%s) RETURNING user_id", (self.owner,))
            original = source.read_bytes()
            ledger = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
            with connect(self.settings) as connection:
                connection.execute('SET TRANSACTION READ ONLY')
                plan = migration.prepare_plan(source, connection)
            self.assertEqual(len(plan['ready']), 1)
            self.assertEqual(len(plan['review']), 1)
            self.assertEqual(self.sql('SELECT * FROM platform_archive_imports'), [])
            with connect(self.settings) as connection:
                result = migration.apply_plan(source, connection, plan, 'migration-test-1')
            self.assertEqual(result['inserted'], 1)
            with connect(self.settings) as connection:
                self.assertEqual(migration.apply_plan(source, connection, plan, 'migration-test-1')['inserted'], 0)
                self.assertEqual(migration.rollback(connection, 'migration-test-1', apply=False)['importsToHide'], 1)
            self.assertEqual(len(self.client.get('/api/v1/report-archives', headers=self.headers).json()['items']), 1)
            with connect(self.settings) as connection:
                migration.rollback(connection, 'migration-test-1', apply=True)
            self.assertEqual(self.client.get('/api/v1/report-archives', headers=self.headers).json()['items'], [])
            self.assertEqual(source.read_bytes(), original)
            self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger)
            self.assertEqual(str(self.sql('SELECT user_id FROM platform_archive_imports')[0]['user_id']), self.owner)

    def test_real_naver_web_cookie_and_mobile_round_trip_without_debit_and_reject_other_or_expired_sessions(self):
        self.owner, self.headers = self.user('same-naver-subject', provider='naver')
        ledger_before = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
        usages_before = self.sql('SELECT * FROM platform_report_usages')
        web_root = Path(os.environ['SHARED_WEB_REPO'])
        sys.path.insert(0, str(web_root))
        spec = importlib.util.spec_from_file_location('shared_web_contract', web_root / 'server.py')
        web = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(web)
        archive_id = self.finish(self.upload()).json()['id']
        previous_url = os.environ.get('WEB_SESSION_INTROSPECTION_URL')
        with tempfile.TemporaryDirectory() as temporary:
            web.DB_PATH = Path(temporary) / 'web.sqlite3'
            web.MOBILE_REPORT_INTERNAL_TOKEN = self.settings.internal_service_token
            web.init_auth_db()
            user = web.create_user('synthetic@example.test', 'Synthetic', 'email', web.hash_password('Synthetic-test-only-password'))
            web_user_id = int(user['id'])
            with web.db_connect() as connection:
                connection.execute('INSERT INTO sessions(id,user_id,created_at,expires_at,platform_auth_version) VALUES (?,?,?,?,0)',
                    ('synthetic-web-session-1234567890', web_user_id, web.unix_now(), web.unix_now()+3600))
            resolved = self.client.post('/api/internal/v1/web/accounts/resolve',
                headers={'X-Internal-Service-Token': self.settings.internal_service_token},
                json={'externalId': str(web_user_id), 'provider': 'naver', 'providerSubject': 'same-naver-subject',
                      'providerClientId': 'test-client', 'email': 'same@example.test', 'displayName': 'Synthetic',
                      'registrationConfirmed': True, 'legacyPaidRemaining': 0})
            self.assertEqual(resolved.status_code, 200, resolved.text)
            self.assertEqual(resolved.json()['userId'], self.owner)
            server = web.ThreadingHTTPServer(('127.0.0.1',0), web.Handler)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                os.environ['WEB_SESSION_INTROSPECTION_URL'] = f'http://127.0.0.1:{server.server_port}/api/internal/platform-session'
                get_settings.cache_clear()
                cookie = {'Cookie': 'bl_session=synthetic-web-session-1234567890',
                          'Origin': 'https://building-land.com', 'X-Archive-Request': '1'}
                response = self.client.get('/api/v1/report-archives', headers=cookie)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['items'][0]['id'], archive_id)
                self.assertEqual(self.client.get(f'/api/v1/report-archives/{archive_id}/content', headers=cookie).content, self.pdf)
                snapshot = self.client.post('/api/v1/report-archives', headers=cookie,
                    json={'requestId': 'web-naver-round-trip', 'snapshot': {'title': 'Web snapshot',
                          'address': 'Synthetic', 'pages': ['<div>Saved web report</div>']},
                          'html': '<html><body>Saved web report</body></html>'})
                self.assertEqual(snapshot.status_code, 200, snapshot.text)
                web_id = snapshot.json()['id']
                ids = {item['id'] for item in self.client.get('/api/v1/report-archives', headers=self.headers).json()['items']}
                self.assertEqual(ids, {archive_id, web_id})
                mobile_html = self.client.get(f'/api/v1/report-archives/{web_id}/content?format=html', headers=self.headers)
                self.assertEqual(mobile_html.status_code, 200)
                self.assertIn(b'Saved web report', mobile_html.content)
                self.assertEqual(self.client.get('/api/v1/report-archives', headers=self.other_headers).json()['items'], [])
                for item_id, content_format in ((archive_id, 'pdf'), (web_id, 'html')):
                    for method, path in [('GET', f'/api/v1/report-archives/{item_id}'),
                                         ('GET', f'/api/v1/report-archives/{item_id}/content?format={content_format}'),
                                         ('DELETE', f'/api/v1/report-archives/{item_id}')]:
                        self.assertEqual(self.client.request(method, path, headers=self.other_headers).status_code, 404)
                        self.assertEqual(self.client.request(method, path).status_code, 401)
                self.assertEqual(self.client.delete(f'/api/v1/report-archives/{archive_id}', headers=cookie).status_code, 200)
                self.assertEqual(self.client.get(f'/api/v1/report-archives/{archive_id}', headers=self.headers).status_code, 404)
                with web.db_connect() as connection:
                    connection.execute('UPDATE sessions SET expires_at=?', (web.unix_now()-1,))
                for method, path in [('GET', '/api/v1/report-archives'), ('GET', f'/api/v1/report-archives/{web_id}'),
                                     ('GET', f'/api/v1/report-archives/{web_id}/content?format=html'),
                                     ('DELETE', f'/api/v1/report-archives/{web_id}')]:
                    self.assertEqual(self.client.request(method, path, headers=cookie).status_code, 401)
                with web.db_connect() as connection:
                    connection.execute('UPDATE sessions SET expires_at=?,revoked_at=?', (web.unix_now()+3600, web.unix_now()))
                self.assertEqual(self.client.get('/api/v1/report-archives', headers=cookie).status_code, 401)
                self.sql("UPDATE mobile_access_tokens SET expires_at=NOW()-INTERVAL '1 second' WHERE user_id=%s RETURNING user_id", (self.owner,))
                for path in ('/api/v1/report-archives', f'/api/v1/report-archives/{web_id}/content?format=html'):
                    self.assertEqual(self.client.get(path, headers=self.headers).status_code, 401)
                self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger_before)
                self.assertEqual(self.sql('SELECT * FROM platform_report_usages'), usages_before)
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=3)
                if previous_url is None:
                    os.environ.pop('WEB_SESSION_INTROSPECTION_URL', None)
                else:
                    os.environ['WEB_SESSION_INTROSPECTION_URL'] = previous_url
                get_settings.cache_clear()
                sys.path.remove(str(web_root))

    def test_existing_unmapped_naver_account_and_different_client_scope_require_review(self):
        owner, _ = self.user('existing-naver-subject', provider='naver')
        ledger = self.sql('SELECT * FROM platform_credit_ledger ORDER BY id')
        payload = {'externalId': 'existing-web-naver', 'provider': 'naver',
                   'providerSubject': 'existing-naver-subject', 'providerClientId': 'test-client',
                   'email': 'same@example.test', 'displayName': 'Same name', 'legacyPaidRemaining': 0}
        for changes, reason in (({}, 'existing_web_account_requires_reviewed_mapping'),
                                ({'registrationConfirmed': True, 'providerClientId': 'another-client'}, 'provider_scope_mismatch')):
            response = self.client.post('/api/internal/v1/web/accounts/resolve',
                headers={'X-Internal-Service-Token': self.settings.internal_service_token}, json={**payload, **changes})
            self.assertEqual(response.status_code, 409, response.text)
            self.assertEqual(response.json()['detail'], reason)
        self.assertEqual(self.sql("SELECT * FROM platform_external_accounts WHERE external_id='existing-web-naver'"), [])
        self.assertEqual(str(self.sql("SELECT user_id FROM platform_identities WHERE provider='naver' AND provider_subject='existing-naver-subject'")[0]['user_id']), owner)
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger ORDER BY id'), ledger)


if __name__ == '__main__':
    unittest.main()
