"""Synthetic-account contracts for a revocable, zero-debit report grant."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import sys
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import psycopg
from fastapi import HTTPException
from psycopg.rows import dict_row

from app.platform.config import get_settings
from app.platform.reports import CanonicalReport, begin_final_usage, complete_final_usage, fail_final_usage
from app.platform.repository import profile_payload


MANAGER_PATH = Path(__file__).resolve().parents[2] / 'scripts/manage_report_test_grant.py'
SPEC = importlib.util.spec_from_file_location('manage_report_test_grant', MANAGER_PATH)
manager = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(manager)


class ReportTestGrantContracts(unittest.TestCase):
    provider = "naver"
    def setUp(self) -> None:
        self.database_url = os.environ['DATABASE_URL']
        self.assertEqual(os.environ.get('APP_ENV'), 'test')
        self.assertIn('127.0.0.1', self.database_url)
        self.owner = str(uuid.uuid4())
        self.other = str(uuid.uuid4())
        self.identity = str(uuid.uuid4())
        with psycopg.connect(self.database_url) as connection:
            connection.execute('TRUNCATE platform_users CASCADE')
            for user_id in (self.owner, self.other):
                connection.execute(
                    """INSERT INTO platform_users
                         (id,email,display_name,free_remaining,paid_remaining)
                       VALUES (%s,'same@example.test','Synthetic',0,0)""", (user_id,),
                )
            connection.execute(
                """INSERT INTO platform_identities
                     (id,user_id,provider,provider_subject,provider_email,provider_email_verified)
                   VALUES (%s,%s,%s,'synthetic-subject-owner','same@example.test',TRUE)""",
                (self.identity, self.owner, self.provider),
            )
            connection.execute(
                """INSERT INTO platform_identities
                     (id,user_id,provider,provider_subject,provider_email,provider_email_verified)
                   VALUES (%s,%s,%s,'synthetic-subject-other','same@example.test',TRUE)""",
                (str(uuid.uuid4()), self.other, self.provider),
            )
            connection.execute(
                """INSERT INTO platform_external_accounts(namespace,external_id,user_id)
                   VALUES ('web','synthetic-web-owner',%s)""", (self.owner,),
            )
        self.settings = get_settings()
        self.canonical = CanonicalReport(
            report={'schemaVersion': 1, 'title': 'Synthetic'}, content_hash='a' * 64,
            renderer_profile='web-a4-v1', response_renderer_version='web-a4-v1',
        )

    def sql(self, query: str, params: tuple = ()):
        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            return [dict(row) for row in connection.execute(query, params).fetchall()]

    def manage(self, action: str, *, apply: bool = False, **overrides) -> int:
        environment = {
            'REPORT_TEST_PROVIDER': self.provider,
            f'REPORT_TEST_{self.provider.upper()}_SUBJECT': 'synthetic-subject-owner',
            'REPORT_TEST_WEB_EXTERNAL_ID': 'synthetic-web-owner',
            'REPORT_TEST_USER_ID': self.owner,
            'REPORT_TEST_EXPECTED_EMAIL': 'same@example.test',
            f'REPORT_TEST_WEB_{self.provider.upper()}_CLIENT_ID': 'test-client',
            f'{self.provider.upper()}_OAUTH_CLIENT_ID': 'test-client',
            'REPORT_TEST_ACTOR': 'synthetic-operator',
        }
        environment.update(overrides)
        with (
            patch.dict(os.environ, environment),
            patch.object(sys, 'argv', ['manage_report_test_grant.py', action, *(['--apply'] if apply else [])]),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            return manager.main()

    def test_verified_grant_no_debit_failure_retry_completion_and_revoke(self):
        self.assertEqual(self.manage('grant'), 0)  # dry run
        self.assertEqual(self.sql('SELECT * FROM platform_report_test_grants'), [])
        self.assertEqual(self.manage('grant', apply=True), 0)
        self.assertEqual(self.manage('grant', apply=True), 0)  # idempotent
        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            owner_profile = profile_payload(connection, self.owner)
            other_profile = profile_payload(connection, self.other)
        self.assertTrue(owner_profile['creditSummary']['reportTestAccess'])
        self.assertFalse(other_profile['creditSummary']['reportTestAccess'])
        self.assertEqual(owner_profile['creditSummary']['availableCredits'], 0)
        first = begin_final_usage(self.settings, user_id=self.owner,
                                  request_id='synthetic:test-grant:1', canonical=self.canonical)
        usage = first['usage']
        self.assertEqual(first['action'], 'render')
        self.assertEqual(usage['no_charge_reason'], 'test_report_grant')
        self.assertIsNone(usage['debit_bucket'])
        self.assertIsNone(usage['debit_ledger_id'])
        with self.assertRaises(HTTPException) as busy:
            begin_final_usage(self.settings, user_id=self.owner,
                              request_id='synthetic:test-grant:1', canonical=self.canonical)
        self.assertEqual(busy.exception.status_code, 409)
        fail_final_usage(self.settings, str(usage['id']), 'synthetic_failure')
        self.assertEqual(self.sql('SELECT status FROM platform_report_usages WHERE id=%s',
                                  (usage['id'],))[0]['status'], 'failed')
        retry = begin_final_usage(self.settings, user_id=self.owner,
                                  request_id='synthetic:test-grant:1', canonical=self.canonical)
        self.assertEqual(retry['usage']['attempt_count'], 2)
        archive_id = complete_final_usage(self.settings, usage=retry['usage'], canonical=self.canonical,
                                          stored_report=self.canonical.report, assets=[])
        done = begin_final_usage(self.settings, user_id=self.owner,
                                 request_id='synthetic:test-grant:1', canonical=self.canonical)
        self.assertEqual(done['action'], 'completed')
        self.assertEqual(str(done['usage']['archive_id']), archive_id)
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger'), [])
        self.assertEqual(self.manage('revoke', apply=True), 0)
        self.assertEqual(self.manage('revoke', apply=True), 0)
        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            self.assertFalse(profile_payload(connection, self.owner)['creditSummary']['reportTestAccess'])
        with self.assertRaises(HTTPException) as exhausted:
            begin_final_usage(self.settings, user_id=self.owner,
                              request_id='synthetic:test-grant:2', canonical=self.canonical)
        self.assertEqual(exhausted.exception.status_code, 402)
        self.assertEqual(len(self.sql('SELECT * FROM platform_report_test_grant_events')), 2)
        self.assertEqual(self.sql('SELECT free_remaining,paid_remaining FROM platform_users WHERE id=%s',
                                  (self.owner,))[0], {'free_remaining': 0, 'paid_remaining': 0})

    def test_wrong_scope_mapping_email_only_and_other_account_never_grant(self):
        self.assertEqual(self.manage('grant', apply=True,
                                    **{f'REPORT_TEST_WEB_{self.provider.upper()}_CLIENT_ID': 'another-client'}), 1)
        self.assertEqual(self.manage('grant', apply=True,
                                    REPORT_TEST_WEB_EXTERNAL_ID='not-mapped'), 1)
        self.assertEqual(self.manage('grant', apply=True,
                                    REPORT_TEST_USER_ID=self.other), 1)
        self.assertEqual(self.sql('SELECT * FROM platform_report_test_grants'), [])
        self.assertEqual(self.manage('grant', apply=True), 0)
        with self.assertRaises(HTTPException) as exhausted:
            begin_final_usage(self.settings, user_id=self.other,
                              request_id='synthetic:other:1', canonical=self.canonical)
        self.assertEqual(exhausted.exception.status_code, 402)
        with psycopg.connect(self.database_url) as connection:
            connection.execute('UPDATE platform_identities SET is_active=FALSE WHERE id=%s',
                               (self.identity,))
        with psycopg.connect(self.database_url, row_factory=dict_row) as connection:
            self.assertFalse(profile_payload(connection, self.owner)['creditSummary']['reportTestAccess'])
        with self.assertRaises(HTTPException) as disconnected:
            begin_final_usage(self.settings, user_id=self.owner,
                              request_id='synthetic:disconnected:1', canonical=self.canonical)
        self.assertEqual(disconnected.exception.status_code, 402)
        self.assertEqual(self.manage('revoke', apply=True), 0)

    def test_parallel_same_request_has_one_zero_debit_reservation(self):
        self.assertEqual(self.manage('grant', apply=True), 0)
        def reserve(_):
            try:
                return begin_final_usage(self.settings, user_id=self.owner,
                                         request_id='synthetic:parallel:1', canonical=self.canonical)['action']
            except HTTPException as error:
                return error.status_code
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(reserve, range(2)))
        self.assertCountEqual(outcomes, ['render', 409])
        self.assertEqual(len(self.sql('SELECT id FROM platform_report_usages')), 1)
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger'), [])


    def test_existing_balances_preserved_and_other_provider_cannot_substitute(self):
        other_provider = 'kakao' if self.provider == 'naver' else 'naver'
        with psycopg.connect(self.database_url) as connection:
            connection.execute('UPDATE platform_users SET free_remaining=2,paid_remaining=7 WHERE id=%s', (self.owner,))
            connection.execute(
                """INSERT INTO platform_identities (id,user_id,provider,provider_subject,provider_email)
                   VALUES (%s,%s,%s,'synthetic-subject-owner','same@example.test')""",
                (str(uuid.uuid4()), self.other, other_provider),
            )
        self.assertEqual(self.manage('grant', apply=True, REPORT_TEST_PROVIDER='google'), 1)
        self.assertEqual(self.manage('grant', apply=True, **{
            'REPORT_TEST_PROVIDER': other_provider,
            f'REPORT_TEST_{other_provider.upper()}_SUBJECT': 'synthetic-subject-owner',
            f'REPORT_TEST_WEB_{other_provider.upper()}_CLIENT_ID': 'test-client',
            f'{other_provider.upper()}_OAUTH_CLIENT_ID': 'test-client',
        }), 1)
        self.assertEqual(self.manage('grant', apply=True), 0)
        usage = begin_final_usage(self.settings, user_id=self.owner,
                                  request_id='synthetic:paid-preserved', canonical=self.canonical)['usage']
        self.assertEqual(usage['no_charge_reason'], 'test_report_grant')
        fail_final_usage(self.settings, str(usage['id']), 'synthetic_failure')
        self.assertEqual(self.sql('SELECT free_remaining,paid_remaining FROM platform_users WHERE id=%s',
                                  (self.owner,))[0], {'free_remaining': 2, 'paid_remaining': 7})
        self.assertEqual(self.sql('SELECT * FROM platform_credit_ledger'), [])


class KakaoReportTestGrantContracts(ReportTestGrantContracts):
    provider = "kakao"
