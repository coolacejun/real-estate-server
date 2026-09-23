"""Inspect or change one reviewed Naver account's report-test grant.

Run only against a verified deployment after migration 015 and matching API/web
code are live. Identity inputs come from independently checked auth records.
Without --apply, grant/revoke are read-only dry runs. Never prints credentials,
provider subjects, email addresses or a connection URL.
"""
from __future__ import annotations

import argparse
import hmac
import json
import os
import sys
import uuid

import psycopg
from psycopg.rows import dict_row


def required(name: str) -> str:
    value = os.environ.get(name, '').strip()
    if not value:
        raise ValueError(f'{name} is required')
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'grant', 'revoke'))
    parser.add_argument('--apply', action='store_true', help='Commit grant/revoke after exact identity checks')
    args = parser.parse_args()
    try:
        database_url = required('DATABASE_URL')
        subject = required('REPORT_TEST_NAVER_SUBJECT')
        web_external_id = required('REPORT_TEST_WEB_EXTERNAL_ID')
        user_id = str(uuid.UUID(required('REPORT_TEST_USER_ID')))
        expected_email = required('REPORT_TEST_EXPECTED_EMAIL').casefold()
        web_client_id = required('REPORT_TEST_WEB_NAVER_CLIENT_ID')
        api_client_id = os.environ.get('NAVER_OAUTH_CLIENT_ID', '').strip() or required('NAVER_CLIENT_ID')
        if not hmac.compare_digest(web_client_id, api_client_id):
            raise ValueError('Web and API Naver client scopes differ')
        actor = required('REPORT_TEST_ACTOR') if args.apply else None
        if actor and len(actor) > 120:
            raise ValueError('Actor label is too long')
        if not database_url.startswith(('postgresql://', 'postgres://')):
            raise ValueError('A PostgreSQL DATABASE_URL is required')
        with psycopg.connect(database_url, row_factory=dict_row) as connection:
            with connection.transaction():
                marker = connection.execute(
                    "SELECT 1 FROM schema_migrations WHERE version='015_report_test_grants'"
                ).fetchone()
                if marker is None:
                    raise ValueError('Migration 015 is not applied')
                user = connection.execute(
                    """SELECT id,email,free_remaining,paid_remaining FROM platform_users
                       WHERE id=%s AND status='active' FOR UPDATE""", (user_id,)
                ).fetchone()
                if user is None:
                    raise ValueError('The expected active central user was not found')
                identity = connection.execute(
                    """SELECT id,user_id,provider_email,is_active FROM platform_identities
                       WHERE provider='naver' AND provider_subject=%s""", (subject,)
                ).fetchone()
                mapping = connection.execute(
                    """SELECT user_id FROM platform_external_accounts
                       WHERE namespace='web' AND external_id=%s""", (web_external_id,)
                ).fetchone()
                if identity is None or str(identity['user_id']) != user_id:
                    raise ValueError('Naver subject is not bound to the expected central user')
                if mapping is None or str(mapping['user_id']) != user_id:
                    raise ValueError('Verified web account is not bound to the same central user')
                if args.action == 'grant' and not identity['is_active']:
                    raise ValueError('Naver identity is disconnected')
                emails = [str(value or '').casefold() for value in (user['email'], identity['provider_email'])]
                if expected_email not in emails:
                    raise ValueError('Email clue does not match this verified identity')
                grant = connection.execute(
                    """SELECT naver_identity_id,active FROM platform_report_test_grants
                       WHERE user_id=%s FOR UPDATE""", (user_id,)
                ).fetchone()
                if grant is not None and str(grant['naver_identity_id']) != str(identity['id']):
                    raise ValueError('An existing grant is bound to another identity')
                before_balance = (int(user['free_remaining']), int(user['paid_remaining']))
                before_ledger = connection.execute(
                    'SELECT count(*) AS count FROM platform_credit_ledger WHERE user_id=%s', (user_id,)
                ).fetchone()['count']
                changed = ((args.action == 'grant' and (grant is None or not grant['active'])) or
                           (args.action == 'revoke' and grant is not None and grant['active']))
                if args.apply and changed:
                    if args.action == 'grant':
                        connection.execute(
                            """INSERT INTO platform_report_test_grants
                                 (user_id,naver_identity_id,active,granted_by)
                               VALUES (%s,%s,TRUE,%s)
                               ON CONFLICT(user_id) DO UPDATE SET
                                 active=TRUE,granted_by=EXCLUDED.granted_by,
                                 granted_at=NOW(),revoked_by=NULL,revoked_at=NULL,updated_at=NOW()""",
                            (user_id, identity['id'], actor),
                        )
                    else:
                        connection.execute(
                            """UPDATE platform_report_test_grants SET active=FALSE,
                                 revoked_by=%s,revoked_at=NOW(),updated_at=NOW()
                               WHERE user_id=%s AND active""", (actor, user_id),
                        )
                    connection.execute(
                        """INSERT INTO platform_report_test_grant_events
                             (id,user_id,action,actor) VALUES (%s,%s,%s,%s)""",
                        (str(uuid.uuid4()), user_id, args.action, actor),
                    )
                after = connection.execute(
                    """SELECT active FROM platform_report_test_grants WHERE user_id=%s""",
                    (user_id,),
                ).fetchone()
                balances = connection.execute(
                    'SELECT free_remaining,paid_remaining FROM platform_users WHERE id=%s', (user_id,)
                ).fetchone()
                after_ledger = connection.execute(
                    'SELECT count(*) AS count FROM platform_credit_ledger WHERE user_id=%s', (user_id,)
                ).fetchone()['count']
                if before_balance != (int(balances['free_remaining']), int(balances['paid_remaining'])) or before_ledger != after_ledger:
                    raise ValueError('Unexpected balance or ledger change; transaction rolled back')
                result = {'identityAndWebMappingVerified': True,
                          'action': args.action, 'wouldChange': changed,
                          'applied': args.apply and changed,
                          'active': bool(after and after['active']),
                          'balancesAndLedgerUnchanged': True}
        print(json.dumps(result))
        return 0
    except (ValueError, psycopg.Error) as error:
        detail = str(error) if isinstance(error, ValueError) else 'PostgreSQL operation failed'
        print(f'BLOCKED: {detail}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
