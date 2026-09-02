from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import psycopg
from fastapi import HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .config import PlatformSettings
from .security import random_token, sha256_text


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid.uuid4())


@contextmanager
def connect(settings: PlatformSettings) -> Iterator[psycopg.Connection]:
    if not settings.database_url:
        raise HTTPException(status_code=503, detail="account service is not configured")
    try:
        with psycopg.connect(settings.database_url, row_factory=dict_row) as connection:
            yield connection
    except HTTPException:
        raise
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="account service is temporarily unavailable") from exc


def assert_schema(connection: psycopg.Connection) -> None:
    try:
        row = connection.execute(
            "SELECT version FROM schema_migrations WHERE version = '009_mobile_platform'"
        ).fetchone()
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="account database migration is required") from exc
    if row is None:
        raise HTTPException(status_code=503, detail="account database migration is required")


def _insert_initial_free_grant(connection: psycopg.Connection, user_id: str) -> None:
    connection.execute(
        """
        INSERT INTO platform_credit_ledger
          (id, user_id, bucket, delta, reason, idempotency_key, reference_type, reference_id, balance_after)
        VALUES (%s, %s, 'free', 3, 'initial_account_grant', %s, 'user', %s, 3)
        ON CONFLICT (idempotency_key) DO NOTHING
        """,
        (new_id(), user_id, f"initial-free:{user_id}", user_id),
    )


def resolve_oauth_identity(
    connection: psycopg.Connection,
    *,
    provider: str,
    subject: str,
    email: str | None,
    display_name: str | None,
    link_user_id: str | None,
) -> str:
    lock_key = f"identity:{provider}:{subject}"
    connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))
    existing = connection.execute(
        "SELECT user_id FROM platform_identities WHERE provider = %s AND provider_subject = %s",
        (provider, subject),
    ).fetchone()
    if existing is not None:
        existing_user_id = str(existing["user_id"])
        if link_user_id and existing_user_id != link_user_id:
            raise HTTPException(status_code=409, detail="social account is linked to another user")
        connection.execute(
            """
            UPDATE platform_identities
            SET provider_email = %s, provider_display_name = %s, updated_at = NOW()
            WHERE provider = %s AND provider_subject = %s
            """,
            (email, display_name, provider, subject),
        )
        return existing_user_id

    if link_user_id:
        user = connection.execute(
            "SELECT id FROM platform_users WHERE id = %s AND status = 'active' FOR UPDATE",
            (link_user_id,),
        ).fetchone()
        if user is None:
            raise HTTPException(status_code=401, detail="login required for account linking")
        user_id = link_user_id
    else:
        user_id = new_id()
        connection.execute(
            """
            INSERT INTO platform_users (id, email, display_name, free_remaining, paid_remaining)
            VALUES (%s, %s, %s, 3, 0)
            """,
            (user_id, email, display_name),
        )
        _insert_initial_free_grant(connection, user_id)

    connection.execute(
        """
        INSERT INTO platform_identities
          (id, user_id, provider, provider_subject, provider_email, provider_display_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (new_id(), user_id, provider, subject, email, display_name),
    )
    connection.execute(
        """
        UPDATE platform_users
        SET email = COALESCE(email, %s), display_name = COALESCE(NULLIF(display_name, ''), %s), updated_at = NOW()
        WHERE id = %s
        """,
        (email, display_name, user_id),
    )
    return user_id


def profile_payload(connection: psycopg.Connection, user_id: str) -> dict[str, Any]:
    user = connection.execute(
        """
        SELECT id, email, display_name, free_remaining, paid_remaining
        FROM platform_users WHERE id = %s AND status = 'active'
        """,
        (user_id,),
    ).fetchone()
    if user is None:
        raise HTTPException(status_code=401, detail="login required")
    identities = connection.execute(
        """
        SELECT provider, provider_email, provider_display_name
        FROM platform_identities WHERE user_id = %s ORDER BY created_at
        """,
        (user_id,),
    ).fetchall()
    entitlements = connection.execute(
        """
        SELECT store, product_id, status, pricing_policy
        FROM platform_entitlements WHERE user_id = %s AND status = 'active' ORDER BY created_at
        """,
        (user_id,),
    ).fetchall()
    free = int(user["free_remaining"])
    paid = int(user["paid_remaining"])
    return {
        "authenticated": True,
        "user": {
            "id": str(user["id"]),
            "name": user["display_name"] or "",
            "displayName": user["display_name"] or "",
            "email": user["email"] or "",
        },
        "socialAccounts": [
            {
                "provider": row["provider"],
                "email": row["provider_email"] or "",
                "displayName": row["provider_display_name"] or "",
            }
            for row in identities
        ],
        "creditSummary": {
            "freeRemaining": free,
            "paidRemaining": paid,
            "availableCredits": free + paid,
        },
        "storeEntitlements": [
            {
                "store": row["store"],
                "productId": row["product_id"],
                "status": row["status"],
                "pricingPolicy": row["pricing_policy"],
            }
            for row in entitlements
        ],
    }


def issue_token_pair(
    connection: psycopg.Connection,
    settings: PlatformSettings,
    *,
    user_id: str,
    device_id: str,
    family_id: str | None = None,
    parent_token_hash: str | None = None,
) -> dict[str, str]:
    access_token = random_token("ma_", 32)
    refresh_token = random_token("mr_", 48)
    family = family_id or new_id()
    connection.execute(
        """
        INSERT INTO mobile_refresh_tokens
          (token_hash, family_id, user_id, device_id, parent_token_hash, expires_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            sha256_text(refresh_token),
            family,
            user_id,
            device_id,
            parent_token_hash,
            utcnow() + timedelta(seconds=settings.refresh_token_ttl_seconds),
        ),
    )
    connection.execute(
        """
        INSERT INTO mobile_access_tokens
          (token_hash, family_id, user_id, device_id, expires_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            sha256_text(access_token),
            family,
            user_id,
            device_id,
            utcnow() + timedelta(seconds=settings.access_token_ttl_seconds),
        ),
    )
    return {"accessToken": access_token, "refreshToken": refresh_token, "familyId": family}


def authenticate_access_token(connection: psycopg.Connection, raw_token: str) -> dict[str, str]:
    token_hash = sha256_text(raw_token)
    row = connection.execute(
        """
        SELECT token.user_id, token.device_id, token.family_id
        FROM mobile_access_tokens AS token
        JOIN platform_users AS users ON users.id = token.user_id
        WHERE token.token_hash = %s AND token.revoked_at IS NULL
          AND token.expires_at > NOW() AND users.status = 'active'
        """,
        (token_hash,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=401, detail="invalid or expired access token")
    return {
        "user_id": str(row["user_id"]),
        "device_id": str(row["device_id"]),
        "family_id": str(row["family_id"]),
    }


def rotate_refresh_token(
    connection: psycopg.Connection,
    settings: PlatformSettings,
    *,
    raw_token: str,
    device_id: str,
) -> tuple[dict[str, str], str]:
    token_hash = sha256_text(raw_token)
    row = connection.execute(
        "SELECT * FROM mobile_refresh_tokens WHERE token_hash = %s FOR UPDATE",
        (token_hash,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=401, detail="invalid refresh token")
    family_id = str(row["family_id"])
    invalid = (
        row["device_id"] != device_id
        or row["expires_at"] <= utcnow()
        or row["revoked_at"] is not None
        or row["rotated_at"] is not None
    )
    if invalid:
        connection.execute(
            """
            UPDATE mobile_refresh_tokens
            SET revoked_at = COALESCE(revoked_at, NOW()), revoke_reason = 'refresh_replay'
            WHERE family_id = %s
            """,
            (family_id,),
        )
        connection.execute(
            "UPDATE mobile_access_tokens SET revoked_at = COALESCE(revoked_at, NOW()) WHERE family_id = %s",
            (family_id,),
        )
        connection.commit()
        raise HTTPException(status_code=401, detail="refresh token replay detected")

    pair = issue_token_pair(
        connection,
        settings,
        user_id=str(row["user_id"]),
        device_id=device_id,
        family_id=family_id,
        parent_token_hash=token_hash,
    )
    replacement_hash = sha256_text(pair["refreshToken"])
    connection.execute(
        """
        UPDATE mobile_refresh_tokens
        SET rotated_at = NOW(), replacement_token_hash = %s, revoked_at = NOW(), revoke_reason = 'rotated'
        WHERE token_hash = %s
        """,
        (replacement_hash, token_hash),
    )
    connection.execute(
        """
        UPDATE mobile_access_tokens SET revoked_at = COALESCE(revoked_at, NOW())
        WHERE family_id = %s AND token_hash <> %s
        """,
        (family_id, sha256_text(pair["accessToken"])),
    )
    return pair, str(row["user_id"])


def revoke_token_family(connection: psycopg.Connection, family_id: str, reason: str) -> None:
    connection.execute(
        """
        UPDATE mobile_refresh_tokens
        SET revoked_at = COALESCE(revoked_at, NOW()), revoke_reason = COALESCE(revoke_reason, %s)
        WHERE family_id = %s
        """,
        (reason, family_id),
    )
    connection.execute(
        "UPDATE mobile_access_tokens SET revoked_at = COALESCE(revoked_at, NOW()) WHERE family_id = %s",
        (family_id,),
    )


def get_or_create_store_account(
    connection: psycopg.Connection, *, user_id: str, platform: str, device_id: str
) -> str:
    row = connection.execute(
        "SELECT account_token FROM mobile_store_accounts WHERE user_id = %s AND platform = %s FOR UPDATE",
        (user_id, platform),
    ).fetchone()
    if row is None:
        token = new_id()
        connection.execute(
            """
            INSERT INTO mobile_store_accounts (user_id, platform, account_token, last_device_id)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, platform, token, device_id),
        )
        return token
    connection.execute(
        """
        UPDATE mobile_store_accounts SET last_device_id = %s, updated_at = NOW()
        WHERE user_id = %s AND platform = %s
        """,
        (device_id, user_id, platform),
    )
    return str(row["account_token"])


def store_account_token(connection: psycopg.Connection, user_id: str, platform: str) -> str:
    row = connection.execute(
        "SELECT account_token FROM mobile_store_accounts WHERE user_id = %s AND platform = %s",
        (user_id, platform),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=409, detail="store catalog must be loaded before verification")
    return str(row["account_token"])


def grant_paid_credits(
    connection: psycopg.Connection,
    *,
    user_id: str,
    credits: int,
    reason: str,
    idempotency_key: str,
    reference_type: str,
    reference_id: str,
    metadata: dict[str, Any] | None = None,
) -> tuple[bool, int]:
    existing = connection.execute(
        "SELECT balance_after FROM platform_credit_ledger WHERE idempotency_key = %s",
        (idempotency_key,),
    ).fetchone()
    if existing is not None:
        return False, int(existing["balance_after"])
    user = connection.execute(
        "SELECT paid_remaining FROM platform_users WHERE id = %s AND status = 'active' FOR UPDATE",
        (user_id,),
    ).fetchone()
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    balance_after = int(user["paid_remaining"]) + credits
    connection.execute(
        "UPDATE platform_users SET paid_remaining = %s, updated_at = NOW() WHERE id = %s",
        (balance_after, user_id),
    )
    connection.execute(
        """
        INSERT INTO platform_credit_ledger
          (id, user_id, bucket, delta, reason, idempotency_key, reference_type, reference_id, balance_after, metadata)
        VALUES (%s, %s, 'paid', %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            new_id(), user_id, credits, reason, idempotency_key,
            reference_type, reference_id, balance_after, Jsonb(metadata or {}),
        ),
    )
    return True, balance_after


def revoke_paid_credits(
    connection: psycopg.Connection,
    *,
    user_id: str,
    credits: int,
    idempotency_key: str,
    reference_type: str,
    reference_id: str,
) -> tuple[bool, int]:
    existing = connection.execute(
        "SELECT balance_after FROM platform_credit_ledger WHERE idempotency_key = %s",
        (idempotency_key,),
    ).fetchone()
    if existing is not None:
        return False, int(existing["balance_after"])
    user = connection.execute(
        "SELECT paid_remaining FROM platform_users WHERE id = %s AND status = 'active' FOR UPDATE",
        (user_id,),
    ).fetchone()
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    if int(user["paid_remaining"]) < credits:
        raise HTTPException(status_code=409, detail="paid credits have already been used")
    balance_after = int(user["paid_remaining"]) - credits
    connection.execute(
        "UPDATE platform_users SET paid_remaining = %s, updated_at = NOW() WHERE id = %s",
        (balance_after, user_id),
    )
    connection.execute(
        """
        INSERT INTO platform_credit_ledger
          (id, user_id, bucket, delta, reason, idempotency_key, reference_type, reference_id, balance_after)
        VALUES (%s, %s, 'paid', %s, 'web_payment_reversal', %s, %s, %s, %s)
        """,
        (new_id(), user_id, -credits, idempotency_key, reference_type, reference_id, balance_after),
    )
    return True, balance_after


def prepare_paid_credit_reversal(
    connection: psycopg.Connection,
    *,
    user_id: str,
    credits: int,
    external_key: str,
) -> dict[str, Any]:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"credit-reversal:{external_key}",),
    )
    existing = connection.execute(
        "SELECT * FROM platform_credit_reversals WHERE external_key = %s FOR UPDATE",
        (external_key,),
    ).fetchone()
    if existing is not None:
        if str(existing["user_id"]) != user_id or int(existing["credits"]) != credits:
            raise HTTPException(status_code=409, detail="credit reversal key was used for another operation")
        if existing["status"] in {"pending", "completed"}:
            return dict(existing)
        attempt = int(existing["attempt_count"]) + 1
        reversal_id = str(existing["id"])
    else:
        attempt = 1
        reversal_id = new_id()
    user = connection.execute(
        "SELECT paid_remaining FROM platform_users WHERE id = %s AND status = 'active' FOR UPDATE",
        (user_id,),
    ).fetchone()
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    if int(user["paid_remaining"]) < credits:
        raise HTTPException(status_code=409, detail="paid credits have already been used")
    balance_after = int(user["paid_remaining"]) - credits
    ledger_id = new_id()
    connection.execute(
        "UPDATE platform_users SET paid_remaining = %s, updated_at = NOW() WHERE id = %s",
        (balance_after, user_id),
    )
    connection.execute(
        """
        INSERT INTO platform_credit_ledger
          (id, user_id, bucket, delta, reason, idempotency_key, reference_type, reference_id, balance_after)
        VALUES (%s, %s, 'paid', %s, 'web_payment_reversal_reserved', %s, 'credit_reversal', %s, %s)
        """,
        (ledger_id, user_id, -credits, f"credit-reversal:{external_key}:{attempt}", reversal_id, balance_after),
    )
    if existing is None:
        connection.execute(
            """
            INSERT INTO platform_credit_reversals
              (id, user_id, external_key, credits, status, attempt_count, debit_ledger_id)
            VALUES (%s, %s, %s, %s, 'pending', 1, %s)
            """,
            (reversal_id, user_id, external_key, credits, ledger_id),
        )
    else:
        connection.execute(
            """
            UPDATE platform_credit_reversals
            SET status = 'pending', attempt_count = %s, debit_ledger_id = %s,
                refund_ledger_id = NULL, rolled_back_at = NULL, updated_at = NOW()
            WHERE id = %s
            """,
            (attempt, ledger_id, reversal_id),
        )
    return dict(
        connection.execute("SELECT * FROM platform_credit_reversals WHERE id = %s", (reversal_id,)).fetchone()
    )


def complete_paid_credit_reversal(
    connection: psycopg.Connection, *, user_id: str, external_key: str
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM platform_credit_reversals WHERE external_key = %s AND user_id = %s FOR UPDATE",
        (external_key, user_id),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="credit reversal not found")
    if row["status"] == "rolled_back":
        raise HTTPException(status_code=409, detail="rolled back credit reversal cannot be completed")
    if row["status"] == "pending":
        connection.execute(
            """
            UPDATE platform_credit_reversals
            SET status = 'completed', completed_at = NOW(), updated_at = NOW()
            WHERE id = %s
            """,
            (row["id"],),
        )
    return dict(connection.execute("SELECT * FROM platform_credit_reversals WHERE id = %s", (row["id"],)).fetchone())


def rollback_paid_credit_reversal(
    connection: psycopg.Connection, *, user_id: str, external_key: str
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM platform_credit_reversals WHERE external_key = %s AND user_id = %s FOR UPDATE",
        (external_key, user_id),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="credit reversal not found")
    if row["status"] == "completed":
        raise HTTPException(status_code=409, detail="completed credit reversal cannot be rolled back")
    if row["status"] == "rolled_back":
        return dict(row)
    user = connection.execute(
        "SELECT paid_remaining FROM platform_users WHERE id = %s FOR UPDATE", (user_id,)
    ).fetchone()
    balance_after = int(user["paid_remaining"]) + int(row["credits"])
    ledger_id = new_id()
    connection.execute(
        "UPDATE platform_users SET paid_remaining = %s, updated_at = NOW() WHERE id = %s",
        (balance_after, user_id),
    )
    connection.execute(
        """
        INSERT INTO platform_credit_ledger
          (id, user_id, bucket, delta, reason, idempotency_key, reference_type, reference_id, balance_after)
        VALUES (%s, %s, 'paid', %s, 'web_payment_reversal_rollback', %s, 'credit_reversal', %s, %s)
        """,
        (
            ledger_id, user_id, int(row["credits"]),
            f"credit-reversal-rollback:{external_key}:{row['attempt_count']}", str(row["id"]), balance_after,
        ),
    )
    connection.execute(
        """
        UPDATE platform_credit_reversals
        SET status = 'rolled_back', refund_ledger_id = %s, rolled_back_at = NOW(), updated_at = NOW()
        WHERE id = %s
        """,
        (ledger_id, row["id"]),
    )
    return dict(connection.execute("SELECT * FROM platform_credit_reversals WHERE id = %s", (row["id"],)).fetchone())


def resolve_external_web_account(
    connection: psycopg.Connection,
    *,
    external_id: str,
    email: str | None,
    display_name: str | None,
    provider: str | None,
    provider_subject: str | None,
) -> str:
    connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"web-account:{external_id}",),
    )
    link = connection.execute(
        "SELECT user_id FROM platform_external_accounts WHERE namespace = 'web' AND external_id = %s",
        (external_id,),
    ).fetchone()
    if link is not None:
        user_id = str(link["user_id"])
        if provider in {"kakao", "naver", "google"} and provider_subject:
            # Re-resolving an already mapped web account is the explicit bridge
            # operation used after an authenticated social-account link.
            resolve_oauth_identity(
                connection,
                provider=provider,
                subject=provider_subject,
                email=email,
                display_name=display_name,
                link_user_id=user_id,
            )
        return user_id

    user_id: str | None = None
    if provider in {"kakao", "naver", "google"} and provider_subject:
        identity = connection.execute(
            "SELECT user_id FROM platform_identities WHERE provider = %s AND provider_subject = %s",
            (provider, provider_subject),
        ).fetchone()
        if identity is not None:
            user_id = str(identity["user_id"])
        else:
            user_id = resolve_oauth_identity(
                connection,
                provider=provider,
                subject=provider_subject,
                email=email,
                display_name=display_name,
                link_user_id=None,
            )
    if user_id is None:
        # Email is profile data, never an account-merge key.
        user_id = resolve_oauth_identity(
            connection,
            provider="web_email",
            subject=f"web:{external_id}",
            email=email,
            display_name=display_name,
            link_user_id=None,
        )
    connection.execute(
        """
        INSERT INTO platform_external_accounts (namespace, external_id, user_id)
        VALUES ('web', %s, %s)
        """,
        (external_id, user_id),
    )
    return user_id
