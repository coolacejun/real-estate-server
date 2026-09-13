BEGIN;
-- 013 remains superseded audit history, never a source for new grants.
ALTER TABLE platform_entitlements ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE platform_entitlements ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ;
CREATE TABLE IF NOT EXISTS legacy_subscription_lineages (
 id UUID PRIMARY KEY, root_digest CHAR(64) UNIQUE NOT NULL,
 user_id UUID REFERENCES platform_users(id) ON DELETE RESTRICT,
 binding_kind TEXT CHECK (binding_kind IN ('provider','reviewed')), evidence_ref TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS legacy_subscription_tokens (
 token_digest CHAR(64) PRIMARY KEY,
 lineage_id UUID NOT NULL REFERENCES legacy_subscription_lineages(id) ON DELETE RESTRICT,
 superseded_by CHAR(64), subscription_state TEXT NOT NULL, expires_at TIMESTAMPTZ,
 entitlement_active BOOLEAN NOT NULL, latest_order_id TEXT, verified_at TIMESTAMPTZ NOT NULL,
 revoked_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS legacy_subscription_tokens_lineage_idx ON legacy_subscription_tokens(lineage_id);
CREATE TABLE IF NOT EXISTS legacy_subscription_grants (
 id UUID PRIMARY KEY, user_id UUID NOT NULL UNIQUE REFERENCES platform_users(id) ON DELETE RESTRICT,
 lineage_id UUID NOT NULL UNIQUE REFERENCES legacy_subscription_lineages(id) ON DELETE RESTRICT,
 source_token_digest CHAR(64) NOT NULL REFERENCES legacy_subscription_tokens(token_digest) ON DELETE RESTRICT,
 source_order_id TEXT NOT NULL UNIQUE, version INTEGER NOT NULL DEFAULT 1 CHECK (version=1),
 credits INTEGER NOT NULL DEFAULT 10 CHECK (credits=10), state TEXT NOT NULL CHECK (state IN ('granted','revoked')),
 grant_ledger_id UUID NOT NULL UNIQUE REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
 reversal_ledger_id UUID UNIQUE REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
 reconciliation_credits INTEGER NOT NULL DEFAULT 0 CHECK (reconciliation_credits BETWEEN 0 AND 10),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), revoked_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS legacy_subscription_order_voids (
 order_id TEXT PRIMARY KEY, reason TEXT NOT NULL CHECK (reason IN ('refunded','revoked','chargeback')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS legacy_subscription_token_revocations (
 token_digest CHAR(64) PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS legacy_subscription_audit (
 id UUID PRIMARY KEY, lineage_id UUID REFERENCES legacy_subscription_lineages(id) ON DELETE RESTRICT,
 event_type TEXT NOT NULL, detail JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO schema_migrations(version) VALUES ('014_legacy_subscription_migration') ON CONFLICT DO NOTHING;
COMMIT;
