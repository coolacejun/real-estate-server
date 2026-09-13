BEGIN;

-- Additive and permanent. Never delete these identities to retry a migration.
CREATE TABLE IF NOT EXISTS legacy_store_purchases (
  id UUID PRIMARY KEY,
  platform TEXT NOT NULL CHECK (platform IN ('ios','android')),
  identity_digest CHAR(64) NOT NULL,
  product_id TEXT NOT NULL,
  user_id UUID REFERENCES platform_users(id) ON DELETE RESTRICT,
  migration_version INTEGER NOT NULL DEFAULT 1 CHECK (migration_version = 1),
  state TEXT NOT NULL CHECK (state IN ('granted','revoked')),
  credits_granted INTEGER NOT NULL CHECK (credits_granted IN (0,10)),
  grant_ledger_id UUID UNIQUE REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
  reversal_ledger_id UUID UNIQUE REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
  reconciliation_credits INTEGER NOT NULL DEFAULT 0 CHECK (reconciliation_credits BETWEEN 0 AND 10),
  evidence_ref TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_at TIMESTAMPTZ,
  UNIQUE(platform, identity_digest),
  CHECK ((credits_granted=10 AND user_id IS NOT NULL AND grant_ledger_id IS NOT NULL)
      OR (credits_granted=0 AND grant_ledger_id IS NULL AND state='revoked'))
);
CREATE INDEX IF NOT EXISTS legacy_store_purchases_user_idx ON legacy_store_purchases(user_id);
CREATE TABLE IF NOT EXISTS legacy_store_aliases (
  platform TEXT NOT NULL,
  alias_digest CHAR(64) NOT NULL,
  purchase_id UUID NOT NULL REFERENCES legacy_store_purchases(id) ON DELETE RESTRICT,
  PRIMARY KEY(platform, alias_digest)
);
-- Trusted operator approval only; mobile clients cannot create these bindings.
CREATE TABLE IF NOT EXISTS legacy_store_bindings (
  platform TEXT NOT NULL CHECK (platform IN ('ios','android')),
  identity_digest CHAR(64) NOT NULL,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  evidence_ref TEXT NOT NULL CHECK (length(evidence_ref) BETWEEN 1 AND 200),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(platform, identity_digest)
);
CREATE TABLE IF NOT EXISTS legacy_store_audit (
  id UUID PRIMARY KEY,
  purchase_id UUID REFERENCES legacy_store_purchases(id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO schema_migrations(version) VALUES ('013_legacy_store_migration') ON CONFLICT DO NOTHING;
COMMIT;
