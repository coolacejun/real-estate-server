BEGIN;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS platform_users (
  id UUID PRIMARY KEY,
  email TEXT,
  display_name TEXT,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'withdrawn', 'suspended')),
  free_remaining INTEGER NOT NULL DEFAULT 3 CHECK (free_remaining >= 0),
  paid_remaining INTEGER NOT NULL DEFAULT 0 CHECK (paid_remaining >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  withdrawn_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS platform_users_email_idx
  ON platform_users (LOWER(email)) WHERE email IS NOT NULL;

CREATE TABLE IF NOT EXISTS platform_identities (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  provider TEXT NOT NULL CHECK (provider IN ('kakao', 'naver', 'google', 'web_email')),
  provider_subject TEXT NOT NULL,
  provider_email TEXT,
  provider_display_name TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (provider, provider_subject)
);

CREATE INDEX IF NOT EXISTS platform_identities_user_idx
  ON platform_identities (user_id, provider);

CREATE TABLE IF NOT EXISTS platform_external_accounts (
  namespace TEXT NOT NULL,
  external_id TEXT NOT NULL,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (namespace, external_id)
);

CREATE INDEX IF NOT EXISTS platform_external_accounts_user_idx
  ON platform_external_accounts (user_id);

CREATE TABLE IF NOT EXISTS platform_credit_ledger (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  bucket TEXT NOT NULL CHECK (bucket IN ('free', 'paid')),
  delta INTEGER NOT NULL CHECK (delta <> 0),
  reason TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  reference_type TEXT,
  reference_id TEXT,
  balance_after INTEGER NOT NULL CHECK (balance_after >= 0),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS platform_credit_ledger_user_idx
  ON platform_credit_ledger (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS platform_credit_reversals (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  external_key TEXT NOT NULL UNIQUE,
  credits INTEGER NOT NULL CHECK (credits > 0),
  status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'rolled_back')),
  attempt_count INTEGER NOT NULL DEFAULT 1,
  debit_ledger_id UUID NOT NULL REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
  refund_ledger_id UUID REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  rolled_back_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS platform_credit_reversals_user_idx
  ON platform_credit_reversals (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS mobile_oauth_flows (
  state_hash CHAR(64) PRIMARY KEY,
  provider TEXT NOT NULL CHECK (provider IN ('kakao', 'naver', 'google')),
  code_challenge TEXT NOT NULL,
  redirect_uri TEXT NOT NULL,
  link_user_id UUID REFERENCES platform_users(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS mobile_oauth_flows_expiry_idx
  ON mobile_oauth_flows (expires_at);

CREATE TABLE IF NOT EXISTS mobile_auth_codes (
  code_hash CHAR(64) PRIMARY KEY,
  state_hash CHAR(64) NOT NULL REFERENCES mobile_oauth_flows(state_hash) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  provider TEXT NOT NULL CHECK (provider IN ('kakao', 'naver', 'google')),
  code_challenge TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS mobile_auth_codes_expiry_idx
  ON mobile_auth_codes (expires_at);

CREATE TABLE IF NOT EXISTS mobile_refresh_tokens (
  token_hash CHAR(64) PRIMARY KEY,
  family_id UUID NOT NULL,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  device_id TEXT NOT NULL,
  parent_token_hash CHAR(64),
  replacement_token_hash CHAR(64),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  rotated_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  revoke_reason TEXT
);

CREATE INDEX IF NOT EXISTS mobile_refresh_tokens_family_idx
  ON mobile_refresh_tokens (family_id, created_at DESC);
CREATE INDEX IF NOT EXISTS mobile_refresh_tokens_user_device_idx
  ON mobile_refresh_tokens (user_id, device_id, created_at DESC);

CREATE TABLE IF NOT EXISTS mobile_access_tokens (
  token_hash CHAR(64) PRIMARY KEY,
  family_id UUID NOT NULL,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  device_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS mobile_access_tokens_family_idx
  ON mobile_access_tokens (family_id, expires_at);
CREATE INDEX IF NOT EXISTS mobile_access_tokens_user_idx
  ON mobile_access_tokens (user_id, expires_at);

CREATE TABLE IF NOT EXISTS mobile_store_accounts (
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  platform TEXT NOT NULL CHECK (platform IN ('ios', 'android')),
  account_token UUID NOT NULL UNIQUE,
  last_device_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (user_id, platform)
);

CREATE TABLE IF NOT EXISTS mobile_store_transactions (
  id UUID PRIMARY KEY,
  platform TEXT NOT NULL CHECK (platform IN ('ios', 'android')),
  transaction_key TEXT NOT NULL,
  verification_digest CHAR(64) NOT NULL,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  product_id TEXT NOT NULL,
  store_environment TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('verified', 'granted', 'entitled', 'revoked')),
  pricing_policy TEXT NOT NULL CHECK (pricing_policy IN ('current', 'retired', 'legacy')),
  credits_granted INTEGER NOT NULL DEFAULT 0 CHECK (credits_granted >= 0),
  post_commit_status TEXT NOT NULL DEFAULT 'not_required'
    CHECK (post_commit_status IN ('not_required', 'pending', 'completed', 'failed')),
  verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (platform, transaction_key),
  UNIQUE (platform, verification_digest)
);

CREATE INDEX IF NOT EXISTS mobile_store_transactions_user_idx
  ON mobile_store_transactions (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS mobile_store_events (
  id UUID PRIMARY KEY,
  transaction_id UUID REFERENCES mobile_store_transactions(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  event_type TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS mobile_store_events_user_idx
  ON mobile_store_events (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS platform_entitlements (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  store TEXT NOT NULL,
  product_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
  pricing_policy TEXT NOT NULL CHECK (pricing_policy IN ('legacy')),
  source_transaction_id UUID REFERENCES mobile_store_transactions(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, store, product_id)
);

CREATE TABLE IF NOT EXISTS platform_report_usages (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  request_id TEXT NOT NULL,
  content_hash CHAR(64) NOT NULL,
  renderer_profile TEXT NOT NULL,
  renderer_version TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
  debit_bucket TEXT CHECK (debit_bucket IN ('free', 'paid')),
  debit_ledger_id UUID REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
  refund_ledger_id UUID REFERENCES platform_credit_ledger(id) ON DELETE RESTRICT,
  archive_id UUID,
  attempt_count INTEGER NOT NULL DEFAULT 1,
  error_code TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  failed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, request_id)
);

CREATE INDEX IF NOT EXISTS platform_report_usages_status_idx
  ON platform_report_usages (status, reserved_at);

CREATE TABLE IF NOT EXISTS platform_report_archives (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  report_id TEXT,
  title TEXT,
  address TEXT,
  included_items JSONB NOT NULL DEFAULT '[]'::jsonb,
  canonical_report JSONB NOT NULL,
  asset_manifest JSONB NOT NULL DEFAULT '[]'::jsonb,
  schema_version INTEGER NOT NULL,
  renderer_profile TEXT NOT NULL,
  renderer_version TEXT NOT NULL,
  mapping_version TEXT,
  content_hash CHAR(64) NOT NULL,
  usage_id UUID NOT NULL UNIQUE REFERENCES platform_report_usages(id) ON DELETE RESTRICT,
  status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'deleted')),
  saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ
);

ALTER TABLE platform_report_usages
  DROP CONSTRAINT IF EXISTS platform_report_usages_archive_id_fkey;
ALTER TABLE platform_report_usages
  ADD CONSTRAINT platform_report_usages_archive_id_fkey
  FOREIGN KEY (archive_id) REFERENCES platform_report_archives(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS platform_report_archives_user_idx
  ON platform_report_archives (user_id, saved_at DESC) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS platform_report_assets (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  content_hash CHAR(64) NOT NULL,
  content_type TEXT NOT NULL,
  storage_key TEXT NOT NULL UNIQUE,
  byte_size BIGINT NOT NULL CHECK (byte_size > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, content_hash)
);

CREATE INDEX IF NOT EXISTS platform_report_assets_user_idx
  ON platform_report_assets (user_id, created_at DESC);

INSERT INTO schema_migrations (version)
VALUES ('009_mobile_platform')
ON CONFLICT (version) DO NOTHING;

COMMIT;
