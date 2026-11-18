BEGIN;

ALTER TABLE platform_identities DROP CONSTRAINT IF EXISTS platform_identities_provider_check;
ALTER TABLE platform_identities ADD CONSTRAINT platform_identities_provider_check
  CHECK (provider IN ('kakao', 'naver', 'google', 'web_email', 'apple'));
ALTER TABLE mobile_oauth_flows DROP CONSTRAINT IF EXISTS mobile_oauth_flows_provider_check;
ALTER TABLE mobile_oauth_flows ADD CONSTRAINT mobile_oauth_flows_provider_check
  CHECK (provider IN ('kakao', 'naver', 'google', 'apple'));

CREATE TABLE IF NOT EXISTS mobile_apple_credentials (
  provider_subject TEXT PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
  refresh_token_ciphertext TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS mobile_apple_credentials_user_idx
  ON mobile_apple_credentials (user_id) WHERE revoked_at IS NULL;

INSERT INTO schema_migrations(version) VALUES ('016_apple_signin') ON CONFLICT DO NOTHING;
COMMIT;
