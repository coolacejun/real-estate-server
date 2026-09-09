BEGIN;

ALTER TABLE mobile_oauth_flows
  ADD COLUMN IF NOT EXISTS nonce_hash CHAR(64);

ALTER TABLE platform_identities
  ADD COLUMN IF NOT EXISTS provider_email_verified BOOLEAN;

CREATE TABLE IF NOT EXISTS mobile_auth_events (
  id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  user_id UUID REFERENCES platform_users(id) ON DELETE SET NULL,
  provider TEXT,
  family_id UUID,
  device_hash CHAR(64),
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS mobile_auth_events_user_idx
  ON mobile_auth_events (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS mobile_auth_events_family_idx
  ON mobile_auth_events (family_id, created_at DESC)
  WHERE family_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS mobile_auth_events_type_idx
  ON mobile_auth_events (event_type, created_at DESC);

INSERT INTO schema_migrations (version)
VALUES ('010_mobile_auth_hardening')
ON CONFLICT (version) DO NOTHING;

COMMIT;
