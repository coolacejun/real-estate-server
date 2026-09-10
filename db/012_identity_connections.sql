BEGIN;
ALTER TABLE platform_users ADD COLUMN IF NOT EXISTS auth_version INTEGER NOT NULL DEFAULT 0;
ALTER TABLE platform_identities ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE platform_identities ADD COLUMN IF NOT EXISTS disconnected_at TIMESTAMPTZ;
ALTER TABLE mobile_oauth_flows ADD COLUMN IF NOT EXISTS link_auth_binding TEXT;
ALTER TABLE mobile_auth_codes ADD COLUMN IF NOT EXISTS pending_identity JSONB;
-- Existing identities remain reserved after disconnect/withdrawal. This prevents
-- silent reassignment and repeated new-account grants. No user/ledger rows move.
INSERT INTO schema_migrations(version) VALUES ('012_identity_connections') ON CONFLICT DO NOTHING;
COMMIT;
