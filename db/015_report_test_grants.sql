BEGIN;

-- A narrowly scoped, revocable permission. No balance or purchase rows change.
CREATE TABLE IF NOT EXISTS platform_report_test_grants (
  user_id UUID PRIMARY KEY REFERENCES platform_users(id) ON DELETE CASCADE,
  naver_identity_id UUID NOT NULL REFERENCES platform_identities(id) ON DELETE RESTRICT,
  active BOOLEAN NOT NULL,
  granted_by TEXT NOT NULL CHECK (length(granted_by) BETWEEN 1 AND 120),
  granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_by TEXT,
  revoked_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK ((active AND revoked_at IS NULL) OR (NOT active AND revoked_at IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS platform_report_test_grant_events (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  action TEXT NOT NULL CHECK (action IN ('grant', 'revoke')),
  actor TEXT NOT NULL CHECK (length(actor) BETWEEN 1 AND 120),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE platform_report_usages
  ADD COLUMN IF NOT EXISTS no_charge_reason TEXT;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'platform_report_usages_test_no_charge_check') THEN
    ALTER TABLE platform_report_usages ADD CONSTRAINT platform_report_usages_test_no_charge_check
      CHECK (no_charge_reason IS NULL OR
             (no_charge_reason = 'test_report_grant' AND debit_bucket IS NULL AND debit_ledger_id IS NULL));
  END IF;
END $$;

INSERT INTO schema_migrations(version) VALUES ('015_report_test_grants')
ON CONFLICT DO NOTHING;

COMMIT;
