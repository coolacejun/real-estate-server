BEGIN;

-- Additive storage: canonical archives, usage, ledger and store rows are untouched.
CREATE TABLE IF NOT EXISTS platform_archive_imports (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  source_kind TEXT NOT NULL CHECK (source_kind IN ('uploaded-pdf', 'legacy-web')),
  source_key TEXT,
  title TEXT NOT NULL,
  address TEXT NOT NULL DEFAULT '',
  provenance TEXT NOT NULL,
  artifact_sha256 CHAR(64) NOT NULL,
  byte_size INTEGER NOT NULL CHECK (byte_size > 0 AND byte_size <= 16777216),
  page_count INTEGER CHECK (page_count BETWEEN 1 AND 200),
  pdf_data BYTEA,
  html_data TEXT,
  json_data JSONB,
  saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ,
  migration_id TEXT,
  UNIQUE (user_id, source_kind, source_key),
  CHECK ((source_kind = 'uploaded-pdf' AND pdf_data IS NOT NULL AND page_count IS NOT NULL
          AND html_data IS NULL AND json_data IS NULL)
         OR (source_kind = 'legacy-web' AND html_data IS NOT NULL AND json_data IS NOT NULL)),
  CHECK (pdf_data IS NULL OR octet_length(pdf_data) = byte_size)
);
CREATE UNIQUE INDEX IF NOT EXISTS platform_archive_pdf_owner_hash_idx
  ON platform_archive_imports(user_id, artifact_sha256)
  WHERE source_kind = 'uploaded-pdf' AND deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS platform_archive_imports_owner_idx
  ON platform_archive_imports(user_id, saved_at DESC, id DESC) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS platform_archive_uploads (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  request_id TEXT NOT NULL,
  fingerprint CHAR(64) NOT NULL,
  expected_sha256 CHAR(64) NOT NULL,
  byte_size INTEGER NOT NULL CHECK (byte_size BETWEEN 1 AND 16777216),
  title TEXT NOT NULL,
  address TEXT NOT NULL DEFAULT '',
  provenance TEXT NOT NULL,
  pdf_data BYTEA,
  page_count INTEGER,
  archive_id UUID REFERENCES platform_archive_imports(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '24 hours',
  UNIQUE (user_id, request_id)
);

CREATE TABLE IF NOT EXISTS platform_archive_events (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES platform_users(id) ON DELETE RESTRICT,
  archive_id UUID,
  event_type TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS platform_account_review_queue (
  id UUID PRIMARY KEY,
  source_key TEXT NOT NULL UNIQUE,
  reason TEXT NOT NULL,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'resolved', 'dismissed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- UNION avoids a second write: committing a canonical final makes its existing ID
-- immediately visible, including rows created before this migration.
CREATE OR REPLACE VIEW platform_archive_catalog AS
SELECT id, user_id, title, address, saved_at, included_items,
       'canonical'::text AS source_kind, 'server-canonical-pdf'::text AS provenance,
       content_hash::text, NULL::text AS artifact_sha256, NULL::integer AS byte_size,
       NULL::integer AS page_count, renderer_profile, renderer_version,
       ARRAY['pdf','html','json']::text[] AS content_formats
FROM platform_report_archives WHERE deleted_at IS NULL AND status = 'ready'
UNION ALL
SELECT id, user_id, title, address, saved_at, '[]'::jsonb AS included_items,
       source_kind, provenance, NULL::text, artifact_sha256::text, byte_size,
       page_count, NULL::text, NULL::text,
       CASE WHEN pdf_data IS NOT NULL THEN ARRAY['pdf']::text[]
            ELSE ARRAY['html','json']::text[] END
FROM platform_archive_imports WHERE deleted_at IS NULL;

INSERT INTO schema_migrations(version) VALUES ('011_shared_archives') ON CONFLICT DO NOTHING;
COMMIT;
