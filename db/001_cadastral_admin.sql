BEGIN;

CREATE TABLE IF NOT EXISTS cadastral_release (
  id BIGSERIAL PRIMARY KEY,
  version TEXT NOT NULL UNIQUE,
  source_name TEXT,
  status TEXT NOT NULL DEFAULT 'PENDING',
  is_active BOOLEAN NOT NULL DEFAULT FALSE,
  records_count BIGINT NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  activated_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS cadastral_release_active_one_idx
  ON cadastral_release ((is_active))
  WHERE is_active = TRUE;

CREATE TABLE IF NOT EXISTS cadastral_import_job (
  id BIGSERIAL PRIMARY KEY,
  release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'QUEUED',
  source_path TEXT NOT NULL,
  total_files INTEGER NOT NULL DEFAULT 0,
  processed_files INTEGER NOT NULL DEFAULT 0,
  inserted_rows BIGINT NOT NULL DEFAULT 0,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS cadastral_import_job_release_id_idx
  ON cadastral_import_job (release_id, id DESC);

CREATE TABLE IF NOT EXISTS cadastral_features (
  id BIGSERIAL PRIMARY KEY,
  release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
  pnu TEXT,
  label TEXT,
  geojson JSONB NOT NULL,
  bbox_min_lon DOUBLE PRECISION NOT NULL,
  bbox_max_lon DOUBLE PRECISION NOT NULL,
  bbox_min_lat DOUBLE PRECISION NOT NULL,
  bbox_max_lat DOUBLE PRECISION NOT NULL,
  label_lon DOUBLE PRECISION,
  label_lat DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cadastral_features_release_idx
  ON cadastral_features (release_id);

CREATE INDEX IF NOT EXISTS cadastral_features_bbox_idx
  ON cadastral_features (bbox_min_lon, bbox_max_lon, bbox_min_lat, bbox_max_lat);

CREATE INDEX IF NOT EXISTS cadastral_features_release_label_point_idx
  ON cadastral_features (release_id, label_lon, label_lat);

COMMIT;
