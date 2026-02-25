BEGIN;

ALTER TABLE cadastral_release
  ADD COLUMN IF NOT EXISTS data_type TEXT NOT NULL DEFAULT 'cadastral';

UPDATE cadastral_release
SET data_type = 'cadastral'
WHERE data_type IS NULL
   OR data_type = '';

DROP INDEX IF EXISTS cadastral_release_active_one_idx;

ALTER TABLE cadastral_release
  DROP CONSTRAINT IF EXISTS cadastral_release_version_key;

CREATE UNIQUE INDEX IF NOT EXISTS cadastral_release_data_type_version_uidx
  ON cadastral_release (data_type, version);

CREATE UNIQUE INDEX IF NOT EXISTS cadastral_release_active_per_type_idx
  ON cadastral_release (data_type)
  WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS cadastral_release_data_type_id_idx
  ON cadastral_release (data_type, id DESC);

ALTER TABLE cadastral_import_job
  ADD COLUMN IF NOT EXISTS data_type TEXT;

UPDATE cadastral_import_job j
SET data_type = COALESCE(j.data_type, r.data_type, 'cadastral')
FROM cadastral_release r
WHERE j.release_id = r.id
  AND (j.data_type IS NULL OR j.data_type = '');

UPDATE cadastral_import_job
SET data_type = 'cadastral'
WHERE data_type IS NULL
   OR data_type = '';

ALTER TABLE cadastral_import_job
  ALTER COLUMN data_type SET DEFAULT 'cadastral';

ALTER TABLE cadastral_import_job
  ALTER COLUMN data_type SET NOT NULL;

CREATE INDEX IF NOT EXISTS cadastral_import_job_data_type_id_idx
  ON cadastral_import_job (data_type, id DESC);

CREATE TABLE IF NOT EXISTS building_info_line (
  id BIGSERIAL PRIMARY KEY,
  release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
  pnu TEXT NOT NULL,
  category TEXT NOT NULL,
  line TEXT NOT NULL,
  source_file TEXT,
  line_no BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS building_info_line_release_pnu_category_idx
  ON building_info_line (release_id, pnu, category);

CREATE TABLE IF NOT EXISTS dataset_import_file (
  id BIGSERIAL PRIMARY KEY,
  release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
  data_type TEXT NOT NULL,
  file_name TEXT NOT NULL,
  file_size BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS dataset_import_file_release_id_idx
  ON dataset_import_file (release_id, id DESC);

COMMIT;
