BEGIN;

CREATE TABLE IF NOT EXISTS dataset_record (
  id BIGSERIAL PRIMARY KEY,
  release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
  data_type TEXT NOT NULL,
  dataset_code TEXT,
  source_file TEXT,
  row_no BIGINT,
  pnu TEXT,
  payload JSONB NOT NULL,
  geometry JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS dataset_record_release_pnu_idx
  ON dataset_record (release_id, pnu);

CREATE INDEX IF NOT EXISTS dataset_record_data_type_release_id_idx
  ON dataset_record (data_type, release_id, id DESC);

CREATE INDEX IF NOT EXISTS dataset_record_dataset_code_idx
  ON dataset_record (data_type, dataset_code, release_id, id DESC);

COMMIT;
