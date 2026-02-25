BEGIN;

CREATE TABLE IF NOT EXISTS dataset_pnu_kv (
  id BIGSERIAL PRIMARY KEY,
  release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
  data_type TEXT NOT NULL,
  pnu TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (release_id, data_type, pnu)
);

CREATE INDEX IF NOT EXISTS dataset_pnu_kv_release_type_pnu_idx
  ON dataset_pnu_kv (release_id, data_type, pnu);

CREATE INDEX IF NOT EXISTS dataset_pnu_kv_data_type_release_idx
  ON dataset_pnu_kv (data_type, release_id);

CREATE OR REPLACE FUNCTION set_dataset_pnu_kv_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_dataset_pnu_kv_updated_at ON dataset_pnu_kv;
CREATE TRIGGER trg_dataset_pnu_kv_updated_at
BEFORE UPDATE ON dataset_pnu_kv
FOR EACH ROW
EXECUTE FUNCTION set_dataset_pnu_kv_updated_at();

COMMIT;
