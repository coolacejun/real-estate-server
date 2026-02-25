BEGIN;

CREATE TABLE IF NOT EXISTS public.land_info_record (
  id BIGSERIAL PRIMARY KEY,
  release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
  dataset_code TEXT,
  pnu TEXT NOT NULL,
  schema_id BIGINT NOT NULL REFERENCES dataset_schema(id) ON DELETE RESTRICT,
  payload_values JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS land_info_record_release_pnu_idx
  ON public.land_info_record (release_id, pnu);

CREATE INDEX IF NOT EXISTS land_info_record_release_dataset_pnu_idx
  ON public.land_info_record (release_id, dataset_code, pnu);

COMMIT;
