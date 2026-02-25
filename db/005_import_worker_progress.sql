BEGIN;

CREATE TABLE IF NOT EXISTS cadastral_import_job_worker (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT NOT NULL REFERENCES cadastral_import_job(id) ON DELETE CASCADE,
  source_file TEXT NOT NULL,
  worker_name TEXT,
  status TEXT NOT NULL DEFAULT 'QUEUED',
  processed_rows BIGINT NOT NULL DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS cadastral_import_job_worker_job_file_uidx
  ON cadastral_import_job_worker (job_id, source_file);

CREATE INDEX IF NOT EXISTS cadastral_import_job_worker_job_status_idx
  ON cadastral_import_job_worker (job_id, status, id DESC);

COMMIT;
