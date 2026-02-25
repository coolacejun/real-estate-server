BEGIN;

ALTER TABLE cadastral_features
  ADD COLUMN IF NOT EXISTS label_lon DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS label_lat DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS cadastral_features_release_label_point_idx
  ON cadastral_features (release_id, label_lon, label_lat);

COMMIT;
