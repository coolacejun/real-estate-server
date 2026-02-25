# Cadastral Tile Serving (On-demand PNG)

## Endpoint
- `GET /v1/tiles/cadastral/{z}/{x}/{y}.png`
- `GET /v1/tile-config`

## Cache strategy
- Server memory cache (LRU)
- Server disk cache (`TILE_CACHE_DIR/cadastral/{version}/{render_rev}/z/x/y.png`)
- CDN cache via headers:
  - `Cache-Control: public, max-age=31536000, immutable`
  - `CDN-Cache-Control: public, max-age=31536000, immutable`

## Versioning
- Bump `CADASTRAL_TILE_VERSION` when source geometry changes.
- App should pass version query (`?v=v1`) on tile URL.
- `cadastral_release` 활성 버전이 있으면 서버 기본 버전으로 사용합니다.
- 렌더러 품질 옵션 변경 시 `CADASTRAL_TILE_RENDER_REV`를 올리면 기존 캐시와 분리됩니다.

## Expected DB schema
API expects a table with geometry JSON + bbox columns for fast range lookup.

Example:

```sql
CREATE TABLE cadastral_features (
  id BIGSERIAL PRIMARY KEY,
  pnu TEXT,
  label TEXT,
  geojson JSONB NOT NULL,
  bbox_min_lon DOUBLE PRECISION NOT NULL,
  bbox_max_lon DOUBLE PRECISION NOT NULL,
  bbox_min_lat DOUBLE PRECISION NOT NULL,
  bbox_max_lat DOUBLE PRECISION NOT NULL
);

CREATE INDEX cadastral_features_bbox_idx
  ON cadastral_features (bbox_min_lon, bbox_max_lon, bbox_min_lat, bbox_max_lat);
```

## Required env vars
- `CADASTRAL_TILE_TABLE`
- `CADASTRAL_TILE_RELEASE_COL`
- `CADASTRAL_TILE_GEOJSON_COL`
- `CADASTRAL_TILE_LABEL_COL`
- `CADASTRAL_TILE_MIN_LON_COL`
- `CADASTRAL_TILE_MAX_LON_COL`
- `CADASTRAL_TILE_MIN_LAT_COL`
- `CADASTRAL_TILE_MAX_LAT_COL`
- `CADASTRAL_TILE_VERSION`
- `CADASTRAL_TILE_RENDER_REV`
- `CADASTRAL_TILE_SUPERSAMPLE`
- `CADASTRAL_TILE_STROKE_WIDTH`
- `CADASTRAL_TILE_LABEL_ENABLED`
- `CADASTRAL_TILE_LABEL_MIN_ZOOM`
- `CADASTRAL_TILE_LABEL_MIN_BOX`
- `CADASTRAL_TILE_LABEL_FONT_PATH`
- `CADASTRAL_TILE_MIN_ZOOM`
- `CADASTRAL_TILE_MAX_ZOOM`
- `CADASTRAL_TILE_MEMORY_CACHE_SIZE`
- `CADASTRAL_TILE_DB_LIMIT`
- `TILE_CACHE_DIR`
