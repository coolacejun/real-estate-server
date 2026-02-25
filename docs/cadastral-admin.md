# Cadastral Admin 운영 가이드

## 관리자 웹
- URL: `http://localhost/admin`
- 브라우저에서 `X-Admin-Token` 값을 입력 후 저장하면 관리 API를 호출합니다.
- 기능:
  - Release 생성/조회/활성화
  - Import Job 생성/조회
  - Import Job 실행(백그라운드)

## 1) 마이그레이션 적용
```bash
cd /Users/jun/server
/Applications/Docker.app/Contents/Resources/bin/docker exec -i postgres \
  psql -U appuser -d appdb < /Users/jun/server/db/001_cadastral_admin.sql
```

## 2) 릴리즈 생성
```bash
curl -X POST "http://localhost/v1/admin/cadastral/releases" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: <ADMIN_TOKEN>" \
  -d '{"version":"2026-01-04","source_name":"연속지적 2026-01-04"}'
```

## 3) Import Job 생성
```bash
curl -X POST "http://localhost/v1/admin/cadastral/import-jobs" \
  -H "Content-Type: application/json" \
  -H "X-Admin-Token: <ADMIN_TOKEN>" \
  -d '{"release_id":1,"source_path":"/Users/jun/server/연속지적","total_files":11}'
```

## 4) 대용량 적재 실행
`api` 컨테이너 안에서 실행하는 것을 권장합니다.

```bash
cd /Users/jun/server
/Applications/Docker.app/Contents/Resources/bin/docker compose exec api \
  python /scripts/import_cadastral_geojson.py \
  --release-id 1 \
  --source-dir /data/uploads/연속지적 \
  --pattern 'AL_D002*.json' \
  --job-id 1 \
  --batch-size 2000 \
  --truncate-release \
  --mark-ready
```

## 5) 활성화(배포)
```bash
curl -X POST "http://localhost/v1/admin/cadastral/releases/1/activate" \
  -H "X-Admin-Token: <ADMIN_TOKEN>"
```

## 6) 앱 동작
- 앱은 타일 URL에 `?v=<tile_version>`를 붙입니다.
- 서버는 기본적으로 활성 릴리즈 버전을 사용하고, URL의 `v`가 있으면 해당 버전 캐시를 사용합니다.
- 엔드포인트: `GET /v1/tile-config`

## 운영 규칙
- 새 데이터 업로드는 항상 `새 release`로만 적재
- 활성화 전까지 앱에는 노출되지 않음
- 문제 발생 시 이전 release를 다시 `activate` 해서 즉시 롤백
