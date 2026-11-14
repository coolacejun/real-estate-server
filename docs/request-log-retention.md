# 접속·오류 로그 보유 및 자동 파기

적용: 2026-10-19 (Asia/Seoul). 목적: 최근 장애 재현, 오류 원인과 서비스 안정성 확인. 운영 기준은 **최대 7일**이며, 법정 의무 보존기간을 의미하지 않는다. 회원·결제·인증 감사 데이터의 보유정책은 이 문서의 대상이 아니다.

## 운영 적용

- web(server.py), API, Traefik의 표준출력·오류는 Docker `journald` 드라이버로 기록한다. 기존 `json-file` 컨테이너를 교체하여 기간 제한 없는 원본 파일을 제거했다. web은 즉시 출력하도록 `PYTHONUNBUFFERED=1`을 적용했다.
- VM `request-log-expiry.timer`가 매분 journal을 회전하고 만료 파일을 삭제한다. `MaxRetentionSec=604500s` 및 vacuum-time=604500s(7일에서 5분을 뺀 안전 여유), `MaxFileSec=60s`. 기록이 없어도 삭제한다. 저장공간 제한(512MB)에 도달하면 먼저 삭제될 수 있다. syslog 등으로 전달하지 않는다.
- API 별도 파일 `/Users/jun/server/data/uploads/logs/server.log`와 Tunnel 출력 `/Users/jun/.cloudflared/logs/tunnel.log`도 UTC 자정 회전 및 당일 포함 7개 날짜까지만 보관한다. API의 두 uvicorn logger는 같은 파일 handler를 공유한다.
- 파일 handler의 매분 정리와 호스트 `com.jun.server.log-retention` LaunchAgent의 매분 정리를 함께 적용했다. 애플리케이션이 유휴 또는 정지 상태여도 호스트 작업이 만료 파일을 지운다. 파일에도 5분 안전 여유를 두어 7일 한도에 앞서 삭제한다. 호스트/VM이 꺼져 있는 동안 작업은 실행할 수 없으며, 기동 시 정리를 재개한다.
- 별도 압축·로그 백업은 만들지 않는다. 로그 디렉터리는 Time Machine 제외 대상으로 설정했다. 삭제는 만료 journal/일별 로그 파일의 파일시스템 삭제다. 저장매체 전체의 물리적 덮어쓰기를 의미하지 않는다.
- 기존 API의 size 회전 로그 6개 및 Tunnel의 오래된 stdout/stderr 파일 2개는 마이그레이션 시 삭제했다. DB나 회원·결제 데이터는 삭제하지 않았다.

## 수집·백업 확인 범위

운영 Docker에는 전용 로그 수집 컨테이너가 없었다. VM의 rsyslog/syslog-ng/fluent-bit/vector/promtail 서비스도 활성 상태가 아니고, syslog/messages 사본도 없었다. Traefik access log는 활성화되지 않았다. API 관리자의 로그 스트림은 API 파일을 읽는 기능이며 별도 서버 사본을 쓰지 않는다.

현재 호스트의 `backup/`은 비어 있고 Time Machine 대상 및 로컬 APFS 스냅샷이 없었다. VM 외장 볼륨에도 스냅샷이 없고 Time Machine 제외 상태였다. 배포 출력에서 발견된 로그는 빌드·테스트 로그이며 운영 요청 로그 덤프는 발견되지 않았다. 복구 작업 로그는 디스크/컨테이너 상태 기록으로 요청 로그가 아니다.

과거 `mini-deploy.sh`는 디렉터리 전체를 원격 192.168.0.81로 rsync할 수 있었으므로 향후 복사에서 로그를 제외하도록 수정했다. 지정 SSH 키가 현재 없으며 원격 서버의 과거 사본은 확인하지 못했다. Cloudflare 사업자 자체 보안·분석 로그는 로컬 Tunnel 로그와 다르며, 관리 콘솔의 보유기간/별도 수집 여부는 이 환경에서 확인하지 못했다. 외부 사본이 없다고 단정하지 않는다.

## 검증과 운영

- 로컬 만료 경계·유휴 회전 테스트 2개 통과.
- API/Tunnel 실제 로그 폴더에 만료된 가상 파일을 만들었고 LaunchAgent의 예약 실행이 삭제했으며 현재 로그는 유지했다. 작업 종료코드 0.
- VM 삭제 서비스 Result=success/ExecMainStatus=0 및 매분 타이머 활성 확인.
- 홈페이지 및 API health HTTP 200, 공개 개인정보처리방침의 7일 문구 확인.
- 운영 환경변수·마운트·포트 범위 유지(기존 빈 HostIp는 같은 의미의 0.0.0.0으로 명시). PostgreSQL/Redis/동기화 컨테이너를 교체하지 않았다. 기존 DB 식별자를 검증하는 복구 baseline을 새 web/API/Traefik 컨테이너에 맞춰 갱신했다.

운영 compose: `/Users/jun/server/output/log-retention-20261019/runtime.private.json`. 환경값이 있으므로 외부 공유 금지. 저장소 `docker-compose.yml`에도 journald 설정을 반영했다. 새 VM에는 `scripts/install_request_log_journal_retention.sh`를 sudo sh로 적용해야 한다. API handler는 `api/app/request_log_retention.py`에 있다. 과거 runtime으로 되돌리면 보유기간 설정도 되돌아갈 수 있으므로 롤백 시 이 설정과 handler를 유지해야 한다.

확인 명령:

```sh
python3 -m unittest scripts/test_request_log_retention.py
launchctl print gui/$(id -u)/com.jun.server.log-retention
COLIMA_HOME=/Volumes/DB_EXTERNAL/colima-target LIMA_HOME=/Volumes/DB_EXTERNAL/colima-target/_lima colima ssh --profile external-pg -- sudo systemctl status request-log-expiry.timer
```
