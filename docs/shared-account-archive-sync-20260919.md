# 웹·앱 동일 네이버 계정과 공유 보관함

> 아래는 공유 보관함 단독 보완의 검증 기록이다. 사용자 후속 선택에 따른 OAuth/
> 파란색 포함 최종 통합 및 게시 대상은 `integrated-shared-oauth-blue-20260920.md`
> 를 따른다. 앱 469b84e는 유지하며 웹은 그 문서의 최종 통합 pin을 사용한다.

## 기존 구현과 이번 보완

이 기능은 이미 작업되어 있었다. 서버의 `archive_routes.py`, `archives.py`,
`shared_session.py`와 웹의 shared archive 경로, 모바일 `e0af5e7`의 선택 PDF
업로드/소유자·API origin 기록/커서 목록/최종 출력 소유자 보호가 기존 구현이다.
현재 원본 Flutter `5d83802`는 이 모바일 통합 변경 이전 버전이므로 그 원본만
읽으면 구현이 없는 것처럼 보인다. 실제 설치/운영 반영 여부는 별도 확인해야 한다.

이번 최종 모바일 기준은 원격 추적 통합 `9f47107c517d419f5b1cc8eb5f29560ef18695e4`다.
기존 구현을 재사용하고 다음만 보완했다.

- 이미 서버가 검증한 upload 단계는 재전송하지 않고 같은 요청으로 확정한다.
- 공유 파일을 내려받을 때 서버 SHA-256과 실제 bytes를 확인한다.
- 선택 PDF 업로드 성공 직후 공유 목록을 갱신한다. 확인창에서 계정이 바뀌면
  중단하며, 로컬 owner/origin 기록 실패를 안내하고 파일을 보존한다.
- 기존 디자인을 유지하면서 공유 보관함/기기 사본의 위치, 추가 차감 없음,
  선택 업로드와 삭제 범위를 명확히 안내한다.
- 웹 삭제 안내와 그 script 캐시 버전/manifest의 두 digest만 갱신한다.

새 서버 저장 API, 회원/잔액 이전, 이메일·닉네임 병합, 보고서 내용이나 가격
변경은 없다. 서버 소스 변경은 회귀 테스트와 이 문서이며 web pin만 함께 갱신한다.

## 실제 사용할 소스

- 서버: `C:/CodexWork/real-estate-server-archive-sync-20260919`, 기준 `dfb87aa18f15e02a0966692134da08acd8c541b2`.
- 웹: `C:/CodexWork/real-estate-web-archive-sync-20260919`, 기준 `a62feb346575a8683945ee32b53aa57c03a30d92`.
- **최종 앱**: `C:/CodexWork/real-estate-mobile-archive-final-20260919`, 브랜치 `work/shared-account-archive-followup-20260919`, 기준 `9f47107`.
- 웹 최종 로컬 commit: `c09fb532ac475945cd69f10d0f1878fef3312085`.
- 앱 최종 로컬 commit: `469b84e3a799a74d424b26492ad472a0ccc55a3a`.
- 초기 `real-estate-mobile-archive-sync-20260919`는 비교용 보존 draft다.
  5d83802에서 시작한 중복 구현이므로 빌드/배포 대상으로 사용하지 않는다.
- 기존 원본 dirty와 파란색 복원, OAuth fix `ec2e586`/`2df3ae8`, 별도 성능
  작업 `6d556d6`은 그대로 보존했다. 이번 작업이 그것들을 통합하거나 배포한 것은 아니다.

## 이용 방법

1. 웹과 앱에서 같은 네이버 계정으로 로그인한다. 검증된 provider subject,
   같은 OAuth client 범위, 웹 external account 매핑이 같은 중앙 사용자여야 한다.
2. 새 앱 최종 보고서는 생성 성공 때 이미 서버 보관함에 저장된다. 기기 저장
   버튼은 로컬 사본을 보존하며 보고서를 다시 생성하거나 건수를 또 차감하지 않는다.
3. 앱의 **공유 보관함**에서 웹·앱 서버 문서를 조회하고 PDF/HTML을 열거나
   내보낸다. 웹에서 저장한 문서도 같은 목록에 나온다.
4. 과거 기기 PDF는 **이 기기 보관함 → 보고서 더보기 → 공유 보관함에 저장**을
   선택하고 계정을 확인한다. 선택한 한 파일만 올라간다. 전체 자동 업로드는 없다.
5. 실패하면 원본을 유지하고 같은 계정/파일로 재시도한다. 응답 유실 뒤에도
   같은 requestId와 서버의 사용자별 해시 중복 방지를 사용한다. 서버 PDF 검증
   한도는 16MiB/200페이지이며 기존 서버 저장 문서는 재생성/재과금하지 않는다.
6. 기기 삭제는 공유 문서를 지우지 않는다. 웹 공유 보관함 삭제는 양쪽 서버
   목록에서 사라지지만 이미 기기에 내려받은 사본은 유지된다.

## 운영 반영 조건

Git 게시, Mac mini 접속/배포, 실제 OAuth, 운영 DB, 5180 재시작, APK 빌드/설치,
스토어 게시를 수행하지 않았다. 따라서 현재 사용자의 폰/공개 사이트에서 이미
동작한다고 주장하지 않는다. 운영 담당자는 기존 Mac mini 배포 경로를 사용한다.

- API와 web에 같은 `SHARED_ARCHIVES_ENABLED=true`, API에
  `ARCHIVE_UPLOADS_ENABLED=true`가 필요한 기존 기능이다. 기본 설정 파일의
  false는 운영 실값의 증거가 아니다. 기존 동작 플래그와 백업/롤백을 확인한다.
- 기존 additive DB 011/012와 중앙 계정/웹 세션 adapter, Traefik의
  `/api/v1/report-archives` 및 모바일 라우팅이 올바르게 반영돼 있어야 한다.
  기존 검토/마이그레이션 도구는 자동 실행하지 않는다.
- 앱 계정 API는 웹 계정과 같은 중앙 origin을 사용해야 한다. 개발 5180의
  SQLite를 운영 중앙 Postgres나 실사용자 자료와 섞지 않는다.
- 네이버 client ID 설정은 웹·API에서 같은 scope여야 한다. 서로 다른 client
  또는 오래된 미연결 웹 계정은 **409 검토 필요**를 유지한다. 같은 이메일이나
  표시 이름만으로 연결하지 않는다. 기존 미연결 계정이면 담당자가 검증된
  subject/client/external mapping 근거를 확인한 뒤 별도의 검토 절차를 밟아야 한다.
  사용자 가입 상태를 새 가입으로 위조하거나 잔액/파일을 임의 이전하지 않는다.
- 검증한 최종 모바일 소스를 기존 릴리스 절차로 앱 업데이트해야 한다.
  기존 성능 수정·원본 dirty·OAuth fix의 별도 통합 여부는 해당 담당자가 결정한다.

## 검증

- Flutter 서비스 40개와 widget/parser/기기 저장/action bar 23개, 합계 63개 통과.
- Flutter 변경 소스/테스트 8파일 analyze 문제 없음. 웹 직접 공유 계정/보고서
  인증 회귀 24개 통과. 전체 관련 테스트는 112개(63+25+24)다.
- 웹 script 구문, 새 cache 참조, renderer manifest와 환경 계약 검증 통과.
- 서버 공유 계정/보관함 25개 통과: 임시 PostgreSQL/SQLite의 실제 웹 cookie
  introspection과 모바일 Bearer, 동일 Naver subject 연결 및 양방향 저장/조회,
  타인/비인증/만료/취소 차단, 공유 삭제, 기존 PDF 업로드·재다운로드 비차감,
  멱등/동시 확정, 네이버 client scope/기존 미연결 계정의 검토 gate 유지.
- 테스트는 합성 계정·임시 PDF와 mock만 사용했다. 실제 사용자 자료는 업로드하지 않았다.
- 현재 결과/최종 SHA와 정적 검증은 `C:/CodexWork/tmp/shared-account-archive-sync-20260919.md`에 기록한다.
