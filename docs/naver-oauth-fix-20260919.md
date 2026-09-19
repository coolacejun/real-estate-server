# 네이버 웹 OAuth 수정 인수인계

이 후보는 중앙 API 코드를 바꾸지 않고 web submodule만 네이버 로그인 수정으로
고정한다. 운영의 실제 중복 callback 원인을 확정했다는 뜻은 아니다.

- 서버 기준: `dfb87aa18f15e02a0966692134da08acd8c541b2`
- 서버 브랜치: `fix/naver-oauth-state-20260919`
- 웹 저장소: https://github.com/noriddori-jpg/real_estate_web
- 웹 브랜치: `fix/naver-oauth-state-20260919`
- 웹 pin: `4d4c11b7c8075295c114b023476bd13157c56b0b`
- 서버 저장소: https://github.com/coolacejun/real-estate-server

웹의 해당 commit을 먼저 게시하고 원격 SHA를 확인한 뒤 이 서버 후보를 게시한다.
기존 shared release guard는 `work/shared-archives-20260911` 전용이므로 새 fix
브랜치는 위 정확한 remote branch/SHA와 gitlink를 별도로 확인해야 한다. 기존
guard를 완화하거나 생략하도록 수정하지 않았다.

담당자는 웹 fix를 검토하고 이 서버 후보의 정확한 web pin을 함께 적용한다.
서버 main만 선택하거나 오래된 web pin을 유지하면 수정이 반영되지 않는다.
이 작업은 Git 게시만을 위한 것이며 운영 배포·재시작·실로그인은 수행하지 않았다.

구현·동시성·보안 상세는 `web/docs/naver-oauth-replay-20260919.md`를 참조한다.
웹 SQLite에 흐름별 cookie 이름 열과 짧은 완료 결과 테이블을 반복 가능하게
추가한다. 완료 결과와 세션 생성/이전 세션 해제를 같은 트랜잭션에서 기록하며
동일 흐름·브라우저·provider·발급 세션이 확인된 재호출만 복귀시킨다. 원문 인증
code/state/cookie를 완료 기록에 저장하지 않는다. 중앙 PostgreSQL은 변경하지
않고 계정·잔액 이관도 없다. 복귀 query는 저장하지 않으며 네 가지 안전한
페이지로 정규화한다.

관련 격리 테스트 55개가 통과했다. pin의 소스가 테스트한 소스와 일치하고
renderer manifest, API/web 환경 calculator·data manifest 복사본도 일치한다.
조회·보고서·결제 계산 코드와 운영 환경값은 변경하지 않았다. 실제 적용 전 웹 DB
백업, origin/callback 설정, 프록시 scheme 및 같은 DB를 쓰는 웹 인스턴스의
버전 일치를 확인한다. 실행 버전을 되돌리더라도 additive SQLite schema와 기존
사용자 행은 삭제하지 않는다.
