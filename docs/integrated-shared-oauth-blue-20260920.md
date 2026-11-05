# 공유 보관함·네이버 로그인·파란색 통합 Git 게시

사용자가 세 변경 모두의 Git 업로드를 선택해 기존 공유 작업 브랜치 위에 통합했다.
각 저장소의 게시 대상은 `work/shared-archives-20260911`이다. 기존 공개 기반과
server pre-push guard의 정확한 web pin 확인 흐름을 유지하며 main/master와
운영 배포 경로는 변경하지 않는다. 새 원격 브랜치는 만들지 않는다.

| 저장소 | 게시 전 공유 HEAD | 통합 소스 |
| --- | --- | --- |
| noriddori-jpg/real_estate_web | a62feb346575a8683945ee32b53aa57c03a30d92 | 1d3899411a5ed873f588fab23da761ecb6975c18 |
| coolacejun/real-estate-server | dfb87aa18f15e02a0966692134da08acd8c541b2 | 이 문서가 포함된 공유 통합 commit |
| coolacejun/real_estate | 9f47107c517d419f5b1cc8eb5f29560ef18695e4 | 469b84e3a799a74d424b26492ad472a0ccc55a3a |

로컬 격리 웹/서버 브랜치는 `work/integrated-shared-oauth-blue-20260920`이며 원격의
기존 공유 브랜치로 일반 fast-forward push한다. 웹의 c09fb532 공유 삭제 안내와
ec2e5868 OAuth 수정 이력을 병합하고 최신 화면에 색상·CSS cache만 복원했다.
서버의 86e77946 공유 회귀와 2df3ae81 OAuth handoff를 병합한 뒤, 서로 다른
web gitlink 충돌을 위 최종 통합 웹 commit으로 명시적으로 해결했다.
중앙 API runtime과 DB schema에는 추가 변경이 없다.

첫 서버 push는 기존 pre-push guard가 상속된 `GIT_DIR` 때문에 web 조회에도
서버 HEAD를 읽어 안전하게 중단됐다. guard의 Git 호출에서 저장소 지정 환경만
제거해 명시한 checkout을 검사하도록 고쳤다. 별도의 실제 임시 Git 저장소 두 개로
훅 환경 아래 HEAD 구분과 dirty web 검출을 회귀 검증한다. 원격 SHA/계약/clean
검사를 제거하거나 훅을 건너뛰지 않는다. API runtime에는 영향이 없다.

원본 dirty 파일과 초기 중복 모바일 draft, 별도 performance 변경은 포함하지
않았다. 모바일은 검증된 최종 469b84e를 그대로 사용한다. 원격 9f47107에 이미
포함된 이전 통합 이력은 유지하며, 별도의 legacy 기능을 새로 추가하지 않는다.

검증: 통합 웹 인증/세션/계정/보고서/원장 58개, 기존 화면/보관함/요금제 10개,
임시 PostgreSQL/SQLite의 서버 공유 보관함 25개(0 skip)가 통과했다. 네이버
subject/client scope와 미연결 기존 계정의 검토 gate, 소유자별 읽기/삭제 보호,
동시 callback/멱등 업로드 및 비차감 조건을 유지한다. HTML/CSS는 색상/cache
이외 내용이 동일하며 renderer digest와 API/web 환경 계약도 일치한다.
Flutter 63개 및 변경 파일 analyze는 동일 소스의 기존 성공 결과를 재사용한다.

게시 순서: 웹 일반 push → ls-remote로 위 SHA 확인 → 기존
`scripts/check_shared_release.py`와 pre-push guard 통과 → 서버 일반 push →
모바일 일반 push → 세 원격 HEAD 및 server web pin 확인. 원격이 앞서면 강제
교체하지 않는다. 자동 승인 심사가 거부하면 원격 쓰기를 중단하고 정확한
저장소/브랜치/SHA를 제시한다. 이 문서 자체는 push 성공의 증거가 아니다.

운영 담당자 단계는 기존 `shared-account-archive-sync-20260919.md`의 공유
플래그/라우팅/계정 매핑 조건과 `naver-oauth-fix-20260919.md`의 SQLite 백업,
정규 callback origin/프록시 scheme/웹 인스턴스 버전 확인을 따른다. 예전 문서의
단독 feature SHA 대신 위 통합 웹 pin을 사용한다. Git 게시만으로 운영 반영이나
휴대폰 업데이트가 완료됐다는 뜻은 아니다. 배포·SSH·실로그인·사용자 DB 이전·
실결제·APK 설치·스토어 게시 및 서명키 회전은 이 작업에서 수행하지 않는다.
