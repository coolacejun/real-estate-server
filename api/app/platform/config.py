from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _csv(name: str, default: str = "") -> tuple[str, ...]:
    return tuple(item.strip() for item in os.getenv(name, default).split(",") if item.strip())


def _positive_int(name: str, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    value = max(minimum, value)
    return min(value, maximum) if maximum is not None else value


@dataclass(frozen=True)
class ProviderSettings:
    client_id: str
    client_secret: str
    authorization_url: str
    token_url: str
    userinfo_url: str
    scope: str

    @property
    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret)


@dataclass(frozen=True)
class PlatformSettings:
    database_url: str
    app_env: str
    oauth_callback_base_url: str
    oauth_redirect_allowlist: tuple[str, ...]
    oauth_state_ttl_seconds: int
    oauth_code_ttl_seconds: int
    access_token_ttl_seconds: int
    refresh_token_ttl_seconds: int
    report_max_body_bytes: int
    report_asset_max_bytes: int
    report_asset_max_count: int
    report_asset_dir: Path
    report_font_path: str
    report_renderer_url: str
    report_renderer_allowed_hosts: tuple[str, ...]
    report_renderer_timeout_seconds: int
    report_renderer_max_pdf_bytes: int
    store_verifier_mode: str
    apple_bundle_id: str
    apple_shared_secret: str
    apple_root_ca_file: str
    google_play_package_name: str
    google_play_service_account_file: str
    environment_data_dir: Path
    internal_service_token: str
    kakao: ProviderSettings
    naver: ProviderSettings
    google: ProviderSettings
    environment_analysis_enabled: bool = False
    shared_archives_enabled: bool = False
    archive_uploads_enabled: bool = False
    web_session_cookie_name: str = 'bl_session'
    web_session_introspection_url: str = 'http://web:5180/api/internal/platform-session'
    shared_web_origins: tuple[str, ...] = ('https://building-land.com', 'https://www.building-land.com')
    legacy_grant_enabled: bool = False
    legacy_notifications_enabled: bool = False
    legacy_policy_file: str = ''
    apple_iap_key_file: str = ''
    apple_iap_key_id: str = ''
    apple_iap_issuer_id: str = ''
    apple_app_id: str = ''
    google_rtdn_audience: str = ''
    google_rtdn_email: str = ''

    @classmethod
    def from_env(cls) -> "PlatformSettings":
        database_url = os.getenv("DATABASE_URL", "").strip()
        return cls(
            database_url=database_url,
            legacy_grant_enabled=os.getenv('LEGACY_GRANT_ENABLED', 'false').lower() == 'true',
            legacy_notifications_enabled=os.getenv('LEGACY_NOTIFICATIONS_ENABLED', 'false').lower() == 'true',
            legacy_policy_file=os.getenv('LEGACY_POLICY_FILE', ''),
            apple_iap_key_file=os.getenv('APPLE_IAP_KEY_FILE', ''),
            apple_iap_key_id=os.getenv('APPLE_IAP_KEY_ID', ''),
            apple_iap_issuer_id=os.getenv('APPLE_IAP_ISSUER_ID', ''),
            apple_app_id=os.getenv('APPLE_APP_ID', ''),
            google_rtdn_audience=os.getenv('GOOGLE_RTDN_AUDIENCE', ''),
            google_rtdn_email=os.getenv('GOOGLE_RTDN_EMAIL', ''),
            app_env=os.getenv("APP_ENV", "production").strip().lower(),
            oauth_callback_base_url=os.getenv(
                "MOBILE_OAUTH_CALLBACK_BASE_URL", "https://building-land.com"
            ).rstrip("/"),
            oauth_redirect_allowlist=_csv(
                "MOBILE_OAUTH_REDIRECT_ALLOWLIST", "buildingland://oauth/callback"
            ),
            oauth_state_ttl_seconds=_positive_int("MOBILE_OAUTH_STATE_TTL_SECONDS", 600, maximum=1800),
            oauth_code_ttl_seconds=_positive_int("MOBILE_OAUTH_CODE_TTL_SECONDS", 180, maximum=600),
            access_token_ttl_seconds=_positive_int("MOBILE_ACCESS_TOKEN_TTL_SECONDS", 900, maximum=3600),
            refresh_token_ttl_seconds=_positive_int(
                "MOBILE_REFRESH_TOKEN_TTL_SECONDS", 60 * 60 * 24 * 90, maximum=60 * 60 * 24 * 365
            ),
            report_max_body_bytes=_positive_int(
                "MOBILE_REPORT_MAX_BODY_BYTES", 64 * 1024 * 1024, minimum=1024 * 1024, maximum=128 * 1024 * 1024
            ),
            report_asset_max_bytes=_positive_int(
                "MOBILE_REPORT_ASSET_MAX_BYTES", 6 * 1024 * 1024, minimum=64 * 1024, maximum=16 * 1024 * 1024
            ),
            report_asset_max_count=_positive_int("MOBILE_REPORT_ASSET_MAX_COUNT", 8, maximum=16),
            report_asset_dir=Path(os.getenv("REPORT_ASSET_DIR", "/data/report-assets")).resolve(),
            report_font_path=os.getenv(
                "REPORT_FONT_PATH", "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
            ),
            report_renderer_url=os.getenv(
                "MOBILE_REPORT_RENDERER_URL",
                "http://web:5180/api/internal/mobile-report-pdf",
            ).strip(),
            report_renderer_allowed_hosts=_csv(
                "MOBILE_REPORT_RENDERER_ALLOWED_HOSTS", "web,localhost,127.0.0.1"
            ),
            report_renderer_timeout_seconds=_positive_int(
                "MOBILE_REPORT_RENDERER_TIMEOUT_SECONDS", 45, minimum=5, maximum=120
            ),
            report_renderer_max_pdf_bytes=_positive_int(
                "MOBILE_REPORT_RENDERER_MAX_PDF_BYTES",
                32 * 1024 * 1024,
                minimum=1024,
                maximum=128 * 1024 * 1024,
            ),
            store_verifier_mode=os.getenv("STORE_VERIFIER_MODE", "production").strip().lower(),
            apple_bundle_id=os.getenv("APPLE_BUNDLE_ID", "").strip(),
            apple_shared_secret=os.getenv("APPLE_SHARED_SECRET", "").strip(),
            apple_root_ca_file=os.getenv("APPLE_ROOT_CA_FILE", "").strip(),
            google_play_package_name=os.getenv("GOOGLE_PLAY_PACKAGE_NAME", "").strip(),
            google_play_service_account_file=os.getenv("GOOGLE_PLAY_SERVICE_ACCOUNT_FILE", "").strip(),
            environment_data_dir=Path(os.getenv("ENVIRONMENT_DATA_DIR", "/data/environment")).resolve(),
            environment_analysis_enabled=os.getenv("ENVIRONMENT_ANALYSIS_ENABLED", "false").strip().lower() == "true",
            shared_archives_enabled=os.getenv('SHARED_ARCHIVES_ENABLED', 'false').lower() == 'true',
            archive_uploads_enabled=os.getenv('ARCHIVE_UPLOADS_ENABLED', 'false').lower() == 'true',
            web_session_cookie_name=os.getenv('SESSION_COOKIE_NAME', 'bl_session'),
            web_session_introspection_url=os.getenv('WEB_SESSION_INTROSPECTION_URL', 'http://web:5180/api/internal/platform-session'),
            shared_web_origins=_csv('SHARED_WEB_ORIGINS', 'https://building-land.com,https://www.building-land.com'),
            internal_service_token=os.getenv("PLATFORM_INTERNAL_SERVICE_TOKEN", "").strip(),
            kakao=ProviderSettings(
                # The *_OAUTH_* names are explicit mobile overrides. The
                # legacy names keep the already configured web app usable
                # without copying credentials into a second secret source.
                client_id=(
                    os.getenv("KAKAO_OAUTH_CLIENT_ID", "").strip()
                    or os.getenv("KAKAO_CLIENT_ID", "").strip()
                ),
                client_secret=(
                    os.getenv("KAKAO_OAUTH_CLIENT_SECRET", "").strip()
                    or os.getenv("KAKAO_CLIENT_SECRET", "").strip()
                ),
                authorization_url="https://kauth.kakao.com/oauth/authorize",
                token_url="https://kauth.kakao.com/oauth/token",
                userinfo_url="https://kapi.kakao.com/v2/user/me",
                scope="account_email,profile_nickname",
            ),
            naver=ProviderSettings(
                client_id=(
                    os.getenv("NAVER_OAUTH_CLIENT_ID", "").strip()
                    or os.getenv("NAVER_CLIENT_ID", "").strip()
                ),
                client_secret=(
                    os.getenv("NAVER_OAUTH_CLIENT_SECRET", "").strip()
                    or os.getenv("NAVER_CLIENT_SECRET", "").strip()
                ),
                authorization_url="https://nid.naver.com/oauth2.0/authorize",
                token_url="https://nid.naver.com/oauth2.0/token",
                userinfo_url="https://openapi.naver.com/v1/nid/me",
                scope="name,email",
            ),
            google=ProviderSettings(
                client_id=os.getenv("GOOGLE_OAUTH_CLIENT_ID", "").strip(),
                client_secret=os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "").strip(),
                authorization_url="https://accounts.google.com/o/oauth2/v2/auth",
                token_url="https://oauth2.googleapis.com/token",
                userinfo_url="https://openidconnect.googleapis.com/v1/userinfo",
                scope="openid email profile",
            ),
        )

    def provider(self, name: str) -> ProviderSettings:
        if name not in {"kakao", "naver", "google"}:
            raise KeyError(name)
        return getattr(self, name)


@lru_cache(maxsize=1)
def get_settings() -> PlatformSettings:
    return PlatformSettings.from_env()
