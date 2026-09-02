from __future__ import annotations

import base64
import copy
import hashlib
import html
import json
import os
import re
import tempfile
import uuid
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from .config import PlatformSettings
from .repository import assert_schema, connect, new_id, utcnow
from .security import REQUEST_ID_RE, SHA256_RE, sha256_bytes


SCHEMA_VERSION = 1
LEGACY_RENDERER_VERSION = "web-a4-canonical-v1"
RENDERER_PROFILES: dict[str, dict[str, Any]] = {
    "web-a4-v1": {"accent": "#16324f", "margin": 38, "fontSize": 9.5},
    "ios-a4-v1": {"accent": "#173d68", "margin": 40, "fontSize": 10},
    "android-a4-v1": {"accent": "#174766", "margin": 36, "fontSize": 9.5},
}
ALLOWED_LAYOUTS = {"cover", "property-report", "broker-disclosure", "opinion"}
DATA_URI_RE = re.compile(r"^data:(image/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=\r\n]+)$", re.IGNORECASE)
ASSET_URI_RE = re.compile(r"^asset://([0-9a-f-]{36})$")
_FONT_LOCK = Lock()
_REGISTERED_FONT: str | None = None


@dataclass(frozen=True)
class CanonicalReport:
    report: dict[str, Any]
    content_hash: str
    renderer_profile: str
    response_renderer_version: str


@dataclass(frozen=True)
class AssetRecord:
    id: str
    content_hash: str
    content_type: str
    storage_key: str
    byte_size: int

    def as_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "contentHash": self.content_hash,
            "contentType": self.content_type,
            "storageKey": self.storage_key,
            "byteSize": self.byte_size,
        }


def _canonical_json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _validate_tree(value: Any, *, depth: int = 0) -> None:
    if depth > 24:
        raise HTTPException(status_code=422, detail="canonical report nesting is too deep")
    if isinstance(value, dict):
        if len(value) > 500:
            raise HTTPException(status_code=422, detail="canonical report object is too large")
        for key, child in value.items():
            if not isinstance(key, str) or len(key) > 120:
                raise HTTPException(status_code=422, detail="canonical report has an invalid key")
            _validate_tree(child, depth=depth + 1)
    elif isinstance(value, list):
        if len(value) > 2000:
            raise HTTPException(status_code=422, detail="canonical report list is too large")
        for child in value:
            _validate_tree(child, depth=depth + 1)
    elif isinstance(value, str) and len(value) > 10 * 1024 * 1024:
        raise HTTPException(status_code=422, detail="canonical report string is too large")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise HTTPException(status_code=422, detail="canonical report contains an unsupported value")


def validate_canonical_report(
    value: object,
    *,
    requested_profile: object = None,
    expected_content_hash: object = None,
) -> CanonicalReport:
    if not isinstance(value, dict):
        raise HTTPException(status_code=422, detail="report must be an object")
    report = copy.deepcopy(value)
    _validate_tree(report)
    if report.get("schemaVersion") != SCHEMA_VERSION:
        raise HTTPException(status_code=422, detail="unsupported report schemaVersion")
    declared_renderer = str(report.get("rendererVersion") or "")
    profile = str(requested_profile or "").strip()
    if profile and profile not in RENDERER_PROFILES:
        raise HTTPException(status_code=422, detail="unsupported rendererProfile")
    if not profile:
        if declared_renderer == LEGACY_RENDERER_VERSION:
            profile = "web-a4-v1"
        elif declared_renderer in RENDERER_PROFILES:
            profile = declared_renderer
        else:
            raise HTTPException(status_code=422, detail="unsupported rendererVersion")
    response_renderer = declared_renderer if declared_renderer == LEGACY_RENDERER_VERSION else profile
    pages = report.get("pages")
    if not isinstance(pages, list) or not pages or len(pages) > 100:
        raise HTTPException(status_code=422, detail="report pages must contain 1 to 100 items")
    page_keys: set[str] = set()
    for page in pages:
        if not isinstance(page, dict):
            raise HTTPException(status_code=422, detail="each report page must be an object")
        page_key = str(page.get("pageKey") or "").strip()
        if not page_key or len(page_key) > 160 or page_key in page_keys:
            raise HTTPException(status_code=422, detail="report pageKey must be unique")
        page_keys.add(page_key)
        if page.get("layout") not in ALLOWED_LAYOUTS:
            raise HTTPException(status_code=422, detail="report page layout is unsupported")
    included_items = report.get("includedItems")
    if not isinstance(included_items, list) or any(not isinstance(item, str) for item in included_items):
        raise HTTPException(status_code=422, detail="includedItems must be a string list")
    content_hash = hashlib.sha256(_canonical_json_bytes(report)).hexdigest()
    expected = str(expected_content_hash or "").strip().lower()
    if expected and (not SHA256_RE.fullmatch(expected) or expected != content_hash):
        raise HTTPException(status_code=422, detail="contentHash does not match canonical report")
    return CanonicalReport(report, content_hash, profile, response_renderer)


def _asset_id(user_id: str, digest: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"building-land:{user_id}:{digest}"))


def _decode_inline_image(value: str, settings: PlatformSettings) -> tuple[bytes, str]:
    match = DATA_URI_RE.fullmatch(value)
    if match is None:
        raise HTTPException(status_code=422, detail="report image data URI is invalid")
    try:
        raw = base64.b64decode(match.group(2), validate=True)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="report image base64 is invalid") from exc
    if not raw or len(raw) > settings.report_asset_max_bytes:
        raise HTTPException(status_code=413, detail="report image exceeds the per-asset limit")
    content_type = match.group(1).lower()
    try:
        from PIL import Image

        with Image.open(BytesIO(raw)) as image:
            image.verify()
        with Image.open(BytesIO(raw)) as image:
            expected_format = {"image/png": "PNG", "image/jpeg": "JPEG", "image/webp": "WEBP"}[
                content_type
            ]
            if image.format != expected_format or image.width * image.height > 40_000_000:
                raise ValueError("image format or dimensions are invalid")
    except Exception as exc:
        raise HTTPException(status_code=422, detail="report image content is invalid") from exc
    return raw, content_type


def materialize_assets(
    settings: PlatformSettings, *, user_id: str, report: dict[str, Any]
) -> tuple[dict[str, Any], list[AssetRecord]]:
    sanitized = copy.deepcopy(report)
    records: dict[str, AssetRecord] = {}
    user_dir = settings.report_asset_dir / user_id
    try:
        user_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as exc:
        raise HTTPException(status_code=503, detail="report asset storage is unavailable") from exc

    def visit(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: visit(child) for key, child in value.items()}
        if isinstance(value, list):
            return [visit(child) for child in value]
        if not isinstance(value, str) or not value.startswith("data:image/"):
            return value
        raw, content_type = _decode_inline_image(value, settings)
        extension = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp"}[content_type]
        digest = sha256_bytes(raw)
        identifier = _asset_id(user_id, digest)
        storage_key = f"{user_id}/{digest}.{extension}"
        target = settings.report_asset_dir / storage_key
        if not target.is_file():
            try:
                handle, temporary = tempfile.mkstemp(prefix=".asset-", dir=user_dir)
                with os.fdopen(handle, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.chmod(temporary, 0o600)
                os.replace(temporary, target)
            except OSError as exc:
                try:
                    if "temporary" in locals() and os.path.exists(temporary):
                        os.unlink(temporary)
                except OSError:
                    pass
                raise HTTPException(status_code=503, detail="report asset could not be persisted") from exc
        records[identifier] = AssetRecord(identifier, digest, content_type, storage_key, len(raw))
        if len(records) > settings.report_asset_max_count:
            raise HTTPException(status_code=413, detail="report contains too many image assets")
        return f"asset://{identifier}"

    sanitized = visit(sanitized)
    return sanitized, list(records.values())


def _font_name(settings: PlatformSettings) -> str:
    global _REGISTERED_FONT
    if _REGISTERED_FONT is not None:
        return _REGISTERED_FONT
    with _FONT_LOCK:
        if _REGISTERED_FONT is not None:
            return _REGISTERED_FONT
        try:
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont

            if Path(settings.report_font_path).is_file():
                pdfmetrics.registerFont(TTFont("BuildingLandReport", settings.report_font_path, subfontIndex=0))
                _REGISTERED_FONT = "BuildingLandReport"
            else:
                _REGISTERED_FONT = "Helvetica"
        except Exception:
            _REGISTERED_FONT = "Helvetica"
        return _REGISTERED_FONT


def _display(value: Any, limit: int = 500) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "예" if value else "아니오"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    else:
        text = str(value)
    return text[:limit]


def _page_lines(page: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for key in ("address", "placeName", "createdAt"):
        if page.get(key):
            lines.append(f"{key}: {_display(page[key])}")
    for row_group_key in ("reportRows", "sourceRows", "brokerRows"):
        groups = page.get(row_group_key)
        if not isinstance(groups, list):
            continue
        for group in groups:
            cells = group if isinstance(group, list) else [group]
            values: list[str] = []
            for cell in cells:
                if isinstance(cell, dict):
                    label = _display(cell.get("label"), 100)
                    value = _display(cell.get("value"), 300)
                    values.append(f"{label}: {value}" if label else value)
                else:
                    values.append(_display(cell, 300))
            if values:
                lines.append("  |  ".join(values))
    if page.get("opinionText"):
        lines.extend(str(page["opinionText"]).splitlines())
    if page.get("environmentDataNotice"):
        lines.append(_display(page["environmentDataNotice"], 800))
    if page.get("enforcementSnapshot"):
        lines.append(_display(page["enforcementSnapshot"], 1200))
    return lines


def _asset_bytes(
    value: str, settings: PlatformSettings, asset_manifest: list[dict[str, Any]] | None
) -> bytes | None:
    data_match = DATA_URI_RE.fullmatch(value)
    if data_match:
        raw, _ = _decode_inline_image(value, settings)
        return raw
    asset_match = ASSET_URI_RE.fullmatch(value)
    if not asset_match or not asset_manifest:
        return None
    record = next((item for item in asset_manifest if item.get("id") == asset_match.group(1)), None)
    if not record:
        return None
    storage_key = str(record.get("storageKey") or "")
    target = (settings.report_asset_dir / storage_key).resolve()
    if settings.report_asset_dir not in target.parents or not target.is_file():
        raise HTTPException(status_code=410, detail="report asset is unavailable")
    return target.read_bytes()


def _asset_content_type(value: str, asset_manifest: list[dict[str, Any]] | None) -> str:
    data_match = DATA_URI_RE.fullmatch(value)
    if data_match:
        return data_match.group(1).lower()
    asset_match = ASSET_URI_RE.fullmatch(value)
    if asset_match and asset_manifest:
        record = next((item for item in asset_manifest if item.get("id") == asset_match.group(1)), None)
        content_type = str((record or {}).get("contentType") or "")
        if content_type in {"image/png", "image/jpeg", "image/webp"}:
            return content_type
    raise HTTPException(status_code=410, detail="report asset metadata is unavailable")


def render_pdf(
    settings: PlatformSettings,
    canonical: CanonicalReport,
    *,
    asset_manifest: list[dict[str, Any]] | None = None,
) -> bytes:
    try:
        from reportlab.lib.colors import HexColor
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.platypus import Image as FlowImage
        from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer
    except ImportError as exc:
        raise HTTPException(status_code=503, detail="canonical PDF renderer is unavailable") from exc

    profile = RENDERER_PROFILES[canonical.renderer_profile]
    buffer = BytesIO()
    font = _font_name(settings)
    page_width, page_height = A4
    margin = float(profile["margin"])
    body_size = float(profile["fontSize"])
    document = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=margin,
        rightMargin=margin,
        topMargin=56,
        bottomMargin=50,
        title=str(canonical.report.get("title") or "Building Land Report"),
        author="building-land",
        pageCompression=1,
    )
    title_style = ParagraphStyle(
        "ReportTitle",
        fontName=font,
        fontSize=17,
        leading=22,
        textColor=HexColor(profile["accent"]),
        spaceAfter=14,
    )
    body_style = ParagraphStyle(
        "ReportBody",
        fontName=font,
        fontSize=body_size,
        leading=body_size + 5,
        textColor=HexColor("#111827"),
        spaceAfter=5,
        wordWrap="CJK",
    )
    story: list[Any] = []
    image_streams: list[BytesIO] = []
    pages = canonical.report["pages"]
    for index, page in enumerate(pages):
        if index:
            story.append(PageBreak())
        story.append(Paragraph(html.escape(_display(page.get("title") or canonical.report.get("title"), 160)), title_style))
        for line in _page_lines(page):
            story.append(Paragraph(html.escape(line).replace("\n", "<br/>") or "&nbsp;", body_style))
        story.append(Spacer(1, 4 * mm))
        image_values: list[str] = []
        if isinstance(page.get("mapImage"), str):
            image_values.append(page["mapImage"])
        if isinstance(page.get("opinionImages"), list):
            image_values.extend(item for item in page["opinionImages"] if isinstance(item, str))
        for image_value in image_values[:6]:
            raw = _asset_bytes(image_value, settings, asset_manifest)
            if not raw:
                continue
            stream = BytesIO(raw)
            image_streams.append(stream)
            try:
                flow_image = FlowImage(stream)
                max_width, max_height = 170 * mm, 92 * mm
                scale = min(max_width / flow_image.imageWidth, max_height / flow_image.imageHeight, 1.0)
                flow_image.drawWidth = flow_image.imageWidth * scale
                flow_image.drawHeight = flow_image.imageHeight * scale
                story.extend([flow_image, Spacer(1, 4 * mm)])
            except Exception as exc:
                raise HTTPException(status_code=422, detail="report image could not be rendered") from exc

    def decorate(pdf_canvas: Any, doc: Any) -> None:
        pdf_canvas.saveState()
        pdf_canvas.setFillColor(HexColor(profile["accent"]))
        pdf_canvas.rect(0, page_height - 18, page_width, 18, stroke=0, fill=1)
        pdf_canvas.setFillColor(HexColor("#6b7280"))
        pdf_canvas.setFont(font, 8)
        pdf_canvas.drawString(margin, 28, _display(canonical.report.get("title") or "보고서", 100))
        pdf_canvas.drawRightString(page_width - margin, 28, f"{doc.page:02d}")
        pdf_canvas.restoreState()

    try:
        document.build(story, onFirstPage=decorate, onLaterPages=decorate)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="canonical PDF rendering failed") from exc
    result = buffer.getvalue()
    if not result.startswith(b"%PDF-"):
        raise HTTPException(status_code=500, detail="canonical PDF renderer returned invalid output")
    return result


def render_html(
    settings: PlatformSettings,
    canonical: CanonicalReport,
    *,
    asset_manifest: list[dict[str, Any]] | None = None,
) -> bytes:
    sections: list[str] = []
    for page in canonical.report["pages"]:
        lines = "".join(f"<p>{html.escape(line)}</p>" for line in _page_lines(page))
        images: list[str] = []
        for key in ("mapImage",):
            if isinstance(page.get(key), str):
                images.append(page[key])
        if isinstance(page.get("opinionImages"), list):
            images.extend(item for item in page["opinionImages"] if isinstance(item, str))
        image_html = ""
        for value in images:
            raw = _asset_bytes(value, settings, asset_manifest)
            if raw:
                mime = _asset_content_type(value, asset_manifest)
                image_html += f'<img alt="report attachment" src="data:{mime};base64,{base64.b64encode(raw).decode("ascii")}">'
        sections.append(
            f'<section><h2>{html.escape(_display(page.get("title") or "보고서"))}</h2>{lines}{image_html}</section>'
        )
    title = html.escape(_display(canonical.report.get("title") or "보고서"))
    body = "".join(sections)
    return (
        "<!doctype html><html lang=\"ko\"><head><meta charset=\"utf-8\">"
        "<meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; img-src data:; style-src 'unsafe-inline'\">"
        f"<title>{title}</title><style>body{{font-family:sans-serif;color:#111827;margin:32px}}"
        "section{page-break-after:always}img{max-width:100%;max-height:360px}p{white-space:pre-wrap}</style>"
        f"</head><body><h1>{title}</h1>{body}</body></html>"
    ).encode("utf-8")


def _restore_usage_credit(connection: Any, usage: dict[str, Any], error_code: str) -> None:
    if usage.get("refund_ledger_id") or not usage.get("debit_bucket"):
        return
    bucket = str(usage["debit_bucket"])
    user = connection.execute(
        f"SELECT {bucket}_remaining FROM platform_users WHERE id = %s FOR UPDATE",
        (usage["user_id"],),
    ).fetchone()
    balance = int(user[f"{bucket}_remaining"]) + 1
    connection.execute(
        f"UPDATE platform_users SET {bucket}_remaining = %s, updated_at = NOW() WHERE id = %s",
        (balance, usage["user_id"]),
    )
    ledger_id = new_id()
    connection.execute(
        """
        INSERT INTO platform_credit_ledger
          (id, user_id, bucket, delta, reason, idempotency_key, reference_type, reference_id, balance_after)
        VALUES (%s, %s, %s, 1, 'report_failure_refund', %s, 'report_usage', %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING
        """,
        (
            ledger_id, usage["user_id"], bucket,
            f"report-refund:{usage['id']}:{usage['attempt_count']}", str(usage["id"]), balance,
        ),
    )
    connection.execute(
        """
        UPDATE platform_report_usages
        SET status = 'failed', refund_ledger_id = %s, error_code = %s,
            failed_at = NOW(), updated_at = NOW()
        WHERE id = %s
        """,
        (ledger_id, error_code[:80], usage["id"]),
    )


def begin_final_usage(
    settings: PlatformSettings,
    *,
    user_id: str,
    request_id: str,
    canonical: CanonicalReport,
) -> dict[str, Any]:
    if not REQUEST_ID_RE.fullmatch(request_id):
        raise HTTPException(status_code=422, detail="requestId is invalid")
    with connect(settings) as connection:
        assert_schema(connection)
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"report:{user_id}:{request_id}",),
        )
        existing = connection.execute(
            "SELECT * FROM platform_report_usages WHERE user_id = %s AND request_id = %s FOR UPDATE",
            (user_id, request_id),
        ).fetchone()
        if existing is not None:
            existing = dict(existing)
            if existing["content_hash"] != canonical.content_hash:
                raise HTTPException(status_code=409, detail="requestId was used with different report content")
            if existing["status"] == "completed":
                return {"action": "completed", "usage": existing}
            if existing["status"] == "pending":
                if existing["reserved_at"] > utcnow() - timedelta(minutes=15):
                    raise HTTPException(status_code=409, detail="report request is already processing")
                _restore_usage_credit(connection, existing, "stale_reservation")
                existing["status"] = "failed"

        user = connection.execute(
            "SELECT free_remaining, paid_remaining FROM platform_users WHERE id = %s AND status = 'active' FOR UPDATE",
            (user_id,),
        ).fetchone()
        if user is None:
            raise HTTPException(status_code=401, detail="login required")
        if int(user["free_remaining"]) > 0:
            bucket = "free"
        elif int(user["paid_remaining"]) > 0:
            bucket = "paid"
        else:
            raise HTTPException(status_code=402, detail="사용 가능한 무료 또는 유료 보고서 건수가 없습니다.")
        balance = int(user[f"{bucket}_remaining"]) - 1
        connection.execute(
            f"UPDATE platform_users SET {bucket}_remaining = %s, updated_at = NOW() WHERE id = %s",
            (balance, user_id),
        )
        usage_id = str(existing["id"]) if existing is not None else new_id()
        attempt = int(existing["attempt_count"]) + 1 if existing is not None else 1
        ledger_id = new_id()
        connection.execute(
            """
            INSERT INTO platform_credit_ledger
              (id, user_id, bucket, delta, reason, idempotency_key, reference_type, reference_id, balance_after)
            VALUES (%s, %s, %s, -1, 'report_final', %s, 'report_usage', %s, %s)
            """,
            (ledger_id, user_id, bucket, f"report-debit:{usage_id}:{attempt}", usage_id, balance),
        )
        if existing is None:
            connection.execute(
                """
                INSERT INTO platform_report_usages
                  (id, user_id, request_id, content_hash, renderer_profile, renderer_version,
                   status, debit_bucket, debit_ledger_id, attempt_count)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, %s, 1)
                """,
                (
                    usage_id, user_id, request_id, canonical.content_hash,
                    canonical.renderer_profile, canonical.response_renderer_version, bucket, ledger_id,
                ),
            )
        else:
            connection.execute(
                """
                UPDATE platform_report_usages
                SET status = 'pending', renderer_profile = %s, renderer_version = %s,
                    debit_bucket = %s, debit_ledger_id = %s, refund_ledger_id = NULL,
                    attempt_count = %s, error_code = NULL, reserved_at = NOW(),
                    completed_at = NULL, failed_at = NULL, updated_at = NOW()
                WHERE id = %s
                """,
                (canonical.renderer_profile, canonical.response_renderer_version, bucket, ledger_id, attempt, usage_id),
            )
        usage = connection.execute("SELECT * FROM platform_report_usages WHERE id = %s", (usage_id,)).fetchone()
        return {"action": "render", "usage": dict(usage)}


def fail_final_usage(settings: PlatformSettings, usage_id: str, error_code: str) -> None:
    with connect(settings) as connection:
        usage = connection.execute(
            "SELECT * FROM platform_report_usages WHERE id = %s FOR UPDATE", (usage_id,)
        ).fetchone()
        if usage is not None and usage["status"] == "pending":
            _restore_usage_credit(connection, dict(usage), error_code)


def complete_final_usage(
    settings: PlatformSettings,
    *,
    usage: dict[str, Any],
    canonical: CanonicalReport,
    stored_report: dict[str, Any],
    assets: list[AssetRecord],
) -> str:
    archive_id = new_id()
    title = str(stored_report.get("title") or "제목 없는 보고서")[:240]
    address = str(stored_report.get("address") or "")[:500]
    included_items = stored_report.get("includedItems") or []
    with connect(settings) as connection:
        current = connection.execute(
            "SELECT * FROM platform_report_usages WHERE id = %s FOR UPDATE", (usage["id"],)
        ).fetchone()
        if current is None or current["status"] != "pending":
            raise HTTPException(status_code=409, detail="report request is no longer pending")
        for asset in assets:
            connection.execute(
                """
                INSERT INTO platform_report_assets
                  (id, user_id, content_hash, content_type, storage_key, byte_size)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, content_hash) DO NOTHING
                """,
                (asset.id, usage["user_id"], asset.content_hash, asset.content_type, asset.storage_key, asset.byte_size),
            )
        connection.execute(
            """
            INSERT INTO platform_report_archives
              (id, user_id, report_id, title, address, included_items, canonical_report,
               asset_manifest, schema_version, renderer_profile, renderer_version,
               mapping_version, content_hash, usage_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                archive_id, usage["user_id"], str(stored_report.get("reportId") or "")[:160] or None,
                title, address, Jsonb(included_items), Jsonb(stored_report), Jsonb([asset.as_json() for asset in assets]),
                SCHEMA_VERSION, canonical.renderer_profile, canonical.response_renderer_version,
                str(stored_report.get("mappingVersion") or "")[:120] or None,
                canonical.content_hash, usage["id"],
            ),
        )
        connection.execute(
            """
            UPDATE platform_report_usages
            SET status = 'completed', archive_id = %s, completed_at = NOW(), updated_at = NOW()
            WHERE id = %s
            """,
            (archive_id, usage["id"]),
        )
    return archive_id


def load_archive(
    settings: PlatformSettings, *, user_id: str, archive_id: str
) -> tuple[dict[str, Any], CanonicalReport]:
    try:
        parsed = str(uuid.UUID(archive_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="archive not found") from exc
    with connect(settings) as connection:
        row = connection.execute(
            """
            SELECT * FROM platform_report_archives
            WHERE id = %s AND user_id = %s AND deleted_at IS NULL AND status = 'ready'
            """,
            (parsed, user_id),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="archive not found")
    report = dict(row["canonical_report"])
    canonical = CanonicalReport(
        report=report,
        content_hash=str(row["content_hash"]),
        renderer_profile=str(row["renderer_profile"]),
        response_renderer_version=str(row["renderer_version"]),
    )
    return dict(row), canonical


def list_archives(settings: PlatformSettings, *, user_id: str) -> list[dict[str, Any]]:
    with connect(settings) as connection:
        rows = connection.execute(
            """
            SELECT id, title, address, status, saved_at, included_items, content_hash,
                   renderer_profile, renderer_version
            FROM platform_report_archives
            WHERE user_id = %s AND deleted_at IS NULL AND status = 'ready'
            ORDER BY saved_at DESC LIMIT 100
            """,
            (user_id,),
        ).fetchall()
    return [
        {
            "id": str(row["id"]),
            "title": row["title"],
            "address": row["address"],
            "status": row["status"],
            "savedAt": row["saved_at"].isoformat(),
            "includedItems": row["included_items"],
            "contentFormats": ["pdf", "html"],
            "contentHash": row["content_hash"],
            "rendererProfile": row["renderer_profile"],
            "rendererVersion": row["renderer_version"],
        }
        for row in rows
    ]
