from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Any


SCHEMA_VERSION = 1
RENDERER_VERSION = "web-a4-canonical-v3"
SUPPORTED_RENDERER_VERSIONS = frozenset(
    {"web-a4-canonical-v1", "web-a4-canonical-v2", RENDERER_VERSION}
)
MAX_CANONICAL_BYTES = 16 * 1024 * 1024
MAX_PAGES = 60
MAX_IMAGES = 12
MAX_IMAGE_BYTES = 6 * 1024 * 1024
MAX_STRING_LENGTH = 24_000

_PAGE_KEYS = {
    "pageKey",
    "reportType",
    "layout",
    "title",
    "footerTitle",
    "pageNo",
    "address",
    "reportDate",
    "createdAt",
    "placeName",
    "reportRows",
    "sourceRows",
    "mapImage",
    "mapCaption",
    "environmentPhoto",
    "environmentDataNotice",
    "opinionText",
    "opinionImages",
    "opinionPart",
    "opinionTotal",
    "opinionLineCount",
    "enforcementSnapshot",
    "brokerSections",
    "brokerHasContinuation",
    "brokerCode",
    "brokerFormData",
    "brokerRows",
}
_LAYOUTS = {"cover", "property-report", "opinion", "broker-disclosure"}
_DATA_IMAGE = re.compile(r"^data:image/(png|jpeg);base64,([A-Za-z0-9+/]+={0,2})$")
_UNSAFE_KEY = re.compile(r"(?:^|_)(?:html|script|style)$", re.IGNORECASE)


class CanonicalReportError(ValueError):
    pass


def _validate_image(value: str) -> str:
    match = _DATA_IMAGE.fullmatch(value)
    if not match:
        raise CanonicalReportError("report image must be an embedded PNG or JPEG")
    try:
        image = base64.b64decode(match.group(2), validate=True)
    except Exception as exc:
        raise CanonicalReportError("report image encoding is invalid") from exc
    if len(image) > MAX_IMAGE_BYTES:
        raise CanonicalReportError("report image is too large")
    expected = b"\x89PNG\r\n\x1a\n" if match.group(1) == "png" else b"\xff\xd8\xff"
    if not image.startswith(expected):
        raise CanonicalReportError("report image type does not match its content")
    return value


def _clean(value: Any, *, depth: int = 0, key: str = "") -> Any:
    if depth > 12:
        raise CanonicalReportError("report payload is too deeply nested")
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if not (-1e18 < float(value) < 1e18):
            raise CanonicalReportError("report number is outside the allowed range")
        return value
    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH and not value.startswith("data:image/"):
            raise CanonicalReportError("report text is too long")
        if value.startswith("data:image/"):
            return _validate_image(value)
        if "\x00" in value:
            raise CanonicalReportError("report text contains an invalid character")
        return value
    if isinstance(value, list):
        if len(value) > 500:
            raise CanonicalReportError("report list is too long")
        return [_clean(item, depth=depth + 1, key=key) for item in value]
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            item_key = str(raw_key)
            if len(item_key) > 80 or _UNSAFE_KEY.search(item_key) or item_key in {"__proto__", "prototype", "constructor", "editedHtml", "isArchivedSnapshot"}:
                raise CanonicalReportError("report payload contains an unsupported field")
            cleaned[item_key] = _clean(raw_value, depth=depth + 1, key=item_key)
        return cleaned
    raise CanonicalReportError("report payload contains an unsupported value")


def validate_canonical_report(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
    if not isinstance(payload, dict):
        raise CanonicalReportError("report payload must be an object")
    canonical = payload.get("report") if isinstance(payload.get("report"), dict) else payload
    if type(canonical.get("schemaVersion")) is not int or canonical["schemaVersion"] != SCHEMA_VERSION:
        raise CanonicalReportError("unsupported report schema version")
    requested_renderer_version = str(canonical.get("rendererVersion") or "")
    if requested_renderer_version not in SUPPORTED_RENDERER_VERSIONS:
        raise CanonicalReportError("unsupported report renderer version")
    pages = canonical.get("pages")
    if not isinstance(pages, list) or not pages or len(pages) > MAX_PAGES:
        raise CanonicalReportError("report page count is invalid")
    image_count = 0
    clean_pages: list[dict[str, Any]] = []
    for raw_page in pages:
        if not isinstance(raw_page, dict) or set(raw_page) - _PAGE_KEYS:
            raise CanonicalReportError("report page contains an unsupported field")
        layout = str(raw_page.get("layout") or "")
        if layout not in _LAYOUTS:
            raise CanonicalReportError("report page layout is invalid")
        page = _clean(raw_page)
        encoded_page = json.dumps(page, ensure_ascii=False, separators=(",", ":"))
        image_count += encoded_page.count("data:image/")
        clean_pages.append(page)
    if image_count > MAX_IMAGES:
        raise CanonicalReportError("report contains too many images")

    included = canonical.get("includedItems")
    if not isinstance(included, list) or len(included) > 20:
        raise CanonicalReportError("report included items are invalid")
    clean_report = {
        "schemaVersion": SCHEMA_VERSION,
        "rendererVersion": requested_renderer_version,
        "mappingVersion": str(canonical.get("mappingVersion") or "legacy-mobile-v1")[:80],
        "reportId": str(canonical.get("reportId") or "")[:220],
        "title": str(canonical.get("title") or "Building Land Report")[:240],
        "address": str(canonical.get("address") or "")[:300],
        "includedItems": [str(item)[:80] for item in included if str(item).strip()],
        "officeInfo": _clean(canonical.get("officeInfo") or {}),
        "reportTheme": str(canonical.get("reportTheme") or "navy")[:40],
        "pages": clean_pages,
    }
    encoded = json.dumps(
        clean_report,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > MAX_CANONICAL_BYTES:
        raise CanonicalReportError("report payload is too large")
    digest = hashlib.sha256(encoded).hexdigest()
    claimed = str(payload.get("contentHash") or "").strip().lower()
    if claimed and claimed != digest:
        raise CanonicalReportError("report content hash does not match")
    return clean_report, digest
