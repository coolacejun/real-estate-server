from __future__ import annotations

import base64
import copy
import hashlib
import html
import json
import os
import re
import socket
import tempfile
import uuid
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from .config import PlatformSettings
from .canonical_v3_contract import (
    CanonicalReportError,
    validate_canonical_report as validate_semantic_report,
)
from .repository import assert_schema, connect, new_id, utcnow
from .security import REQUEST_ID_RE, SHA256_RE, sha256_bytes


SCHEMA_VERSION = 1
LEGACY_RENDERER_VERSION = "web-a4-canonical-v1"
SEMANTIC_RENDERER_VERSIONS = {"web-a4-canonical-v2", "web-a4-canonical-v3"}
RENDERER_PROFILES: dict[str, dict[str, Any]] = {
    "web-a4-v1": {"accent": "#16324f", "margin": 38, "fontSize": 9.5},
    "ios-a4-v1": {"accent": "#173d68", "margin": 40, "fontSize": 10},
    "android-a4-v1": {"accent": "#174766", "margin": 36, "fontSize": 9.5},
}
ALLOWED_LAYOUTS = {"cover", "property-report", "broker-disclosure", "opinion"}
DATA_URI_RE = re.compile(r"^data:(image/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=\r\n]+)$", re.IGNORECASE)
ASSET_URI_RE = re.compile(r"^asset://([0-9a-f-]{36})$")
CHROMIUM_PRODUCER_RE = re.compile(
    rb"/Producer\s*\((?:[^)]*(?:Skia|Chrom(?:e|ium))[^)]*)\)", re.IGNORECASE
)
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
    if declared_renderer and declared_renderer not in (
        set(RENDERER_PROFILES) | {LEGACY_RENDERER_VERSION} | SEMANTIC_RENDERER_VERSIONS
    ):
        raise HTTPException(status_code=422, detail="unsupported rendererVersion")
    if declared_renderer in SEMANTIC_RENDERER_VERSIONS:
        try:
            report, _ = validate_semantic_report({"report": report})
        except (CanonicalReportError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="invalid semantic canonical report") from exc
    profile = str(requested_profile or "").strip()
    if profile and profile not in RENDERER_PROFILES:
        raise HTTPException(status_code=422, detail="unsupported rendererProfile")
    if not profile:
        if declared_renderer in {LEGACY_RENDERER_VERSION} | SEMANTIC_RENDERER_VERSIONS:
            profile = "web-a4-v1"
        elif declared_renderer in RENDERER_PROFILES:
            profile = declared_renderer
        else:
            raise HTTPException(status_code=422, detail="unsupported rendererVersion")
    response_renderer = (
        declared_renderer
        if declared_renderer in {LEGACY_RENDERER_VERSION} | SEMANTIC_RENDERER_VERSIONS
        else profile
    )
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

            candidates = [
                Path(settings.report_font_path),
                Path("/usr/share/fonts/truetype/nanum/NanumGothic.ttf"),
                Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
                Path("/usr/share/fonts/opentype/noto/NotoSansCJKkr-Regular.otf"),
            ]
            font_path = next((candidate for candidate in candidates if candidate.is_file()), None)
            if font_path is None:
                _REGISTERED_FONT = "Helvetica"
            else:
                pdfmetrics.registerFont(TTFont("BuildingLandReport", font_path, subfontIndex=0))
                _REGISTERED_FONT = "BuildingLandReport"
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


def _maybe_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return value
    try:
        return json.loads(stripped)
    except (TypeError, ValueError):
        return value


def _friendly_value(value: Any, limit: int = 700) -> str:
    value = _maybe_json(value)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "예" if value else "아니오"
    if isinstance(value, list):
        parts = [_friendly_value(item, 180) for item in value]
        return ", ".join(part for part in parts if part)[:limit]
    if isinstance(value, dict):
        name = _friendly_value(
            value.get("name") or value.get("label") or value.get("title") or value.get("mode"), 180
        )
        details: list[str] = []
        mode = _friendly_value(value.get("mode"), 80)
        if mode and mode != name:
            details.append(mode)
        minutes = _friendly_value(value.get("minutes"), 40)
        if minutes:
            details.append(minutes if minutes.endswith("분") else f"{minutes}분")
        detail = " ".join(details) or _friendly_value(
            value.get("time") or value.get("distance") or value.get("value"), 180
        )
        if name:
            return (f"{name} ({detail})" if detail else name)[:limit]
        parts = [_friendly_value(item, 180) for item in value.values()]
        return ", ".join(part for part in parts if part)[:limit]
    return str(value)[:limit]


def _report_row_groups(page: dict[str, Any]) -> list[list[dict[str, Any]]]:
    raw_groups = page.get("reportRows")
    if not isinstance(raw_groups, list):
        return []
    groups: list[list[dict[str, Any]]] = []
    for raw_group in raw_groups:
        candidates = raw_group if isinstance(raw_group, list) else [raw_group]
        cells: list[dict[str, Any]] = []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            label = _friendly_value(candidate.get("label"), 100)
            value = _friendly_value(candidate.get("value"), 700)
            if label or value:
                default_span = 6 if len(candidates) == 1 else 3
                try:
                    span = int(candidate.get("span") or default_span)
                except (TypeError, ValueError):
                    span = default_span
                cells.append(
                    {
                        "label": label,
                        "value": value,
                        "span": max(2, min(6, span)),
                    }
                )
        if cells:
            groups.append(cells)
    return groups


def _floor_rows(page: dict[str, Any]) -> list[tuple[str, str, str]]:
    source_rows = page.get("sourceRows")
    if isinstance(source_rows, dict) and isinstance(source_rows.get("floors"), list):
        result: list[tuple[str, str, str]] = []
        for item in source_rows["floors"]:
            if not isinstance(item, dict):
                continue
            floor = _friendly_value(item.get("floor") or item.get("floorName"), 80)
            usage = _friendly_value(item.get("usage") or item.get("mainPurpsCdNm"), 240)
            area = _friendly_value(item.get("area") or item.get("areaM2"), 120)
            if area and re.fullmatch(r"[\d,.]+", area):
                area = f"{area}㎡"
            structure = _friendly_value(item.get("structure") or item.get("strctCdNm"), 180)
            detail = " / ".join(part for part in (area, structure) if part)
            display = _friendly_value(item.get("display"), 500)
            if not (floor or usage or detail) and display:
                parts = [part.strip() for part in re.split(r"\s*[|/·]\s*", display, maxsplit=2)]
                floor = parts[0] if parts else ""
                usage = parts[1] if len(parts) > 1 else ""
                detail = parts[2] if len(parts) > 2 else ""
            if floor or usage or detail:
                result.append((floor, usage, detail))
        if result:
            return result
    result = []
    for group in _report_row_groups(page):
        for cell in group:
            if cell["label"] in {"층별개요", "층별 개요", "층 정보"}:
                parts = [part.strip() for part in re.split(r"\s*[|/·]\s*", cell["value"], maxsplit=2)]
                if parts:
                    result.append(
                        (
                            parts[0],
                            parts[1] if len(parts) > 1 else "",
                            parts[2] if len(parts) > 2 else "",
                        )
                    )
    return result


_BROKER_LABELS = {
    "landArea": "면적(㎡)",
    "landCategory": "지목",
    "useApprovalDate": "준공년도 (증·개축년도)",
    "officialBuildingUse": "건축물대장상 용도",
    "actualUse": "실제 용도",
    "structure": "구조",
    "direction": "방향",
    "seismicApply": "내진설계 적용여부",
    "seismicAbility": "내진능력",
    "violationStatus": "건축물대장상 위반건축물 여부",
    "violationDetail": "위반내용",
    "useArea": "용도지역",
    "useDistrict": "용도지구",
    "useZone": "용도구역",
    "buildingCoverageLimit": "건폐율 상한",
    "floorAreaRatioLimit": "용적률 상한",
    "districtPlan": "지구단위계획구역 및 도시·군관리계획",
    "otherRestrictions": "그 밖의 이용제한 및 거래규제사항",
    "permitDetails": "허가·신고 구역 여부",
    "speculationDetails": "투기지역 여부",
    "landTransactionPermit": "토지거래허가구역",
    "landSpeculationArea": "토지 투기지역",
    "housingSpeculationArea": "주택 투기지역",
    "overheatedSpeculationArea": "투기과열지구",
    "parkingType": "주차장 유형",
    "parkingDetail": "주차장 상세",
    "bus": "버스",
    "rail": "지하철",
    "elementarySchool": "초등학교",
    "middleSchool": "중학교",
    "highSchool": "고등학교",
}
_BROKER_SECTIONS = (
    (
        "① 중개대상물 확인 사항",
        (
            "landArea", "landCategory", "useApprovalDate", "officialBuildingUse",
            "actualUse", "structure", "direction", "seismicApply", "seismicAbility",
            "violationStatus", "violationDetail",
        ),
    ),
    (
        "③ 토지이용계획 및 거래규제",
        (
            "useArea", "useDistrict", "useZone", "buildingCoverageLimit",
            "floorAreaRatioLimit", "districtPlan", "otherRestrictions", "permitDetails",
            "speculationDetails", "landTransactionPermit", "landSpeculationArea",
            "housingSpeculationArea", "overheatedSpeculationArea",
        ),
    ),
    (
        "⑤ 입지조건 및 주차",
        ("bus", "rail", "parkingType", "parkingDetail", "elementarySchool", "middleSchool", "highSchool"),
    ),
)


def _broker_values(page: dict[str, Any]) -> dict[str, Any]:
    raw = page.get("brokerRows")
    if isinstance(raw, dict):
        return dict(raw)
    values: dict[str, Any] = {}
    if not isinstance(raw, list):
        return values
    for group in raw:
        candidates = group if isinstance(group, list) else [group]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            key = str(
                item.get("key") or item.get("field") or item.get("name") or item.get("label") or ""
            ).strip()
            if key:
                values[key] = item.get("value")
                continue
            for nested_key, nested_value in item.items():
                if nested_key not in {"label", "span"}:
                    values[str(nested_key)] = nested_value
    return values


def _broker_sections(page: dict[str, Any]) -> list[tuple[str, list[tuple[str, str]]]]:
    values = _broker_values(page)
    used: set[str] = set()
    sections: list[tuple[str, list[tuple[str, str]]]] = []
    for title, keys in _BROKER_SECTIONS:
        rows: list[tuple[str, str]] = []
        for key in keys:
            if key not in values:
                continue
            used.add(key)
            rendered = _friendly_value(values[key], 900)
            if rendered:
                rows.append((_BROKER_LABELS[key], rendered))
        if rows:
            sections.append((title, rows))
    extras = [
        _friendly_value(values[key], 900)
        for key in sorted(values)
        if key not in used and _friendly_value(values[key], 900)
    ]
    if extras:
        sections.append(("추가 정보", [(f"추가 정보 {index}", value) for index, value in enumerate(extras, 1)]))
    return sections


def _page_image_references(page: dict[str, Any]) -> list[tuple[str, str]]:
    images: list[tuple[str, str]] = []
    if isinstance(page.get("mapImage"), str):
        images.append(("지도", page["mapImage"]))
    raw_opinions = page.get("opinionImages")
    if isinstance(raw_opinions, list):
        for index, item in enumerate(raw_opinions, 1):
            if isinstance(item, str):
                images.append((f"의견 사진 {index}", item))
            elif isinstance(item, dict) and isinstance(item.get("src"), str):
                name = _friendly_value(item.get("name"), 100) or f"의견 사진 {index}"
                images.append((name, item["src"]))
    return images


def _enforcement_rows(value: Any) -> list[tuple[str, str]]:
    value = _maybe_json(value)
    if not isinstance(value, dict):
        rendered = _friendly_value(value, 1200)
        return [("위반건축물 정보", rendered)] if rendered else []
    labels = {
        "isViolation": "위반 여부", "violation": "위반 여부", "status": "상태",
        "description": "상세 내용", "source": "출처", "checkedAt": "확인 시각",
    }
    rows: list[tuple[str, str]] = []
    unknown = 1
    for key, item in value.items():
        rendered = _friendly_value(item, 800)
        if not rendered:
            continue
        label = labels.get(str(key))
        if label is None:
            label = f"추가 확인 정보 {unknown}"
            unknown += 1
        rows.append((label, rendered))
    return rows


_INCLUDED_ITEM_LABELS = {
    "cover": "표지",
    "building": "건축물정보",
    "land": "토지정보",
    "cadastre": "지적도",
    "cadastral": "지적도",
    "ai": "주변환경분석",
    "environment": "주변환경분석",
    "broker": "중개대상물 확인·설명",
    "brokerDisclosure": "중개대상물 확인·설명",
    "opinion": "설명 및 의견",
    "description": "설명 및 의견",
    "enforcement": "위반건축물 검토",
}


def _included_item_labels(items: list[Any]) -> str:
    return ", ".join(
        _INCLUDED_ITEM_LABELS.get(str(item), _friendly_value(item, 100))
        for item in items
        if _friendly_value(item, 100)
    )


def _page_lines(page: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for label, key in (("주소", "address"), ("장소", "placeName"), ("작성일", "createdAt")):
        if page.get(key):
            lines.append(f"{label}: {_friendly_value(page[key])}")
    for group in _report_row_groups(page):
        rendered = [f"{cell['label']}: {cell['value']}" if cell["label"] else cell["value"] for cell in group]
        if rendered:
            lines.append("  |  ".join(rendered))
    for section, rows in _broker_sections(page):
        lines.append(section)
        lines.extend(f"{label}: {value}" for label, value in rows)
    if page.get("opinionText"):
        lines.extend(str(page["opinionText"]).splitlines())
    if page.get("environmentDataNotice"):
        lines.append(_friendly_value(page["environmentDataNotice"], 800))
    lines.extend(f"{label}: {value}" for label, value in _enforcement_rows(page.get("enforcementSnapshot")))
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
    try:
        raw = target.read_bytes()
    except OSError as exc:
        raise HTTPException(status_code=410, detail="report asset is unavailable") from exc
    if not raw or len(raw) > settings.report_asset_max_bytes:
        raise HTTPException(status_code=410, detail="report asset size is invalid")
    try:
        expected_size = int(record.get("byteSize"))
    except (TypeError, ValueError):
        expected_size = len(raw)
    expected_hash = str(record.get("contentHash") or "").lower()
    if expected_size != len(raw) or not SHA256_RE.fullmatch(expected_hash) or sha256_bytes(raw) != expected_hash:
        raise HTTPException(status_code=410, detail="report asset integrity check failed")
    content_type = str(record.get("contentType") or "")
    if content_type not in {"image/png", "image/jpeg", "image/webp"}:
        raise HTTPException(status_code=410, detail="report asset type is invalid")
    try:
        from PIL import Image

        with Image.open(BytesIO(raw)) as image:
            image.verify()
        with Image.open(BytesIO(raw)) as image:
            expected_format = {"image/png": "PNG", "image/jpeg": "JPEG", "image/webp": "WEBP"}[content_type]
            if image.format != expected_format or image.width * image.height > 40_000_000:
                raise ValueError("image format or dimensions are invalid")
    except Exception as exc:
        raise HTTPException(status_code=410, detail="report asset content is invalid") from exc
    return raw


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


def _render_reportlab_pdf(
    settings: PlatformSettings,
    canonical: CanonicalReport,
    *,
    asset_manifest: list[dict[str, Any]] | None = None,
) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.colors import HexColor
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.platypus import Image as FlowImage
        from reportlab.platypus import KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
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
    label_style = ParagraphStyle(
        "ReportLabel",
        parent=body_style,
        fontSize=max(body_size - 1, 8),
        leading=body_size + 3,
        textColor=HexColor("#374151"),
        spaceAfter=0,
    )
    header_style = ParagraphStyle(
        "ReportHeader",
        parent=label_style,
        textColor=colors.white,
    )
    section_style = ParagraphStyle(
        "ReportSection",
        parent=body_style,
        fontSize=body_size + 1,
        leading=body_size + 5,
        textColor=HexColor(profile["accent"]),
        spaceBefore=7,
        spaceAfter=5,
    )
    note_style = ParagraphStyle(
        "ReportNote",
        parent=body_style,
        fontSize=max(body_size - 1, 8),
        leading=body_size + 3,
        textColor=HexColor("#4b5563"),
    )
    content_width = page_width - margin * 2

    def paragraph(value: Any, style: ParagraphStyle = body_style) -> Paragraph:
        text = html.escape(_friendly_value(value, 5000)).replace("\n", "<br/>") or "&nbsp;"
        return Paragraph(text, style)

    def simple_table(rows: list[tuple[str, str]], *, header: tuple[str, str] | None = None) -> Table:
        data: list[list[Any]] = []
        repeat_rows = 0
        if header:
            data.append([paragraph(header[0], header_style), paragraph(header[1], header_style)])
            repeat_rows = 1
        data.extend([paragraph(label, label_style), paragraph(value)] for label, value in rows)
        table = Table(
            data,
            colWidths=[content_width * 0.29, content_width * 0.71],
            repeatRows=repeat_rows,
            splitByRow=1,
            splitInRow=1,
            hAlign="LEFT",
        )
        commands: list[tuple[Any, ...]] = [
            ("FONTNAME", (0, 0), (-1, -1), font),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.35, HexColor("#cbd5e1")),
            ("BACKGROUND", (0, 0), (0, -1), HexColor("#f1f5f9")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]
        if header:
            commands.extend(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), HexColor(profile["accent"])),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ]
            )
        table.setStyle(TableStyle(commands))
        return table

    def field_table(groups: list[list[dict[str, Any]]]) -> Table | None:
        data: list[list[Any]] = []
        spans: list[tuple[Any, ...]] = []
        for group in groups:
            row: list[Any] = []
            column = 0
            for cell in group:
                if column >= 6:
                    break
                width = min(int(cell["span"]), 6 - column)
                if width < 2:
                    break
                row.extend([paragraph(cell["label"], label_style), paragraph(cell["value"])])
                row.extend([""] * (width - 2))
                if width > 2:
                    spans.append(("SPAN", (column + 1, len(data)), (column + width - 1, len(data))))
                column += width
            row.extend([""] * (6 - len(row)))
            data.append(row)
        if not data:
            return None
        table = Table(
            data,
            colWidths=[content_width * factor for factor in (0.12, 0.20, 0.18, 0.12, 0.20, 0.18)],
            splitByRow=1,
            splitInRow=1,
            hAlign="LEFT",
        )
        commands: list[tuple[Any, ...]] = [
            ("FONTNAME", (0, 0), (-1, -1), font),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("GRID", (0, 0), (-1, -1), 0.35, HexColor("#cbd5e1")),
            ("BACKGROUND", (0, 0), (-1, -1), colors.white),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]
        for row_index, group in enumerate(groups):
            column = 0
            for cell in group:
                if column >= 6:
                    break
                span = min(int(cell["span"]), 6 - column)
                if span < 2:
                    break
                commands.append(("BACKGROUND", (column, row_index), (column, row_index), HexColor("#f1f5f9")))
                column += span
        commands.extend(spans)
        table.setStyle(TableStyle(commands))
        return table

    def floor_table(rows: list[tuple[str, str, str]]) -> Table:
        data = [[paragraph("층", header_style), paragraph("용도", header_style), paragraph("면적 / 구조", header_style)]]
        data.extend([paragraph(floor), paragraph(usage), paragraph(detail)] for floor, usage, detail in rows)
        table = Table(
            data,
            colWidths=[content_width * 0.17, content_width * 0.38, content_width * 0.45],
            repeatRows=1,
            splitByRow=1,
            splitInRow=1,
            hAlign="LEFT",
        )
        table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, -1), font),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("GRID", (0, 0), (-1, -1), 0.35, HexColor("#cbd5e1")),
                    ("BACKGROUND", (0, 0), (-1, 0), HexColor(profile["accent"])),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, HexColor("#f8fafc")]),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        return table

    story: list[Any] = []
    image_streams: list[BytesIO] = []
    pages = canonical.report["pages"]
    for index, page in enumerate(pages):
        if index:
            story.append(PageBreak())
        story.append(Paragraph(html.escape(_display(page.get("title") or canonical.report.get("title"), 160)), title_style))
        address = page.get("address") or canonical.report.get("address")
        if address:
            story.extend([simple_table([("주소", _friendly_value(address, 700))]), Spacer(1, 3 * mm)])

        layout = page.get("layout")
        groups = _report_row_groups(page)
        if layout == "cover":
            cover_rows: list[tuple[str, str]] = []
            if page.get("placeName"):
                cover_rows.append(("대상", _friendly_value(page["placeName"])))
            if page.get("createdAt"):
                cover_rows.append(("작성일", _friendly_value(page["createdAt"])))
            items = canonical.report.get("includedItems")
            if isinstance(items, list) and items:
                cover_rows.append(("포함 항목", _included_item_labels(items)))
            if cover_rows:
                story.extend([simple_table(cover_rows), Spacer(1, 3 * mm)])
        elif layout == "broker-disclosure":
            for section_title, rows in _broker_sections(page):
                story.extend(
                    [KeepTogether([paragraph(section_title, section_style), simple_table(rows)]), Spacer(1, 2 * mm)]
                )
        else:
            floors = _floor_rows(page)
            floor_labels = {"층별개요", "층별 개요", "층 정보"}
            filtered_groups = [
                [cell for cell in group if cell["label"] not in floor_labels]
                for group in groups
            ]
            table = field_table([group for group in filtered_groups if group])
            if table is not None:
                story.extend([table, Spacer(1, 3 * mm)])
            if floors:
                story.extend([paragraph("층별 개요", section_style), floor_table(floors), Spacer(1, 3 * mm)])
            enforcement = _enforcement_rows(page.get("enforcementSnapshot"))
            if enforcement:
                story.extend([paragraph("위반건축물 확인", section_style), simple_table(enforcement), Spacer(1, 3 * mm)])
            if page.get("environmentDataNotice"):
                story.extend([paragraph("자료 안내", section_style), paragraph(page["environmentDataNotice"], note_style)])

        if page.get("opinionText"):
            story.extend([paragraph("의견", section_style), paragraph(page["opinionText"])])
        story.append(Spacer(1, 3 * mm))
        for image_label, image_value in _page_image_references(page)[:6]:
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
                story.extend([paragraph(image_label, section_style), flow_image, Spacer(1, 4 * mm)])
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


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


def _renderer_endpoint(settings: PlatformSettings) -> str:
    raw = settings.report_renderer_url
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    allowed = {item.lower() for item in settings.report_renderer_allowed_hosts}
    if (
        parsed.scheme not in {"http", "https"}
        or not host
        or host not in allowed
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path != "/api/internal/mobile-report-pdf"
    ):
        raise HTTPException(status_code=503, detail="canonical PDF renderer endpoint is invalid")
    if settings.app_env == "production" and parsed.scheme != "http":
        # Production uses the isolated compose network. TLS endpoints are supported
        # outside production, but an external hostname is never accepted implicitly.
        raise HTTPException(status_code=503, detail="canonical PDF renderer endpoint is invalid")
    return raw


def _renderer_image_data_uri(
    value: object,
    settings: PlatformSettings,
    asset_manifest: list[dict[str, Any]] | None,
) -> str:
    if not isinstance(value, str) or not (DATA_URI_RE.fullmatch(value) or ASSET_URI_RE.fullmatch(value)):
        raise HTTPException(status_code=422, detail="report image reference is invalid")
    raw = _asset_bytes(value, settings, asset_manifest)
    if raw is None:
        raise HTTPException(status_code=410, detail="report asset metadata is unavailable")
    mime = _asset_content_type(value, asset_manifest)
    return f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"


def _renderer_report_payload(
    settings: PlatformSettings,
    canonical: CanonicalReport,
    asset_manifest: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    report = copy.deepcopy(canonical.report)
    images: dict[str, str] = {}
    semantic = canonical.response_renderer_version in SEMANTIC_RENDERER_VERSIONS

    def bundled_reference(value: object) -> str:
        data_uri = _renderer_image_data_uri(value, settings, asset_manifest)
        digest = sha256_bytes(data_uri.encode("ascii"))
        images[digest] = data_uri
        # Semantic v2/v3 validates the complete inline report before the web
        # renderer materializes images. Restore archived references here too.
        return data_uri if semantic else f"renderer-asset://{digest}"

    for page in report["pages"]:
        map_image = page.get("mapImage")
        if map_image is not None and map_image != "":
            page["mapImage"] = bundled_reference(map_image)
        photo = page.get("environmentPhoto")
        if photo is not None and photo != "":
            if not isinstance(photo, dict) or not isinstance(photo.get("src"), str):
                raise HTTPException(status_code=422, detail="environmentPhoto is invalid")
            page["environmentPhoto"] = {**photo, "src": bundled_reference(photo["src"])}
        opinions = page.get("opinionImages")
        if opinions is None:
            continue
        if not isinstance(opinions, list) or len(opinions) > 6:
            raise HTTPException(status_code=422, detail="opinionImages is invalid")
        normalized: list[object] = []
        for item in opinions:
            if isinstance(item, str):
                normalized.append(bundled_reference(item))
            elif isinstance(item, dict) and isinstance(item.get("src"), str):
                normalized.append({**item, "src": bundled_reference(item["src"])})
            else:
                raise HTTPException(status_code=422, detail="opinionImages is invalid")
        page["opinionImages"] = normalized
    if len(images) > settings.report_asset_max_count:
        raise HTTPException(status_code=413, detail="report contains too many image assets")
    if semantic:
        try:
            report, _ = validate_semantic_report({"report": report, "contentHash": canonical.content_hash})
        except (CanonicalReportError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="semantic report integrity check failed") from exc
        return {"report": report, "rendererProfile": canonical.renderer_profile, "contentHash": canonical.content_hash}
    return {"report": report, "rendererProfile": canonical.renderer_profile, "assetBundle": images}


def _renderer_error_detail(raw: bytes) -> str:
    try:
        payload = json.loads(raw[:4096].decode("utf-8"))
    except (UnicodeDecodeError, ValueError, TypeError):
        return "canonical PDF renderer rejected the request"
    if not isinstance(payload, dict):
        return "canonical PDF renderer rejected the request"
    detail = str(payload.get("detail") or "").strip()
    return detail[:240] if detail else "canonical PDF renderer rejected the request"


def _request_renderer_pdf(settings: PlatformSettings, payload: dict[str, Any]) -> bytes:
    if not settings.internal_service_token:
        raise HTTPException(status_code=503, detail="canonical PDF renderer authentication is not configured")
    body = _canonical_json_bytes(payload)
    request = Request(
        _renderer_endpoint(settings),
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "X-Internal-Service-Token": settings.internal_service_token,
        },
    )
    opener = build_opener(ProxyHandler({}), _NoRedirect())
    try:
        with opener.open(request, timeout=settings.report_renderer_timeout_seconds + 5) as response:
            if response.headers.get("X-Report-Renderer") != "chromium-skia":
                raise HTTPException(status_code=502, detail="canonical PDF renderer identity is invalid")
            length_header = response.headers.get("Content-Length")
            if length_header:
                try:
                    if int(length_header) > settings.report_renderer_max_pdf_bytes:
                        raise HTTPException(status_code=502, detail="canonical PDF renderer output is too large")
                except ValueError as exc:
                    raise HTTPException(status_code=502, detail="canonical PDF renderer length is invalid") from exc
            result = response.read(settings.report_renderer_max_pdf_bytes + 1)
    except HTTPException:
        raise
    except HTTPError as exc:
        status = exc.code if exc.code in {413, 422, 502, 503, 504} else 502
        raise HTTPException(status_code=status, detail=_renderer_error_detail(exc.read(4096))) from exc
    except (socket.timeout, TimeoutError) as exc:
        raise HTTPException(status_code=504, detail="canonical PDF renderer timed out") from exc
    except URLError as exc:
        if isinstance(exc.reason, (socket.timeout, TimeoutError)):
            raise HTTPException(status_code=504, detail="canonical PDF renderer timed out") from exc
        raise HTTPException(status_code=503, detail="canonical PDF renderer is unavailable") from exc
    except OSError as exc:
        raise HTTPException(status_code=503, detail="canonical PDF renderer is unavailable") from exc
    if len(result) > settings.report_renderer_max_pdf_bytes:
        raise HTTPException(status_code=502, detail="canonical PDF renderer output is too large")
    if not result.startswith(b"%PDF-") or not CHROMIUM_PRODUCER_RE.search(result[:256_000]):
        raise HTTPException(status_code=502, detail="canonical PDF renderer returned a non-Chromium artifact")
    return result


def render_pdf(
    settings: PlatformSettings,
    canonical: CanonicalReport,
    *,
    asset_manifest: list[dict[str, Any]] | None = None,
) -> bytes:
    """Render canonical JSON only through the isolated Chromium service.

    The ReportLab implementation remains in this module solely as a rollback
    reference and is intentionally never used as a success-path fallback.
    """

    return _request_renderer_pdf(
        settings,
        _renderer_report_payload(settings, canonical, asset_manifest),
    )


def render_html(
    settings: PlatformSettings,
    canonical: CanonicalReport,
    *,
    asset_manifest: list[dict[str, Any]] | None = None,
) -> bytes:
    sections: list[str] = []
    for page in canonical.report["pages"]:
        lines = "".join(f"<p>{html.escape(line)}</p>" for line in _page_lines(page))
        images = [value for _, value in _page_image_references(page)]
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
