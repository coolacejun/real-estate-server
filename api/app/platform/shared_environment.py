from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor, wait
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable


CALCULATION_VERSION = "environment-web-v2"
RADIUS_PROFILE = "web-v1"
CATEGORY_RADII = {"schools": 1500, "bus": 700, "rail": 3000, "parks": 1500, "amenities": 700, "securityLights": 500, "cctv": 500}
MAX_CACHE_ENTRIES = 256
CACHE_TTL_SECONDS = 60 * 60
CATEGORY_TIMEOUT_SECONDS = 8.0

REGION_KEYS_BY_CODE = {
    "11": "seoul",
    "26": "busan",
    "27": "daegu",
    "28": "incheon",
    "29": "gwangju",
    "30": "daejeon",
    "31": "ulsan",
    "36": "sejong",
    "41": "gyeonggi",
    "42": "gangwon",
    "43": "chungbuk",
    "44": "chungnam",
    "45": "jeonbuk",
    "46": "jeonnam",
    "47": "gyeongbuk",
    "48": "gyeongnam",
    "50": "jeju",
    "51": "gangwon",
    "52": "jeonbuk",
}

REGION_ALIASES = OrderedDict(
    [
        ("seoul", ("서울특별시", "서울시", "서울")),
        ("busan", ("부산광역시", "부산시", "부산")),
        ("daegu", ("대구광역시", "대구시", "대구")),
        ("incheon", ("인천광역시", "인천시", "인천")),
        ("gwangju", ("광주광역시", "광주")),
        ("daejeon", ("대전광역시", "대전시", "대전")),
        ("ulsan", ("울산광역시", "울산시", "울산")),
        ("sejong", ("세종특별자치시", "세종시", "세종")),
        ("gyeonggi", ("경기도", "경기")),
        ("gangwon", ("강원특별자치도", "강원도", "강원")),
        ("chungbuk", ("충청북도", "충북")),
        ("chungnam", ("충청남도", "충남")),
        ("jeonbuk", ("전북특별자치도", "전라북도", "전북")),
        ("jeonnam", ("전라남도", "전남")),
        ("gyeongbuk", ("경상북도", "경북")),
        ("gyeongnam", ("경상남도", "경남")),
        ("jeju", ("제주특별자치도", "제주도", "제주")),
    ]
)

AMENITY_EXCLUDE_KEYWORDS = (
    "골프",
    "스크린",
    "연습장",
    "사료",
    "동물",
    "펫",
    "애견",
    "학원",
    "교습",
    "독서실",
    "스터디",
    "PC방",
    "피시방",
    "노래",
    "당구",
    "헬스",
    "필라테스",
    "요가",
    "미용",
    "네일",
    "의류",
    "세탁",
    "철물",
    "공구",
    "인테리어",
)


class EnvironmentAnalysisValidationError(ValueError):
    pass


class EnvironmentAnalysisRateLimitError(RuntimeError):
    pass


_analysis_cache: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
_analysis_cache_lock = threading.Lock()
_rate_windows: OrderedDict[str, tuple[float, int]] = OrderedDict()
_rate_lock = threading.Lock()
_category_executor = ThreadPoolExecutor(max_workers=7, thread_name_prefix="environment-analysis")
_analysis_limit = threading.BoundedSemaphore(2)
_category_slots = threading.BoundedSemaphore(14)


def _submit_category(job: Callable[[], dict[str, Any]]) -> Future:
    # Timed-out filesystem reads may still run. Keep both running and queued
    # category jobs bounded even after the HTTP analysis has returned.
    if not _category_slots.acquire(blocking=False):
        future = Future()
        future.set_exception(EnvironmentAnalysisRateLimitError("environment category queue is full"))
        return future
    try:
        future = _category_executor.submit(job)
    except Exception:
        _category_slots.release()
        raise
    future.add_done_callback(lambda _: _category_slots.release())
    return future


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def enforce_rate_limit(identifier: str, *, limit: int = 60, window_seconds: int = 60) -> None:
    key = (identifier or "unknown")[:128]
    now = time.monotonic()
    with _rate_lock:
        started_at, count = _rate_windows.get(key, (now, 0))
        if now - started_at >= window_seconds:
            started_at, count = now, 0
        if count >= limit:
            raise EnvironmentAnalysisRateLimitError("environment analysis rate limit exceeded")
        _rate_windows[key] = (started_at, count + 1)
        _rate_windows.move_to_end(key)
        while len(_rate_windows) > 1024:
            _rate_windows.popitem(last=False)


def distance_meters(from_lat: float, from_lng: float, to_lat: float, to_lng: float) -> float:
    radius = 6_371_000.0
    lat1 = math.radians(from_lat)
    lat2 = math.radians(to_lat)
    delta_lat = math.radians(to_lat - from_lat)
    delta_lng = math.radians(to_lng - from_lng)
    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lng / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def format_distance(meters: float) -> str:
    if meters < 1000:
        return f"{math.floor(meters + 0.5):,}m"
    return f"{Decimal(meters / 1000).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)}km"


def format_radius(radius_meters: int) -> str:
    if radius_meters < 1000:
        return f"{radius_meters}m"
    value = radius_meters / 1000
    return f"{value:.1f}km" if radius_meters % 1000 else f"{int(value)}km"


def _as_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _first(mapping: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _clean(mapping.get(key))
        if value:
            return value
    return ""


def _safe_data_path(data_root: Path, *parts: str) -> Path:
    root = data_root.resolve()
    path = root.joinpath(*parts).resolve()
    if path != root and root not in path.parents:
        raise ValueError("invalid dataset path")
    return path


def region_key_for_request(payload: dict[str, Any]) -> str:
    pnu = _clean(payload.get("parcelId"))
    if len(pnu) >= 2 and pnu[:2] in REGION_KEYS_BY_CODE:
        return REGION_KEYS_BY_CODE[pnu[:2]]
    address = payload.get("address") if isinstance(payload.get("address"), dict) else {}
    text = f"{_clean(address.get('parcel'))} {_clean(address.get('road'))}".strip()
    for key, aliases in REGION_ALIASES.items():
        if any(text == alias or text.startswith(f"{alias} ") for alias in aliases):
            return key
    return ""


def validate_request(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise EnvironmentAnalysisValidationError("request body must be an object")
    allowed = {"location", "address", "parcelId", "buildingId", "radiusProfile", "calculationVersion"}
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise EnvironmentAnalysisValidationError(f"unsupported fields: {', '.join(unknown)}")

    location = payload.get("location")
    if not isinstance(location, dict):
        raise EnvironmentAnalysisValidationError("location is required")
    if set(location) - {"lat", "lng", "crs"}:
        raise EnvironmentAnalysisValidationError("location contains unsupported fields")
    lat = _as_float(location.get("lat"))
    lng = _as_float(location.get("lng"))
    if lat is None or not -90 <= lat <= 90:
        raise EnvironmentAnalysisValidationError("location.lat must be between -90 and 90")
    if lng is None or not -180 <= lng <= 180:
        raise EnvironmentAnalysisValidationError("location.lng must be between -180 and 180")
    crs = _clean(location.get("crs") or "EPSG:4326").upper()
    if crs != "EPSG:4326":
        raise EnvironmentAnalysisValidationError("only EPSG:4326 is supported")

    address = payload.get("address") or {}
    if not isinstance(address, dict) or set(address) - {"parcel", "road"}:
        raise EnvironmentAnalysisValidationError("address contains unsupported fields")
    parcel = _clean(address.get("parcel"))
    road = _clean(address.get("road"))
    if len(parcel) > 300 or len(road) > 300:
        raise EnvironmentAnalysisValidationError("address is too long")

    pnu = _clean(payload.get("parcelId"))
    if pnu and (len(pnu) != 19 or not pnu.isdigit()):
        raise EnvironmentAnalysisValidationError("parcelId must be a 19 digit PNU")
    building_id = _clean(payload.get("buildingId"))
    if len(building_id) > 100:
        raise EnvironmentAnalysisValidationError("buildingId is too long")
    if _clean(payload.get("radiusProfile") or RADIUS_PROFILE) != RADIUS_PROFILE:
        raise EnvironmentAnalysisValidationError("unsupported radiusProfile")
    if _clean(payload.get("calculationVersion") or CALCULATION_VERSION) not in {CALCULATION_VERSION, "environment-web-v1"}:
        raise EnvironmentAnalysisValidationError("unsupported calculationVersion")

    normalized = {
        "location": {"lat": lat, "lng": lng, "crs": "EPSG:4326"},
        "address": {"parcel": parcel, "road": road},
        "parcelId": pnu,
        "buildingId": building_id,
        "radiusProfile": RADIUS_PROFILE,
        "calculationVersion": _clean(payload.get("calculationVersion") or CALCULATION_VERSION),
    }
    return normalized


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _normalize_item(row: dict[str, Any], fallback_type: str) -> dict[str, Any] | None:
    lat = _as_float(row.get("lat"))
    lng = _as_float(row.get("lng"))
    if lat is None or lng is None:
        return None
    category = _first(row, "category", "detail")
    return {
        "id": _clean(row.get("id")),
        "name": _clean(row.get("name")),
        "type": _clean(row.get("type")) or fallback_type,
        "address": _clean(row.get("address")),
        "category": category,
        "dataDate": _first(row, "dataDate", "baseDate", "데이터기준일자", "기준일자", "제공일자"),
        "lat": lat,
        "lng": lng,
    }


def _amenity_type(item: dict[str, Any]) -> str:
    text = " ".join(_clean(item.get(key)) for key in ("name", "type", "category"))
    if any(keyword in text for keyword in AMENITY_EXCLUDE_KEYWORDS):
        return ""
    if any(keyword in text for keyword in ("약국", "온누리약국", "메디팜", "팜약국")):
        return "약국"
    if any(keyword in text for keyword in ("병원", "의원", "치과", "한의원", "보건소", "의료원", "내과", "외과", "정형", "소아", "피부과")):
        return "의료"
    if any(keyword in text for keyword in ("편의점", "씨유", "CU", "지에스25", "GS25", "세븐일레븐", "이마트24", "미니스톱")):
        return "편의점"
    if any(keyword in text for keyword in ("카페", "커피", "다방", "로스터", "베이커리", "디저트")):
        return "카페"
    if any(keyword in text for keyword in ("마트", "슈퍼", "식료품", "반찬", "정육", "수산물", "채소", "과일", "농산", "축산", "건어물", "젓갈", "할인점")):
        return "마트/식료품"
    return ""


def _dedupe(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in items:
        location = item.get("address") or f"{item['lat']:.5f},{item['lng']:.5f}"
        key = f"{item.get('type', '')}|{item.get('name', '')}|{location}"
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _dataset_source(path: Path, *, name: str, as_of: str = "") -> dict[str, str]:
    return {"name": name or "unknown", "file": path.name, "asOf": as_of}


def _summarize(
    lat: float,
    lng: float,
    items: list[dict[str, Any]],
    *,
    radius: int,
    limit: int,
    source: dict[str, str],
    use_bounds: bool = True,
) -> dict[str, Any]:
    lat_range = radius / 111_320
    lng_range = radius / (111_320 * max(math.cos(math.radians(lat)), 0.25))
    nearby: list[dict[str, Any]] = []
    for item in items:
        if use_bounds and (abs(item["lat"] - lat) > lat_range or abs(item["lng"] - lng) > lng_range):
            continue
        distance = distance_meters(lat, lng, item["lat"], item["lng"])
        if distance <= radius:
            nearby.append({**item, "distanceMeters": distance})
    nearby.sort(key=lambda item: item["distanceMeters"])
    source = {**source, "asOf": next((item["dataDate"] for item in nearby if item.get("dataDate")), source.get("asOf", ""))}
    by_type: dict[str, int] = {}
    for item in nearby:
        key = item.get("type") or "기타"
        by_type[key] = by_type.get(key, 0) + 1
    nearest = [
        {
            "name": item.get("name", ""),
            "type": item.get("type", ""),
            "category": item.get("category", ""),
            "address": item.get("address", ""),
            "distanceMeters": math.floor(item["distanceMeters"] + 0.5),
            "distanceText": format_distance(item["distanceMeters"]),
        }
        for item in nearby[:limit]
    ]
    return {
        "status": "ok",
        "radiusMeters": radius,
        "total": len(nearby),
        "within300": sum(1 for item in nearby if item["distanceMeters"] <= 300),
        "within500": sum(1 for item in nearby if item["distanceMeters"] <= 500),
        "within1000": sum(1 for item in nearby if item["distanceMeters"] <= 1000),
        "byType": by_type,
        "nearest": nearest,
        "source": source,
        "asOf": source.get("asOf", ""),
    }


@lru_cache(maxsize=64)
def _load_generic_csv(path_text: str, fallback_type: str, amenity: bool = False, identity: str = "") -> tuple[list[dict[str, Any]], dict[str, str]]:
    path = Path(path_text)
    items: list[dict[str, Any]] = []
    for row in _read_csv(path):
        item = _normalize_item(row, fallback_type)
        if not item or not item.get("name"):
            continue
        if amenity:
            item_type = _amenity_type(item)
            if not item_type:
                continue
            item["type"] = item_type
        items.append(item)
    if amenity:
        items = _dedupe(items)
    as_of = next((_clean(item.get("dataDate")) for item in items if item.get("dataDate")), "")
    return items, _dataset_source(path, name="unknown", as_of=as_of)


@lru_cache(maxsize=4)
def _load_schools(path_text: str, identity: str = "") -> tuple[list[dict[str, Any]], dict[str, str]]:
    path = Path(path_text)
    items: list[dict[str, Any]] = []
    provider = ""
    for row in _read_csv(path):
        lat = _as_float(row.get("위도"))
        lng = _as_float(row.get("경도"))
        status = _clean(row.get("운영상태"))
        if lat is None or lng is None or status == "폐교":
            continue
        provider = provider or _clean(row.get("제공기관명"))
        level = _clean(row.get("학교급구분")) or "기타"
        items.append(
            {
                "id": _clean(row.get("학교ID")),
                "name": _clean(row.get("학교명")),
                "type": level,
                "level": level,
                "address": _clean(row.get("소재지도로명주소")) or _clean(row.get("소재지지번주소")),
                "dataDate": _clean(row.get("데이터기준일자")),
                "lat": lat,
                "lng": lng,
            }
        )
    as_of = next((_clean(item.get("dataDate")) for item in items if item.get("dataDate")), "")
    return items, _dataset_source(path, name=provider or "unknown", as_of=as_of)


@lru_cache(maxsize=2)
def _load_count_json(path_text: str, *, cctv: bool, identity: str = "") -> tuple[list[dict[str, Any]], dict[str, str]]:
    path = Path(path_text)
    payload = json.loads(path.read_text(encoding="utf-8"))
    purposes = payload.get("purposes") if isinstance(payload.get("purposes"), list) else []
    items: list[dict[str, Any]] = []
    for raw in payload.get("items") if isinstance(payload.get("items"), list) else []:
        if not isinstance(raw, list) or len(raw) < 3:
            continue
        lat, lng = _as_float(raw[0]), _as_float(raw[1])
        if lat is None or lng is None:
            continue
        count = max(1, round(_as_float(raw[2]) or 1))
        item: dict[str, Any] = {"lat": lat, "lng": lng, "count": count}
        if cctv:
            purpose_index = max(0, round(_as_float(raw[3]) or 0)) if len(raw) > 3 else 0
            item["purpose"] = _clean(purposes[purpose_index]) if purpose_index < len(purposes) else "미분류"
        items.append(item)
    as_of = _clean(payload.get("generatedAt") or payload.get("dataDate") or payload.get("baseDate"))
    source = _dataset_source(path, name=_clean(payload.get("source")) or "unknown", as_of=as_of)
    return items, source


def _school_category(data_root: Path, request: dict[str, Any]) -> dict[str, Any]:
    path = _safe_data_path(data_root, "schools.csv")
    items, source = _load_schools(str(path), _file_digest(path))
    lat, lng = request["location"]["lat"], request["location"]["lng"]
    summary = _summarize(lat, lng, items, radius=1500, limit=8, source=source, use_bounds=False)
    by_level: dict[str, int] = {}
    nearby_names = {item["name"] for item in summary["nearest"]}
    # Count all schools in range, not only the nearest list.
    ordered = sorted(((distance_meters(lat, lng, item["lat"], item["lng"]), item) for item in items), key=lambda pair: pair[0])
    for distance, item in ordered:
        if distance <= 1500:
            level = item.get("level") or "기타"
            by_level[level] = by_level.get(level, 0) + 1
    summary["byLevel"] = by_level
    summary["nearest"] = [
        {**item, "level": item.get("type", "기타")}
        for item in summary["nearest"]
        if item["name"] in nearby_names
    ]
    return summary


def _generic_category(
    data_root: Path,
    request: dict[str, Any],
    *,
    folder: str | None,
    filename: str | None,
    fallback_type: str,
    radius: int,
    limit: int,
    amenity: bool = False,
) -> dict[str, Any]:
    if folder:
        region = region_key_for_request(request)
        if not region:
            raise FileNotFoundError("regional dataset cannot be selected from address or PNU")
        path = _safe_data_path(data_root, folder, f"{region}.csv")
    else:
        path = _safe_data_path(data_root, filename or "")
    items, source = _load_generic_csv(str(path), fallback_type, amenity, _file_digest(path))
    return _summarize(
        request["location"]["lat"],
        request["location"]["lng"],
        items,
        radius=radius,
        limit=limit,
        source=source,
    )


def _rail_category(data_root: Path, request: dict[str, Any]) -> dict[str, Any]:
    rail, rail_source = _load_generic_csv(str(_safe_data_path(data_root, "rail-stations.csv")), "철도역", identity=_file_digest(_safe_data_path(data_root, "rail-stations.csv")))
    subway, subway_source = _load_generic_csv(str(_safe_data_path(data_root, "subway-stations.csv")), "도시철도역", identity=_file_digest(_safe_data_path(data_root, "subway-stations.csv")))
    items = _dedupe([*rail, *subway])
    as_of = rail_source.get("asOf") or subway_source.get("asOf", "")
    source = {"name": "unknown", "file": "rail-stations.csv + subway-stations.csv", "asOf": as_of}
    return _summarize(
        request["location"]["lat"], request["location"]["lng"], items, radius=3000, limit=6, source=source
    )


def _count_category(data_root: Path, request: dict[str, Any], *, cctv: bool) -> dict[str, Any]:
    filename = "cctv.json" if cctv else "security-lights.json"
    items, source = _load_count_json(str(_safe_data_path(data_root, filename)), cctv=cctv, identity=_file_digest(_safe_data_path(data_root, filename)))
    lat, lng, radius = request["location"]["lat"], request["location"]["lng"], 500
    nearby: list[dict[str, Any]] = []
    for item in items:
        distance = distance_meters(lat, lng, item["lat"], item["lng"])
        if distance <= radius:
            nearby.append({**item, "distanceMeters": distance})
    nearby.sort(key=lambda item: item["distanceMeters"])
    result: dict[str, Any] = {
        "status": "ok",
        "radiusMeters": radius,
        "total": sum(item["count"] for item in nearby),
        "facilityCount": len(nearby),
        "within300": sum(item["count"] for item in nearby if item["distanceMeters"] <= 300),
        "nearest": [
            {
                **({"purpose": item.get("purpose", "미분류")} if cctv else {}),
                "count": item["count"],
                "distanceMeters": math.floor(item["distanceMeters"] + 0.5),
                "distanceText": format_distance(item["distanceMeters"]),
            }
            for item in nearby[:6]
        ],
        "source": source,
        "asOf": source.get("asOf", ""),
    }
    if cctv:
        by_purpose: dict[str, int] = {}
        for item in nearby:
            purpose = item.get("purpose") or "미분류"
            by_purpose[purpose] = by_purpose.get(purpose, 0) + item["count"]
        result["byPurpose"] = by_purpose
    return result


def _format_nearest(items: list[dict[str, Any]], limit: int) -> str:
    values = []
    for item in items[:limit]:
        detail = ", ".join(value for value in (_clean(item.get("type")), _clean(item.get("distanceText"))) if value)
        values.append(f"{_clean(item.get('name'))}({detail})" if detail else _clean(item.get("name")))
    return ", ".join(value for value in values if value)


def _format_bus_nearest(items: list[dict[str, Any]], limit: int = 4) -> str:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        name = re.sub(r"·+", "·", re.sub(r"\s*[.ㆍ·]\s*", "·", _clean(item.get("name"))))
        name_key = re.sub(r"[\s·.,()_-]", "", name).lower()
        if not name or name_key in seen:
            continue
        seen.add(name_key)
        result.append(f"{name} 정류장({_clean(item.get('distanceText'))})")
        if len(result) >= limit:
            break
    return ", ".join(result)


def build_report_rows(request: dict[str, Any], categories: dict[str, Any]) -> list[dict[str, str]]:
    address = request["address"]
    school = categories.get("schools", {})
    bus = categories.get("bus", {})
    rail = categories.get("rail", {})
    park = categories.get("parks", {})
    amenity = categories.get("amenities", {})
    light = categories.get("securityLights", {})
    cctv = categories.get("cctv", {})

    school_levels = ", ".join(f"{key} {value}개" for key, value in school.get("byLevel", {}).items())
    nearest_schools = _format_nearest(school.get("nearest", []), 4)
    school_text = (
        f"반경 1.5km 내 학교 {school.get('total', 0)}개가 확인됩니다"
        f"{' (' + school_levels + ')' if school_levels else ''}. 반경 500m {school.get('within500', 0)}개, "
        f"1km {school.get('within1000', 0)}개이며 가까운 학교는 {nearest_schools or '확인 필요'}입니다."
        if school.get("status") == "ok" and school.get("total", 0) > 0
        else "반경 1.5km 내 학교 위치 데이터가 확인되지 않습니다. 통학권은 현장 보행로와 교육청 자료를 추가 확인합니다."
    )
    bus_text = (
        f"반경 {format_radius(bus.get('radiusMeters', 700))} 내 버스정류장 {bus.get('total', 0)}개가 확인됩니다. "
        f"반경 300m {bus.get('within300', 0)}개, 500m {bus.get('within500', 0)}개이며 가까운 정류장"
        f"(동일 명칭은 가장 가까운 위치 기준)은 {_format_bus_nearest(bus.get('nearest', [])) or '확인 필요'}입니다."
        if bus.get("status") == "ok" and bus.get("total", 0) > 0
        else f"반경 {format_radius(bus.get('radiusMeters', 700))} 내 버스정류장 위치 데이터가 확인되지 않습니다."
    )
    rail_text = (
        f"도시철도·철도역 기준 반경 {format_radius(rail.get('radiusMeters', 3000))} 내 {rail.get('total', 0)}개가 확인되며 "
        f"가까운 역은 {_format_nearest(rail.get('nearest', []), 3) or '확인 필요'}입니다."
        if rail.get("status") == "ok" and rail.get("total", 0) > 0
        else f"도시철도·철도역 기준 반경 {format_radius(rail.get('radiusMeters', 3000))} 내 역 위치 데이터가 확인되지 않습니다."
    )
    amenity_text = (
        f"상권 데이터는 실제 영업 여부와 차이가 있을 수 있어 반경 {format_radius(amenity.get('radiusMeters', 700))} 전체 후보 수는 "
        f"참고용으로만 봅니다. 가까운 생활편의시설 후보는 {_format_nearest(amenity.get('nearest', []), 5) or '확인 필요'}이며, "
        "현장 확인이 필요합니다."
        if amenity.get("status") == "ok" and amenity.get("total", 0) > 0
        else f"반경 {format_radius(amenity.get('radiusMeters', 700))} 내 생활편의시설 위치 데이터가 확인되지 않습니다."
    )
    park_text = (
        f"반경 {format_radius(park.get('radiusMeters', 1500))} 내 도시공원 {park.get('total', 0)}개가 확인됩니다. "
        f"가까운 공원은 {_format_nearest(park.get('nearest', []), 4) or '확인 필요'}입니다."
        if park.get("status") == "ok" and park.get("total", 0) > 0
        else f"반경 {format_radius(park.get('radiusMeters', 1500))} 내 도시공원 위치 데이터가 확인되지 않습니다."
    )
    light_nearest = ", ".join(
        f"{item.get('distanceText', '-')}{' ' + str(item.get('count')) + '개' if item.get('count', 1) > 1 else ''}"
        for item in light.get("nearest", [])[:3]
    )
    light_text = (
        f"반경 500m 내 보안등 {light.get('total', 0):,}개가 확인됩니다. 반경 300m 내 {light.get('within300', 0):,}개이며, "
        f"가까운 보안등은 {light_nearest or '확인 필요'}입니다. 골목길 야간 보행환경은 현장 조도와 함께 확인하세요."
        if light.get("status") == "ok" and light.get("total", 0) > 0
        else "반경 500m 내 보안등 위치 데이터가 확인되지 않습니다. 골목길 야간 조도와 현장 상태는 별도 확인이 필요합니다."
    )
    cctv_nearest = ", ".join(
        f"{item.get('purpose', 'CCTV')} {item.get('distanceText', '-')}{' ' + str(item.get('count')) + '대' if item.get('count', 1) > 1 else ''}"
        for item in cctv.get("nearest", [])[:3]
    )
    purpose_text = ", ".join(
        f"{purpose} {count:,}대"
        for purpose, count in sorted(cctv.get("byPurpose", {}).items(), key=lambda item: item[1], reverse=True)[:3]
    )
    cctv_text = (
        f"반경 500m 내 CCTV {cctv.get('total', 0):,}대가 확인됩니다. 설치 지점은 {cctv.get('facilityCount', 0):,}곳, "
        f"반경 300m 내 {cctv.get('within300', 0):,}대입니다"
        f"{'. 주요 용도는 ' + purpose_text + '입니다' if purpose_text else ''}. 가까운 CCTV는 {cctv_nearest or '확인 필요'}입니다. "
        "실제 작동 여부와 사각지대는 현장에서 확인하세요."
        if cctv.get("status") == "ok" and cctv.get("total", 0) > 0
        else "반경 500m 내 CCTV 위치 데이터가 확인되지 않습니다. 현장 방범시설과 사각지대는 별도 확인이 필요합니다."
    )

    rows = [
        {"label": "대상지", "value": address.get("parcel") or "-"},
        {"label": "도로명주소", "value": address.get("road") or "도로명주소 확인 필요"},
        {"label": "교통환경", "value": f"도로접면은 지적도와 현장 접근 조건 확인 필요로 표시됩니다. {bus_text} {rail_text}"},
        {"label": "학군환경", "value": school_text},
        {"label": "생활편의시설", "value": f"건축물 용도 및 주소 검색 대상지 기준입니다. {amenity_text}"},
        {"label": "공원환경", "value": park_text},
        {"label": "보안등", "value": light_text},
        {"label": "CCTV", "value": cctv_text},
    ]


    labels = {"교통환경": ("bus", "rail"), "학군환경": ("schools",), "생활편의시설": ("amenities",), "공원환경": ("parks",), "보안등": ("securityLights",), "CCTV": ("cctv",)}
    for row in rows:
        failed = [key for key in labels.get(row["label"], ()) if categories.get(key, {}).get("status") != "ok"]
        if failed:
            row["value"] += " 일부 주변환경 데이터를 불러오지 못했습니다. 다시 조회하여 확인해 주세요."
    return rows


@lru_cache(maxsize=256)
def _content_digest(path_text: str, size: int, modified_ns: int, changed_ns: int) -> str:
    digest = hashlib.sha256()
    with Path(path_text).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_digest(path: Path) -> str:
    stat = path.stat()
    return _content_digest(str(path.resolve()), stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def dataset_identity(data_root: Path, region: str) -> dict[str, str]:
    names = ["schools.csv", "rail-stations.csv", "subway-stations.csv", "security-lights.json", "cctv.json"]
    if region:
        names += [f"{folder}/{region}.csv" for folder in ("bus-stops", "parks", "amenities")]
    result = {}
    for name in sorted(names):
        try:
            result[name] = _file_digest(_safe_data_path(data_root, *name.split("/")))
        except OSError:
            result[name] = "missing"
    return result


@lru_cache(maxsize=1)
def _pinned_digests() -> dict[str, str]:
    try:
        manifest = json.loads(Path(__file__).with_name("environment-data-manifest.json").read_text(encoding="utf-8"))
        files = manifest["files"]
        if manifest.get("calculationVersion") != CALCULATION_VERSION or not isinstance(files, dict):
            raise ValueError("version mismatch")
        if not files or any(not re.fullmatch(r"[0-9a-f]{64}", digest) for digest in files.values()):
            raise ValueError("invalid dataset digest")
        return files
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise RuntimeError("environment dataset manifest is unavailable") from exc


def _verify_dataset_inputs(data_root: Path, region: str) -> None:
    pinned = _pinned_digests()
    for name, digest in dataset_identity(data_root, region).items():
        # A missing category remains an explicit partial failure. A different
        # dataset is a deployment mismatch, never a new-version result.
        if digest != "missing" and digest != pinned.get(name):
            raise RuntimeError("environment dataset does not match the pinned release")


def _dataset_fingerprint(data_root: Path, region: str) -> str:
    return hashlib.sha256(json.dumps(dataset_identity(data_root, region), sort_keys=True).encode()).hexdigest()


def _cache_key(request: dict[str, Any], data_root: Path) -> str:
    region = region_key_for_request(request)
    target_key = request.get("parcelId") or f"{request['location']['lat']:.5f},{request['location']['lng']:.5f}"
    value = "|".join(
        (
            request["calculationVersion"],
            RADIUS_PROFILE,
            json.dumps(request, sort_keys=True, ensure_ascii=False),
            target_key,
            region,
            _dataset_fingerprint(data_root, region),
        )
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _get_cached(key: str) -> dict[str, Any] | None:
    now = time.monotonic()
    with _analysis_cache_lock:
        cached = _analysis_cache.get(key)
        if not cached:
            return None
        created, value = cached
        if now - created >= CACHE_TTL_SECONDS:
            _analysis_cache.pop(key, None)
            return None
        _analysis_cache.move_to_end(key)
        result = deepcopy(value)
        result["cache"] = {"hit": True, "key": key[:16], "ttlSeconds": CACHE_TTL_SECONDS}
        return result


def _put_cached(key: str, value: dict[str, Any]) -> None:
    with _analysis_cache_lock:
        _analysis_cache[key] = (time.monotonic(), deepcopy(value))
        _analysis_cache.move_to_end(key)
        while len(_analysis_cache) > MAX_CACHE_ENTRIES:
            _analysis_cache.popitem(last=False)


def clear_caches_for_tests() -> None:
    _load_generic_csv.cache_clear()
    _load_schools.cache_clear()
    _load_count_json.cache_clear()
    with _analysis_cache_lock:
        _analysis_cache.clear()
    with _rate_lock:
        _rate_windows.clear()


def analyze_environment_request(
    payload: dict[str, Any],
    *,
    data_root: Path | None = None,
    category_timeout_seconds: float = CATEGORY_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    if not _analysis_limit.acquire(blocking=False):
        raise EnvironmentAnalysisRateLimitError("environment analysis is busy")
    try:
        return _analyze_environment_request(
            payload, data_root=data_root, category_timeout_seconds=category_timeout_seconds
        )
    finally:
        _analysis_limit.release()


def _analyze_environment_request(
    payload: dict[str, Any],
    *,
    data_root: Path | None = None,
    category_timeout_seconds: float = CATEGORY_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    request = validate_request(payload)
    root = (data_root or Path(__file__).resolve().parent / "data").resolve()
    if request["calculationVersion"] == CALCULATION_VERSION:
        _verify_dataset_inputs(root, region_key_for_request(request))
    fingerprint = _dataset_fingerprint(root, region_key_for_request(request))
    cache_key = _cache_key(request, root)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    jobs: OrderedDict[str, Callable[[], dict[str, Any]]] = OrderedDict(
        [
            ("schools", lambda: _school_category(root, request)),
            (
                "bus",
                lambda: _generic_category(
                    root, request, folder="bus-stops", filename=None, fallback_type="버스정류장", radius=700, limit=20
                ),
            ),
            ("rail", lambda: _rail_category(root, request)),
            (
                "parks",
                lambda: _generic_category(
                    root, request, folder="parks", filename=None, fallback_type="공원", radius=1500, limit=8
                ),
            ),
            (
                "amenities",
                lambda: _generic_category(
                    root,
                    request,
                    folder="amenities",
                    filename=None,
                    fallback_type="생활편의",
                    radius=700,
                    limit=10,
                    amenity=True,
                ),
            ),
            ("securityLights", lambda: _count_category(root, request, cctv=False)),
            ("cctv", lambda: _count_category(root, request, cctv=True)),
        ]
    )
    futures = {name: _submit_category(job) for name, job in jobs.items()}
    done, not_done = wait(futures.values(), timeout=max(0.05, category_timeout_seconds))
    categories: OrderedDict[str, dict[str, Any]] = OrderedDict()
    errors: list[dict[str, str]] = []
    for name, future in futures.items():
        if future in not_done:
            future.cancel()
            categories[name] = {"status": "error", "radiusMeters": CATEGORY_RADII[name], "nearest": []}
            errors.append({"category": name, "code": "timeout", "message": "데이터 조회 시간이 초과되었습니다."})
            continue
        try:
            categories[name] = future.result()
        except Exception:
            categories[name] = {"status": "error", "radiusMeters": CATEGORY_RADII[name], "nearest": []}
            errors.append(
                {
                    "category": name,
                    "code": "dataset_unavailable",
                    "message": "데이터셋을 불러오지 못했습니다.",
                }
            )

    if fingerprint != _dataset_fingerprint(root, region_key_for_request(request)):
        raise RuntimeError("environment datasets changed during analysis")
    report_rows = build_report_rows(request, categories)
    sources = [
        {"category": name, **category["source"]}
        for name, category in categories.items()
        if isinstance(category.get("source"), dict)
    ]
    result = {
        "ok": not errors,
        "partial": bool(errors) and len(errors) < len(categories),
        "calculationVersion": request["calculationVersion"],
        "radiusProfile": RADIUS_PROFILE,
        "location": request["location"],
        "address": request["address"],
        "parcelId": request["parcelId"],
        "buildingId": request["buildingId"],
        "datasetFingerprint": fingerprint,
        "datasetFiles": dataset_identity(root, region_key_for_request(request)),
        "categories": categories,
        "reportRows": report_rows,
        "sources": sources,
        "generatedAt": utc_now_iso(),
        "errors": errors,
        "cache": {"hit": False, "key": cache_key[:16], "ttlSeconds": CACHE_TTL_SECONDS},
    }
    if not errors:
        _put_cached(cache_key, result)
    return result
