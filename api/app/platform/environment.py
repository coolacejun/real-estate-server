from __future__ import annotations

import csv
import json
import math
import re
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from threading import BoundedSemaphore
from typing import Any, Iterable

from fastapi import HTTPException

from .config import PlatformSettings
from . import shared_environment


# Reads can outlive an HTTP timeout. Slots are released when the underlying
# future actually finishes, so retries cannot create an unbounded job queue.
CATEGORY_TIMEOUT_SECONDS = 8.0
_category_executor = ThreadPoolExecutor(max_workers=7, thread_name_prefix="environment-analysis")
_analysis_slots = BoundedSemaphore(2)
_category_slots = BoundedSemaphore(14)


def _submit_category(job) -> Future:
    if not _category_slots.acquire(blocking=False):
        raise HTTPException(status_code=503, detail="environment analysis is busy")
    try:
        future = _category_executor.submit(job)
    except Exception:
        _category_slots.release()
        raise
    future.add_done_callback(lambda _: _category_slots.release())
    return future


REGION_ALIASES = (
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
)
REGIONS_BY_CODE = {
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
RADIUS_PROFILE = {
    "bus": 700.0,
    "rail": 3000.0,
    "schools": 1500.0,
    "amenities": 700.0,
    "parks": 1500.0,
    "securityLights": 500.0,
    "cctv": 500.0,
}
AMENITY_EXCLUDE_KEYWORDS = (
    "골프", "스크린", "연습장", "사료", "동물", "펫", "애견", "학원", "교습",
    "독서실", "스터디", "PC방", "피시방", "노래", "당구", "헬스", "필라테스",
    "요가", "미용", "네일", "의류", "세탁", "철물", "공구", "인테리어",
)


@dataclass(frozen=True, slots=True)
class Point:
    lat: float
    lng: float
    name: str
    kind: str
    identifier: str = ""
    address: str = ""
    category: str = ""
    count: int = 1
    data_date: str = ""
    status: str = ""


def _text(value: object) -> str:
    return str(value or "").strip()


def _float(value: object) -> float | None:
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _positive_int(value: object) -> int:
    parsed = _float(value)
    if parsed is None:
        return 1
    return max(1, int(math.floor(parsed + 0.5)))


def _region(addresses: Iterable[str], parcel_id: str) -> str | None:
    if parcel_id:
        region = REGIONS_BY_CODE.get(parcel_id[:2])
        if region:
            return region
    for address in addresses:
        compact = _text(address).lstrip(" ,|/")
        for slug, aliases in REGION_ALIASES:
            if any(
                compact == alias
                or compact.startswith(f"{alias} ")
                or compact.startswith(f"{alias},")
                or compact.startswith(f"{alias}/")
                for alias in aliases
            ):
                return slug
    return None


def _normalize_school_level(value: object) -> str:
    text = _text(value)
    if "초" in text:
        return "초등학교"
    if "중" in text:
        return "중학교"
    if "고" in text:
        return "고등학교"
    if "특수" in text:
        return "특수학교"
    return text or "학교"


def _distance_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    earth = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat, dlng = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
    value = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlng / 2) ** 2
    return earth * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _round_distance(value: float) -> int:
    return int(math.floor(value + 0.5))


def _format_distance(value: float) -> str:
    if value < 1000:
        return f"{_round_distance(value):,}m"
    return f"{value / 1000:.1f}km"


@lru_cache(maxsize=12)
def _csv_points(path_text: str, modified_ns: int) -> tuple[Point, ...]:
    del modified_ns
    path = Path(path_text)
    points: list[Point] = []
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = set(reader.fieldnames or ())
        if not fields.intersection({"lat", "위도", "latitude"}) or not fields.intersection(
            {"lng", "경도", "longitude"}
        ):
            raise ValueError("environment CSV is missing coordinate columns")
        for row in reader:
            lat = _float(row.get("lat") or row.get("위도") or row.get("latitude"))
            lng = _float(row.get("lng") or row.get("경도") or row.get("longitude"))
            if lat is None or lng is None or not (32 <= lat <= 39.5 and 124 <= lng <= 132):
                continue
            road_address = _text(row.get("소재지도로명주소"))
            parcel_address = _text(row.get("소재지지번주소"))
            points.append(
                Point(
                    lat=lat,
                    lng=lng,
                    name=_text(row.get("name") or row.get("학교명") or row.get("시설명")),
                    kind=_text(row.get("type") or row.get("학교급구분") or row.get("category")),
                    identifier=_text(row.get("id") or row.get("학교ID")),
                    address=_text(row.get("address")) or road_address or parcel_address,
                    category=_text(row.get("category") or row.get("detail")),
                    data_date=_text(
                        row.get("dataDate")
                        or row.get("baseDate")
                        or row.get("데이터기준일자")
                        or row.get("기준일자")
                        or row.get("제공일자")
                    ),
                    status=_text(row.get("운영상태")),
                )
            )
    if not points:
        raise ValueError("environment CSV contains no valid coordinates")
    return tuple(points)


@lru_cache(maxsize=4)
def _json_points(path_text: str, modified_ns: int) -> tuple[Point, ...]:
    del modified_ns
    path = Path(path_text)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise ValueError("invalid environment JSON dataset")
    purposes = payload.get("purposes") if isinstance(payload.get("purposes"), list) else []
    data_date = _text(payload.get("generatedAt") or payload.get("dataDate") or payload.get("baseDate"))
    points: list[Point] = []
    for item in payload["items"]:
        if not isinstance(item, list) or len(item) < 2:
            continue
        lat, lng = _float(item[0]), _float(item[1])
        if lat is None or lng is None or not (32 <= lat <= 39.5 and 124 <= lng <= 132):
            continue
        purpose_index = int(_float(item[3]) or 0) if len(item) > 3 else -1
        kind = _text(purposes[purpose_index]) if 0 <= purpose_index < len(purposes) else ""
        points.append(
            Point(
                lat=lat,
                lng=lng,
                name=_text(payload.get("source")),
                kind=kind,
                count=_positive_int(item[2]) if len(item) > 2 else 1,
                data_date=data_date,
            )
        )
    if not points:
        raise ValueError("environment JSON contains no valid coordinates")
    return tuple(points)


def _load(path: Path) -> tuple[Point, ...]:
    stat = path.stat()
    if not path.is_file():
        raise FileNotFoundError(path.name)
    return (
        _json_points(str(path.resolve()), stat.st_mtime_ns)
        if path.suffix.lower() == ".json"
        else _csv_points(str(path.resolve()), stat.st_mtime_ns)
    )


def _nearby(
    points: Iterable[Point], lat: float, lng: float, radius: float
) -> list[tuple[float, Point]]:
    lat_delta = radius / 111_320.0
    lng_delta = radius / (111_320.0 * max(math.cos(math.radians(lat)), 0.25))
    matches: list[tuple[float, Point]] = []
    for point in points:
        if abs(point.lat - lat) > lat_delta or abs(point.lng - lng) > lng_delta:
            continue
        distance = _distance_m(lat, lng, point.lat, point.lng)
        if distance <= radius:
            matches.append((distance, point))
    matches.sort(key=lambda item: item[0])
    return matches


def _dedupe(points: Iterable[Point]) -> tuple[Point, ...]:
    seen: set[str] = set()
    result: list[Point] = []
    for point in points:
        coordinate = f"{point.lat:.5f},{point.lng:.5f}"
        key = f"{point.kind}|{point.name}|{point.address or coordinate}"
        if key in seen:
            continue
        seen.add(key)
        result.append(point)
    return tuple(result)


def _amenity_type(point: Point) -> str:
    text = f"{point.name} {point.kind} {point.category}"
    if any(keyword in text for keyword in AMENITY_EXCLUDE_KEYWORDS):
        return ""
    if re.search(r"약국|온누리약국|메디팜|팜약국", text):
        return "약국"
    if re.search(r"병원|의원|치과|한의원|보건소|의료원|내과|외과|정형|소아|피부과", text):
        return "의료"
    if re.search(r"편의점|씨유|CU|지에스25|GS25|세븐일레븐|이마트24|미니스톱", text):
        return "편의점"
    if re.search(r"카페|커피|다방|로스터|베이커리|디저트", text):
        return "카페"
    if re.search(r"마트|슈퍼|식료품|반찬|정육|수산물|채소|과일|농산|축산|건어물|젓갈|할인점", text):
        return "마트/식료품"
    return ""


def _normalize_amenities(points: Iterable[Point]) -> tuple[Point, ...]:
    normalized: list[Point] = []
    for point in points:
        kind = _amenity_type(point)
        if not point.name or not kind:
            continue
        normalized.append(
            Point(
                lat=point.lat,
                lng=point.lng,
                name=point.name,
                kind=kind,
                identifier=point.identifier,
                address=point.address,
                category=point.category,
                data_date=point.data_date,
            )
        )
    return _dedupe(normalized)


def _dataset_date(points: Iterable[Point]) -> str:
    return next((point.data_date for point in points if point.data_date), "")


def _source(
    base: Path,
    path: Path,
    category: str,
    label: str,
    points: Iterable[Point],
) -> dict[str, Any]:
    relative = path.resolve().relative_to(base.resolve()).as_posix()
    return {
        "category": category,
        "name": label,
        "file": relative,
        "dataset": path.name,
        "asOf": _dataset_date(points),
        "updatedAt": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
    }


def _item(distance: float, point: Point) -> dict[str, Any]:
    return {
        "name": point.name,
        "type": point.kind,
        "category": point.category,
        "dataDate": point.data_date,
        "distanceMeters": _round_distance(distance),
        "distanceText": _format_distance(distance),
        "address": point.address,
    }


def _generic_category(
    points: Iterable[Point], lat: float, lng: float, radius: float, limit: int
) -> dict[str, Any]:
    point_tuple = tuple(points)
    nearby = _nearby(point_tuple, lat, lng, radius)
    by_type: dict[str, int] = {}
    for _, point in nearby:
        key = point.kind or "기타"
        by_type[key] = by_type.get(key, 0) + 1
    items = [_item(distance, point) for distance, point in nearby[:limit]]
    data_date = next(
        (point.data_date for _, point in nearby if point.data_date),
        _dataset_date(point_tuple),
    )
    return {
        "status": "ok",
        "hasCoordinates": True,
        "radiusMeters": int(radius),
        "total": len(nearby),
        "count": len(nearby),
        "within300": sum(distance <= 300 for distance, _ in nearby),
        "within500": sum(distance <= 500 for distance, _ in nearby),
        "within1000": sum(distance <= 1000 for distance, _ in nearby),
        "byType": by_type,
        "dataDate": data_date,
        "items": items,
        "nearest": items,
    }


def _bus_nearest(items: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    seen: set[str] = set()
    nearest: list[dict[str, Any]] = []
    for item in items:
        display_name = re.sub(r"\s*[.ㆍ·]\s*", "·", _text(item.get("name")))
        display_name = re.sub(r"·+", "·", display_name)
        key = re.sub(r"[\s·.,()_-]", "", display_name).lower()
        if not display_name or key in seen:
            continue
        seen.add(key)
        normalized = dict(item)
        normalized["name"] = display_name
        nearest.append(normalized)
        if len(nearest) >= limit:
            break
    return nearest


def _school_category(
    points: Iterable[Point], lat: float, lng: float
) -> dict[str, Any]:
    active = tuple(point for point in points if point.name and point.status != "폐교")
    nearby = _nearby(active, lat, lng, RADIUS_PROFILE["schools"])
    by_level: dict[str, int] = {}
    normalized: list[tuple[float, Point]] = []
    for distance, point in nearby:
        level = _normalize_school_level(point.kind)
        by_level[level] = by_level.get(level, 0) + 1
        normalized.append(
            (
                distance,
                Point(
                    lat=point.lat,
                    lng=point.lng,
                    name=point.name,
                    kind=level,
                    identifier=point.identifier,
                    address=point.address,
                    data_date=point.data_date,
                    status=point.status,
                ),
            )
        )
    schools = [
        {
            "name": point.name,
            "level": point.kind,
            "type": point.kind,
            "distanceMeters": _round_distance(distance),
            "distanceText": _format_distance(distance),
            "address": point.address,
        }
        for distance, point in normalized[:8]
    ]

    def level_summary(level: str) -> dict[str, Any]:
        rows = [
            {
                "name": point.name,
                "level": point.kind,
                "type": point.kind,
                "distanceMeters": _round_distance(distance),
                "distanceText": _format_distance(distance),
                "address": point.address,
            }
            for distance, point in normalized
            if point.kind == level
        ]
        return {"count": len(rows), "nearest": rows[0] if rows else None}

    return {
        "status": "ok",
        "hasCoordinates": True,
        "radiusMeters": int(RADIUS_PROFILE["schools"]),
        "total": len(normalized),
        "count": len(normalized),
        "within500": sum(distance <= 500 for distance, _ in normalized),
        "within1000": sum(distance <= 1000 for distance, _ in normalized),
        "byLevel": by_level,
        "dataDate": _dataset_date(active),
        "schools": schools,
        "items": schools,
        "nearest": schools,
        "elementary": level_summary("초등학교"),
        "middle": level_summary("중학교"),
        "high": level_summary("고등학교"),
    }


def _security_category(
    points: Iterable[Point], lat: float, lng: float
) -> dict[str, Any]:
    point_tuple = tuple(points)
    nearby = _nearby(point_tuple, lat, lng, RADIUS_PROFILE["securityLights"])
    items = [
        {
            "count": point.count,
            "distanceMeters": _round_distance(distance),
            "distanceText": _format_distance(distance),
        }
        for distance, point in nearby[:6]
    ]
    total = sum(point.count for _, point in nearby)
    return {
        "status": "ok",
        "hasCoordinates": True,
        "radiusMeters": int(RADIUS_PROFILE["securityLights"]),
        "total": total,
        "count": total,
        "facilityCount": len(nearby),
        "within300": sum(point.count for distance, point in nearby if distance <= 300),
        "dataDate": _dataset_date(point_tuple),
        "items": items,
        "nearest": items,
    }


def _cctv_category(points: Iterable[Point], lat: float, lng: float) -> dict[str, Any]:
    point_tuple = tuple(points)
    nearby = _nearby(point_tuple, lat, lng, RADIUS_PROFILE["cctv"])
    by_purpose: dict[str, int] = {}
    for _, point in nearby:
        purpose = point.kind or "미분류"
        by_purpose[purpose] = by_purpose.get(purpose, 0) + point.count
    items = [
        {
            "purpose": point.kind or "미분류",
            "count": point.count,
            "distanceMeters": _round_distance(distance),
            "distanceText": _format_distance(distance),
        }
        for distance, point in nearby[:6]
    ]
    total = sum(point.count for _, point in nearby)
    return {
        "status": "ok",
        "hasCoordinates": True,
        "radiusMeters": int(RADIUS_PROFILE["cctv"]),
        "total": total,
        "count": total,
        "facilityCount": len(nearby),
        "within300": sum(point.count for distance, point in nearby if distance <= 300),
        "byPurpose": by_purpose,
        "dataDate": _dataset_date(point_tuple),
        "items": items,
        "nearest": items,
    }


def _error_category(key: str) -> dict[str, Any]:
    category: dict[str, Any] = {
        "status": "error",
        "hasCoordinates": True,
        "radiusMeters": int(RADIUS_PROFILE[key]),
        "total": 0,
        "count": 0,
        "items": [],
        "nearest": [],
    }
    if key == "schools":
        category.update(
            {
                "schools": [],
                "byLevel": {},
                "elementary": {"count": 0, "nearest": None},
                "middle": {"count": 0, "nearest": None},
                "high": {"count": 0, "nearest": None},
            }
        )
    return category


def _nearest_text(category: dict[str, Any], limit: int) -> str:
    return ", ".join(
        f"{item.get('name') or item.get('purpose') or '시설'}({item.get('distanceText') or '-'})"
        for item in category.get("nearest", [])[:limit]
    )


def _report_rows(
    parcel_address: str,
    road_address: str,
    categories: dict[str, Any],
) -> list[dict[str, str]]:
    bus = categories["bus"]
    rail = categories["rail"]
    schools = categories["schools"]
    amenities = categories["amenities"]
    parks = categories["parks"]
    lights = categories["securityLights"]
    cctv = categories["cctv"]

    def count_text(category: dict[str, Any], unit: str = "개") -> str:
        if category.get("status") != "ok":
            return "데이터 확인 불가"
        return f"{int(category.get('total') or 0):,}{unit}"

    traffic = (
        f"반경 700m 버스정류장 {count_text(bus)}, 가까운 정류장 {_nearest_text(bus, 4) or '없음'}. "
        f"반경 3km 철도·도시철도역 {count_text(rail)}, 가까운 역 {_nearest_text(rail, 3) or '없음'}."
    )
    school_levels = ", ".join(
        f"{label} {schools.get(key, {}).get('count', 0)}개"
        for key, label in (("elementary", "초등학교"), ("middle", "중학교"), ("high", "고등학교"))
    )
    school_text = (
        "학교 데이터 확인 불가"
        if schools.get("status") != "ok"
        else f"반경 1.5km 학교 {schools.get('total', 0)}개 ({school_levels}), 가까운 학교 {_nearest_text(schools, 4) or '없음'}."
    )
    return [
        {"label": "대상지", "value": parcel_address or "좌표 기준 조회"},
        {"label": "도로명주소", "value": road_address or "도로명주소 확인 필요"},
        {"label": "교통환경", "value": traffic},
        {"label": "학군환경", "value": school_text},
        {
            "label": "생활편의시설",
            "value": f"반경 700m 생활편의시설 {count_text(amenities)}, 가까운 시설 {_nearest_text(amenities, 5) or '없음'}.",
        },
        {
            "label": "공원환경",
            "value": f"반경 1.5km 도시공원 {count_text(parks)}, 가까운 공원 {_nearest_text(parks, 4) or '없음'}.",
        },
        {"label": "보안등", "value": f"반경 500m 보안등 {count_text(lights)}."},
        {"label": "CCTV", "value": f"반경 500m CCTV {count_text(cctv, '대')}."},
    ]


def _legacy_categories(categories: dict[str, Any]) -> dict[str, Any]:
    bus, rail = categories["bus"], categories["rail"]
    candidates = [
        item
        for category in (bus, rail)
        for item in category.get("nearest", [])
        if int(item.get("distanceMeters") or 0) <= 1000
    ]
    candidates.sort(key=lambda item: int(item.get("distanceMeters") or 0))
    traffic_ok = bus.get("status") == "ok" or rail.get("status") == "ok"
    traffic_count = int(bus.get("total") or 0) + int(rail.get("within1000") or 0)
    traffic = {
        "status": "ok" if traffic_ok else "error",
        "radiusMeters": 1000,
        "count": traffic_count,
        "nearest": candidates[0] if candidates else None,
    }
    schools = categories["schools"]
    school_nearest = next(
        (
            item
            for item in schools.get("nearest", [])
            if int(item.get("distanceMeters") or 0) <= 1000
        ),
        None,
    )
    school = {
        "status": schools.get("status"),
        "radiusMeters": 1000,
        "count": int(schools.get("within1000") or 0),
        "nearest": school_nearest,
    }

    def count_alias(source: dict[str, Any], radius: int, count_key: str) -> dict[str, Any]:
        return {
            "status": source.get("status"),
            "radiusMeters": radius,
            "count": int(source.get(count_key) or 0),
            "nearest": next(
                (
                    item
                    for item in source.get("nearest", [])
                    if int(item.get("distanceMeters") or 0) <= radius
                ),
                None,
            ),
        }

    return {
        "traffic": traffic,
        "school": school,
        "convenience": count_alias(categories["amenities"], 500, "within500"),
        "park": count_alias(categories["parks"], 1000, "within1000"),
        "streetlight": {
            "status": categories["securityLights"].get("status"),
            "radiusMeters": 500,
            "count": int(categories["securityLights"].get("total") or 0),
        },
    }


def analyze_environment(settings: PlatformSettings, payload: object) -> dict[str, Any]:
    if not _analysis_slots.acquire(blocking=False):
        raise HTTPException(status_code=503, detail="environment analysis is busy")
    try:
        return _analyze_environment(settings, payload)
    finally:
        _analysis_slots.release()


def _analyze_environment(settings: PlatformSettings, payload: object) -> dict[str, Any]:
    if isinstance(payload, dict) and payload.get("calculationVersion") == shared_environment.CALCULATION_VERSION:
        try:
            result = shared_environment.analyze_environment_request(payload, data_root=settings.environment_data_dir)
        except shared_environment.EnvironmentAnalysisValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail="environment analysis is unavailable") from exc
        if not any(category.get("status") == "ok" for category in result["categories"].values()):
            raise HTTPException(status_code=503, detail="environment datasets are unavailable")
        return result
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="request body must be an object")
    location = payload.get("location") if isinstance(payload.get("location"), dict) else {}
    address = payload.get("address") if isinstance(payload.get("address"), dict) else {}
    lat, lng = _float(location.get("lat")), _float(location.get("lng"))
    if lat is None or lng is None or not (-90 <= lat <= 90 and -180 <= lng <= 180):
        raise HTTPException(status_code=422, detail="location must contain valid WGS84 coordinates")
    if location.get("crs") != "EPSG:4326":
        raise HTTPException(status_code=422, detail="only EPSG:4326 is supported")
    if payload.get("radiusProfile") != "web-v1" or payload.get("calculationVersion") != "environment-web-v1":
        raise HTTPException(status_code=422, detail="unsupported environment analysis version")
    parcel_id = _text(payload.get("parcelId"))
    if parcel_id and (len(parcel_id) != 19 or not parcel_id.isdigit()):
        raise HTTPException(status_code=422, detail="parcelId must contain 19 digits")
    parcel_address = _text(address.get("parcel"))
    road_address = _text(address.get("road"))
    region = _region((parcel_address, road_address), parcel_id)
    base = settings.environment_data_dir
    categories: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    jobs: list[tuple[str, Future]] = []

    def fail(key: str, code: str = "dataset_unavailable", retryable: bool = True) -> None:
        categories[key] = _error_category(key)
        errors.append({"category": key, "code": code, "retryable": retryable})

    def evaluate(
        key: str,
        files: list[tuple[Path, str]],
        builder,
        transform=lambda points: tuple(points),
    ) -> None:
        def calculate():
            loaded = [(path, label, _load(path)) for path, label in files]
            combined = transform(point for _, _, points in loaded for point in points)
            return builder(combined), [
                _source(base, path, key, label, points) for path, label, points in loaded
            ]

        try:
            jobs.append((key, _submit_category(calculate)))
        except HTTPException:
            fail(key, "analysis_busy")

    if region is None:
        for key in ("bus", "amenities", "parks"):
            fail(key, "region_unresolved", False)
    else:
        evaluate(
            "bus",
            [(base / "bus-stops" / f"{region}.csv", "지자체 버스정류장")],
            lambda points: {
                **_generic_category(points, lat, lng, RADIUS_PROFILE["bus"], 20),
                "regionKey": region,
            },
        )
        evaluate(
            "amenities",
            [(base / "amenities" / f"{region}.csv", "소상공인시장진흥공단 상가정보")],
            lambda points: {**_generic_category(points, lat, lng, RADIUS_PROFILE["amenities"], 10), "regionKey": region},
            _normalize_amenities,
        )
        evaluate(
            "parks",
            [(base / "parks" / f"{region}.csv", "지자체 도시공원정보")],
            lambda points: {**_generic_category(points, lat, lng, RADIUS_PROFILE["parks"], 8), "regionKey": region},
        )

    evaluate(
        "rail",
        [
            (base / "rail-stations.csv", "국가철도공단 철도역"),
            (base / "subway-stations.csv", "도시철도역"),
        ],
        lambda points: _generic_category(points, lat, lng, RADIUS_PROFILE["rail"], 6),
        _dedupe,
    )
    evaluate(
        "schools",
        [(base / "schools.csv", "한국교육시설안전원 학교정보")],
        lambda points: _school_category(points, lat, lng),
    )
    evaluate(
        "securityLights",
        [(base / "security-lights.json", "지자체 보안등정보")],
        lambda points: _security_category(points, lat, lng),
    )
    evaluate(
        "cctv",
        [(base / "cctv.json", "공공 CCTV정보")],
        lambda points: _cctv_category(points, lat, lng),
    )

    completed, _ = wait([future for _, future in jobs], timeout=CATEGORY_TIMEOUT_SECONDS)
    for key, future in jobs:
        if future not in completed:
            future.cancel()
            fail(key, "dataset_timeout")
            continue
        try:
            category, category_sources = future.result()
        except (OSError, ValueError, TypeError, csv.Error, json.JSONDecodeError):
            fail(key)
        else:
            categories[key] = category
            sources.extend(category_sources)
    if categories["bus"].get("status") == "ok":
        categories["bus"]["nearest"] = _bus_nearest(categories["bus"]["items"])

    successes = sum(
        categories.get(key, {}).get("status") == "ok" for key in RADIUS_PROFILE
    )
    if successes == 0:
        raise HTTPException(status_code=503, detail="environment datasets are unavailable")
    categories.update(_legacy_categories(categories))
    return {
        "ok": not errors,
        "calculationVersion": "environment-web-v1",
        "radiusProfile": "web-v1",
        "location": {"lat": lat, "lng": lng, "crs": "EPSG:4326"},
        "address": {"parcel": parcel_address, "road": road_address},
        "categories": categories,
        "reportRows": _report_rows(parcel_address, road_address, categories),
        "sources": sources,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "errors": errors,
        "partial": bool(errors),
    }
