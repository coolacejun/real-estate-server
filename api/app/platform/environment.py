from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from fastapi import HTTPException

from .config import PlatformSettings


REGIONS = {
    "서울": "seoul", "부산": "busan", "대구": "daegu", "인천": "incheon",
    "광주": "gwangju", "대전": "daejeon", "울산": "ulsan", "세종": "sejong",
    "경기": "gyeonggi", "강원": "gangwon", "충북": "chungbuk", "충남": "chungnam",
    "전북": "jeonbuk", "전남": "jeonnam", "경북": "gyeongbuk", "경남": "gyeongnam",
    "제주": "jeju",
}
RADIUS_PROFILE = {
    "traffic": 1000.0,
    "school": 1000.0,
    "convenience": 500.0,
    "park": 1000.0,
    "streetlight": 500.0,
    "cctv": 500.0,
}


@dataclass(frozen=True)
class Point:
    lat: float
    lng: float
    name: str
    kind: str
    count: int = 1
    data_date: str = ""


def _float(value: object) -> float | None:
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _region(address: str) -> str | None:
    compact = address.strip()
    for prefix, slug in REGIONS.items():
        if compact.startswith(prefix):
            return slug
    return None


def _distance_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    earth = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat, dlng = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
    value = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlng / 2) ** 2
    return earth * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


@lru_cache(maxsize=96)
def _csv_points(path_text: str, modified_ns: int) -> tuple[Point, ...]:
    del modified_ns
    path = Path(path_text)
    points: list[Point] = []
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            lat = _float(row.get("lat") or row.get("위도") or row.get("latitude"))
            lng = _float(row.get("lng") or row.get("경도") or row.get("longitude"))
            if lat is None or lng is None or not (32 <= lat <= 39.5 and 124 <= lng <= 132):
                continue
            points.append(
                Point(
                    lat=lat,
                    lng=lng,
                    name=str(row.get("name") or row.get("학교명") or row.get("시설명") or "").strip(),
                    kind=str(row.get("type") or row.get("학교급구분") or row.get("category") or "").strip(),
                    data_date=str(row.get("dataDate") or row.get("데이터기준일자") or "").strip(),
                )
            )
    return tuple(points)


@lru_cache(maxsize=8)
def _json_points(path_text: str, modified_ns: int) -> tuple[Point, ...]:
    del modified_ns
    path = Path(path_text)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise ValueError("invalid environment dataset")
    purposes = payload.get("purposes") if isinstance(payload.get("purposes"), list) else []
    points: list[Point] = []
    for item in payload["items"]:
        if not isinstance(item, list) or len(item) < 2:
            continue
        lat, lng = _float(item[0]), _float(item[1])
        if lat is None or lng is None:
            continue
        count = max(1, int(item[2])) if len(item) > 2 else 1
        purpose_index = int(item[3]) if len(item) > 3 else -1
        kind = str(purposes[purpose_index]) if 0 <= purpose_index < len(purposes) else ""
        points.append(Point(lat, lng, str(payload.get("source") or ""), kind, count))
    return tuple(points)


def _load_csv(path: Path) -> tuple[Point, ...]:
    return _csv_points(str(path.resolve()), path.stat().st_mtime_ns)


def _load_json(path: Path) -> tuple[Point, ...]:
    return _json_points(str(path.resolve()), path.stat().st_mtime_ns)


def _nearby(points: Iterable[Point], lat: float, lng: float, radius: float) -> list[tuple[float, Point]]:
    lat_delta = radius / 111_000.0
    lng_delta = radius / max(1.0, 111_000.0 * math.cos(math.radians(lat)))
    matches: list[tuple[float, Point]] = []
    for point in points:
        if abs(point.lat - lat) > lat_delta or abs(point.lng - lng) > lng_delta:
            continue
        distance = _distance_m(lat, lng, point.lat, point.lng)
        if distance <= radius:
            matches.append((distance, point))
    matches.sort(key=lambda item: item[0])
    return matches


def _source(path: Path, label: str) -> dict[str, Any]:
    return {
        "name": label,
        "dataset": path.name,
        "updatedAt": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
    }


def analyze_environment(settings: PlatformSettings, payload: object) -> dict[str, Any]:
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
    parcel_id = str(payload.get("parcelId") or "")
    if parcel_id and (len(parcel_id) != 19 or not parcel_id.isdigit()):
        raise HTTPException(status_code=422, detail="parcelId must contain 19 digits")
    parcel_address = str(address.get("parcel") or "").strip()
    road_address = str(address.get("road") or "").strip()
    region = _region(parcel_address or road_address)
    base = settings.environment_data_dir
    categories: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    report_rows = [
        {"label": "대상 위치", "value": parcel_address or f"{lat:.6f}, {lng:.6f}"},
        {"label": "도로명주소", "value": road_address or "확인되지 않음"},
    ]

    def evaluate(key: str, label: str, files: list[tuple[Path, str]], formatter) -> None:
        try:
            missing = [path for path, _ in files if not path.is_file()]
            if missing:
                raise FileNotFoundError(missing[0].name)
            combined: list[Point] = []
            for path, source_label in files:
                combined.extend(_load_json(path) if path.suffix == ".json" else _load_csv(path))
                sources.append(_source(path, source_label))
            matches = _nearby(combined, lat, lng, RADIUS_PROFILE[key])
            category, text = formatter(matches)
            categories[key] = {"status": "ok", "radiusMeters": int(RADIUS_PROFILE[key]), **category}
            report_rows.append({"label": label, "value": text})
        except (OSError, ValueError, csv.Error) as exc:
            categories[key] = {"status": "error", "radiusMeters": int(RADIUS_PROFILE[key])}
            errors.append({"category": key, "code": "dataset_unavailable", "retryable": True})

    if region is None:
        for key, label in (("traffic", "교통"), ("convenience", "생활편의"), ("park", "공원")):
            categories[key] = {"status": "error", "radiusMeters": int(RADIUS_PROFILE[key])}
            errors.append({"category": key, "code": "region_unresolved", "retryable": False})
    else:
        evaluate(
            "traffic", "교통",
            [(base / "rail-stations.csv", "국가철도공단 철도역"), (base / "subway-stations.csv", "도시철도역"), (base / "bus-stops" / f"{region}.csv", "지자체 버스정류장")],
            lambda rows: (
                {"count": len(rows), "nearest": ({"name": rows[0][1].name, "type": rows[0][1].kind, "distanceMeters": round(rows[0][0])} if rows else None)},
                (f"{rows[0][1].name} {round(rows[0][0])}m, 반경 1km {len(rows)}개" if rows else "반경 1km 내 교통시설 없음"),
            ),
        )
        evaluate(
            "convenience", "생활편의", [(base / "amenities" / f"{region}.csv", "소상공인시장진흥공단 상가정보")],
            lambda rows: ({"count": len(rows)}, f"반경 500m 생활편의시설 {len(rows)}개"),
        )
        evaluate(
            "park", "공원", [(base / "parks" / f"{region}.csv", "지자체 도시공원정보")],
            lambda rows: (
                {"count": len(rows), "nearest": ({"name": rows[0][1].name, "distanceMeters": round(rows[0][0])} if rows else None)},
                (f"{rows[0][1].name} {round(rows[0][0])}m" if rows else "반경 1km 내 공원 없음"),
            ),
        )

    evaluate(
        "school", "학교", [(base / "schools.csv", "한국교육시설안전원 학교정보")],
        lambda rows: (
            {"count": len(rows), "nearest": ({"name": rows[0][1].name, "type": rows[0][1].kind, "distanceMeters": round(rows[0][0])} if rows else None)},
            (f"{rows[0][1].name} {round(rows[0][0])}m" if rows else "반경 1km 내 학교 없음"),
        ),
    )
    evaluate(
        "streetlight", "보안등", [(base / "security-lights.json", "지자체 보안등정보")],
        lambda rows: ({"count": sum(item.count for _, item in rows)}, f"반경 500m 보안등 {sum(item.count for _, item in rows)}개"),
    )
    evaluate(
        "cctv", "CCTV", [(base / "cctv.json", "공공 CCTV정보")],
        lambda rows: ({"count": sum(item.count for _, item in rows)}, f"반경 500m CCTV {sum(item.count for _, item in rows)}대"),
    )

    successes = sum(1 for value in categories.values() if value.get("status") == "ok")
    if successes == 0:
        raise HTTPException(status_code=503, detail="environment datasets are unavailable")
    unique_sources = {item["dataset"]: item for item in sources}
    return {
        "calculationVersion": "environment-web-v1",
        "radiusProfile": "web-v1",
        "location": {"lat": lat, "lng": lng, "crs": "EPSG:4326"},
        "address": {"parcel": parcel_address, "road": road_address},
        "categories": categories,
        "reportRows": report_rows,
        "sources": list(unique_sources.values()),
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "errors": errors,
        "partial": bool(errors),
    }
