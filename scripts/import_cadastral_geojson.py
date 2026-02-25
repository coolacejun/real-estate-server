#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import glob
import hashlib
import heapq
import json
import math
import numbers
import os
import sys
import tempfile
import threading
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import psycopg
from dataset_record_store import DatasetSchemaRegistry

try:
    import ijson
except ImportError as exc:  # pragma: no cover
    raise SystemExit("ijson이 필요합니다. api 컨테이너 재빌드 후 실행하세요.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="연속지적 GeoJSON -> cadastral_features 적재")
    parser.add_argument("--data-type", default="cadastral")
    parser.add_argument("--release-id", type=int, required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--pattern", default="AL_D002*.json")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("CADASTRAL_IMPORT_WORKERS", "0") or "0"),
    )
    parser.add_argument("--label-precision", type=float, default=0.00002)
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--truncate-release", action="store_true")
    parser.add_argument("--operation-mode", default="full", choices=("full", "update"))
    parser.add_argument("--tile-change-file", default="")
    parser.add_argument(
        "--op-columns",
        default="작업구분,변경구분,변동구분,갱신구분,op,operation,crud,A8,a8",
    )
    parser.add_argument("--mark-ready", action="store_true", default=False)
    parser.add_argument("--activate-on-complete", action="store_true", default=False)
    return parser.parse_args()


def iter_features(path: Path) -> Iterable[dict[str, Any]]:
    if path.suffix.lower() == ".ndjson":
        with path.open("r", encoding="utf-8", errors="ignore") as fp:
            for raw in fp:
                line = raw.strip()
                if not line:
                    continue
                try:
                    feature = json.loads(line)
                except Exception:
                    continue
                if isinstance(feature, dict):
                    yield feature
        return

    with path.open("rb") as fp:
        for feature in ijson.items(fp, "features.item"):
            if isinstance(feature, dict):
                yield feature


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _dataset_code_from_name(path: Path) -> str:
    stem = path.stem.upper()
    parts = stem.split("_")
    if len(parts) >= 2 and parts[0] == "AL" and parts[1].startswith("D"):
        return f"{parts[0]}_{parts[1]}"
    return stem[:40]


def _normalize_op_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace(" ", "").replace("_", "").replace("-", "")
    return text


def _parse_operation(payload: dict[str, Any], op_columns: list[str]) -> str:
    add_tokens = {"add", "insert", "create", "new", "추가", "신규", "등록", "a", "i", "c"}
    update_tokens = {"update", "modify", "수정", "변경", "정정", "갱신", "u", "m"}
    delete_tokens = {"delete", "remove", "삭제", "말소", "d", "r"}
    # Update 소스에서 작업구분이 A8/a8에 담기는 경우가 많아서 fallback으로 항상 함께 검사.
    seen: set[str] = set()
    for key in [*op_columns, "A8", "a8"]:
        if key in seen:
            continue
        seen.add(key)
        if key not in payload:
            continue
        token = _normalize_op_text(payload.get(key))
        if not token:
            continue
        if token in delete_tokens:
            return "delete"
        if token in add_tokens:
            return "add"
        if token in update_tokens:
            return "update"
    return "none"


def _iter_positions(node: Any) -> Iterable[tuple[float, float]]:
    if isinstance(node, (list, tuple)):
        if len(node) >= 2 and isinstance(node[0], numbers.Number) and isinstance(node[1], numbers.Number):
            yield float(node[0]), float(node[1])
            return
        for child in node:
            yield from _iter_positions(child)


def geometry_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float] | None:
    coords = geometry.get("coordinates")
    if coords is None:
        return None

    min_lon = float("inf")
    max_lon = float("-inf")
    min_lat = float("inf")
    max_lat = float("-inf")
    count = 0

    for lon, lat in _iter_positions(coords):
        min_lon = min(min_lon, lon)
        max_lon = max(max_lon, lon)
        min_lat = min(min_lat, lat)
        max_lat = max(max_lat, lat)
        count += 1

    if count == 0:
        return None

    return (min_lon, max_lon, min_lat, max_lat)


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _normalize_bbox_values(
    min_lon: Any,
    max_lon: Any,
    min_lat: Any,
    max_lat: Any,
) -> tuple[float, float, float, float] | None:
    lon0 = _safe_float(min_lon)
    lon1 = _safe_float(max_lon)
    lat0 = _safe_float(min_lat)
    lat1 = _safe_float(max_lat)
    if lon0 is None or lon1 is None or lat0 is None or lat1 is None:
        return None

    if lon0 > lon1:
        lon0, lon1 = lon1, lon0
    if lat0 > lat1:
        lat0, lat1 = lat1, lat0

    lon0 = max(-180.0, min(180.0, lon0))
    lon1 = max(-180.0, min(180.0, lon1))
    lat0 = max(-85.05112878, min(85.05112878, lat0))
    lat1 = max(-85.05112878, min(85.05112878, lat1))
    if lon0 > lon1 or lat0 > lat1:
        return None
    return lon0, lon1, lat0, lat1


class TileChangeCollector:
    def __init__(self, output_path: str, *, max_boxes: int = 20000) -> None:
        self.output_path = Path(output_path).resolve() if str(output_path).strip() else None
        self.max_boxes = max(100, int(max_boxes))
        self.boxes: list[list[float]] = []
        self.overflow_bbox_count = 0
        self.global_bbox: list[float] | None = None

    @property
    def enabled(self) -> bool:
        return self.output_path is not None

    def _merge_global(self, bbox: tuple[float, float, float, float]) -> None:
        min_lon, max_lon, min_lat, max_lat = bbox
        if self.global_bbox is None:
            self.global_bbox = [min_lon, max_lon, min_lat, max_lat]
            return
        self.global_bbox[0] = min(self.global_bbox[0], min_lon)
        self.global_bbox[1] = max(self.global_bbox[1], max_lon)
        self.global_bbox[2] = min(self.global_bbox[2], min_lat)
        self.global_bbox[3] = max(self.global_bbox[3], max_lat)

    def add(self, min_lon: Any, max_lon: Any, min_lat: Any, max_lat: Any) -> None:
        if not self.enabled:
            return
        normalized = _normalize_bbox_values(min_lon, max_lon, min_lat, max_lat)
        if normalized is None:
            return
        self._merge_global(normalized)
        if len(self.boxes) < self.max_boxes:
            self.boxes.append([normalized[0], normalized[1], normalized[2], normalized[3]])
        else:
            self.overflow_bbox_count += 1

    def add_many(self, rows: Iterable[tuple[Any, Any, Any, Any]]) -> None:
        for min_lon, max_lon, min_lat, max_lat in rows:
            self.add(min_lon, max_lon, min_lat, max_lat)

    def write(
        self,
        *,
        release_id: int,
        job_id: int | None,
        operation_mode: str,
        processed_files: int,
        inserted_rows: int,
    ) -> None:
        if not self.enabled or self.output_path is None:
            return
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "release_id": int(release_id),
            "job_id": int(job_id) if job_id is not None else None,
            "operation_mode": str(operation_mode or "").strip().lower(),
            "processed_files": int(processed_files),
            "inserted_rows": int(inserted_rows),
            "bbox_count": len(self.boxes),
            "overflow_bbox_count": int(self.overflow_bbox_count),
            "boxes": self.boxes,
            "global_bbox": self.global_bbox,
        }
        tmp_path = self.output_path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        tmp_path.replace(self.output_path)


def _ring_core_points(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if len(points) >= 2:
        x0, y0 = points[0]
        x1, y1 = points[-1]
        if abs(x0 - x1) < 1e-12 and abs(y0 - y1) < 1e-12:
            return points[:-1]
    return points


def _ring_signed_area(points: list[tuple[float, float]]) -> float:
    core = _ring_core_points(points)
    n = len(core)
    if n < 3:
        return 0.0
    area2 = 0.0
    for i in range(n):
        x1, y1 = core[i]
        x2, y2 = core[(i + 1) % n]
        area2 += (x1 * y2) - (x2 * y1)
    return area2 * 0.5


def _ring_centroid(points: list[tuple[float, float]]) -> tuple[float, float] | None:
    core = _ring_core_points(points)
    n = len(core)
    if n == 0:
        return None
    if n < 3:
        xs = [p[0] for p in core]
        ys = [p[1] for p in core]
        return sum(xs) / n, sum(ys) / n

    area2 = 0.0
    cx_num = 0.0
    cy_num = 0.0
    for i in range(n):
        x1, y1 = core[i]
        x2, y2 = core[(i + 1) % n]
        cross = (x1 * y2) - (x2 * y1)
        area2 += cross
        cx_num += (x1 + x2) * cross
        cy_num += (y1 + y2) * cross

    if abs(area2) < 1e-12:
        xs = [p[0] for p in core]
        ys = [p[1] for p in core]
        return sum(xs) / n, sum(ys) / n
    return cx_num / (3.0 * area2), cy_num / (3.0 * area2)


def _ring_bbox(points: list[tuple[float, float]]) -> tuple[float, float, float, float] | None:
    core = _ring_core_points(points)
    if not core:
        return None
    xs = [p[0] for p in core]
    ys = [p[1] for p in core]
    return min(xs), min(ys), max(xs), max(ys)


def _point_in_ring(x: float, y: float, ring: list[tuple[float, float]]) -> bool:
    core = _ring_core_points(ring)
    n = len(core)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = core[i]
        xj, yj = core[j]
        intersects = ((yi > y) != (yj > y)) and (
            x < ((xj - xi) * (y - yi) / ((yj - yi) if abs(yj - yi) > 1e-12 else 1e-12)) + xi
        )
        if intersects:
            inside = not inside
        j = i
    return inside


def _dist_to_segment(px: float, py: float, ax: float, ay: float, bx: float, by: float) -> float:
    dx = bx - ax
    dy = by - ay
    if dx == 0.0 and dy == 0.0:
        return math.hypot(px - ax, py - ay)
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    qx = ax + t * dx
    qy = ay + t * dy
    return math.hypot(px - qx, py - qy)


def _point_to_polygon_signed_distance(x: float, y: float, polygon: list[list[tuple[float, float]]]) -> float:
    if not polygon:
        return float("-inf")
    outer = _ring_core_points(polygon[0])
    if len(outer) < 3:
        return float("-inf")

    inside = _point_in_ring(x, y, outer)
    if inside:
        for hole in polygon[1:]:
            if _point_in_ring(x, y, hole):
                inside = False
                break

    min_dist = float("inf")
    for ring in polygon:
        core = _ring_core_points(ring)
        n = len(core)
        if n < 2:
            continue
        for i in range(n):
            ax, ay = core[i]
            bx, by = core[(i + 1) % n]
            d = _dist_to_segment(x, y, ax, ay, bx, by)
            if d < min_dist:
                min_dist = d

    if min_dist == float("inf"):
        min_dist = 0.0
    return min_dist if inside else -min_dist


def _polylabel(
    polygon: list[list[tuple[float, float]]],
    precision: float,
) -> tuple[float, float, float] | None:
    if not polygon:
        return None

    outer = _ring_core_points(polygon[0])
    if len(outer) < 3:
        return None

    box = _ring_bbox(outer)
    if box is None:
        return None
    min_x, min_y, max_x, max_y = box
    width = max_x - min_x
    height = max_y - min_y
    if width <= 0 or height <= 0:
        center = _ring_centroid(outer)
        if center is None:
            return None
        cx, cy = center
        dist = _point_to_polygon_signed_distance(cx, cy, polygon)
        return cx, cy, max(0.0, dist)

    def make_cell(cell_x: float, cell_y: float, h: float) -> tuple[float, float, float, float, float, float]:
        d = _point_to_polygon_signed_distance(cell_x, cell_y, polygon)
        max_d = d + h * math.sqrt(2.0)
        return (-max_d, cell_x, cell_y, h, d, max_d)

    cell_size = min(width, height)
    h = cell_size * 0.5
    heap: list[tuple[float, float, float, float, float, float]] = []

    x = min_x
    while x < max_x:
        y = min_y
        while y < max_y:
            heapq.heappush(heap, make_cell(x + h, y + h, h))
            y += cell_size
        x += cell_size

    centroid = _ring_centroid(outer) or ((min_x + max_x) * 0.5, (min_y + max_y) * 0.5)
    best = make_cell(centroid[0], centroid[1], 0.0)
    bbox_center = make_cell((min_x + max_x) * 0.5, (min_y + max_y) * 0.5, 0.0)
    if bbox_center[4] > best[4]:
        best = bbox_center

    search_precision = max(1e-12, precision)
    while heap:
        cell = heapq.heappop(heap)
        if cell[4] > best[4]:
            best = cell
        if (cell[5] - best[4]) <= search_precision:
            continue
        h2 = cell[3] * 0.5
        if h2 <= 0:
            continue
        heapq.heappush(heap, make_cell(cell[1] - h2, cell[2] - h2, h2))
        heapq.heappush(heap, make_cell(cell[1] + h2, cell[2] - h2, h2))
        heapq.heappush(heap, make_cell(cell[1] - h2, cell[2] + h2, h2))
        heapq.heappush(heap, make_cell(cell[1] + h2, cell[2] + h2, h2))

    return best[1], best[2], max(0.0, best[4])


def _parse_ring_points(ring: Any) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    if not isinstance(ring, list):
        return points
    for point in ring:
        if (
            isinstance(point, (list, tuple))
            and len(point) >= 2
            and isinstance(point[0], numbers.Number)
            and isinstance(point[1], numbers.Number)
        ):
            points.append((float(point[0]), float(point[1])))
    return points


def geometry_surfaces(geometry: dict[str, Any]) -> list[list[list[tuple[float, float]]]]:
    g_type = geometry.get("type")
    coords = geometry.get("coordinates")
    surfaces: list[list[list[tuple[float, float]]]] = []

    if g_type == "Polygon" and isinstance(coords, list):
        polygon: list[list[tuple[float, float]]] = []
        for ring in coords:
            points = _parse_ring_points(ring)
            if len(_ring_core_points(points)) >= 3:
                polygon.append(points)
        if polygon:
            surfaces.append(polygon)
        return surfaces

    if g_type == "MultiPolygon" and isinstance(coords, list):
        for polygon_raw in coords:
            if not isinstance(polygon_raw, list):
                continue
            polygon: list[list[tuple[float, float]]] = []
            for ring in polygon_raw:
                points = _parse_ring_points(ring)
                if len(_ring_core_points(points)) >= 3:
                    polygon.append(points)
            if polygon:
                surfaces.append(polygon)
    return surfaces


def geometry_label_point(geometry: dict[str, Any], min_precision: float) -> tuple[float, float] | None:
    surfaces = geometry_surfaces(geometry)
    best_point: tuple[float, float] | None = None
    best_area = -1.0

    for surface in surfaces:
        outer = _ring_core_points(surface[0])
        if len(outer) < 3:
            continue
        bbox = _ring_bbox(outer)
        if bbox is None:
            continue
        width = max(0.0, bbox[2] - bbox[0])
        height = max(0.0, bbox[3] - bbox[1])
        adaptive_precision = max(min_precision, min(width, height) / 120.0) if min(width, height) > 0 else min_precision
        point = _polylabel(surface, precision=adaptive_precision)
        if point is None:
            continue
        area = abs(_ring_signed_area(outer))
        if area > best_area:
            best_area = area
            best_point = (point[0], point[1])

    return best_point


def update_release_status(conn: psycopg.Connection, release_id: int, status: str, records_count: int | None = None) -> None:
    if records_count is None:
        conn.execute(
            """
            UPDATE cadastral_release
            SET status = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (status, release_id),
        )
        return

    conn.execute(
        """
        UPDATE cadastral_release
        SET status = %s, records_count = %s, updated_at = NOW()
        WHERE id = %s
        """,
        (status, records_count, release_id),
    )


def update_job(
    conn: psycopg.Connection,
    job_id: int,
    status: str | None = None,
    total_files: int | None = None,
    processed_files: int | None = None,
    inserted_rows: int | None = None,
    error_message: str | None = None,
) -> None:
    updates: list[str] = []
    params: list[Any] = []

    if status is not None:
        updates.append("status = %s")
        params.append(status)
        if status == "RUNNING":
            updates.append("started_at = COALESCE(started_at, NOW())")
        if status in {"FAILED", "SUCCEEDED", "CANCELLED"}:
            updates.append("finished_at = NOW()")

    if total_files is not None:
        updates.append("total_files = %s")
        params.append(total_files)

    if processed_files is not None:
        updates.append("processed_files = %s")
        params.append(processed_files)

    if inserted_rows is not None:
        updates.append("inserted_rows = %s")
        params.append(inserted_rows)

    if error_message is not None:
        updates.append("error_message = %s")
        params.append(error_message[:2000])

    if not updates:
        return

    updates.append("updated_at = NOW()")
    params.append(job_id)

    query = f"UPDATE cadastral_import_job SET {', '.join(updates)} WHERE id = %s"
    conn.execute(query, params)


def increment_job_progress(
    conn: psycopg.Connection,
    job_id: int | None,
    inserted_rows_delta: int = 0,
    processed_files_delta: int = 0,
) -> None:
    if job_id is None:
        return
    updates: list[str] = []
    params: list[Any] = []
    if inserted_rows_delta > 0:
        updates.append("inserted_rows = COALESCE(inserted_rows, 0) + %s")
        params.append(int(inserted_rows_delta))
    if processed_files_delta > 0:
        updates.append("processed_files = COALESCE(processed_files, 0) + %s")
        params.append(int(processed_files_delta))
    if not updates:
        return
    updates.append("updated_at = NOW()")
    params.append(int(job_id))
    conn.execute(
        f"UPDATE cadastral_import_job SET {', '.join(updates)} WHERE id = %s",
        params,
    )


def ensure_job_worker_progress_table(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cadastral_import_job_worker (
          id BIGSERIAL PRIMARY KEY,
          job_id BIGINT NOT NULL REFERENCES cadastral_import_job(id) ON DELETE CASCADE,
          source_file TEXT NOT NULL,
          worker_name TEXT,
          status TEXT NOT NULL DEFAULT 'QUEUED',
          processed_rows BIGINT NOT NULL DEFAULT 0,
          error_message TEXT,
          started_at TIMESTAMPTZ,
          finished_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS cadastral_import_job_worker_job_file_uidx
          ON cadastral_import_job_worker (job_id, source_file)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS cadastral_import_job_worker_job_status_idx
          ON cadastral_import_job_worker (job_id, status, id DESC)
        """
    )


def reset_job_worker_progress(conn: psycopg.Connection, job_id: int | None, files: list[Path]) -> None:
    if job_id is None:
        return
    ensure_job_worker_progress_table(conn)
    conn.execute("DELETE FROM cadastral_import_job_worker WHERE job_id = %s", (int(job_id),))
    if not files:
        return
    rows = [(int(job_id), path.name) for path in files]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO cadastral_import_job_worker (
              job_id, source_file, status, processed_rows, updated_at
            )
            VALUES (%s, %s, 'QUEUED', 0, NOW())
            """,
            rows,
        )


def mark_worker_running(
    conn: psycopg.Connection,
    job_id: int | None,
    source_file: str,
    worker_name: str,
) -> None:
    if job_id is None:
        return
    conn.execute(
        """
        INSERT INTO cadastral_import_job_worker (
          job_id, source_file, worker_name, status, processed_rows, error_message, started_at, finished_at, updated_at
        )
        VALUES (%s, %s, %s, 'RUNNING', 0, NULL, NOW(), NULL, NOW())
        ON CONFLICT (job_id, source_file)
        DO UPDATE
           SET worker_name = EXCLUDED.worker_name,
               status = 'RUNNING',
               error_message = NULL,
               started_at = COALESCE(cadastral_import_job_worker.started_at, NOW()),
               finished_at = NULL,
               updated_at = NOW()
        """,
        (int(job_id), source_file, worker_name),
    )


def increment_worker_progress(
    conn: psycopg.Connection,
    job_id: int | None,
    source_file: str,
    processed_rows_delta: int,
) -> None:
    if job_id is None or processed_rows_delta <= 0:
        return
    conn.execute(
        """
        UPDATE cadastral_import_job_worker
        SET processed_rows = COALESCE(processed_rows, 0) + %s,
            updated_at = NOW()
        WHERE job_id = %s
          AND source_file = %s
        """,
        (int(processed_rows_delta), int(job_id), source_file),
    )


def mark_worker_finished(
    conn: psycopg.Connection,
    job_id: int | None,
    source_file: str,
    status: str,
    error_message: str | None = None,
) -> None:
    if job_id is None:
        return
    final_error = (error_message or "").strip()
    if len(final_error) > 2000:
        final_error = final_error[:2000]
    conn.execute(
        """
        UPDATE cadastral_import_job_worker
        SET status = %s,
            error_message = CASE WHEN %s = '' THEN NULL ELSE %s END,
            finished_at = NOW(),
            updated_at = NOW()
        WHERE job_id = %s
          AND source_file = %s
        """,
        (status, final_error, final_error, int(job_id), source_file),
    )


def mark_open_workers_as_failed(conn: psycopg.Connection, job_id: int | None, reason: str) -> None:
    if job_id is None:
        return
    final_error = (reason or "").strip()
    if len(final_error) > 2000:
        final_error = final_error[:2000]
    conn.execute(
        """
        UPDATE cadastral_import_job_worker
        SET status = 'FAILED',
            error_message = CASE
                WHEN COALESCE(error_message, '') = '' THEN %s
                ELSE error_message
            END,
            finished_at = COALESCE(finished_at, NOW()),
            updated_at = NOW()
        WHERE job_id = %s
          AND status IN ('QUEUED', 'RUNNING')
        """,
        (final_error, int(job_id)),
    )


def ensure_dataset_import_file_table(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_import_file (
          id BIGSERIAL PRIMARY KEY,
          release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
          data_type TEXT NOT NULL,
          file_name TEXT NOT NULL,
          file_size BIGINT NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS dataset_import_file_release_id_idx
          ON dataset_import_file (release_id, id DESC)
        """
    )


def load_recorded_update_file_names(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT file_name
            FROM dataset_import_file
            WHERE release_id = %s
              AND data_type = %s
            """,
            (int(release_id), str(data_type or "").strip().lower()),
        )
        rows = cur.fetchall()
    return {
        str(row[0]).strip()
        for row in rows
        if row and row[0] is not None and str(row[0]).strip()
    }


def record_update_file_name(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
    file_name: str,
    file_size: int,
) -> None:
    normalized_name = str(file_name or "").strip()
    normalized_type = str(data_type or "").strip().lower()
    if not normalized_name:
        return
    conn.execute(
        """
        INSERT INTO dataset_import_file (
          release_id, data_type, file_name, file_size
        )
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (
          SELECT 1
          FROM dataset_import_file
          WHERE release_id = %s
            AND data_type = %s
            AND file_name = %s
        )
        """,
        (
            int(release_id),
            normalized_type,
            normalized_name,
            max(0, int(file_size)),
            int(release_id),
            normalized_type,
            normalized_name,
        ),
    )


def insert_batch(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return

    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY cadastral_features (
              release_id, pnu, label, geojson,
              bbox_min_lon, bbox_max_lon, bbox_min_lat, bbox_max_lat,
              label_lon, label_lat
            )
            FROM STDIN
            """,
        ) as copy:
            for row in rows:
                copy.write_row(row)


def insert_dataset_batch(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return

    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY dataset_record (
              release_id, data_type, dataset_code, source_file, row_no, pnu,
              schema_id, payload_values, payload, geometry
            )
            FROM STDIN
            """,
        ) as copy:
            for row in rows:
                copy.write_row(row)


def _resolve_worker_count(requested: int) -> int:
    if requested > 0:
        return max(1, int(requested))
    cpu_count = max(1, int(os.cpu_count() or 1))
    return max(1, cpu_count)


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _split_geojson_file_round_robin(
    source: Path,
    split_root: Path,
    worker_count: int,
) -> tuple[list[Path], dict[str, Any]]:
    shards_per_file = max(
        2,
        min(
            worker_count,
            _parse_positive_int_env("CADASTRAL_SPLIT_SHARDS_PER_FILE", worker_count),
        ),
    )
    token = hashlib.sha1(str(source).encode("utf-8")).hexdigest()[:8]

    handles: list[Any] = []
    split_paths: list[Path] = []
    counts: list[int] = [0 for _ in range(shards_per_file)]
    source_feature_count = 0
    try:
        for i in range(shards_per_file):
            split_path = split_root / f"{source.stem}__{token}__shard_{i+1:03d}.ndjson"
            split_paths.append(split_path)
            handles.append(split_path.open("w", encoding="utf-8"))

        for idx, feature in enumerate(iter_features(source)):
            source_feature_count += 1
            target_idx = idx % shards_per_file
            handles[target_idx].write(
                json.dumps(feature, ensure_ascii=False, default=_json_default, separators=(",", ":"))
            )
            handles[target_idx].write("\n")
            counts[target_idx] += 1
    finally:
        for h in handles:
            with contextlib.suppress(Exception):
                h.close()

    final_files = [split_paths[i] for i, c in enumerate(counts) if c > 0]
    shard_counts = [int(c) for c in counts if c > 0]
    if len(final_files) <= 1:
        return [source], {
            "source_feature_count": int(source_feature_count),
            "shard_feature_counts": shard_counts,
            "shard_count": len(shard_counts),
        }
    return final_files, {
        "source_feature_count": int(source_feature_count),
        "shard_feature_counts": shard_counts,
        "shard_count": len(shard_counts),
    }


def _prepare_parallel_input_files(
    files: list[Path],
    worker_count: int,
) -> tuple[list[Path], tempfile.TemporaryDirectory[str] | None, list[dict[str, Any]]]:
    if worker_count <= 1 or not files:
        return files, None, []

    temp_dir = tempfile.TemporaryDirectory(prefix="cadastral_split_")
    split_root = Path(temp_dir.name)
    all_split_files: list[Path] = []
    split_meta_entries: list[dict[str, Any]] = []

    for source in files:
        split_files, split_meta = _split_geojson_file_round_robin(source, split_root, worker_count)
        all_split_files.extend(split_files)
        split_meta_entries.append(split_meta)
        print(
            f"[INFO] 파일 분할: source={source.name}, shards={len(split_files)}",
            flush=True,
        )

    if not all_split_files:
        with contextlib.suppress(Exception):
            temp_dir.cleanup()
        return files, None, []

    print(
        f"[INFO] 분할 완료: sources={len(files)}, total_shards={len(all_split_files)}",
        flush=True,
    )
    return all_split_files, temp_dir, split_meta_entries


def _import_full_file(
    db_url: str,
    release_id: int,
    data_type: str,
    path: Path,
    batch_size: int,
    progress_step: int,
    job_id: int | None,
    label_precision: float,
) -> dict[str, Any]:
    source_file = path.name
    worker_name = threading.current_thread().name
    dataset_code = _dataset_code_from_name(path)
    feature_rows: list[tuple[Any, ...]] = []
    dataset_rows: list[tuple[Any, ...]] = []
    inserted_rows = 0
    reported_rows = 0

    def flush_rows(conn: psycopg.Connection) -> None:
        nonlocal inserted_rows
        nonlocal reported_rows
        if not feature_rows and not dataset_rows:
            return
        with conn.transaction():
            if feature_rows:
                insert_batch(conn, feature_rows)
                inserted_now = len(feature_rows)
                inserted_rows += inserted_now
                reported_rows += inserted_now
            if dataset_rows:
                insert_dataset_batch(conn, dataset_rows)
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        feature_rows.clear()
        dataset_rows.clear()

    with psycopg.connect(db_url, autocommit=True) as conn:
        schema_registry = DatasetSchemaRegistry(conn, data_type)
        with conn.transaction():
            schema_registry.ensure_release_partition(release_id)
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            row_no = 0
            for feature in iter_features(path):
                row_no += 1
                properties = feature.get("properties")
                properties = properties if isinstance(properties, dict) else {}
                geometry = feature.get("geometry")
                geometry_dict = geometry if isinstance(geometry, dict) else None

                pnu_raw = properties.get("A1") or properties.get("pnu")
                label_raw = properties.get("A4") or properties.get("A5") or properties.get("label")
                pnu = str(pnu_raw).strip() if pnu_raw is not None else ""
                if not pnu:
                    continue

                schema_id, payload_values = schema_registry.encode_payload(
                    dataset_code,
                    properties,
                    json_default=_json_default,
                )
                dataset_rows.append(
                    (
                        release_id,
                        data_type,
                        dataset_code,
                        source_file,
                        row_no,
                        pnu,
                        schema_id,
                        payload_values,
                        None,
                        schema_registry.encode_geometry(geometry_dict, json_default=_json_default),
                    )
                )

                label = str(label_raw).strip() if label_raw is not None else ""
                if geometry_dict:
                    bbox = geometry_bbox(geometry_dict)
                    if bbox is not None:
                        label_point = geometry_label_point(
                            geometry_dict,
                            min_precision=max(1e-12, label_precision),
                        )
                        feature_rows.append(
                            (
                                release_id,
                                pnu,
                                label or None,
                                json.dumps(geometry_dict, ensure_ascii=False, default=_json_default, separators=(",", ":")),
                                bbox[0],
                                bbox[1],
                                bbox[2],
                                bbox[3],
                                label_point[0] if label_point else None,
                                label_point[1] if label_point else None,
                            )
                        )

                if len(feature_rows) >= batch_size or len(dataset_rows) >= batch_size:
                    flush_rows(conn)

            flush_rows(conn)
            if reported_rows > 0:
                with conn.transaction():
                    increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                    increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                    reported_rows = 0
            with conn.transaction():
                increment_job_progress(conn, job_id, processed_files_delta=1)
                mark_worker_finished(conn, job_id, source_file, "SUCCEEDED")
        except Exception as exc:
            with contextlib.suppress(Exception):
                with conn.transaction():
                    if reported_rows > 0:
                        increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                        increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                    mark_worker_finished(conn, job_id, source_file, "FAILED", str(exc))
            raise

    return {
        "file": source_file,
        "inserted_rows": inserted_rows,
    }


def main() -> int:
    args = parse_args()
    db_url = args.db_url.strip()
    if not db_url:
        raise SystemExit("DATABASE_URL이 필요합니다. --db-url 또는 환경변수로 전달하세요.")

    source_dir = Path(args.source_dir)
    files = sorted(Path(p).resolve() for p in glob.glob(str(source_dir / args.pattern)))
    if not files:
        raise SystemExit(f"대상 파일이 없습니다: {source_dir}/{args.pattern}")

    release_id = args.release_id
    batch_size = max(100, args.batch_size)
    operation_mode = (args.operation_mode or "full").strip().lower()
    is_update_mode = operation_mode == "update"
    op_columns = [col.strip() for col in str(args.op_columns or "").split(",") if col.strip()]
    try:
        tile_change_max_boxes = int(os.getenv("CADASTRAL_TILE_CHANGE_MAX_BOXES", "20000") or "20000")
    except Exception:
        tile_change_max_boxes = 20000
    tile_changes = TileChangeCollector(
        str(args.tile_change_file or ""),
        max_boxes=tile_change_max_boxes,
    )

    progress_step = max(5000, batch_size)
    data_type = (args.data_type or "cadastral").strip().lower()
    inserted_total = 0
    processed_files = 0
    release_is_active = False

    requested_workers = int(args.workers or 0)
    worker_count = _resolve_worker_count(requested_workers)
    temp_split_dir: Any | None = None
    split_meta_entries: list[dict[str, Any]] = []
    if not is_update_mode:
        files, temp_split_dir, split_meta_entries = _prepare_parallel_input_files(files, worker_count)
    parallel_full_mode = (not is_update_mode) and worker_count > 1 and len(files) > 1

    try:
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data_type, is_active FROM cadastral_release WHERE id = %s", (release_id,))
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(f"release not found: {release_id}")
                data_type = str(row[0] or data_type)
                release_is_active = bool(row[1])
            with conn.transaction():
                ensure_dataset_import_file_table(conn)
            if is_update_mode:
                with conn.transaction():
                    recorded_files = load_recorded_update_file_names(
                        conn,
                        release_id=release_id,
                        data_type=data_type,
                    )
                if recorded_files:
                    skipped_files: list[str] = []
                    pending_files: list[Path] = []
                    for path in files:
                        if path.name in recorded_files:
                            skipped_files.append(path.name)
                            continue
                        pending_files.append(path)
                    files = pending_files
                    if skipped_files:
                        preview = ", ".join(skipped_files[:5])
                        suffix = "" if len(skipped_files) <= 5 else f", ... +{len(skipped_files) - 5}"
                        print(
                            f"[INFO] update 중복 파일 스킵: count={len(skipped_files)}, files={preview}{suffix}",
                            flush=True,
                        )
            schema_registry = DatasetSchemaRegistry(conn, data_type)
            with conn.transaction():
                schema_registry.ensure_release_partition(release_id)
                update_release_status(conn, release_id, "IMPORTING")
                if args.job_id is not None:
                    update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                    reset_job_worker_progress(conn, args.job_id, files)

            if args.truncate_release:
                with conn.transaction():
                    conn.execute("DELETE FROM cadastral_features WHERE release_id = %s", (release_id,))
                    conn.execute("DELETE FROM dataset_record WHERE release_id = %s", (release_id,))
                    conn.execute(
                        """
                        DELETE FROM dataset_import_file
                        WHERE release_id = %s
                          AND data_type = %s
                        """,
                        (release_id, data_type),
                    )

            if parallel_full_mode:
                print(
                    f"[INFO] full 병렬 적재 시작: workers={worker_count}, files={len(files)}, batch_size={batch_size}, executor=thread",
                    flush=True,
                )
                futures: dict[concurrent.futures.Future[dict[str, Any]], Path] = {}
                executor: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
                try:
                    for path in files:
                        future = executor.submit(
                            _import_full_file,
                            db_url,
                            release_id,
                            data_type,
                            path,
                            batch_size,
                            progress_step,
                            args.job_id,
                            args.label_precision,
                        )
                        futures[future] = path

                    for future in concurrent.futures.as_completed(futures):
                        path = futures[future]
                        try:
                            result = future.result()
                        except Exception as exc:
                            raise RuntimeError(f"{path.name} 처리 실패: {exc}") from exc
                        inserted_total += int(result.get("inserted_rows") or 0)
                        processed_files += 1
                        print(f"[OK] {result.get('file', path.name)} 처리 완료 (누적 rows: {inserted_total})", flush=True)
                finally:
                    executor.shutdown(wait=True, cancel_futures=True)
            else:
                for path in files:
                    rows: list[tuple[Any, ...]] = []
                    dataset_rows: list[tuple[Any, ...]] = []
                    pending_delete_pnus: set[str] = set()
                    cleared_pnus: set[str] = set()
                    dataset_code = _dataset_code_from_name(path)
                    row_no = 0
                    reported_rows = 0
                    with conn.transaction():
                        mark_worker_running(conn, args.job_id, path.name, "single-runner")

                    def flush_rows() -> None:
                        nonlocal inserted_total
                        nonlocal reported_rows
                        if not rows and not dataset_rows and not pending_delete_pnus:
                            return
                        with conn.transaction():
                            def collect_existing_bboxes(pnus: list[str]) -> None:
                                if (not is_update_mode) or (not pnus) or (not tile_changes.enabled):
                                    return
                                with conn.cursor() as cur:
                                    cur.execute(
                                        """
                                        SELECT bbox_min_lon, bbox_max_lon, bbox_min_lat, bbox_max_lat
                                        FROM cadastral_features
                                        WHERE release_id = %s
                                          AND pnu = ANY(%s)
                                        """,
                                        (release_id, pnus),
                                    )
                                    tile_changes.add_many(cur.fetchall())

                            if is_update_mode:
                                delete_pnus = sorted(set(pending_delete_pnus))
                                if delete_pnus:
                                    collect_existing_bboxes(delete_pnus)
                                    conn.execute(
                                        """
                                        DELETE FROM dataset_record
                                        WHERE release_id = %s
                                          AND data_type = %s
                                          AND dataset_code = %s
                                          AND pnu = ANY(%s)
                                        """,
                                        (release_id, data_type, dataset_code, delete_pnus),
                                    )
                                    conn.execute(
                                        """
                                        DELETE FROM cadastral_features
                                        WHERE release_id = %s
                                          AND pnu = ANY(%s)
                                        """,
                                        (release_id, delete_pnus),
                                    )
                                    cleared_pnus.update(delete_pnus)

                                upsert_all = {str(row[5]) for row in dataset_rows if row[5]}
                                upsert_pnus = sorted(pnu for pnu in upsert_all if pnu not in cleared_pnus)
                                if upsert_pnus:
                                    collect_existing_bboxes(upsert_pnus)
                                    conn.execute(
                                        """
                                        DELETE FROM dataset_record
                                        WHERE release_id = %s
                                          AND data_type = %s
                                          AND dataset_code = %s
                                          AND pnu = ANY(%s)
                                        """,
                                        (release_id, data_type, dataset_code, upsert_pnus),
                                    )
                                    conn.execute(
                                        """
                                        DELETE FROM cadastral_features
                                        WHERE release_id = %s
                                          AND pnu = ANY(%s)
                                        """,
                                        (release_id, upsert_pnus),
                                    )
                                    cleared_pnus.update(upsert_pnus)

                            if rows:
                                if tile_changes.enabled:
                                    for row in rows:
                                        tile_changes.add(row[4], row[5], row[6], row[7])
                                insert_batch(conn, rows)
                                inserted_now = len(rows)
                                inserted_total += inserted_now
                                reported_rows += inserted_now
                            if dataset_rows:
                                insert_dataset_batch(conn, dataset_rows)
                            if reported_rows >= progress_step:
                                increment_job_progress(conn, args.job_id, inserted_rows_delta=reported_rows)
                                increment_worker_progress(conn, args.job_id, path.name, processed_rows_delta=reported_rows)
                                reported_rows = 0

                        rows.clear()
                        dataset_rows.clear()
                        pending_delete_pnus.clear()

                    for feature in iter_features(path):
                        row_no += 1
                        properties = feature.get("properties")
                        properties = properties if isinstance(properties, dict) else {}
                        geometry = feature.get("geometry")
                        geometry_dict = geometry if isinstance(geometry, dict) else None

                        pnu_raw = properties.get("A1") or properties.get("pnu")
                        label_raw = properties.get("A4") or properties.get("A5") or properties.get("label")
                        pnu = str(pnu_raw).strip() if pnu_raw is not None else ""
                        if not pnu:
                            continue
                        if is_update_mode:
                            op = _parse_operation(properties, op_columns)
                            if op == "delete":
                                pending_delete_pnus.add(pnu)
                                if len(pending_delete_pnus) >= batch_size:
                                    flush_rows()
                                continue
                        label = str(label_raw).strip() if label_raw is not None else ""
                        schema_id, payload_values = schema_registry.encode_payload(
                            dataset_code,
                            properties,
                            json_default=_json_default,
                        )
                        dataset_rows.append(
                            (
                                release_id,
                                data_type,
                                dataset_code,
                                path.name,
                                row_no,
                                pnu,
                                schema_id,
                                payload_values,
                                None,
                                schema_registry.encode_geometry(geometry_dict, json_default=_json_default),
                            )
                        )

                        if geometry_dict:
                            bbox = geometry_bbox(geometry_dict)
                            if bbox is not None:
                                label_point = geometry_label_point(
                                    geometry_dict,
                                    min_precision=max(1e-12, args.label_precision),
                                )
                                rows.append(
                                    (
                                        release_id,
                                        pnu,
                                        label or None,
                                        json.dumps(geometry_dict, ensure_ascii=False, default=_json_default, separators=(",", ":")),
                                        bbox[0],
                                        bbox[1],
                                        bbox[2],
                                        bbox[3],
                                        label_point[0] if label_point else None,
                                        label_point[1] if label_point else None,
                                    )
                                )

                        if len(rows) >= batch_size or len(dataset_rows) >= batch_size:
                            flush_rows()

                    if rows or dataset_rows or pending_delete_pnus:
                        flush_rows()

                    with conn.transaction():
                        if reported_rows > 0:
                            increment_job_progress(conn, args.job_id, inserted_rows_delta=reported_rows)
                            increment_worker_progress(conn, args.job_id, path.name, processed_rows_delta=reported_rows)
                        increment_job_progress(conn, args.job_id, processed_files_delta=1)
                        mark_worker_finished(conn, args.job_id, path.name, "SUCCEEDED")
                        if is_update_mode:
                            file_size = 0
                            with contextlib.suppress(Exception):
                                file_size = int(path.stat().st_size)
                            record_update_file_name(
                                conn,
                                release_id=release_id,
                                data_type=data_type,
                                file_name=path.name,
                                file_size=file_size,
                            )

                    processed_files += 1
                    print(f"[OK] {path.name} 처리 완료 (누적 rows: {inserted_total})", flush=True)

            final_status = "IMPORTING"
            if args.activate_on_complete:
                final_records_count = inserted_total
                if is_update_mode:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT COUNT(*)
                            FROM cadastral_features
                            WHERE release_id = %s
                            """,
                            (release_id,),
                        )
                        count_row = cur.fetchone()
                        final_records_count = int(count_row[0] or 0) if count_row else 0

                with conn.transaction():
                    conn.execute(
                        """
                        UPDATE cadastral_release
                        SET is_active = FALSE,
                            status = CASE WHEN status = 'ACTIVE' THEN 'READY' ELSE status END,
                            updated_at = NOW()
                        WHERE is_active = TRUE
                          AND data_type = %s
                          AND id <> %s
                        """,
                        (data_type, release_id),
                    )
                    conn.execute(
                        """
                        UPDATE cadastral_release
                        SET is_active = TRUE,
                            status = 'ACTIVE',
                            activated_at = NOW(),
                            records_count = %s,
                            updated_at = NOW()
                        WHERE id = %s
                          AND data_type = %s
                        """,
                        (final_records_count, release_id, data_type),
                    )
                    if args.job_id is not None:
                        update_job(
                            conn,
                            args.job_id,
                            status="SUCCEEDED",
                            inserted_rows=inserted_total,
                            error_message="",
                        )
            else:
                if args.mark_ready:
                    final_status = "ACTIVE" if release_is_active else "READY"
                with conn.transaction():
                    if is_update_mode:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT COUNT(*)
                                FROM cadastral_features
                                WHERE release_id = %s
                                """,
                                (release_id,),
                            )
                            count_row = cur.fetchone()
                            final_count = int(count_row[0] or 0) if count_row else 0
                    else:
                        final_count = inserted_total
                    update_release_status(conn, release_id, final_status, records_count=final_count)
                    if args.job_id is not None:
                        update_job(
                            conn,
                            args.job_id,
                            status="SUCCEEDED",
                            inserted_rows=inserted_total,
                            error_message="",
                        )

            if split_meta_entries:
                source_feature_count = sum(int(item.get("source_feature_count") or 0) for item in split_meta_entries)
                shard_feature_total = sum(
                    sum(int(count) for count in (item.get("shard_feature_counts") or []))
                    for item in split_meta_entries
                )
                split_status = "OK" if source_feature_count == shard_feature_total else "MISMATCH"
                print(
                    f"[INFO] 분할 검증: sources={len(split_meta_entries)}, "
                    f"source_features={source_feature_count}, "
                    f"shard_features_total={shard_feature_total}, "
                    f"status={split_status}",
                    flush=True,
                )

            if is_update_mode and tile_changes.enabled:
                tile_changes.write(
                    release_id=release_id,
                    job_id=args.job_id,
                    operation_mode=operation_mode,
                    processed_files=processed_files,
                    inserted_rows=inserted_total,
                )

    except Exception as exc:
        error_text = str(exc)
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.transaction():
                update_release_status(conn, release_id, "FAILED")
                if args.job_id is not None:
                    update_job(conn, args.job_id, status="FAILED", error_message=error_text)
                    mark_open_workers_as_failed(conn, args.job_id, error_text)
        raise
    finally:
        if temp_split_dir is not None:
            with contextlib.suppress(Exception):
                temp_split_dir.cleanup()

    print(
        f"완료: release_id={release_id}, files={processed_files}, inserted_rows={inserted_total}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
