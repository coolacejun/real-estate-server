from __future__ import annotations

import asyncio
import base64
import contextlib
import datetime as dt
import fcntl
import fnmatch
import gzip
import hashlib
import json
import logging
import math
import os
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import OrderedDict, deque
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any, Dict

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image, ImageDraw, ImageFilter, ImageFont
from psycopg import sql
from logging.handlers import RotatingFileHandler

from .pnu_aliases import pnu_query_candidates as _build_pnu_query_candidates
from .platform import router as platform_router
from .platform.body_limit import RequestBodyLimitMiddleware

app = FastAPI(title="building-land API", version="1.2.0")
app.add_middleware(RequestBodyLimitMiddleware)
app.include_router(platform_router)
APP_DIR = Path(__file__).resolve().parent
ADMIN_STATIC_DIR = APP_DIR / "static" / "admin"
SERVER_LOG_FILE = Path(os.getenv("SERVER_LOG_FILE", "/data/uploads/logs/server.log"))
_SERVER_LOG_HANDLER_LOCK = Lock()
_SERVER_LOG_HANDLER_READY = False

if ADMIN_STATIC_DIR.exists():
    app.mount("/admin/static", StaticFiles(directory=ADMIN_STATIC_DIR), name="admin-static")


def _configure_server_log_file_handler() -> None:
    global _SERVER_LOG_HANDLER_READY
    if _SERVER_LOG_HANDLER_READY:
        return

    with _SERVER_LOG_HANDLER_LOCK:
        if _SERVER_LOG_HANDLER_READY:
            return

        SERVER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

        for logger_name in ("uvicorn.error", "uvicorn.access"):
            logger = logging.getLogger(logger_name)
            has_file_handler = any(
                isinstance(handler, RotatingFileHandler)
                and Path(getattr(handler, "baseFilename", "")) == SERVER_LOG_FILE
                for handler in logger.handlers
            )
            if has_file_handler:
                continue

            file_handler = RotatingFileHandler(
                SERVER_LOG_FILE,
                maxBytes=20 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.INFO)
            logger.addHandler(file_handler)

        _SERVER_LOG_HANDLER_READY = True


def _read_log_tail_lines(path: Path, limit: int) -> list[str]:
    if limit <= 0 or not path.exists():
        return []

    out: deque[str] = deque(maxlen=limit)
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            out.append(line.rstrip("\n"))
    return list(out)


_configure_server_log_file_handler()

logger = logging.getLogger("site_plan_report")


def ok(data: Any) -> Dict[str, Any]:
    return {"ok": True, "data": data, "error": None}


class LruBytesCache:
    def __init__(self, max_items: int) -> None:
        self.max_items = max(1, max_items)
        self._cache: OrderedDict[str, bytes] = OrderedDict()
        self._lock = Lock()

    def get(self, key: str) -> bytes | None:
        with self._lock:
            value = self._cache.pop(key, None)
            if value is None:
                return None
            self._cache[key] = value
            return value

    def put(self, key: str, value: bytes) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.pop(key)
            self._cache[key] = value
            while len(self._cache) > self.max_items:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def delete(self, key: str) -> bool:
        with self._lock:
            return self._cache.pop(key, None) is not None

    def delete_prefix(self, prefix: str) -> int:
        removed = 0
        with self._lock:
            targets = [key for key in self._cache.keys() if key.startswith(prefix)]
            for key in targets:
                if self._cache.pop(key, None) is not None:
                    removed += 1
        return removed


_TILE_LOCKS: dict[str, asyncio.Lock] = {}
_TILE_LOCKS_GUARD = asyncio.Lock()
_ACTIVE_RELEASE_CACHE_LOCK = Lock()
_ACTIVE_RELEASE_CACHE_TTL = float(os.getenv("ACTIVE_RELEASE_CACHE_TTL_SEC", "5"))
_ACTIVE_RELEASE_CACHE: dict[str, Any] = {
    "loaded_at_by_type": {},
    "release_by_type": {},
}
_DATA_TYPE_PATTERN = re.compile(r"^[a-z0-9_]{1,64}$")
LAND_INFO_COMPONENTS: dict[str, dict[str, str]] = {
    "AL_D155": {
        "data_type": "land_info_al_d155",
        "key": "land_use_plan",
        "name": "토지이용계획",
    },
    "AL_D157": {
        "data_type": "land_info_al_d157",
        "key": "land_movement",
        "name": "토지이동",
    },
    "AL_D161": {
        "data_type": "land_info_al_d161",
        "key": "land_ownership",
        "name": "토지소유",
    },
    "AL_D195": {
        "data_type": "land_info_al_d195",
        "key": "land_characteristic",
        "name": "토지특성",
    },
}
LAND_INFO_DATA_TYPE_TO_CODE: dict[str, str] = {
    item["data_type"]: code for code, item in LAND_INFO_COMPONENTS.items()
}
LAND_INFO_COMPONENT_DATA_TYPES = set(LAND_INFO_DATA_TYPE_TO_CODE)
VALID_RELEASE_STATUSES = {
    "PENDING",
    "IMPORTING",
    "READY",
    "ACTIVE",
    "FAILED",
    "ARCHIVED",
}
VALID_IMPORT_JOB_STATUSES = {
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
}
VALID_OPERATION_MODES = {"full", "update"}
_IMPORT_RUNNERS: dict[int, asyncio.Task[None]] = {}
_IMPORT_RUNNERS_GUARD = asyncio.Lock()
_IMPORT_PROCESSES: dict[int, asyncio.subprocess.Process] = {}
_IMPORT_PROCESSES_GUARD = asyncio.Lock()
_CLEAR_DATA_TYPE_TASKS: dict[str, asyncio.Task[None]] = {}
_CLEAR_DATA_TYPE_STATUS: dict[str, Dict[str, Any]] = {}
_CLEAR_DATA_TYPE_GUARD = asyncio.Lock()
APP_CONFIG_PLATFORMS = ("android", "ios")


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url


def _ensure_import_worker_progress_table() -> None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
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
    except Exception:
        return


def _ensure_dataset_import_file_table() -> None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
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
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS dataset_import_file_data_type_id_idx
                      ON dataset_import_file (data_type, id DESC)
                    """
                )
    except Exception:
        return


def _ensure_app_version_config_table() -> None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_version_config (
                      platform TEXT PRIMARY KEY,
                      min_required_version BIGINT NOT NULL DEFAULT 0,
                      latest_version BIGINT NOT NULL DEFAULT 0,
                      force_update BOOLEAN NOT NULL DEFAULT FALSE,
                      title TEXT NOT NULL DEFAULT '',
                      message TEXT NOT NULL DEFAULT '',
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                conn.execute(
                    """
                    ALTER TABLE app_version_config
                    ADD COLUMN IF NOT EXISTS title TEXT NOT NULL DEFAULT ''
                    """
                )
                conn.execute(
                    """
                    ALTER TABLE app_version_config
                    ADD COLUMN IF NOT EXISTS message TEXT NOT NULL DEFAULT ''
                    """
                )
                conn.execute(
                    """
                    INSERT INTO app_version_config (platform)
                    VALUES ('android'), ('ios')
                    ON CONFLICT (platform) DO NOTHING
                    """
                )
    except Exception:
        return


def _ensure_app_maintenance_config_table() -> None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_maintenance_config (
                      platform TEXT PRIMARY KEY,
                      maintenance_notice_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                      maintenance_notice_title TEXT NOT NULL DEFAULT '',
                      maintenance_notice_message TEXT NOT NULL DEFAULT '',
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                try:
                    # Legacy migration: keep existing maintenance settings if old columns exist.
                    conn.execute(
                        """
                        INSERT INTO app_maintenance_config (
                          platform, maintenance_notice_enabled, maintenance_notice_title, maintenance_notice_message, updated_at
                        )
                        SELECT
                          platform,
                          COALESCE(maintenance_notice_enabled, FALSE),
                          COALESCE(maintenance_notice_title, ''),
                          COALESCE(maintenance_notice_message, ''),
                          COALESCE(updated_at, NOW())
                        FROM app_version_config
                        ON CONFLICT (platform) DO NOTHING
                        """
                    )
                except Exception:
                    pass
                conn.execute(
                    """
                    INSERT INTO app_maintenance_config (platform)
                    VALUES ('android'), ('ios')
                    ON CONFLICT (platform) DO NOTHING
                    """
                )
    except Exception:
        return


def _recover_stale_running_import_jobs() -> None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, release_id
                        FROM cadastral_import_job
                        WHERE status = 'RUNNING'
                        """
                    )
                    rows = cur.fetchall()
                    if not rows:
                        return
                    job_ids = [int(row[0]) for row in rows]
                    release_ids = sorted({int(row[1]) for row in rows if row[1] is not None})

                    cur.execute(
                        """
                        UPDATE cadastral_import_job
                        SET status = 'FAILED',
                            error_message = CASE
                                WHEN COALESCE(error_message, '') = '' THEN 'server restarted during import'
                                ELSE error_message
                            END,
                            finished_at = NOW(),
                            updated_at = NOW()
                        WHERE id = ANY(%s)
                        """,
                        (job_ids,),
                    )
                    cur.execute(
                        """
                        UPDATE cadastral_import_job_worker
                        SET status = 'FAILED',
                            error_message = CASE
                                WHEN COALESCE(error_message, '') = '' THEN 'server restarted during import'
                                ELSE error_message
                            END,
                            finished_at = COALESCE(finished_at, NOW()),
                            updated_at = NOW()
                        WHERE job_id = ANY(%s)
                          AND status IN ('QUEUED', 'RUNNING')
                        """,
                        (job_ids,),
                    )

                    if release_ids:
                        cur.execute(
                            """
                            UPDATE cadastral_release
                            SET status = CASE WHEN status = 'IMPORTING' THEN 'FAILED' ELSE status END,
                                updated_at = NOW()
                            WHERE id = ANY(%s)
                            """,
                            (release_ids,),
                        )
    except Exception:
        return


_ensure_import_worker_progress_table()
_ensure_dataset_import_file_table()
_ensure_app_version_config_table()
_ensure_app_maintenance_config_table()
_recover_stale_running_import_jobs()


def _upload_base_dir() -> Path:
    return Path(os.getenv("CADASTRAL_UPLOAD_BASE_DIR", "/data/uploads/admin")).resolve()


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "y", "yes", "on"}:
        return True
    if text in {"0", "false", "f", "n", "no", "off", ""}:
        return False
    return default


def _normalize_operation_mode(value: str | None, default: str = "full") -> str:
    mode = str(value or default).strip().lower()
    if not mode:
        mode = default
    if mode not in VALID_OPERATION_MODES:
        raise HTTPException(status_code=400, detail=f"invalid operation_mode: {value}")
    return mode


def _normalize_data_type(value: str | None, default: str = "cadastral") -> str:
    normalized = (value or default).strip().lower().replace("-", "_")
    if not normalized:
        normalized = default
    if not _DATA_TYPE_PATTERN.fullmatch(normalized):
        raise HTTPException(status_code=400, detail=f"invalid data_type: {value}")
    return normalized


def _land_info_component_code_for_data_type(data_type: str | None) -> str | None:
    normalized = str(data_type or "").strip().lower().replace("-", "_")
    return LAND_INFO_DATA_TYPE_TO_CODE.get(normalized)


def _land_info_component_data_type_for_code(dataset_code: Any) -> str | None:
    component = LAND_INFO_COMPONENTS.get(str(dataset_code or "").strip().upper())
    return component["data_type"] if component else None


def _is_land_info_component_data_type(data_type: str | None) -> bool:
    normalized = str(data_type or "").strip().lower().replace("-", "_")
    return normalized in LAND_INFO_COMPONENT_DATA_TYPES


def _is_land_info_family_data_type(data_type: str | None) -> bool:
    normalized = str(data_type or "").strip().lower().replace("-", "_")
    return normalized == "land_info" or normalized in LAND_INFO_COMPONENT_DATA_TYPES


def _data_type_env_suffix(data_type: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", data_type).upper()


def _default_import_pattern_for_data_type(data_type: str) -> str:
    data_type = _normalize_data_type(data_type)
    env_key = f"IMPORT_PATTERN_{_data_type_env_suffix(data_type)}"
    configured = os.getenv(env_key, "").strip()
    if configured:
        return configured
    defaults = {
        "cadastral": "AL_D002*.json",
        "building_info": "*.txt",
        "building_integrated_info": "AL_D010*.json",
        "land_info": "AL_D1*.csv",
    }
    component_code = _land_info_component_code_for_data_type(data_type)
    if component_code:
        return f"{component_code}*.csv"
    return defaults.get(data_type, "*")


def _default_batch_size_for_data_type(data_type: str, operation_mode: str = "full") -> int:
    normalized = _normalize_data_type(data_type)
    mode = _normalize_operation_mode(operation_mode)
    suffix = _data_type_env_suffix(normalized)
    env_keys = [
        f"IMPORT_BATCH_SIZE_{suffix}_{mode.upper()}",
        f"IMPORT_BATCH_SIZE_{suffix}",
        "IMPORT_BATCH_SIZE_DEFAULT",
    ]
    for key in env_keys:
        raw = os.getenv(key, "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
            if value >= 100:
                return value
        except Exception:
            continue

    if normalized == "building_info":
        return 12000 if mode == "full" else 4000
    return 2000


def _import_script_path_for_data_type(data_type: str) -> str:
    data_type = _normalize_data_type(data_type)
    env_key = f"IMPORT_SCRIPT_{_data_type_env_suffix(data_type)}"
    configured = os.getenv(env_key, "").strip()
    if configured:
        return configured

    defaults = {
        "cadastral": os.getenv("CADASTRAL_IMPORT_SCRIPT_PATH", "/scripts/import_cadastral_geojson.py"),
        "building_info": os.getenv("BUILDING_INFO_IMPORT_SCRIPT_PATH", "/scripts/import_building_info_text.py"),
        "building_integrated_info": os.getenv(
            "BUILDING_INTEGRATED_INFO_IMPORT_SCRIPT_PATH",
            "/scripts/import_building_integrated_geojson.py",
        ),
        "land_info": os.getenv("LAND_INFO_IMPORT_SCRIPT_PATH", "/scripts/import_land_info_csv.py"),
    }
    if _is_land_info_component_data_type(data_type):
        return os.getenv("LAND_INFO_IMPORT_SCRIPT_PATH", "/scripts/import_land_info_csv.py")
    if data_type in defaults:
        return defaults[data_type]

    convention_path = f"/scripts/import_{data_type}.py"
    if Path(convention_path).exists():
        return convention_path
    return os.getenv("DEFAULT_IMPORT_SCRIPT_PATH", "/scripts/import_generic_files.py")


def _choose_import_pattern(upload_dir: Path, data_type: str) -> str:
    data_type = _normalize_data_type(data_type)
    names = [p.name for p in upload_dir.iterdir() if p.is_file()]
    default_pattern = _default_import_pattern_for_data_type(data_type)
    if _count_pattern_files(upload_dir, default_pattern) > 0:
        return default_pattern

    if any(name.upper().startswith("AL_D002") and name.lower().endswith(".json") for name in names):
        return "AL_D002*.json"
    if any(name.lower().endswith(".txt") for name in names):
        return "*.txt"
    if any(name.lower().endswith(".json") for name in names):
        return "*.json"
    if any(name.lower().endswith(".geojson") for name in names):
        return "*.geojson"
    if any(name.lower().endswith(".csv") for name in names):
        return "*.csv"

    raise HTTPException(status_code=400, detail=f"업로드된 파일에서 적재 가능한 파일을 찾을 수 없습니다: data_type={data_type}")


def _count_pattern_files(upload_dir: Path, pattern: str) -> int:
    lower_pattern = pattern.lower()
    count = 0
    for path in upload_dir.iterdir():
        if not path.is_file():
            continue
        name = path.name
        if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(name.lower(), lower_pattern):
            count += 1
    return count


def _candidate_import_patterns_for_data_type(data_type: str) -> list[str]:
    normalized = _normalize_data_type(data_type)
    default_pattern = _default_import_pattern_for_data_type(normalized)
    candidates: list[str] = [default_pattern]

    typed_patterns = {
        "cadastral": ["AL_D002*.json", "*.json", "*.geojson"],
        "building_info": ["*.txt"],
        "building_integrated_info": ["AL_D010*.json", "*.json", "*.geojson"],
        "land_info": ["AL_D1*.csv", "CH_D1*.csv", "*.csv"],
    }
    candidates.extend(typed_patterns.get(normalized, []))
    component_code = _land_info_component_code_for_data_type(normalized)
    if component_code:
        candidates.extend([f"{component_code}*.csv", "*.csv"])

    unique: list[str] = []
    seen: set[str] = set()
    for pattern in candidates:
        normalized_pattern = str(pattern or "").strip()
        if not normalized_pattern or normalized_pattern in seen:
            continue
        seen.add(normalized_pattern)
        unique.append(normalized_pattern)
    return unique


def _detect_import_pattern(upload_dir: Path, data_type: str) -> str | None:
    normalized = _normalize_data_type(data_type)
    for pattern in _candidate_import_patterns_for_data_type(normalized):
        if _count_pattern_files(upload_dir, pattern) > 0:
            return pattern

    if normalized in {"cadastral", "building_info", "building_integrated_info", "land_info"} or _is_land_info_component_data_type(normalized):
        return None

    try:
        return _choose_import_pattern(upload_dir, normalized)
    except HTTPException:
        return None


def _default_source_dir_for_data_type(data_type: str, operation_mode: str = "full") -> Path:
    normalized = _normalize_data_type(data_type)
    mode = _normalize_operation_mode(operation_mode)
    mode_env_key = f"SOURCE_DIR_{_data_type_env_suffix(normalized)}_{mode.upper()}"
    mode_configured = os.getenv(mode_env_key, "").strip()
    if mode_configured:
        return Path(mode_configured).resolve()

    env_key = f"SOURCE_DIR_{_data_type_env_suffix(normalized)}"
    configured = os.getenv(env_key, "").strip()
    if configured:
        return Path(configured).resolve()

    defaults = {
        "cadastral": f"/data/uploads/연속지적/{mode}",
        "building_info": f"/data/source/building_info/{mode}",
        "building_integrated_info": f"/data/source/building_integrated_info/{mode}",
        "land_info": f"/data/source/land_info/{mode}",
    }
    component_code = _land_info_component_code_for_data_type(normalized)
    if component_code:
        return Path(f"/data/source/land_info/{component_code}/{mode}").resolve()
    return Path(defaults.get(normalized, f"/data/source/{normalized}/{mode}")).resolve()


def _import_browse_roots_for_data_type(data_type: str, operation_mode: str = "full") -> list[Path]:
    normalized = _normalize_data_type(data_type)
    mode = _normalize_operation_mode(operation_mode)
    configured_roots = os.getenv("ADMIN_IMPORT_BROWSE_ROOTS", "").strip()
    roots: list[Path] = []

    if configured_roots:
        for raw in configured_roots.split(","):
            text = raw.strip()
            if text:
                roots.append(Path(text).resolve())
    else:
        if normalized == "cadastral":
            roots.append(Path("/data/uploads").resolve())
        else:
            roots.append(Path("/data/source").resolve())

    default_dir = _default_source_dir_for_data_type(normalized, mode)
    roots.extend([default_dir, default_dir.parent])

    unique: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def _scan_import_path_options(data_type: str, operation_mode: str, max_depth: int, limit: int) -> Dict[str, Any]:
    normalized = _normalize_data_type(data_type)
    mode = _normalize_operation_mode(operation_mode)
    pattern = _default_import_pattern_for_data_type(normalized)
    default_dir = _default_source_dir_for_data_type(normalized, mode)
    roots = _import_browse_roots_for_data_type(normalized, mode)
    options: list[dict[str, Any]] = []
    seen_dirs: set[str] = set()

    def _scan_dir(directory: Path) -> tuple[int, int, str | None, list[Path]]:
        file_count = 0
        subdirs: list[Path] = []
        for child in directory.iterdir():
            try:
                if child.is_file():
                    file_count += 1
                    continue
                if child.is_dir() and not child.is_symlink():
                    subdirs.append(child)
            except Exception:
                continue
        subdirs.sort(key=lambda item: item.name.lower())
        detected_pattern = _detect_import_pattern(directory, normalized) if file_count > 0 else None
        matched_files = _count_pattern_files(directory, detected_pattern) if detected_pattern else 0
        return file_count, matched_files, detected_pattern, subdirs

    for root in roots:
        if len(options) >= limit:
            break
        root_resolved = root.resolve()
        if not root_resolved.exists() or not root_resolved.is_dir():
            continue

        queue: deque[tuple[Path, int]] = deque([(root_resolved, 0)])
        while queue and len(options) < limit:
            current, depth = queue.popleft()
            current_resolved = current.resolve()
            key = str(current_resolved)
            if key in seen_dirs:
                continue
            seen_dirs.add(key)

            try:
                file_count, matched_files, detected_pattern, subdirs = _scan_dir(current_resolved)
            except Exception:
                continue

            is_root = current_resolved == root_resolved
            is_default = current_resolved == default_dir
            importable = bool(detected_pattern and matched_files > 0)
            if importable or is_root or is_default:
                options.append(
                    {
                        "path": str(current_resolved),
                        "name": current_resolved.name or str(current_resolved),
                        "exists": True,
                        "file_count": file_count,
                        "matched_files": matched_files,
                        "detected_pattern": detected_pattern,
                        "importable": importable,
                        "is_default": is_default,
                        "is_root": is_root,
                        "depth": depth,
                    }
                )

            if depth >= max_depth:
                continue

            for subdir in subdirs:
                sub_resolved = subdir.resolve()
                if str(sub_resolved) in seen_dirs:
                    continue
                queue.append((sub_resolved, depth + 1))

    default_key = str(default_dir)
    if default_key not in seen_dirs:
        options.append(
            {
                "path": default_key,
                "name": default_dir.name or default_key,
                "exists": default_dir.exists() and default_dir.is_dir(),
                "file_count": 0,
                "matched_files": 0,
                "detected_pattern": None,
                "importable": False,
                "is_default": True,
                "is_root": False,
                "depth": 0,
            }
        )

    options.sort(
        key=lambda item: (
            0 if item["is_default"] else 1,
            0 if item["matched_files"] > 0 else 1,
            -int(item["matched_files"]),
            -int(item["file_count"]),
            item["path"],
        )
    )
    options = options[:limit]

    recommended_source_path = default_key
    if not default_dir.exists():
        for item in options:
            if item.get("matched_files", 0) > 0:
                recommended_source_path = str(item["path"])
                break

    return {
        "data_type": normalized,
        "operation_mode": mode,
        "pattern": pattern,
        "default_source_path": default_key,
        "recommended_source_path": recommended_source_path,
        "roots": [str(root) for root in roots],
        "options": options,
    }


def _next_auto_release_version() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _require_admin(x_admin_token: str | None, admin_token: str | None = None) -> None:
    configured_token = os.getenv("ADMIN_TOKEN", "").strip()
    if not configured_token:
        return

    provided_token = (x_admin_token or "").strip() or (admin_token or "").strip()
    if provided_token != configured_token:
        raise HTTPException(status_code=403, detail="admin token is invalid")


def _normalize_app_platform(platform: str) -> str:
    normalized = str(platform or "").strip().lower()
    if normalized not in APP_CONFIG_PLATFORMS:
        raise HTTPException(status_code=400, detail=f"invalid platform: {platform}")
    return normalized


def _to_non_negative_int(value: Any, field_name: str) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError
        normalized = int(str(value).strip())
    except Exception:
        raise HTTPException(status_code=400, detail=f"{field_name} must be a non-negative integer")
    if normalized < 0:
        raise HTTPException(status_code=400, detail=f"{field_name} must be a non-negative integer")
    return normalized


def _default_app_config(platform: str) -> Dict[str, Any]:
    normalized = _normalize_app_platform(platform)
    return {
        "platform": normalized,
        "min_required_version": 0,
        "latest_version": 0,
        "force_update": False,
        "title": "",
        "message": "",
        "updated_at": None,
    }


def _app_config_etag(config: Dict[str, Any]) -> str:
    payload = {
        "platform": str(config.get("platform") or ""),
        "min_required_version": int(config.get("min_required_version") or 0),
        "latest_version": int(config.get("latest_version") or 0),
        "force_update": bool(config.get("force_update")),
        "title": str(config.get("title") or ""),
        "message": str(config.get("message") or ""),
        "updated_at": str(config.get("updated_at") or ""),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()[:24]
    return f'W/"app-config-{digest}"'


def _fetch_app_config(platform: str) -> Dict[str, Any]:
    normalized = _normalize_app_platform(platform)
    fallback = _default_app_config(normalized)
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT min_required_version, latest_version, force_update, title, message, updated_at
                    FROM app_version_config
                    WHERE platform = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
    except Exception:
        return fallback

    if not row:
        return fallback

    min_required = row[0]
    latest = row[1]
    force_update = row[2]
    title = row[3]
    message = row[4]
    updated_at = row[5]
    return {
        "platform": normalized,
        "min_required_version": int(min_required or 0),
        "latest_version": int(latest or 0),
        "force_update": bool(force_update),
        "title": str(title or ""),
        "message": str(message or ""),
        "updated_at": updated_at,
    }


def _update_app_config(platform: str, body: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_app_platform(platform)
    current = _fetch_app_config(normalized)
    min_required = current["min_required_version"]
    latest = current["latest_version"]
    force_update = current["force_update"]
    title = current["title"]
    message = current["message"]

    if "min_required_version" in body:
        min_required = _to_non_negative_int(body.get("min_required_version"), "min_required_version")
    if "latest_version" in body:
        latest = _to_non_negative_int(body.get("latest_version"), "latest_version")
    if "force_update" in body:
        force_update = _to_bool(body.get("force_update"), default=False)
    if "title" in body:
        title = str(body.get("title", "")).strip()
    if "message" in body:
        message = str(body.get("message", "")).strip()

    if latest < min_required:
        raise HTTPException(
            status_code=400,
            detail="latest_version must be greater than or equal to min_required_version",
        )

    if len(title) > 100:
        raise HTTPException(status_code=400, detail="title must be 100 characters or fewer")

    if len(message) > 500:
        raise HTTPException(status_code=400, detail="message must be 500 characters or fewer")

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO app_version_config (
                      platform, min_required_version, latest_version, force_update, title, message, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (platform) DO UPDATE
                    SET min_required_version = EXCLUDED.min_required_version,
                        latest_version = EXCLUDED.latest_version,
                        force_update = EXCLUDED.force_update,
                        title = EXCLUDED.title,
                        message = EXCLUDED.message,
                        updated_at = NOW()
                    """,
                    (
                        normalized,
                        min_required,
                        latest,
                        force_update,
                        title,
                        message,
                    ),
                )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to update app config: {exc}")

    return _fetch_app_config(normalized)


def _default_maintenance_config(platform: str) -> Dict[str, Any]:
    normalized = _normalize_app_platform(platform)
    return {
        "platform": normalized,
        "maintenance_notice_enabled": False,
        "maintenance_notice_title": "",
        "maintenance_notice_message": "",
        "updated_at": None,
    }


def _maintenance_config_etag(config: Dict[str, Any]) -> str:
    payload = {
        "platform": str(config.get("platform") or ""),
        "maintenance_notice_enabled": bool(config.get("maintenance_notice_enabled")),
        "maintenance_notice_title": str(config.get("maintenance_notice_title") or ""),
        "maintenance_notice_message": str(config.get("maintenance_notice_message") or ""),
        "updated_at": str(config.get("updated_at") or ""),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()[:24]
    return f'W/"maintenance-config-{digest}"'


def _fetch_maintenance_config(platform: str) -> Dict[str, Any]:
    normalized = _normalize_app_platform(platform)
    fallback = _default_maintenance_config(normalized)
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT maintenance_notice_enabled, maintenance_notice_title, maintenance_notice_message, updated_at
                    FROM app_maintenance_config
                    WHERE platform = %s
                    LIMIT 1
                    """,
                    (normalized,),
                )
                row = cur.fetchone()
    except Exception:
        return fallback

    if not row:
        return fallback

    maintenance_notice_enabled = row[0]
    maintenance_notice_title = row[1]
    maintenance_notice_message = row[2]
    updated_at = row[3]
    return {
        "platform": normalized,
        "maintenance_notice_enabled": bool(maintenance_notice_enabled),
        "maintenance_notice_title": str(maintenance_notice_title or ""),
        "maintenance_notice_message": str(maintenance_notice_message or ""),
        "updated_at": updated_at,
    }


def _update_maintenance_config(platform: str, body: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_app_platform(platform)
    current = _fetch_maintenance_config(normalized)
    maintenance_notice_enabled = current["maintenance_notice_enabled"]
    maintenance_notice_title = current["maintenance_notice_title"]
    maintenance_notice_message = current["maintenance_notice_message"]

    if "maintenance_notice_enabled" in body:
        maintenance_notice_enabled = _to_bool(body.get("maintenance_notice_enabled"), default=False)
    if "maintenance_notice_title" in body:
        maintenance_notice_title = str(body.get("maintenance_notice_title", "")).strip()
    if "maintenance_notice_message" in body:
        maintenance_notice_message = str(body.get("maintenance_notice_message", "")).strip()

    if len(maintenance_notice_title) > 100:
        raise HTTPException(
            status_code=400,
            detail="maintenance_notice_title must be 100 characters or fewer",
        )
    if len(maintenance_notice_message) > 500:
        raise HTTPException(
            status_code=400,
            detail="maintenance_notice_message must be 500 characters or fewer",
        )

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO app_maintenance_config (
                      platform, maintenance_notice_enabled, maintenance_notice_title, maintenance_notice_message, updated_at
                    )
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (platform) DO UPDATE
                    SET maintenance_notice_enabled = EXCLUDED.maintenance_notice_enabled,
                        maintenance_notice_title = EXCLUDED.maintenance_notice_title,
                        maintenance_notice_message = EXCLUDED.maintenance_notice_message,
                        updated_at = NOW()
                    """,
                    (
                        normalized,
                        maintenance_notice_enabled,
                        maintenance_notice_title,
                        maintenance_notice_message,
                    ),
                )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to update maintenance config: {exc}")

    return _fetch_maintenance_config(normalized)


def _query_active_release_uncached(data_type: str = "cadastral") -> dict[str, Any] | None:
    normalized_type = _normalize_data_type(data_type)
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        """
                        SELECT id, version, status, activated_at, data_type
                        FROM cadastral_release
                        WHERE is_active = TRUE
                          AND data_type = %s
                        ORDER BY activated_at DESC NULLS LAST, id DESC
                        LIMIT 1
                        """,
                        (normalized_type,),
                    )
                    row = cur.fetchone()
                except Exception:
                    cur.execute(
                        """
                        SELECT id, version, status, activated_at
                        FROM cadastral_release
                        WHERE is_active = TRUE
                        ORDER BY activated_at DESC NULLS LAST, id DESC
                        LIMIT 1
                        """
                    )
                    row = cur.fetchone()
    except Exception:
        return None

    if not row:
        return None

    return {
        "id": row[0],
        "version": row[1],
        "status": row[2],
        "activated_at": row[3],
        "data_type": row[4] if len(row) > 4 else "cadastral",
    }


def _clear_active_release_cache(data_type: str | None = None) -> None:
    with _ACTIVE_RELEASE_CACHE_LOCK:
        if data_type is None:
            _ACTIVE_RELEASE_CACHE["loaded_at_by_type"] = {}
            _ACTIVE_RELEASE_CACHE["release_by_type"] = {}
            return

        normalized_type = _normalize_data_type(data_type)
        _ACTIVE_RELEASE_CACHE["loaded_at_by_type"].pop(normalized_type, None)
        _ACTIVE_RELEASE_CACHE["release_by_type"].pop(normalized_type, None)
        if normalized_type == "land_info" or normalized_type in LAND_INFO_COMPONENT_DATA_TYPES:
            _ACTIVE_RELEASE_CACHE["loaded_at_by_type"].pop("land_info_components", None)
            _ACTIVE_RELEASE_CACHE["release_by_type"].pop("land_info_components", None)


def _active_release(data_type: str = "cadastral", force_refresh: bool = False) -> dict[str, Any] | None:
    normalized_type = _normalize_data_type(data_type)
    now = time.time()

    with _ACTIVE_RELEASE_CACHE_LOCK:
        loaded_at_by_type: dict[str, float] = _ACTIVE_RELEASE_CACHE["loaded_at_by_type"]
        release_by_type: dict[str, Any] = _ACTIVE_RELEASE_CACHE["release_by_type"]
        loaded_at = loaded_at_by_type.get(normalized_type, 0.0)
        if not force_refresh and (now - loaded_at) < _ACTIVE_RELEASE_CACHE_TTL:
            return release_by_type.get(normalized_type)

    release = _query_active_release_uncached(normalized_type)
    with _ACTIVE_RELEASE_CACHE_LOCK:
        _ACTIVE_RELEASE_CACHE["loaded_at_by_type"][normalized_type] = now
        _ACTIVE_RELEASE_CACHE["release_by_type"][normalized_type] = release

    return release


def _query_active_land_info_component_releases_uncached() -> dict[str, dict[str, Any]]:
    component_types = sorted(LAND_INFO_COMPONENT_DATA_TYPES)
    if not component_types:
        return {}
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, version, status, activated_at, data_type, metadata, records_count
                    FROM cadastral_release
                    WHERE is_active = TRUE
                      AND data_type = ANY(%s)
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    """,
                    (component_types,),
                )
                rows = cur.fetchall()
    except Exception:
        return {}

    by_code: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _land_info_component_code_for_data_type(str(row[4] or ""))
        if not code or code in by_code:
            continue
        metadata = row[5] if isinstance(row[5], dict) else {}
        by_code[code] = {
            "id": row[0],
            "version": row[1],
            "status": row[2],
            "activated_at": row[3],
            "data_type": row[4],
            "metadata": metadata,
            "records_count": int(row[6] or 0),
            "dataset_code": code,
            "dataset_name": LAND_INFO_COMPONENTS.get(code, {}).get("name"),
        }
    return by_code


def _active_land_info_component_releases(force_refresh: bool = False) -> dict[str, dict[str, Any]]:
    cache_key = "land_info_components"
    now = time.time()
    with _ACTIVE_RELEASE_CACHE_LOCK:
        loaded_at_by_type: dict[str, float] = _ACTIVE_RELEASE_CACHE["loaded_at_by_type"]
        release_by_type: dict[str, Any] = _ACTIVE_RELEASE_CACHE["release_by_type"]
        loaded_at = loaded_at_by_type.get(cache_key, 0.0)
        if not force_refresh and (now - loaded_at) < _ACTIVE_RELEASE_CACHE_TTL:
            cached = release_by_type.get(cache_key)
            return cached if isinstance(cached, dict) else {}

    releases = _query_active_land_info_component_releases_uncached()
    with _ACTIVE_RELEASE_CACHE_LOCK:
        _ACTIVE_RELEASE_CACHE["loaded_at_by_type"][cache_key] = now
        _ACTIVE_RELEASE_CACHE["release_by_type"][cache_key] = releases
    return releases


def _tile_memory_cache() -> LruBytesCache:
    if not hasattr(_tile_memory_cache, "instance"):
        max_items = int(os.getenv("CADASTRAL_TILE_MEMORY_CACHE_SIZE", "512"))
        _tile_memory_cache.instance = LruBytesCache(max_items=max_items)
    return _tile_memory_cache.instance  # type: ignore[attr-defined]


def _tile_size() -> int:
    return int(os.getenv("CADASTRAL_TILE_SIZE", "256"))


def _tile_supersample() -> int:
    return max(1, int(os.getenv("CADASTRAL_TILE_SUPERSAMPLE", "2")))


def _tile_stroke_width() -> float:
    return max(0.5, float(os.getenv("CADASTRAL_TILE_STROKE_WIDTH", "1.0")))


def _tile_stroke_alpha() -> int:
    value = int(os.getenv("CADASTRAL_TILE_STROKE_ALPHA", "150"))
    return max(0, min(255, value))


def _tile_label_enabled() -> bool:
    return os.getenv("CADASTRAL_TILE_LABEL_ENABLED", "true").lower() in {"1", "true", "yes", "on"}


def _tile_label_min_zoom() -> int:
    return int(os.getenv("CADASTRAL_TILE_LABEL_MIN_ZOOM", "17"))


def _tile_label_min_box() -> float:
    return float(os.getenv("CADASTRAL_TILE_LABEL_MIN_BOX", "14.0"))


def _tile_label_min_font_px() -> float:
    return float(os.getenv("CADASTRAL_TILE_LABEL_MIN_FONT_PX", "11.0"))


def _tile_label_max_font_px() -> float:
    return float(os.getenv("CADASTRAL_TILE_LABEL_MAX_FONT_PX", "44.0"))


def _tile_label_stroke_width() -> float:
    return float(os.getenv("CADASTRAL_TILE_LABEL_STROKE_WIDTH", "1.6"))


def _tile_label_zoom_scale(z: int) -> float:
    min_zoom = _tile_min_zoom()
    max_zoom = _tile_max_zoom()
    min_scale = float(os.getenv("CADASTRAL_TILE_LABEL_MIN_ZOOM_SCALE", "0.45"))
    max_scale = float(os.getenv("CADASTRAL_TILE_LABEL_MAX_ZOOM_SCALE", "1.0"))

    min_scale = max(0.2, min(1.0, min_scale))
    max_scale = max(min_scale, min(2.0, max_scale))
    if max_zoom <= min_zoom:
        return max_scale

    t = (z - min_zoom) / float(max_zoom - min_zoom)
    t = max(0.0, min(1.0, t))
    return min_scale + (max_scale - min_scale) * t


def _tile_label_font_path() -> str:
    return os.getenv("CADASTRAL_TILE_LABEL_FONT_PATH", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")


def _tile_downsample_filter() -> str:
    return os.getenv("CADASTRAL_TILE_DOWNSAMPLE_FILTER", "lanczos").strip().lower()


def _tile_unsharp_radius() -> float:
    return max(0.0, float(os.getenv("CADASTRAL_TILE_UNSHARP_RADIUS", "0.0")))


def _tile_unsharp_percent() -> int:
    return max(0, int(os.getenv("CADASTRAL_TILE_UNSHARP_PERCENT", "0")))


def _tile_unsharp_threshold() -> int:
    return max(0, int(os.getenv("CADASTRAL_TILE_UNSHARP_THRESHOLD", "0")))


def _tile_prefilter_blur_radius() -> float:
    return max(0.0, float(os.getenv("CADASTRAL_TILE_PREFILTER_BLUR_RADIUS", "0.0")))


def _tile_render_rev() -> str:
    return os.getenv("CADASTRAL_TILE_RENDER_REV", "r2")


def _tile_min_zoom() -> int:
    return int(os.getenv("CADASTRAL_TILE_MIN_ZOOM", "17"))


def _tile_max_zoom() -> int:
    return int(os.getenv("CADASTRAL_TILE_MAX_ZOOM", "21"))


def _tile_version() -> str:
    release = _active_release("cadastral")
    if release and release.get("version"):
        return str(release["version"])
    return os.getenv("CADASTRAL_TILE_VERSION", "v1")


def _tile_cache_root(version: str | None = None) -> Path:
    root = Path(os.getenv("TILE_CACHE_DIR", "/data/uploads/tile_cache"))
    return root / "cadastral" / (version or _tile_version()) / _tile_render_rev()


def _tile_path(z: int, x: int, y: int, version: str | None = None) -> Path:
    return _tile_cache_root(version=version) / str(z) / str(x) / f"{y}.png"


def _tile_change_hint_file_path(job_id: int) -> Path:
    base_dir = Path(os.getenv("CADASTRAL_TILE_CHANGE_HINT_DIR", "/tmp"))
    return base_dir / f"cadastral_tile_changes_job_{int(job_id)}.json"


def _to_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _normalize_bbox(
    min_lon: Any,
    max_lon: Any,
    min_lat: Any,
    max_lat: Any,
) -> tuple[float, float, float, float] | None:
    lon0 = _to_float(min_lon)
    lon1 = _to_float(max_lon)
    lat0 = _to_float(min_lat)
    lat1 = _to_float(max_lat)
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


def _lon_to_tile_x_index(lon: float, z: int) -> int:
    n = 1 << z
    raw = ((lon + 180.0) / 360.0) * n
    return max(0, min(n - 1, int(math.floor(raw))))


def _lat_to_tile_y_index(lat: float, z: int) -> int:
    n = 1 << z
    clamped = max(min(lat, 85.05112878), -85.05112878)
    lat_rad = math.radians(clamped)
    merc = math.log(math.tan(math.pi / 4.0 + lat_rad / 2.0))
    raw = (1.0 - merc / math.pi) * 0.5 * n
    return max(0, min(n - 1, int(math.floor(raw))))


def _tile_range_for_bbox(
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
    z: int,
) -> tuple[int, int, int, int]:
    x0 = _lon_to_tile_x_index(min_lon, z)
    x1 = _lon_to_tile_x_index(max_lon, z)
    y0 = _lat_to_tile_y_index(max_lat, z)
    y1 = _lat_to_tile_y_index(min_lat, z)
    if x0 > x1:
        x0, x1 = x1, x0
    if y0 > y1:
        y0, y1 = y1, y0
    return x0, x1, y0, y1


def _release_version_by_id(release_id: int) -> str | None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT version
                    FROM cadastral_release
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (int(release_id),),
                )
                row = cur.fetchone()
    except Exception:
        return None
    if not row or row[0] is None:
        return None
    return str(row[0])


def _tile_public_base_url() -> str:
    return os.getenv("CADASTRAL_TILE_PUBLIC_BASE_URL", "").strip().rstrip("/")


def _cloudflare_tile_purge_enabled() -> bool:
    explicit = os.getenv("CLOUDFLARE_TILE_PURGE_ENABLED", "").strip()
    if explicit:
        return _to_bool(explicit, False)
    return (
        bool(os.getenv("CLOUDFLARE_API_TOKEN", "").strip())
        and bool(os.getenv("CLOUDFLARE_ZONE_ID", "").strip())
        and bool(_tile_public_base_url())
    )


def _cloudflare_api_base_url() -> str:
    return os.getenv("CLOUDFLARE_API_BASE_URL", "https://api.cloudflare.com/client/v4").strip().rstrip("/")


def _cloudflare_tile_purge_batch_size() -> int:
    try:
        value = int(os.getenv("CLOUDFLARE_TILE_PURGE_BATCH_SIZE", "30"))
    except Exception:
        value = 30
    return max(1, min(100, value))


def _cloudflare_tile_purge_timeout_sec() -> float:
    try:
        value = float(os.getenv("CLOUDFLARE_TILE_PURGE_TIMEOUT_SEC", "10"))
    except Exception:
        value = 10.0
    return max(3.0, min(60.0, value))


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _build_tile_urls_for_cdn_purge(version: str, tile_keys: set[tuple[int, int, int]]) -> list[str]:
    base_url = _tile_public_base_url()
    if not base_url or not tile_keys:
        return []
    encoded_version = urllib.parse.quote(version, safe="")
    return [
        f"{base_url}/v1/tiles/cadastral/{z}/{x}/{y}.png?v={encoded_version}"
        for z, x, y in sorted(tile_keys)
    ]


def _purge_cloudflare_tile_urls(urls: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "enabled": False,
        "attempted": False,
        "requested_urls": len(urls),
        "batch_size": _cloudflare_tile_purge_batch_size(),
        "total_batches": 0,
        "succeeded_batches": 0,
        "failed_batches": 0,
        "error": None,
    }
    if not urls:
        return result

    if not _cloudflare_tile_purge_enabled():
        result["error"] = "disabled or missing cloudflare config"
        return result

    api_token = os.getenv("CLOUDFLARE_API_TOKEN", "").strip()
    zone_id = os.getenv("CLOUDFLARE_ZONE_ID", "").strip()
    if not api_token or not zone_id:
        result["error"] = "missing CLOUDFLARE_API_TOKEN or CLOUDFLARE_ZONE_ID"
        return result

    endpoint = f"{_cloudflare_api_base_url()}/zones/{zone_id}/purge_cache"
    timeout_sec = _cloudflare_tile_purge_timeout_sec()
    batch_size = _cloudflare_tile_purge_batch_size()
    batches = _chunked(urls, batch_size)
    result["enabled"] = True
    result["attempted"] = True
    result["total_batches"] = len(batches)

    first_error: str | None = None
    for batch in batches:
        payload = json.dumps({"files": batch}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(endpoint, data=payload, method="POST")
        req.add_header("Authorization", f"Bearer {api_token}")
        req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", "ignore")
                status = int(getattr(resp, "status", 0) or 0)
            parsed = json.loads(raw) if raw else {}
            success = status in {200, 201, 202} and isinstance(parsed, dict) and bool(parsed.get("success"))
            if success:
                result["succeeded_batches"] = int(result["succeeded_batches"]) + 1
                continue

            result["failed_batches"] = int(result["failed_batches"]) + 1
            if first_error is None:
                errors = parsed.get("errors") if isinstance(parsed, dict) else None
                first_error = f"status={status}, errors={errors!r}"
        except urllib.error.HTTPError as exc:
            result["failed_batches"] = int(result["failed_batches"]) + 1
            if first_error is None:
                err_text = ""
                with contextlib.suppress(Exception):
                    err_text = exc.read().decode("utf-8", "ignore")
                first_error = f"http_error={exc.code}, body={err_text[:500]}"
        except Exception as exc:
            result["failed_batches"] = int(result["failed_batches"]) + 1
            if first_error is None:
                first_error = str(exc)[:500]

    result["error"] = first_error
    return result


def _invalidate_cadastral_tiles_from_hint(
    *,
    job_id: int,
    release_id: int,
    version: str,
    hint_path: Path | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "job_id": int(job_id),
        "release_id": int(release_id),
        "release_version": version,
        "hint_path": str(hint_path) if hint_path is not None else None,
        "hint_found": False,
        "boxes_used": 0,
        "overflow_bbox_count": 0,
        "tile_candidates": 0,
        "memory_removed": 0,
        "disk_removed": 0,
        "fallback_full_version_clear": False,
        "cdn_purge_enabled": _cloudflare_tile_purge_enabled(),
        "cdn_purge_requested_urls": 0,
        "cdn_purge_batches_total": 0,
        "cdn_purge_batches_succeeded": 0,
        "cdn_purge_batches_failed": 0,
        "cdn_purge_error": None,
        "error": None,
    }

    if hint_path is None:
        result["error"] = "hint path is not configured"
        return result

    try:
        if not hint_path.exists():
            result["error"] = "hint file not found"
            return result

        payload = json.loads(hint_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            result["error"] = "invalid hint payload"
            return result

        result["hint_found"] = True
        overflow_bbox_count = int(payload.get("overflow_bbox_count") or 0)
        result["overflow_bbox_count"] = overflow_bbox_count

        boxes: list[tuple[float, float, float, float]] = []
        raw_boxes = payload.get("boxes")
        if isinstance(raw_boxes, list):
            for item in raw_boxes:
                if not isinstance(item, (list, tuple)) or len(item) != 4:
                    continue
                normalized = _normalize_bbox(item[0], item[1], item[2], item[3])
                if normalized is not None:
                    boxes.append(normalized)

        global_bbox = payload.get("global_bbox")
        normalized_global: tuple[float, float, float, float] | None = None
        if isinstance(global_bbox, (list, tuple)) and len(global_bbox) == 4:
            normalized_global = _normalize_bbox(
                global_bbox[0],
                global_bbox[1],
                global_bbox[2],
                global_bbox[3],
            )

        if overflow_bbox_count > 0 and normalized_global is not None:
            boxes.append(normalized_global)
        if not boxes and normalized_global is not None:
            boxes = [normalized_global]

        result["boxes_used"] = len(boxes)
        if not boxes:
            result["error"] = "no usable changed bbox"
            return result

        max_tiles = max(500, int(os.getenv("CADASTRAL_TILE_INVALIDATION_MAX_TILES", "50000")))
        min_zoom = _tile_min_zoom()
        max_zoom = _tile_max_zoom()
        tile_keys: set[tuple[int, int, int]] = set()
        capped = False

        for min_lon, max_lon, min_lat, max_lat in boxes:
            for z in range(min_zoom, max_zoom + 1):
                x0, x1, y0, y1 = _tile_range_for_bbox(min_lon, max_lon, min_lat, max_lat, z)
                for x in range(x0, x1 + 1):
                    for y in range(y0, y1 + 1):
                        tile_keys.add((z, x, y))
                        if len(tile_keys) > max_tiles:
                            capped = True
                            break
                    if capped:
                        break
                if capped:
                    break
            if capped:
                break

        result["tile_candidates"] = len(tile_keys)
        purge_urls = _build_tile_urls_for_cdn_purge(version, tile_keys)
        result["cdn_purge_requested_urls"] = len(purge_urls)

        if capped:
            prefix = f"{version}:{_tile_render_rev()}:"
            result["fallback_full_version_clear"] = True
            result["memory_removed"] = _tile_memory_cache().delete_prefix(prefix)
            cache_root = _tile_cache_root(version=version)
            if cache_root.exists():
                shutil.rmtree(cache_root, ignore_errors=True)
            result["cdn_purge_error"] = "skip cdn purge because tile candidate overflow"
            return result

        memory_cache = _tile_memory_cache()
        memory_removed = 0
        disk_removed = 0
        rev = _tile_render_rev()

        for z, x, y in tile_keys:
            cache_key = f"{version}:{rev}:{z}:{x}:{y}"
            if memory_cache.delete(cache_key):
                memory_removed += 1

        for z, x, y in tile_keys:
            tile_file = _tile_path(z, x, y, version=version)
            if tile_file.exists():
                try:
                    tile_file.unlink()
                    disk_removed += 1
                except Exception:
                    continue

        result["memory_removed"] = memory_removed
        result["disk_removed"] = disk_removed
        purge_result = _purge_cloudflare_tile_urls(purge_urls)
        result["cdn_purge_enabled"] = bool(purge_result.get("enabled"))
        result["cdn_purge_batches_total"] = int(purge_result.get("total_batches") or 0)
        result["cdn_purge_batches_succeeded"] = int(purge_result.get("succeeded_batches") or 0)
        result["cdn_purge_batches_failed"] = int(purge_result.get("failed_batches") or 0)
        result["cdn_purge_error"] = purge_result.get("error")
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result
    finally:
        with contextlib.suppress(Exception):
            if hint_path.exists():
                hint_path.unlink()


def _build_empty_tile(size: int) -> bytes:
    image = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    output = BytesIO()
    image.save(output, format="PNG", optimize=True)
    return output.getvalue()


def _empty_tile_bytes() -> bytes:
    if not hasattr(_empty_tile_bytes, "cache"):
        _empty_tile_bytes.cache = {}  # type: ignore[attr-defined]
    cache = _empty_tile_bytes.cache  # type: ignore[attr-defined]
    size = _tile_size()
    if size not in cache:
        cache[size] = _build_empty_tile(size)
    return cache[size]


def _tile_bounds(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    n = 2 ** z
    west = x / n * 360.0 - 180.0
    east = (x + 1) / n * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return west, south, east, north


def _lon_to_world_x(lon: float, z: int, tile_size: int) -> float:
    return ((lon + 180.0) / 360.0) * (2 ** z) * tile_size


def _lat_to_world_y(lat: float, z: int, tile_size: int) -> float:
    clamped_lat = max(min(lat, 85.05112878), -85.05112878)
    rad = math.radians(clamped_lat)
    merc_y = math.log(math.tan(math.pi / 4 + rad / 2))
    return (1 - merc_y / math.pi) / 2 * (2 ** z) * tile_size


def _lonlat_to_tile_pixel(lon: float, lat: float, z: int, x: int, y: int, tile_size: int) -> tuple[float, float]:
    world_x = _lon_to_world_x(lon, z, tile_size)
    world_y = _lat_to_world_y(lat, z, tile_size)
    origin_x = x * tile_size
    origin_y = y * tile_size
    return world_x - origin_x, world_y - origin_y


def _safe_json_loads(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                return loaded
        except json.JSONDecodeError:
            return None
    return None


def _polygon_surfaces(geometry: dict[str, Any]) -> list[list[list[list[float]]]]:
    g_type = geometry.get("type")
    coords = geometry.get("coordinates")

    surfaces: list[list[list[list[float]]]] = []
    if g_type == "Polygon" and isinstance(coords, list):
        polygon = [ring for ring in coords if isinstance(ring, list)]
        if polygon:
            surfaces.append(polygon)
        return surfaces

    if g_type == "MultiPolygon" and isinstance(coords, list):
        for polygon in coords:
            if isinstance(polygon, list):
                rings = [ring for ring in polygon if isinstance(ring, list)]
                if rings:
                    surfaces.append(rings)
    return surfaces


def _label_text(value: Any, pnu: Any = None) -> str:
    text = str(value or "").strip()
    pnu_text = str(pnu or "").strip()

    is_mountain = len(pnu_text) >= 11 and pnu_text[10] == "2"
    computed_from_pnu = ""
    if len(pnu_text) >= 19 and pnu_text.isdigit():
        main_no = int(pnu_text[11:15])
        sub_no = int(pnu_text[15:19])
        computed_from_pnu = f"{main_no}" if sub_no == 0 else f"{main_no}-{sub_no}"
        if is_mountain:
            computed_from_pnu = f"산{computed_from_pnu}"

    if text:
        match = re.search(r"(산?\d+(?:-\d+)?)", text)
        if match:
            candidate = match.group(1)
            if is_mountain and not candidate.startswith("산"):
                candidate = f"산{candidate}"
            return candidate
        if text.isdigit() or re.match(r"^\d+-\d+$", text):
            return f"산{text}" if is_mountain else text

    if computed_from_pnu:
        return computed_from_pnu
    return ""


def _ring_bbox(points: list[tuple[float, float]]) -> tuple[float, float, float, float] | None:
    if not points:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return min(xs), min(ys), max(xs), max(ys)


def _ring_core_points(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if len(points) >= 2:
        x0, y0 = points[0]
        x1, y1 = points[-1]
        if abs(x0 - x1) < 1e-6 and abs(y0 - y1) < 1e-6:
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

    if abs(area2) < 1e-6:
        xs = [p[0] for p in core]
        ys = [p[1] for p in core]
        return sum(xs) / n, sum(ys) / n

    return cx_num / (3.0 * area2), cy_num / (3.0 * area2)


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


def _dist_to_segment(
    px: float,
    py: float,
    ax: float,
    ay: float,
    bx: float,
    by: float,
) -> float:
    dx = bx - ax
    dy = by - ay
    if dx == 0.0 and dy == 0.0:
        return math.hypot(px - ax, py - ay)
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    qx = ax + t * dx
    qy = ay + t * dy
    return math.hypot(px - qx, py - qy)


def _point_to_polygon_signed_distance(
    x: float,
    y: float,
    polygon: list[list[tuple[float, float]]],
) -> float:
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


def _resolve_resample_filter() -> Any:
    name = _tile_downsample_filter()
    if hasattr(Image, "Resampling"):
        mapping = {
            "nearest": Image.Resampling.NEAREST,
            "box": Image.Resampling.BOX,
            "bilinear": Image.Resampling.BILINEAR,
            "hamming": Image.Resampling.HAMMING,
            "bicubic": Image.Resampling.BICUBIC,
            "lanczos": Image.Resampling.LANCZOS,
        }
        return mapping.get(name, Image.Resampling.LANCZOS)

    mapping = {
        "nearest": Image.NEAREST,
        "box": Image.BOX,
        "bilinear": Image.BILINEAR,
        "hamming": Image.HAMMING,
        "bicubic": Image.BICUBIC,
        "lanczos": Image.LANCZOS,
    }
    return mapping.get(name, Image.LANCZOS)


def _load_tile_features_png(z: int, x: int, y: int) -> list[dict[str, Any]]:
    table_name = os.getenv("CADASTRAL_TILE_TABLE", "cadastral_features")
    geojson_col = os.getenv("CADASTRAL_TILE_GEOJSON_COL", "geojson")
    label_col = os.getenv("CADASTRAL_TILE_LABEL_COL", "label")
    pnu_col = os.getenv("CADASTRAL_TILE_PNU_COL", "pnu")
    label_lon_col = os.getenv("CADASTRAL_TILE_LABEL_LON_COL", "label_lon")
    label_lat_col = os.getenv("CADASTRAL_TILE_LABEL_LAT_COL", "label_lat")
    min_lon_col = os.getenv("CADASTRAL_TILE_MIN_LON_COL", "bbox_min_lon")
    max_lon_col = os.getenv("CADASTRAL_TILE_MAX_LON_COL", "bbox_max_lon")
    min_lat_col = os.getenv("CADASTRAL_TILE_MIN_LAT_COL", "bbox_min_lat")
    max_lat_col = os.getenv("CADASTRAL_TILE_MAX_LAT_COL", "bbox_max_lat")
    row_limit_raw = int(os.getenv("CADASTRAL_TILE_DB_LIMIT", "6000"))
    row_limit = row_limit_raw if row_limit_raw > 0 else None
    release_col = os.getenv("CADASTRAL_TILE_RELEASE_COL", "release_id")
    active_release = _active_release("cadastral")

    west, south, east, north = _tile_bounds(z, x, y)

    def _query_rows(with_release_filter: bool, with_label_point: bool) -> list[tuple[Any, str, str, Any, Any]]:
        bbox_expr = sql.SQL("box(point({min_lon_col}, {min_lat_col}), point({max_lon_col}, {max_lat_col}))").format(
            min_lon_col=sql.Identifier(min_lon_col),
            min_lat_col=sql.Identifier(min_lat_col),
            max_lon_col=sql.Identifier(max_lon_col),
            max_lat_col=sql.Identifier(max_lat_col),
        )
        tile_box_expr = sql.SQL("box(point(%s, %s), point(%s, %s))")
        clauses = [
            sql.SQL("{bbox_expr} && {tile_box_expr}").format(
                bbox_expr=bbox_expr,
                tile_box_expr=tile_box_expr,
            )
        ]
        params: list[Any] = [west, south, east, north]

        if with_release_filter and active_release:
            clauses.append(sql.SQL("{release_col} = %s").format(release_col=sql.Identifier(release_col)))
            params.append(active_release["id"])

        if with_label_point:
            select_sql = sql.SQL(
                "SELECT {geojson_col}, COALESCE({label_col}::text, ''), COALESCE({pnu_col}::text, ''), "
                "{label_lon_col}, {label_lat_col} "
            ).format(
                geojson_col=sql.Identifier(geojson_col),
                label_col=sql.Identifier(label_col),
                pnu_col=sql.Identifier(pnu_col),
                label_lon_col=sql.Identifier(label_lon_col),
                label_lat_col=sql.Identifier(label_lat_col),
            )
        else:
            select_sql = sql.SQL(
                "SELECT {geojson_col}, COALESCE({label_col}::text, ''), COALESCE({pnu_col}::text, ''), "
                "NULL::double precision AS label_lon, NULL::double precision AS label_lat "
            ).format(
                geojson_col=sql.Identifier(geojson_col),
                label_col=sql.Identifier(label_col),
                pnu_col=sql.Identifier(pnu_col),
            )

        query = (
            select_sql
            + sql.SQL("FROM {table_name} WHERE ").format(table_name=sql.Identifier(table_name))
            + sql.SQL(" AND ").join(clauses)
        )
        if row_limit is not None:
            query = query + sql.SQL(" LIMIT %s")
            params.append(row_limit)

        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    try:
        if active_release:
            try:
                try:
                    rows = _query_rows(with_release_filter=True, with_label_point=True)
                except Exception:
                    rows = _query_rows(with_release_filter=True, with_label_point=False)
            except Exception:
                try:
                    rows = _query_rows(with_release_filter=False, with_label_point=True)
                except Exception:
                    rows = _query_rows(with_release_filter=False, with_label_point=False)
        else:
            try:
                rows = _query_rows(with_release_filter=False, with_label_point=True)
            except Exception:
                rows = _query_rows(with_release_filter=False, with_label_point=False)
    except Exception:
        return []

    features: list[dict[str, Any]] = []
    for geojson_raw, label, pnu, label_lon, label_lat in rows:
        geom = _safe_json_loads(geojson_raw)
        if geom is None:
            continue
        features.append(
            {
                "geometry": geom,
                "label": label,
                "pnu": pnu,
                "label_lon": float(label_lon) if isinstance(label_lon, (float, int)) else None,
                "label_lat": float(label_lat) if isinstance(label_lat, (float, int)) else None,
            }
        )

    return features


def _load_tile_features_json(z: int, x: int, y: int) -> list[dict[str, Any]]:
    table_name = os.getenv("CADASTRAL_TILE_TABLE", "cadastral_features")
    geojson_col = os.getenv("CADASTRAL_TILE_GEOJSON_COL", "geojson")
    label_col = os.getenv("CADASTRAL_TILE_LABEL_COL", "label")
    pnu_col = os.getenv("CADASTRAL_TILE_PNU_COL", "pnu")
    label_lon_col = os.getenv("CADASTRAL_TILE_LABEL_LON_COL", "label_lon")
    label_lat_col = os.getenv("CADASTRAL_TILE_LABEL_LAT_COL", "label_lat")
    min_lon_col = os.getenv("CADASTRAL_TILE_MIN_LON_COL", "bbox_min_lon")
    max_lon_col = os.getenv("CADASTRAL_TILE_MAX_LON_COL", "bbox_max_lon")
    min_lat_col = os.getenv("CADASTRAL_TILE_MIN_LAT_COL", "bbox_min_lat")
    max_lat_col = os.getenv("CADASTRAL_TILE_MAX_LAT_COL", "bbox_max_lat")
    row_limit_raw = int(os.getenv("CADASTRAL_VECTOR_TILE_DB_LIMIT", os.getenv("CADASTRAL_TILE_DB_LIMIT", "6000")))
    row_limit = row_limit_raw if row_limit_raw > 0 else None
    release_col = os.getenv("CADASTRAL_TILE_RELEASE_COL", "release_id")
    active_release = _active_release("cadastral")

    west, south, east, north = _tile_bounds(z, x, y)

    def _query_rows(with_release_filter: bool, with_label_point: bool) -> list[tuple[Any, str, str, Any, Any]]:
        bbox_expr = sql.SQL("box(point({min_lon_col}, {min_lat_col}), point({max_lon_col}, {max_lat_col}))").format(
            min_lon_col=sql.Identifier(min_lon_col),
            min_lat_col=sql.Identifier(min_lat_col),
            max_lon_col=sql.Identifier(max_lon_col),
            max_lat_col=sql.Identifier(max_lat_col),
        )
        tile_box_expr = sql.SQL("box(point(%s, %s), point(%s, %s))")
        clauses = [
            sql.SQL("{bbox_expr} && {tile_box_expr}").format(
                bbox_expr=bbox_expr,
                tile_box_expr=tile_box_expr,
            )
        ]
        params: list[Any] = [west, south, east, north]

        if with_release_filter and active_release:
            clauses.append(sql.SQL("{release_col} = %s").format(release_col=sql.Identifier(release_col)))
            params.append(active_release["id"])

        if with_label_point:
            select_sql = sql.SQL(
                "SELECT {geojson_col}, COALESCE({label_col}::text, ''), COALESCE({pnu_col}::text, ''), "
                "{label_lon_col}, {label_lat_col} "
            ).format(
                geojson_col=sql.Identifier(geojson_col),
                label_col=sql.Identifier(label_col),
                pnu_col=sql.Identifier(pnu_col),
                label_lon_col=sql.Identifier(label_lon_col),
                label_lat_col=sql.Identifier(label_lat_col),
            )
        else:
            select_sql = sql.SQL(
                "SELECT {geojson_col}, COALESCE({label_col}::text, ''), COALESCE({pnu_col}::text, ''), "
                "NULL::double precision AS label_lon, NULL::double precision AS label_lat "
            ).format(
                geojson_col=sql.Identifier(geojson_col),
                label_col=sql.Identifier(label_col),
                pnu_col=sql.Identifier(pnu_col),
            )

        query = (
            select_sql
            + sql.SQL("FROM {table_name} WHERE ").format(table_name=sql.Identifier(table_name))
            + sql.SQL(" AND ").join(clauses)
        )
        if row_limit is not None:
            query = query + sql.SQL(" LIMIT %s")
            params.append(row_limit)

        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    try:
        if active_release:
            try:
                try:
                    rows = _query_rows(with_release_filter=True, with_label_point=True)
                except Exception:
                    rows = _query_rows(with_release_filter=True, with_label_point=False)
            except Exception:
                try:
                    rows = _query_rows(with_release_filter=False, with_label_point=True)
                except Exception:
                    rows = _query_rows(with_release_filter=False, with_label_point=False)
        else:
            try:
                rows = _query_rows(with_release_filter=False, with_label_point=True)
            except Exception:
                rows = _query_rows(with_release_filter=False, with_label_point=False)
    except Exception:
        return []

    features: list[dict[str, Any]] = []
    for geojson_raw, label, pnu, label_lon, label_lat in rows:
        geom = _safe_json_loads(geojson_raw)
        if geom is None:
            continue
        features.append(
            {
                "geometry": geom,
                "label": label,
                "pnu": pnu,
                "label_lon": float(label_lon) if isinstance(label_lon, (float, int)) else None,
                "label_lat": float(label_lat) if isinstance(label_lat, (float, int)) else None,
            }
        )

    return features


def _render_cadastral_tile(z: int, x: int, y: int) -> bytes:
    features = _load_tile_features_png(z, x, y)
    if not features:
        return _empty_tile_bytes()

    tile_size = _tile_size()
    supersample = _tile_supersample()
    render_size = tile_size * supersample

    image = Image.new("RGBA", (render_size, render_size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image, "RGBA")
    stroke_color = (0, 0, 0, _tile_stroke_alpha())
    stroke_width = max(1, int(round(_tile_stroke_width() * supersample)))
    label_enabled = _tile_label_enabled() and z >= _tile_label_min_zoom()
    label_min_font = _tile_label_min_font_px() * supersample
    label_max_font = _tile_label_max_font_px() * supersample
    label_zoom_scale = _tile_label_zoom_scale(z)
    effective_label_max_font = max(label_min_font, label_max_font * label_zoom_scale)
    label_stroke_width = max(1, int(round(_tile_label_stroke_width() * supersample)))
    font_cache: dict[int, ImageFont.FreeTypeFont | ImageFont.ImageFont] = {}

    def get_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        size = max(8, size)
        cached = font_cache.get(size)
        if cached:
            return cached
        try:
            font = ImageFont.truetype(_tile_label_font_path(), size=size)
        except Exception:
            font = ImageFont.load_default()
        font_cache[size] = font
        return font

    drawn = 0
    for feature in features:
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict):
            continue

        label_lon = feature.get("label_lon")
        label_lat = feature.get("label_lat")
        pre_label_point: tuple[float, float] | None = None
        if isinstance(label_lon, float) and isinstance(label_lat, float):
            pre_label_point = _lonlat_to_tile_pixel(label_lon, label_lat, z, x, y, render_size)

        surfaces = _polygon_surfaces(geometry)
        best_label: dict[str, float] | None = None
        best_score = 0.0
        for surface in surfaces:
            pixel_surface: list[list[tuple[float, float]]] = []
            for ring in surface:
                pixel_points = []
                for point in ring:
                    if not isinstance(point, (list, tuple)) or len(point) < 2:
                        continue
                    lon = float(point[0])
                    lat = float(point[1])
                    pixel_points.append(_lonlat_to_tile_pixel(lon, lat, z, x, y, render_size))

                core = _ring_core_points(pixel_points)
                if len(core) < 3:
                    continue

                draw.line(core + [core[0]], fill=stroke_color, width=stroke_width)
                drawn += 1
                pixel_surface.append(core)

            if not label_enabled or not pixel_surface:
                continue

            outer_box = _ring_bbox(pixel_surface[0])
            if outer_box is None:
                continue

            cx = 0.0
            cy = 0.0
            radius = -1.0
            if pre_label_point is not None:
                px, py = pre_label_point
                pre_radius = _point_to_polygon_signed_distance(px, py, pixel_surface)
                if pre_radius > radius:
                    cx = px
                    cy = py
                    radius = pre_radius

            centroid = _ring_centroid(pixel_surface[0])
            if centroid is not None:
                c_radius = _point_to_polygon_signed_distance(centroid[0], centroid[1], pixel_surface)
                if c_radius > radius:
                    cx = centroid[0]
                    cy = centroid[1]
                    radius = c_radius

            box_cx = (outer_box[0] + outer_box[2]) * 0.5
            box_cy = (outer_box[1] + outer_box[3]) * 0.5
            box_radius = _point_to_polygon_signed_distance(box_cx, box_cy, pixel_surface)
            if box_radius > radius:
                cx = box_cx
                cy = box_cy
                radius = box_radius

            area = abs(_ring_signed_area(pixel_surface[0]))
            score = area + (radius * 8.0)
            if score > best_score:
                best_score = score
                best_label = {
                    "cx": cx,
                    "cy": cy,
                    "radius": radius,
                }

        if label_enabled and best_label is not None:
            label = _label_text(feature.get("label"), feature.get("pnu"))
            if label:
                fit_radius = best_label["radius"] * (0.55 + 0.35 * label_zoom_scale)
                target = fit_radius * (0.70 + 0.30 * label_zoom_scale)
                font_size = int(max(label_min_font, min(effective_label_max_font, target)))
                font = get_font(font_size)
                text_box = draw.textbbox((0, 0), label, font=font)
                text_w = text_box[2] - text_box[0]
                text_h = text_box[3] - text_box[1]
                text_diag = math.hypot(text_w * 0.5, text_h * 0.5)

                while font_size > int(label_min_font) and text_diag > fit_radius:
                    font_size -= 1
                    font = get_font(font_size)
                    text_box = draw.textbbox((0, 0), label, font=font)
                    text_w = text_box[2] - text_box[0]
                    text_h = text_box[3] - text_box[1]
                    text_diag = math.hypot(text_w * 0.5, text_h * 0.5)

                while font_size < int(effective_label_max_font):
                    next_font = get_font(font_size + 1)
                    next_box = draw.textbbox((0, 0), label, font=next_font)
                    next_w = next_box[2] - next_box[0]
                    next_h = next_box[3] - next_box[1]
                    next_diag = math.hypot(next_w * 0.5, next_h * 0.5)
                    if next_diag > fit_radius:
                        break
                    font_size += 1
                    font = next_font
                    text_w = next_w
                    text_h = next_h
                    text_diag = next_diag

                # 작은 필지여도 최소 폰트로 라벨을 표시한다.
                draw.text(
                    (best_label["cx"] - text_w / 2, best_label["cy"] - text_h / 2),
                    label,
                    fill=(17, 24, 39, 225),
                    font=font,
                    stroke_width=label_stroke_width,
                    stroke_fill=(255, 255, 255, 215),
                )

    if drawn == 0:
        return _empty_tile_bytes()

    if supersample > 1:
        blur_radius = _tile_prefilter_blur_radius()
        if blur_radius > 0:
            image = image.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        resample = _resolve_resample_filter()
        image = image.resize((tile_size, tile_size), resample=resample)

    unsharp_percent = _tile_unsharp_percent()
    unsharp_radius = _tile_unsharp_radius()
    if unsharp_percent > 0 and unsharp_radius > 0:
        image = image.filter(
            ImageFilter.UnsharpMask(
                radius=unsharp_radius,
                percent=unsharp_percent,
                threshold=_tile_unsharp_threshold(),
            )
        )

    output = BytesIO()
    image.save(output, format="PNG", optimize=True)
    return output.getvalue()


def _save_tile(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_bytes(content)
    tmp_path.replace(path)


async def _get_tile_lock(cache_key: str) -> asyncio.Lock:
    async with _TILE_LOCKS_GUARD:
        lock = _TILE_LOCKS.get(cache_key)
        if lock is None:
            lock = asyncio.Lock()
            _TILE_LOCKS[cache_key] = lock
        return lock


async def _get_or_create_tile(z: int, x: int, y: int, version: str) -> tuple[bytes, str]:
    cache_key = f"{version}:{_tile_render_rev()}:{z}:{x}:{y}"
    memory_cache = _tile_memory_cache()

    from_memory = memory_cache.get(cache_key)
    if from_memory is not None:
        return from_memory, "memory"

    tile_path = _tile_path(z, x, y, version=version)
    if tile_path.exists():
        data = await asyncio.to_thread(tile_path.read_bytes)
        memory_cache.put(cache_key, data)
        return data, "disk"

    lock = await _get_tile_lock(cache_key)
    async with lock:
        from_memory = memory_cache.get(cache_key)
        if from_memory is not None:
            return from_memory, "memory"

        if tile_path.exists():
            data = await asyncio.to_thread(tile_path.read_bytes)
            memory_cache.put(cache_key, data)
            return data, "disk"

        data = await asyncio.to_thread(_render_cadastral_tile, z, x, y)
        await asyncio.to_thread(_save_tile, tile_path, data)
        memory_cache.put(cache_key, data)
        return data, "render"


def _cadastral_cache_headers(z: int, x: int, y: int, cache_state: str, version: str) -> dict[str, str]:
    etag = f'W/"cad-{version}-{_tile_render_rev()}-{z}-{x}-{y}"'
    return {
        "Cache-Control": "public, max-age=31536000, immutable",
        "CDN-Cache-Control": "public, max-age=31536000, immutable",
        "ETag": etag,
        "Vary": "Accept-Encoding",
        "X-Tile-Cache": cache_state,
        "X-Tile-Version": version,
        "X-Tile-Render-Rev": _tile_render_rev(),
    }


def _cadastral_nostore_headers(z: int, x: int, y: int, cache_state: str, version: str) -> dict[str, str]:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "Vary": "Accept-Encoding",
        "X-Tile-Cache": cache_state,
        "X-Tile-Version": version,
        "X-Tile-Render-Rev": _tile_render_rev(),
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return ok({"data_dir": os.getenv("DATA_DIR", "/data/uploads")})


@app.get("/v1/tiles/cadastral/{z}/{x}/{y}.png")
async def get_cadastral_tile(
    z: int,
    x: int,
    y: int,
    request: Request,
    v: str | None = Query(default=None),
) -> Response:
    tile_version = v or _tile_version()
    headers = _cadastral_cache_headers(z, x, y, "skip", version=tile_version)
    etag = headers["ETag"]

    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)

    min_zoom = _tile_min_zoom()
    max_zoom = _tile_max_zoom()
    max_index = (1 << z) - 1 if z >= 0 else -1
    if (
        z < min_zoom
        or z > max_zoom
        or x < 0
        or y < 0
        or x > max_index
        or y > max_index
    ):
        return Response(
            content=_empty_tile_bytes(),
            media_type="image/png",
            headers={**headers, "X-Tile-Cache": "range"},
        )

    data, cache_state = await _get_or_create_tile(z, x, y, version=tile_version)
    headers = _cadastral_cache_headers(z, x, y, cache_state, version=tile_version)

    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)

    return Response(content=data, media_type="image/png", headers=headers)


@app.get("/v1/tiles/cadastral/{z}/{x}/{y}.json")
async def get_cadastral_tile_features(
    z: int,
    x: int,
    y: int,
    v: str | None = Query(default=None),
) -> Response:
    tile_version = v or _tile_version()
    headers = _cadastral_nostore_headers(z, x, y, "skip", version=tile_version)

    min_zoom = _tile_min_zoom()
    max_zoom = _tile_max_zoom()
    max_index = (1 << z) - 1 if z >= 0 else -1
    if (
        z < min_zoom
        or z > max_zoom
        or x < 0
        or y < 0
        or x > max_index
        or y > max_index
    ):
        payload = ok({"z": z, "x": x, "y": y, "items": []})
        return JSONResponse(content=jsonable_encoder(payload), headers={**headers, "X-Tile-Cache": "range"})

    features = await asyncio.to_thread(_load_tile_features_json, z, x, y)
    payload = ok({"z": z, "x": x, "y": y, "items": features})
    return JSONResponse(content=jsonable_encoder(payload), headers={**headers, "X-Tile-Cache": "db"})

_BUILDING_INFO_DATASET_TO_BUCKET: dict[str, str] = {
    "building_info_total": "total",
    "building_info_single": "single",
    "building_info_floor": "floor",
    "building_info_room": "room",
}

_BUILDING_INFO_BUCKET_COLUMNS: dict[str, list[str]] = {
    "total": [
        # TotalBuilding.fromLine expects exactly 21 fields.
        "대장_구분_코드_명",
        "대지_면적(㎡)",
        "건축_면적(㎡)",
        "건폐_율(%)",
        "연면적(㎡)",
        "용적_률_산정_연면적(㎡)",
        "용적_률(%)",
        "기타_용도",
        "세대_수(세대)",
        "가구_수(가구)",
        "주_건축물_수",
        "부속_건축물_수",
        "옥내_기계식_대수(대)",
        "옥외_기계식_대수(대)",
        "옥내_자주식_대수(대)",
        "옥외_자주식_대수(대)",
        "허가_일",
        "착공_일",
        "사용승인_일",
        "호_수(호)",
        "생성_일자",
    ],
    "single": [
        # Building.fromLine expects exactly 28 fields.
        "관리_건축물대장_PK",
        # 2nd field is regstrKindCd for mobile parser compatibility.
        "대장_종류_코드",
        "동_명",
        "주_부속_구분_코드",
        "대지_면적(㎡)",
        "건축_면적(㎡)",
        "건폐_율(%)",
        "연면적(㎡)",
        "용적_률_산정_연면적(㎡)",
        "구조_코드_명",
        "주_용도_코드_명",
        "기타_용도",
        "세대_수(세대)",
        "가구_수(가구)",
        "지상_층_수",
        "지하_층_수",
        "승용_승강기_수",
        "부속_건축물_수",
        "옥내_기계식_대수(대)",
        "옥외_기계식_대수(대)",
        "옥내_자주식_대수(대)",
        "옥외_자주식_대수(대)",
        "허가_일",
        "착공_일",
        "사용승인_일",
        "호_수(호)",
        "내진_설계_적용_여부",
        "내진_능력",
    ],
    "floor": [
        # Floor.fromLine expects exactly 8 fields.
        "관리_건축물대장_PK",
        "동_명",
        "층_구분_코드",
        "층_번호",
        "층_번호_명",
        "구조_코드_명",
        "기타_용도",
        "면적(㎡)",
    ],
    "room": [
        # Room.fromLine expects exactly 10 fields.
        "동_명",
        "호_명",
        "층_구분_코드",
        "층_번호",
        "전유_공용_구분_코드_명",
        "층_번호_명",
        "구조_코드_명",
        "주_용도_코드_명",
        "기타_용도",
        "면적(㎡)",
    ],
}


def _load_dataset_schema_columns(schema_ids: list[int]) -> dict[int, list[str]]:
    unique_ids = sorted({int(item) for item in schema_ids if item is not None})
    if not unique_ids:
        return {}

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, columns
                    FROM dataset_schema
                    WHERE id = ANY(%s)
                    """,
                    (unique_ids,),
                )
                rows = cur.fetchall()
    except Exception:
        return {}

    result: dict[int, list[str]] = {}
    for row in rows:
        schema_id = int(row[0])
        columns_raw = row[1]
        if isinstance(columns_raw, list):
            result[schema_id] = [str(col) for col in columns_raw]
        else:
            result[schema_id] = []
    return result


def _decode_dataset_payload(
    payload_raw: Any,
    schema_id_raw: Any,
    payload_values_raw: Any,
    schema_columns_by_id: dict[int, list[str]],
) -> dict[str, Any]:
    if isinstance(payload_raw, dict):
        return payload_raw

    def _decode_values_from_gzip_marker(marker: str) -> list[Any]:
        if not marker.startswith("gz:"):
            return []
        try:
            compressed = base64.b64decode(marker[3:], validate=True)
            raw = gzip.decompress(compressed)
            loaded = json.loads(raw.decode("utf-8", "ignore"))
            if isinstance(loaded, list):
                return loaded
        except Exception:
            return []
        return []

    values: list[Any] = []
    if isinstance(payload_values_raw, list):
        values = payload_values_raw
    elif isinstance(payload_values_raw, str):
        gz_values = _decode_values_from_gzip_marker(payload_values_raw)
        if gz_values:
            values = gz_values
        else:
            try:
                loaded = json.loads(payload_values_raw)
                if isinstance(loaded, list):
                    values = loaded
                elif isinstance(loaded, str):
                    values = _decode_values_from_gzip_marker(loaded)
            except Exception:
                values = []
    elif isinstance(payload_values_raw, (bytes, bytearray, memoryview)):
        try:
            loaded = json.loads(bytes(payload_values_raw).decode("utf-8", "ignore"))
            if isinstance(loaded, list):
                values = loaded
            elif isinstance(loaded, str):
                values = _decode_values_from_gzip_marker(loaded)
        except Exception:
            values = []

    if not values:
        return {}

    schema_id: int | None = None
    if isinstance(schema_id_raw, int):
        schema_id = int(schema_id_raw)
    elif isinstance(schema_id_raw, str) and schema_id_raw.isdigit():
        schema_id = int(schema_id_raw)

    columns = schema_columns_by_id.get(schema_id or -1, [])
    payload: dict[str, Any] = {}
    if columns:
        max_len = min(len(columns), len(values))
        for idx in range(max_len):
            payload[str(columns[idx])] = values[idx]
        for idx in range(max_len, len(values)):
            payload[f"col_{idx + 1}"] = values[idx]
        return payload

    for idx, value in enumerate(values):
        payload[f"col_{idx + 1}"] = value
    return payload


def _decode_dataset_geometry(geometry_raw: Any) -> dict[str, Any] | None:
    if isinstance(geometry_raw, dict):
        return geometry_raw

    text = ""
    if isinstance(geometry_raw, str):
        text = geometry_raw
    elif isinstance(geometry_raw, (bytes, bytearray, memoryview)):
        text = bytes(geometry_raw).decode("utf-8", "ignore")
    else:
        return None

    text = text.strip()
    if not text:
        return None

    def _decode_gz(marker: str) -> dict[str, Any] | None:
        if not marker.startswith("gz:"):
            return None
        try:
            compressed = base64.b64decode(marker[3:], validate=True)
            raw = gzip.decompress(compressed)
            loaded = json.loads(raw.decode("utf-8", "ignore"))
            if isinstance(loaded, dict):
                return loaded
        except Exception:
            return None
        return None

    if text.startswith("gz:"):
        return _decode_gz(text)

    if text[:1] in {"{", "["}:
        try:
            loaded = json.loads(text)
            if isinstance(loaded, dict):
                return loaded
            if isinstance(loaded, str):
                return _decode_gz(loaded)
        except Exception:
            return None
    return None


def _building_info_bucket_from_record(dataset_code: Any, payload: dict[str, Any]) -> str:
    bucket = _BUILDING_INFO_DATASET_TO_BUCKET.get(str(dataset_code or "").strip())
    if bucket:
        return bucket
    category = str(payload.get("_category") or "").strip().lower()
    if category in _BUILDING_INFO_BUCKET_COLUMNS:
        return category
    return "single"


def _payload_to_building_info_line(payload: dict[str, Any], bucket: str) -> str:
    columns = _BUILDING_INFO_BUCKET_COLUMNS.get(bucket, [])
    parts: list[str] = []
    for column in columns:
        value = payload.get(column, "")
        parts.append(str(value if value is not None else ""))
    return "|".join(parts)


def _pnu_query_candidates(pnu: str) -> list[str]:
    return _build_pnu_query_candidates(pnu)


def _decode_json_or_gz_marker(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    if text.startswith("gz:"):
        try:
            compressed = base64.b64decode(text[3:], validate=True)
            raw = gzip.decompress(compressed)
            return json.loads(raw.decode("utf-8", "ignore"))
        except Exception:
            return value
    if text[:1] in {"{", "["}:
        try:
            return json.loads(text)
        except Exception:
            return value
    return value


def _fetch_dataset_pnu_kv_payload(data_type: str, pnu: str) -> Any | None:
    normalized_type = _normalize_data_type(data_type)
    active_release = _active_release(normalized_type)
    if not active_release:
        return None
    pnu_candidates = _pnu_query_candidates(pnu)
    if not pnu_candidates:
        return None

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                for candidate_pnu in pnu_candidates:
                    cur.execute(
                        """
                        SELECT payload
                        FROM dataset_pnu_kv
                        WHERE release_id = %s
                          AND data_type = %s
                          AND pnu = %s
                        LIMIT 1
                        """,
                        (active_release["id"], normalized_type, candidate_pnu),
                    )
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        return _decode_json_or_gz_marker(row[0])
    except Exception:
        return None
    return None


def _normalize_pnu_kv_records(payload: Any) -> list[Dict[str, Any]]:
    decoded = _decode_json_or_gz_marker(payload)
    raw_records: list[Any] = []

    if isinstance(decoded, list):
        raw_records = decoded
    elif isinstance(decoded, dict):
        for key in ("items", "records", "rows", "parts"):
            candidate = _decode_json_or_gz_marker(decoded.get(key))
            if isinstance(candidate, list):
                raw_records = candidate
                break
        if not raw_records and "dataset_code" in decoded:
            raw_records = [decoded]

    if not raw_records:
        return []

    normalized: list[Dict[str, Any]] = []
    for row in raw_records:
        item = _decode_json_or_gz_marker(row)
        if not isinstance(item, dict):
            continue

        payload_obj = _decode_json_or_gz_marker(item.get("payload"))
        if isinstance(payload_obj, list):
            payload_obj = {"values": payload_obj}
        if not isinstance(payload_obj, dict):
            payload_obj = {}

        geometry_obj = _decode_json_or_gz_marker(item.get("geometry"))
        if geometry_obj is not None and not isinstance(geometry_obj, (dict, list)):
            geometry_obj = None

        normalized.append(
            {
                "dataset_code": item.get("dataset_code") or item.get("code"),
                "payload": payload_obj,
                "geometry": geometry_obj,
                "source_file": item.get("source_file") or item.get("source"),
                "row_no": item.get("row_no"),
            }
        )
    return normalized


def _extract_building_info_buckets_from_kv_payload(payload: Any) -> dict[str, list[str]]:
    decoded = _decode_json_or_gz_marker(payload)
    buckets: dict[str, list[str]] = {
        "total": [],
        "single": [],
        "floor": [],
        "room": [],
    }

    bucket_source: dict[str, Any] | None = None
    if isinstance(decoded, dict):
        if isinstance(decoded.get("buckets"), dict):
            bucket_source = decoded.get("buckets")
        else:
            bucket_source = decoded

    if bucket_source is not None:
        for bucket in ("total", "single", "floor", "room"):
            raw_lines = _decode_json_or_gz_marker(bucket_source.get(bucket))
            if not isinstance(raw_lines, list):
                continue
            lines: list[str] = []
            for item in raw_lines:
                if isinstance(item, str):
                    lines.append(item)
                elif isinstance(item, dict):
                    lines.append(_payload_to_building_info_line(item, bucket))
                elif item is not None:
                    lines.append(str(item))
            buckets[bucket] = lines
        if any(buckets.values()):
            return buckets

    for record in _normalize_pnu_kv_records(decoded):
        payload_obj = record.get("payload")
        if not isinstance(payload_obj, dict):
            continue
        bucket = _building_info_bucket_from_record(record.get("dataset_code"), payload_obj)
        line = _payload_to_building_info_line(payload_obj, bucket)
        if bucket in buckets:
            buckets[bucket].append(line)
        else:
            buckets["single"].append(line)

    return buckets


def _fetch_building_info_line(pnu: str) -> str | None:
    pnu_candidates = _pnu_query_candidates(pnu)
    if not pnu_candidates:
        return None

    def _total_preview_meta(conn: psycopg.Connection, release_id: int, candidate_pnu: str) -> dict[str, Any] | None:
        """Return metadata for opening the MOLIT IRTS building register preview page.

        The preview URL needs (mgmBldrgstPk, regstrKindCd, pnuCode). Our compact 'total' bucket
        omits mgmBldrgstPk/regstrKindCd to keep payload small, so we fetch the PK from lookup.
        """
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT building_mgmt_pk
                    FROM building_info_lookup
                    WHERE release_id = %s
                      AND pnu = %s
                      AND dataset_code = 'building_info_total'
                    ORDER BY id
                    LIMIT 1
                    """,
                    (int(release_id), str(candidate_pnu)),
                )
                row = cur.fetchone()
            if not row or row[0] is None:
                return None
            mgm_pk = str(row[0]).strip()
            if not mgm_pk:
                return None
            # NOTE: IRTS uses regstrKindCd as a query param. For '총괄표제부' preview this is "1".
            return {
                "mgmBldrgstPk": mgm_pk,
                "regstrKindCd": "1",
                "irtsReqRegstrKindCd": "1",
            }
        except Exception:
            return None

    def _bucket_irts_meta(buckets: dict[str, list[str]]) -> dict[str, Any]:
        meta: dict[str, Any] = {}
        if buckets.get("total"):
            meta["total"] = {
                "irtsReqRegstrKindCd": "1",
            }
        if buckets.get("single"):
            meta["single"] = {
                "irtsReqRegstrKindCd": "2",
            }
        return meta

    def _attach_meta(buckets: dict[str, list[str]], *, conn: psycopg.Connection, release_id: int, candidate_pnu: str) -> dict[str, Any]:
        meta: dict[str, Any] = _bucket_irts_meta(buckets)
        total_meta = _total_preview_meta(conn, release_id, candidate_pnu)
        if total_meta is not None:
            merged_total = dict(meta.get("total") or {})
            merged_total.update(total_meta)
            meta["total"] = merged_total
        if not meta:
            return buckets
        enriched: dict[str, Any] = dict(buckets)
        enriched["meta"] = meta
        return enriched

    kv_payload = _fetch_dataset_pnu_kv_payload("building_info", pnu)
    if kv_payload is not None:
        buckets = _extract_building_info_buckets_from_kv_payload(kv_payload)
        if any(buckets.values()):
            active_release = _active_release("building_info")
            if not active_release:
                meta_only = _bucket_irts_meta(buckets)
                if meta_only:
                    enriched = dict(buckets)
                    enriched["meta"] = meta_only
                    return pnu + json.dumps(enriched, ensure_ascii=False)
                return pnu + json.dumps(buckets, ensure_ascii=False)
            try:
                with psycopg.connect(_db_url()) as conn:
                    for candidate_pnu in pnu_candidates:
                        enriched = _attach_meta(
                            buckets,
                            conn=conn,
                            release_id=int(active_release["id"]),
                            candidate_pnu=candidate_pnu,
                        )
                        # Prefer the first candidate that has meta; otherwise fall back to the first one.
                        if isinstance(enriched, dict) and isinstance(enriched.get("meta"), dict):
                            return pnu + json.dumps(enriched, ensure_ascii=False)
            except Exception:
                pass
            meta_only = _bucket_irts_meta(buckets)
            if meta_only:
                enriched = dict(buckets)
                enriched["meta"] = meta_only
                return pnu + json.dumps(enriched, ensure_ascii=False)
            return pnu + json.dumps(buckets, ensure_ascii=False)

    active_release = _active_release("building_info")
    if active_release:
        # Preferred storage: insert-only line table (fast import, avoids KV upsert bloat).
        try:
            with psycopg.connect(_db_url()) as conn:
                for candidate_pnu in pnu_candidates:
                    buckets: dict[str, list[str]] = {
                        "total": [],
                        "single": [],
                        "floor": [],
                        "room": [],
                    }
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT category, line
                            FROM building_info_line
                            WHERE release_id = %s
                              AND pnu = %s
                            ORDER BY
                              CASE category
                                WHEN 'building_info_total' THEN 0
                                WHEN 'total' THEN 0
                                WHEN 'building_info_single' THEN 1
                                WHEN 'single' THEN 1
                                WHEN 'building_info_floor' THEN 2
                                WHEN 'floor' THEN 2
                                WHEN 'building_info_room' THEN 3
                                WHEN 'room' THEN 3
                                ELSE 4
                              END,
                              id
                            LIMIT 20000
                            """,
                            (active_release["id"], candidate_pnu),
                        )
                        rows = cur.fetchall()
                    if not rows:
                        continue
                    for row in rows:
                        category = str(row[0] or "").strip().lower()
                        line_text = row[1]
                        if line_text is None:
                            continue
                        line_value = str(line_text)
                        if category in buckets:
                            buckets[category].append(line_value)
                        else:
                            buckets["single"].append(line_value)
                    if any(buckets.values()):
                        enriched = _attach_meta(
                            buckets,
                            conn=conn,
                            release_id=int(active_release["id"]),
                            candidate_pnu=candidate_pnu,
                        )
                        return pnu + json.dumps(enriched, ensure_ascii=False)
        except Exception:
            pass

        try:
            with psycopg.connect(_db_url()) as conn:
                for candidate_pnu in pnu_candidates:
                    buckets: dict[str, list[str]] = {
                        "total": [],
                        "single": [],
                        "floor": [],
                        "room": [],
                    }
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT dataset_code, payload, schema_id, payload_values
                            FROM dataset_record
                            WHERE release_id = %s
                              AND data_type = 'building_info'
                              AND pnu = %s
                            ORDER BY
                              CASE dataset_code
                                WHEN 'building_info_total' THEN 0
                                WHEN 'building_info_single' THEN 1
                                WHEN 'building_info_floor' THEN 2
                                WHEN 'building_info_room' THEN 3
                                ELSE 4
                              END,
                              id
                            LIMIT 20000
                            """,
                            (active_release["id"], candidate_pnu),
                        )
                        rows = cur.fetchall()
                    if not rows:
                        continue
                    schema_columns_by_id = _load_dataset_schema_columns(
                        [int(row[2]) for row in rows if row[2] is not None]
                    )
                    for row in rows:
                        payload = _decode_dataset_payload(
                            payload_raw=row[1],
                            schema_id_raw=row[2],
                            payload_values_raw=row[3],
                            schema_columns_by_id=schema_columns_by_id,
                        )
                        bucket = _building_info_bucket_from_record(row[0], payload)
                        line = _payload_to_building_info_line(payload, bucket)
                        if bucket in buckets:
                            buckets[bucket].append(line)
                        else:
                            buckets["single"].append(line)
                    if any(buckets.values()):
                        enriched = _attach_meta(
                            buckets,
                            conn=conn,
                            release_id=int(active_release["id"]),
                            candidate_pnu=candidate_pnu,
                        )
                        return pnu + json.dumps(enriched, ensure_ascii=False)
        except Exception:
            pass

    # The active release stores building register data in building_info_line (with
    # dataset_record retained as a compatibility path). Older deployments used four
    # *_line_v views, but those views are not part of the current schema. A miss in
    # the authoritative stores is therefore an empty result, not a legacy DB lookup.
    return None


def _land_info_rows_to_records(rows: list[tuple[Any, ...]]) -> list[Dict[str, Any]]:
    if not rows:
        return []
    schema_columns_by_id = _load_dataset_schema_columns(
        [int(row[1]) for row in rows if row[1] is not None]
    )
    return [
        {
            "dataset_code": row[0],
            "payload": _decode_dataset_payload(
                payload_raw=None,
                schema_id_raw=row[1],
                payload_values_raw=row[2],
                schema_columns_by_id=schema_columns_by_id,
            ),
            "geometry": None,
            "source_file": None,
            "row_no": None,
        }
        for row in rows
    ]


def _land_info_payload_pick(payload: dict[str, Any], keys: list[str], default: str = "") -> str:
    for key in keys:
        value = payload.get(key)
        text = str(value or "").strip()
        if text:
            return text
    return default


def _land_info_legacy_csv_value(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        text = default
    return text.replace("\r", " ").replace("\n", " ").replace(",", " ")


def _land_info_legacy_digits(value: Any, default: str = "0") -> str:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return digits or default


def _land_info_legacy_line(values: list[Any]) -> str:
    return ",".join(_land_info_legacy_csv_value(value) for value in values)


def _land_info_records_to_legacy_payload(records: list[Dict[str, Any]]) -> dict[str, list[str]]:
    legacy: dict[str, list[str]] = {
        "landCharacteristic": [],
        "landUse": [],
        "landPossession": [],
        "landMove": [],
    }
    for record in records:
        if not isinstance(record, dict):
            continue
        dataset_code = str(record.get("dataset_code") or "").strip().upper()
        payload_raw = record.get("payload")
        if not isinstance(payload_raw, dict):
            continue
        payload = {str(key): value for key, value in payload_raw.items()}

        if dataset_code == "AL_D195":
            legacy["landCharacteristic"].append(
                _land_info_legacy_line(
                    [
                        _land_info_payload_pick(payload, ["지목명", "지목"]),
                        _land_info_payload_pick(payload, ["토지면적", "대장면적", "면적"], "0"),
                        _land_info_legacy_digits(
                            _land_info_payload_pick(payload, ["공시지가", "공시지가(원/㎡)", "지가"]),
                        ),
                        _land_info_payload_pick(payload, ["토지이용상황"]),
                        _land_info_payload_pick(payload, ["지형높이"]),
                        _land_info_payload_pick(payload, ["지형형상"]),
                        _land_info_payload_pick(payload, ["도로접면"]),
                        _land_info_payload_pick(payload, ["용도지역명1", "용도지역지구명"]),
                        _land_info_payload_pick(payload, ["용도지역명2"]),
                        _land_info_legacy_digits(
                            _land_info_payload_pick(payload, ["기준년월", "등록일자", "데이터기준일자"]),
                        ),
                    ]
                )
            )
        elif dataset_code == "AL_D155":
            legacy["landUse"].append(
                _land_info_legacy_line(
                    [
                        _land_info_payload_pick(payload, ["저촉여부", "저촉여부코드"]),
                        _land_info_payload_pick(payload, ["용도지역지구코드"]),
                        _land_info_payload_pick(payload, ["용도지역지구명"]),
                        _land_info_payload_pick(payload, ["등록일자", "데이터기준일자"]),
                        _land_info_payload_pick(payload, ["비고내용"]),
                    ]
                )
            )
        elif dataset_code == "AL_D161":
            legacy["landPossession"].append(
                _land_info_legacy_line(
                    [
                        _land_info_payload_pick(payload, ["소유구분"]),
                        _land_info_payload_pick(payload, ["거주지구분"]),
                        _land_info_payload_pick(payload, ["소유권변동원인"]),
                        _land_info_payload_pick(payload, ["소유권변동일자"]),
                        _land_info_payload_pick(payload, ["공유인수"], "0"),
                        _land_info_payload_pick(payload, ["집합건물일련번호"], "0000"),
                        _land_info_payload_pick(payload, ["기준일", "데이터기준일자", "기준연월"]),
                    ]
                )
            )
        elif dataset_code == "AL_D157":
            legacy["landMove"].append(
                _land_info_legacy_line(
                    [
                        _land_info_payload_pick(payload, ["지목", "지목명"]),
                        _land_info_payload_pick(payload, ["토지이동사유"]),
                        _land_info_payload_pick(payload, ["토지이동일자"]),
                        _land_info_payload_pick(payload, ["토지이력순번", "토지이동이력순번"]),
                        _land_info_payload_pick(payload, ["데이터기준일자"]),
                    ]
                )
            )
    return legacy


def _fetch_land_info_component_records(pnu: str, limit: int) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(2000, int(limit)))
    pnu_candidates = _pnu_query_candidates(pnu)
    if not pnu_candidates:
        return []

    component_releases = _active_land_info_component_releases()
    monolith_release = _active_release("land_info")
    release_pairs: list[tuple[str, int]] = []
    for code in sorted(LAND_INFO_COMPONENTS):
        release = component_releases.get(code)
        if release and release.get("id") is not None:
            release_pairs.append((code, int(release["id"])))
        elif monolith_release and monolith_release.get("id") is not None:
            release_pairs.append((code, int(monolith_release["id"])))

    if not release_pairs and monolith_release and monolith_release.get("id") is not None:
        release_pairs = [("", int(monolith_release["id"]))]
    if not release_pairs:
        return []

    try:
        with psycopg.connect(_db_url()) as conn:
            land_rows: list[tuple[Any, ...]] = []
            for candidate_pnu in pnu_candidates:
                with conn.cursor() as cur:
                    if len(release_pairs) == 1 and not release_pairs[0][0]:
                        cur.execute(
                            """
                            SELECT dataset_code, schema_id, payload_values
                            FROM public.land_info_record
                            WHERE release_id = %s
                              AND pnu = %s
                            ORDER BY dataset_code, id
                            LIMIT %s
                            """,
                            (release_pairs[0][1], candidate_pnu, safe_limit),
                        )
                        land_rows = cur.fetchall()
                    else:
                        collected_rows: list[tuple[Any, ...]] = []
                        seen_ids: set[int] = set()
                        for dataset_code, release_id in release_pairs:
                            cur.execute(
                                """
                                SELECT id, dataset_code, schema_id, payload_values
                                FROM public.land_info_record
                                WHERE release_id = %s
                                  AND dataset_code = %s
                                  AND pnu = %s
                                ORDER BY id
                                LIMIT 1
                                """,
                                (release_id, dataset_code, candidate_pnu),
                            )
                            for row in cur.fetchall():
                                seen_ids.add(int(row[0]))
                                collected_rows.append((row[1], row[2], row[3]))
                        for dataset_code, release_id in release_pairs:
                            remaining = safe_limit - len(collected_rows)
                            if remaining <= 0:
                                break
                            cur.execute(
                                """
                                SELECT id, dataset_code, schema_id, payload_values
                                FROM public.land_info_record
                                WHERE release_id = %s
                                  AND dataset_code = %s
                                  AND pnu = %s
                                  AND NOT (id = ANY(%s))
                                ORDER BY id
                                LIMIT %s
                                """,
                                (release_id, dataset_code, candidate_pnu, sorted(seen_ids), remaining),
                            )
                            for row in cur.fetchall():
                                seen_ids.add(int(row[0]))
                                collected_rows.append((row[1], row[2], row[3]))
                        land_rows = collected_rows
                if land_rows:
                    break
    except Exception:
        land_rows = []

    return _land_info_rows_to_records(land_rows)


def _fetch_dataset_records(data_type: str, pnu: str, limit: int = 300) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(2000, int(limit)))
    normalized_type = _normalize_data_type(data_type)

    if normalized_type == "land_info":
        component_records = _fetch_land_info_component_records(pnu, safe_limit)
        if component_records:
            return component_records[:safe_limit]

    kv_payload = _fetch_dataset_pnu_kv_payload(normalized_type, pnu)
    kv_records = _normalize_pnu_kv_records(kv_payload)
    if kv_records:
        return kv_records[:safe_limit]

    active_release = _active_release(normalized_type)
    if not active_release:
        return []
    pnu_candidates = _pnu_query_candidates(pnu)
    if not pnu_candidates:
        return []

    try:
        with psycopg.connect(_db_url()) as conn:
            rows: list[tuple[Any, ...]] = []
            for candidate_pnu in pnu_candidates:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT dataset_code, payload, schema_id, payload_values, geometry, source_file, row_no
                        FROM dataset_record
                        WHERE release_id = %s
                          AND data_type = %s
                          AND pnu = %s
                        ORDER BY id
                        LIMIT %s
                        """,
                        (active_release["id"], normalized_type, candidate_pnu, safe_limit),
                    )
                    rows = cur.fetchall()
                if rows:
                    break
    except Exception:
        return []

    schema_columns_by_id = _load_dataset_schema_columns(
        [int(row[2]) for row in rows if row[2] is not None]
    )
    result: list[Dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "dataset_code": row[0],
                "payload": _decode_dataset_payload(
                    payload_raw=row[1],
                    schema_id_raw=row[2],
                    payload_values_raw=row[3],
                    schema_columns_by_id=schema_columns_by_id,
                ),
                "geometry": _decode_dataset_geometry(row[4]),
                "source_file": row[5],
                "row_no": row[6],
            }
        )
    return result


def _normalize_number_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        number = float(text)
    except Exception:
        return text
    normalized = f"{number:.6f}".rstrip("0").rstrip(".")
    return normalized or "0"


def _normalize_building_name(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace(" ", "").upper()
    text = text.replace("제", "")
    if text.endswith("동"):
        text = text[:-1]
    return text


def _building_info_candidates_from_lookup(pnu: str) -> list[dict[str, Any]]:
    active_release = _active_release("building_info")
    if not active_release:
        return []
    pnu_candidates = _pnu_query_candidates(pnu)
    if not pnu_candidates:
        return []

    try:
        with psycopg.connect(_db_url()) as conn:
            rows: list[tuple[Any, ...]] = []
            for candidate_pnu in pnu_candidates:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                          building_mgmt_pk,
                          COALESCE(building_name, ''),
                          COALESCE(building_name_norm, ''),
                          COALESCE(area_text, ''),
                          violation_raw,
                          is_violation
                        FROM building_info_lookup
                        WHERE release_id = %s
                          AND pnu = %s
                          AND dataset_code IN ('building_info_single', 'building_info_total')
                        ORDER BY id
                        LIMIT 5000
                        """,
                        (active_release["id"], candidate_pnu),
                    )
                    rows = cur.fetchall()
                if rows:
                    break
    except Exception:
        return []

    candidates: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for key, name, normalized_name, area, violation_raw, is_violation in rows:
        mgmt_key = str(key or "").strip()
        if not mgmt_key or mgmt_key in seen_keys:
            continue
        seen_keys.add(mgmt_key)
        candidates.append(
            {
                "key": mgmt_key,
                "name": str(name or "").strip(),
                "normalized_name": str(normalized_name or "").strip(),
                "area": _normalize_number_text(area),
                "violation_raw": str(violation_raw).strip() if violation_raw is not None else None,
                "is_violation": bool(is_violation) if isinstance(is_violation, bool) else None,
            }
        )
    return candidates


def _building_info_candidates_for_pnu(pnu: str) -> list[dict[str, Any]]:
    lookup_candidates = _building_info_candidates_from_lookup(pnu)
    if lookup_candidates:
        return lookup_candidates

    kv_payload = _fetch_dataset_pnu_kv_payload("building_info", pnu)
    if kv_payload is not None:
        buckets = _extract_building_info_buckets_from_kv_payload(kv_payload)
        candidates: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        lines = buckets.get("single")
        if isinstance(lines, list):
            for line in lines:
                if not isinstance(line, str):
                    continue
                fields = line.split("|")
                if not fields:
                    continue
                key = str(fields[0]).strip()
                if not key or key in seen_keys:
                    continue
                seen_keys.add(key)
                name = str(fields[2]).strip() if len(fields) > 2 else ""
                area = _normalize_number_text(fields[5] if len(fields) > 5 else "")
                candidates.append(
                    {
                        "key": key,
                        "name": name,
                        "normalized_name": _normalize_building_name(name),
                        "area": area,
                    }
                )

        if candidates and isinstance(kv_payload, dict):
            raw_map = _decode_json_or_gz_marker(
                kv_payload.get("violation_by_key")
                or kv_payload.get("violation_by_mgmt_pk")
                or kv_payload.get("violation_map")
            )
            flag_map = _decode_json_or_gz_marker(
                kv_payload.get("is_violation_by_key")
                or kv_payload.get("is_violation_by_mgmt_pk")
                or kv_payload.get("is_violation_map")
            )
            if isinstance(raw_map, dict) or isinstance(flag_map, dict):
                for cand in candidates:
                    cand_key = str(cand.get("key") or "").strip()
                    if not cand_key:
                        continue
                    violation_raw: str | None = None
                    if isinstance(raw_map, dict) and cand_key in raw_map:
                        raw_value = raw_map.get(cand_key)
                        if raw_value is not None:
                            raw_text = str(raw_value).strip()
                            violation_raw = raw_text if raw_text else None
                    is_violation: bool | None = None
                    if isinstance(flag_map, dict) and cand_key in flag_map:
                        flag_value = flag_map.get(cand_key)
                        if isinstance(flag_value, bool):
                            is_violation = bool(flag_value)
                        elif isinstance(flag_value, (int, float)):
                            is_violation = bool(flag_value)
                        elif isinstance(flag_value, str):
                            is_violation = _violation_flag(flag_value)
                    if violation_raw is not None:
                        cand["violation_raw"] = violation_raw
                    if is_violation is None and violation_raw is not None:
                        is_violation = _violation_flag(violation_raw)
                    if is_violation is not None:
                        cand["is_violation"] = bool(is_violation)

        if candidates:
            return candidates

    blob = _fetch_building_info_line(pnu)
    if not blob or not blob.startswith(pnu):
        return []

    payload_text = blob[len(pnu) :]
    try:
        payload_obj = json.loads(payload_text)
    except Exception:
        return []

    if not isinstance(payload_obj, dict):
        return []

    candidates: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for bucket_name in ("single",):
        lines = payload_obj.get(bucket_name)
        if not isinstance(lines, list):
            continue
        for line in lines:
            if not isinstance(line, str):
                continue
            fields = line.split("|")
            if not fields:
                continue
            key = str(fields[0]).strip()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            name = str(fields[2]).strip() if len(fields) > 2 else ""
            area = _normalize_number_text(fields[5] if len(fields) > 5 else "")
            candidates.append(
                {
                    "key": key,
                    "name": name,
                    "normalized_name": _normalize_building_name(name),
                    "area": area,
                }
            )
    return candidates


def _match_building_info_key(payload: dict[str, Any], candidates: list[dict[str, Any]]) -> str | None:
    if not candidates:
        return None

    names: list[str] = []
    for key in ("A24", "건물명", "A25", "동명"):
        value = payload.get(key)
        if value is None:
            continue
        name = str(value).strip()
        if name and name not in names:
            names.append(name)

    area = _normalize_number_text(payload.get("A12") or payload.get("건축면적") or payload.get("면적"))

    for name in names:
        name_matches = [c for c in candidates if c["name"] and c["name"] == name]
        if len(name_matches) == 1:
            return name_matches[0]["key"]
        if area:
            named_area_matches = [c for c in name_matches if c["area"] and c["area"] == area]
            if len(named_area_matches) == 1:
                return named_area_matches[0]["key"]

    for name in names:
        normalized_name = _normalize_building_name(name)
        if not normalized_name:
            continue
        name_matches = [
            c
            for c in candidates
            if c.get("normalized_name") and c.get("normalized_name") == normalized_name
        ]
        if len(name_matches) == 1:
            return name_matches[0]["key"]
        if area:
            named_area_matches = [c for c in name_matches if c["area"] and c["area"] == area]
            if len(named_area_matches) == 1:
                return named_area_matches[0]["key"]

    if area:
        area_matches = [c for c in candidates if c["area"] and c["area"] == area]
        if len(area_matches) == 1:
            return area_matches[0]["key"]

    sequence = str(payload.get("A19") or payload.get("건물관리번호") or "").strip()
    if sequence:
        sequence_matches = [c for c in candidates if c["key"].endswith(sequence)]
        if len(sequence_matches) == 1:
            return sequence_matches[0]["key"]

    if len(candidates) == 1:
        return candidates[0]["key"]
    return None


def _violation_raw(payload: dict[str, Any]) -> str | None:
    keys = ("A20", "위반건축물여부", "위반여부", "violation")
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _violation_flag(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if normalized in {"Y", "1", "TRUE", "T"}:
        return True
    if normalized in {"N", "0", "FALSE", "F"}:
        return False
    return None


def _fetch_building_geo_with_violation(
    pnu: str,
    limit: int = 300,
) -> list[Dict[str, Any]]:
    building_info_candidates = _building_info_candidates_for_pnu(pnu)
    candidates_by_key: dict[str, dict[str, Any]] = {
        str(item.get("key")): item for item in building_info_candidates if item.get("key")
    }

    safe_limit = max(1, min(2000, int(limit)))
    rows = _fetch_dataset_records("building_integrated_info", pnu, limit=safe_limit)
    if not rows:
        return []

    items: list[Dict[str, Any]] = []
    for row in rows:
        payload_obj = row.get("payload")
        if not isinstance(payload_obj, dict):
            continue
        decoded_geometry = row.get("geometry")
        if decoded_geometry is None:
            continue
        dataset_code = row.get("dataset_code")
        source_file = row.get("source_file")
        row_no = row.get("row_no")
        raw_violation = _violation_raw(payload_obj)
        original_building_id = payload_obj.get("A1") or payload_obj.get("GIS건물통합식별번호")
        legacy_building_id = _match_building_info_key(payload_obj, building_info_candidates)
        candidate = candidates_by_key.get(str(legacy_building_id)) if legacy_building_id else None
        if raw_violation is None and candidate is not None:
            if candidate.get("violation_raw"):
                raw_violation = str(candidate.get("violation_raw"))
            elif isinstance(candidate.get("is_violation"), bool):
                raw_violation = "Y" if bool(candidate.get("is_violation")) else "N"
        flag = _violation_flag(raw_violation)
        if flag is None and candidate is not None and isinstance(candidate.get("is_violation"), bool):
            flag = bool(candidate.get("is_violation"))
        items.append(
            {
                "dataset_code": dataset_code,
                "geometry": decoded_geometry,
                "source_file": source_file,
                "row_no": row_no,
                "building_id": original_building_id,
                "building_legacy_id": legacy_building_id,
                "building_name": payload_obj.get("A24")
                or payload_obj.get("건물명")
                or payload_obj.get("A25")
                or payload_obj.get("동명"),
                "violation": {
                    "raw": raw_violation,
                    "is_violation": flag,
                },
            }
        )

    return items


def _geometry_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float] | None:
    surfaces = _polygon_surfaces(geometry)
    if not surfaces:
        return None

    min_lon: float | None = None
    max_lon: float | None = None
    min_lat: float | None = None
    max_lat: float | None = None

    for polygon in surfaces:
        for ring in polygon:
            if not isinstance(ring, list):
                continue
            for point in ring:
                if not isinstance(point, (list, tuple)) or len(point) < 2:
                    continue
                lon = _to_float(point[0])
                lat = _to_float(point[1])
                if lon is None or lat is None:
                    continue
                min_lon = lon if min_lon is None else min(min_lon, lon)
                max_lon = lon if max_lon is None else max(max_lon, lon)
                min_lat = lat if min_lat is None else min(min_lat, lat)
                max_lat = lat if max_lat is None else max(max_lat, lat)

    if min_lon is None or max_lon is None or min_lat is None or max_lat is None:
        return None
    return min_lon, max_lon, min_lat, max_lat


def _expand_bbox_for_surroundings(
    bbox: tuple[float, float, float, float],
    *,
    padding_ratio: float = 0.35,
) -> tuple[float, float, float, float] | None:
    min_lon, max_lon, min_lat, max_lat = bbox
    safe_ratio = _to_float(padding_ratio)
    if safe_ratio is None:
        safe_ratio = 0.35
    safe_ratio = max(0.05, min(20.0, safe_ratio))

    min_padding_deg = _to_float(os.getenv("CADASTRAL_GEO_SURROUNDINGS_MIN_PADDING_DEG", "0.00025"))
    if min_padding_deg is None:
        min_padding_deg = 0.00025
    min_padding_deg = max(0.0, min_padding_deg)

    lon_span = max(0.0, max_lon - min_lon)
    lat_span = max(0.0, max_lat - min_lat)
    major_span = max(lon_span, lat_span)

    # 세로/가로 편향이 큰 필지에서도 좁은 축의 주변 지적이 비지 않도록 최소 축 비율 보정
    minor_axis_ratio = _to_float(os.getenv("CADASTRAL_GEO_SURROUNDINGS_MINOR_AXIS_RATIO", "0.30"))
    if minor_axis_ratio is None:
        minor_axis_ratio = 0.30
    minor_axis_ratio = max(0.0, min(1.0, minor_axis_ratio))

    effective_lon_span = max(lon_span, major_span * minor_axis_ratio)
    effective_lat_span = max(lat_span, major_span * minor_axis_ratio)

    lon_pad = max(min_padding_deg, effective_lon_span * safe_ratio)
    lat_pad = max(min_padding_deg, effective_lat_span * safe_ratio)
    return _normalize_bbox(
        min_lon - lon_pad,
        max_lon + lon_pad,
        min_lat - lat_pad,
        max_lat + lat_pad,
    )


def _geometry_polylabel(
    geometry: dict[str, Any],
    *,
    pre_label_lon: float | None = None,
    pre_label_lat: float | None = None,
) -> dict[str, float] | None:
    surfaces = _polygon_surfaces(geometry)
    if not surfaces:
        return None

    best_label: dict[str, float] | None = None
    best_score = float("-inf")

    for surface in surfaces:
        polygon: list[list[tuple[float, float]]] = []
        for ring in surface:
            if not isinstance(ring, list):
                continue
            points: list[tuple[float, float]] = []
            for point in ring:
                if not isinstance(point, (list, tuple)) or len(point) < 2:
                    continue
                lon = _to_float(point[0])
                lat = _to_float(point[1])
                if lon is None or lat is None:
                    continue
                points.append((lon, lat))
            core = _ring_core_points(points)
            if len(core) < 3:
                continue
            polygon.append(core)
        if not polygon:
            continue

        outer = polygon[0]
        outer_box = _ring_bbox(outer)
        if outer_box is None:
            continue

        cx = 0.0
        cy = 0.0
        radius = float("-inf")

        if isinstance(pre_label_lon, float) and isinstance(pre_label_lat, float):
            pre_radius = _point_to_polygon_signed_distance(pre_label_lon, pre_label_lat, polygon)
            if pre_radius > radius:
                cx = pre_label_lon
                cy = pre_label_lat
                radius = pre_radius

        centroid = _ring_centroid(outer)
        if centroid is not None:
            c_radius = _point_to_polygon_signed_distance(centroid[0], centroid[1], polygon)
            if c_radius > radius:
                cx = centroid[0]
                cy = centroid[1]
                radius = c_radius

        box_cx = (outer_box[0] + outer_box[2]) * 0.5
        box_cy = (outer_box[1] + outer_box[3]) * 0.5
        box_radius = _point_to_polygon_signed_distance(box_cx, box_cy, polygon)
        if box_radius > radius:
            cx = box_cx
            cy = box_cy
            radius = box_radius

        area = abs(_ring_signed_area(outer))
        score = area + (max(0.0, radius) * 8.0)
        if score > best_score:
            best_score = score
            best_label = {
                "lon": cx,
                "lat": cy,
                "distance": max(0.0, radius),
            }

    return best_label


def _fetch_cadastral_geo_items(
    pnu: str,
    limit: int = 300,
    *,
    include_surroundings: bool = False,
    surroundings_padding_ratio: float = 0.35,
) -> list[Dict[str, Any]]:
    table_name = os.getenv("CADASTRAL_TILE_TABLE", "cadastral_features")
    geojson_col = os.getenv("CADASTRAL_TILE_GEOJSON_COL", "geojson")
    label_col = os.getenv("CADASTRAL_TILE_LABEL_COL", "label")
    pnu_col = os.getenv("CADASTRAL_TILE_PNU_COL", "pnu")
    label_lon_col = os.getenv("CADASTRAL_TILE_LABEL_LON_COL", "label_lon")
    label_lat_col = os.getenv("CADASTRAL_TILE_LABEL_LAT_COL", "label_lat")
    min_lon_col = os.getenv("CADASTRAL_TILE_MIN_LON_COL", "bbox_min_lon")
    max_lon_col = os.getenv("CADASTRAL_TILE_MAX_LON_COL", "bbox_max_lon")
    min_lat_col = os.getenv("CADASTRAL_TILE_MIN_LAT_COL", "bbox_min_lat")
    max_lat_col = os.getenv("CADASTRAL_TILE_MAX_LAT_COL", "bbox_max_lat")
    release_col = os.getenv("CADASTRAL_TILE_RELEASE_COL", "release_id")
    active_release = _active_release("cadastral")
    safe_limit = max(1, min(6000, int(limit)))
    pnu_candidates = _pnu_query_candidates(pnu)
    if not pnu_candidates:
        return []

    def _query(with_release_filter: bool, candidate_pnu: str) -> list[tuple[Any, Any, Any, Any, Any]]:
        clauses = [sql.SQL("{pnu_col} = %s").format(pnu_col=sql.Identifier(pnu_col))]
        params: list[Any] = [candidate_pnu]

        if with_release_filter and active_release:
            clauses.append(
                sql.SQL("{release_col} = %s").format(release_col=sql.Identifier(release_col))
            )
            params.append(active_release["id"])

        params.append(safe_limit)

        query = (
            sql.SQL(
                "SELECT {geojson_col}, COALESCE({label_col}::text, ''), COALESCE({pnu_col}::text, ''), "
                "{label_lon_col}, {label_lat_col} "
            ).format(
                geojson_col=sql.Identifier(geojson_col),
                label_col=sql.Identifier(label_col),
                pnu_col=sql.Identifier(pnu_col),
                label_lon_col=sql.Identifier(label_lon_col),
                label_lat_col=sql.Identifier(label_lat_col),
            )
            + sql.SQL("FROM {table_name} WHERE ").format(table_name=sql.Identifier(table_name))
            + sql.SQL(" AND ").join(clauses)
            + sql.SQL(" LIMIT %s")
        )
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def _query_surroundings(
        with_release_filter: bool,
        candidate_pnu: str,
        *,
        bbox: tuple[float, float, float, float],
    ) -> list[tuple[Any, Any, Any, Any, Any]]:
        min_lon, max_lon, min_lat, max_lat = bbox
        center_lon = (min_lon + max_lon) / 2.0
        center_lat = (min_lat + max_lat) / 2.0
        fetch_limit = max(200, min(12000, safe_limit * 4))

        clauses = [
            sql.SQL("{max_lon_col} >= %s").format(max_lon_col=sql.Identifier(max_lon_col)),
            sql.SQL("{min_lon_col} <= %s").format(min_lon_col=sql.Identifier(min_lon_col)),
            sql.SQL("{max_lat_col} >= %s").format(max_lat_col=sql.Identifier(max_lat_col)),
            sql.SQL("{min_lat_col} <= %s").format(min_lat_col=sql.Identifier(min_lat_col)),
            sql.SQL("{pnu_col} <> %s").format(pnu_col=sql.Identifier(pnu_col)),
        ]
        params: list[Any] = [min_lon, max_lon, min_lat, max_lat, candidate_pnu]

        if with_release_filter and active_release:
            clauses.append(
                sql.SQL("{release_col} = %s").format(release_col=sql.Identifier(release_col))
            )
            params.append(active_release["id"])

        params.extend([center_lon, center_lat, fetch_limit])
        query = (
            sql.SQL(
                "SELECT {geojson_col}, COALESCE({label_col}::text, ''), COALESCE({pnu_col}::text, ''), "
                "{label_lon_col}, {label_lat_col} "
            ).format(
                geojson_col=sql.Identifier(geojson_col),
                label_col=sql.Identifier(label_col),
                pnu_col=sql.Identifier(pnu_col),
                label_lon_col=sql.Identifier(label_lon_col),
                label_lat_col=sql.Identifier(label_lat_col),
            )
            + sql.SQL("FROM {table_name} WHERE ").format(table_name=sql.Identifier(table_name))
            + sql.SQL(" AND ").join(clauses)
            + sql.SQL(" ORDER BY ")
            + sql.SQL(
                "ABS((({min_lon_col} + {max_lon_col}) / 2.0) - %s) + "
                "ABS((({min_lat_col} + {max_lat_col}) / 2.0) - %s)"
            ).format(
                min_lon_col=sql.Identifier(min_lon_col),
                max_lon_col=sql.Identifier(max_lon_col),
                min_lat_col=sql.Identifier(min_lat_col),
                max_lat_col=sql.Identifier(max_lat_col),
            )
            + sql.SQL(" LIMIT %s")
        )
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    rows: list[tuple[Any, Any, Any, Any, Any]] = []
    matched_candidate_pnu = ""
    for candidate_pnu in pnu_candidates:
        try:
            rows = _query(with_release_filter=True, candidate_pnu=candidate_pnu)
        except Exception:
            try:
                rows = _query(with_release_filter=False, candidate_pnu=candidate_pnu)
            except Exception:
                rows = []
        if rows:
            matched_candidate_pnu = candidate_pnu
            break

    items: list[Dict[str, Any]] = []
    seen_keys: set[str] = set()

    def _item_key(item_pnu: str, geometry_obj: dict[str, Any]) -> str:
        try:
            geometry_key = json.dumps(geometry_obj, ensure_ascii=False, sort_keys=True)
        except Exception:
            geometry_key = str(geometry_obj)
        return f"{item_pnu}|{geometry_key}"

    for geojson_raw, label_raw, pnu_raw, label_lon_raw, label_lat_raw in rows:
        geometry = _safe_json_loads(geojson_raw)
        if not geometry:
            continue
        label_lon = _to_float(label_lon_raw)
        label_lat = _to_float(label_lat_raw)
        polylabel = _geometry_polylabel(
            geometry,
            pre_label_lon=label_lon,
            pre_label_lat=label_lat,
        )
        item_pnu = str(pnu_raw or pnu)
        key = _item_key(item_pnu, geometry)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        items.append(
            {
                "pnu": item_pnu,
                "label": _label_text(label_raw, item_pnu),
                "geometry": geometry,
                "label_lon": label_lon,
                "label_lat": label_lat,
                "polylabel": polylabel,
                "source": table_name,
            }
        )

    if not include_surroundings or not items or len(items) >= safe_limit:
        return items[:safe_limit]

    target_bboxes = []
    for item in items:
        geometry_obj = item.get("geometry")
        if not isinstance(geometry_obj, dict):
            continue
        bbox = _geometry_bbox(geometry_obj)
        if bbox is not None:
            target_bboxes.append(bbox)

    if not target_bboxes:
        return items[:safe_limit]

    merged_bbox = (
        min(b[0] for b in target_bboxes),
        max(b[1] for b in target_bboxes),
        min(b[2] for b in target_bboxes),
        max(b[3] for b in target_bboxes),
    )
    expanded_bbox = _expand_bbox_for_surroundings(
        merged_bbox,
        padding_ratio=surroundings_padding_ratio,
    )
    if expanded_bbox is None:
        return items[:safe_limit]

    candidate_pnu = matched_candidate_pnu or pnu_candidates[0]
    try:
        surrounding_rows = _query_surroundings(
            with_release_filter=True,
            candidate_pnu=candidate_pnu,
            bbox=expanded_bbox,
        )
    except Exception:
        try:
            surrounding_rows = _query_surroundings(
                with_release_filter=False,
                candidate_pnu=candidate_pnu,
                bbox=expanded_bbox,
            )
        except Exception:
            surrounding_rows = []

    for geojson_raw, label_raw, pnu_raw, label_lon_raw, label_lat_raw in surrounding_rows:
        if len(items) >= safe_limit:
            break
        geometry = _safe_json_loads(geojson_raw)
        if not geometry:
            continue
        label_lon = _to_float(label_lon_raw)
        label_lat = _to_float(label_lat_raw)
        polylabel = _geometry_polylabel(
            geometry,
            pre_label_lon=label_lon,
            pre_label_lat=label_lat,
        )
        item_pnu = str(pnu_raw or pnu)
        key = _item_key(item_pnu, geometry)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        items.append(
            {
                "pnu": item_pnu,
                "label": _label_text(label_raw, item_pnu),
                "geometry": geometry,
                "label_lon": label_lon,
                "label_lat": label_lat,
                "polylabel": polylabel,
                "source": table_name,
            }
        )
    return items[:safe_limit]


def _fetch_cadastral_geo_items_by_bounds(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    *,
    limit: int = 3000,
    zoom: int = 18,
) -> list[Dict[str, Any]]:
    normalized_bbox = _normalize_bbox(min_lon, max_lon, min_lat, max_lat)
    if normalized_bbox is None:
        return []

    table_name = os.getenv("CADASTRAL_TILE_TABLE", "cadastral_features")
    geojson_col = os.getenv("CADASTRAL_TILE_GEOJSON_COL", "geojson")
    label_col = os.getenv("CADASTRAL_TILE_LABEL_COL", "label")
    pnu_col = os.getenv("CADASTRAL_TILE_PNU_COL", "pnu")
    label_lon_col = os.getenv("CADASTRAL_TILE_LABEL_LON_COL", "label_lon")
    label_lat_col = os.getenv("CADASTRAL_TILE_LABEL_LAT_COL", "label_lat")
    min_lon_col = os.getenv("CADASTRAL_TILE_MIN_LON_COL", "bbox_min_lon")
    max_lon_col = os.getenv("CADASTRAL_TILE_MAX_LON_COL", "bbox_max_lon")
    min_lat_col = os.getenv("CADASTRAL_TILE_MIN_LAT_COL", "bbox_min_lat")
    max_lat_col = os.getenv("CADASTRAL_TILE_MAX_LAT_COL", "bbox_max_lat")
    release_col = os.getenv("CADASTRAL_TILE_RELEASE_COL", "release_id")
    active_release = _active_release("cadastral")
    safe_limit = max(1, min(6000, int(limit)))
    safe_zoom = max(0, min(22, int(zoom)))

    bbox_pad = _to_float(os.getenv("CADASTRAL_GEO_BOUNDS_EDGE_PAD_DEG", "0.00001"))
    if bbox_pad is None:
        bbox_pad = 0.00001
    bbox_pad = max(0.0, bbox_pad)
    if safe_zoom <= 16:
        bbox_pad = max(bbox_pad, 0.00003)

    expanded_bbox = _normalize_bbox(
        normalized_bbox[0] - bbox_pad,
        normalized_bbox[1] + bbox_pad,
        normalized_bbox[2] - bbox_pad,
        normalized_bbox[3] + bbox_pad,
    )
    if expanded_bbox is None:
        return []

    min_lon, max_lon, min_lat, max_lat = expanded_bbox
    center_lon = (min_lon + max_lon) / 2.0
    center_lat = (min_lat + max_lat) / 2.0

    def _query(with_release_filter: bool) -> list[tuple[Any, Any, Any, Any, Any]]:
        clauses = [
            sql.SQL("{max_lon_col} >= %s").format(max_lon_col=sql.Identifier(max_lon_col)),
            sql.SQL("{min_lon_col} <= %s").format(min_lon_col=sql.Identifier(min_lon_col)),
            sql.SQL("{max_lat_col} >= %s").format(max_lat_col=sql.Identifier(max_lat_col)),
            sql.SQL("{min_lat_col} <= %s").format(min_lat_col=sql.Identifier(min_lat_col)),
        ]
        params: list[Any] = [min_lon, max_lon, min_lat, max_lat]

        if with_release_filter and active_release:
            clauses.append(
                sql.SQL("{release_col} = %s").format(release_col=sql.Identifier(release_col))
            )
            params.append(active_release["id"])

        params.extend([center_lon, center_lat, safe_limit])
        query = (
            sql.SQL(
                "SELECT {geojson_col}, COALESCE({label_col}::text, ''), COALESCE({pnu_col}::text, ''), "
                "{label_lon_col}, {label_lat_col} "
            ).format(
                geojson_col=sql.Identifier(geojson_col),
                label_col=sql.Identifier(label_col),
                pnu_col=sql.Identifier(pnu_col),
                label_lon_col=sql.Identifier(label_lon_col),
                label_lat_col=sql.Identifier(label_lat_col),
            )
            + sql.SQL("FROM {table_name} WHERE ").format(table_name=sql.Identifier(table_name))
            + sql.SQL(" AND ").join(clauses)
            + sql.SQL(" ORDER BY ")
            + sql.SQL(
                "ABS((({min_lon_col} + {max_lon_col}) / 2.0) - %s) + "
                "ABS((({min_lat_col} + {max_lat_col}) / 2.0) - %s), {pnu_col}"
            ).format(
                min_lon_col=sql.Identifier(min_lon_col),
                max_lon_col=sql.Identifier(max_lon_col),
                min_lat_col=sql.Identifier(min_lat_col),
                max_lat_col=sql.Identifier(max_lat_col),
                pnu_col=sql.Identifier(pnu_col),
            )
            + sql.SQL(" LIMIT %s")
        )
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    try:
        rows = _query(with_release_filter=True)
    except Exception:
        try:
            rows = _query(with_release_filter=False)
        except Exception:
            rows = []

    items: list[Dict[str, Any]] = []
    seen_keys: set[str] = set()

    def _item_key(item_pnu: str, geometry_obj: dict[str, Any]) -> str:
        try:
            geometry_key = json.dumps(geometry_obj, ensure_ascii=False, sort_keys=True)
        except Exception:
            geometry_key = str(geometry_obj)
        return f"{item_pnu}|{geometry_key}"

    for geojson_raw, label_raw, pnu_raw, label_lon_raw, label_lat_raw in rows:
        geometry = _safe_json_loads(geojson_raw)
        if not geometry:
            continue
        label_lon = _to_float(label_lon_raw)
        label_lat = _to_float(label_lat_raw)
        polylabel = _geometry_polylabel(
            geometry,
            pre_label_lon=label_lon,
            pre_label_lat=label_lat,
        )
        item_pnu = str(pnu_raw or "")
        key = _item_key(item_pnu, geometry)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        items.append(
            {
                "pnu": item_pnu,
                "label": _label_text(label_raw, item_pnu),
                "geometry": geometry,
                "label_lon": label_lon,
                "label_lat": label_lat,
                "polylabel": polylabel,
                "source": table_name,
            }
        )

    return items[:safe_limit]


def _find_open_import_job_for_data_type(
    conn: psycopg.Connection,
    data_type: str,
    exclude_job_id: int | None = None,
) -> tuple[Any, ...] | None:
    normalized_type = _normalize_data_type(data_type)
    params: list[Any] = [normalized_type]
    exclude_sql = ""
    if exclude_job_id is not None:
        exclude_sql = " AND j.id <> %s"
        params.append(int(exclude_job_id))

    query = f"""
        SELECT
          j.id,
          j.status,
          j.release_id,
          COALESCE(j.data_type, r.data_type, 'cadastral') AS data_type
        FROM cadastral_import_job j
        LEFT JOIN cadastral_release r ON r.id = j.release_id
        WHERE COALESCE(j.data_type, r.data_type, 'cadastral') = %s
          AND j.status IN ('QUEUED', 'RUNNING')
          {exclude_sql}
        ORDER BY CASE WHEN j.status = 'RUNNING' THEN 0 ELSE 1 END, j.id DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def _ensure_no_open_import_job_for_data_type(
    conn: psycopg.Connection,
    data_type: str,
    exclude_job_id: int | None = None,
) -> None:
    row = _find_open_import_job_for_data_type(
        conn,
        data_type=data_type,
        exclude_job_id=exclude_job_id,
    )
    if not row:
        return

    job_id, status, release_id, normalized_type = row
    raise HTTPException(
        status_code=409,
        detail=(
            f"동일 데이터 유형 작업이 이미 진행 중입니다: "
            f"data_type={normalized_type}, job_id={job_id}, release_id={release_id}, status={status}"
        ),
    )


def _release_row_to_dict(row: tuple[Any, ...]) -> Dict[str, Any]:
    return {
        "id": row[0],
        "version": row[1],
        "data_type": row[2] if len(row) > 10 else "cadastral",
        "source_name": row[3] if len(row) > 10 else row[2],
        "status": row[4] if len(row) > 10 else row[3],
        "is_active": row[5] if len(row) > 10 else row[4],
        "records_count": row[6] if len(row) > 10 else row[5],
        "metadata": (row[7] if len(row) > 10 else row[6]) or {},
        "created_at": row[8] if len(row) > 10 else row[7],
        "updated_at": row[9] if len(row) > 10 else row[8],
        "activated_at": row[10] if len(row) > 10 else row[9],
    }


def _import_job_row_to_dict(row: tuple[Any, ...]) -> Dict[str, Any]:
    return {
        "id": row[0],
        "release_id": row[1],
        "release_version": row[2],
        "status": row[3],
        "source_path": row[4],
        "total_files": row[5],
        "processed_files": row[6],
        "inserted_rows": row[7],
        "error_message": row[8],
        "created_at": row[9],
        "started_at": row[10],
        "finished_at": row[11],
        "updated_at": row[12],
        "data_type": row[13] if len(row) > 13 else "cadastral",
    }


def _import_job_worker_row_to_dict(row: tuple[Any, ...]) -> Dict[str, Any]:
    return {
        "id": row[0],
        "job_id": row[1],
        "release_id": row[2],
        "release_version": row[3],
        "data_type": row[4],
        "source_file": row[5],
        "worker_name": row[6],
        "status": row[7],
        "processed_rows": row[8],
        "error_message": row[9],
        "started_at": row[10],
        "finished_at": row[11],
        "updated_at": row[12],
    }


def _update_file_row_to_dict(row: tuple[Any, ...]) -> Dict[str, Any]:
    return {
        "id": row[0],
        "release_id": row[1],
        "release_version": row[2],
        "data_type": row[3],
        "file_name": row[4],
        "file_size": int(row[5] or 0),
        "created_at": row[6],
        "release_is_active": bool(row[7]) if len(row) > 7 else False,
        "release_status": row[8] if len(row) > 8 else None,
    }


def _load_recent_update_file_rows(
    conn: psycopg.Connection,
    *,
    data_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> list[tuple[Any, ...]]:
    clauses: list[str] = ["COALESCE(r.metadata ->> 'operation_mode', '') = 'update'"]
    params: list[Any] = []

    if data_type is not None:
        clauses.append("COALESCE(f.data_type, r.data_type, 'cadastral') = %s")
        params.append(str(data_type))

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT
          f.id,
          f.release_id,
          r.version,
          COALESCE(f.data_type, r.data_type, 'cadastral') AS data_type,
          f.file_name,
          f.file_size,
          f.created_at,
          r.is_active,
          r.status
        FROM dataset_import_file f
        LEFT JOIN cadastral_release r ON r.id = f.release_id
        {where_clause}
        ORDER BY f.id DESC
        LIMIT %s OFFSET %s
    """
    params.extend([int(limit), int(offset)])

    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _admin_cadastral_snapshot(
    release_limit: int = 100,
    job_limit: int = 200,
    worker_limit: int = 600,
) -> Dict[str, Any]:
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  id, version, data_type, source_name, status, is_active, records_count,
                  metadata, created_at, updated_at, activated_at
                FROM cadastral_release
                ORDER BY id DESC
                LIMIT %s
                """,
                (release_limit,),
            )
            release_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                  j.id, j.release_id, r.version, j.status, j.source_path,
                  j.total_files, j.processed_files, j.inserted_rows, j.error_message,
                  j.created_at, j.started_at, j.finished_at, j.updated_at,
                  COALESCE(j.data_type, r.data_type, 'cadastral') AS data_type
                FROM cadastral_import_job j
                LEFT JOIN cadastral_release r ON r.id = j.release_id
                ORDER BY j.id DESC
                LIMIT %s
                """,
                (job_limit,),
            )
            job_rows = cur.fetchall()
            job_ids = [int(row[0]) for row in job_rows if row and row[0] is not None]

            worker_rows: list[tuple[Any, ...]] = []
            if job_ids:
                try:
                    cur.execute(
                        """
                        SELECT
                          w.id,
                          w.job_id,
                          j.release_id,
                          r.version,
                          COALESCE(j.data_type, r.data_type, 'cadastral') AS data_type,
                          w.source_file,
                          w.worker_name,
                          w.status,
                          w.processed_rows,
                          w.error_message,
                          w.started_at,
                          w.finished_at,
                          w.updated_at
                        FROM cadastral_import_job_worker w
                        JOIN cadastral_import_job j ON j.id = w.job_id
                        LEFT JOIN cadastral_release r ON r.id = j.release_id
                        WHERE w.job_id = ANY(%s)
                        ORDER BY w.job_id DESC, w.id ASC
                        LIMIT %s
                        """,
                        (job_ids, worker_limit),
                    )
                    worker_rows = cur.fetchall()
                except Exception:
                    worker_rows = []

            update_file_rows: list[tuple[Any, ...]] = []
            try:
                update_file_rows = _load_recent_update_file_rows(
                    conn,
                    limit=200,
                    offset=0,
                )
            except Exception:
                update_file_rows = []

    return {
        "tile_config": tile_config()["data"],
        "releases": [_release_row_to_dict(row) for row in release_rows],
        "jobs": [_import_job_row_to_dict(row) for row in job_rows],
        "job_workers": [_import_job_worker_row_to_dict(row) for row in worker_rows],
        "update_files": [_update_file_row_to_dict(row) for row in update_file_rows],
    }


async def _register_import_runner(job_id: int, task: asyncio.Task[None]) -> None:
    async with _IMPORT_RUNNERS_GUARD:
        _IMPORT_RUNNERS[job_id] = task


async def _register_import_process(job_id: int, process: asyncio.subprocess.Process) -> None:
    async with _IMPORT_PROCESSES_GUARD:
        _IMPORT_PROCESSES[job_id] = process


async def _pop_import_runner(job_id: int) -> None:
    async with _IMPORT_RUNNERS_GUARD:
        _IMPORT_RUNNERS.pop(job_id, None)


async def _pop_import_process(job_id: int) -> asyncio.subprocess.Process | None:
    async with _IMPORT_PROCESSES_GUARD:
        return _IMPORT_PROCESSES.pop(job_id, None)


async def _has_import_runner(job_id: int) -> bool:
    async with _IMPORT_RUNNERS_GUARD:
        task = _IMPORT_RUNNERS.get(job_id)
        return task is not None and not task.done()


async def _cancel_import_job_runner(job_id: int) -> None:
    process: asyncio.subprocess.Process | None = None
    async with _IMPORT_PROCESSES_GUARD:
        process = _IMPORT_PROCESSES.get(job_id)

    if process is not None and process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except Exception:
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                with contextlib.suppress(Exception):
                    await process.wait()

    task: asyncio.Task[None] | None = None
    async with _IMPORT_RUNNERS_GUARD:
        task = _IMPORT_RUNNERS.get(job_id)

    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


def _cleanup_old_releases_after_success(release_id: int, data_type: str) -> tuple[list[int], str | None]:
    normalized_type = _normalize_data_type(data_type)
    deleted_ids: list[int] = []
    skip_reason: str | None = None
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT is_active
                        FROM cadastral_release
                        WHERE id = %s
                          AND data_type = %s
                        """,
                        (release_id, normalized_type),
                    )
                    row = cur.fetchone()
                    if not row:
                        return [], "release not found"
                    if not bool(row[0]):
                        return [], "target release is not active"
                    cur.execute(
                        """
                        SELECT id
                        FROM cadastral_release
                        WHERE data_type = %s
                          AND id <> %s
                        ORDER BY id
                        """,
                        (normalized_type, release_id),
                    )
                    old_release_ids = [int(item[0]) for item in cur.fetchall()]
                    for old_release_id in old_release_ids:
                        try:
                            cur.execute("SELECT drop_dataset_record_partition(%s)", (old_release_id,))
                        except Exception:
                            continue
                    cur.execute(
                        """
                        DELETE FROM cadastral_release
                        WHERE data_type = %s
                          AND id <> %s
                        RETURNING id
                        """,
                        (normalized_type, release_id),
                    )
                    deleted_ids = [int(item[0]) for item in cur.fetchall()]
    except Exception as exc:
        return [], f"cleanup failed: {exc}"

    _clear_active_release_cache(normalized_type)
    if normalized_type == "cadastral":
        _tile_memory_cache().clear()
        tile_cache_dir = Path(os.getenv("TILE_CACHE_DIR", "/data/uploads/tile_cache")) / "cadastral"
        if tile_cache_dir.exists():
            try:
                shutil.rmtree(tile_cache_dir)
            except Exception:
                skip_reason = "tile cache cleanup failed"
    return deleted_ids, skip_reason


def _auto_drop_failed_release_partition_enabled() -> bool:
    # Keep storage bounded by default: drop non-active failed/cancelled release partitions automatically.
    return _to_bool(os.getenv("IMPORT_AUTO_DROP_FAILED_RELEASE_PARTITION", "1"), True)


def _cleanup_failed_or_cancelled_release_partition(release_id: int, data_type: str) -> tuple[bool, str | None]:
    if int(release_id or 0) <= 0:
        return False, "invalid release id"
    if not _auto_drop_failed_release_partition_enabled():
        return False, "disabled by env"

    normalized_type = _normalize_data_type(data_type)
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT is_active, status
                        FROM cadastral_release
                        WHERE id = %s
                          AND data_type = %s
                        """,
                        (int(release_id), normalized_type),
                    )
                    row = cur.fetchone()
                    if not row:
                        return False, "release not found"

                    is_active = bool(row[0])
                    status = str(row[1] or "").strip().upper()
                    if is_active:
                        return False, "release is active"
                    if status not in {"FAILED", "CANCELLED"}:
                        return False, f"release status is {status or 'UNKNOWN'}"

                    cur.execute("SELECT drop_dataset_record_partition(%s)", (int(release_id),))
                    land_info_table = conn.execute("SELECT to_regclass('public.land_info_record')").fetchone()
                    if land_info_table and land_info_table[0]:
                        conn.execute("DELETE FROM public.land_info_record WHERE release_id = %s", (int(release_id),))
    except Exception as exc:
        return False, f"cleanup failed: {exc}"

    return True, None


def _parallel_worker_config(data_type: str) -> tuple[str, int]:
    normalized = _normalize_data_type(data_type)
    suffix = _data_type_env_suffix(normalized)
    env_keys = [
        f"IMPORT_WORKERS_{suffix}",
        f"{suffix}_IMPORT_WORKERS",
    ]
    # Backward compatibility
    if normalized == "building_info":
        env_keys.insert(0, "BUILDING_INFO_IMPORT_WORKERS")
    elif normalized == "cadastral":
        env_keys.insert(0, "CADASTRAL_IMPORT_WORKERS")
    elif normalized == "building_integrated_info":
        env_keys.insert(0, "BUILDING_INTEGRATED_INFO_IMPORT_WORKERS")
    elif normalized == "land_info":
        env_keys.insert(0, "LAND_INFO_IMPORT_WORKERS")
    elif normalized in LAND_INFO_COMPONENT_DATA_TYPES:
        env_keys.insert(0, "LAND_INFO_IMPORT_WORKERS")

    for key in env_keys:
        raw = os.getenv(key, "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except Exception:
            continue
        if value > 0:
            return "fixed", max(1, value)
        if value == 0:
            return "auto", 0
    return "auto", 0


async def _run_import_job_subprocess(
    job_id: int,
    release_id: int,
    data_type: str,
    source_path: str,
    pattern: str,
    batch_size: int,
    truncate_release: bool,
    merge_by_pnu: bool,
    operation_mode: str,
    mark_ready: bool,
    activate_on_complete: bool = False,
    cleanup_old_releases_on_success: bool = False,
    workers: int = 0,
) -> None:
    normalized_type = _normalize_data_type(data_type)
    normalized_operation_mode = (operation_mode or "full").strip().lower()
    script_path = _import_script_path_for_data_type(normalized_type)
    tile_change_hint_path: Path | None = None
    cmd = [
        "python",
        script_path,
        "--data-type",
        normalized_type,
        "--release-id",
        str(release_id),
        "--source-dir",
        source_path,
        "--pattern",
        pattern,
        "--job-id",
        str(job_id),
        "--batch-size",
        str(batch_size),
        "--operation-mode",
        operation_mode,
    ]
    if normalized_type == "cadastral" and normalized_operation_mode == "update":
        tile_change_hint_path = _tile_change_hint_file_path(job_id)
        with contextlib.suppress(Exception):
            if tile_change_hint_path.exists():
                tile_change_hint_path.unlink()
        cmd.extend(["--tile-change-file", str(tile_change_hint_path)])
    label_precision = os.getenv("CADASTRAL_IMPORT_LABEL_PRECISION", "").strip()
    if label_precision and normalized_type == "cadastral":
        cmd.extend(["--label-precision", label_precision])
    if int(workers or 0) > 0:
        cmd.extend(["--workers", str(int(workers))])
    if truncate_release:
        cmd.append("--truncate-release")
    if merge_by_pnu and normalized_type == "building_info":
        cmd.append("--merge-by-pnu")
    if mark_ready:
        cmd.append("--mark-ready")
    if activate_on_complete:
        cmd.append("--activate-on-complete")

    env = dict(os.environ)
    env["DATABASE_URL"] = _db_url()
    env["IMPORT_DATA_TYPE"] = normalized_type

    error_message = ""
    process: asyncio.subprocess.Process | None = None
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        await _register_import_process(job_id, process)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            stderr_text = (stderr or b"").decode("utf-8", "ignore")
            stdout_text = (stdout or b"").decode("utf-8", "ignore")
            payload = (stderr_text or stdout_text or "import failed").strip()
            # Surface the raw return code: negative means "killed by signal" (e.g. -9).
            error_message = f"[rc={process.returncode}] {payload}"[:2000]
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        error_message = str(exc)[:2000]
    finally:
        await _pop_import_process(job_id)
        await _pop_import_runner(job_id)

    if not error_message:
        if normalized_type == "cadastral" and normalized_operation_mode == "update":
            logger = logging.getLogger("app.import")
            release_version = _release_version_by_id(release_id) or _tile_version()
            invalidation_result = _invalidate_cadastral_tiles_from_hint(
                job_id=job_id,
                release_id=release_id,
                version=release_version,
                hint_path=tile_change_hint_path,
            )
            logger.info(
                "tile cache invalidation done: data_type=%s release_id=%s job_id=%s "
                "boxes=%s tiles=%s disk_removed=%s memory_removed=%s fallback_full=%s "
                "cdn_enabled=%s cdn_urls=%s cdn_batches=%s/%s cdn_error=%s error=%s",
                normalized_type,
                release_id,
                job_id,
                invalidation_result.get("boxes_used"),
                invalidation_result.get("tile_candidates"),
                invalidation_result.get("disk_removed"),
                invalidation_result.get("memory_removed"),
                invalidation_result.get("fallback_full_version_clear"),
                invalidation_result.get("cdn_purge_enabled"),
                invalidation_result.get("cdn_purge_requested_urls"),
                invalidation_result.get("cdn_purge_batches_succeeded"),
                invalidation_result.get("cdn_purge_batches_total"),
                invalidation_result.get("cdn_purge_error"),
                invalidation_result.get("error"),
            )
        if cleanup_old_releases_on_success:
            deleted_ids, skip_reason = _cleanup_old_releases_after_success(release_id, normalized_type)
            logger = logging.getLogger("app.import")
            if deleted_ids:
                logger.info(
                    "import cleanup done: data_type=%s release_id=%s deleted=%s",
                    normalized_type,
                    release_id,
                    len(deleted_ids),
                )
            elif skip_reason:
                logger.warning(
                    "import cleanup skipped: data_type=%s release_id=%s reason=%s",
                    normalized_type,
                    release_id,
                    skip_reason,
                )
        with contextlib.suppress(Exception):
            if tile_change_hint_path is not None and tile_change_hint_path.exists():
                tile_change_hint_path.unlink()
        return

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                failed_update = conn.execute(
                    """
                    UPDATE cadastral_import_job
                    SET status = 'FAILED',
                        error_message = %s,
                        finished_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                      AND status <> 'CANCELLED'
                    """,
                    (error_message, job_id),
                )
                if failed_update.rowcount > 0:
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
                        (error_message, job_id),
                    )
                    conn.execute(
                        """
                        UPDATE cadastral_release
                        SET status = 'FAILED',
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (release_id,),
                    )
    except Exception:
        return

    # Full import failures (or cancellations) can leave huge non-active partitions.
    # Auto-drop those failed/cancelled partitions to avoid unbounded disk growth.
    if normalized_operation_mode == "full":
        cleaned, reason = _cleanup_failed_or_cancelled_release_partition(release_id, normalized_type)
        logger = logging.getLogger("app.import")
        if cleaned:
            logger.info(
                "failed/cancelled release partition cleanup done: data_type=%s release_id=%s",
                normalized_type,
                release_id,
            )
        elif reason:
            logger.warning(
                "failed/cancelled release partition cleanup skipped: data_type=%s release_id=%s reason=%s",
                normalized_type,
                release_id,
                reason,
            )

    with contextlib.suppress(Exception):
        if tile_change_hint_path is not None and tile_change_hint_path.exists():
            tile_change_hint_path.unlink()


async def _start_import_job_runner(
    job_id: int,
    release_id: int,
    data_type: str,
    source_path: str,
    pattern: str,
    batch_size: int,
    truncate_release: bool,
    merge_by_pnu: bool,
    operation_mode: str,
    mark_ready: bool,
    activate_on_complete: bool = False,
    cleanup_old_releases_on_success: bool = False,
) -> Dict[str, Any]:
    if await _has_import_runner(job_id):
        raise HTTPException(status_code=409, detail="import job is already running")
    normalized_type = _normalize_data_type(data_type)
    normalized_operation_mode = (operation_mode or "full").strip().lower()
    if normalized_operation_mode not in {"full", "update"}:
        raise HTTPException(status_code=400, detail=f"invalid operation_mode: {operation_mode}")
    merge_mode = bool(merge_by_pnu) and normalized_type == "building_info"
    truncate_mode = bool(truncate_release) and not merge_mode
    parallel_capable_types = {"building_info", "cadastral", "building_integrated_info", "land_info", *LAND_INFO_COMPONENT_DATA_TYPES}
    parallel_import_enabled = (
        normalized_type in parallel_capable_types
        and normalized_operation_mode == "full"
        and not merge_mode
    )
    workers_mode, workers_value = _parallel_worker_config(normalized_type)
    if workers_mode == "fixed" and workers_value <= 1:
        parallel_import_enabled = False

    task = asyncio.create_task(
        _run_import_job_subprocess(
            job_id=job_id,
            release_id=release_id,
            data_type=normalized_type,
            source_path=source_path,
            pattern=pattern,
            batch_size=batch_size,
            truncate_release=truncate_mode,
            merge_by_pnu=merge_mode,
            operation_mode=normalized_operation_mode,
            mark_ready=mark_ready,
            activate_on_complete=activate_on_complete,
            cleanup_old_releases_on_success=cleanup_old_releases_on_success,
            workers=workers_value,
        )
    )
    await _register_import_runner(job_id, task)

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE cadastral_import_job
                    SET status = 'RUNNING',
                        data_type = %s,
                        error_message = NULL,
                        started_at = COALESCE(started_at, NOW()),
                        finished_at = NULL,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (normalized_type, job_id),
                )
                conn.execute(
                    """
                    UPDATE cadastral_release
                    SET status = 'IMPORTING',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (release_id,),
                )
    except Exception as exc:
        task.cancel()
        await _pop_import_runner(job_id)
        raise HTTPException(status_code=500, detail=f"failed to mark import job running: {exc}")

    return {
        "job_id": job_id,
        "release_id": release_id,
        "data_type": normalized_type,
        "status": "RUNNING",
        "pattern": pattern,
        "batch_size": batch_size,
        "truncate_release": truncate_mode,
        "merge_by_pnu": merge_mode,
        "operation_mode": normalized_operation_mode,
        "parallel_import_enabled": parallel_import_enabled,
        "parallel_workers_mode": workers_mode,
        "parallel_workers": workers_value,
        "mark_ready": mark_ready,
        "activate_on_complete": activate_on_complete,
        "cleanup_old_releases_on_success": cleanup_old_releases_on_success,
    }


@app.get("/v1/app-config")
def app_config(
    request: Request,
    response: Response,
    platform: str = Query(..., pattern="^(android|ios)$"),
) -> Any:
    config = _fetch_app_config(platform)
    etag = _app_config_etag(config)
    if_none_match = (request.headers.get("if-none-match") or "").strip()
    if if_none_match and if_none_match == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"})

    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "no-cache"
    return ok(config)


@app.get("/v1/maintenance-status")
def maintenance_status(
    request: Request,
    response: Response,
    platform: str = Query(..., pattern="^(android|ios)$"),
) -> Any:
    config = _fetch_maintenance_config(platform)
    etag = _maintenance_config_etag(config)
    if_none_match = (request.headers.get("if-none-match") or "").strip()
    if if_none_match and if_none_match == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "no-cache"})

    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "no-cache"
    return ok(config)


@app.get("/v1/admin/app-config")
def get_admin_app_config(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    return ok({platform: _fetch_app_config(platform) for platform in APP_CONFIG_PLATFORMS})


@app.patch("/v1/admin/app-config/{platform}")
def update_admin_app_config(
    platform: str,
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    return ok(_update_app_config(platform, body))


@app.get("/v1/admin/maintenance-config")
def get_admin_maintenance_config(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    return ok({platform: _fetch_maintenance_config(platform) for platform in APP_CONFIG_PLATFORMS})


@app.patch("/v1/admin/maintenance-config/{platform}")
def update_admin_maintenance_config(
    platform: str,
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    return ok(_update_maintenance_config(platform, body))


@app.get("/v1/tile-config")
def tile_config() -> Dict[str, Any]:
    release = _active_release("cadastral")
    release_version = _tile_version()
    render_rev = _tile_render_rev()
    return ok(
        {
            "version": f"{release_version}-{render_rev}",
            "release_version": release_version,
            "render_rev": render_rev,
            "release_id": release["id"] if release else None,
            "min_zoom": _tile_min_zoom(),
            "max_zoom": _tile_max_zoom(),
            "tile_size": _tile_size(),
            "tile_url_template": "/v1/tiles/cadastral/{z}/{x}/{y}.png?v={version}",
        }
    )


@app.get("/admin")
def admin_page() -> Response:
    page_path = ADMIN_STATIC_DIR / "index.html"
    if not page_path.exists():
        raise HTTPException(status_code=404, detail="admin page is not configured")
    return FileResponse(page_path)


@app.get("/admin/")
def admin_page_slash() -> Response:
    return admin_page()


@app.get("/admin/logs")
def admin_logs_page() -> Response:
    page_path = ADMIN_STATIC_DIR / "logs.html"
    if not page_path.exists():
        raise HTTPException(status_code=404, detail="admin logs page is not configured")
    return FileResponse(page_path)


@app.get("/admin/logs/")
def admin_logs_page_slash() -> Response:
    return admin_logs_page()


def _building_hub_sync_dir() -> Path:
    return Path(os.getenv("BUILDING_HUB_SYNC_DIR", "/data/source/building_info_hub")).resolve()


def _cadastral_sync_dir() -> Path:
    return Path(os.getenv("CADASTRAL_SYNC_BASE_DIR", "/data/uploads/연속지적/auto")).resolve()


def _building_integrated_sync_dir() -> Path:
    return Path(os.getenv("BUILDING_INTEGRATED_SYNC_BASE_DIR", "/data/source/building_integrated_info/auto")).resolve()


def _land_movement_sync_dir() -> Path:
    return Path(os.getenv("LAND_MOVEMENT_SYNC_BASE_DIR", "/data/source/land_info/auto/land_movement")).resolve()


def _land_info_sync_dir() -> Path:
    return Path(os.getenv("LAND_INFO_SYNC_BASE_DIR", "/data/source/land_info/auto")).resolve()


def _land_info_worker_dir() -> Path:
    return Path(os.getenv("LAND_INFO_WORKER_DIR", "/worker/land-info-worker")).resolve()


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists() or not path.is_file():
            return None
        with path.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _path_modified_at(path: Path) -> str | None:
    try:
        return dt.datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat(timespec="seconds")
    except Exception:
        return None


def _path_file_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except Exception:
        return 0


def _path_mtime(path: Path) -> float:
    try:
        return float(path.stat().st_mtime)
    except Exception:
        return 0.0


def _recent_path_items(root: Path, pattern: str, *, limit: int = 8, recursive: bool = False) -> dict[str, Any]:
    paths: list[Path] = []
    try:
        if root.exists() and root.is_dir():
            iterator = root.rglob(pattern) if recursive else root.glob(pattern)
            paths = [path for path in iterator if path.is_file()]
    except Exception:
        paths = []

    paths.sort(key=_path_mtime, reverse=True)
    total_size = 0
    latest_mtime = 0.0
    for path in paths:
        try:
            stat = path.stat()
        except Exception:
            continue
        total_size += int(stat.st_size)
        latest_mtime = max(latest_mtime, float(stat.st_mtime))

    items = []
    for path in paths[:limit]:
        items.append(
            {
                "name": path.name,
                "path": str(path),
                "parent": path.parent.name,
                "size": _path_file_size(path),
                "modified_at": _path_modified_at(path),
            }
        )

    return {
        "count": len(paths),
        "total_size": total_size,
        "latest_modified_at": (
            dt.datetime.fromtimestamp(latest_mtime).astimezone().isoformat(timespec="seconds")
            if latest_mtime > 0
            else None
        ),
        "items": items,
    }


def _cadastral_sync_local_work(sync_dir: Path) -> dict[str, Any]:
    raw = _recent_path_items(sync_dir / "raw", "*.zip", limit=8)
    staging = _recent_path_items(sync_dir / "staging", "*.geojson", limit=8, recursive=True)
    manifests = _recent_path_items(sync_dir / "manifests", "*.json", limit=5)

    latest_stage_dir = None
    stage_dirs: list[Path] = []
    try:
        staging_dir = sync_dir / "staging"
        if staging_dir.exists() and staging_dir.is_dir():
            stage_dirs = [path for path in staging_dir.iterdir() if path.is_dir()]
    except Exception:
        stage_dirs = []
    if stage_dirs:
        latest = max(stage_dirs, key=lambda path: path.stat().st_mtime if path.exists() else 0)
        latest_stage_dir = {
            "name": latest.name,
            "path": str(latest),
            "modified_at": _path_modified_at(latest),
        }

    latest_activity_at = max(
        [value for value in (raw.get("latest_modified_at"), staging.get("latest_modified_at"), manifests.get("latest_modified_at")) if value],
        default=None,
    )
    failed_log = sync_dir / "failed_wgs84" / "failed.txt"
    failed_tail: list[str] = []
    try:
        if failed_log.exists():
            failed_tail = [line for line in failed_log.read_text(encoding="utf-8", errors="replace").splitlines() if line][-5:]
    except Exception:
        failed_tail = []

    return {
        "sync_dir": str(sync_dir),
        "raw": raw,
        "staging": staging,
        "manifests": manifests,
        "latest_stage_dir": latest_stage_dir,
        "latest_activity_at": latest_activity_at,
        "failed_tail": failed_tail,
    }


def _land_info_sync_local_work(sync_dir: Path, worker_dir: Path) -> dict[str, Any]:
    staging = _recent_path_items(sync_dir / "staging", "*.csv", limit=8, recursive=True)
    extracted = _recent_path_items(sync_dir / "extracted", "*.csv", limit=8, recursive=True)
    manifests = _recent_path_items(sync_dir / "manifests", "*.json", limit=5)
    worker_requests = _recent_path_items(worker_dir / "requests", "*.json", limit=8)
    worker_downloads = _recent_path_items(worker_dir / "downloads", "*.zip", limit=12, recursive=True)
    worker_manifests = _recent_path_items(worker_dir / "manifests", "*.json", limit=8)

    latest_stage_dir = None
    stage_dirs: list[Path] = []
    try:
        staging_dir = sync_dir / "staging"
        if staging_dir.exists() and staging_dir.is_dir():
            stage_dirs = [path for path in staging_dir.iterdir() if path.is_dir()]
    except Exception:
        stage_dirs = []
    if stage_dirs:
        latest = max(stage_dirs, key=lambda path: path.stat().st_mtime if path.exists() else 0)
        latest_stage_dir = {
            "name": latest.name,
            "path": str(latest),
            "modified_at": _path_modified_at(latest),
        }

    latest_activity_at = max(
        [
            value
            for value in (
                staging.get("latest_modified_at"),
                extracted.get("latest_modified_at"),
                manifests.get("latest_modified_at"),
                worker_requests.get("latest_modified_at"),
                worker_downloads.get("latest_modified_at"),
                worker_manifests.get("latest_modified_at"),
            )
            if value
        ],
        default=None,
    )
    return {
        "sync_dir": str(sync_dir),
        "worker_dir": str(worker_dir),
        "staging": staging,
        "extracted": extracted,
        "manifests": manifests,
        "worker_requests": worker_requests,
        "worker_downloads": worker_downloads,
        "worker_manifests": worker_manifests,
        "latest_stage_dir": latest_stage_dir,
        "latest_activity_at": latest_activity_at,
    }


def _land_movement_sync_local_work(sync_dir: Path) -> dict[str, Any]:
    raw = _recent_path_items(sync_dir / "raw", "*.zip", limit=8)
    staging = _recent_path_items(sync_dir / "staging", "*.csv", limit=8, recursive=True)
    extracted = _recent_path_items(sync_dir / "extracted", "*.csv", limit=8, recursive=True)
    manifests = _recent_path_items(sync_dir / "manifests", "*.json", limit=5)

    latest_stage_dir = None
    stage_dirs: list[Path] = []
    try:
        staging_dir = sync_dir / "staging"
        if staging_dir.exists() and staging_dir.is_dir():
            stage_dirs = [path for path in staging_dir.iterdir() if path.is_dir()]
    except Exception:
        stage_dirs = []
    if stage_dirs:
        latest = max(stage_dirs, key=lambda path: path.stat().st_mtime if path.exists() else 0)
        latest_stage_dir = {
            "name": latest.name,
            "path": str(latest),
            "modified_at": _path_modified_at(latest),
        }

    latest_activity_at = max(
        [
            value
            for value in (
                raw.get("latest_modified_at"),
                staging.get("latest_modified_at"),
                extracted.get("latest_modified_at"),
                manifests.get("latest_modified_at"),
            )
            if value
        ],
        default=None,
    )
    failed_log = sync_dir / "failed" / "failed.txt"
    failed_tail: list[str] = []
    try:
        if failed_log.exists():
            failed_tail = [line for line in failed_log.read_text(encoding="utf-8", errors="replace").splitlines() if line][-5:]
    except Exception:
        failed_tail = []

    return {
        "sync_dir": str(sync_dir),
        "raw": raw,
        "staging": staging,
        "extracted": extracted,
        "manifests": manifests,
        "latest_stage_dir": latest_stage_dir,
        "latest_activity_at": latest_activity_at,
        "failed_tail": failed_tail,
    }


def _land_info_direct_worker_dir() -> Path:
    return Path(os.getenv("LAND_INFO_DIRECT_WORKER_DIR", "/data/uploads/land_info_direct")).resolve()


def _land_info_direct_worker_dirs() -> dict[str, Path]:
    base = _land_info_direct_worker_dir()
    dirs = {
        "base": base,
        "requests": base / "requests",
        "uploads": base / "uploads",
        "chunks": base / "uploads" / "chunks",
        "accepted": base / "uploads" / "accepted",
        "heartbeats": base / "heartbeats",
        "manifests": base / "manifests",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


_DIRECT_LAND_INFO_PROCESSING_LOCK = Lock()
_DIRECT_LAND_INFO_PROCESSING_REQUESTS: set[str] = set()


def _safe_worker_file_name(value: str, default: str = "file") -> str:
    text = str(value or "").strip()
    text = re.sub(r"[\\/]+", "_", text)
    text = re.sub(r"[^0-9A-Za-z_.가-힣-]+", "_", text)
    text = text.strip("._ ")
    return text[:180] or default


def _direct_request_path(request_id: str) -> Path:
    safe_id = _safe_worker_file_name(request_id, "request")
    return _land_info_direct_worker_dirs()["requests"] / f"{safe_id}.json"


def _direct_upload_meta_path(upload_id: str) -> Path:
    safe_id = _safe_worker_file_name(upload_id, "upload")
    return _land_info_direct_worker_dirs()["uploads"] / f"{safe_id}.json"


def _write_direct_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _worker_now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def _require_land_info_worker(
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> str:
    expected = str(os.getenv("LAND_INFO_WORKER_TOKEN", "") or "").strip()
    worker_id = str(x_worker_id or "").strip()
    token = str(x_worker_token or "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="LAND_INFO_WORKER_TOKEN is not configured")
    if not worker_id or token != expected:
        raise HTTPException(status_code=403, detail="worker token is invalid")
    return worker_id


def _load_direct_request(request_id: str) -> dict[str, Any]:
    data = _read_json_file(_direct_request_path(request_id)) or {}
    if not data:
        raise HTTPException(status_code=404, detail="worker request not found")
    return data


def _write_direct_request_data(request_id: str, data: dict[str, Any]) -> None:
    _write_direct_json(_direct_request_path(request_id), data)


def _direct_processor_script_path() -> str:
    return os.getenv(
        "LAND_INFO_DIRECT_PROCESSOR_SCRIPT",
        "/scripts/process_land_info_direct_request.py",
    )


def _direct_processor_cleanup_on_success() -> bool:
    return str(os.getenv("LAND_INFO_DIRECT_CLEANUP_ON_SUCCESS", "1")).strip().lower() in {
        "1",
        "true",
        "t",
        "yes",
        "y",
        "on",
    }


async def _run_land_info_direct_processor(request_id: str) -> None:
    logger = logging.getLogger("app.land_info_direct")
    with _DIRECT_LAND_INFO_PROCESSING_LOCK:
        if request_id in _DIRECT_LAND_INFO_PROCESSING_REQUESTS:
            return
        _DIRECT_LAND_INFO_PROCESSING_REQUESTS.add(request_id)

    try:
        script_path = _direct_processor_script_path()
        cmd = [
            "python",
            script_path,
            "--request-id",
            request_id,
            "--direct-dir",
            str(_land_info_direct_worker_dir()),
            "--import-timeout",
            str(int(float(os.getenv("LAND_INFO_SYNC_IMPORT_TIMEOUT_SECONDS", "86400") or "86400"))),
        ]
        if _direct_processor_cleanup_on_success():
            cmd.append("--cleanup-on-success")
        else:
            cmd.append("--no-cleanup-on-success")

        data = _read_json_file(_direct_request_path(request_id)) or {}
        data["status"] = "server_processing"
        data["server_processor_command"] = [cmd[0], Path(script_path).name, *cmd[2:]]
        data["server_processing_started_at"] = _worker_now_iso()
        data["updated_at"] = _worker_now_iso()
        _write_direct_request_data(request_id, data)

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        data = _read_json_file(_direct_request_path(request_id)) or data
        data["server_processor_pid"] = process.pid
        data["updated_at"] = _worker_now_iso()
        _write_direct_request_data(request_id, data)

        stdout, stderr = await process.communicate()
        stdout_text = stdout.decode("utf-8", "replace")[-4000:] if stdout else ""
        stderr_text = stderr.decode("utf-8", "replace")[-4000:] if stderr else ""
        if stdout_text:
            logger.info("land_info direct processor stdout request=%s\n%s", request_id, stdout_text)
        if stderr_text:
            logger.warning("land_info direct processor stderr request=%s\n%s", request_id, stderr_text)
        if process.returncode != 0:
            data = _read_json_file(_direct_request_path(request_id)) or {}
            if str(data.get("status") or "").strip().lower() != "server_failed":
                data["status"] = "server_failed"
                data["server_failed_at"] = _worker_now_iso()
            data["server_processor_returncode"] = int(process.returncode or 0)
            data["server_processor_stdout_tail"] = stdout_text
            data["server_processor_stderr_tail"] = stderr_text
            data["updated_at"] = _worker_now_iso()
            _write_direct_request_data(request_id, data)
    except Exception as exc:
        logger.exception("land_info direct processor failed before completion: request=%s", request_id)
        data = _read_json_file(_direct_request_path(request_id)) or {}
        data["status"] = "server_failed"
        data["server_failed_at"] = _worker_now_iso()
        data["server_error"] = str(exc)[:2000]
        data["updated_at"] = _worker_now_iso()
        _write_direct_request_data(request_id, data)
    finally:
        with _DIRECT_LAND_INFO_PROCESSING_LOCK:
            _DIRECT_LAND_INFO_PROCESSING_REQUESTS.discard(request_id)


def _start_land_info_direct_processor(request_id: str) -> bool:
    with _DIRECT_LAND_INFO_PROCESSING_LOCK:
        if request_id in _DIRECT_LAND_INFO_PROCESSING_REQUESTS:
            return False
    asyncio.create_task(_run_land_info_direct_processor(request_id))
    return True


def _direct_file_statuses(request_data: dict[str, Any]) -> dict[str, Any]:
    statuses = request_data.get("file_statuses")
    if not isinstance(statuses, dict):
        statuses = {}
        request_data["file_statuses"] = statuses
    return statuses


def _direct_catalog_items(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    items = catalog.get("items")
    if not isinstance(items, list):
        return []
    cleaned: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        file_id = str(item.get("file_id") or "").strip()
        dataset_code = str(item.get("dataset_code") or "").strip()
        ds_file_id = str(item.get("ds_file_id") or "").strip()
        file_no = str(item.get("file_no") or "").strip()
        if not file_id or not dataset_code or not ds_file_id or not file_no:
            continue
        cleaned.append(dict(item))
    return sorted(cleaned, key=lambda row: str(row.get("file_id") or ""))


def _direct_catalog_item_is_incremental(item: dict[str, Any]) -> bool:
    file_gbn = str(item.get("fileGbnCd") or item.get("file_gbn_cd") or "").strip().upper()
    file_kind = str(item.get("file_kind") or item.get("fileKind") or "").strip().lower()
    operation_mode = str(item.get("operation_mode") or item.get("operationMode") or "").strip().lower()
    source_dataset_code = str(item.get("source_dataset_code") or item.get("sourceDatasetCode") or "").strip().upper()
    file_id = str(item.get("file_id") or "").strip().upper()
    incremental_raw = item.get("is_incremental")
    is_incremental = (
        incremental_raw is True
        or str(incremental_raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    )
    return (
        is_incremental
        or file_gbn == "CH"
        or file_kind in {"change", "changed", "incremental", "delta", "update"}
        or operation_mode == "update"
        or source_dataset_code.startswith("CH_")
        or file_id.startswith("CH_")
    )


def _direct_catalog_signature(items: list[dict[str, Any]], supplied: Any = None) -> str:
    supplied_text = str(supplied or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{32,64}", supplied_text):
        return supplied_text
    material = [
        {
            "file_id": item.get("file_id"),
            "dataset_code": item.get("dataset_code"),
            "region_code": item.get("region_code"),
            "base_date": item.get("base_date"),
            "updated_date": item.get("updated_date"),
            "file_no": item.get("file_no"),
            "ds_file_id": item.get("ds_file_id"),
            "size_bytes": item.get("size_bytes"),
        }
        for item in items
    ]
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _direct_catalog_items_by_dataset_code(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        dataset_code = str(item.get("dataset_code") or "").strip().upper()
        if dataset_code not in LAND_INFO_COMPONENTS:
            continue
        grouped.setdefault(dataset_code, []).append(item)
    return {
        code: sorted(rows, key=lambda row: str(row.get("file_id") or ""))
        for code, rows in sorted(grouped.items())
    }


def _direct_catalog_dataset_signature(items: list[dict[str, Any]]) -> str:
    return _direct_catalog_signature(items)


def _direct_catalog_snapshot_key(items: list[dict[str, Any]], catalog: dict[str, Any]) -> str:
    by_code: dict[str, set[str]] = {}
    summaries = catalog.get("datasets")
    if isinstance(summaries, list):
        for row in summaries:
            if not isinstance(row, dict):
                continue
            code = str(row.get("dataset_code") or "").strip()
            if not code:
                continue
            dates = row.get("base_dates")
            if isinstance(dates, list):
                by_code.setdefault(code, set()).update(str(value).strip() for value in dates if str(value).strip())
    for item in items:
        code = str(item.get("dataset_code") or "").strip()
        base_date = str(item.get("base_date") or "").strip()
        if code and base_date:
            by_code.setdefault(code, set()).add(base_date)
    return "|".join(f"{code}={max(dates)}" for code, dates in sorted(by_code.items()) if dates)


def _direct_catalog_dataset_snapshot_key(items: list[dict[str, Any]]) -> str:
    dates = {
        str(item.get("base_date") or "").strip()
        for item in items
        if str(item.get("base_date") or "").strip()
    }
    return max(dates) if dates else ""


def _active_land_info_release_metadata() -> dict[str, Any] | None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, version, records_count, metadata
                    FROM cadastral_release
                    WHERE data_type = 'land_info'
                      AND is_active = TRUE
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
    except Exception:
        return None
    if not row:
        return None
    metadata = row[3] if isinstance(row[3], dict) else {}
    return {
        "id": int(row[0]),
        "version": row[1],
        "records_count": int(row[2] or 0),
        "metadata": metadata,
    }


def _direct_request_is_worker_runnable(request_data: dict[str, Any]) -> bool:
    status = str(request_data.get("status") or "requested").strip().lower()
    return status in {"pending", "requested", "claimed", "in_progress"}


_DIRECT_REQUEST_RETRYABLE_STATUSES = frozenset({"completed_with_failures", "failed", "server_failed"})
_DIRECT_REQUEST_RETRY_COPY_FIELDS = (
    "data_type",
    "operation_mode",
    "source",
    "source_signature",
    "changed_source_signature",
    "snapshot_key",
    "activate",
    "test_mode",
    "expected_count",
    "items",
    "component_dataset_codes",
    "component_data_types",
    "component_status",
    "source_catalog",
    "worker_hostname",
    "worker_version",
)


def _validate_direct_request_id(request_id: str) -> str:
    normalized = str(request_id or "").strip()
    if (
        not normalized
        or len(normalized) > 180
        or normalized != request_id
        or _safe_worker_file_name(normalized, "") != normalized
    ):
        raise HTTPException(status_code=400, detail="invalid worker request_id")
    return normalized


def _direct_retry_purpose_key(request_data: dict[str, Any]) -> str:
    changed_signature = str(request_data.get("changed_source_signature") or "").strip().lower()
    if changed_signature:
        return f"changed:{changed_signature}"
    items = request_data.get("items")
    cleaned_items = [dict(item) for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    if cleaned_items:
        return f"items:{_direct_catalog_signature(cleaned_items)}"
    source_signature = str(request_data.get("source_signature") or "").strip().lower()
    snapshot_key = str(request_data.get("snapshot_key") or "").strip()
    return f"source:{source_signature}:{snapshot_key}"


def _direct_retry_root_request_id(request_data: dict[str, Any]) -> str:
    return str(
        request_data.get("retry_root_request_id")
        or request_data.get("parent_request_id")
        or request_data.get("request_id")
        or ""
    ).strip()


def _direct_request_records(requests_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in requests_dir.glob("*.json"):
        data = _read_json_file(path)
        if not isinstance(data, dict) or not data:
            continue
        request_id = str(data.get("request_id") or "").strip()
        if not request_id or _safe_worker_file_name(request_id, "") != request_id:
            continue
        records.append(data)
    return records


@contextlib.contextmanager
def _direct_retry_file_lock(requests_dir: Path) -> Any:
    lock_path = requests_dir / ".retry.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _create_land_info_retry_request(
    request_id: str,
    *,
    worker_id: str,
    retry_reason: str | None,
) -> dict[str, Any]:
    request_id = _validate_direct_request_id(request_id)
    dirs = _land_info_direct_worker_dirs()
    with _direct_retry_file_lock(dirs["requests"]):
        parent_path = _direct_request_path(request_id)
        parent = _read_json_file(parent_path) or {}
        if not parent or str(parent.get("request_id") or "").strip() != request_id:
            raise HTTPException(status_code=404, detail="worker request not found")

        parent_status = str(parent.get("status") or "requested").strip().lower()
        if parent_status not in _DIRECT_REQUEST_RETRYABLE_STATUSES:
            raise HTTPException(
                status_code=409,
                detail=f"worker request status is not retryable: {parent_status}",
            )

        records = _direct_request_records(dirs["requests"])
        direct_children = [
            record
            for record in records
            if str(record.get("parent_request_id") or "").strip() == request_id
        ]
        if len(direct_children) > 1:
            raise HTTPException(status_code=409, detail="worker request has multiple retry children")
        if direct_children:
            existing = direct_children[0]
            return {
                "created": False,
                "request_created": False,
                "request_id": existing.get("request_id"),
                "reason": "existing_retry",
                "request": existing,
            }

        root_request_id = _direct_retry_root_request_id(parent) or request_id
        purpose_key = _direct_retry_purpose_key(parent)
        for record in records:
            if not record.get("parent_request_id") or not _direct_request_is_worker_runnable(record):
                continue
            same_chain = _direct_retry_root_request_id(record) == root_request_id
            same_purpose = _direct_retry_purpose_key(record) == purpose_key
            if same_chain or same_purpose:
                return {
                    "created": False,
                    "request_created": False,
                    "request_id": record.get("request_id"),
                    "reason": "existing_runnable_retry",
                    "request": record,
                }

        try:
            retry_seq = int(parent.get("retry_seq") or 0) + 1
        except (TypeError, ValueError):
            raise HTTPException(status_code=409, detail="worker request has invalid retry_seq")
        if retry_seq <= 0:
            raise HTTPException(status_code=409, detail="worker request has invalid retry_seq")

        retry_digest = hashlib.sha256(
            f"{root_request_id}\n{request_id}\n{retry_seq}\n{purpose_key}".encode("utf-8")
        ).hexdigest()[:20]
        retry_request_id = f"land_info_retry_{retry_digest}"
        retry_path = _direct_request_path(retry_request_id)
        if retry_path.exists():
            existing = _read_json_file(retry_path) or {}
            if str(existing.get("parent_request_id") or "").strip() == request_id:
                return {
                    "created": False,
                    "request_created": False,
                    "request_id": retry_request_id,
                    "reason": "existing_retry",
                    "request": existing,
                }
            raise HTTPException(status_code=409, detail="retry request_id collision")

        now = _worker_now_iso()
        request_data = {
            key: parent[key]
            for key in _DIRECT_REQUEST_RETRY_COPY_FIELDS
            if key in parent
        }
        request_data.update(
            {
                "request_id": retry_request_id,
                "created_at": now,
                "updated_at": now,
                "status": "requested",
                "force_redownload": True,
                "parent_request_id": request_id,
                "retry_root_request_id": root_request_id,
                "retry_seq": retry_seq,
                "retry_source_status": parent_status,
                "retry_requested_by_worker": worker_id,
                "created_by_worker": worker_id,
            }
        )
        if retry_reason:
            request_data["retry_reason"] = retry_reason
        _write_direct_json(retry_path, request_data)
        return {
            "created": True,
            "request_created": True,
            "request_id": retry_request_id,
            "reason": "retry_created",
            "request": request_data,
        }


def _land_info_direct_worker_status() -> dict[str, Any]:
    dirs = _land_info_direct_worker_dirs()
    heartbeats: list[dict[str, Any]] = []
    for path in sorted(dirs["heartbeats"].glob("*.json"), key=_path_mtime, reverse=True):
        data = _read_json_file(path) or {}
        heartbeats.append(
            {
                "worker_id": data.get("worker_id") or path.stem,
                "hostname": data.get("hostname"),
                "version": data.get("version"),
                "status": data.get("status"),
                "current_request_id": data.get("current_request_id"),
                "current_file_id": data.get("current_file_id"),
                "message": data.get("message"),
                "download_dir": data.get("download_dir"),
                "free_bytes": data.get("free_bytes"),
                "received_at": data.get("received_at"),
                "modified_at": _path_modified_at(path),
            }
        )

    requests: list[dict[str, Any]] = []
    for path in sorted(dirs["requests"].glob("*.json"), key=_path_mtime, reverse=True)[:20]:
        data = _read_json_file(path) or {}
        request_id = str(data.get("request_id") or path.stem)
        file_statuses = data.get("file_statuses")
        status_counts: dict[str, int] = {}
        if isinstance(file_statuses, dict):
            for value in file_statuses.values():
                if not isinstance(value, dict):
                    continue
                status = str(value.get("status") or "unknown").strip().lower() or "unknown"
                status_counts[status] = status_counts.get(status, 0) + 1
        changed_dataset_codes = data.get("component_dataset_codes")
        if not isinstance(changed_dataset_codes, list):
            request_items = data.get("items") if isinstance(data.get("items"), list) else []
            grouped_items = _direct_catalog_items_by_dataset_code(
                [item for item in request_items if isinstance(item, dict)]
            )
            changed_dataset_codes = sorted(grouped_items)
        component_data_types = data.get("component_data_types")
        if not isinstance(component_data_types, list):
            component_data_types = [
                LAND_INFO_COMPONENTS[code]["data_type"]
                for code in changed_dataset_codes
                if code in LAND_INFO_COMPONENTS
            ]
        request_accepted_files: list[Path] = []
        request_accepted_dir = dirs["accepted"] / _safe_worker_file_name(request_id, "request")
        if request_accepted_dir.exists():
            request_accepted_files = [
                accepted_path
                for accepted_path in request_accepted_dir.glob("*.zip")
                if accepted_path.is_file()
            ]
        expected_count = None
        try:
            if data.get("expected_count") is not None:
                expected_count = max(0, int(str(data.get("expected_count")).strip()))
        except Exception:
            expected_count = None
        accepted_count = int(status_counts.get("accepted", 0) or len(request_accepted_files))
        failed_count = int(status_counts.get("failed", 0) or data.get("failed_count") or 0)
        completed_file_count = accepted_count + failed_count
        progress_percent = None
        if expected_count and expected_count > 0:
            status_text = str(data.get("status") or "").strip().lower()
            if status_text in {"server_processing", "server_processed", "processed"}:
                progress_percent = 100.0
            else:
                progress_percent = max(0.0, min(100.0, completed_file_count / expected_count * 100.0))
        requests.append(
            {
                "request_id": request_id,
                "status": data.get("status") or "requested",
                "force_redownload": bool(data.get("force_redownload", False)),
                "parent_request_id": data.get("parent_request_id"),
                "retry_root_request_id": data.get("retry_root_request_id"),
                "retry_seq": data.get("retry_seq"),
                "expected_count": expected_count,
                "uploaded_count": data.get("uploaded_count"),
                "failed_count": data.get("failed_count"),
                "source_signature": data.get("source_signature"),
                "changed_source_signature": data.get("changed_source_signature"),
                "snapshot_key": data.get("snapshot_key"),
                "changed_dataset_codes": changed_dataset_codes,
                "component_data_types": component_data_types,
                "claimed_by": data.get("claimed_by"),
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
                "worker_completed_at": data.get("worker_completed_at"),
                "file_status_counts": status_counts,
                "accepted_upload_count": len(request_accepted_files),
                "accepted_upload_bytes": sum(_path_file_size(accepted_path) for accepted_path in request_accepted_files),
                "completed_file_count": completed_file_count,
                "progress_percent": progress_percent,
                "path": str(path),
            }
        )

    accepted_files: list[Path] = []
    if dirs["accepted"].exists():
        accepted_files = [path for path in dirs["accepted"].rglob("*.zip") if path.is_file()]
    return {
        "base_dir": str(dirs["base"]),
        "latest_heartbeat": heartbeats[0] if heartbeats else None,
        "heartbeats": heartbeats[:10],
        "request_count": len(list(dirs["requests"].glob("*.json"))),
        "recent_requests": requests,
        "accepted_upload_count": len(accepted_files),
        "accepted_upload_bytes": sum(_path_file_size(path) for path in accepted_files),
        "accepted_dir": str(dirs["accepted"]),
    }


@app.post("/v1/worker/land-info/updates/ensure")
def worker_land_info_ensure_update(
    body: Dict[str, Any],
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    payload = dict(body or {})
    catalog = payload.get("source_catalog")
    if not isinstance(catalog, dict):
        raise HTTPException(status_code=400, detail="source_catalog is required")
    items = _direct_catalog_items(catalog)
    if not items:
        raise HTTPException(status_code=400, detail="source_catalog.items has no downloadable files")

    items_by_code = _direct_catalog_items_by_dataset_code(items)
    if not items_by_code:
        raise HTTPException(status_code=400, detail="source_catalog.items has no supported land_info dataset codes")
    incremental_items = [item for item in items if _direct_catalog_item_is_incremental(item)]
    if incremental_items:
        incremental_codes = sorted(
            {
                str(item.get("dataset_code") or "").strip().upper()
                for item in incremental_items
                if str(item.get("dataset_code") or "").strip()
            }
        )
        return ok(
            {
                "created": False,
                "request_created": False,
                "request_id": None,
                "reason": "incremental_not_enabled",
                "message": "land_info direct incremental import is not enabled yet; send full AL files or wait for server-side AL_D157 delta support.",
                "incremental_supported_dataset_codes": ["AL_D157"],
                "incremental_dataset_codes": incremental_codes,
                "incremental_expected_count": len(incremental_items),
                "incremental_samples": [
                    {
                        "file_id": item.get("file_id"),
                        "dataset_code": item.get("dataset_code"),
                        "source_dataset_code": item.get("source_dataset_code") or item.get("sourceDatasetCode"),
                        "fileGbnCd": item.get("fileGbnCd") or item.get("file_gbn_cd"),
                        "operation_mode": item.get("operation_mode") or item.get("operationMode"),
                    }
                    for item in incremental_items[:5]
                ],
            }
        )

    source_signature = _direct_catalog_signature(items, catalog.get("signature"))
    snapshot_key = _direct_catalog_snapshot_key(items, catalog)
    active = _active_land_info_release_metadata()
    active_components = _active_land_info_component_releases(force_refresh=True)
    changed_items: list[dict[str, Any]] = []
    component_status: dict[str, dict[str, Any]] = {}
    for dataset_code, dataset_items in items_by_code.items():
        component_signature = _direct_catalog_dataset_signature(dataset_items)
        component_snapshot = _direct_catalog_dataset_snapshot_key(dataset_items)
        active_component = active_components.get(dataset_code)
        active_metadata = (
            active_component.get("metadata")
            if isinstance(active_component, dict) and isinstance(active_component.get("metadata"), dict)
            else {}
        )
        active_signatures = {
            str(active_metadata.get("land_info_source_signature") or "").strip().lower(),
            str(active_metadata.get("source_signature") or "").strip().lower(),
            str(active_metadata.get("vworld_source_signature") or "").strip().lower(),
        }
        active_snapshots = {
            str(active_metadata.get("land_info_snapshot_key") or "").strip(),
            str(active_metadata.get("snapshot_key") or "").strip(),
            str(active_metadata.get("land_info_base_date") or "").strip(),
            str(active_metadata.get("base_date") or "").strip(),
        }
        up_to_date = bool(
            active_component
            and (
                component_signature in active_signatures
                or (component_snapshot and component_snapshot in active_snapshots)
            )
        )
        if not up_to_date:
            changed_items.extend(dataset_items)
        component_status[dataset_code] = {
            "data_type": LAND_INFO_COMPONENTS[dataset_code]["data_type"],
            "dataset_name": LAND_INFO_COMPONENTS[dataset_code]["name"],
            "source_signature": component_signature,
            "snapshot_key": component_snapshot,
            "active_release": active_component,
            "up_to_date": up_to_date,
            "expected_count": len(dataset_items),
        }

    if not changed_items:
        return ok(
            {
                "created": False,
                "request_created": False,
                "request_id": None,
                "reason": "already_active",
                "active_release": active,
                "component_status": component_status,
                "source_signature": source_signature,
                "snapshot_key": snapshot_key,
            }
        )

    changed_signature = _direct_catalog_signature(changed_items)
    request_id = f"land_info_update_{changed_signature[:16]}"
    if bool(payload.get("dry_run")):
        return ok(
            {
                "created": False,
                "request_created": False,
                "request_id": request_id,
                "reason": "dry_run",
                "expected_count": len(changed_items),
                "changed_dataset_codes": sorted(_direct_catalog_items_by_dataset_code(changed_items)),
                "component_status": component_status,
                "source_signature": source_signature,
                "snapshot_key": snapshot_key,
            }
        )

    request_path = _direct_request_path(request_id)
    existing = _read_json_file(request_path)
    if isinstance(existing, dict) and existing:
        status = str(existing.get("status") or "requested").strip().lower()
        if _direct_request_is_worker_runnable(existing):
            return ok(
                {
                    "created": False,
                    "request_created": False,
                    "request_id": request_id,
                    "reason": f"existing_{status}",
                    "request": existing,
                    "source_signature": source_signature,
                    "snapshot_key": snapshot_key,
                }
            )
        return ok(
            {
                "created": False,
                "request_created": False,
                "request_id": request_id,
                "reason": f"existing_{status}",
                "source_signature": source_signature,
                "snapshot_key": snapshot_key,
            }
        )

    now = _worker_now_iso()
    dirs = _land_info_direct_worker_dirs()
    request_data = {
        "request_id": request_id,
        "created_at": now,
        "updated_at": now,
        "status": "requested",
        "data_type": "land_info",
        "operation_mode": str(catalog.get("operation_mode") or "full"),
        "source": str(catalog.get("source") or "vworld"),
        "source_signature": source_signature,
        "changed_source_signature": changed_signature,
        "snapshot_key": snapshot_key,
        "activate": bool(payload.get("activate", True)),
        "test_mode": bool(payload.get("test_mode", False)),
        "expected_count": len(changed_items),
        "items": changed_items,
        "component_dataset_codes": sorted(_direct_catalog_items_by_dataset_code(changed_items)),
        "component_data_types": [
            LAND_INFO_COMPONENTS[code]["data_type"]
            for code in sorted(_direct_catalog_items_by_dataset_code(changed_items))
        ],
        "component_status": component_status,
        "created_by_worker": worker_id,
        "worker_hostname": payload.get("hostname"),
        "worker_version": payload.get("version"),
        "source_catalog": {
            "source": catalog.get("source"),
            "data_type": catalog.get("data_type"),
            "operation_mode": catalog.get("operation_mode"),
            "discovered_at": catalog.get("discovered_at"),
            "expected_count": catalog.get("expected_count"),
            "signature": source_signature,
            "snapshot_key": snapshot_key,
            "changed_signature": changed_signature,
            "datasets": catalog.get("datasets"),
        },
    }
    _write_direct_json(request_path, request_data)
    _write_direct_json(dirs["manifests"] / f"{_safe_worker_file_name(request_id, 'request')}.source_catalog.json", catalog)
    return ok(
        {
            "created": True,
            "request_created": True,
            "request_id": request_id,
            "reason": "new_source_catalog",
            "request": request_data,
            "changed_dataset_codes": request_data["component_dataset_codes"],
            "component_status": component_status,
            "source_signature": source_signature,
            "snapshot_key": snapshot_key,
        }
    )


@app.post("/v1/worker/land-info/requests/{request_id}/retry")
def worker_land_info_retry_request(
    request_id: str,
    body: Dict[str, Any] | None = None,
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    payload = dict(body or {})
    unknown_fields = sorted(set(payload) - {"reason"})
    if unknown_fields:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported retry fields: {', '.join(unknown_fields)}",
        )
    reason_value = payload.get("reason")
    if reason_value is not None and not isinstance(reason_value, str):
        raise HTTPException(status_code=400, detail="reason must be a string")
    retry_reason = str(reason_value or "").strip() or None
    if retry_reason and len(retry_reason) > 500:
        raise HTTPException(status_code=400, detail="reason must be at most 500 characters")
    return ok(
        _create_land_info_retry_request(
            request_id,
            worker_id=worker_id,
            retry_reason=retry_reason,
        )
    )


@app.post("/v1/worker/land-info/heartbeat")
async def worker_land_info_heartbeat(
    body: Dict[str, Any],
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    dirs = _land_info_direct_worker_dirs()
    payload = dict(body or {})
    payload["worker_id"] = worker_id
    payload["received_at"] = _worker_now_iso()
    _write_direct_json(dirs["heartbeats"] / f"{_safe_worker_file_name(worker_id, 'worker')}.json", payload)
    return ok({"server_time": _worker_now_iso()})


@app.get("/v1/worker/land-info/requests/next")
def worker_land_info_next_request(
    worker_id: str = Query(default=""),
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    header_worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    resolved_worker_id = str(worker_id or header_worker_id).strip()
    dirs = _land_info_direct_worker_dirs()
    candidates: list[dict[str, Any]] = []
    for path in sorted(dirs["requests"].glob("*.json"), key=lambda item: item.stat().st_mtime):
        data = _read_json_file(path) or {}
        if not isinstance(data, dict):
            continue
        status = str(data.get("status") or "pending").strip().lower()
        claimed_by = str(data.get("claimed_by") or "").strip()
        if status in {"pending", "requested"} or (
            status in {"claimed", "in_progress"} and claimed_by == resolved_worker_id
        ):
            candidates.append(data)
    if not candidates:
        return ok(None)
    return ok(candidates[0])


@app.post("/v1/worker/land-info/requests/{request_id}/claim")
def worker_land_info_claim_request(
    request_id: str,
    body: Dict[str, Any],
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    requested_worker_id = str((body or {}).get("worker_id") or worker_id).strip()
    data = _load_direct_request(request_id)
    claimed_by = str(data.get("claimed_by") or "").strip()
    status = str(data.get("status") or "pending").strip().lower()
    if claimed_by and claimed_by != requested_worker_id and status not in {"pending", "requested"}:
        return {"ok": False, "error": "already_claimed"}
    data["status"] = "claimed"
    data["claimed_by"] = requested_worker_id
    data["claimed_at"] = data.get("claimed_at") or _worker_now_iso()
    data["updated_at"] = _worker_now_iso()
    _write_direct_json(_direct_request_path(request_id), data)
    return ok({"claimed": True, "request_id": request_id})


@app.post("/v1/worker/land-info/requests/{request_id}/files/{file_id}/status")
def worker_land_info_file_status(
    request_id: str,
    file_id: str,
    body: Dict[str, Any],
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    data = _load_direct_request(request_id)
    statuses = _direct_file_statuses(data)
    payload = dict(body or {})
    payload["worker_id"] = worker_id
    payload["file_id"] = file_id
    payload["updated_at"] = _worker_now_iso()
    statuses[file_id] = payload
    data["updated_at"] = _worker_now_iso()
    _write_direct_json(_direct_request_path(request_id), data)
    return ok({"request_id": request_id, "file_id": file_id, "status": payload.get("status")})


@app.post("/v1/worker/land-info/uploads/init")
def worker_land_info_upload_init(
    body: Dict[str, Any],
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    request_id = str((body or {}).get("request_id") or "").strip()
    file_id = str((body or {}).get("file_id") or "").strip()
    file_name = _safe_worker_file_name(str((body or {}).get("file_name") or f"{file_id}.zip"), "upload.zip")
    sha256 = str((body or {}).get("sha256") or "").strip().lower()
    try:
        file_size = int((body or {}).get("file_size") or 0)
        chunk_size = int((body or {}).get("chunk_size") or 0)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid file_size or chunk_size")
    if not request_id or not file_id or file_size <= 0 or not sha256:
        raise HTTPException(status_code=400, detail="request_id, file_id, file_size and sha256 are required")

    upload_id = hashlib.sha256(f"{request_id}\n{file_id}\n{file_name}\n{sha256}".encode("utf-8")).hexdigest()[:32]
    dirs = _land_info_direct_worker_dirs()
    meta_path = _direct_upload_meta_path(upload_id)
    meta = _read_json_file(meta_path) or {}
    chunks_dir = dirs["chunks"] / upload_id
    accepted_path = dirs["accepted"] / _safe_worker_file_name(request_id, "request") / file_name
    received_chunks: list[int] = []
    if chunks_dir.exists():
        for chunk_path in chunks_dir.glob("*.chunk"):
            with contextlib.suppress(Exception):
                received_chunks.append(int(chunk_path.stem))
    if accepted_path.exists() and accepted_path.stat().st_size == file_size:
        meta_status = str(meta.get("status") or "")
        if meta_status in {"accepted", "processed", "uploaded"}:
            return ok(
                {
                    "upload_id": upload_id,
                    "status": meta_status,
                    "already_uploaded": True,
                    "received_chunks": sorted(received_chunks),
                    "received_bytes": int(accepted_path.stat().st_size),
                }
            )

    meta.update(
        {
            "upload_id": upload_id,
            "request_id": request_id,
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "sha256": sha256,
            "chunk_size": chunk_size,
            "worker_id": worker_id,
            "status": "uploading",
            "updated_at": _worker_now_iso(),
            "created_at": meta.get("created_at") or _worker_now_iso(),
        }
    )
    _write_direct_json(meta_path, meta)
    return ok(
        {
            "upload_id": upload_id,
            "received_chunks": sorted(received_chunks),
            "received_bytes": sum(path.stat().st_size for path in chunks_dir.glob("*.chunk")) if chunks_dir.exists() else 0,
            "status": "uploading",
        }
    )


@app.put("/v1/worker/land-info/uploads/{upload_id}/chunks/{chunk_index}")
async def worker_land_info_upload_chunk(
    upload_id: str,
    chunk_index: int,
    request: Request,
    x_chunk_offset: str | None = Header(default=None),
    x_chunk_size: str | None = Header(default=None),
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_land_info_worker(x_worker_id, x_worker_token)
    if chunk_index < 0:
        raise HTTPException(status_code=400, detail="chunk_index must be non-negative")
    meta = _read_json_file(_direct_upload_meta_path(upload_id)) or {}
    if not meta:
        raise HTTPException(status_code=404, detail="upload not found")
    raw = await request.body()
    expected_size = int(x_chunk_size or len(raw) or 0)
    if expected_size != len(raw):
        raise HTTPException(status_code=400, detail="chunk size mismatch")
    dirs = _land_info_direct_worker_dirs()
    chunks_dir = dirs["chunks"] / _safe_worker_file_name(upload_id, "upload")
    chunks_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = chunks_dir / f"{int(chunk_index):08d}.chunk"
    chunk_path.write_bytes(raw)
    meta["status"] = "uploading"
    meta["updated_at"] = _worker_now_iso()
    meta["last_chunk_index"] = int(chunk_index)
    if x_chunk_offset is not None:
        meta["last_chunk_offset"] = x_chunk_offset
    _write_direct_json(_direct_upload_meta_path(upload_id), meta)
    received_bytes = sum(path.stat().st_size for path in chunks_dir.glob("*.chunk"))
    return ok({"upload_id": upload_id, "chunk_index": int(chunk_index), "received_bytes": received_bytes})


@app.post("/v1/worker/land-info/uploads/{upload_id}/complete")
def worker_land_info_upload_complete(
    upload_id: str,
    body: Dict[str, Any],
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_land_info_worker(x_worker_id, x_worker_token)
    meta_path = _direct_upload_meta_path(upload_id)
    meta = _read_json_file(meta_path) or {}
    if not meta:
        raise HTTPException(status_code=404, detail="upload not found")
    expected_sha = str((body or {}).get("sha256") or meta.get("sha256") or "").strip().lower()
    expected_size = int((body or {}).get("file_size") or meta.get("file_size") or 0)
    dirs = _land_info_direct_worker_dirs()
    chunks_dir = dirs["chunks"] / _safe_worker_file_name(upload_id, "upload")
    if not chunks_dir.exists():
        raise HTTPException(status_code=400, detail="upload chunks not found")
    accepted_dir = dirs["accepted"] / _safe_worker_file_name(str(meta.get("request_id") or "request"), "request")
    accepted_dir.mkdir(parents=True, exist_ok=True)
    file_name = _safe_worker_file_name(str(meta.get("file_name") or f"{upload_id}.zip"), "upload.zip")
    target = accepted_dir / file_name
    tmp = target.with_suffix(target.suffix + ".tmp")
    digest = hashlib.sha256()
    total = 0
    with tmp.open("wb") as out:
        for chunk_path in sorted(chunks_dir.glob("*.chunk")):
            raw = chunk_path.read_bytes()
            out.write(raw)
            digest.update(raw)
            total += len(raw)
    actual_sha = digest.hexdigest()
    if expected_size and total != expected_size:
        with contextlib.suppress(Exception):
            tmp.unlink()
        raise HTTPException(status_code=400, detail=f"uploaded file size mismatch: expected={expected_size} actual={total}")
    if expected_sha and actual_sha != expected_sha:
        with contextlib.suppress(Exception):
            tmp.unlink()
        raise HTTPException(status_code=400, detail="uploaded sha256 mismatch")
    zip_verified = zipfile.is_zipfile(tmp)
    if zip_verified:
        with zipfile.ZipFile(tmp) as archive:
            zip_verified = archive.testzip() is None
    if not zip_verified:
        with contextlib.suppress(Exception):
            tmp.unlink()
        raise HTTPException(status_code=400, detail="uploaded zip verification failed")
    tmp.replace(target)
    with contextlib.suppress(Exception):
        shutil.rmtree(chunks_dir)
    meta.update(
        {
            "status": "accepted",
            "server_path": str(target),
            "sha256_verified": True,
            "zip_verified": True,
            "accepted_at": _worker_now_iso(),
            "updated_at": _worker_now_iso(),
        }
    )
    _write_direct_json(meta_path, meta)
    return ok(
        {
            "upload_id": upload_id,
            "status": "accepted",
            "server_path": str(target),
            "sha256_verified": True,
            "zip_verified": True,
        }
    )


@app.post("/v1/worker/land-info/requests/{request_id}/complete")
async def worker_land_info_request_complete(
    request_id: str,
    body: Dict[str, Any],
    x_worker_id: str | None = Header(default=None),
    x_worker_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    worker_id = _require_land_info_worker(x_worker_id, x_worker_token)
    data = _load_direct_request(request_id)
    uploaded_count = int((body or {}).get("uploaded_count") or 0)
    failed_count = int((body or {}).get("failed_count") or 0)
    data["status"] = "server_processing" if failed_count == 0 else "completed_with_failures"
    data["worker_id"] = worker_id
    data["uploaded_count"] = uploaded_count
    data["failed_count"] = failed_count
    data["worker_completed_at"] = _worker_now_iso()
    data["updated_at"] = _worker_now_iso()
    _write_direct_json(_direct_request_path(request_id), data)
    processor_started = False
    if failed_count == 0:
        processor_started = _start_land_info_direct_processor(request_id)
    return ok(
        {
            "request_id": request_id,
            "status": data["status"],
            "processor_started": processor_started,
        }
    )


def _parse_datetime(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
        return parsed
    except Exception:
        return None


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    zero_based = (year * 12 + (month - 1)) + delta
    return zero_based // 12, zero_based % 12 + 1


def _building_hub_release_data_month(release: dict[str, Any] | None) -> str | None:
    if not release:
        return None
    candidates: list[str] = []
    for key in ("version", "source_name"):
        value = release.get(key)
        if value:
            candidates.append(str(value))
    metadata = release.get("metadata")
    if isinstance(metadata, dict):
        for key in ("version", "source_name", "latest_month", "data_month"):
            value = metadata.get(key)
            if value:
                candidates.append(str(value))

    patterns = (
        re.compile(r"hub-(\d{4})(\d{2})"),
        re.compile(r"(\d{4})-(\d{1,2})"),
        re.compile(r"(\d{4})년\s*(\d{1,2})월"),
    )
    for text in candidates:
        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            year = int(match.group(1))
            month = int(match.group(2))
            if 1 <= month <= 12:
                return f"{year:04d}-{month:02d}"
    return None


def _building_hub_expected_upload_at(data_month: str | None) -> str | None:
    if not data_month:
        return None
    match = re.fullmatch(r"(\d{4})-(\d{2})", data_month)
    if not match:
        return None
    upload_day = max(1, min(28, int(os.getenv("BUILDING_HUB_SYNC_UPLOAD_DAY", "20") or "20")))
    upload_hour = max(0, min(23, int(os.getenv("BUILDING_HUB_SYNC_UPLOAD_CHECK_HOUR", "13") or "13")))
    upload_year, upload_month = _add_months(int(match.group(1)), int(match.group(2)), 2)
    local_tz = dt.datetime.now().astimezone().tzinfo
    upload_at = dt.datetime(upload_year, upload_month, upload_day, upload_hour, 0, 0, tzinfo=local_tz)
    return upload_at.isoformat(timespec="seconds")


def _building_hub_scheduler_state(
    *,
    next_expected_upload_at: str | None,
    latest_run: dict[str, Any] | None,
    latest_job: dict[str, Any] | None,
) -> dict[str, Any]:
    now = dt.datetime.now().astimezone()
    window_days = float(os.getenv("BUILDING_HUB_SYNC_UPLOAD_WINDOW_DAYS", "7") or "7")
    window_interval = int(float(os.getenv("BUILDING_HUB_SYNC_WINDOW_INTERVAL_SECONDS", "86400") or "86400"))
    late_interval = int(float(os.getenv("BUILDING_HUB_SYNC_LATE_INTERVAL_SECONDS", "86400") or "86400"))
    status = "WAITING"
    reason = "다음 예상 업로드일 전"
    next_check_at = next_expected_upload_at

    job_status = str((latest_job or {}).get("status") or "").upper()
    if job_status in {"QUEUED", "RUNNING"}:
        return {
            "status": "IMPORTING",
            "reason": f"import job {latest_job.get('id')} {job_status}",
            "next_check_at": None,
            "window_interval_seconds": window_interval,
            "late_interval_seconds": late_interval,
        }

    latest_run_status = str((latest_run or {}).get("status") or "")
    if latest_run_status == "failed":
        status = "ERROR"
        reason = str((latest_run or {}).get("error") or "최근 자동화 실행 실패")

    expected_dt = None
    if next_expected_upload_at:
        expected_dt = dt.datetime.fromisoformat(next_expected_upload_at)
    if expected_dt is not None and status != "ERROR":
        if now < expected_dt:
            status = "WAITING"
            reason = "다음 예상 업로드일 전"
            next_check_at = next_expected_upload_at
        else:
            upload_window_end = expected_dt + dt.timedelta(days=max(1.0, window_days))
            if now <= upload_window_end:
                status = "CHECKING_DAILY"
                reason = "예상 업로드 기간"
                next_check_at = (now + dt.timedelta(seconds=window_interval)).isoformat(timespec="seconds")
            else:
                status = "CHECKING_LATE"
                reason = "예상 기간 이후 미게시 확인"
                next_check_at = (now + dt.timedelta(seconds=late_interval)).isoformat(timespec="seconds")

    return {
        "status": status,
        "reason": reason,
        "next_check_at": next_check_at,
        "window_interval_seconds": window_interval,
        "late_interval_seconds": late_interval,
    }


@app.get("/v1/admin/building-hub-sync/status")
def get_building_hub_sync_status(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    sync_dir = _building_hub_sync_dir()
    cycle_manifest = _read_json_file(sync_dir / "cycle_manifest.json") or {}
    fetch_manifest = _read_json_file(sync_dir / "manifest.json") or {}
    latest_run = cycle_manifest.get("latest") if isinstance(cycle_manifest.get("latest"), dict) else None
    latest_fetch = fetch_manifest.get("latest") if isinstance(fetch_manifest.get("latest"), dict) else None

    active_release = None
    latest_job = None
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    FROM cadastral_release
                    WHERE data_type = 'building_info'
                      AND is_active = TRUE
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    active_release = _release_row_to_dict(row)

                cur.execute(
                    """
                    SELECT
                      j.id, j.release_id, r.version, j.status, j.source_path,
                      j.total_files, j.processed_files, j.inserted_rows, j.error_message,
                      j.created_at, j.started_at, j.finished_at, j.updated_at,
                      COALESCE(j.data_type, r.data_type, 'building_info') AS data_type
                    FROM cadastral_import_job j
                    LEFT JOIN cadastral_release r ON r.id = j.release_id
                    WHERE COALESCE(j.data_type, r.data_type, 'building_info') = 'building_info'
                    ORDER BY j.id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    latest_job = _import_job_row_to_dict(row)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load building hub sync status: {exc}")

    data_month = _building_hub_release_data_month(active_release)
    next_expected_upload_at = _building_hub_expected_upload_at(data_month)
    scheduler = _building_hub_scheduler_state(
        next_expected_upload_at=next_expected_upload_at,
        latest_run=latest_run,
        latest_job=latest_job,
    )

    return ok(
        {
            "sync_dir": str(sync_dir),
            "manifest_exists": bool(cycle_manifest),
            "source_manifest_exists": bool(fetch_manifest),
            "active_release": active_release,
            "active_data_month": data_month,
            "next_expected_upload_at": next_expected_upload_at,
            "scheduler": scheduler,
            "latest_run": latest_run,
            "latest_fetch": latest_fetch,
            "latest_job": latest_job,
            "config": {
                "upload_day": int(os.getenv("BUILDING_HUB_SYNC_UPLOAD_DAY", "20") or "20"),
                "upload_check_hour": int(os.getenv("BUILDING_HUB_SYNC_UPLOAD_CHECK_HOUR", "13") or "13"),
                "upload_window_days": float(os.getenv("BUILDING_HUB_SYNC_UPLOAD_WINDOW_DAYS", "7") or "7"),
                "window_interval_seconds": int(float(os.getenv("BUILDING_HUB_SYNC_WINDOW_INTERVAL_SECONDS", "86400") or "86400")),
                "late_interval_seconds": int(float(os.getenv("BUILDING_HUB_SYNC_LATE_INTERVAL_SECONDS", "86400") or "86400")),
            },
        }
    )


def _incremental_sync_scheduler_state(
    *,
    env_prefix: str,
    latest_run: dict[str, Any] | None,
    latest_job: dict[str, Any] | None,
    local_work: dict[str, Any],
) -> dict[str, Any]:
    check_interval = int(float(os.getenv(f"{env_prefix}_CHECK_INTERVAL_SECONDS", "86400") or "86400"))
    retry_interval = int(float(os.getenv(f"{env_prefix}_RETRY_SECONDS", "3600") or "3600"))
    status = "WAITING"
    reason = "다음 확인 대기"
    next_check_at = None

    job_status = str((latest_job or {}).get("status") or "").upper()
    if job_status in {"QUEUED", "RUNNING"}:
        return {
            "status": "IMPORTING",
            "reason": f"import job {latest_job.get('id')} {job_status}",
            "next_check_at": None,
            "check_interval_seconds": check_interval,
            "retry_interval_seconds": retry_interval,
        }

    latest_run_status = str((latest_run or {}).get("status") or "")
    if latest_run_status == "failed":
        status = "ERROR"
        reason = str((latest_run or {}).get("error") or "최근 자동화 실행 실패")
        finished = _parse_datetime((latest_run or {}).get("finished_at"))
        if finished:
            next_check_at = (finished + dt.timedelta(seconds=retry_interval)).isoformat(timespec="seconds")
    else:
        finished = _parse_datetime((latest_run or {}).get("finished_at"))
        activity = _parse_datetime(local_work.get("latest_activity_at"))
        if activity and (not finished or activity > finished):
            status = "PREPARING"
            reason = "증분 다운로드/변환 진행 중"
        elif latest_run_status in {"imported", "noop", "check_only"}:
            status = "SLEEPING"
            reason = "최근 사이클 완료"
            if finished:
                next_check_at = (finished + dt.timedelta(seconds=check_interval)).isoformat(timespec="seconds")
        elif latest_run_status:
            status = str(latest_run_status).upper()
            reason = "최근 manifest 기준"

    return {
        "status": status,
        "reason": reason,
        "next_check_at": next_check_at,
        "check_interval_seconds": check_interval,
        "retry_interval_seconds": retry_interval,
    }


def _cadastral_scheduler_state(
    *,
    latest_run: dict[str, Any] | None,
    latest_job: dict[str, Any] | None,
    local_work: dict[str, Any],
) -> dict[str, Any]:
    return _incremental_sync_scheduler_state(
        env_prefix="CADASTRAL_SYNC",
        latest_run=latest_run,
        latest_job=latest_job,
        local_work=local_work,
    )


@app.get("/v1/admin/cadastral-sync/status")
def get_cadastral_sync_status(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    sync_dir = _cadastral_sync_dir()
    cycle_manifest = _read_json_file(sync_dir / "cycle_manifest.json") or {}
    latest_run = cycle_manifest.get("latest") if isinstance(cycle_manifest.get("latest"), dict) else None
    local_work = _cadastral_sync_local_work(sync_dir)

    active_release = None
    latest_job = None
    recent_update_files: list[dict[str, Any]] = []
    update_file_count = 0
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    FROM cadastral_release
                    WHERE data_type = 'cadastral'
                      AND is_active = TRUE
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    active_release = _release_row_to_dict(row)

                cur.execute(
                    """
                    SELECT
                      j.id, j.release_id, r.version, j.status, j.source_path,
                      j.total_files, j.processed_files, j.inserted_rows, j.error_message,
                      j.created_at, j.started_at, j.finished_at, j.updated_at,
                      COALESCE(j.data_type, r.data_type, 'cadastral') AS data_type
                    FROM cadastral_import_job j
                    LEFT JOIN cadastral_release r ON r.id = j.release_id
                    WHERE COALESCE(j.data_type, r.data_type, 'cadastral') = 'cadastral'
                    ORDER BY j.id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    latest_job = _import_job_row_to_dict(row)

                update_rows = _load_recent_update_file_rows(conn, data_type="cadastral", limit=8, offset=0)
                recent_update_files = [_update_file_row_to_dict(item) for item in update_rows]
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM dataset_import_file f
                    LEFT JOIN cadastral_release r ON r.id = f.release_id
                    WHERE COALESCE(f.data_type, r.data_type, 'cadastral') = 'cadastral'
                      AND COALESCE(r.metadata ->> 'operation_mode', '') = 'update'
                    """
                )
                count_row = cur.fetchone()
                update_file_count = int(count_row[0] or 0) if count_row else 0
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load cadastral sync status: {exc}")

    scheduler = _cadastral_scheduler_state(
        latest_run=latest_run,
        latest_job=latest_job,
        local_work=local_work,
    )

    return ok(
        {
            "sync_dir": str(sync_dir),
            "manifest_exists": bool(cycle_manifest),
            "active_release": active_release,
            "scheduler": scheduler,
            "latest_run": latest_run,
            "latest_job": latest_job,
            "recent_update_files": recent_update_files,
            "update_file_count": update_file_count,
            "local_work": local_work,
            "config": {
                "check_interval_seconds": int(float(os.getenv("CADASTRAL_SYNC_CHECK_INTERVAL_SECONDS", "86400") or "86400")),
                "retry_interval_seconds": int(float(os.getenv("CADASTRAL_SYNC_RETRY_SECONDS", "3600") or "3600")),
                "initial_delay_seconds": int(float(os.getenv("CADASTRAL_SYNC_INITIAL_DELAY_SECONDS", "0") or "0")),
                "poll_interval_seconds": int(float(os.getenv("CADASTRAL_SYNC_POLL_INTERVAL_SECONDS", "30") or "30")),
                "import_timeout_seconds": int(float(os.getenv("CADASTRAL_SYNC_IMPORT_TIMEOUT_SECONDS", "14400") or "14400")),
                "max_direct_download_mb": float(os.getenv("CADASTRAL_SYNC_MAX_DIRECT_DOWNLOAD_MB", "500") or "500"),
                "cleanup_on_success": str(os.getenv("CADASTRAL_SYNC_CLEANUP_ON_SUCCESS", "1")).strip().lower()
                in {"1", "true", "t", "yes", "y", "on"},
                "max_files": int(float(os.getenv("CADASTRAL_SYNC_MAX_FILES", "0") or "0")),
                "credentials_configured": bool(os.getenv("VWORLD_USER_ID", "").strip())
                and bool(os.getenv("VWORLD_USER_PASSWORD", "").strip()),
            },
        }
    )


@app.get("/v1/admin/building-integrated-sync/status")
def get_building_integrated_sync_status(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    sync_dir = _building_integrated_sync_dir()
    cycle_manifest = _read_json_file(sync_dir / "cycle_manifest.json") or {}
    latest_run = cycle_manifest.get("latest") if isinstance(cycle_manifest.get("latest"), dict) else None
    local_work = _cadastral_sync_local_work(sync_dir)

    active_release = None
    latest_job = None
    recent_update_files: list[dict[str, Any]] = []
    update_file_count = 0
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    FROM cadastral_release
                    WHERE data_type = 'building_integrated_info'
                      AND is_active = TRUE
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    active_release = _release_row_to_dict(row)

                cur.execute(
                    """
                    SELECT
                      j.id, j.release_id, r.version, j.status, j.source_path,
                      j.total_files, j.processed_files, j.inserted_rows, j.error_message,
                      j.created_at, j.started_at, j.finished_at, j.updated_at,
                      COALESCE(j.data_type, r.data_type, 'building_integrated_info') AS data_type
                    FROM cadastral_import_job j
                    LEFT JOIN cadastral_release r ON r.id = j.release_id
                    WHERE COALESCE(j.data_type, r.data_type, 'building_integrated_info') = 'building_integrated_info'
                    ORDER BY j.id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    latest_job = _import_job_row_to_dict(row)

                update_rows = _load_recent_update_file_rows(conn, data_type="building_integrated_info", limit=8, offset=0)
                recent_update_files = [_update_file_row_to_dict(item) for item in update_rows]
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM dataset_import_file f
                    LEFT JOIN cadastral_release r ON r.id = f.release_id
                    WHERE COALESCE(f.data_type, r.data_type, 'building_integrated_info') = 'building_integrated_info'
                      AND COALESCE(r.metadata ->> 'operation_mode', '') = 'update'
                    """
                )
                count_row = cur.fetchone()
                update_file_count = int(count_row[0] or 0) if count_row else 0
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load building integrated sync status: {exc}")

    scheduler = _incremental_sync_scheduler_state(
        env_prefix="BUILDING_INTEGRATED_SYNC",
        latest_run=latest_run,
        latest_job=latest_job,
        local_work=local_work,
    )

    return ok(
        {
            "sync_dir": str(sync_dir),
            "manifest_exists": bool(cycle_manifest),
            "active_release": active_release,
            "scheduler": scheduler,
            "latest_run": latest_run,
            "latest_job": latest_job,
            "recent_update_files": recent_update_files,
            "update_file_count": update_file_count,
            "local_work": local_work,
            "config": {
                "check_interval_seconds": int(float(os.getenv("BUILDING_INTEGRATED_SYNC_CHECK_INTERVAL_SECONDS", "86400") or "86400")),
                "retry_interval_seconds": int(float(os.getenv("BUILDING_INTEGRATED_SYNC_RETRY_SECONDS", "3600") or "3600")),
                "initial_delay_seconds": int(float(os.getenv("BUILDING_INTEGRATED_SYNC_INITIAL_DELAY_SECONDS", "0") or "0")),
                "poll_interval_seconds": int(float(os.getenv("BUILDING_INTEGRATED_SYNC_POLL_INTERVAL_SECONDS", "30") or "30")),
                "import_timeout_seconds": int(float(os.getenv("BUILDING_INTEGRATED_SYNC_IMPORT_TIMEOUT_SECONDS", "14400") or "14400")),
                "max_direct_download_mb": float(os.getenv("BUILDING_INTEGRATED_SYNC_MAX_DIRECT_DOWNLOAD_MB", "500") or "500"),
                "cleanup_on_success": str(os.getenv("BUILDING_INTEGRATED_SYNC_CLEANUP_ON_SUCCESS", "1")).strip().lower()
                in {"1", "true", "t", "yes", "y", "on"},
                "max_files": int(float(os.getenv("BUILDING_INTEGRATED_SYNC_MAX_FILES", "0") or "0")),
                "credentials_configured": bool(os.getenv("VWORLD_USER_ID", "").strip())
                and bool(os.getenv("VWORLD_USER_PASSWORD", "").strip()),
            },
        }
    )


@app.get("/v1/admin/land-movement-sync/status")
def get_land_movement_sync_status(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    sync_dir = _land_movement_sync_dir()
    cycle_manifest = _read_json_file(sync_dir / "cycle_manifest.json") or {}
    latest_run = cycle_manifest.get("latest") if isinstance(cycle_manifest.get("latest"), dict) else None
    local_work = _land_movement_sync_local_work(sync_dir)

    data_type = "land_info_al_d157"
    active_release = None
    latest_job = None
    recent_update_files: list[dict[str, Any]] = []
    update_file_count = 0
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    FROM cadastral_release
                    WHERE data_type = %s
                      AND is_active = TRUE
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """,
                    (data_type,),
                )
                row = cur.fetchone()
                if row:
                    active_release = _release_row_to_dict(row)

                cur.execute(
                    """
                    SELECT
                      j.id, j.release_id, r.version, j.status, j.source_path,
                      j.total_files, j.processed_files, j.inserted_rows, j.error_message,
                      j.created_at, j.started_at, j.finished_at, j.updated_at,
                      COALESCE(j.data_type, r.data_type, %s) AS data_type
                    FROM cadastral_import_job j
                    LEFT JOIN cadastral_release r ON r.id = j.release_id
                    WHERE COALESCE(j.data_type, r.data_type, %s) = %s
                    ORDER BY j.id DESC
                    LIMIT 1
                    """,
                    (data_type, data_type, data_type),
                )
                row = cur.fetchone()
                if row:
                    latest_job = _import_job_row_to_dict(row)

                cur.execute(
                    """
                    SELECT
                      f.id, f.release_id, r.version,
                      COALESCE(f.data_type, r.data_type, %s) AS data_type,
                      f.file_name, f.file_size, f.created_at, r.is_active, r.status
                    FROM dataset_import_file f
                    LEFT JOIN cadastral_release r ON r.id = f.release_id
                    WHERE COALESCE(f.data_type, r.data_type, %s) = %s
                      AND f.file_name LIKE 'CH_D157%%'
                    ORDER BY f.id DESC
                    LIMIT 8
                    """,
                    (data_type, data_type, data_type),
                )
                recent_update_files = [_update_file_row_to_dict(item) for item in cur.fetchall()]

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM dataset_import_file f
                    LEFT JOIN cadastral_release r ON r.id = f.release_id
                    WHERE COALESCE(f.data_type, r.data_type, %s) = %s
                      AND f.file_name LIKE 'CH_D157%%'
                    """,
                    (data_type, data_type),
                )
                count_row = cur.fetchone()
                update_file_count = int(count_row[0] or 0) if count_row else 0
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load land movement sync status: {exc}")

    scheduler = _incremental_sync_scheduler_state(
        env_prefix="LAND_MOVEMENT_SYNC",
        latest_run=latest_run,
        latest_job=latest_job,
        local_work=local_work,
    )

    return ok(
        {
            "sync_dir": str(sync_dir),
            "manifest_exists": bool(cycle_manifest),
            "active_release": active_release,
            "scheduler": scheduler,
            "latest_run": latest_run,
            "latest_job": latest_job,
            "recent_update_files": recent_update_files,
            "update_file_count": update_file_count,
            "local_work": local_work,
            "config": {
                "check_interval_seconds": int(float(os.getenv("LAND_MOVEMENT_SYNC_CHECK_INTERVAL_SECONDS", "86400") or "86400")),
                "retry_interval_seconds": int(float(os.getenv("LAND_MOVEMENT_SYNC_RETRY_SECONDS", "3600") or "3600")),
                "initial_delay_seconds": int(float(os.getenv("LAND_MOVEMENT_SYNC_INITIAL_DELAY_SECONDS", "0") or "0")),
                "poll_interval_seconds": int(float(os.getenv("LAND_MOVEMENT_SYNC_POLL_INTERVAL_SECONDS", "30") or "30")),
                "import_timeout_seconds": int(float(os.getenv("LAND_MOVEMENT_SYNC_IMPORT_TIMEOUT_SECONDS", "14400") or "14400")),
                "max_direct_download_mb": float(os.getenv("LAND_MOVEMENT_SYNC_MAX_DIRECT_DOWNLOAD_MB", "500") or "500"),
                "cleanup_on_success": str(os.getenv("LAND_MOVEMENT_SYNC_CLEANUP_ON_SUCCESS", "1")).strip().lower()
                in {"1", "true", "t", "yes", "y", "on"},
                "max_files": int(float(os.getenv("LAND_MOVEMENT_SYNC_MAX_FILES", "0") or "0")),
                "credentials_configured": bool(os.getenv("VWORLD_USER_ID", "").strip())
                and bool(os.getenv("VWORLD_USER_PASSWORD", "").strip()),
            },
        }
    )


@app.get("/v1/admin/land-info-sync/status")
def get_land_info_sync_status(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    sync_dir = _land_info_sync_dir()
    worker_dir = _land_info_worker_dir()
    cycle_manifest = _read_json_file(sync_dir / "cycle_manifest.json") or {}
    latest_run = cycle_manifest.get("latest") if isinstance(cycle_manifest.get("latest"), dict) else None
    local_work = _land_info_sync_local_work(sync_dir, worker_dir)

    active_release = None
    component_releases: dict[str, dict[str, Any]] = {}
    latest_job = None
    recent_import_files: list[dict[str, Any]] = []
    import_file_count = 0
    dataset_presence: dict[str, bool] = {}
    family_data_types = ["land_info", *sorted(LAND_INFO_COMPONENT_DATA_TYPES)]
    try:
        component_releases = _active_land_info_component_releases(force_refresh=True)
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    FROM cadastral_release
                    WHERE data_type = 'land_info'
                      AND is_active = TRUE
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    active_release = _release_row_to_dict(row)

                cur.execute(
                    """
                    SELECT
                      j.id, j.release_id, r.version, j.status, j.source_path,
                      j.total_files, j.processed_files, j.inserted_rows, j.error_message,
                      j.created_at, j.started_at, j.finished_at, j.updated_at,
                      COALESCE(j.data_type, r.data_type, 'land_info') AS data_type
                    FROM cadastral_import_job j
                    LEFT JOIN cadastral_release r ON r.id = j.release_id
                    WHERE COALESCE(j.data_type, r.data_type, 'land_info') = ANY(%s)
                    ORDER BY j.id DESC
                    LIMIT 1
                    """,
                    (family_data_types,),
                )
                row = cur.fetchone()
                if row:
                    latest_job = _import_job_row_to_dict(row)

                cur.execute(
                    """
                    SELECT
                      f.id, f.release_id, r.version,
                      COALESCE(f.data_type, r.data_type, 'land_info') AS data_type,
                      f.file_name, f.file_size, f.created_at, r.is_active, r.status
                    FROM dataset_import_file f
                    LEFT JOIN cadastral_release r ON r.id = f.release_id
                    WHERE COALESCE(f.data_type, r.data_type, 'land_info') = ANY(%s)
                    ORDER BY f.id DESC
                    LIMIT 12
                    """,
                    (family_data_types,),
                )
                recent_import_files = [_update_file_row_to_dict(item) for item in cur.fetchall()]

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM dataset_import_file f
                    LEFT JOIN cadastral_release r ON r.id = f.release_id
                    WHERE COALESCE(f.data_type, r.data_type, 'land_info') = ANY(%s)
                    """,
                    (family_data_types,),
                )
                count_row = cur.fetchone()
                import_file_count = int(count_row[0] or 0) if count_row else 0

                latest_verification = latest_run.get("verification") if isinstance(latest_run, dict) else None
                if isinstance(latest_verification, dict) and isinstance(latest_verification.get("dataset_presence"), dict):
                    dataset_presence = dict(latest_verification.get("dataset_presence") or {})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load land info sync status: {exc}")

    scheduler = _incremental_sync_scheduler_state(
        env_prefix="LAND_INFO_SYNC",
        latest_run=latest_run,
        latest_job=latest_job,
        local_work=local_work,
    )

    return ok(
        {
            "sync_dir": str(sync_dir),
            "worker_dir": str(worker_dir),
            "manifest_exists": bool(cycle_manifest),
            "active_release": active_release,
            "component_releases": component_releases,
            "scheduler": scheduler,
            "latest_run": latest_run,
            "latest_job": latest_job,
            "recent_import_files": recent_import_files,
            "import_file_count": import_file_count,
            "dataset_presence": dataset_presence,
            "local_work": local_work,
            "direct_worker": _land_info_direct_worker_status(),
            "config": {
                "check_interval_seconds": int(float(os.getenv("LAND_INFO_SYNC_CHECK_INTERVAL_SECONDS", "86400") or "86400")),
                "pending_interval_seconds": int(float(os.getenv("LAND_INFO_SYNC_PENDING_INTERVAL_SECONDS", "600") or "600")),
                "retry_interval_seconds": int(float(os.getenv("LAND_INFO_SYNC_RETRY_SECONDS", "3600") or "3600")),
                "initial_delay_seconds": int(float(os.getenv("LAND_INFO_SYNC_INITIAL_DELAY_SECONDS", "0") or "0")),
                "poll_interval_seconds": int(float(os.getenv("LAND_INFO_SYNC_POLL_INTERVAL_SECONDS", "30") or "30")),
                "import_timeout_seconds": int(float(os.getenv("LAND_INFO_SYNC_IMPORT_TIMEOUT_SECONDS", "86400") or "86400")),
                "stable_seconds": int(float(os.getenv("LAND_INFO_SYNC_STABLE_SECONDS", "60") or "60")),
                "cleanup_on_success": str(os.getenv("LAND_INFO_SYNC_CLEANUP_ON_SUCCESS", "1")).strip().lower()
                in {"1", "true", "t", "yes", "y", "on"},
            },
        }
    )


@app.get("/v1/admin/cadastral/releases")
def list_cadastral_releases(
    status: str | None = Query(None),
    data_type: str | None = Query(None),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    status_upper = status.upper() if status else None
    normalized_type = _normalize_data_type(data_type) if data_type else None

    if status_upper and status_upper not in VALID_RELEASE_STATUSES:
        raise HTTPException(status_code=400, detail=f"invalid status: {status}")

    query = """
        SELECT
          id, version, data_type, source_name, status, is_active, records_count,
          metadata, created_at, updated_at, activated_at
        FROM cadastral_release
        {where_clause}
        ORDER BY id DESC
        LIMIT %s OFFSET %s
    """
    params: list[Any] = []
    clauses: list[str] = []
    if status_upper:
        clauses.append("status = %s")
        params.append(status_upper)
    if normalized_type:
        clauses.append("data_type = %s")
        params.append(normalized_type)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.extend([limit, offset])
    query = query.format(where_clause=where_clause)

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to list releases: {exc}")

    return ok([_release_row_to_dict(row) for row in rows])


@app.get("/v1/admin/cadastral/storage-summary")
def get_storage_summary(
    top_table_limit: int = Query(20, ge=1, le=200),
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    def _size_row_to_dict(row: tuple[Any, ...]) -> Dict[str, Any]:
        return {
            "name": str(row[0] or ""),
            "total_bytes": int(row[1] or 0),
            "table_bytes": int(row[2] or 0),
            "indexes_bytes": int(row[3] or 0),
            "toast_bytes": int(row[4] or 0),
        }

    measured_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), pg_database_size(current_database())")
                db_row = cur.fetchone() or ("", 0)
                db_name = str(db_row[0] or "")
                db_size_bytes = int(db_row[1] or 0)

                cur.execute(
                    """
                    SELECT
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    FROM cadastral_release
                    WHERE is_active = TRUE
                    ORDER BY data_type, id DESC
                    """
                )
                active_release_rows = cur.fetchall()
                active_releases = [_release_row_to_dict(row) for row in active_release_rows]
                active_release_ids = [
                    int(item["id"]) for item in active_releases if item.get("id") is not None
                ]

                partition_names = [f"dataset_record_r{release_id}" for release_id in active_release_ids]
                tracked_names = [
                    "dataset_pnu_kv",
                    "building_info_line",
                    "building_info_lookup",
                    "land_info_record",
                    "cadastral_features",
                    "dataset_record",
                    "dataset_record_p_default",
                    *partition_names,
                ]
                seen_names: set[str] = set()
                tracked_names = [name for name in tracked_names if not (name in seen_names or seen_names.add(name))]

                cur.execute(
                    """
                    SELECT
                      relname,
                      pg_total_relation_size(relid) AS total_bytes,
                      pg_relation_size(relid) AS table_bytes,
                      pg_indexes_size(relid) AS indexes_bytes,
                      pg_total_relation_size(relid)
                        - pg_relation_size(relid)
                        - pg_indexes_size(relid) AS toast_bytes
                    FROM pg_catalog.pg_statio_user_tables
                    WHERE relname = ANY(%s)
                    ORDER BY pg_total_relation_size(relid) DESC
                    """,
                    (tracked_names,),
                )
                tracked_tables = [_size_row_to_dict(row) for row in cur.fetchall()]
                sizes_by_name = {row["name"]: row for row in tracked_tables}

                cur.execute(
                    """
                    SELECT
                      relname,
                      pg_total_relation_size(relid) AS total_bytes,
                      pg_relation_size(relid) AS table_bytes,
                      pg_indexes_size(relid) AS indexes_bytes,
                      pg_total_relation_size(relid)
                        - pg_relation_size(relid)
                        - pg_indexes_size(relid) AS toast_bytes
                    FROM pg_catalog.pg_statio_user_tables
                    ORDER BY pg_total_relation_size(relid) DESC
                    LIMIT %s
                    """,
                    (int(top_table_limit),),
                )
                top_tables = [_size_row_to_dict(row) for row in cur.fetchall()]

                kv_logical: list[Dict[str, Any]] = []
                kv_by_release_type: dict[tuple[int, str], Dict[str, Any]] = {}
                if active_release_ids:
                    cur.execute(
                        """
                        SELECT
                          release_id,
                          data_type,
                          COUNT(*) AS rows,
                          COALESCE(SUM(pg_column_size(payload)), 0) AS payload_bytes
                        FROM dataset_pnu_kv
                        WHERE release_id = ANY(%s)
                        GROUP BY release_id, data_type
                        ORDER BY payload_bytes DESC
                        """,
                        (active_release_ids,),
                    )
                    for row in cur.fetchall():
                        item = {
                            "release_id": int(row[0] or 0),
                            "data_type": str(row[1] or "").strip().lower(),
                            "rows": int(row[2] or 0),
                            "payload_bytes": int(row[3] or 0),
                        }
                        kv_logical.append(item)
                        kv_by_release_type[(item["release_id"], item["data_type"])] = item

                data_types: list[Dict[str, Any]] = []
                for release in active_releases:
                    release_id = int(release["id"])
                    data_type = str(release.get("data_type") or "cadastral").strip().lower()
                    parts: list[Dict[str, Any]] = []

                    partition_name = f"dataset_record_r{release_id}"
                    part = sizes_by_name.get(partition_name)
                    if part:
                        parts.append(part)

                    if data_type == "cadastral":
                        cf = sizes_by_name.get("cadastral_features")
                        if cf:
                            parts.append(cf)
                    elif data_type == "building_info":
                        bi_line = sizes_by_name.get("building_info_line")
                        if bi_line:
                            parts.append(bi_line)
                        bi_lookup = sizes_by_name.get("building_info_lookup")
                        if bi_lookup:
                            parts.append(bi_lookup)
                    elif data_type == "land_info":
                        li = sizes_by_name.get("land_info_record")
                        if li:
                            parts.append(li)

                    storage_total_bytes = sum(int(item.get("total_bytes") or 0) for item in parts)
                    kv_item = kv_by_release_type.get((release_id, data_type))
                    data_types.append(
                        {
                            "data_type": data_type,
                            "active_release": release,
                            "storage_parts": parts,
                            "storage_total_bytes": storage_total_bytes,
                            "kv_logical": kv_item,
                        }
                    )

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load storage summary: {exc}")

    return ok(
        {
            "measured_at": measured_at,
            "db": {"name": db_name, "size_bytes": db_size_bytes},
            "data_types": data_types,
            "tracked_tables": tracked_tables,
            "top_tables": top_tables,
            "kv_logical": kv_logical,
        }
    )


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_open_import_job_ids_for_data_type(normalized_type: str) -> list[int]:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT j.id
                    FROM cadastral_import_job j
                    LEFT JOIN cadastral_release r ON r.id = j.release_id
                    WHERE COALESCE(j.data_type, r.data_type, 'cadastral') = %s
                      AND j.status IN ('QUEUED', 'RUNNING')
                    ORDER BY j.id DESC
                    """,
                    (normalized_type,),
                )
                return [int(row[0]) for row in cur.fetchall()]
    except Exception as exc:
        raise RuntimeError(f"failed to load running jobs: {exc}") from exc


def _clear_data_type_db_and_storage(normalized_type: str, open_job_ids: list[int]) -> Dict[str, Any]:
    all_job_ids: list[int] = []
    release_ids: list[int] = []

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT j.id
                        FROM cadastral_import_job j
                        LEFT JOIN cadastral_release r ON r.id = j.release_id
                        WHERE COALESCE(j.data_type, r.data_type, 'cadastral') = %s
                        ORDER BY j.id DESC
                        """,
                        (normalized_type,),
                    )
                    all_job_ids = [int(row[0]) for row in cur.fetchall()]

                    cur.execute(
                        """
                        DELETE FROM cadastral_release
                        WHERE data_type = %s
                        RETURNING id
                        """,
                        (normalized_type,),
                    )
                    release_ids = [int(row[0]) for row in cur.fetchall()]
                    for deleted_release_id in release_ids:
                        with contextlib.suppress(Exception):
                            cur.execute("SELECT drop_dataset_record_partition(%s)", (deleted_release_id,))
    except Exception as exc:
        raise RuntimeError(f"failed to clear data_type: {exc}") from exc

    _clear_active_release_cache(normalized_type)

    cleanup_errors: list[str] = []
    upload_dir = _upload_base_dir() / normalized_type
    upload_dir_removed = False
    if upload_dir.exists():
        try:
            shutil.rmtree(upload_dir)
            upload_dir_removed = True
        except Exception as exc:
            cleanup_errors.append(f"upload dir cleanup failed: {exc}")

    tile_cache_dir = Path(os.getenv("TILE_CACHE_DIR", "/data/uploads/tile_cache")) / "cadastral"
    tile_cache_removed = False
    if normalized_type == "cadastral":
        _tile_memory_cache().clear()
        if tile_cache_dir.exists():
            try:
                shutil.rmtree(tile_cache_dir)
                tile_cache_removed = True
            except Exception as exc:
                cleanup_errors.append(f"tile cache cleanup failed: {exc}")

    return {
        "data_type": normalized_type,
        "cancelled_open_job_ids": open_job_ids,
        "deleted_job_ids": all_job_ids,
        "deleted_release_ids": release_ids,
        "deleted_jobs": len(all_job_ids),
        "deleted_releases": len(release_ids),
        "upload_dir": str(upload_dir),
        "upload_dir_removed": upload_dir_removed,
        "tile_cache_dir": str(tile_cache_dir) if normalized_type == "cadastral" else None,
        "tile_cache_removed": tile_cache_removed,
        "cleanup_errors": cleanup_errors,
    }


async def _run_clear_data_type_once(normalized_type: str) -> Dict[str, Any]:
    open_job_ids = await asyncio.to_thread(_load_open_import_job_ids_for_data_type, normalized_type)
    for job_id in open_job_ids:
        await _cancel_import_job_runner(job_id)
    return await asyncio.to_thread(_clear_data_type_db_and_storage, normalized_type, open_job_ids)


async def _update_clear_data_type_status(
    normalized_type: str,
    *,
    job_id: int,
    status: str,
    started_at: str | None = None,
    finished_at: str | None = None,
    error_message: str | None = None,
    result: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    now = _utc_now_iso()
    async with _CLEAR_DATA_TYPE_GUARD:
        current = _CLEAR_DATA_TYPE_STATUS.get(normalized_type)
        if current and int(current.get("job_id") or 0) != int(job_id):
            return dict(current)

        record = {
            "job_id": int(job_id),
            "data_type": normalized_type,
            "status": status,
            "started_at": started_at or (current.get("started_at") if current else now),
            "updated_at": now,
            "finished_at": finished_at,
            "error_message": error_message,
            "result": result,
        }
        _CLEAR_DATA_TYPE_STATUS[normalized_type] = record
        return dict(record)


async def _start_clear_data_type_background_job(normalized_type: str) -> Dict[str, Any]:
    async with _CLEAR_DATA_TYPE_GUARD:
        running = _CLEAR_DATA_TYPE_TASKS.get(normalized_type)
        current = _CLEAR_DATA_TYPE_STATUS.get(normalized_type)
        if running is not None and not running.done() and current:
            payload = dict(current)
            payload["already_running"] = True
            return payload

        started_at = _utc_now_iso()
        job_id = int(time.time() * 1000)
        initial = {
            "job_id": job_id,
            "data_type": normalized_type,
            "status": "RUNNING",
            "started_at": started_at,
            "updated_at": started_at,
            "finished_at": None,
            "error_message": None,
            "result": None,
        }
        _CLEAR_DATA_TYPE_STATUS[normalized_type] = initial
        task = asyncio.create_task(_run_clear_data_type_background_job(normalized_type, job_id))
        _CLEAR_DATA_TYPE_TASKS[normalized_type] = task

    payload = dict(initial)
    payload["already_running"] = False
    return payload


async def _run_clear_data_type_background_job(normalized_type: str, job_id: int) -> None:
    result: Dict[str, Any] | None = None
    error_message: str | None = None
    status = "SUCCEEDED"

    try:
        result = await _run_clear_data_type_once(normalized_type)
    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)[:2000]

    finished_at = _utc_now_iso()
    await _update_clear_data_type_status(
        normalized_type,
        job_id=job_id,
        status=status,
        finished_at=finished_at,
        error_message=error_message,
        result=result,
    )

    async with _CLEAR_DATA_TYPE_GUARD:
        current_task = _CLEAR_DATA_TYPE_TASKS.get(normalized_type)
        if current_task is asyncio.current_task():
            _CLEAR_DATA_TYPE_TASKS.pop(normalized_type, None)


async def _get_clear_data_type_status(normalized_type: str) -> Dict[str, Any]:
    async with _CLEAR_DATA_TYPE_GUARD:
        record = _CLEAR_DATA_TYPE_STATUS.get(normalized_type)
        if record:
            return dict(record)
    return {
        "job_id": None,
        "data_type": normalized_type,
        "status": "IDLE",
        "started_at": None,
        "updated_at": None,
        "finished_at": None,
        "error_message": None,
        "result": None,
    }


@app.post("/v1/admin/cadastral/data-types/{data_type}/clear")
async def clear_cadastral_data_type(
    data_type: str,
    body: Dict[str, Any],
    background: bool = Query(False),
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    normalized_type = _normalize_data_type(data_type)

    if not _to_bool(body.get("confirm"), False):
        raise HTTPException(status_code=400, detail="confirm=true is required")

    if not background:
        try:
            return ok(await _run_clear_data_type_once(normalized_type))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    return ok(await _start_clear_data_type_background_job(normalized_type))


@app.get("/v1/admin/cadastral/data-types/{data_type}/clear-status")
async def clear_cadastral_data_type_status(
    data_type: str,
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    normalized_type = _normalize_data_type(data_type)
    return ok(await _get_clear_data_type_status(normalized_type))


@app.post("/v1/admin/cadastral/releases")
def create_cadastral_release(
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    version = str(body.get("version", "")).strip()
    if not version:
        raise HTTPException(status_code=400, detail="version is required")
    data_type = _normalize_data_type(str(body.get("data_type", "cadastral")))

    source_name = str(body.get("source_name", "")).strip() or None
    metadata = body.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cadastral_release (version, data_type, source_name, status, metadata)
                    VALUES (%s, %s, %s, 'PENDING', %s::jsonb)
                    RETURNING
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    """,
                    (version, data_type, source_name, json.dumps(metadata, ensure_ascii=False)),
                )
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to create release: {exc}")

    return ok(_release_row_to_dict(row))


@app.post("/v1/admin/cadastral/releases/{release_id}/activate")
def activate_cadastral_release(
    release_id: int,
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data_type
                    FROM cadastral_release
                    WHERE id = %s
                    """,
                    (release_id,),
                )
                release_info = cur.fetchone()
                if not release_info:
                    raise HTTPException(status_code=404, detail="release not found")
                target_type = _normalize_data_type(str(release_info[0] or "cadastral"))

                cur.execute(
                    """
                    UPDATE cadastral_release
                    SET is_active = FALSE,
                        status = CASE WHEN status = 'ACTIVE' THEN 'READY' ELSE status END,
                        updated_at = NOW()
                    WHERE is_active = TRUE
                      AND data_type = %s
                    """,
                    (target_type,),
                )
                cur.execute(
                    """
                    UPDATE cadastral_release
                    SET is_active = TRUE,
                        status = 'ACTIVE',
                        activated_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                      AND data_type = %s
                    RETURNING
                      id, version, data_type, source_name, status, is_active, records_count,
                      metadata, created_at, updated_at, activated_at
                    """,
                    (release_id, target_type),
                )
                row = cur.fetchone()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to activate release: {exc}")

    if not row:
        raise HTTPException(status_code=404, detail="release not found")

    _clear_active_release_cache(row[2] if len(row) > 2 else "cadastral")
    return ok(_release_row_to_dict(row))


@app.patch("/v1/admin/cadastral/releases/{release_id}")
def update_cadastral_release(
    release_id: int,
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    status = body.get("status")
    records_count = body.get("records_count")

    updates: list[str] = []
    params: list[Any] = []

    if status is not None:
        status_upper = str(status).upper()
        if status_upper not in VALID_RELEASE_STATUSES:
            raise HTTPException(status_code=400, detail=f"invalid status: {status}")
        updates.append("status = %s")
        params.append(status_upper)

    if records_count is not None:
        updates.append("records_count = %s")
        params.append(int(records_count))

    metadata = body.get("metadata")
    if metadata is not None:
        if not isinstance(metadata, dict):
            raise HTTPException(status_code=400, detail="metadata must be object")
        updates.append("metadata = %s::jsonb")
        params.append(json.dumps(metadata, ensure_ascii=False))

    if not updates:
        raise HTTPException(status_code=400, detail="no update fields")

    updates.append("updated_at = NOW()")
    set_clause = ", ".join(updates)
    params.append(release_id)

    query = f"""
        UPDATE cadastral_release
        SET {set_clause}
        WHERE id = %s
        RETURNING
          id, version, data_type, source_name, status, is_active, records_count,
          metadata, created_at, updated_at, activated_at
    """

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to update release: {exc}")

    if not row:
        raise HTTPException(status_code=404, detail="release not found")

    return ok(_release_row_to_dict(row))


@app.get("/v1/admin/cadastral/import-jobs")
def list_cadastral_import_jobs(
    release_id: int | None = Query(None),
    data_type: str | None = Query(None),
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    normalized_type = _normalize_data_type(data_type) if data_type else None

    query = """
        SELECT
          j.id, j.release_id, r.version, j.status, j.source_path,
          j.total_files, j.processed_files, j.inserted_rows, j.error_message,
          j.created_at, j.started_at, j.finished_at, j.updated_at,
          COALESCE(j.data_type, r.data_type, 'cadastral') AS data_type
        FROM cadastral_import_job j
        LEFT JOIN cadastral_release r ON r.id = j.release_id
        {where_clause}
        ORDER BY j.id DESC
        LIMIT %s OFFSET %s
    """
    params: list[Any] = []
    clauses: list[str] = []
    if release_id is not None:
        clauses.append("j.release_id = %s")
        params.append(release_id)
    if normalized_type is not None:
        clauses.append("COALESCE(j.data_type, r.data_type, 'cadastral') = %s")
        params.append(normalized_type)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.extend([limit, offset])
    query = query.format(where_clause=where_clause)

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to list import jobs: {exc}")

    return ok([_import_job_row_to_dict(row) for row in rows])


@app.get("/v1/admin/cadastral/import-job-workers")
def list_cadastral_import_job_workers(
    job_id: int | None = Query(None),
    data_type: str | None = Query(None),
    active_only: bool = Query(True),
    limit: int = Query(600, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    normalized_type = _normalize_data_type(data_type) if data_type else None

    clauses: list[str] = []
    params: list[Any] = []
    if job_id is not None:
        clauses.append("w.job_id = %s")
        params.append(int(job_id))
    if normalized_type is not None:
        clauses.append("COALESCE(j.data_type, r.data_type, 'cadastral') = %s")
        params.append(normalized_type)
    if active_only:
        clauses.append("w.status IN ('QUEUED', 'RUNNING')")

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT
          w.id,
          w.job_id,
          j.release_id,
          r.version,
          COALESCE(j.data_type, r.data_type, 'cadastral') AS data_type,
          w.source_file,
          w.worker_name,
          w.status,
          w.processed_rows,
          w.error_message,
          w.started_at,
          w.finished_at,
          w.updated_at
        FROM cadastral_import_job_worker w
        JOIN cadastral_import_job j ON j.id = w.job_id
        LEFT JOIN cadastral_release r ON r.id = j.release_id
        {where_clause}
        ORDER BY w.job_id DESC, w.id ASC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
    except Exception as exc:
        if "cadastral_import_job_worker" in str(exc):
            return ok([])
        raise HTTPException(status_code=500, detail=f"failed to list import job workers: {exc}")

    return ok([_import_job_worker_row_to_dict(row) for row in rows])


@app.get("/v1/admin/cadastral/update-files")
def list_cadastral_update_files(
    data_type: str | None = Query(None),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    normalized_type = _normalize_data_type(data_type) if data_type else None

    try:
        with psycopg.connect(_db_url()) as conn:
            rows = _load_recent_update_file_rows(
                conn,
                data_type=normalized_type,
                limit=limit,
                offset=offset,
            )
    except Exception as exc:
        if "dataset_import_file" in str(exc):
            return ok([])
        raise HTTPException(status_code=500, detail=f"failed to list update files: {exc}")

    return ok([_update_file_row_to_dict(row) for row in rows])


@app.get("/v1/admin/cadastral/import-path-options")
def list_cadastral_import_path_options(
    data_type: str = Query("cadastral"),
    operation_mode: str = Query("full"),
    max_depth: int = Query(3, ge=0, le=8),
    limit: int = Query(300, ge=1, le=2000),
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    try:
        payload = _scan_import_path_options(
            data_type,
            operation_mode=operation_mode,
            max_depth=max_depth,
            limit=limit,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to list import paths: {exc}")
    return ok(payload)


@app.get("/v1/admin/cadastral/events")
async def stream_cadastral_admin_events(
    request: Request,
    x_admin_token: str | None = Header(default=None),
    admin_token: str | None = Query(default=None),
    release_limit: int = Query(100, ge=1, le=200),
    job_limit: int = Query(200, ge=1, le=200),
    worker_limit: int = Query(600, ge=1, le=2000),
    interval_ms: int = Query(2500, ge=1000, le=30000),
) -> StreamingResponse:
    cookie_token = request.cookies.get("cadastral_admin_token")
    _require_admin(x_admin_token, admin_token or cookie_token)

    async def _stream() -> Any:
        last_payload = ""
        event_id = 0
        interval_sec = interval_ms / 1000.0

        while True:
            if await request.is_disconnected():
                break

            try:
                snapshot = await asyncio.to_thread(
                    _admin_cadastral_snapshot,
                    release_limit,
                    job_limit,
                    worker_limit,
                )
                payload = json.dumps(
                    jsonable_encoder(snapshot),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )

                if payload != last_payload:
                    event_id += 1
                    last_payload = payload
                    yield f"id: {event_id}\n"
                    yield "event: snapshot\n"
                    yield f"data: {payload}\n\n"
                else:
                    yield "event: ping\n"
                    yield 'data: {"ok":true}\n\n'
            except Exception as exc:
                event_id += 1
                error_payload = json.dumps(
                    {"message": f"snapshot failed: {exc}"},
                    ensure_ascii=False,
                )
                yield f"id: {event_id}\n"
                yield "event: error\n"
                yield f"data: {error_payload}\n\n"

            await asyncio.sleep(interval_sec)

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/v1/admin/server-logs/events")
async def stream_admin_server_logs(
    request: Request,
    x_admin_token: str | None = Header(default=None),
    admin_token: str | None = Query(default=None),
    interval_ms: int = Query(1200, ge=500, le=10000),
    tail_lines: int = Query(120, ge=0, le=1000),
) -> StreamingResponse:
    cookie_token = request.cookies.get("cadastral_admin_token")
    _require_admin(x_admin_token, admin_token or cookie_token)

    _configure_server_log_file_handler()

    async def _stream() -> Any:
        event_id = 0
        interval_sec = interval_ms / 1000.0
        log_path = SERVER_LOG_FILE
        position = 0

        try:
            if tail_lines > 0:
                for line in _read_log_tail_lines(log_path, tail_lines):
                    event_id += 1
                    payload = json.dumps({"line": line}, ensure_ascii=False, separators=(",", ":"))
                    yield f"id: {event_id}\n"
                    yield "event: line\n"
                    yield f"data: {payload}\n\n"
            if log_path.exists():
                position = log_path.stat().st_size
        except Exception as exc:
            event_id += 1
            payload = json.dumps({"line": f"[tail-read-error] {exc}"}, ensure_ascii=False, separators=(",", ":"))
            yield f"id: {event_id}\n"
            yield "event: line\n"
            yield f"data: {payload}\n\n"

        while True:
            if await request.is_disconnected():
                break

            try:
                if not log_path.exists():
                    yield "event: ping\n"
                    yield 'data: {"ok":true}\n\n'
                    await asyncio.sleep(interval_sec)
                    continue

                size = log_path.stat().st_size
                if size < position:
                    position = 0

                if size > position:
                    with log_path.open("r", encoding="utf-8", errors="replace") as f:
                        f.seek(position)
                        chunk = f.read()
                        position = f.tell()

                    for raw in chunk.splitlines():
                        event_id += 1
                        payload = json.dumps({"line": raw}, ensure_ascii=False, separators=(",", ":"))
                        yield f"id: {event_id}\n"
                        yield "event: line\n"
                        yield f"data: {payload}\n\n"
                else:
                    yield "event: ping\n"
                    yield 'data: {"ok":true}\n\n'
            except Exception as exc:
                event_id += 1
                payload = json.dumps({"line": f"[stream-error] {exc}"}, ensure_ascii=False, separators=(",", ":"))
                yield f"id: {event_id}\n"
                yield "event: line\n"
                yield f"data: {payload}\n\n"

            await asyncio.sleep(interval_sec)

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/v1/admin/cadastral/import-jobs")
def create_cadastral_import_job(
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    release_id = body.get("release_id")
    source_path = str(body.get("source_path", "")).strip()
    total_files = int(body.get("total_files", 0))

    if release_id is None:
        raise HTTPException(status_code=400, detail="release_id is required")
    if not source_path:
        raise HTTPException(status_code=400, detail="source_path is required")

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data_type
                    FROM cadastral_release
                    WHERE id = %s
                    """,
                    (int(release_id),),
                )
                release_row = cur.fetchone()
                if not release_row:
                    raise HTTPException(status_code=404, detail="release not found")
                data_type = _normalize_data_type(str(release_row[0] or "cadastral"))
                _ensure_no_open_import_job_for_data_type(conn, data_type)

                cur.execute(
                    """
                    INSERT INTO cadastral_import_job
                      (release_id, data_type, status, source_path, total_files)
                    VALUES (%s, %s, 'QUEUED', %s, %s)
                    RETURNING
                      id, release_id, NULL::text, status, source_path,
                      total_files, processed_files, inserted_rows, error_message,
                      created_at, started_at, finished_at, updated_at, data_type
                    """,
                    (int(release_id), data_type, source_path, total_files),
                )
                row = cur.fetchone()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to create import job: {exc}")

    return ok(_import_job_row_to_dict(row))


@app.post("/v1/admin/cadastral/upload-and-import")
async def upload_and_import_cadastral(
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)
    raise HTTPException(
        status_code=410,
        detail="파일 업로드 방식은 비활성화되었습니다. 서버 경로 적재를 사용하세요.",
    )


@app.post("/v1/admin/cadastral/import-from-path")
async def import_cadastral_from_server_path(
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    data_type_normalized = _normalize_data_type(str(body.get("data_type", "cadastral")))
    operation_mode = _normalize_operation_mode(str(body.get("operation_mode", "full")))
    is_full_mode = operation_mode == "full"
    is_update_mode = operation_mode == "update"
    source_path = str(body.get("source_path", "")).strip()
    if not source_path:
        raise HTTPException(status_code=400, detail="source_path is required")

    source_dir = Path(source_path).resolve()
    if not source_dir.exists() or not source_dir.is_dir():
        raise HTTPException(status_code=400, detail=f"source_path not found or not directory: {source_dir}")

    requested_pattern = str(body.get("pattern", "")).strip()
    pattern = requested_pattern or _choose_import_pattern(source_dir, data_type_normalized)
    total_files = _count_pattern_files(source_dir, pattern)
    if total_files < 1:
        raise HTTPException(status_code=400, detail=f"적재 대상 파일이 없습니다 (pattern={pattern})")

    active_release = _active_release(data_type_normalized, force_refresh=True)
    normalized_version = str(body.get("version", "")).strip()
    if is_update_mode:
        if not active_release:
            raise HTTPException(
                status_code=409,
                detail=f"update mode requires active release: data_type={data_type_normalized}",
            )
        if not normalized_version:
            normalized_version = str(active_release.get("version") or "")
        if not normalized_version:
            normalized_version = _next_auto_release_version()
    else:
        if not normalized_version:
            normalized_version = _next_auto_release_version()

    source_name_input = str(body.get("source_name", "")).strip()
    mark_ready = _to_bool(body.get("mark_ready"), True)
    default_batch_size = _default_batch_size_for_data_type(data_type_normalized, operation_mode)
    try:
        safe_batch_size = max(100, int(body.get("batch_size", default_batch_size)))
    except Exception:
        safe_batch_size = default_batch_size

    used_existing_release = False
    release_id = 0
    job_id = 0

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.transaction():
                _ensure_no_open_import_job_for_data_type(conn, data_type_normalized)
                with conn.cursor() as cur:
                    existing_release = None
                    if is_update_mode and active_release:
                        existing_release = (int(active_release["id"]),)
                    elif not is_full_mode:
                        cur.execute(
                            """
                            SELECT id
                            FROM cadastral_release
                            WHERE version = %s
                              AND data_type = %s
                            LIMIT 1
                            """,
                            (normalized_version, data_type_normalized),
                        )
                        existing_release = cur.fetchone()

                    if is_full_mode:
                        cur.execute(
                            """
                            SELECT 1
                            FROM cadastral_release
                            WHERE version = %s
                              AND data_type = %s
                            LIMIT 1
                            """,
                            (normalized_version, data_type_normalized),
                        )
                        if cur.fetchone():
                            normalized_version = f"{normalized_version}-{_next_auto_release_version()}"

                    normalized_source_name = (
                        source_name_input or f"서버경로({data_type_normalized}) {normalized_version}"
                    )
                    metadata_patch = json.dumps(
                        {
                            "trigger": "server_path",
                            "data_type": data_type_normalized,
                            "source_path": str(source_dir),
                            "pattern": pattern,
                            "total_files": total_files,
                            "operation_mode": operation_mode,
                        },
                        ensure_ascii=False,
                    )

                    if existing_release:
                        used_existing_release = True
                        release_id = int(existing_release[0])
                        cur.execute(
                            """
                            UPDATE cadastral_release
                            SET source_name = %s,
                                status = 'PENDING',
                                metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                                updated_at = NOW()
                            WHERE id = %s
                              AND data_type = %s
                            """,
                            (
                                normalized_source_name,
                                metadata_patch,
                                release_id,
                                data_type_normalized,
                            ),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO cadastral_release (version, data_type, source_name, status, metadata)
                            VALUES (%s, %s, %s, 'PENDING', %s::jsonb)
                            RETURNING id
                            """,
                            (
                                normalized_version,
                                data_type_normalized,
                                normalized_source_name,
                                metadata_patch,
                            ),
                        )
                        release_row = cur.fetchone()
                        if not release_row:
                            raise HTTPException(status_code=500, detail="release 생성 실패")
                        release_id = int(release_row[0])

                    cur.execute(
                        """
                        INSERT INTO cadastral_import_job
                          (release_id, data_type, status, source_path, total_files)
                        VALUES (%s, %s, 'QUEUED', %s, %s)
                        RETURNING id
                        """,
                        (release_id, data_type_normalized, str(source_dir), total_files),
                    )
                    job_row = cur.fetchone()
                    if not job_row:
                        raise HTTPException(status_code=500, detail="import job 생성 실패")
                    job_id = int(job_row[0])
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"서버 경로 적재 시작 실패: {exc}")

    run_state = await _start_import_job_runner(
        job_id=job_id,
        release_id=release_id,
        data_type=data_type_normalized,
        source_path=str(source_dir),
        pattern=pattern,
        batch_size=safe_batch_size,
        truncate_release=(used_existing_release and is_full_mode),
        merge_by_pnu=False,
        operation_mode=operation_mode,
        mark_ready=bool(mark_ready),
        activate_on_complete=True,
        cleanup_old_releases_on_success=is_full_mode,
    )

    return ok(
        {
            "mode": "update" if used_existing_release else "create",
            "data_type": data_type_normalized,
            "release_id": release_id,
            "release_version": normalized_version,
            "job_id": job_id,
            "source_path": str(source_dir),
            "pattern": pattern,
            "total_files": total_files,
            "operation_mode": operation_mode,
            "full_replace": is_full_mode,
            "cleanup_old_releases_on_success": bool(
                run_state.get("cleanup_old_releases_on_success", False)
            ),
            "mark_ready": bool(mark_ready),
            "activate_on_complete": True,
            "truncate_if_exists": bool(run_state.get("truncate_release", False)),
            "run": run_state,
        }
    )


@app.patch("/v1/admin/cadastral/import-jobs/{job_id}")
async def update_cadastral_import_job(
    job_id: int,
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    updates: list[str] = []
    params: list[Any] = []

    requested_status: str | None = None
    if "status" in body:
        requested_status = str(body["status"]).upper()
        if requested_status not in VALID_IMPORT_JOB_STATUSES:
            raise HTTPException(status_code=400, detail=f"invalid job status: {requested_status}")
        updates.append("status = %s")
        params.append(requested_status)
        if requested_status == "RUNNING":
            updates.append("started_at = COALESCE(started_at, NOW())")
        if requested_status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            updates.append("finished_at = NOW()")

    if "processed_files" in body:
        updates.append("processed_files = %s")
        params.append(int(body["processed_files"]))

    if "inserted_rows" in body:
        updates.append("inserted_rows = %s")
        params.append(int(body["inserted_rows"]))

    if "error_message" in body:
        updates.append("error_message = %s")
        params.append(body["error_message"])

    if not updates:
        raise HTTPException(status_code=400, detail="no update fields")

    updates.append("updated_at = NOW()")
    set_clause = ", ".join(updates)
    params.append(job_id)

    query = f"""
        UPDATE cadastral_import_job
        SET {set_clause}
        WHERE id = %s
        RETURNING
          id, release_id, NULL::text, status, source_path,
          total_files, processed_files, inserted_rows, error_message,
          created_at, started_at, finished_at, updated_at, data_type
    """

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to update import job: {exc}")

    if not row:
        raise HTTPException(status_code=404, detail="import job not found")

    if requested_status == "CANCELLED":
        await _cancel_import_job_runner(job_id)
        release_id = row[1]
        release_data_type = str(row[13] or "cadastral")
        try:
            with psycopg.connect(_db_url()) as conn:
                with conn.transaction():
                    conn.execute(
                        """
                        UPDATE cadastral_import_job_worker
                        SET status = 'CANCELLED',
                            error_message = CASE
                                WHEN COALESCE(error_message, '') = '' THEN 'cancelled by admin'
                                ELSE error_message
                            END,
                            finished_at = COALESCE(finished_at, NOW()),
                            updated_at = NOW()
                        WHERE job_id = %s
                          AND status IN ('QUEUED', 'RUNNING')
                        """,
                        (job_id,),
                    )
                    conn.execute(
                        """
                        UPDATE cadastral_release
                        SET status = CASE WHEN is_active THEN 'ACTIVE' ELSE 'FAILED' END,
                            updated_at = NOW()
                        WHERE id = %s
                          AND status = 'IMPORTING'
                        """,
                        (release_id,),
                    )
        except Exception:
            pass
        cleaned, reason = _cleanup_failed_or_cancelled_release_partition(int(release_id), release_data_type)
        logger = logging.getLogger("app.import")
        if cleaned:
            logger.info(
                "cancelled release partition cleanup done: data_type=%s release_id=%s job_id=%s",
                release_data_type,
                release_id,
                job_id,
            )
        elif reason:
            logger.warning(
                "cancelled release partition cleanup skipped: data_type=%s release_id=%s job_id=%s reason=%s",
                release_data_type,
                release_id,
                job_id,
                reason,
            )

    return ok(_import_job_row_to_dict(row))


@app.post("/v1/admin/cadastral/import-jobs/{job_id}/run")
async def run_cadastral_import_job(
    job_id: int,
    body: Dict[str, Any],
    x_admin_token: str | None = Header(default=None),
) -> Dict[str, Any]:
    _require_admin(x_admin_token)

    operation_mode = _normalize_operation_mode(str(body.get("operation_mode", "full")))
    truncate_release = operation_mode == "full"
    mark_ready = bool(body.get("mark_ready", True))
    activate_on_complete = bool(body.get("activate_on_complete", True))

    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      j.id,
                      j.release_id,
                      j.status,
                      j.source_path,
                      COALESCE(j.data_type, r.data_type, 'cadastral') AS data_type
                    FROM cadastral_import_job j
                    LEFT JOIN cadastral_release r ON r.id = j.release_id
                    WHERE j.id = %s
                    """,
                    (job_id,),
                )
                job_row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load import job: {exc}")

    if not job_row:
        raise HTTPException(status_code=404, detail="import job not found")

    _, release_id, status, source_path, data_type = job_row
    if status == "RUNNING":
        raise HTTPException(status_code=409, detail="import job status is already RUNNING")

    normalized_type = _normalize_data_type(str(data_type or "cadastral"))
    default_batch_size = _default_batch_size_for_data_type(normalized_type, operation_mode)
    try:
        batch_size = max(100, int(body.get("batch_size", default_batch_size)))
    except Exception:
        batch_size = default_batch_size
    default_pattern = _default_import_pattern_for_data_type(normalized_type)
    pattern = str(body.get("pattern", default_pattern)).strip() or default_pattern
    try:
        with psycopg.connect(_db_url()) as conn:
            _ensure_no_open_import_job_for_data_type(
                conn,
                normalized_type,
                exclude_job_id=job_id,
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to validate import job concurrency: {exc}")

    run_state = await _start_import_job_runner(
        job_id=job_id,
        release_id=release_id,
        data_type=normalized_type,
        source_path=str(source_path),
        pattern=pattern,
        batch_size=batch_size,
        truncate_release=truncate_release,
        merge_by_pnu=False,
        operation_mode=operation_mode,
        mark_ready=mark_ready,
        activate_on_complete=activate_on_complete,
        cleanup_old_releases_on_success=(operation_mode == "full"),
    )
    return ok(run_state)


@app.get("/v1/simple-data/{doc_name}")
def simple_data(doc_name: str) -> Dict[str, Any]:
    return ok({"doc_name": doc_name, "data": {}})


_SITE_REPORT_USE_LABELS = {
    "house": "단독주택",
    "multi": "다가구/다세대",
    "retail": "근린생활시설",
    "mixed": "상가주택",
}

_SITE_REPORT_PRIORITY_LABELS = {
    "budget": "공사비 우선",
    "speed": "인허가 속도 우선",
    "yield": "수익성 우선",
    "balance": "균형안",
}

_SITE_REPORT_ENGINE_CASE_CACHE_LOCK = Lock()
_SITE_REPORT_ENGINE_CASE_CACHE: dict[str, Any] = {
    "path": "",
    "mtime": 0.0,
    "payload": None,
}


def _site_report_number(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).replace(",", "").strip()
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _site_report_short_address(address: str) -> str:
    parts = [part for part in str(address or "").split() if part]
    return " ".join(parts[:3]) if parts else "입력 대지"


def _site_report_money(value: float) -> str:
    safe = max(0, int(round(value)))
    if safe >= 10000:
        eok = safe // 10000
        rest = safe % 10000
        if rest:
            return f"{eok}억 {rest:,}만원"
        return f"{eok}억원"
    return f"{safe:,}만원"


def _site_report_cost_rate(use: str, priority: str) -> dict[str, float]:
    base_by_use = {
        "house": {"low": 850.0, "high": 1120.0},
        "multi": {"low": 780.0, "high": 980.0},
        "retail": {"low": 760.0, "high": 1040.0},
        "mixed": {"low": 820.0, "high": 1080.0},
    }
    base = dict(base_by_use.get(use, base_by_use["mixed"]))
    if priority == "budget":
        return {"low": base["low"] * 0.92, "high": base["high"] * 0.96}
    if priority == "yield":
        return {"low": base["low"] * 1.04, "high": base["high"] * 1.12}
    return base


def _site_report_floor_plan(use: str) -> dict[str, Any]:
    rooms_by_use = {
        "house": [
            {"label": "거실", "size": "12평", "x": 34, "y": 36, "w": 112, "h": 82, "accent": True},
            {"label": "주방", "size": "7평", "x": 183, "y": 36, "w": 84, "h": 82},
            {"label": "침실", "size": "8평", "x": 304, "y": 36, "w": 76, "h": 82},
            {"label": "안방", "size": "10평", "x": 34, "y": 154, "w": 112, "h": 164},
            {"label": "욕실", "size": "3평", "x": 183, "y": 154, "w": 82, "h": 62},
            {"label": "서재", "size": "5평", "x": 304, "y": 256, "w": 76, "h": 62},
        ],
        "multi": [
            {"label": "세대 A", "size": "14평", "x": 34, "y": 36, "w": 112, "h": 82, "accent": True},
            {"label": "세대 B", "size": "13평", "x": 183, "y": 36, "w": 197, "h": 82},
            {"label": "계단실", "size": "5평", "x": 34, "y": 154, "w": 112, "h": 164},
            {"label": "세대 C", "size": "12평", "x": 183, "y": 154, "w": 82, "h": 62},
            {"label": "세대 D", "size": "12평", "x": 304, "y": 256, "w": 76, "h": 62},
        ],
        "retail": [
            {"label": "매장", "size": "22평", "x": 34, "y": 36, "w": 112, "h": 82, "accent": True},
            {"label": "전시", "size": "12평", "x": 183, "y": 36, "w": 197, "h": 82},
            {"label": "창고", "size": "6평", "x": 34, "y": 154, "w": 112, "h": 164},
            {"label": "사무", "size": "5평", "x": 183, "y": 154, "w": 82, "h": 62},
            {"label": "화장실", "size": "3평", "x": 304, "y": 256, "w": 76, "h": 62},
        ],
        "mixed": [
            {"label": "상가", "size": "16평", "x": 34, "y": 36, "w": 112, "h": 82, "accent": True},
            {"label": "주차", "size": "8평", "x": 183, "y": 36, "w": 84, "h": 82},
            {"label": "계단", "size": "4평", "x": 304, "y": 36, "w": 76, "h": 82},
            {"label": "거실", "size": "10평", "x": 34, "y": 154, "w": 112, "h": 164},
            {"label": "주방", "size": "5평", "x": 183, "y": 154, "w": 82, "h": 62},
            {"label": "침실", "size": "7평", "x": 304, "y": 256, "w": 76, "h": 62},
        ],
    }
    return {
        "name": _SITE_REPORT_USE_LABELS.get(use, _SITE_REPORT_USE_LABELS["mixed"]),
        "rooms": rooms_by_use.get(use, rooms_by_use["mixed"]),
    }


def _site_report_data_count(value: Any, key: str = "items") -> int:
    if isinstance(value, dict):
        data = value.get("data")
        if isinstance(data, dict):
            count = data.get("count")
            if isinstance(count, int):
                return count
            items = data.get(key)
            if isinstance(items, list):
                return len(items)
        items = value.get(key)
        if isinstance(items, list):
            return len(items)
    if isinstance(value, list):
        return len(value)
    return 0


def _site_report_engine_cases_path() -> Path:
    return Path(
        os.getenv(
            "SITE_PLAN_ENGINE_CASES_PATH",
            "/Users/jun/site_plan/data/full_drawing_audit/engine_seed_cases_a_only.json",
        )
    )


def _load_site_report_engine_cases() -> dict[str, Any]:
    path = _site_report_engine_cases_path()
    if not path.exists():
        return {"summary": {}, "records": []}

    try:
        stat = path.stat()
    except OSError:
        return {"summary": {}, "records": []}

    with _SITE_REPORT_ENGINE_CASE_CACHE_LOCK:
        if (
            _SITE_REPORT_ENGINE_CASE_CACHE.get("path") == str(path)
            and _SITE_REPORT_ENGINE_CASE_CACHE.get("mtime") == stat.st_mtime
            and isinstance(_SITE_REPORT_ENGINE_CASE_CACHE.get("payload"), dict)
        ):
            return _SITE_REPORT_ENGINE_CASE_CACHE["payload"]

        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as exc:
            logger.warning("failed to load site report engine cases: %s", exc)
            payload = {"summary": {}, "records": []}

        _SITE_REPORT_ENGINE_CASE_CACHE.update(
            {
                "path": str(path),
                "mtime": stat.st_mtime,
                "payload": payload,
            }
        )
        return payload


def _site_report_case_keywords(use: str) -> list[str]:
    keywords = {
        "house": ["단독", "주택", "거실", "주방", "침실", "안방"],
        "multi": ["다가구", "다세대", "세대", "원룸", "공용", "계단"],
        "retail": ["근린", "상가", "매장", "판매", "전시", "사무", "창고"],
        "mixed": ["상가", "근린", "주거", "거실", "침실", "계단", "주차"],
    }
    return keywords.get(use, keywords["mixed"])


def _site_report_case_program_text(case: dict[str, Any]) -> str:
    program = case.get("program_summary") if isinstance(case, dict) else {}
    if not isinstance(program, dict):
        return ""
    parts: list[str] = []
    for item in program.get("top_room_labels") or []:
        if isinstance(item, dict):
            parts.append(str(item.get("label") or ""))
    for item in program.get("top_categories") or []:
        if isinstance(item, dict):
            parts.append(str(item.get("category") or ""))
    return " ".join(part for part in parts if part)


def _site_report_case_values(case: dict[str, Any]) -> dict[str, dict[str, Any]]:
    values = case.get("extracted_values")
    return values if isinstance(values, dict) else {}


def _site_report_case_area_similarity(body: dict[str, Any], case: dict[str, Any]) -> float:
    target_pyeong = _site_report_number(body.get("area_pyeong") or body.get("area"), 0)
    if target_pyeong <= 0:
        return 0.0

    values = _site_report_case_values(case)
    metric = values.get("gross_floor_area") or values.get("site_area") or values.get("building_area")
    if not isinstance(metric, dict):
        return 0.0

    case_value = _site_report_number(metric.get("value"), 0)
    if case_value <= 0:
        return 0.0

    target_m2 = target_pyeong * 3.305785
    ratio = min(target_m2, case_value) / max(target_m2, case_value)
    return max(0.0, min(15.0, ratio * 15.0))


def _site_report_score_engine_case(body: dict[str, Any], case: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    use = str(body.get("use") or "mixed").strip()
    program_text = _site_report_case_program_text(case)
    keyword_hits = [word for word in _site_report_case_keywords(use) if word and word in program_text]
    program = case.get("program_summary") if isinstance(case.get("program_summary"), dict) else {}
    values = _site_report_case_values(case)
    usage = str(case.get("engine_usage") or "")
    bundle_type = str(case.get("bundle_type") or "")

    base_score = _site_report_number(case.get("engine_score"), 0) * 0.42
    usage_bonus = {
        "primary_engine_case": 24,
        "secondary_engine_case": 16,
        "floor_pattern_source": 10,
        "site_value_reference": 8,
        "overview_value_reference": 6,
    }.get(usage, 3)
    keyword_bonus = min(len(keyword_hits), 4) * 5
    area_bonus = _site_report_case_area_similarity(body, case)
    signal_bonus = 0.0
    if _site_report_number(program.get("core_candidate_count"), 0) > 0:
        signal_bonus += 4
    if _site_report_number(program.get("parking_candidate_count"), 0) > 0:
        signal_bonus += 4
    if _site_report_number(program.get("road_candidate_count"), 0) > 0:
        signal_bonus += 4
    if values:
        signal_bonus += min(len(values), 5)

    score = round(min(100.0, base_score + usage_bonus + keyword_bonus + area_bonus + signal_bonus), 1)
    reasons = []
    if bundle_type == "engine_complete_a":
        reasons.append("A등급 개요/면적표·배치·평면이 함께 있는 사례")
    elif usage == "secondary_engine_case":
        reasons.append("A등급 개요/면적표와 평면을 함께 가진 보조 사례")
    elif usage == "floor_pattern_source":
        reasons.append("A등급 평면 패턴 참고 사례")
    if keyword_hits:
        reasons.append(f"요청 용도와 겹치는 공간 키워드: {', '.join(keyword_hits[:4])}")
    if area_bonus > 0:
        reasons.append("입력 목표 면적과 사례 추출 면적이 비교 가능")
    if program.get("core_candidate_count"):
        reasons.append("코어 후보가 추출됨")
    if program.get("parking_candidate_count"):
        reasons.append("주차 후보가 추출됨")

    differences = []
    if usage != "primary_engine_case":
        differences.append("개요·배치·평면 중 일부 묶음이 부족해 보조 근거로 사용")
    if not values:
        differences.append("면적값 추출 근거가 약해 공사비·규모 판단에는 제한")
    return score, reasons[:5], differences[:3]


def _site_report_compact_engine_case(case: dict[str, Any], score: float, reasons: list[str], differences: list[str]) -> dict[str, Any]:
    selected = case.get("selected_files") if isinstance(case.get("selected_files"), dict) else {}

    def ids(name: str) -> list[str]:
        items = selected.get(name)
        if not isinstance(items, list):
            return []
        return [str(item.get("file_id")) for item in items if isinstance(item, dict) and item.get("file_id")]

    program = case.get("program_summary") if isinstance(case.get("program_summary"), dict) else {}
    labels = []
    for item in program.get("top_room_labels") or []:
        if isinstance(item, dict) and item.get("label"):
            labels.append(str(item["label"]))

    return {
        "case_id": str(case.get("case_id") or ""),
        "project_hash": str(case.get("project_hash") or ""),
        "match_score": score,
        "bundle_type": str(case.get("bundle_type") or ""),
        "engine_usage": str(case.get("engine_usage") or ""),
        "room_labels": labels[:8],
        "values": _site_report_case_values(case),
        "selected_file_ids": {
            "overview": ids("overview"),
            "site": ids("site"),
            "floor": ids("floor"),
        },
        "reasons": reasons,
        "differences": differences,
    }


def _match_site_report_engine_cases(body: dict[str, Any], limit: int = 3) -> dict[str, Any]:
    payload = _load_site_report_engine_cases()
    records = payload.get("records")
    if not isinstance(records, list) or not records:
        return {
            "source": "a_grade_case_engine",
            "available": False,
            "summary": payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
            "matched_cases": [],
        }

    scored: list[tuple[float, dict[str, Any], list[str], list[str]]] = []
    for case in records:
        if not isinstance(case, dict):
            continue
        usage = str(case.get("engine_usage") or "")
        if usage not in {
            "primary_engine_case",
            "secondary_engine_case",
            "floor_pattern_source",
            "site_value_reference",
            "overview_value_reference",
        }:
            continue
        score, reasons, differences = _site_report_score_engine_case(body, case)
        if score <= 0:
            continue
        scored.append((score, case, reasons, differences))

    scored.sort(key=lambda item: item[0], reverse=True)
    matched = [_site_report_compact_engine_case(case, score, reasons, differences) for score, case, reasons, differences in scored[:limit]]
    return {
        "source": "a_grade_case_engine",
        "available": bool(matched),
        "summary": payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
        "matched_cases": matched,
    }


def _site_report_clip(value: Any, limit: int) -> Any:
    if limit <= 0:
        return ""
    if isinstance(value, str):
        return value if len(value) <= limit else value[:limit] + "...(truncated)"
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        out: list[Any] = []
        budget = limit
        for item in value[:20]:
            clipped = _site_report_clip(item, max(200, budget // 2))
            out.append(clipped)
            budget -= len(json.dumps(clipped, ensure_ascii=False, default=str))
            if budget <= 0:
                break
        return out
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        budget = limit
        for key, item in value.items():
            clipped = _site_report_clip(item, max(200, budget // 2))
            out[str(key)] = clipped
            budget -= len(json.dumps({str(key): clipped}, ensure_ascii=False, default=str))
            if budget <= 0:
                break
        return out
    return str(value)[:limit]


def _collect_site_plan_report_data(pnu: str, supplied_data: Any = None) -> dict[str, Any]:
    supplied = supplied_data if isinstance(supplied_data, dict) else {}
    data: dict[str, Any] = {
        "pnu": pnu,
        "land_info": supplied.get("landInfo"),
        "building_info": supplied.get("buildingInfo"),
        "land_geo": supplied.get("landGeo"),
        "building_violations": supplied.get("buildingViolations"),
        "source": "supplied",
    }

    if not pnu:
        return data

    data["source"] = "server"
    try:
        data["land_info"] = {"records": _fetch_dataset_records("land_info", pnu, limit=40)}
    except Exception as exc:
        data["land_info_error"] = str(exc)

    try:
        building_line = _fetch_building_info_line(pnu)
        data["building_info"] = {"lines": [building_line] if building_line else []}
    except Exception as exc:
        data["building_info_error"] = str(exc)

    try:
        land_items = _fetch_cadastral_geo_items(
            pnu,
            limit=120,
            include_surroundings=True,
            surroundings_padding_ratio=0.35,
        )
        data["land_geo"] = {
            "count": len(land_items),
            "items": [
                {
                    "pnu": item.get("pnu"),
                    "label": item.get("label"),
                    "bbox": _geometry_bbox(item.get("geometry")) if isinstance(item.get("geometry"), dict) else None,
                }
                for item in land_items[:20]
            ],
        }
    except Exception as exc:
        data["land_geo_error"] = str(exc)

    try:
        building_items = _fetch_building_geo_with_violation(pnu, limit=300)
        violation_items = [
            item
            for item in building_items
            if isinstance(item.get("violation"), dict) and item["violation"].get("is_violation") is True
        ]
        data["building_violations"] = {
            "count": len(building_items),
            "has_violation": len(violation_items) > 0,
            "items": [
                {
                    "building_name": item.get("building_name"),
                    "building_id": item.get("building_legacy_id") or item.get("building_id"),
                    "violation": item.get("violation"),
                }
                for item in building_items[:30]
            ],
        }
    except Exception as exc:
        data["building_violations_error"] = str(exc)

    return data


def _site_report_basis(site_data: dict[str, Any]) -> dict[str, str]:
    land_count = _site_report_data_count(site_data.get("land_geo"))
    land_record_count = _site_report_data_count(site_data.get("land_info"), key="records")
    building_lines = _site_report_data_count(site_data.get("building_info"), key="lines")
    violations = site_data.get("building_violations")
    has_violation = None
    if isinstance(violations, dict):
        payload = violations.get("data") if isinstance(violations.get("data"), dict) else violations
        has_violation = payload.get("has_violation") if isinstance(payload, dict) else None

    return {
        "land": f"{land_count or land_record_count}개 데이터 조회" if (land_count or land_record_count) else "조회 대기",
        "building": f"{building_lines}개 대장 라인" if building_lines else "조회 대기",
        "violation": "위반 이력 있음" if has_violation is True else "위반 이력 없음" if has_violation is False else "검토 필요",
    }


def _build_site_plan_report_draft(body: dict[str, Any], site_data: dict[str, Any]) -> dict[str, Any]:
    address = str(body.get("address") or "입력 대지").strip() or "입력 대지"
    pnu = str(body.get("pnu") or "").strip()
    use = str(body.get("use") or "mixed").strip()
    priority = str(body.get("priority") or "balance").strip()
    area = max(10.0, _site_report_number(body.get("area_pyeong") or body.get("area") or 45, 45.0))
    budget = max(0.0, _site_report_number(body.get("budget_manwon") or body.get("budget") or 0, 0.0))
    use_label = _SITE_REPORT_USE_LABELS.get(use, _SITE_REPORT_USE_LABELS["mixed"])
    priority_label = _SITE_REPORT_PRIORITY_LABELS.get(priority, _SITE_REPORT_PRIORITY_LABELS["balance"])
    rates = _site_report_cost_rate(use, priority)
    low = round(area * rates["low"])
    high = round(area * rates["high"])
    basis = _site_report_basis(site_data)
    design_engine = site_data.get("design_engine") if isinstance(site_data.get("design_engine"), dict) else {}
    matched_cases = design_engine.get("matched_cases") if isinstance(design_engine.get("matched_cases"), list) else []
    primary_matches = [case for case in matched_cases if isinstance(case, dict) and case.get("engine_usage") == "primary_engine_case"]
    case_basis_label = f"A등급 사례 {len(matched_cases)}건" if matched_cases else ("PNU 연결" if pnu else "주소 기준")
    case_basis_caption = (
        f"1차 후보 {len(primary_matches)}건 포함"
        if primary_matches
        else (pnu or "후보 선택 대기")
    )
    budget_fit = budget >= low if budget else False
    today = time.strftime("%Y. %-m. %-d.") if os.name != "nt" else time.strftime("%Y. %#m. %#d.")

    design_by_use = {
        "house": "거실과 주방을 채광이 좋은 면에 두고, 침실은 소음이 적은 후면으로 분리합니다.",
        "multi": "반복 가능한 세대 모듈과 단순한 코어를 우선해 공사비와 인허가 변수를 줄입니다.",
        "retail": "도로 접근부의 가시성과 진입 동선을 우선하고 후면에 창고·관리 영역을 둡니다.",
        "mixed": "1층 근린생활시설과 상부 주거의 출입 동선을 명확히 분리합니다.",
    }
    design_by_priority = {
        "budget": "골조와 외피를 단순화하고 습식 공간을 모아 공사비 변동 폭을 낮춥니다.",
        "speed": "법규 해석이 단순한 매스와 층별 프로그램으로 인허가 검토 시간을 줄입니다.",
        "yield": "임대 가능한 면을 도로 접근부에 배치하고 공용부 면적을 압축합니다.",
        "balance": "채광, 임대성, 공사비를 균형 있게 맞춘 중간 밀도 안을 우선합니다.",
    }

    return {
        "title": f"{_site_report_short_address(address)} 대지 리포트",
        "type": "서버 초안",
        "date": today,
        "recommendation": f"{use_label} {priority_label} 설계안",
        "summary": (
            f"{address} 기준으로 {area:g}평 규모의 배치와 공사비 범위를 산정했습니다. "
            "실제 인허가와 견적은 현장 조사, 지자체 조례, 구조·설비 검토 후 확정됩니다."
        ),
        "metrics": [
            {"label": "권장 규모", "value": f"{area:g}평", "caption": "사용자 입력 기준"},
            {"label": "공사비 범위", "value": f"{_site_report_money(low)}~{_site_report_money(high)}", "caption": "개략 추정"},
            {"label": "예산 적합도", "value": "양호" if budget_fit else "조정 필요", "caption": f"{_site_report_money(budget)} 입력" if budget else "예산 미입력"},
            {"label": "데이터 근거", "value": case_basis_label, "caption": case_basis_caption},
        ],
        "design": [
            design_by_use.get(use, design_by_use["mixed"]),
            design_by_priority.get(priority, design_by_priority["balance"]),
            (
                f"A등급 유사 사례 {len(matched_cases)}건의 공간명·코어·주차 추출값을 보조 근거로 사용합니다."
                if matched_cases
                else "조회 데이터와 현장 조건을 대조해 접도, 채광, 주차, 피난 동선을 우선 검토합니다."
            ),
        ],
        "costNote": (
            f"{use_label}의 목표 연면적 {area:g}평을 기준으로 {_site_report_money(low)}에서 "
            f"{_site_report_money(high)} 사이가 1차 범위입니다. 지하층, 철거, 특수 구조, "
            "외장재 사양, 민원 대응 비용은 별도 검토 항목입니다."
        ),
        "risks": [
            "지자체 조례와 도로 접도 조건을 추가 확인해야 합니다." if pnu else "PNU가 확정되면 지적 형상과 토지 정보를 정밀 조회합니다.",
            "기존 건축물·위반 여부는 최신 공부와 현장 확인이 필요합니다.",
            "실제 견적은 실시설계 도면과 구조·기계·전기 사양 확정 후 보정합니다.",
        ],
        "basis": basis,
        "floorPlan": _site_report_floor_plan(use),
        "caseMatches": matched_cases,
    }


def _site_report_extract_json(text: str) -> dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            try:
                parsed = json.loads(raw[start : end + 1])
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
    return None


def _normalize_site_report(ai_report: Any, fallback: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(ai_report, dict):
        return fallback

    result = dict(fallback)
    for key in ("title", "type", "date", "recommendation", "summary", "costNote"):
        value = ai_report.get(key)
        if isinstance(value, str) and value.strip():
            result[key] = value.strip()

    for key in ("design", "risks"):
        value = ai_report.get(key)
        if isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
            if items:
                result[key] = items[:5]

    metrics = ai_report.get("metrics")
    if isinstance(metrics, list):
        normalized_metrics = []
        for item in metrics:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            value = str(item.get("value") or "").strip()
            caption = str(item.get("caption") or "").strip()
            if label and value:
                normalized_metrics.append({"label": label, "value": value, "caption": caption})
        if len(normalized_metrics) >= 4:
            result["metrics"] = normalized_metrics[:4]

    basis = ai_report.get("basis")
    if isinstance(basis, dict):
        result["basis"] = {
            "land": str(basis.get("land") or fallback["basis"]["land"]),
            "building": str(basis.get("building") or fallback["basis"]["building"]),
            "violation": str(basis.get("violation") or fallback["basis"]["violation"]),
        }

    case_matches = ai_report.get("caseMatches")
    fallback_matches = fallback.get("caseMatches") if isinstance(fallback.get("caseMatches"), list) else []
    if isinstance(case_matches, list):
        normalized_matches = []
        for item in case_matches[:3]:
            if not isinstance(item, dict):
                continue
            case_id = str(item.get("case_id") or "").strip()
            if not case_id:
                continue
            reasons = item.get("reasons")
            differences = item.get("differences")
            normalized_matches.append(
                {
                    "case_id": case_id,
                    "project_hash": str(item.get("project_hash") or ""),
                    "match_score": _site_report_number(item.get("match_score"), 0),
                    "bundle_type": str(item.get("bundle_type") or ""),
                    "engine_usage": str(item.get("engine_usage") or ""),
                    "room_labels": [str(label) for label in item.get("room_labels", [])[:8]] if isinstance(item.get("room_labels"), list) else [],
                    "selected_file_ids": item.get("selected_file_ids") if isinstance(item.get("selected_file_ids"), dict) else {},
                    "reasons": [str(reason) for reason in reasons[:5]] if isinstance(reasons, list) else [],
                    "differences": [str(diff) for diff in differences[:3]] if isinstance(differences, list) else [],
                }
            )
        result["caseMatches"] = normalized_matches or fallback_matches
    elif fallback_matches:
        result["caseMatches"] = fallback_matches

    floor_plan = ai_report.get("floorPlan")
    if isinstance(floor_plan, dict):
        fallback_plan = fallback.get("floorPlan", {})
        rooms = floor_plan.get("rooms")
        normalized_rooms = []
        if isinstance(rooms, list):
            for room in rooms:
                if not isinstance(room, dict):
                    continue
                label = str(room.get("label") or "").strip()
                size = str(room.get("size") or "").strip()
                if not label or not size:
                    continue
                normalized_rooms.append(
                    {
                        "label": label,
                        "size": size,
                        "x": int(_site_report_number(room.get("x"), 34)),
                        "y": int(_site_report_number(room.get("y"), 36)),
                        "w": int(_site_report_number(room.get("w"), 90)),
                        "h": int(_site_report_number(room.get("h"), 70)),
                        "accent": bool(room.get("accent", False)),
                    }
                )
        result["floorPlan"] = {
            "name": str(floor_plan.get("name") or fallback_plan.get("name") or "예상 평면"),
            "rooms": normalized_rooms[:8] if normalized_rooms else fallback_plan.get("rooms", []),
        }

    return result


def _build_site_report_prompt(body: dict[str, Any], site_data: dict[str, Any]) -> tuple[str, str]:
    max_chars = int(_site_report_number(os.getenv("OPENAI_REPORT_DATA_LIMIT_CHARS"), 28000))
    compact_data = _site_report_clip(site_data, max(4000, max_chars))
    request_context = {
        "address": body.get("address"),
        "pnu": body.get("pnu"),
        "use": body.get("use"),
        "area_pyeong": body.get("area_pyeong") or body.get("area"),
        "budget_manwon": body.get("budget_manwon") or body.get("budget"),
        "priority": body.get("priority"),
    }
    system_prompt = (
        "당신은 한국 건축 기획 보고서를 작성하는 시니어 건축 컨설턴트입니다. "
        "제공된 토지·건축물 데이터를 근거로 하되, 인허가와 견적은 현장 조사와 전문가 검토 후 확정된다는 점을 명확히 유지하세요. "
        "design_engine.matched_cases가 있으면 유사 사례 기반 근거로만 사용하고 원본 도면을 복제하거나 특정 프로젝트를 식별하지 마세요. "
        "응답은 반드시 한국어 JSON 객체만 반환하세요."
    )
    user_prompt = (
        "대지 리포트를 작성하세요.\n\n"
        f"요청 정보:\n{json.dumps(request_context, ensure_ascii=False)}\n\n"
        f"서버 조회 데이터:\n{json.dumps(compact_data, ensure_ascii=False, default=str)}\n\n"
        "반환 JSON 스키마:\n"
        "{"
        "\"title\": string,"
        "\"type\": \"GPT 분석 리포트\","
        "\"date\": string,"
        "\"recommendation\": string,"
        "\"summary\": string,"
        "\"metrics\": [{\"label\": string, \"value\": string, \"caption\": string}],"
        "\"design\": [string],"
        "\"costNote\": string,"
        "\"risks\": [string],"
        "\"caseMatches\": [{\"case_id\": string, \"project_hash\": string, \"match_score\": number, \"bundle_type\": string, \"engine_usage\": string, \"room_labels\": [string], \"reasons\": [string], \"differences\": [string]}],"
        "\"basis\": {\"land\": string, \"building\": string, \"violation\": string},"
        "\"floorPlan\": {\"name\": string, \"rooms\": [{\"label\": string, \"size\": string, \"x\": number, \"y\": number, \"w\": number, \"h\": number, \"accent\": boolean}]}"
        "}\n\n"
        "규칙: metrics는 정확히 4개, design은 3~5개, risks는 3~5개로 작성하세요. "
        "caseMatches는 서버 조회 데이터의 design_engine.matched_cases 중 최대 3개만 요약하세요. "
        "floorPlan.rooms 좌표는 x 18~330, y 18~285, w 50~210, h 45~170 범위 안에서 SVG 평면도에 들어가게 작성하세요. "
        "공사비는 만원 또는 억원 단위로 표시하고 확정 견적처럼 단정하지 마세요."
    )
    return system_prompt, user_prompt


def _request_openai_site_report(body: dict[str, Any], site_data: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    model = os.getenv("OPENAI_MODEL", "gpt-5.5").strip() or "gpt-5.5"
    max_tokens = int(_site_report_number(os.getenv("OPENAI_REPORT_MAX_COMPLETION_TOKENS"), 3500))
    timeout = float(_site_report_number(os.getenv("OPENAI_REPORT_TIMEOUT_SEC"), 60))
    system_prompt, user_prompt = _build_site_report_prompt(body, site_data)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
        "max_completion_tokens": max(1200, max_tokens),
    }
    request = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8", "replace")
    parsed = json.loads(raw)
    content = parsed["choices"][0]["message"]["content"]
    ai_report = _site_report_extract_json(content)
    if not ai_report:
        raise RuntimeError("OpenAI response did not contain a JSON report")
    return {"model": model, "report": ai_report}


@app.post("/v1/reports/site-plan")
def create_site_plan_report(body: Dict[str, Any]) -> Dict[str, Any]:
    address = str(body.get("address") or "").strip()
    if not address:
        raise HTTPException(status_code=400, detail="address is required")

    pnu = str(body.get("pnu") or "").strip()
    site_data = _collect_site_plan_report_data(pnu, body.get("data"))
    site_data["design_engine"] = _match_site_report_engine_cases(body)
    fallback = _build_site_plan_report_draft(body, site_data)
    model = os.getenv("OPENAI_MODEL", "gpt-5.5").strip() or "gpt-5.5"

    try:
        ai_result = _request_openai_site_report(body, site_data)
        report = _normalize_site_report(ai_result.get("report"), fallback)
        report["type"] = report.get("type") or "GPT 분석 리포트"
        return ok(
            {
                "report": report,
                "generated_by": "openai",
                "model": ai_result.get("model") or model,
                "basis": _site_report_basis(site_data),
            }
        )
    except Exception as exc:
        logger.warning("site plan report fell back to server draft: %s", exc)
        return ok(
            {
                "report": fallback,
                "generated_by": "server_draft",
                "model": model,
                "warning": str(exc)[:240],
                "basis": _site_report_basis(site_data),
            }
        )


def _fetch_building_info_active_release_summary() -> dict[str, Any] | None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, version, source_name, status, is_active, records_count, activated_at, updated_at
                    FROM cadastral_release
                    WHERE data_type = 'building_info'
                      AND is_active = TRUE
                    ORDER BY activated_at DESC NULLS LAST, id DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to load active building_info release: {exc}")

    if not row:
        return None

    return {
        "id": row[0],
        "version": row[1],
        "source_name": row[2],
        "status": row[3],
        "is_active": row[4],
        "records_count": row[5],
        "activated_at": row[6],
        "updated_at": row[7],
    }


def _select_building_info_verify_pnu(release_id: int) -> str | None:
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pnu
                    FROM building_info_lookup
                    WHERE release_id = %s
                      AND COALESCE(pnu, '') <> ''
                    ORDER BY pnu
                    LIMIT 1
                    """,
                    (release_id,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])

                cur.execute(
                    """
                    SELECT pnu
                    FROM building_info_line
                    WHERE release_id = %s
                      AND COALESCE(pnu, '') <> ''
                    ORDER BY pnu
                    LIMIT 1
                    """,
                    (release_id,),
                )
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to select building_info verification pnu: {exc}")

    if not row or not row[0]:
        return None
    return str(row[0])


def _decode_building_info_client_payload(line: str, pnu: str) -> dict[str, Any]:
    raw = str(line or "")
    payload_text = ""
    for candidate in _pnu_query_candidates(pnu):
        if candidate and raw.startswith(candidate):
            payload_text = raw[len(candidate) :]
            break
    if not payload_text:
        json_start = raw.find("{")
        if json_start >= 0:
            payload_text = raw[json_start:]
    if not payload_text:
        return {}

    try:
        parsed = json.loads(payload_text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _building_info_bucket_counts(payload: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for bucket in ("total", "single", "floor", "room"):
        value = payload.get(bucket)
        counts[bucket] = len(value) if isinstance(value, list) else 0
    return counts


@app.get("/v1/data/building_info/verify")
def verify_building_info_client_fetch(
    pnu: str | None = Query(None),
    include_sample: bool = Query(False),
) -> Dict[str, Any]:
    release = _fetch_building_info_active_release_summary()
    if not release:
        raise HTTPException(status_code=503, detail="active building_info release is not available")

    requested_pnu = str(pnu or "").strip()
    selected_pnu = requested_pnu or _select_building_info_verify_pnu(int(release["id"]))
    if not selected_pnu:
        raise HTTPException(status_code=503, detail="no building_info sample pnu is available")

    started_at = time.perf_counter()
    line = _fetch_building_info_line(selected_pnu)
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    if not line:
        status_code = 404 if requested_pnu else 503
        raise HTTPException(status_code=status_code, detail=f"building_info payload not found for pnu={selected_pnu}")

    payload = _decode_building_info_client_payload(line, selected_pnu)
    bucket_counts = _building_info_bucket_counts(payload)
    has_payload = any(bucket_counts.values())
    if not has_payload:
        raise HTTPException(status_code=503, detail=f"building_info payload could not be decoded for pnu={selected_pnu}")

    client_endpoint = f"/v1/data/building_info/{urllib.parse.quote(selected_pnu)}?format=compressed"
    result: dict[str, Any] = {
        "status": "ok",
        "collection": "building_info",
        "pnu": selected_pnu,
        "sample_source": "request" if requested_pnu else "active_release",
        "release": release,
        "client_endpoint": client_endpoint,
        "checks": {
            "active_release": True,
            "client_fetch": True,
            "payload_decoded": True,
            "has_payload": has_payload,
        },
        "response": {
            "format": "compressed",
            "part_count": 1,
            "first_part_bytes": len(line.encode("utf-8")),
            "elapsed_ms": elapsed_ms,
        },
        "bucket_counts": bucket_counts,
        "has_meta": isinstance(payload.get("meta"), dict),
    }
    if include_sample:
        result["sample"] = {"format": "compressed", "parts": [line]}
    return ok(result)


@app.get("/v1/data/{collection}/{pnu}")
def get_data(
    collection: str,
    pnu: str,
    format: str = Query("compressed", pattern="^(compressed|lines)$"),
) -> Dict[str, Any]:
    if collection == "cadastral":
        raise HTTPException(
            status_code=410,
            detail="legacy /v1/data/cadastral is disabled; use /v1/geo/land or /v1/tiles/cadastral",
        )

    if collection == "building_info":
        line = _fetch_building_info_line(pnu)
        if not line:
            if format == "lines":
                return ok({"format": "lines", "lines": []})
            return ok({"format": "compressed", "parts": []})
        if format == "lines":
            return ok({"format": "lines", "lines": [line]})
        return ok({"format": "compressed", "parts": [line]})

    if collection == "land_info":
        records = _fetch_dataset_records(collection, pnu)
        if not records:
            if format == "lines":
                return ok({"format": "lines", "lines": []})
            return ok({"format": "compressed", "parts": []})
        legacy_line = pnu + json.dumps(_land_info_records_to_legacy_payload(records), ensure_ascii=False)
        if format == "lines":
            return ok({"format": "lines", "lines": [legacy_line]})
        return ok({"format": "compressed", "parts": [legacy_line]})

    if collection == "building_integrated_info":
        records = _fetch_dataset_records(collection, pnu)
        if format == "lines":
            lines = [json.dumps(item["payload"], ensure_ascii=False) for item in records]
            return ok({"format": "lines", "lines": lines})
        return ok({"format": "compressed", "parts": records})

    if format == "lines":
        return ok({"format": "lines", "lines": []})
    return ok({"format": "compressed", "parts": []})


@app.post("/v1/data/batch")
def get_data_batch(body: Dict[str, Any]) -> Dict[str, Any]:
    items = body.get("items", [])
    format = body.get("format", "compressed")
    result = []
    for item in items:
        entry = {
            "collection": item.get("collection"),
            "pnu": item.get("pnu"),
        }
        if format == "lines":
            entry["lines"] = []
        else:
            entry["parts"] = []
        result.append(entry)
    return ok(result)


@app.get("/v1/tile/{root}/{parent}/{id}")
def get_tile(root: str, parent: str, id: str) -> Dict[str, Any]:
    return ok({"format": "compressed", "parts": []})


@app.post("/v1/tile/batch")
def get_tile_batch(body: Dict[str, Any]) -> Dict[str, Any]:
    tiles = body.get("tiles", [])
    result = []
    for tile in tiles:
        result.append(
            {
                "root": tile.get("root"),
                "parent": tile.get("parent"),
                "id": tile.get("id"),
                "parts": [],
            }
        )
    return ok(result)


@app.get("/v1/pnu/{pnu}/polygon")
def get_polygon(
    pnu: str,
    format: str = Query("raw", pattern="^(raw|points)$"),
) -> Dict[str, Any]:
    if format == "points":
        return ok({"format": "points", "landPolygon": [], "buildingPolygon": []})
    return ok({"format": "raw", "payload": ""})


@app.get("/v1/geo/building")
def get_building_geo(
    pnu: str = Query(...),
    limit: int = Query(300, ge=1, le=2000),
) -> Dict[str, Any]:
    items = _fetch_building_geo_with_violation(pnu, limit=limit)
    return ok(
        {
            "pnu": pnu,
            "count": len(items),
            "items": items,
        }
    )


@app.get("/v1/geo/building/violations")
def get_building_violations(
    pnu: str = Query(...),
    limit: int = Query(300, ge=1, le=2000),
) -> Dict[str, Any]:
    items = _fetch_building_geo_with_violation(pnu, limit=limit)
    violation_items = [
        {
            "dataset_code": item["dataset_code"],
            "source_file": item["source_file"],
            "row_no": item["row_no"],
            "building_id": item["building_legacy_id"] or item["building_id"],
            "building_source_id": item["building_id"],
            "building_legacy_id": item["building_legacy_id"],
            "building_name": item["building_name"],
            "violation": item["violation"],
        }
        for item in items
    ]
    has_violation = any(i["violation"]["is_violation"] is True for i in violation_items)
    return ok(
        {
            "pnu": pnu,
            "count": len(violation_items),
            "has_violation": has_violation,
            "items": violation_items,
        }
    )


@app.get("/v1/geo/land/bounds")
def get_land_geo_by_bounds(
    min_lon: float = Query(...),
    min_lat: float = Query(...),
    max_lon: float = Query(...),
    max_lat: float = Query(...),
    zoom: int = Query(18, ge=0, le=22),
    limit: int = Query(3000, ge=1, le=6000),
) -> Dict[str, Any]:
    items = _fetch_cadastral_geo_items_by_bounds(
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        limit=limit,
        zoom=zoom,
    )
    return ok(
        {
            "bounds": {
                "min_lon": min_lon,
                "min_lat": min_lat,
                "max_lon": max_lon,
                "max_lat": max_lat,
            },
            "zoom": zoom,
            "count": len(items),
            "items": items,
        }
    )


@app.get("/v1/geo/land/{pnu}")
def get_land_geo(
    pnu: str,
    limit: int = Query(200, ge=1, le=6000),
    include_surroundings: bool = Query(False),
    surroundings_padding_ratio: float = Query(0.35, ge=0.05, le=20.0),
) -> Dict[str, Any]:
    items = _fetch_cadastral_geo_items(
        pnu,
        limit=limit,
        include_surroundings=include_surroundings,
        surroundings_padding_ratio=surroundings_padding_ratio,
    )
    return ok(
        {
            "pnu": pnu,
            "count": len(items),
            "items": items,
        }
    )


@app.post("/v1/geo/land/polygons")
def get_land_polygons(body: Dict[str, Any]) -> Dict[str, Any]:
    _ = body.get("prefixes", [])
    return ok([])


@app.post("/v1/geo/land/features")
def get_land_features(body: Dict[str, Any]) -> Dict[str, Any]:
    _ = body.get("prefixes", [])
    return ok([])


def _kakao_rest_api_key() -> str:
    for key in ("KAKAO_REST_API_KEY", "KAKAO_API_KEY", "KAKAO_LOCAL_REST_API_KEY"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return ""


def _kakao_get_json(path: str, params: dict[str, Any]) -> dict[str, Any]:
    api_key = _kakao_rest_api_key()
    if not api_key:
        raise RuntimeError("KAKAO_REST_API_KEY is not set")

    query = urllib.parse.urlencode(
        {key: value for key, value in params.items() if value is not None},
        doseq=True,
    )
    url = f"https://dapi.kakao.com{path}?{query}"
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"KakaoAK {api_key}"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.loads(response.read().decode("utf-8", "replace"))


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _pnu_from_kakao_address(address: Any) -> str:
    if not isinstance(address, dict):
        return ""
    b_code = _digits_only(address.get("b_code"))
    main_no = _digits_only(address.get("main_address_no"))
    sub_no = _digits_only(address.get("sub_address_no"))
    if len(b_code) != 10 or not main_no:
        return ""
    land_type = "2" if str(address.get("mountain_yn") or "").upper() == "Y" else "1"
    return f"{b_code}{land_type}{main_no.zfill(4)}{(sub_no or '0').zfill(4)}"


def _normalize_kakao_address_doc(doc: Any) -> dict[str, Any] | None:
    if not isinstance(doc, dict):
        return None
    item = dict(doc)
    address = item.get("address")
    road_address = item.get("road_address")
    if not isinstance(address, dict):
        address = {}
    if not isinstance(road_address, dict):
        road_address = {}

    pnu = _pnu_from_kakao_address(address)
    address_name = str(
        item.get("address_name")
        or address.get("address_name")
        or item.get("road_address_name")
        or road_address.get("address_name")
        or item.get("place_name")
        or ""
    )
    road_address_name = str(
        item.get("road_address_name")
        or road_address.get("address_name")
        or ""
    )

    item["pnu"] = pnu
    item["address_name"] = address_name
    item["road_address_name"] = road_address_name
    item["building_name"] = str(road_address.get("building_name") or item.get("place_name") or "")
    item["source"] = item.get("source") or "kakao"
    return item


def _kakao_coord2address_document(x: Any, y: Any) -> dict[str, Any] | None:
    if x in (None, "") or y in (None, ""):
        return None
    payload = _kakao_get_json(
        "/v2/local/geo/coord2address.json",
        {"x": x, "y": y},
    )
    documents = payload.get("documents")
    if isinstance(documents, list) and documents:
        first = documents[0]
        return first if isinstance(first, dict) else None
    return None


def _kakao_b_code_for_coord(x: Any, y: Any) -> str:
    if x in (None, "") or y in (None, ""):
        return ""
    payload = _kakao_get_json(
        "/v2/local/geo/coord2regioncode.json",
        {"x": x, "y": y},
    )
    documents = payload.get("documents")
    if not isinstance(documents, list):
        return ""
    for item in documents:
        if not isinstance(item, dict):
            continue
        if item.get("region_type") == "B":
            return _digits_only(item.get("code"))
    return ""


def _attach_kakao_b_code(doc: dict[str, Any], x: Any, y: Any) -> dict[str, Any]:
    address = doc.get("address")
    if not isinstance(address, dict):
        return doc
    if _digits_only(address.get("b_code")):
        return doc
    b_code = _kakao_b_code_for_coord(x, y)
    if b_code:
        address = dict(address)
        address["b_code"] = b_code
        doc["address"] = address
    return doc


def _normalize_kakao_keyword_doc(doc: Any) -> dict[str, Any] | None:
    if not isinstance(doc, dict):
        return None
    item = dict(doc)
    try:
        resolved = _kakao_coord2address_document(item.get("x"), item.get("y"))
    except Exception as exc:
        logger.warning("failed to enrich kakao keyword address: %s", exc)
        resolved = None

    if isinstance(resolved, dict):
        resolved = _attach_kakao_b_code(resolved, item.get("x"), item.get("y"))
        if isinstance(resolved.get("address"), dict):
            item["address"] = resolved["address"]
        if isinstance(resolved.get("road_address"), dict):
            item["road_address"] = resolved["road_address"]

    if not item.get("address_name"):
        item["address_name"] = item.get("road_address_name") or item.get("place_name") or ""
    item["source"] = "kakao_keyword"
    return _normalize_kakao_address_doc(item)


@app.get("/v1/addr/search")
def addr_search(
    query: str = Query(...),
    page: int = Query(1),
    page_size: int = Query(10),
) -> Dict[str, Any]:
    safe_query = str(query or "").strip()
    safe_page = max(1, int(page or 1))
    safe_page_size = max(1, min(15, int(page_size or 10)))
    if not safe_query:
        return ok({"query": safe_query, "page": safe_page, "page_size": safe_page_size, "documents": []})

    try:
        payload = _kakao_get_json(
            "/v2/local/search/address.json",
            {
                "query": safe_query,
                "page": safe_page,
                "size": safe_page_size,
                "analyze_type": "exact",
            },
        )
        raw_documents = payload.get("documents") if isinstance(payload.get("documents"), list) else []
        documents = [
            item
            for item in (_normalize_kakao_address_doc(doc) for doc in raw_documents)
            if item is not None
        ]
        using_keyword = False

        if not documents:
            keyword_payload = _kakao_get_json(
                "/v2/local/search/keyword.json",
                {
                    "query": safe_query,
                    "page": safe_page,
                    "size": safe_page_size,
                },
            )
            raw_documents = (
                keyword_payload.get("documents") if isinstance(keyword_payload.get("documents"), list) else []
            )
            documents = [
                item
                for item in (_normalize_kakao_keyword_doc(doc) for doc in raw_documents)
                if item is not None
            ]
            payload = keyword_payload
            using_keyword = True

        return ok(
            {
                "query": safe_query,
                "page": safe_page,
                "page_size": safe_page_size,
                "provider": "kakao",
                "mode": "keyword" if using_keyword else "address",
                "meta": payload.get("meta", {}),
                "documents": documents,
            }
        )
    except Exception as exc:
        logger.warning("address search failed: %s", exc)
        return ok(
            {
                "query": safe_query,
                "page": safe_page,
                "page_size": safe_page_size,
                "provider": "kakao",
                "documents": [],
                "error_message": str(exc)[:240],
            }
        )


@app.get("/v1/addr/coord2address")
def coord2address(x: float = Query(...), y: float = Query(...)) -> Dict[str, Any]:
    try:
        payload = _kakao_get_json("/v2/local/geo/coord2address.json", {"x": x, "y": y})
        documents = [
            item
            for item in (
                _normalize_kakao_address_doc(_attach_kakao_b_code(doc, x, y) if isinstance(doc, dict) else doc)
                for doc in payload.get("documents", [])
            )
            if item is not None
        ]
        return ok({"x": x, "y": y, "provider": "kakao", "meta": payload.get("meta", {}), "documents": documents})
    except Exception as exc:
        logger.warning("coord2address failed: %s", exc)
        return ok({"x": x, "y": y, "provider": "kakao", "documents": [], "error_message": str(exc)[:240]})


@app.get("/v1/addr/coord2region")
def coord2region(x: float = Query(...), y: float = Query(...)) -> Dict[str, Any]:
    try:
        payload = _kakao_get_json("/v2/local/geo/coord2regioncode.json", {"x": x, "y": y})
        return ok(
            {
                "x": x,
                "y": y,
                "provider": "kakao",
                "meta": payload.get("meta", {}),
                "documents": payload.get("documents", []),
            }
        )
    except Exception as exc:
        logger.warning("coord2region failed: %s", exc)
        return ok({"x": x, "y": y, "provider": "kakao", "documents": [], "error_message": str(exc)[:240]})


@app.get("/v1/addr/position")
def position(lng: float = Query(...), lat: float = Query(...)) -> Dict[str, Any]:
    return ok({"lng": lng, "lat": lat, "results": []})


@app.get("/v1/addr/geocode")
def geocode(address: str = Query(...), epsg: str = Query("EPSG:4326")) -> Dict[str, Any]:
    result = addr_search(query=address, page=1, page_size=1)
    documents = result.get("data", {}).get("documents", []) if isinstance(result, dict) else []
    results = [
        {
            "address": item.get("address_name"),
            "road_address": item.get("road_address_name"),
            "pnu": item.get("pnu"),
            "x": item.get("x"),
            "y": item.get("y"),
        }
        for item in documents
        if isinstance(item, dict)
    ]
    return ok({"address": address, "epsg": epsg, "provider": "kakao", "results": results})
