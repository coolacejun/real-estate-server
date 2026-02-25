from __future__ import annotations

import asyncio
import base64
import contextlib
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
from collections import OrderedDict, deque
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Any, Dict

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image, ImageDraw, ImageFilter, ImageFont
from psycopg import sql
from logging.handlers import RotatingFileHandler

app = FastAPI(title="building-land API", version="1.2.0")
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
        "land_info": ["AL_D1*.csv", "*.csv"],
    }
    candidates.extend(typed_patterns.get(normalized, []))

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

    if normalized in {"cadastral", "building_info", "building_integrated_info", "land_info"}:
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


def _load_tile_features(z: int, x: int, y: int) -> list[dict[str, Any]]:
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
    row_limit = int(os.getenv("CADASTRAL_TILE_DB_LIMIT", "6000"))
    release_col = os.getenv("CADASTRAL_TILE_RELEASE_COL", "release_id")
    active_release = _active_release("cadastral")

    west, south, east, north = _tile_bounds(z, x, y)

    def _query_rows(with_release_filter: bool, with_label_point: bool) -> list[tuple[Any, str, str, Any, Any]]:
        clauses = [
            sql.SQL("{max_lon_col} >= %s").format(max_lon_col=sql.Identifier(max_lon_col)),
            sql.SQL("{min_lon_col} <= %s").format(min_lon_col=sql.Identifier(min_lon_col)),
            sql.SQL("{max_lat_col} >= %s").format(max_lat_col=sql.Identifier(max_lat_col)),
            sql.SQL("{min_lat_col} <= %s").format(min_lat_col=sql.Identifier(min_lat_col)),
        ]
        params: list[Any] = [west, east, south, north]

        if with_release_filter and active_release:
            clauses.append(sql.SQL("{release_col} = %s").format(release_col=sql.Identifier(release_col)))
            params.append(active_release["id"])

        params.append(row_limit)

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
            + sql.SQL(" LIMIT %s")
        )

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
    features = _load_tile_features(z, x, y)
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
    """Return query candidates for PNU.

    Some upstream clients occasionally send a PNU with wrong 산/대지 flag (11th digit).
    We keep the original first, then try toggled flag as a fallback.
    """
    raw = str(pnu or "").strip()
    if not raw:
        return []
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 19:
        return [raw]
    norm = digits[-19:]
    candidates = [norm]
    land_flag = norm[10]
    if land_flag in {"0", "1"}:
        toggled = f"{norm[:10]}{'1' if land_flag == '0' else '0'}{norm[11:]}"
        if toggled != norm:
            candidates.append(toggled)
    return candidates


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

    sql_text = """
    SELECT
      COALESCE((SELECT json_agg(line)::text FROM total_building_line_v WHERE pnu = %s), '[]') AS total,
      COALESCE((SELECT json_agg(line)::text FROM building_line_v WHERE pnu = %s), '[]') AS single,
      COALESCE((SELECT json_agg(line)::text FROM floor_line_v WHERE pnu = %s), '[]') AS floor,
      COALESCE((SELECT json_agg(line)::text FROM room_line_v WHERE pnu = %s), '[]') AS room
    """
    try:
        with psycopg.connect(_db_url()) as conn:
            with conn.cursor() as cur:
                for candidate_pnu in pnu_candidates:
                    cur.execute(sql_text, (candidate_pnu, candidate_pnu, candidate_pnu, candidate_pnu))
                    row = cur.fetchone()
                    if not row:
                        continue
                    total = json.loads(row[0] or "[]")
                    single = json.loads(row[1] or "[]")
                    floor = json.loads(row[2] or "[]")
                    room = json.loads(row[3] or "[]")
                    if total or single or floor or room:
                        payload = {
                            "total": total,
                            "single": single,
                            "floor": floor,
                            "room": room,
                        }
                        # Legacy view storage: attempt to attach meta as well.
                        active_release = _active_release("building_info")
                        if not active_release:
                            meta_only = _bucket_irts_meta(payload)  # type: ignore[arg-type]
                            if meta_only:
                                enriched = dict(payload)
                                enriched["meta"] = meta_only
                                return pnu + json.dumps(enriched, ensure_ascii=False)
                            return pnu + json.dumps(payload, ensure_ascii=False)
                        enriched = payload
                        try:
                            enriched = _attach_meta(
                                payload,  # type: ignore[arg-type]
                                conn=conn,
                                release_id=int(active_release["id"]),
                                candidate_pnu=candidate_pnu,
                            )
                        except Exception:
                            enriched = payload
                        return pnu + json.dumps(enriched, ensure_ascii=False)
    except Exception:
        return None
    return None


def _fetch_dataset_records(data_type: str, pnu: str, limit: int = 300) -> list[Dict[str, Any]]:
    safe_limit = max(1, min(2000, int(limit)))
    normalized_type = _normalize_data_type(data_type)

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

    if normalized_type == "land_info":
        try:
            with psycopg.connect(_db_url()) as conn:
                land_rows: list[tuple[Any, ...]] = []
                for candidate_pnu in pnu_candidates:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT dataset_code, schema_id, payload_values
                            FROM public.land_info_record
                            WHERE release_id = %s
                              AND pnu = %s
                            ORDER BY id
                            LIMIT %s
                            """,
                            (active_release["id"], candidate_pnu, safe_limit),
                        )
                        land_rows = cur.fetchall()
                    if land_rows:
                        break
        except Exception:
            land_rows = []

        if land_rows:
            schema_columns_by_id = _load_dataset_schema_columns(
                [int(row[1]) for row in land_rows if row[1] is not None]
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
                for row in land_rows
            ]

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
                "label": str(label_raw or ""),
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
                "label": str(label_raw or ""),
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
                "label": str(label_raw or ""),
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
    parallel_capable_types = {"building_info", "cadastral", "building_integrated_info", "land_info"}
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


@app.get("/v1/data/{collection}/{pnu}")
def get_data(
    collection: str,
    pnu: str,
    format: str = Query("compressed", pattern="^(compressed|lines)$"),
) -> Dict[str, Any]:
    if collection == "building_info":
        line = _fetch_building_info_line(pnu)
        if not line:
            if format == "lines":
                return ok({"format": "lines", "lines": []})
            return ok({"format": "compressed", "parts": []})
        if format == "lines":
            return ok({"format": "lines", "lines": [line]})
        return ok({"format": "compressed", "parts": [line]})

    if collection in {"building_integrated_info", "land_info"}:
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


@app.get("/v1/addr/search")
def addr_search(
    query: str = Query(...),
    page: int = Query(1),
    page_size: int = Query(10),
) -> Dict[str, Any]:
    return ok({"query": query, "page": page, "page_size": page_size, "documents": []})


@app.get("/v1/addr/coord2address")
def coord2address(x: float = Query(...), y: float = Query(...)) -> Dict[str, Any]:
    return ok({"x": x, "y": y, "documents": []})


@app.get("/v1/addr/coord2region")
def coord2region(x: float = Query(...), y: float = Query(...)) -> Dict[str, Any]:
    return ok({"x": x, "y": y, "documents": []})


@app.get("/v1/addr/position")
def position(lng: float = Query(...), lat: float = Query(...)) -> Dict[str, Any]:
    return ok({"lng": lng, "lat": lat, "results": []})


@app.get("/v1/addr/geocode")
def geocode(address: str = Query(...), epsg: str = Query("EPSG:4326")) -> Dict[str, Any]:
    return ok({"address": address, "epsg": epsg, "results": []})
