#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import csv
import datetime as dt
import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import psycopg
except Exception:  # pragma: no cover - host fallback can still run discovery-only pieces.
    psycopg = None  # type: ignore[assignment]


DEFAULT_USER_AGENT = "Mozilla/5.0"
TERMINAL_JOB_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED"}


LAND_INFO_DATASETS: tuple[dict[str, str], ...] = (
    {"key": "land_use_plan", "name": "토지이용계획", "code": "AL_D155", "page_id": "14"},
    {"key": "land_movement", "name": "토지이동", "code": "AL_D157", "page_id": "13"},
    {"key": "land_ownership", "name": "토지소유", "code": "AL_D161", "page_id": "12"},
    {"key": "land_characteristic", "name": "토지특성", "code": "AL_D195", "page_id": "4"},
)


@dataclass(frozen=True)
class LandInfoFullItem:
    dataset_key: str
    dataset_name: str
    dataset_code: str
    page_id: str
    base_date: str
    updated_date: str
    file_no: str
    ds_file_id: str
    title: str
    region: str
    size_text: str
    size_bytes: int

    @property
    def date_compact(self) -> str:
        return self.base_date.replace("-", "")

    @property
    def expected_glob(self) -> str:
        return f"{self.dataset_code}_*_{self.date_compact}.zip"

    @property
    def download_url(self) -> str:
        return (
            "https://www.vworld.kr/dtmk/downloadResourceFile.do?"
            + urllib.parse.urlencode({"ds_id": self.ds_file_id, "fileNo": self.file_no})
        )


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_worker_dir = os.getenv("LAND_INFO_WORKER_DIR", "")
    if not default_worker_dir:
        default_worker_dir = "/worker/land-info-worker"
        if not Path(default_worker_dir).exists() and Path("/Volumes/land-info-worker").exists():
            default_worker_dir = "/Volumes/land-info-worker"
    parser = argparse.ArgumentParser(
        description="Coordinate VWorld land_info full downloads through a Windows worker and import them."
    )
    parser.add_argument("--repo-root", default=str(repo_root))
    parser.add_argument(
        "--base-dir",
        default=os.getenv("LAND_INFO_SYNC_BASE_DIR", str(repo_root / "토지정보/auto")),
    )
    parser.add_argument("--worker-dir", default=default_worker_dir)
    parser.add_argument(
        "--api-base",
        default=os.getenv("BUILDING_LAND_API_BASE", os.getenv("LAND_INFO_SYNC_API_BASE", "http://localhost")),
    )
    parser.add_argument("--env-file", default=str(repo_root / ".env"))
    parser.add_argument("--page-count", type=int, default=int(os.getenv("LAND_INFO_SYNC_PAGE_COUNT", "5") or "5"))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("LAND_INFO_SYNC_PAGE_SIZE", "100") or "100"))
    parser.add_argument("--fetch-timeout", type=float, default=float(os.getenv("LAND_INFO_SYNC_FETCH_TIMEOUT", "120") or "120"))
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("LAND_INFO_SYNC_POLL_INTERVAL_SECONDS", "30") or "30"))
    parser.add_argument("--import-timeout", type=float, default=float(os.getenv("LAND_INFO_SYNC_IMPORT_TIMEOUT_SECONDS", "86400") or "86400"))
    parser.add_argument("--stable-seconds", type=float, default=float(os.getenv("LAND_INFO_SYNC_STABLE_SECONDS", "60") or "60"))
    parser.add_argument("--manifest", default="cycle_manifest.json")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--cleanup-on-success",
        action=argparse.BooleanOptionalAction,
        default=_truthy_env("LAND_INFO_SYNC_CLEANUP_ON_SUCCESS", default=True),
    )
    return parser.parse_args()


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "t", "yes", "y", "on"}


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def database_url(args: argparse.Namespace) -> str:
    direct = os.getenv("DATABASE_URL", "").strip()
    if direct:
        return direct
    env = load_env_file(Path(args.env_file))
    if env.get("DATABASE_URL"):
        return str(env["DATABASE_URL"]).strip()
    host = os.getenv("POSTGRES_HOST", env.get("POSTGRES_HOST", "")).strip()
    if not host:
        return ""
    port = os.getenv("POSTGRES_PORT", env.get("POSTGRES_PORT", "5432")).strip() or "5432"
    user = os.getenv("POSTGRES_USER", env.get("POSTGRES_USER", "appuser"))
    password = os.getenv("POSTGRES_PASSWORD", env.get("POSTGRES_PASSWORD", ""))
    db = os.getenv("POSTGRES_DB", env.get("POSTGRES_DB", "appdb"))
    return f"postgresql://{urllib.parse.quote(user)}:{urllib.parse.quote(password)}@{host}:{port}/{urllib.parse.quote(db)}"


def http_json(
    api_base: str,
    path: str,
    *,
    method: str = "GET",
    token: str = "",
    body: dict[str, Any] | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    url = api_base.rstrip("/") + path
    payload = None
    headers: dict[str, str] = {"Accept": "application/json"}
    if token:
        headers["x-admin-token"] = token
    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"{method} {path} failed: HTTP {exc.code} {detail}") from exc
    return json.loads(raw)


def fetch_url(url: str, *, timeout: float) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def strip_tags(value: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def size_to_bytes(value: str, unit: str) -> int:
    number = float(str(value or "0").replace(",", ""))
    normalized = str(unit or "").strip().upper()
    if normalized.startswith("GB"):
        return int(number * 1024 * 1024 * 1024)
    if normalized.startswith("MB"):
        return int(number * 1024 * 1024)
    if normalized.startswith("KB"):
        return int(number * 1024)
    if normalized.startswith("BYTE") or normalized == "B":
        return int(number)
    return int(number)


def _match_first(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.S)
    return strip_tags(match.group(1)) if match else ""


def _span_date(text: str, label: str) -> str:
    match = re.search(rf"<span>{re.escape(label)}<em class=\"xxs\">([^<]+)</em></span>", text)
    return match.group(1).strip() if match else ""


def parse_full_items(html: str, dataset: dict[str, str]) -> list[LandInfoFullItem]:
    rows: list[LandInfoFullItem] = []
    parts = re.split(r'<li><!--v-for="n in 10"-->|<li>', html)
    for part in parts:
        if 'name="dsFileSq"' not in part or "구분<em>전체데이터</em>" not in part:
            continue
        file_no = _match_first(part, r'name="dsFileSq" value="([^"]+)"')
        ds_file_id = _match_first(part, r'name="dsFileId" value="([^"]+)"')
        base_date = _span_date(part, "기준일")
        updated_date = _span_date(part, "갱신일")
        title = _match_first(part, r'<div class="tit min">(.*?)</div>')
        region = _match_first(part, r'<span class="sigunguNm1">\s*(.*?)</span>')
        text = strip_tags(part)
        size_match = re.search(r"용량\s*([0-9,]+)\s*(BYTES|KB|MB|GB)", text, flags=re.I)
        size_value = size_match.group(1) if size_match else "0"
        size_unit = size_match.group(2) if size_match else "KB"
        if not file_no or not ds_file_id or not base_date:
            continue
        rows.append(
            LandInfoFullItem(
                dataset_key=dataset["key"],
                dataset_name=dataset["name"],
                dataset_code=dataset["code"],
                page_id=dataset["page_id"],
                base_date=base_date,
                updated_date=updated_date,
                file_no=file_no,
                ds_file_id=ds_file_id,
                title=title,
                region=region,
                size_text=f"{size_value} {size_unit}",
                size_bytes=size_to_bytes(size_value, size_unit),
            )
        )
    return rows


def list_page_url(dataset: dict[str, str], page_index: int, page_size: int) -> str:
    params = {
        "searchKeyword": "",
        "searchSvcCde": "",
        "searchOrganization": "",
        "searchBrmCode": "",
        "searchTagList": "",
        "searchFrm": "",
        "pageIndex": "1",
        "gidmCd": "01",
        "gidsCd": "0108",
        "sortType": "00",
        "svcCde": "NA",
        "dsId": dataset["page_id"],
        "dataSetSeq": dataset["page_id"],
        "listPageIndex": "1",
        "datPageIndex": str(page_index),
        "datPageSize": str(page_size),
        "pageSize": str(page_size),
        "pageUnit": str(page_size),
        "fileGbnCd": "AL",
        "formatSelect": "CSV",
        "startDate": (dt.date.today() - dt.timedelta(days=900)).isoformat(),
        "endDate": dt.date.today().isoformat(),
    }
    return "https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?" + urllib.parse.urlencode(params)


def discover_latest_items(args: argparse.Namespace) -> tuple[list[LandInfoFullItem], dict[str, str]]:
    all_latest: list[LandInfoFullItem] = []
    base_dates: dict[str, str] = {}
    for dataset in LAND_INFO_DATASETS:
        found: list[LandInfoFullItem] = []
        seen: set[str] = set()
        for page in range(1, max(1, args.page_count) + 1):
            raw = fetch_url(list_page_url(dataset, page, args.page_size), timeout=args.fetch_timeout)
            rows = parse_full_items(raw.decode("utf-8", "replace"), dataset)
            for row in rows:
                if row.file_no in seen:
                    continue
                seen.add(row.file_no)
                found.append(row)
            if page > 1 and not rows:
                break
        if not found:
            raise RuntimeError(f"latest full items not found: {dataset['name']} {dataset['code']}")
        latest_date = max(item.base_date for item in found)
        latest = [item for item in found if item.base_date == latest_date]
        if not latest:
            raise RuntimeError(f"latest full item selection failed: {dataset['name']} {latest_date}")
        base_dates[dataset["code"]] = latest_date
        all_latest.extend(sorted(latest, key=lambda item: int(item.file_no) if item.file_no.isdigit() else item.file_no))
    return all_latest, base_dates


def snapshot_key(base_dates: dict[str, str]) -> str:
    return "|".join(f"{code}={base_dates.get(code, '')}" for code in sorted(base_dates))


def request_id_for_snapshot(key: str) -> str:
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    compact = re.sub(r"[^0-9]+", "", key)
    prefix = compact[:8] if compact else dt.date.today().strftime("%Y%m%d")
    return f"land_info_full_{prefix}_{digest}"


def ensure_dirs(base_dir: Path, worker_dir: Path) -> dict[str, Path]:
    dirs = {
        "base": base_dir,
        "extracted": base_dir / "extracted",
        "staging": base_dir / "staging",
        "manifests": base_dir / "manifests",
        "worker_requests": worker_dir / "requests",
        "worker_downloads": worker_dir / "downloads",
        "worker_manifests": worker_dir / "manifests",
        "worker_logs": worker_dir / "logs",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def write_manifest(base_dir: Path, name: str, data: dict[str, Any]) -> None:
    manifests_dir = base_dir / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    (manifests_dir / name).write_text(payload, encoding="utf-8")
    (base_dir / "cycle_manifest.json").write_text(
        json.dumps({"latest": data}, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def write_request(requests_dir: Path, request: dict[str, Any], *, force: bool) -> Path:
    target = requests_dir / f"{request['request_id']}.json"
    if target.exists() and not force:
        return target
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(request, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(target)
    latest = requests_dir / "latest_land_info_full_request.json"
    latest.write_text(json.dumps(request, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target


def read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
            return data if isinstance(data, dict) else None
    except Exception:
        return None
    return None


def active_release_metadata(args: argparse.Namespace) -> dict[str, Any] | None:
    if psycopg is None:
        return None
    db_url = database_url(args)
    if not db_url:
        return None
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, version, records_count, metadata
                FROM cadastral_release
                WHERE data_type = 'land_info'
                  AND is_active IS TRUE
                ORDER BY activated_at DESC NULLS LAST, id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            metadata = row[3] if isinstance(row[3], dict) else {}
            return {
                "id": int(row[0]),
                "version": row[1],
                "records_count": int(row[2] or 0),
                "metadata": metadata,
            }


def worker_completed_manifest(worker_manifests: Path, request_id: str) -> dict[str, Any] | None:
    candidates = [
        worker_manifests / f"{request_id}.completed.json",
        worker_manifests / f"{request_id}.json",
    ]
    for path in candidates:
        data = read_json_file(path)
        if data and str(data.get("status") or "").lower() in {"completed", "succeeded", "done"}:
            return data
    return None


def _is_stable(path: Path, stable_seconds: float) -> bool:
    try:
        stat = path.stat()
    except OSError:
        return False
    return stat.st_size > 0 and (time.time() - stat.st_mtime) >= max(0.0, stable_seconds)


def collect_completed_zip_files(
    downloads_dir: Path,
    worker_manifests: Path,
    request_id: str,
    items: list[LandInfoFullItem],
    *,
    stable_seconds: float,
) -> tuple[list[Path], dict[str, Any]]:
    expected_counts: dict[str, int] = {}
    dates_by_code: dict[str, str] = {}
    for item in items:
        expected_counts[item.dataset_code] = expected_counts.get(item.dataset_code, 0) + 1
        dates_by_code[item.dataset_code] = item.date_compact

    completed = worker_completed_manifest(worker_manifests, request_id)
    manifest_files: list[Path] = []
    if completed:
        for entry in completed.get("files") or []:
            if isinstance(entry, str):
                raw = entry
            elif isinstance(entry, dict):
                raw = str(entry.get("path") or entry.get("file_path") or entry.get("file_name") or "")
            else:
                continue
            if not raw:
                continue
            path = Path(raw)
            if not path.is_absolute():
                path = downloads_dir / raw
            if path.exists() and path.suffix.lower() == ".zip" and _is_stable(path, 0):
                manifest_files.append(path)

    found_by_code: dict[str, list[Path]] = {code: [] for code in expected_counts}
    candidates = manifest_files or sorted(downloads_dir.rglob("*.zip"))
    for path in candidates:
        name = path.name.upper()
        if not _is_stable(path, stable_seconds):
            continue
        for code, compact in dates_by_code.items():
            pattern = f"{code}_*_{compact}.ZIP"
            if fnmatch.fnmatch(name, pattern):
                found_by_code.setdefault(code, []).append(path)
                break

    selected: list[Path] = []
    missing: dict[str, int] = {}
    found_counts: dict[str, int] = {}
    for code, expected_count in expected_counts.items():
        unique = sorted({path.resolve() for path in found_by_code.get(code, [])})
        found_counts[code] = len(unique)
        if len(unique) < expected_count:
            missing[code] = expected_count - len(unique)
        selected.extend(unique[:expected_count])

    return selected, {
        "expected_counts": expected_counts,
        "found_counts": found_counts,
        "missing_counts": missing,
        "completed_manifest": completed,
    }


def _can_decode_sample(raw: bytes, encoding: str, max_trim: int) -> bool:
    if not raw:
        return True
    upper_trim = max(0, min(max_trim, len(raw) - 1))
    for trim in range(0, upper_trim + 1):
        chunk = raw if trim == 0 else raw[:-trim]
        if not chunk:
            continue
        try:
            chunk.decode(encoding)
            return True
        except UnicodeDecodeError:
            continue
    return False


def detect_encoding(path: Path) -> str:
    with path.open("rb") as fp:
        raw = fp.read(131072)
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if _can_decode_sample(raw, "utf-8", max_trim=3):
        return "utf-8"
    if _can_decode_sample(raw, "cp949", max_trim=1):
        return "cp949"
    if _can_decode_sample(raw, "euc-kr", max_trim=1):
        return "euc-kr"
    if any(byte >= 0x80 for byte in raw):
        return "cp949"
    return "utf-8"


def prepare_staging(zip_paths: list[Path], extracted_root: Path, stage_dir: Path, *, force: bool) -> dict[str, Any]:
    if stage_dir.exists():
        if not force:
            shutil.rmtree(stage_dir)
        else:
            shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    prepared: list[dict[str, Any]] = []
    csv_paths: list[Path] = []
    for zip_path in zip_paths:
        if not zipfile.is_zipfile(zip_path):
            raise RuntimeError(f"invalid zip file: {zip_path}")
        work_dir = extracted_root / zip_path.stem
        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(work_dir)
        extracted_csvs = sorted([*work_dir.rglob("*.csv"), *work_dir.rglob("*.CSV")])
        if not extracted_csvs:
            raise RuntimeError(f"no csv found in zip: {zip_path.name}")
        copied: list[str] = []
        for csv_path in extracted_csvs:
            target = stage_dir / csv_path.name
            if target.exists():
                target = stage_dir / f"{csv_path.stem}_{zip_path.stem}{csv_path.suffix}"
            shutil.copy2(csv_path, target)
            csv_paths.append(target)
            copied.append(str(target))
        prepared.append(
            {
                "zip_path": str(zip_path),
                "zip_name": zip_path.name,
                "zip_size": zip_path.stat().st_size,
                "extracted_dir": str(work_dir),
                "csv_files": copied,
            }
        )
    return {
        "stage_dir": str(stage_dir),
        "prepared": prepared,
        "csv_files": [str(path) for path in csv_paths],
    }


def find_sample_pnu(stage_dir: Path) -> str:
    for path in sorted(stage_dir.glob("AL_D1*.csv")):
        enc = detect_encoding(path)
        with path.open("r", encoding=enc, errors="replace", newline="") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                for key in ("고유번호", "PNU", "pnu"):
                    value = str((row or {}).get(key) or "").strip()
                    if value:
                        return value
    return ""


def start_import(api_base: str, token: str, stage_dir: Path, request_id: str, snapshot: str) -> dict[str, Any]:
    body = {
        "data_type": "land_info",
        "operation_mode": "full",
        "source_path": str(stage_dir),
        "pattern": "AL_D1*.csv",
        "source_name": f"VWorld land_info full {snapshot}",
        "version": f"{request_id}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}",
        "mark_ready": True,
    }
    payload = http_json(
        api_base,
        "/v1/admin/cadastral/import-from-path",
        method="POST",
        token=token,
        body=body,
        timeout=120,
    )
    data = payload.get("data")
    if not isinstance(data, dict) or not data.get("job_id") or not data.get("release_id"):
        raise RuntimeError(f"unexpected import response: {payload}")
    return data


def latest_job(api_base: str, token: str, job_id: int) -> dict[str, Any] | None:
    payload = http_json(api_base, "/v1/admin/cadastral/import-jobs?data_type=land_info&limit=20", token=token, timeout=60)
    data = payload.get("data")
    if not isinstance(data, list):
        return None
    for item in data:
        if int(item.get("id") or 0) == int(job_id):
            return item
    return None


def wait_for_import(api_base: str, token: str, job_id: int, timeout: float, poll_interval: float) -> dict[str, Any]:
    started = time.time()
    last_status = ""
    while True:
        job = latest_job(api_base, token, job_id)
        if not job:
            raise RuntimeError(f"import job not found: job_id={job_id}")
        status = str(job.get("status") or "")
        if status != last_status or status == "RUNNING":
            print(
                "[land-info-cycle] import "
                f"job={job_id} status={status} files={job.get('processed_files')}/{job.get('total_files')} "
                f"rows={job.get('inserted_rows')}",
                flush=True,
            )
            last_status = status
        if status in TERMINAL_JOB_STATUSES:
            if status != "SUCCEEDED":
                raise RuntimeError(f"import failed: {json.dumps(job, ensure_ascii=False, default=str)}")
            return job
        if time.time() - started > timeout:
            raise RuntimeError(f"import timed out: job_id={job_id}")
        time.sleep(max(5.0, poll_interval))


def verify_import(
    args: argparse.Namespace,
    *,
    expected_csv_names: list[str],
    sample_pnu: str,
    api_base: str,
    snapshot: str,
    request_id: str,
) -> dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; cannot verify imported land_info files")
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    expected = sorted({name for name in expected_csv_names if name})
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, version, records_count
                FROM cadastral_release
                WHERE data_type = 'land_info'
                  AND is_active IS TRUE
                ORDER BY activated_at DESC NULLS LAST, id DESC
                LIMIT 1
                """
            )
            release_row = cur.fetchone()
            if not release_row:
                raise RuntimeError("active land_info release not found after import")
            release_id = int(release_row[0])
            cur.execute(
                """
                SELECT DISTINCT file_name
                FROM dataset_import_file
                WHERE release_id = %s
                  AND data_type = 'land_info'
                  AND file_name = ANY(%s)
                """,
                (release_id, expected),
            )
            recorded = sorted({str(row[0]) for row in cur.fetchall() if row and row[0]})
            presence = {
                dataset["code"]: any(name.startswith(f"{dataset['code']}_") for name in recorded)
                for dataset in LAND_INFO_DATASETS
            }
            metadata_patch = {
                "land_info_snapshot_key": snapshot,
                "land_info_request_id": request_id,
                "land_info_datasets": {dataset["code"]: dataset["name"] for dataset in LAND_INFO_DATASETS},
            }
            cur.execute(
                """
                UPDATE cadastral_release
                SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (json.dumps(metadata_patch, ensure_ascii=False), release_id),
            )
    missing = sorted(set(expected) - set(recorded))
    missing_codes = sorted(code for code, ok in presence.items() if not ok)
    if missing:
        preview = ", ".join(missing[:10])
        suffix = "" if len(missing) <= 10 else f", ... +{len(missing) - 10}"
        raise RuntimeError(f"import verification failed: missing file records: {preview}{suffix}")
    if missing_codes:
        raise RuntimeError(f"import verification failed: missing dataset codes: {', '.join(missing_codes)}")

    client_check: dict[str, Any] = {"sample_pnu": sample_pnu, "ok": False}
    if sample_pnu:
        payload = http_json(api_base, f"/v1/data/land_info/{urllib.parse.quote(sample_pnu)}?format=compressed", timeout=60)
        data = payload.get("data") if isinstance(payload, dict) else None
        parts = data.get("parts") if isinstance(data, dict) else None
        client_check["parts_count"] = len(parts) if isinstance(parts, list) else 0
        client_check["ok"] = bool(isinstance(parts, list) and parts)
        if not client_check["ok"]:
            raise RuntimeError(f"client verification failed: pnu={sample_pnu}")

    return {
        "release_id": release_id,
        "release_version": release_row[1],
        "records_count": int(release_row[2] or 0),
        "expected_count": len(expected),
        "recorded_count": len(recorded),
        "missing": missing,
        "dataset_presence": presence,
        "client_check": client_check,
    }


def cleanup_sources(zip_paths: list[Path], stage_dir: Path, prepared: dict[str, Any]) -> dict[str, Any]:
    cleanup: dict[str, Any] = {"stage_dir": str(stage_dir), "extracted_dirs": [], "worker_zips": []}
    with contextlib.suppress(Exception):
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
    for entry in prepared.get("prepared") or []:
        extracted_dir = Path(str(entry.get("extracted_dir") or ""))
        if extracted_dir.exists():
            with contextlib.suppress(Exception):
                shutil.rmtree(extracted_dir)
                cleanup["extracted_dirs"].append(str(extracted_dir))
    for path in zip_paths:
        if path.exists():
            with contextlib.suppress(Exception):
                path.unlink()
                cleanup["worker_zips"].append(str(path))
    return cleanup


def import_script_path(args: argparse.Namespace) -> Path:
    candidates = [
        Path("/scripts/import_land_info_csv.py"),
        Path(args.repo_root).resolve() / "scripts" / "import_land_info_csv.py",
        Path(__file__).resolve().with_name("import_land_info_csv.py"),
    ]
    for path in candidates:
        if path.exists():
            return path
    raise RuntimeError("import_land_info_csv.py not found")


def _zip_member_basename(name: str) -> str:
    return re.split(r"[\\/]", str(name or ""))[-1].strip()


def inspect_zip_csv_entries(zip_paths: list[Path]) -> dict[str, list[dict[str, str]]]:
    by_zip: dict[str, list[dict[str, str]]] = {}
    used_names: set[str] = set()
    for zip_path in zip_paths:
        if not zipfile.is_zipfile(zip_path):
            raise RuntimeError(f"invalid zip file: {zip_path}")
        entries: list[dict[str, str]] = []
        with zipfile.ZipFile(zip_path) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                base_name = _zip_member_basename(info.filename)
                if not base_name.lower().endswith(".csv"):
                    continue
                target_name = base_name
                if target_name in used_names:
                    suffix = Path(base_name).suffix
                    stem = Path(base_name).stem
                    target_name = f"{stem}_{zip_path.stem}{suffix}"
                    counter = 2
                    while target_name in used_names:
                        target_name = f"{stem}_{zip_path.stem}_{counter}{suffix}"
                        counter += 1
                used_names.add(target_name)
                entries.append({"member": info.filename, "csv_name": target_name})
        if not entries:
            raise RuntimeError(f"no csv found in zip: {zip_path.name}")
        by_zip[str(zip_path)] = entries
    return by_zip


def extract_zip_csv_entries(zip_path: Path, entries: list[dict[str, str]], stage_dir: Path) -> list[Path]:
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as archive:
        for entry in entries:
            member = str(entry["member"])
            target = stage_dir / str(entry["csv_name"])
            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            extracted.append(target)
    return extracted


def _dataset_code_from_csv_name(name: str) -> str:
    stem = Path(name).stem.upper()
    token = stem.split("_")[0:2]
    if len(token) >= 2 and token[0] in {"AL", "CH"} and token[1].startswith("D"):
        return "_".join(["AL", token[1]])
    return stem[:40]


def create_streaming_import_run(
    args: argparse.Namespace,
    *,
    source_path: Path,
    request_id: str,
    snapshot: str,
    base_dates: dict[str, str],
    expected_csv_count: int,
) -> dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; cannot create land_info import release")
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    version = f"{request_id}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}"
    metadata = {
        "trigger": "land_info_full_cycle_streaming",
        "data_type": "land_info",
        "operation_mode": "full",
        "streaming": True,
        "snapshot_key": snapshot,
        "base_dates": base_dates,
        "request_id": request_id,
        "source_path": str(source_path),
        "pattern": "AL_D1*.csv",
        "total_files": expected_csv_count,
    }
    with psycopg.connect(db_url) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM cadastral_release
                    WHERE data_type = 'land_info'
                      AND version = %s
                    LIMIT 1
                    """,
                    (version,),
                )
                if cur.fetchone():
                    version = f"{version}-{dt.datetime.now().strftime('%f')}"
                cur.execute(
                    """
                    INSERT INTO cadastral_release (version, data_type, source_name, status, metadata)
                    VALUES (%s, 'land_info', %s, 'PENDING', %s::jsonb)
                    RETURNING id
                    """,
                    (
                        version,
                        f"VWorld land_info full {snapshot}",
                        json.dumps(metadata, ensure_ascii=False),
                    ),
                )
                release_row = cur.fetchone()
                if not release_row:
                    raise RuntimeError("failed to create land_info release")
                release_id = int(release_row[0])
                cur.execute(
                    """
                    INSERT INTO cadastral_import_job
                      (release_id, data_type, status, source_path, total_files)
                    VALUES (%s, 'land_info', 'QUEUED', %s, %s)
                    RETURNING id
                    """,
                    (release_id, str(source_path), int(expected_csv_count)),
                )
                job_row = cur.fetchone()
                if not job_row:
                    raise RuntimeError("failed to create land_info import job")
                job_id = int(job_row[0])
    return {"release_id": release_id, "release_version": version, "job_id": job_id}


def run_import_for_csv(
    args: argparse.Namespace,
    *,
    release_id: int,
    job_id: int,
    expected_csv_count: int,
    stage_dir: Path,
    csv_path: Path,
    timeout: float,
) -> None:
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    script_path = import_script_path(args)
    env = os.environ.copy()
    env["DATABASE_URL"] = db_url
    cmd = [
        sys.executable,
        str(script_path),
        "--data-type",
        "land_info",
        "--release-id",
        str(release_id),
        "--source-dir",
        str(stage_dir),
        "--pattern",
        csv_path.name,
        "--job-id",
        str(job_id),
        "--job-total-files",
        str(expected_csv_count),
        "--operation-mode",
        "full",
        "--keep-job-open",
        "--no-reset-job-workers",
    ]
    completed = subprocess.run(
        cmd,
        env=env,
        cwd=str(Path(args.repo_root).resolve()) if Path(args.repo_root).exists() else None,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"land_info import failed: file={csv_path.name}, exit={completed.returncode}")


def verify_streamed_csv_file(args: argparse.Namespace, *, release_id: int, csv_path: Path) -> dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; cannot verify streamed file")
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    dataset_code = _dataset_code_from_csv_name(csv_path.name)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT file_size
                FROM dataset_import_file
                WHERE release_id = %s
                  AND data_type = 'land_info'
                  AND file_name = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(release_id), csv_path.name),
            )
            file_row = cur.fetchone()
            if not file_row:
                raise RuntimeError(f"file verification failed: missing dataset_import_file row: {csv_path.name}")
            cur.execute(
                """
                SELECT COUNT(*)
                FROM public.land_info_record
                WHERE release_id = %s
                  AND dataset_code = %s
                """,
                (int(release_id), dataset_code),
            )
            row = cur.fetchone()
            dataset_rows = int(row[0] or 0) if row else 0
            if dataset_rows <= 0:
                raise RuntimeError(f"file verification failed: no rows for dataset_code={dataset_code}")
    return {
        "csv_name": csv_path.name,
        "csv_size": int(file_row[0] or 0),
        "dataset_code": dataset_code,
        "dataset_rows": dataset_rows,
    }


def mark_streaming_job_failed(args: argparse.Namespace, *, release_id: int | None, job_id: int | None, error: str) -> None:
    if psycopg is None or not release_id or not job_id:
        return
    db_url = database_url(args)
    if not db_url:
        return
    with contextlib.suppress(Exception):
        with psycopg.connect(db_url) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE cadastral_import_job
                    SET status = 'FAILED',
                        error_message = %s,
                        finished_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (str(error)[:2000], int(job_id)),
                )
                conn.execute(
                    """
                    UPDATE cadastral_release
                    SET status = CASE WHEN is_active THEN status ELSE 'FAILED' END,
                        updated_at = NOW()
                    WHERE id = %s
                      AND data_type = 'land_info'
                    """,
                    (int(release_id),),
                )


def activate_streaming_release(
    args: argparse.Namespace,
    *,
    release_id: int,
    job_id: int,
    expected_csv_names: list[str],
    snapshot: str,
    request_id: str,
) -> dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; cannot activate streamed release")
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    expected = sorted({name for name in expected_csv_names if name})
    with psycopg.connect(db_url) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT file_name
                    FROM dataset_import_file
                    WHERE release_id = %s
                      AND data_type = 'land_info'
                      AND file_name = ANY(%s)
                    """,
                    (int(release_id), expected),
                )
                recorded = sorted({str(row[0]) for row in cur.fetchall() if row and row[0]})
                missing = sorted(set(expected) - set(recorded))
                if missing:
                    preview = ", ".join(missing[:10])
                    suffix = "" if len(missing) <= 10 else f", ... +{len(missing) - 10}"
                    raise RuntimeError(f"activation blocked: missing file records: {preview}{suffix}")

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM public.land_info_record
                    WHERE release_id = %s
                    """,
                    (int(release_id),),
                )
                row = cur.fetchone()
                records_count = int(row[0] or 0) if row else 0
                if records_count <= 0:
                    raise RuntimeError("activation blocked: imported land_info rows are empty")

                presence = {
                    dataset["code"]: any(name.startswith(f"{dataset['code']}_") for name in recorded)
                    for dataset in LAND_INFO_DATASETS
                }
                missing_codes = sorted(code for code, ok in presence.items() if not ok)
                if missing_codes:
                    raise RuntimeError(f"activation blocked: missing dataset codes: {', '.join(missing_codes)}")

                metadata_patch = {
                    "land_info_snapshot_key": snapshot,
                    "land_info_request_id": request_id,
                    "land_info_datasets": {dataset["code"]: dataset["name"] for dataset in LAND_INFO_DATASETS},
                    "streaming_import": True,
                }
                cur.execute(
                    """
                    UPDATE cadastral_release
                    SET is_active = FALSE,
                        status = CASE WHEN status = 'ACTIVE' THEN 'READY' ELSE status END,
                        updated_at = NOW()
                    WHERE is_active = TRUE
                      AND data_type = 'land_info'
                      AND id <> %s
                    """,
                    (int(release_id),),
                )
                cur.execute(
                    """
                    UPDATE cadastral_release
                    SET is_active = TRUE,
                        status = 'ACTIVE',
                        activated_at = NOW(),
                        records_count = %s,
                        metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id = %s
                      AND data_type = 'land_info'
                    RETURNING version
                    """,
                    (records_count, json.dumps(metadata_patch, ensure_ascii=False), int(release_id)),
                )
                release_row = cur.fetchone()
                if not release_row:
                    raise RuntimeError(f"release not found: {release_id}")
                cur.execute(
                    """
                    UPDATE cadastral_import_job
                    SET status = 'SUCCEEDED',
                        total_files = %s,
                        processed_files = GREATEST(processed_files, %s),
                        error_message = NULL,
                        finished_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (len(expected), len(expected), int(job_id)),
                )
    return {
        "release_id": int(release_id),
        "release_version": release_row[0],
        "records_count": records_count,
        "expected_count": len(expected),
        "recorded_count": len(recorded),
        "dataset_presence": presence,
    }


def restore_previous_active_release(
    args: argparse.Namespace,
    *,
    previous_release_id: int | None,
    failed_release_id: int | None,
) -> None:
    if psycopg is None or not previous_release_id or not failed_release_id:
        return
    db_url = database_url(args)
    if not db_url:
        return
    with contextlib.suppress(Exception):
        with psycopg.connect(db_url) as conn:
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE cadastral_release
                    SET is_active = FALSE,
                        status = CASE WHEN status = 'ACTIVE' THEN 'READY' ELSE status END,
                        updated_at = NOW()
                    WHERE id = %s
                      AND data_type = 'land_info'
                    """,
                    (int(failed_release_id),),
                )
                conn.execute(
                    """
                    UPDATE cadastral_release
                    SET is_active = TRUE,
                        status = 'ACTIVE',
                        updated_at = NOW()
                    WHERE id = %s
                      AND data_type = 'land_info'
                    """,
                    (int(previous_release_id),),
                )


def cleanup_old_land_info_releases(args: argparse.Namespace, *, active_release_id: int) -> dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; cannot cleanup old land_info releases")
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    deleted_ids: list[int] = []
    skipped_partitions: list[int] = []
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM cadastral_release
                WHERE data_type = 'land_info'
                  AND id <> %s
                ORDER BY id
                """,
                (int(active_release_id),),
            )
            old_release_ids = [int(row[0]) for row in cur.fetchall()]
        for old_release_id in old_release_ids:
            try:
                with conn.transaction():
                    conn.execute("SELECT drop_dataset_record_partition(%s)", (old_release_id,))
            except Exception:
                skipped_partitions.append(old_release_id)
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM cadastral_release
                    WHERE data_type = 'land_info'
                      AND id <> %s
                    RETURNING id
                    """,
                    (int(active_release_id),),
                )
                deleted_ids = [int(row[0]) for row in cur.fetchall()]
    return {"deleted_release_ids": deleted_ids, "partition_cleanup_skipped_ids": skipped_partitions}


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    worker_dir = Path(args.worker_dir).resolve()
    dirs = ensure_dirs(base_dir, worker_dir)
    env = load_env_file(Path(args.env_file))
    token = os.getenv("ADMIN_TOKEN", env.get("ADMIN_TOKEN", ""))
    api_base = args.api_base.rstrip("/")
    run_id = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    manifest_name = f"land_info_full_{run_id}.json"
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "started_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "status": "running",
        "base_dir": str(base_dir),
        "worker_dir": str(worker_dir),
    }
    stream_release_id: int | None = None
    stream_job_id: int | None = None
    previous_active_id: int | None = None
    activated_release = False

    try:
        items, base_dates = discover_latest_items(args)
        snap = snapshot_key(base_dates)
        request_id = request_id_for_snapshot(snap)
        active = active_release_metadata(args)
        previous_active_id = int(active["id"]) if active and active.get("id") else None
        active_snapshot = ((active or {}).get("metadata") or {}).get("land_info_snapshot_key")
        manifest.update(
            {
                "snapshot_key": snap,
                "base_dates": base_dates,
                "request_id": request_id,
                "expected_count": len(items),
                "expected_items": [asdict(item) | {"download_url": item.download_url} for item in items],
                "active_release": active,
            }
        )
        print(
            f"[land-info-cycle] snapshot={snap} expected_files={len(items)} active_snapshot={active_snapshot or '-'}",
            flush=True,
        )
        if active_snapshot == snap and not args.force:
            manifest["status"] = "noop"
            manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            write_manifest(base_dir, manifest_name, manifest)
            return 0

        request = {
            "request_id": request_id,
            "created_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "status": "requested",
            "data_type": "land_info",
            "operation_mode": "full",
            "snapshot_key": snap,
            "base_dates": base_dates,
            "download_dir": str(dirs["worker_downloads"]),
            "expected_count": len(items),
            "items": [asdict(item) | {"download_url": item.download_url, "expected_glob": item.expected_glob} for item in items],
        }
        request_path = write_request(dirs["worker_requests"], request, force=args.force)
        manifest["request_path"] = str(request_path)

        if args.check_only:
            manifest["status"] = "check_only"
            manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            write_manifest(base_dir, manifest_name, manifest)
            return 0

        zip_paths, worker_state = collect_completed_zip_files(
            dirs["worker_downloads"],
            dirs["worker_manifests"],
            request_id,
            items,
            stable_seconds=args.stable_seconds,
        )
        manifest["worker_state"] = worker_state
        manifest["worker_zip_files"] = [str(path) for path in zip_paths]
        if worker_state.get("missing_counts"):
            manifest["status"] = "waiting_worker"
            manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            write_manifest(base_dir, manifest_name, manifest)
            print(f"[land-info-cycle] waiting for worker files: {worker_state.get('missing_counts')}", flush=True)
            return 0

        zip_csv_entries = inspect_zip_csv_entries(zip_paths)
        expected_csv_names = [
            entry["csv_name"]
            for zip_path in zip_paths
            for entry in zip_csv_entries.get(str(zip_path), [])
            if entry.get("csv_name")
        ]
        if len(expected_csv_names) < len(items):
            raise RuntimeError(
                f"csv count mismatch: csvs={len(expected_csv_names)} expected_zips={len(items)}"
            )

        stream = create_streaming_import_run(
            args,
            source_path=dirs["staging"] / f"full_{run_id}",
            request_id=request_id,
            snapshot=snap,
            base_dates=base_dates,
            expected_csv_count=len(expected_csv_names),
        )
        stream_release_id = int(stream["release_id"])
        stream_job_id = int(stream["job_id"])
        manifest["import"] = stream
        manifest["streaming"] = {
            "mode": "zip_by_zip",
            "expected_csv_count": len(expected_csv_names),
            "processed_count": 0,
            "processed": [],
            "cleanup": {"stage_dirs": [], "worker_zips": []},
        }
        write_manifest(base_dir, manifest_name, manifest)

        sample_pnu = ""
        stage_root = dirs["staging"] / f"full_{run_id}"
        processed_files: list[dict[str, Any]] = []
        cleanup: dict[str, list[str]] = {"stage_dirs": [], "worker_zips": [], "worker_zip_errors": []}
        for index, zip_path in enumerate(zip_paths, start=1):
            entries = zip_csv_entries.get(str(zip_path), [])
            stage_dir = stage_root / zip_path.stem
            print(
                f"[land-info-cycle] streaming {index}/{len(zip_paths)} zip={zip_path.name} csvs={len(entries)}",
                flush=True,
            )
            csv_paths = extract_zip_csv_entries(zip_path, entries, stage_dir)
            if not sample_pnu:
                sample_pnu = find_sample_pnu(stage_dir)
                manifest["sample_pnu"] = sample_pnu
            zip_processed: list[dict[str, Any]] = []
            for csv_path in csv_paths:
                run_import_for_csv(
                    args,
                    release_id=stream_release_id,
                    job_id=stream_job_id,
                    expected_csv_count=len(expected_csv_names),
                    stage_dir=stage_dir,
                    csv_path=csv_path,
                    timeout=args.import_timeout,
                )
                file_verification = verify_streamed_csv_file(
                    args,
                    release_id=stream_release_id,
                    csv_path=csv_path,
                )
                zip_processed.append(file_verification)
                processed_files.append(
                    {
                        "zip_name": zip_path.name,
                        **file_verification,
                    }
                )
                manifest["streaming"]["processed"] = processed_files[-20:]
                manifest["streaming"]["processed_count"] = len(processed_files)
                manifest["streaming"]["current_zip"] = zip_path.name
                write_manifest(base_dir, manifest_name, manifest)

            with contextlib.suppress(Exception):
                if stage_dir.exists():
                    shutil.rmtree(stage_dir)
                    cleanup["stage_dirs"].append(str(stage_dir))
            if args.cleanup_on_success and zip_processed and zip_path.exists():
                try:
                    zip_path.unlink()
                    cleanup["worker_zips"].append(str(zip_path))
                except Exception as cleanup_exc:
                    cleanup["worker_zip_errors"].append(f"{zip_path}: {cleanup_exc}")
            manifest["streaming"]["cleanup"] = cleanup
            manifest["streaming"]["processed_count"] = len(processed_files)
            write_manifest(base_dir, manifest_name, manifest)

        if len(processed_files) != len(expected_csv_names):
            raise RuntimeError(
                f"processed csv count mismatch: processed={len(processed_files)} expected={len(expected_csv_names)}"
            )

        activation = activate_streaming_release(
            args,
            release_id=stream_release_id,
            job_id=stream_job_id,
            expected_csv_names=expected_csv_names,
            snapshot=snap,
            request_id=request_id,
        )
        activated_release = True
        verification = verify_import(
            args,
            expected_csv_names=expected_csv_names,
            sample_pnu=sample_pnu,
            api_base=api_base,
            snapshot=snap,
            request_id=request_id,
        )
        old_release_cleanup: dict[str, Any] | None = None
        if args.cleanup_on_success:
            try:
                old_release_cleanup = cleanup_old_land_info_releases(args, active_release_id=stream_release_id)
            except Exception as cleanup_exc:
                old_release_cleanup = {"error": str(cleanup_exc)}
                print(f"[land-info-cycle] old release cleanup failed: {cleanup_exc}", file=sys.stderr, flush=True)
        with contextlib.suppress(Exception):
            if stage_root.exists() and not any(stage_root.iterdir()):
                stage_root.rmdir()
        manifest["import"] = stream
        manifest["activation"] = activation
        manifest["verification"] = verification
        manifest["old_release_cleanup"] = old_release_cleanup
        manifest["status"] = "imported"
        manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        write_manifest(base_dir, manifest_name, manifest)
        return 0
    except Exception as exc:
        if activated_release and previous_active_id and stream_release_id:
            restore_previous_active_release(
                args,
                previous_release_id=previous_active_id,
                failed_release_id=stream_release_id,
            )
            manifest["restored_previous_release_id"] = previous_active_id
        mark_streaming_job_failed(
            args,
            release_id=stream_release_id,
            job_id=stream_job_id,
            error=str(exc),
        )
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        write_manifest(base_dir, manifest_name, manifest)
        print(f"[land-info-cycle] failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
