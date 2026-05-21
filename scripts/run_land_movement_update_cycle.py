#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as dt
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]


DATA_TYPE = "land_info_al_d157"
DATASET_CODE = "AL_D157"
VWORLD_DS_FILE_ID = "20171128DS00149"
DATASET_PAGE_ID = "13"
TERMINAL_JOB_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED"}
DEFAULT_USER_AGENT = "Mozilla/5.0"


@dataclass(frozen=True)
class LandMovementChangeItem:
    base_date: str
    updated_date: str
    file_no: str
    ds_file_id: str
    size_text: str
    size_kb: int

    @property
    def date_compact(self) -> str:
        return self.base_date.replace("-", "")

    @property
    def zip_name(self) -> str:
        return f"CH_D157_00_{self.date_compact}.zip"

    @property
    def csv_name(self) -> str:
        return f"CH_D157_00_{self.date_compact}.csv"

    @property
    def size_mb(self) -> float:
        return self.size_kb / 1024.0


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "t", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Download missing VWorld land movement CH CSV files and import AL_D157 updates."
    )
    parser.add_argument("--repo-root", default=str(repo_root))
    parser.add_argument(
        "--base-dir",
        default=os.getenv(
            "LAND_MOVEMENT_SYNC_BASE_DIR",
            str(repo_root / "data/source/land_info/auto/land_movement"),
        ),
    )
    parser.add_argument(
        "--api-base",
        default=os.getenv("BUILDING_LAND_API_BASE", os.getenv("LAND_MOVEMENT_SYNC_API_BASE", "http://localhost")),
    )
    parser.add_argument("--env-file", default=str(repo_root / ".env"))
    parser.add_argument("--page-count", type=int, default=int(os.getenv("LAND_MOVEMENT_SYNC_PAGE_COUNT", "20") or "20"))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("LAND_MOVEMENT_SYNC_PAGE_SIZE", "25") or "25"))
    parser.add_argument("--fetch-timeout", type=float, default=float(os.getenv("LAND_MOVEMENT_SYNC_FETCH_TIMEOUT", "120") or "120"))
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("LAND_MOVEMENT_SYNC_POLL_INTERVAL_SECONDS", "30") or "30"))
    parser.add_argument(
        "--import-timeout",
        type=float,
        default=float(os.getenv("LAND_MOVEMENT_SYNC_IMPORT_TIMEOUT_SECONDS", "14400") or "14400"),
    )
    parser.add_argument(
        "--max-direct-download-mb",
        type=float,
        default=float(os.getenv("LAND_MOVEMENT_SYNC_MAX_DIRECT_DOWNLOAD_MB", "500") or "500"),
    )
    parser.add_argument("--max-files", type=int, default=int(os.getenv("LAND_MOVEMENT_SYNC_MAX_FILES", "0") or "0"))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("LAND_MOVEMENT_SYNC_IMPORT_BATCH_SIZE", "2000") or "2000"))
    parser.add_argument("--manifest", default="cycle_manifest.json")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--cleanup-on-success",
        action=argparse.BooleanOptionalAction,
        default=_truthy_env("LAND_MOVEMENT_SYNC_CLEANUP_ON_SUCCESS", default=True),
    )
    parser.add_argument(
        "--start-date",
        default=os.getenv("LAND_MOVEMENT_SYNC_START_DATE", ""),
        help="Optional lower bound, YYYY-MM-DD. Defaults to active AL_D157 data date.",
    )
    return parser.parse_args()


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


def fetch_url(
    opener: urllib.request.OpenerDirector,
    url: str,
    *,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 120.0,
) -> tuple[bytes, dict[str, str]]:
    request_headers = {"User-Agent": DEFAULT_USER_AGENT}
    request_headers.update(headers or {})
    request = urllib.request.Request(url, data=data, headers=request_headers)
    with opener.open(request, timeout=timeout) as response:
        raw = response.read()
        return raw, {str(k): str(v) for k, v in response.headers.items()}


def strip_tags(value: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def size_to_kb(value: str, unit: str) -> int:
    number = float(str(value or "0").replace(",", ""))
    normalized = str(unit or "").strip().upper()
    if normalized.startswith("GB"):
        return int(number * 1024 * 1024)
    if normalized.startswith("MB"):
        return int(number * 1024)
    if normalized.startswith("KB"):
        return int(number)
    if normalized.startswith("BYTE") or normalized == "B":
        return max(0, int(number / 1024))
    return int(number)


def _match_first(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


def _span_date(text: str, label: str) -> str:
    match = re.search(rf"<span>\s*{re.escape(label)}<em class=\"xxs\">([^<]+)</em></span>", text)
    return match.group(1).strip() if match else ""


def parse_change_items(html: str) -> list[LandMovementChangeItem]:
    rows: list[LandMovementChangeItem] = []
    parts = re.split(r'<li><!--v-for="n in 10"-->|<li>', html)
    for part in parts:
        if "토지이동이력정보" not in part or "구분<em>변동데이터</em>" not in part:
            continue
        file_no = _match_first(part, r'name="dsFileSq" value="([^"]+)"')
        ds_file_id = _match_first(part, r'name="dsFileId" value="([^"]+)"')
        base_date = _span_date(part, "기준일")
        updated_date = _span_date(part, "갱신일")
        text = strip_tags(part)
        size_match = re.search(r"용량\s*([0-9,]+)\s*(BYTES|KB|MB|GB)", text, flags=re.I)
        size_value = size_match.group(1) if size_match else "0"
        size_unit = size_match.group(2) if size_match else "KB"
        if not file_no or not base_date:
            continue
        rows.append(
            LandMovementChangeItem(
                base_date=base_date,
                updated_date=updated_date,
                file_no=file_no,
                ds_file_id=ds_file_id or VWORLD_DS_FILE_ID,
                size_text=f"{size_value} {size_unit}",
                size_kb=size_to_kb(size_value, size_unit),
            )
        )
    return rows


def list_page_url(page_index: int, page_size: int) -> str:
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
        "dsId": DATASET_PAGE_ID,
        "dataSetSeq": DATASET_PAGE_ID,
        "listPageIndex": "1",
        "datPageIndex": str(page_index),
        "datPageSize": str(page_size),
        "pageSize": "10",
        "pageUnit": str(page_size),
        "fileGbnCd": "CH",
        "formatSelect": "CSV",
        "startDate": (dt.date.today() - dt.timedelta(days=370)).isoformat(),
        "endDate": dt.date.today().isoformat(),
    }
    return "https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?" + urllib.parse.urlencode(params)


def discover_change_items(
    opener: urllib.request.OpenerDirector,
    args: argparse.Namespace,
    lower_bound: str,
) -> list[LandMovementChangeItem]:
    found: list[LandMovementChangeItem] = []
    seen_file_no: set[str] = set()
    for page in range(1, max(1, args.page_count) + 1):
        raw, _headers = fetch_url(opener, list_page_url(page, args.page_size), timeout=args.fetch_timeout)
        rows = parse_change_items(raw.decode("utf-8", "replace"))
        for row in rows:
            if row.file_no in seen_file_no:
                continue
            seen_file_no.add(row.file_no)
            found.append(row)
        min_date = min((row.base_date for row in rows), default="")
        if min_date and min_date <= lower_bound:
            break
    return sorted(found, key=lambda item: (item.base_date, item.file_no))


def active_release_and_recorded(args: argparse.Namespace) -> tuple[dict[str, Any], set[str]]:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; run this inside the API/scheduler container")
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, version, source_name, records_count, metadata
                FROM cadastral_release
                WHERE data_type = %s
                  AND is_active IS TRUE
                ORDER BY id DESC
                LIMIT 1
                """,
                (DATA_TYPE,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"active release not found: {DATA_TYPE}")
            release = {
                "id": int(row[0]),
                "version": row[1],
                "source_name": row[2],
                "records_count": row[3],
                "metadata": row[4] if isinstance(row[4], dict) else {},
            }
            cur.execute(
                """
                SELECT DISTINCT file_name
                FROM dataset_import_file
                WHERE release_id = %s
                  AND data_type = %s
                """,
                (int(release["id"]), DATA_TYPE),
            )
            recorded = {str(item[0]).strip() for item in cur.fetchall() if item and item[0]}
    return release, recorded


def date_from_land_movement_file(name: str) -> str:
    matches = re.findall(r"(?:CH|AL)_D157(?:_\d{2})?_(20\d{6})", str(name or "").upper())
    if not matches:
        return ""
    compact = max(matches)
    return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"


def first_date_from_text(value: Any) -> str:
    text = str(value or "")
    match = re.search(r"(20\d{2})-(\d{2})-(\d{2})", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = re.search(r"(20\d{2})(\d{2})(\d{2})", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return ""


def release_lower_bound(release: dict[str, Any], recorded: set[str], explicit_start: str) -> str:
    if explicit_start:
        return explicit_start
    dates = [date_from_land_movement_file(name) for name in recorded]
    dates = [date for date in dates if date]
    if dates:
        return max(dates)
    metadata = release.get("metadata") if isinstance(release.get("metadata"), dict) else {}
    for key in ("land_info_base_date", "base_date", "snapshot_key", "land_info_snapshot_key"):
        parsed = first_date_from_text(metadata.get(key))
        if parsed:
            return parsed
    parsed = first_date_from_text(release.get("source_name"))
    if parsed:
        return parsed
    return "1970-01-01"


def login_vworld(opener: urllib.request.OpenerDirector, args: argparse.Namespace, env: dict[str, str]) -> None:
    user_id = os.getenv("VWORLD_USER_ID", env.get("VWORLD_USER_ID", "")).strip()
    password = os.getenv("VWORLD_USER_PASSWORD", env.get("VWORLD_USER_PASSWORD", "")).strip()
    if not user_id or not password:
        raise RuntimeError("VWORLD_USER_ID/VWORLD_USER_PASSWORD are required")

    fetch_url(opener, list_page_url(1, args.page_size), timeout=args.fetch_timeout)
    payload = urllib.parse.urlencode(
        {
            "usrIdeE": base64.b64encode(user_id.encode("utf-8")).decode("ascii"),
            "usrPwdE": base64.b64encode(password.encode("utf-8")).decode("ascii"),
            "nextUrl": "",
        }
    ).encode("utf-8")
    raw, _headers = fetch_url(
        opener,
        "https://www.vworld.kr/v4po_usrlogin_a004.do",
        data=payload,
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": list_page_url(1, args.page_size),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=args.fetch_timeout,
    )
    data = json.loads(raw.decode("utf-8", "replace"))
    result = (data.get("resultMap") or {}).get("result")
    message = (data.get("resultMap") or {}).get("msg") or ""
    if result != "success":
        raise RuntimeError(f"VWorld login failed: {message}")


def ensure_dirs(base_dir: Path) -> dict[str, Path]:
    dirs = {
        "raw": base_dir / "raw",
        "extracted": base_dir / "extracted",
        "staging": base_dir / "staging",
        "failed": base_dir / "failed",
        "manifests": base_dir / "manifests",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    (dirs["failed"] / "failed.txt").touch(exist_ok=True)
    return dirs


def download_zip(
    opener: urllib.request.OpenerDirector,
    item: LandMovementChangeItem,
    raw_dir: Path,
    timeout: float,
    force: bool,
) -> Path:
    target = raw_dir / item.zip_name
    if target.exists() and target.stat().st_size > 0 and not force:
        if zipfile.is_zipfile(target):
            return target
        print(f"[land-movement-cycle] remove invalid cached zip: {target}", flush=True)
        with contextlib.suppress(Exception):
            target.unlink()
    url = (
        "https://www.vworld.kr/dtmk/downloadResourceFile.do?"
        + urllib.parse.urlencode({"ds_id": item.ds_file_id or VWORLD_DS_FILE_ID, "fileNo": item.file_no})
    )
    raw, headers = fetch_url(opener, url, headers={"Referer": list_page_url(1, 25)}, timeout=timeout)
    if not raw:
        raise RuntimeError(f"empty download: {item.zip_name} fileNo={item.file_no}")
    if not raw.startswith(b"PK"):
        content_type = headers.get("Content-Type", "")
        raise RuntimeError(f"unexpected download payload: {item.zip_name} content_type={content_type} bytes={len(raw)}")
    tmp = target.with_suffix(".zip.tmp")
    tmp.write_bytes(raw)
    tmp.replace(target)
    return target


def extract_zip_to_csv(zip_path: Path, item: LandMovementChangeItem, extracted_root: Path, output_dir: Path, force: bool) -> Path:
    output = output_dir / item.csv_name
    if output.exists() and output.stat().st_size > 0 and not force:
        return output
    work_dir = extracted_root / zip_path.stem
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path) as archive:
            bad_entry = archive.testzip()
            if bad_entry:
                raise RuntimeError(f"zip verification failed: {bad_entry}")
            csv_members = [info for info in archive.infolist() if not info.is_dir() and info.filename.lower().endswith(".csv")]
            if not csv_members:
                raise RuntimeError("no csv found")
            source = csv_members[0]
            extracted = work_dir / Path(source.filename.replace("\\", "/")).name
            with archive.open(source) as src, extracted.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
    except Exception as exc:
        raise RuntimeError(f"unzip failed: {zip_path.name}: {exc}") from exc
    if output.exists():
        output.unlink()
    shutil.copy2(extracted, output)
    if not output.exists() or output.stat().st_size <= 0:
        raise RuntimeError(f"empty csv output: {output}")
    return output


def start_import(api_base: str, token: str, stage_dir: Path, items: list[LandMovementChangeItem], batch_size: int) -> dict[str, Any]:
    first_date = items[0].base_date
    last_date = items[-1].base_date
    body = {
        "data_type": DATA_TYPE,
        "operation_mode": "update",
        "source_path": str(stage_dir),
        "pattern": "CH_D157*.csv",
        "source_name": f"VWorld land movement CH {first_date}..{last_date}",
        "mark_ready": True,
        "batch_size": max(100, int(batch_size)),
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
    payload = http_json(api_base, f"/v1/admin/cadastral/import-jobs?data_type={DATA_TYPE}&limit=20", token=token, timeout=60)
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
                "[land-movement-cycle] import "
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


def verify_recorded_update_files(args: argparse.Namespace, expected_file_names: list[str]) -> dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; cannot verify imported update files")
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    expected = sorted({name for name in expected_file_names if name})
    if not expected:
        return {"expected_count": 0, "recorded_count": 0, "missing": []}
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, version, records_count
                FROM cadastral_release
                WHERE data_type = %s
                  AND is_active IS TRUE
                ORDER BY id DESC
                LIMIT 1
                """,
                (DATA_TYPE,),
            )
            release_row = cur.fetchone()
            if not release_row:
                raise RuntimeError(f"active release not found after import: {DATA_TYPE}")
            release_id = int(release_row[0])
            cur.execute(
                """
                SELECT DISTINCT file_name
                FROM dataset_import_file
                WHERE release_id = %s
                  AND data_type = %s
                  AND file_name = ANY(%s)
                """,
                (release_id, DATA_TYPE, expected),
            )
            recorded = sorted({str(row[0]) for row in cur.fetchall() if row and row[0]})
    missing = sorted(set(expected) - set(recorded))
    result = {
        "release_id": release_id,
        "release_version": release_row[1],
        "records_count": int(release_row[2] or 0),
        "expected_count": len(expected),
        "recorded_count": len(recorded),
        "missing": missing,
    }
    if missing:
        preview = ", ".join(missing[:10])
        suffix = "" if len(missing) <= 10 else f", ... +{len(missing) - 10}"
        raise RuntimeError(f"import verification failed: missing update file records: {preview}{suffix}")
    return result


def write_manifest(base_dir: Path, name: str, data: dict[str, Any]) -> None:
    manifests_dir = base_dir / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    (manifests_dir / name).write_text(payload, encoding="utf-8")
    (base_dir / "cycle_manifest.json").write_text(
        json.dumps({"latest": data}, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True


def acquire_cycle_lock(path: Path) -> int | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"{os.getpid()}\n{dt.datetime.now().astimezone().isoformat(timespec='seconds')}\n".encode("utf-8"))
        return fd
    except FileExistsError:
        pid = 0
        with contextlib.suppress(Exception):
            first_line = path.read_text(encoding="utf-8", errors="replace").splitlines()[0]
            pid = int(first_line.strip())
        if pid and not process_exists(pid):
            print(f"[land-movement-cycle] removing stale cycle lock pid={pid}: {path}", flush=True)
            with contextlib.suppress(Exception):
                path.unlink()
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"{os.getpid()}\n{dt.datetime.now().astimezone().isoformat(timespec='seconds')}\n".encode("utf-8"))
            return fd
        return None


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    dirs = ensure_dirs(base_dir)
    env = load_env_file(Path(args.env_file))
    token = os.getenv("ADMIN_TOKEN", env.get("ADMIN_TOKEN", ""))
    api_base = args.api_base.rstrip("/")
    run_id = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    manifest_name = f"land_movement_ch_{run_id}.json"
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "started_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "status": "running",
        "data_type": DATA_TYPE,
        "dataset_code": DATASET_CODE,
    }
    lock_path = base_dir / ".cycle.lock"
    lock_fd: int | None = None

    try:
        lock_fd = acquire_cycle_lock(lock_path)
        if lock_fd is None:
            manifest["status"] = "skipped_lock"
            manifest["error"] = f"cycle lock exists: {lock_path}"
            manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            write_manifest(base_dir, manifest_name, manifest)
            return 0

        release, recorded = active_release_and_recorded(args)
        lower_bound = release_lower_bound(release, recorded, args.start_date)
        jar = CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        all_items = discover_change_items(opener, args, lower_bound)
        missing = [
            item
            for item in all_items
            if item.base_date > lower_bound and item.csv_name not in recorded
        ]
        if args.max_files > 0:
            missing = missing[: args.max_files]
        too_large = [item for item in missing if item.size_mb > args.max_direct_download_mb]
        manifest.update(
            {
                "active_release": release,
                "lower_bound": lower_bound,
                "discovered_count": len(all_items),
                "missing_count": len(missing),
                "missing": [asdict(item) for item in missing],
                "too_large": [asdict(item) for item in too_large],
            }
        )
        print(
            f"[land-movement-cycle] active_release={release.get('id')} lower_bound={lower_bound} "
            f"missing={len(missing)} too_large={len(too_large)}",
            flush=True,
        )
        if too_large:
            raise RuntimeError(
                "direct download size limit exceeded: "
                + ", ".join(f"{item.csv_name} {item.size_text}" for item in too_large[:10])
            )
        if not missing:
            manifest["status"] = "noop"
            manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            write_manifest(base_dir, manifest_name, manifest)
            return 0
        if args.check_only:
            manifest["status"] = "check_only"
            manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            write_manifest(base_dir, manifest_name, manifest)
            return 0

        login_vworld(opener, args, env)
        stage_dir = dirs["staging"] / f"update_{run_id}"
        stage_dir.mkdir(parents=True, exist_ok=True)
        prepared: list[dict[str, Any]] = []
        failed: list[dict[str, str]] = []
        for item in missing:
            try:
                print(f"[land-movement-cycle] download {item.zip_name} fileNo={item.file_no} size={item.size_text}", flush=True)
                zip_path = download_zip(opener, item, dirs["raw"], args.fetch_timeout, args.force)
                print(f"[land-movement-cycle] extract {zip_path.name} -> {item.csv_name}", flush=True)
                csv_path = extract_zip_to_csv(zip_path, item, dirs["extracted"], stage_dir, args.force)
                prepared.append(
                    {
                        "item": asdict(item),
                        "zip_path": str(zip_path),
                        "zip_size": zip_path.stat().st_size,
                        "csv_path": str(csv_path),
                        "csv_size": csv_path.stat().st_size,
                    }
                )
            except Exception as exc:
                failed.append({"file": item.zip_name, "error": str(exc)})
                with (dirs["failed"] / "failed.txt").open("a", encoding="utf-8") as fp:
                    fp.write(f"{item.zip_name}\t{exc}\n")
                print(f"[land-movement-cycle] failed {item.zip_name}: {exc}", file=sys.stderr, flush=True)
                break
        manifest["prepared"] = prepared
        manifest["failed"] = failed
        if failed:
            raise RuntimeError(f"download/extract failed: {failed[0]}")

        import_data = start_import(api_base, token, stage_dir, missing, args.batch_size)
        job = wait_for_import(api_base, token, int(import_data["job_id"]), args.import_timeout, args.poll_interval)
        verification = verify_recorded_update_files(args, [item.csv_name for item in missing])
        manifest["import"] = import_data
        manifest["job"] = job
        manifest["verification"] = verification
        manifest["status"] = "imported"
        manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        write_manifest(base_dir, manifest_name, manifest)
        if args.cleanup_on_success:
            cleanup: dict[str, Any] = {"stage_dir": str(stage_dir), "extracted_dirs": [], "raw_zips": []}
            with contextlib.suppress(Exception):
                shutil.rmtree(stage_dir)
            with contextlib.suppress(Exception):
                for item in missing:
                    extracted_dir = dirs["extracted"] / item.zip_name.replace(".zip", "")
                    shutil.rmtree(extracted_dir)
                    cleanup["extracted_dirs"].append(str(extracted_dir))
            with contextlib.suppress(Exception):
                for item in missing:
                    raw_zip = dirs["raw"] / item.zip_name
                    if raw_zip.exists():
                        raw_zip.unlink()
                        cleanup["raw_zips"].append(str(raw_zip))
            manifest["source_cleanup"] = cleanup
            write_manifest(base_dir, manifest_name, manifest)
        return 0
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        manifest["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        write_manifest(base_dir, manifest_name, manifest)
        print(f"[land-movement-cycle] failed: {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        if lock_fd is not None:
            with contextlib.suppress(Exception):
                os.close(lock_fd)
            with contextlib.suppress(Exception):
                lock_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
