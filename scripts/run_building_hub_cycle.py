#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any

import fetch_building_hub as hub

try:
    import psycopg
except Exception:  # pragma: no cover - host fallback can still use docker compose psql.
    psycopg = None  # type: ignore[assignment]


TERMINAL_JOB_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Run the full building_info sync/import/verify/cleanup cycle."
    )
    parser.add_argument("--repo-root", default=str(repo_root))
    parser.add_argument(
        "--base-dir",
        default=os.getenv("BUILDING_HUB_SYNC_DIR", str(repo_root / "data/source/building_info_hub")),
    )
    parser.add_argument(
        "--visible-source-dir",
        default=str(repo_root / "data/source/building_info"),
        help="Host directory mounted into the API container as /data/source/building_info.",
    )
    parser.add_argument("--api-base", default=os.getenv("BUILDING_LAND_API_BASE", "http://localhost"))
    parser.add_argument("--env-file", default=str(repo_root / ".env"))
    parser.add_argument("--page-count", type=int, default=80)
    parser.add_argument("--fetch-timeout", type=float, default=300.0)
    parser.add_argument("--poll-interval", type=float, default=30.0)
    parser.add_argument("--import-timeout", type=float, default=4 * 60 * 60)
    parser.add_argument("--manifest", default="cycle_manifest.json")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--force-import", action="store_true")
    parser.add_argument("--skip-cleanup", action="store_true")
    parser.add_argument(
        "--keep-source-files",
        action="store_true",
        default=str(os.getenv("BUILDING_HUB_SYNC_KEEP_SOURCE_FILES", "")).strip().lower()
        in {"1", "true", "t", "yes", "y", "on"},
        help="Keep downloaded/extracted/staged source files after a verified successful import.",
    )
    parser.add_argument(
        "--vacuum-full",
        action="store_true",
        help="Run VACUUM FULL after old release deletion. This can lock large tables for a long time.",
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


def discover_latest(page_count: int, timeout: float) -> dict[str, Any]:
    jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    list_html, _csrf = hub.fetch_list_page(opener, page_count, timeout)
    items = hub.discover_latest_items(list_html)
    found_codes = {item.task_code for item in items}
    missing = [name for code, name in hub.TARGET_TASKS.items() if code not in found_codes]
    if missing:
        raise RuntimeError(f"missing target items: {', '.join(missing)}")
    latest_month = max(item.month for item in items)
    month_by_task = {item.task_code: item.month for item in items}
    same_month = all(month == latest_month for month in month_by_task.values())
    return {
        "latest_month": latest_month,
        "same_month": same_month,
        "month_by_task": month_by_task,
        "items": [asdict(item) for item in items],
    }


def month_version_fragment(month: str) -> str:
    return month.replace("-", "")


def release_matches_month(release: dict[str, Any] | None, month: str) -> bool:
    if not release:
        return False
    fragment = month_version_fragment(month)
    version = str(release.get("version") or "")
    source_name = str(release.get("source_name") or "")
    return f"hub-{fragment}" in version or month in source_name


def list_building_releases(api_base: str, token: str) -> list[dict[str, Any]]:
    payload = http_json(
        api_base,
        "/v1/admin/cadastral/releases?data_type=building_info&limit=50",
        token=token,
        timeout=60,
    )
    data = payload.get("data")
    return data if isinstance(data, list) else []


def active_release_from_list(releases: list[dict[str, Any]]) -> dict[str, Any] | None:
    for release in releases:
        if release.get("is_active") is True:
            return release
    return None


def run_fetch_script(args: argparse.Namespace) -> None:
    cmd = [
        sys.executable,
        str(Path(args.repo_root) / "scripts/fetch_building_hub.py"),
        "--base-dir",
        str(Path(args.base_dir)),
        "--page-count",
        str(args.page_count),
        "--download",
        "--extract",
        "--skip-probe",
        "--force",
        "--timeout",
        str(args.fetch_timeout),
    ]
    print("[cycle] fetching latest building hub files", flush=True)
    subprocess.run(cmd, cwd=args.repo_root, check=True)


def prepare_visible_staging(base_dir: Path, visible_source_dir: Path, month: str) -> tuple[Path, str]:
    source_dir = base_dir / "staging" / "full"
    if not source_dir.is_dir():
        raise RuntimeError(f"staging source directory is missing: {source_dir}")

    stage_name = f"hub_staging_full_{month.replace('-', '_')}"
    target_dir = visible_source_dir / stage_name
    target_dir.mkdir(parents=True, exist_ok=True)

    for task_code in hub.TARGET_TASKS:
        file_name = hub.expected_filename_for_task(task_code)
        source = source_dir / file_name
        target = target_dir / file_name
        if not source.exists():
            raise RuntimeError(f"staged file is missing: {source}")
        if target.exists() or target.is_symlink():
            target.unlink()
        try:
            os.link(source, target)
        except OSError:
            shutil.copy2(source, target)

    return target_dir, f"/data/source/building_info/{stage_name}"


def cleanup_source_files(base_dir: Path, visible_source_dir: Path, month: str, host_stage: Path | None = None) -> dict[str, Any]:
    stage_name = f"hub_staging_full_{month.replace('-', '_')}"
    targets = [
        ("raw_dir", base_dir / "raw" / month),
        ("extracted_dir", base_dir / "extracted" / month),
        ("staging_dir", base_dir / "staging" / "full"),
        ("visible_stage_dir", host_stage if host_stage is not None else visible_source_dir / stage_name),
    ]
    result: dict[str, Any] = {"removed": [], "missing": [], "failed": []}
    for label, target in targets:
        path = target.resolve()
        allowed_roots = [base_dir.resolve(), visible_source_dir.resolve()]
        if not any(path == root or root in path.parents for root in allowed_roots):
            result["failed"].append({"label": label, "path": str(path), "error": "outside allowed roots"})
            continue
        if not path.exists():
            result["missing"].append({"label": label, "path": str(path)})
            continue
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            result["removed"].append({"label": label, "path": str(path)})
        except Exception as exc:
            result["failed"].append({"label": label, "path": str(path), "error": str(exc)})
    return result


def start_import(
    api_base: str,
    token: str,
    container_source_path: str,
    month: str,
) -> dict[str, Any]:
    version = f"hub-{month_version_fragment(month)}-{time.strftime('%Y%m%d%H%M%S')}"
    body = {
        "data_type": "building_info",
        "operation_mode": "full",
        "source_path": container_source_path,
        "pattern": "*.txt",
        "version": version,
        "source_name": f"Building Hub register {month}",
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


def latest_job_for_release(api_base: str, token: str, release_id: int) -> dict[str, Any] | None:
    payload = http_json(
        api_base,
        f"/v1/admin/cadastral/import-jobs?release_id={release_id}&limit=5",
        token=token,
        timeout=60,
    )
    data = payload.get("data")
    if not isinstance(data, list):
        return None
    for item in data:
        if int(item.get("release_id") or 0) == release_id:
            return item
    return None


def is_transient_import_poll_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, urllib.error.URLError)):
        return True
    text = str(exc).lower()
    if "timed out" in text or "timeout" in text:
        return True
    return any(f"http {status}" in text for status in ("502", "503", "504"))


def wait_for_import(
    api_base: str,
    token: str,
    release_id: int,
    job_id: int,
    timeout: float,
    poll_interval: float,
) -> dict[str, Any]:
    started = time.time()
    last_status = ""
    poll_failures = 0
    last_poll_error: str | None = None
    while True:
        elapsed = time.time() - started
        if elapsed > timeout:
            suffix = f"; last poll error: {last_poll_error}" if last_poll_error else ""
            raise RuntimeError(f"import timed out: job_id={job_id}{suffix}")
        try:
            job = latest_job_for_release(api_base, token, release_id)
        except Exception as exc:
            if not is_transient_import_poll_error(exc):
                raise
            poll_failures += 1
            last_poll_error = f"{type(exc).__name__}: {exc}"
            print(
                "[cycle] import status poll failed "
                f"job={job_id} failures={poll_failures} error={last_poll_error}",
                file=sys.stderr,
                flush=True,
            )
            sleep_for = min(max(5.0, poll_interval), max(1.0, timeout - elapsed))
            time.sleep(sleep_for)
            continue
        poll_failures = 0
        last_poll_error = None
        if not job:
            raise RuntimeError(f"import job not found: job_id={job_id} release_id={release_id}")
        status = str(job.get("status") or "")
        if status != last_status or status == "RUNNING":
            print(
                "[cycle] import "
                f"job={job_id} status={status} files={job.get('processed_files')}/{job.get('total_files')} "
                f"rows={job.get('inserted_rows')}",
                flush=True,
            )
            last_status = status
        if status in TERMINAL_JOB_STATUSES:
            if int(job.get("id") or 0) != int(job_id):
                raise RuntimeError(f"unexpected terminal job: expected={job_id} actual={job.get('id')}")
            if status != "SUCCEEDED":
                raise RuntimeError(f"import failed: {json.dumps(job, ensure_ascii=False, default=str)}")
            return job
        time.sleep(max(5.0, poll_interval))


def db_fetch_first_column(args: argparse.Namespace, sql_text: str, params: tuple[Any, ...] = ()) -> list[Any]:
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; run this inside the API/scheduler container")
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, params)
            return [row[0] for row in cur.fetchall()]


def db_execute_returning_ids(args: argparse.Namespace, sql_text: str, params: tuple[Any, ...] = ()) -> list[int]:
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; run this inside the API/scheduler container")
    with psycopg.connect(db_url) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(sql_text, params)
                rows = cur.fetchall() if cur.description else []
    return [int(row[0]) for row in rows if row and row[0] is not None]


def db_execute(args: argparse.Namespace, sql_text: str, params: tuple[Any, ...] = ()) -> None:
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; run this inside the API/scheduler container")
    with psycopg.connect(db_url) as conn:
        with conn.transaction():
            conn.execute(sql_text, params)


def db_execute_autocommit(args: argparse.Namespace, sql_text: str) -> None:
    db_url = database_url(args)
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; run this inside the API/scheduler container")
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute(sql_text)


def select_sample_pnu(args: argparse.Namespace, release_id: int) -> str:
    sql_text = """
    WITH candidate AS (
      SELECT pnu
      FROM building_info_lookup
      WHERE release_id = %s
        AND COALESCE(pnu, '') <> ''
      LIMIT 1
    ), fallback AS (
      SELECT pnu
      FROM building_info_line
      WHERE release_id = %s
        AND COALESCE(pnu, '') <> ''
      LIMIT 1
    )
    SELECT pnu FROM candidate
    UNION ALL
    SELECT pnu FROM fallback
    LIMIT 1;
    """
    lines = db_fetch_first_column(args, sql_text, (int(release_id), int(release_id)))
    if not lines:
        raise RuntimeError(f"no sample pnu found for release_id={release_id}")
    return str(lines[0])


def decode_client_line(line: str, pnu: str) -> dict[str, Any]:
    raw = str(line or "")
    payload_text = ""
    for candidate in pnu_candidates(pnu):
        if candidate and raw.startswith(candidate):
            payload_text = raw[len(candidate) :]
            break
    if not payload_text:
        json_start = raw.find("{")
        if json_start >= 0:
            payload_text = raw[json_start:]
    if not payload_text:
        return {}
    with contextlib.suppress(Exception):
        parsed = json.loads(payload_text)
        if isinstance(parsed, dict):
            return parsed
    return {}


def pnu_candidates(pnu: str) -> list[str]:
    digits = "".join(ch for ch in str(pnu or "") if ch.isdigit())
    if len(digits) < 19:
        return [str(pnu or "").strip()]
    norm = digits[-19:]
    candidates = [norm]
    if norm[10] in {"0", "1"}:
        toggled = f"{norm[:10]}{'1' if norm[10] == '0' else '0'}{norm[11:]}"
        if toggled != norm:
            candidates.append(toggled)
    return candidates


def bucket_counts(payload: dict[str, Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for bucket in ("total", "single", "floor", "room"):
        value = payload.get(bucket)
        result[bucket] = len(value) if isinstance(value, list) else 0
    return result


def verify_client_fetch(
    args: argparse.Namespace,
    api_base: str,
    release_id: int,
    pnu: str | None = None,
) -> dict[str, Any]:
    query = ""
    if pnu:
        query = "?pnu=" + urllib.parse.quote(pnu)
    try:
        payload = http_json(api_base, f"/v1/data/building_info/verify{query}", timeout=120)
        data = payload.get("data")
        if isinstance(data, dict) and data.get("status") == "ok":
            checks = data.get("checks")
            if isinstance(checks, dict) and checks.get("client_fetch") is True:
                return {"mode": "verify_route", **data}
    except Exception as exc:
        print(f"[cycle] verify route unavailable, falling back to client endpoint: {exc}", flush=True)

    sample_pnu = pnu or select_sample_pnu(args, release_id)
    path = f"/v1/data/building_info/{urllib.parse.quote(sample_pnu)}?format=compressed"
    started = time.perf_counter()
    payload = http_json(api_base, path, timeout=120)
    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
    data = payload.get("data")
    parts = data.get("parts") if isinstance(data, dict) else None
    if not isinstance(parts, list) or not parts or not isinstance(parts[0], str):
        raise RuntimeError(f"client payload is empty for pnu={sample_pnu}")
    decoded = decode_client_line(parts[0], sample_pnu)
    counts = bucket_counts(decoded)
    if not any(counts.values()):
        raise RuntimeError(f"client payload could not be decoded for pnu={sample_pnu}")
    return {
        "mode": "client_endpoint",
        "status": "ok",
        "pnu": sample_pnu,
        "client_endpoint": path,
        "response": {
            "format": "compressed",
            "part_count": len(parts),
            "first_part_bytes": len(parts[0].encode("utf-8")),
            "elapsed_ms": elapsed_ms,
        },
        "bucket_counts": counts,
        "has_meta": isinstance(decoded.get("meta"), dict),
    }


def cleanup_old_releases(args: argparse.Namespace, active_release_id: int) -> dict[str, Any]:
    old_release_ids = db_fetch_first_column(
        args,
        """
        SELECT id
        FROM cadastral_release
        WHERE data_type = 'building_info'
          AND id <> %s
        ORDER BY id
        """,
        (int(active_release_id),),
    )
    for release_id in old_release_ids:
        with contextlib.suppress(Exception):
            db_execute(args, "SELECT drop_dataset_record_partition(%s)", (int(release_id),))

    delete_sql = """
    DELETE FROM cadastral_release
    WHERE data_type = 'building_info'
      AND id <> %s
    RETURNING id;
    """
    deleted_ids = db_execute_returning_ids(args, delete_sql, (int(active_release_id),))
    if args.vacuum_full:
        print("[cycle] running VACUUM FULL for building_info tables", flush=True)
        db_execute_autocommit(args, "VACUUM (FULL, ANALYZE) building_info_line;")
        db_execute_autocommit(args, "VACUUM (FULL, ANALYZE) building_info_lookup;")
    return {"deleted_release_ids": deleted_ids, "deleted_releases": len(deleted_ids)}


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"runs": []}
    with contextlib.suppress(Exception):
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    return {"runs": []}


def write_manifest(path: Path, run: dict[str, Any]) -> None:
    manifest = load_manifest(path)
    runs = manifest.get("runs")
    if not isinstance(runs, list):
        runs = []
    runs.append(run)
    manifest["latest"] = run
    manifest["runs"] = runs[-100:]
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


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


def acquire_lock(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        pid = 0
        with contextlib.suppress(Exception):
            first_line = path.read_text(encoding="utf-8", errors="replace").splitlines()[0]
            pid = int(first_line.strip())
        if pid and not process_exists(pid):
            print(f"[cycle] removing stale cycle lock pid={pid}: {path}", flush=True)
            with contextlib.suppress(Exception):
                path.unlink()
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        else:
            raise RuntimeError(f"cycle lock exists: {path}") from exc
    os.write(fd, f"{os.getpid()}\n{time.strftime('%Y-%m-%dT%H:%M:%S%z')}\n".encode("ascii"))
    return fd


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    base_dir = Path(args.base_dir).resolve()
    manifest_path = base_dir / args.manifest
    env = load_env_file(Path(args.env_file))
    token = os.getenv("ADMIN_TOKEN", env.get("ADMIN_TOKEN", ""))
    api_base = args.api_base.rstrip("/")
    lock_path = base_dir / ".cycle.lock"

    lock_fd = acquire_lock(lock_path)
    run: dict[str, Any] = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "status": "running",
        "api_base": api_base,
    }
    try:
        latest = discover_latest(args.page_count, args.fetch_timeout)
        run["latest"] = latest
        latest_month = str(latest["latest_month"])
        print(f"[cycle] latest_month={latest_month} same_month={latest['same_month']}", flush=True)
        if not latest["same_month"]:
            run["status"] = "skipped_partial_upload"
            print("[cycle] target files are not all on the same latest month yet", flush=True)
            return 0

        releases = list_building_releases(api_base, token)
        active = active_release_from_list(releases)
        run["active_before"] = active
        if args.check_only:
            run["status"] = "checked"
            return 0

        if active and release_matches_month(active, latest_month) and not args.force_import:
            print("[cycle] active release already matches latest month; verifying only", flush=True)
            verification = verify_client_fetch(args, api_base, int(active["id"]))
            run["verification"] = verification
            if not args.skip_cleanup:
                run["cleanup"] = cleanup_old_releases(args, int(active["id"]))
            if not args.keep_source_files:
                run["source_cleanup"] = cleanup_source_files(
                    base_dir,
                    Path(args.visible_source_dir).resolve(),
                    latest_month,
                )
            run["status"] = "skipped_already_current"
            return 0

        run_fetch_script(args)
        host_stage, container_source = prepare_visible_staging(
            base_dir,
            Path(args.visible_source_dir).resolve(),
            latest_month,
        )
        run["staging"] = {
            "host_path": str(host_stage),
            "container_source_path": container_source,
        }

        import_start = start_import(api_base, token, container_source, latest_month)
        run["import_start"] = import_start
        job = wait_for_import(
            api_base,
            token,
            int(import_start["release_id"]),
            int(import_start["job_id"]),
            args.import_timeout,
            args.poll_interval,
        )
        run["import_job"] = job

        releases_after = list_building_releases(api_base, token)
        active_after = active_release_from_list(releases_after)
        run["active_after"] = active_after
        if not active_after or int(active_after.get("id") or 0) != int(import_start["release_id"]):
            raise RuntimeError(
                f"new release is not active after import: expected={import_start['release_id']} actual={active_after}"
            )

        verification = verify_client_fetch(args, api_base, int(active_after["id"]))
        run["verification"] = verification
        if not args.skip_cleanup:
            run["cleanup"] = cleanup_old_releases(args, int(active_after["id"]))
        if not args.keep_source_files:
            run["source_cleanup"] = cleanup_source_files(
                base_dir,
                Path(args.visible_source_dir).resolve(),
                latest_month,
                host_stage,
            )

        run["status"] = "imported"
        return 0
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = str(exc)
        print(f"[cycle] failed: {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        run["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        with contextlib.suppress(Exception):
            write_manifest(manifest_path, run)
        with contextlib.suppress(Exception):
            os.close(lock_fd)
        with contextlib.suppress(Exception):
            lock_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
