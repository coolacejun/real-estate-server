#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

import psycopg


LAND_INFO_COMPONENTS: dict[str, dict[str, str]] = {
    "AL_D155": {"data_type": "land_info_al_d155", "name": "land_use_plan"},
    "AL_D157": {"data_type": "land_info_al_d157", "name": "land_movement"},
    "AL_D161": {"data_type": "land_info_al_d161", "name": "land_ownership"},
    "AL_D195": {"data_type": "land_info_al_d195", "name": "land_characteristic"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process direct-uploaded VWorld land_info ZIPs by component.")
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--direct-dir", default=os.getenv("LAND_INFO_DIRECT_WORKER_DIR", "/data/uploads/land_info_direct"))
    parser.add_argument("--import-script", default=os.getenv("LAND_INFO_IMPORT_SCRIPT_PATH", "/scripts/import_land_info_csv.py"))
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("LAND_INFO_DIRECT_IMPORT_BATCH_SIZE", "2000") or "2000"))
    parser.add_argument("--import-timeout", type=float, default=float(os.getenv("LAND_INFO_SYNC_IMPORT_TIMEOUT_SECONDS", "86400") or "86400"))
    parser.add_argument("--cleanup-on-success", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def safe_name(value: str, default: str = "file") -> str:
    text = str(value or "").strip()
    text = re.sub(r"[\\/]+", "_", text)
    text = re.sub(r"[^0-9A-Za-z_.-]+", "_", text)
    text = text.strip("._ ")
    return text[:180] or default


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def request_path(direct_dir: Path, request_id: str) -> Path:
    return direct_dir / "requests" / f"{safe_name(request_id, 'request')}.json"


def update_request(direct_dir: Path, request_id: str, **fields: Any) -> dict[str, Any]:
    path = request_path(direct_dir, request_id)
    data = read_json(path)
    data.update(fields)
    data["updated_at"] = now_iso()
    write_json(path, data)
    return data


def direct_signature(items: list[dict[str, Any]]) -> str:
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
        for item in sorted(items, key=lambda row: str(row.get("file_id") or ""))
    ]
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def component_snapshot(dataset_code: str, items: list[dict[str, Any]]) -> str:
    dates = sorted({str(item.get("base_date") or "").strip() for item in items if str(item.get("base_date") or "").strip()})
    return f"{dataset_code}={dates[-1]}" if dates else dataset_code


def group_items(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        code = str(item.get("dataset_code") or "").strip().upper()
        if code in LAND_INFO_COMPONENTS:
            grouped.setdefault(code, []).append(item)
    return {code: sorted(rows, key=lambda row: str(row.get("file_id") or "")) for code, rows in sorted(grouped.items())}


def expected_zip_names(items: list[dict[str, Any]]) -> dict[str, str]:
    names: dict[str, str] = {}
    for item in items:
        file_id = str(item.get("file_id") or "").strip()
        if not file_id:
            continue
        expected = str(item.get("expected_glob") or "").strip()
        if expected and "*" not in expected and "?" not in expected:
            names[file_id] = expected
        else:
            names[file_id] = f"{file_id}.zip"
    return names


def zip_path_for_item(accepted_dir: Path, item: dict[str, Any]) -> Path:
    file_id = str(item.get("file_id") or "").strip()
    candidates: list[Path] = []
    expected = str(item.get("expected_glob") or "").strip()
    if expected:
        candidates.extend(sorted(accepted_dir.glob(expected)))
    if file_id:
        candidates.extend(sorted(accepted_dir.glob(f"{file_id}.zip")))
    if not candidates and file_id:
        candidates.extend(sorted(accepted_dir.glob(f"{file_id}*.zip")))
    unique = []
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen or not path.is_file():
            continue
        seen.add(resolved)
        unique.append(path)
    if not unique:
        raise RuntimeError(f"accepted zip not found: {file_id or expected}")
    return unique[0]


def zip_member_basename(name: str) -> str:
    base = Path(str(name or "").replace("\\", "/")).name
    return safe_name(base, "data.csv")


def inspect_zip_csv_entries(zip_paths: list[Path]) -> dict[str, list[dict[str, str]]]:
    by_zip: dict[str, list[dict[str, str]]] = {}
    used_names: set[str] = set()
    for zip_path in zip_paths:
        if not zipfile.is_zipfile(zip_path):
            raise RuntimeError(f"invalid zip file: {zip_path}")
        entries: list[dict[str, str]] = []
        with zipfile.ZipFile(zip_path) as archive:
            bad_entry = archive.testzip()
            if bad_entry:
                raise RuntimeError(f"zip verification failed: {zip_path.name}:{bad_entry}")
            for info in archive.infolist():
                if info.is_dir() or not info.filename.lower().endswith(".csv"):
                    continue
                base_name = zip_member_basename(info.filename)
                stem = Path(base_name).stem
                suffix = Path(base_name).suffix or ".csv"
                target_name = f"{stem}_{zip_path.stem}{suffix}"
                counter = 2
                while target_name in used_names:
                    target_name = f"{stem}_{zip_path.stem}_{counter}{suffix}"
                    counter += 1
                used_names.add(target_name)
                entries.append({"member": info.filename, "target_name": target_name})
        if not entries:
            raise RuntimeError(f"no csv found in zip: {zip_path.name}")
        by_zip[str(zip_path)] = entries
    return by_zip


def extract_zip_entries(zip_path: Path, entries: list[dict[str, str]], stage_dir: Path) -> list[Path]:
    stage_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as archive:
        for entry in entries:
            target = stage_dir / safe_name(str(entry["target_name"]), "data.csv")
            with archive.open(str(entry["member"])) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
            extracted.append(target)
    return extracted


def create_import_run(
    db_url: str,
    *,
    request_id: str,
    dataset_code: str,
    data_type: str,
    source_path: Path,
    expected_csv_count: int,
    component_items: list[dict[str, Any]],
) -> dict[str, Any]:
    signature = direct_signature(component_items)
    snapshot = component_snapshot(dataset_code, component_items)
    version = f"{request_id}_{dataset_code}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}"
    metadata = {
        "trigger": "land_info_direct_worker",
        "data_type": data_type,
        "dataset_code": dataset_code,
        "operation_mode": "full",
        "land_info_source_signature": signature,
        "source_signature": signature,
        "land_info_snapshot_key": snapshot,
        "snapshot_key": snapshot,
        "land_info_base_date": snapshot.split("=", 1)[1] if "=" in snapshot else "",
        "request_id": request_id,
        "source_path": str(source_path),
        "pattern": f"{dataset_code}*.csv",
        "total_files": expected_csv_count,
    }
    with psycopg.connect(db_url) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM cadastral_release
                    WHERE data_type = %s
                      AND version = %s
                    LIMIT 1
                    """,
                    (data_type, version),
                )
                if cur.fetchone():
                    version = f"{version}-{dt.datetime.now().strftime('%f')}"
                cur.execute(
                    """
                    INSERT INTO cadastral_release (version, data_type, source_name, status, metadata)
                    VALUES (%s, %s, %s, 'PENDING', %s::jsonb)
                    RETURNING id
                    """,
                    (
                        version,
                        data_type,
                        f"VWorld land_info {dataset_code} {snapshot}",
                        json.dumps(metadata, ensure_ascii=False),
                    ),
                )
                release_row = cur.fetchone()
                if not release_row:
                    raise RuntimeError("failed to create component release")
                release_id = int(release_row[0])
                cur.execute(
                    """
                    INSERT INTO cadastral_import_job
                      (release_id, data_type, status, source_path, total_files)
                    VALUES (%s, %s, 'QUEUED', %s, %s)
                    RETURNING id
                    """,
                    (release_id, data_type, str(source_path), int(expected_csv_count)),
                )
                job_row = cur.fetchone()
                if not job_row:
                    raise RuntimeError("failed to create component import job")
                job_id = int(job_row[0])
    return {
        "release_id": release_id,
        "release_version": version,
        "job_id": job_id,
        "snapshot_key": snapshot,
        "source_signature": signature,
    }


def run_import_for_csv(
    args: argparse.Namespace,
    *,
    data_type: str,
    release_id: int,
    job_id: int,
    expected_csv_count: int,
    stage_dir: Path,
    csv_path: Path,
) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = args.db_url
    cmd = [
        sys.executable,
        str(args.import_script),
        "--data-type",
        data_type,
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
        "--batch-size",
        str(max(100, int(args.batch_size))),
        "--operation-mode",
        "full",
        "--keep-job-open",
        "--no-reset-job-workers",
    ]
    completed = subprocess.run(cmd, env=env, timeout=float(args.import_timeout), check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"component import failed: data_type={data_type} file={csv_path.name} exit={completed.returncode}")


def verify_csv_file(db_url: str, *, release_id: int, data_type: str, dataset_code: str, csv_name: str) -> dict[str, Any]:
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT file_size
                FROM dataset_import_file
                WHERE release_id = %s
                  AND data_type = %s
                  AND file_name = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(release_id), data_type, csv_name),
            )
            file_row = cur.fetchone()
            if not file_row:
                raise RuntimeError(f"missing dataset_import_file row: {csv_name}")
            cur.execute(
                """
                SELECT COUNT(*)
                FROM public.land_info_record
                WHERE release_id = %s
                  AND dataset_code = %s
                """,
                (int(release_id), dataset_code),
            )
            count_row = cur.fetchone()
            dataset_rows = int(count_row[0] or 0) if count_row else 0
            if dataset_rows <= 0:
                raise RuntimeError(f"no imported rows: dataset_code={dataset_code}")
    return {"csv_name": csv_name, "csv_size": int(file_row[0] or 0), "dataset_rows": dataset_rows}


def mark_failed(db_url: str, *, release_id: int | None, job_id: int | None, data_type: str, error: str) -> None:
    if not release_id and not job_id:
        return
    with contextlib.suppress(Exception):
        with psycopg.connect(db_url) as conn:
            with conn.transaction():
                if job_id:
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
                if release_id:
                    conn.execute(
                        """
                        UPDATE cadastral_release
                        SET status = CASE WHEN is_active THEN status ELSE 'FAILED' END,
                            updated_at = NOW()
                        WHERE id = %s
                          AND data_type = %s
                        """,
                        (int(release_id), data_type),
                    )


def activate_component_release(
    db_url: str,
    *,
    release_id: int,
    job_id: int,
    data_type: str,
    dataset_code: str,
    expected_csv_names: list[str],
    metadata_patch: dict[str, Any],
) -> dict[str, Any]:
    expected = sorted({name for name in expected_csv_names if name})
    with psycopg.connect(db_url) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT file_name
                    FROM dataset_import_file
                    WHERE release_id = %s
                      AND data_type = %s
                      AND file_name = ANY(%s)
                    """,
                    (int(release_id), data_type, expected),
                )
                recorded = sorted({str(row[0]) for row in cur.fetchall() if row and row[0]})
                missing = sorted(set(expected) - set(recorded))
                if missing:
                    preview = ", ".join(missing[:10])
                    raise RuntimeError(f"activation blocked: missing file records: {preview}")

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
                records_count = int(row[0] or 0) if row else 0
                if records_count <= 0:
                    raise RuntimeError(f"activation blocked: empty rows for {dataset_code}")

                cur.execute(
                    """
                    UPDATE cadastral_release
                    SET is_active = FALSE,
                        status = CASE WHEN status = 'ACTIVE' THEN 'READY' ELSE status END,
                        updated_at = NOW()
                    WHERE is_active = TRUE
                      AND data_type = %s
                      AND id <> %s
                    """,
                    (data_type, int(release_id)),
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
                      AND data_type = %s
                    RETURNING version
                    """,
                    (records_count, json.dumps(metadata_patch, ensure_ascii=False), int(release_id), data_type),
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
    }


def cleanup_monolithic_land_info_if_components_complete(db_url: str) -> dict[str, Any]:
    component_types = sorted(component["data_type"] for component in LAND_INFO_COMPONENTS.values())
    with psycopg.connect(db_url) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data_type
                    FROM cadastral_release
                    WHERE is_active = TRUE
                      AND data_type = ANY(%s)
                    """,
                    (component_types,),
                )
                active_types = {str(row[0]) for row in cur.fetchall() if row and row[0]}
                missing = sorted(set(component_types) - active_types)
                if missing:
                    return {"deleted_release_ids": [], "skipped": True, "reason": f"missing active components: {', '.join(missing)}"}

                cur.execute(
                    """
                    SELECT id
                    FROM cadastral_release
                    WHERE data_type = 'land_info'
                    ORDER BY id
                    """
                )
                release_ids = [int(row[0]) for row in cur.fetchall()]
                for release_id in release_ids:
                    with contextlib.suppress(Exception):
                        cur.execute("SELECT drop_dataset_record_partition(%s)", (release_id,))
                cur.execute(
                    """
                    DELETE FROM cadastral_release
                    WHERE data_type = 'land_info'
                    RETURNING id
                    """
                )
                deleted_ids = [int(row[0]) for row in cur.fetchall()]
    return {"deleted_release_ids": deleted_ids, "skipped": False, "reason": None}


def process_component(
    args: argparse.Namespace,
    *,
    direct_dir: Path,
    request_id: str,
    dataset_code: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    component = LAND_INFO_COMPONENTS[dataset_code]
    data_type = component["data_type"]
    accepted_dir = direct_dir / "uploads" / "accepted" / safe_name(request_id, "request")
    processing_dir = direct_dir / "processing" / safe_name(request_id, "request") / dataset_code
    processing_dir.mkdir(parents=True, exist_ok=True)

    zip_paths = [zip_path_for_item(accepted_dir, item) for item in items]
    zip_entries = inspect_zip_csv_entries(zip_paths)
    expected_csv_names = [
        entry["target_name"]
        for zip_path in zip_paths
        for entry in zip_entries.get(str(zip_path), [])
    ]
    run = create_import_run(
        args.db_url,
        request_id=request_id,
        dataset_code=dataset_code,
        data_type=data_type,
        source_path=processing_dir,
        expected_csv_count=len(expected_csv_names),
        component_items=items,
    )
    release_id = int(run["release_id"])
    job_id = int(run["job_id"])
    processed_files: list[dict[str, Any]] = []

    try:
        for index, zip_path in enumerate(zip_paths, start=1):
            stage_dir = processing_dir / zip_path.stem
            if stage_dir.exists():
                shutil.rmtree(stage_dir)
            entries = zip_entries.get(str(zip_path), [])
            csv_paths = extract_zip_entries(zip_path, entries, stage_dir)
            zip_processed: list[dict[str, Any]] = []
            for csv_path in csv_paths:
                run_import_for_csv(
                    args,
                    data_type=data_type,
                    release_id=release_id,
                    job_id=job_id,
                    expected_csv_count=len(expected_csv_names),
                    stage_dir=stage_dir,
                    csv_path=csv_path,
                )
                verification = verify_csv_file(
                    args.db_url,
                    release_id=release_id,
                    data_type=data_type,
                    dataset_code=dataset_code,
                    csv_name=csv_path.name,
                )
                zip_processed.append(verification)
            processed_files.extend(zip_processed)
            if args.cleanup_on_success:
                with contextlib.suppress(Exception):
                    shutil.rmtree(stage_dir)
                if zip_processed:
                    zip_path.unlink()
            print(
                f"[direct-land-info] {dataset_code} {index}/{len(zip_paths)} processed {zip_path.name}",
                flush=True,
            )
        metadata_patch = {
            "land_info_source_signature": run["source_signature"],
            "source_signature": run["source_signature"],
            "land_info_snapshot_key": run["snapshot_key"],
            "snapshot_key": run["snapshot_key"],
            "land_info_base_date": str(run["snapshot_key"]).split("=", 1)[1] if "=" in str(run["snapshot_key"]) else "",
            "land_info_request_id": request_id,
            "land_info_dataset_code": dataset_code,
            "land_info_dataset_name": component["name"],
            "direct_worker_import": True,
        }
        activated = activate_component_release(
            args.db_url,
            release_id=release_id,
            job_id=job_id,
            data_type=data_type,
            dataset_code=dataset_code,
            expected_csv_names=expected_csv_names,
            metadata_patch=metadata_patch,
        )
        if args.cleanup_on_success:
            with contextlib.suppress(Exception):
                shutil.rmtree(processing_dir)
        return {
            "dataset_code": dataset_code,
            "data_type": data_type,
            "release_id": release_id,
            "job_id": job_id,
            "zip_count": len(zip_paths),
            "csv_count": len(expected_csv_names),
            "processed_files": processed_files[-5:],
            "activation": activated,
        }
    except Exception as exc:
        mark_failed(args.db_url, release_id=release_id, job_id=job_id, data_type=data_type, error=str(exc))
        raise


def main() -> int:
    args = parse_args()
    if not args.db_url:
        raise SystemExit("DATABASE_URL is required")
    direct_dir = Path(args.direct_dir).resolve()
    request_id = str(args.request_id).strip()
    path = request_path(direct_dir, request_id)
    request = read_json(path)
    if not request:
        raise SystemExit(f"request not found: {path}")
    items = request.get("items")
    if not isinstance(items, list) or not items:
        raise SystemExit("request items are empty")

    grouped = group_items([item for item in items if isinstance(item, dict)])
    if not grouped:
        raise SystemExit("request has no supported land_info component items")

    update_request(
        direct_dir,
        request_id,
        status="server_processing",
        server_processing_started_at=now_iso(),
        server_processor_pid=os.getpid(),
    )
    result: dict[str, Any] = {
        "request_id": request_id,
        "started_at": now_iso(),
        "components": {},
    }
    try:
        for dataset_code, component_items in grouped.items():
            component_result = process_component(
                args,
                direct_dir=direct_dir,
                request_id=request_id,
                dataset_code=dataset_code,
                items=component_items,
            )
            result["components"][dataset_code] = component_result
            update_request(
                direct_dir,
                request_id,
                status="server_processing",
                server_processing_result=result,
            )
        cleanup_result: dict[str, Any] | None = None
        if args.cleanup_on_success:
            try:
                cleanup_result = cleanup_monolithic_land_info_if_components_complete(args.db_url)
            except Exception as cleanup_exc:
                cleanup_result = {"error": str(cleanup_exc)}
            result["monolithic_cleanup"] = cleanup_result
        result["finished_at"] = now_iso()
        update_request(
            direct_dir,
            request_id,
            status="server_processed",
            server_processed_at=result["finished_at"],
            server_processing_result=result,
        )
        accepted_dir = direct_dir / "uploads" / "accepted" / safe_name(request_id, "request")
        if args.cleanup_on_success and accepted_dir.exists() and not any(accepted_dir.iterdir()):
            with contextlib.suppress(Exception):
                accepted_dir.rmdir()
        return 0
    except Exception as exc:
        result["failed_at"] = now_iso()
        result["error"] = str(exc)
        update_request(
            direct_dir,
            request_id,
            status="server_failed",
            server_failed_at=result["failed_at"],
            server_error=str(exc)[:2000],
            server_processing_result=result,
        )
        raise


if __name__ == "__main__":
    started = time.time()
    try:
        raise SystemExit(main())
    finally:
        print(f"[direct-land-info] elapsed={time.time() - started:.1f}s", flush=True)
