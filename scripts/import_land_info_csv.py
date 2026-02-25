#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import csv
import glob
import hashlib
import os
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any, Iterable

import psycopg
from dataset_record_store import DatasetSchemaRegistry
from dataset_pnu_kv_store import clear_array_dataset_code
from dataset_pnu_kv_store import delete_release_rows as delete_kv_release_rows
from dataset_pnu_kv_store import upsert_array_payloads as upsert_kv_array_payloads


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="토지정보 CSV 적재 (기본: dataset_record; 선택: dataset_pnu_kv)"
    )
    parser.add_argument("--data-type", default="land_info")
    parser.add_argument(
        "--store",
        default=os.getenv("DATASET_STORE_MODE", "auto"),
        choices=("auto", "pnu_kv", "dataset_record", "land_info_slim"),
    )
    parser.add_argument("--release-id", type=int, required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--pattern", default="AL_D1*.csv")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("LAND_INFO_IMPORT_WORKERS", "0") or "0"),
    )
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--truncate-release", action="store_true")
    parser.add_argument("--operation-mode", default="full", choices=("full", "update"))
    parser.add_argument(
        "--op-columns",
        default="작업구분,변경구분,변동구분,갱신구분,op,operation,crud,A8,a8",
    )
    parser.add_argument("--mark-ready", action="store_true", default=False)
    parser.add_argument("--activate-on-complete", action="store_true", default=False)
    return parser.parse_args()


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


def _detect_encoding(path: Path) -> str:
    # Avoid Path.read_bytes(): it loads the whole file into memory which can OOM on multi-GB CSVs.
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


def _iter_dict_rows(path: Path) -> Iterable[tuple[int, dict[str, str]]]:
    enc = _detect_encoding(path)
    with path.open("r", encoding=enc, errors="replace", newline="") as fp:
        reader = csv.DictReader(fp)
        for row_no, row in enumerate(reader, start=2):
            normalized: dict[str, str] = {}
            for key, value in (row or {}).items():
                k = (key or "").replace("\ufeff", "").strip()
                if not k:
                    continue
                normalized[k] = (value or "").strip()
            if normalized:
                yield row_no, normalized


def _dataset_code_from_name(path: Path) -> str:
    stem = path.stem.upper()
    token = stem.split("_")[0:2]
    if len(token) >= 2 and token[0] == "AL" and token[1].startswith("D"):
        return "_".join(token)
    return stem[:40]


def _extract_pnu(payload: dict[str, str]) -> str | None:
    candidates = ("고유번호", "PNU", "pnu")
    for key in candidates:
        value = payload.get(key)
        if value:
            return value
    for key, value in payload.items():
        norm = str(key or "").replace("\ufeff", "").strip().replace(" ", "").replace("_", "").lower()
        if not norm:
            continue
        # Some source files use column names like "PNU코드", "pnu_code", etc.
        if norm == "pnu" or norm.startswith("pnu") or "pnu" in norm or "고유번호" in norm:
            text = str(value or "").strip()
            if text:
                return text
    return None


def _normalize_op_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace(" ", "").replace("_", "").replace("-", "")
    return text


def _parse_operation(payload: dict[str, str], op_columns: list[str]) -> str:
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
    conn.execute(f"UPDATE cadastral_import_job SET {', '.join(updates)} WHERE id = %s", params)


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


def insert_batch(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> None:
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
            """
        ) as copy:
            for row in rows:
                copy.write_row(row)


def insert_land_info_slim_batch(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY public.land_info_record (
              release_id, dataset_code, pnu, schema_id, payload_values
            )
            FROM STDIN
            """
        ) as copy:
            for row in rows:
                copy.write_row(row)


def activate_release(conn: psycopg.Connection, release_id: int, data_type: str, records_count: int) -> None:
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
        (records_count, release_id, data_type),
    )


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


def _truthy_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "t", "yes", "y", "on"}


def _land_info_store_source_metadata() -> bool:
    # source_file/row_no are useful for debugging, but huge nationwide loads pay a large storage overhead.
    # Keep disabled by default for land_info to reduce disk usage.
    return _truthy_env("LAND_INFO_STORE_SOURCE_METADATA", default=False)


def _split_csv_file_round_robin(
    source: Path,
    split_root: Path,
    worker_count: int,
) -> tuple[list[Path], dict[str, Any]]:
    shards_per_file = max(
        2,
        min(
            worker_count,
            _parse_positive_int_env("LAND_INFO_SPLIT_SHARDS_PER_FILE", worker_count),
        ),
    )
    token = hashlib.sha1(str(source).encode("utf-8")).hexdigest()[:8]
    source_encoding = _detect_encoding(source)
    shard_encoding = "utf-8"

    split_paths: list[Path] = [
        split_root / f"{source.stem}__{token}__shard_{idx + 1:03d}.csv"
        for idx in range(shards_per_file)
    ]
    handles: list[Any] = []
    writers: list[csv.writer] = []
    counts: list[int] = [0 for _ in range(shards_per_file)]
    source_row_count = 0

    try:
        for split_path in split_paths:
            handle = split_path.open("w", encoding=shard_encoding, newline="")
            handles.append(handle)
            writers.append(csv.writer(handle))

        with source.open("r", encoding=source_encoding, errors="replace", newline="") as src_fp:
            reader = csv.reader(src_fp)
            header = None
            for first_row in reader:
                if first_row and any(str(cell).strip() for cell in first_row):
                    header = first_row
                    break
            if not header:
                return [source], {
                    "source_row_count": 0,
                    "shard_row_counts": [],
                    "shard_count": 0,
                }
            for writer in writers:
                writer.writerow(header)

            for idx, row in enumerate(reader):
                target_idx = idx % shards_per_file
                writers[target_idx].writerow(row)
                counts[target_idx] += 1
                source_row_count += 1
    finally:
        for handle in handles:
            with contextlib.suppress(Exception):
                handle.close()

    final_files = [split_paths[i] for i, c in enumerate(counts) if c > 0]
    shard_counts = [int(c) for c in counts if c > 0]
    if len(final_files) <= 1:
        return [source], {
            "source_row_count": int(source_row_count),
            "shard_row_counts": shard_counts,
            "shard_count": len(shard_counts),
        }
    return final_files, {
        "source_row_count": int(source_row_count),
        "shard_row_counts": shard_counts,
        "shard_count": len(shard_counts),
    }


def _prepare_parallel_input_files(
    files: list[Path],
    worker_count: int,
) -> tuple[list[Path], tempfile.TemporaryDirectory[str] | None, list[dict[str, Any]]]:
    if worker_count <= 1 or not files:
        return files, None, []

    temp_dir = tempfile.TemporaryDirectory(prefix="land_info_split_")
    split_root = Path(temp_dir.name)
    all_split_files: list[Path] = []
    split_meta_entries: list[dict[str, Any]] = []

    for source in files:
        split_files, split_meta = _split_csv_file_round_robin(source, split_root, worker_count)
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


def _ensure_dataset_pnu_kv_table(conn: psycopg.Connection) -> None:
    row = conn.execute("SELECT to_regclass('public.dataset_pnu_kv')").fetchone()
    if not row or not row[0]:
        raise RuntimeError(
            "dataset_pnu_kv 테이블이 없습니다. db/007_dataset_pnu_kv.sql 마이그레이션을 먼저 적용하세요."
        )


def _ensure_land_info_slim_table(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.land_info_record (
          id BIGSERIAL PRIMARY KEY,
          release_id BIGINT NOT NULL REFERENCES cadastral_release(id) ON DELETE CASCADE,
          dataset_code TEXT,
          pnu TEXT NOT NULL,
          schema_id BIGINT NOT NULL REFERENCES dataset_schema(id) ON DELETE RESTRICT,
          payload_values JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS land_info_record_release_pnu_idx
          ON public.land_info_record (release_id, pnu)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS land_info_record_release_dataset_pnu_idx
          ON public.land_info_record (release_id, dataset_code, pnu)
        """
    )


def _import_full_file_land_info_slim(
    db_url: str,
    release_id: int,
    data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    dataset_code = _dataset_code_from_name(path)
    source_file = path.name
    worker_name = threading.current_thread().name
    rows: list[tuple[Any, ...]] = []
    inserted_rows = 0
    reported_rows = 0
    read_rows = 0

    def flush_rows(conn: psycopg.Connection) -> None:
        nonlocal inserted_rows
        nonlocal reported_rows
        if not rows:
            return
        with conn.transaction():
            insert_land_info_slim_batch(conn, rows)
            inserted_now = len(rows)
            inserted_rows += inserted_now
            reported_rows += inserted_now
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        rows.clear()

    with psycopg.connect(db_url, autocommit=True) as conn:
        schema_registry = DatasetSchemaRegistry(conn, data_type)
        with conn.transaction():
            _ensure_land_info_slim_table(conn)
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            for _, payload in _iter_dict_rows(path):
                read_rows += 1
                pnu = _extract_pnu(payload)
                if not pnu:
                    continue
                schema_id, payload_values = schema_registry.encode_payload(dataset_code, payload)
                rows.append(
                    (
                        release_id,
                        dataset_code,
                        pnu,
                        schema_id,
                        payload_values,
                    )
                )
                if len(rows) >= batch_size:
                    flush_rows(conn)

            flush_rows(conn)
            if read_rows > 0 and inserted_rows == 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, read_rows={read_rows}, inserted_rows={inserted_rows}"
                )
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


def _import_update_file_land_info_slim(
    db_url: str,
    release_id: int,
    data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
    op_columns: list[str],
) -> dict[str, Any]:
    dataset_code = _dataset_code_from_name(path)
    source_file = path.name
    worker_name = threading.current_thread().name
    rows: list[tuple[Any, ...]] = []
    pending_delete_pnus: set[str] = set()
    cleared_pnus: set[str] = set()
    inserted_rows = 0
    reported_rows = 0
    read_rows = 0
    delete_ops = 0

    def flush_rows(conn: psycopg.Connection) -> None:
        nonlocal inserted_rows
        nonlocal reported_rows
        if not rows and not pending_delete_pnus:
            return
        with conn.transaction():
            delete_pnus = sorted(set(pending_delete_pnus))
            if delete_pnus:
                conn.execute(
                    """
                    DELETE FROM public.land_info_record
                    WHERE release_id = %s
                      AND dataset_code = %s
                      AND pnu = ANY(%s)
                    """,
                    (release_id, dataset_code, delete_pnus),
                )
                cleared_pnus.update(delete_pnus)

            upsert_all = {str(row[2]) for row in rows if row[2]}
            upsert_pnus = sorted(pnu for pnu in upsert_all if pnu not in cleared_pnus)
            if upsert_pnus:
                conn.execute(
                    """
                    DELETE FROM public.land_info_record
                    WHERE release_id = %s
                      AND dataset_code = %s
                      AND pnu = ANY(%s)
                    """,
                    (release_id, dataset_code, upsert_pnus),
                )
                cleared_pnus.update(upsert_pnus)

            if rows:
                insert_land_info_slim_batch(conn, rows)
                inserted_now = len(rows)
                inserted_rows += inserted_now
                reported_rows += inserted_now
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        rows.clear()
        pending_delete_pnus.clear()

    with psycopg.connect(db_url, autocommit=True) as conn:
        schema_registry = DatasetSchemaRegistry(conn, data_type)
        with conn.transaction():
            _ensure_land_info_slim_table(conn)
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            for _, payload in _iter_dict_rows(path):
                read_rows += 1
                pnu = _extract_pnu(payload)
                if not pnu:
                    continue

                op = _parse_operation(payload, op_columns)
                if op == "delete":
                    delete_ops += 1
                    pending_delete_pnus.add(pnu)
                    if rows:
                        rows[:] = [row for row in rows if str(row[2]) != pnu]
                    if len(pending_delete_pnus) >= batch_size:
                        flush_rows(conn)
                    continue

                schema_id, payload_values = schema_registry.encode_payload(dataset_code, payload)
                rows.append((release_id, dataset_code, pnu, schema_id, payload_values))
                if len(rows) >= batch_size:
                    flush_rows(conn)

            flush_rows(conn)
            if read_rows > 0 and inserted_rows == 0 and delete_ops <= 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, read_rows={read_rows}, inserted_rows={inserted_rows}"
                )
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


def _import_full_file(
    db_url: str,
    release_id: int,
    data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    dataset_code = _dataset_code_from_name(path)
    source_file = path.name
    store_source_metadata = _land_info_store_source_metadata()
    row_source_file = source_file if store_source_metadata else None
    worker_name = threading.current_thread().name
    rows: list[tuple[Any, ...]] = []
    inserted_rows = 0
    reported_rows = 0
    read_rows = 0

    def flush_rows(conn: psycopg.Connection) -> None:
        nonlocal inserted_rows
        nonlocal reported_rows
        if not rows:
            return
        with conn.transaction():
            insert_batch(conn, rows)
            inserted_now = len(rows)
            inserted_rows += inserted_now
            reported_rows += inserted_now
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        rows.clear()

    with psycopg.connect(db_url, autocommit=True) as conn:
        schema_registry = DatasetSchemaRegistry(conn, data_type)
        with conn.transaction():
            schema_registry.ensure_release_partition(release_id)
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            for row_no, payload in _iter_dict_rows(path):
                read_rows += 1
                pnu = _extract_pnu(payload)
                if not pnu:
                    continue
                schema_id, payload_values = schema_registry.encode_payload(dataset_code, payload)
                rows.append(
                    (
                        release_id,
                        data_type,
                        dataset_code,
                        row_source_file,
                        row_no if store_source_metadata else None,
                        pnu,
                        schema_id,
                        payload_values,
                        None,
                        None,
                    )
                )
                if len(rows) >= batch_size:
                    flush_rows(conn)

            flush_rows(conn)
            if read_rows > 0 and inserted_rows == 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, read_rows={read_rows}, inserted_rows={inserted_rows}"
                )
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


def _import_full_file_pnu_kv(
    db_url: str,
    release_id: int,
    data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    dataset_code = _dataset_code_from_name(path)
    source_file = path.name
    store_source_metadata = _land_info_store_source_metadata()
    row_source_file = source_file if store_source_metadata else None
    worker_name = threading.current_thread().name
    records_by_pnu: dict[str, list[dict[str, Any]]] = {}
    buffered_records = 0
    inserted_rows = 0
    reported_rows = 0
    read_rows = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_records
        nonlocal inserted_rows
        nonlocal reported_rows
        if not records_by_pnu:
            return
        with conn.transaction():
            upsert_kv_array_payloads(
                conn,
                release_id=release_id,
                data_type=data_type,
                records_by_pnu=records_by_pnu,
            )
            inserted_rows += buffered_records
            reported_rows += buffered_records
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        records_by_pnu.clear()
        buffered_records = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            for row_no, payload in _iter_dict_rows(path):
                read_rows += 1
                pnu = _extract_pnu(payload)
                if not pnu:
                    continue
                records_by_pnu.setdefault(pnu, []).append(
                    {
                        "dataset_code": dataset_code,
                        "payload": payload,
                        "geometry": None,
                        "source_file": row_source_file,
                        "row_no": row_no if store_source_metadata else None,
                    }
                )
                buffered_records += 1
                if buffered_records >= batch_size:
                    flush(conn)

            flush(conn)
            if read_rows > 0 and inserted_rows == 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, read_rows={read_rows}, inserted_rows={inserted_rows}"
                )
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


def _import_update_file_pnu_kv(
    db_url: str,
    release_id: int,
    data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
    op_columns: list[str],
) -> dict[str, Any]:
    dataset_code = _dataset_code_from_name(path)
    source_file = path.name
    store_source_metadata = _land_info_store_source_metadata()
    row_source_file = source_file if store_source_metadata else None
    worker_name = threading.current_thread().name
    records_by_pnu: dict[str, list[dict[str, Any]]] = {}
    pending_delete_pnus: set[str] = set()
    cleared_pnus: set[str] = set()
    buffered_records = 0
    inserted_rows = 0
    reported_rows = 0
    read_rows = 0
    delete_ops = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_records
        nonlocal inserted_rows
        nonlocal reported_rows
        if not records_by_pnu and not pending_delete_pnus:
            return

        delete_pnus = sorted({pnu for pnu in pending_delete_pnus if pnu})
        # Clear existing dataset_code entries first (delete operations + first-seen PNUs for upsert).
        with conn.transaction():
            if delete_pnus:
                clear_array_dataset_code(
                    conn,
                    release_id=release_id,
                    data_type=data_type,
                    dataset_code=dataset_code,
                    pnus=delete_pnus,
                )
                cleared_pnus.update(delete_pnus)

            upsert_all = {pnu for pnu in records_by_pnu.keys() if pnu}
            upsert_clear_pnus = sorted(pnu for pnu in upsert_all if pnu not in cleared_pnus)
            if upsert_clear_pnus:
                clear_array_dataset_code(
                    conn,
                    release_id=release_id,
                    data_type=data_type,
                    dataset_code=dataset_code,
                    pnus=upsert_clear_pnus,
                )
                cleared_pnus.update(upsert_clear_pnus)

            if records_by_pnu:
                upsert_kv_array_payloads(
                    conn,
                    release_id=release_id,
                    data_type=data_type,
                    records_by_pnu=records_by_pnu,
                )
                inserted_rows += buffered_records
                reported_rows += buffered_records
                if reported_rows >= progress_step:
                    increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                    increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                    reported_rows = 0

        records_by_pnu.clear()
        pending_delete_pnus.clear()
        buffered_records = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            for row_no, payload in _iter_dict_rows(path):
                read_rows += 1
                pnu = _extract_pnu(payload)
                if not pnu:
                    continue

                op = _parse_operation(payload, op_columns)
                if op == "delete":
                    delete_ops += 1
                    pending_delete_pnus.add(pnu)
                    removed = records_by_pnu.pop(pnu, None)
                    if removed:
                        buffered_records -= len(removed)
                    if len(pending_delete_pnus) >= batch_size:
                        flush(conn)
                    continue

                records_by_pnu.setdefault(pnu, []).append(
                    {
                        "dataset_code": dataset_code,
                        "payload": payload,
                        "geometry": None,
                        "source_file": row_source_file,
                        "row_no": row_no if store_source_metadata else None,
                    }
                )
                buffered_records += 1
                if buffered_records >= batch_size:
                    flush(conn)

            flush(conn)
            if read_rows > 0 and inserted_rows == 0 and delete_ops <= 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, read_rows={read_rows}, inserted_rows={inserted_rows}"
                )

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
        raise SystemExit("DATABASE_URL이 필요합니다.")

    source_dir = Path(args.source_dir)
    files = sorted(Path(p).resolve() for p in glob.glob(str(source_dir / args.pattern)))
    if not files:
        raise SystemExit(f"대상 파일이 없습니다: {source_dir}/{args.pattern}")

    release_id = args.release_id
    batch_size = max(100, args.batch_size)
    progress_step = max(5000, batch_size)
    operation_mode = (args.operation_mode or "full").strip().lower()
    is_update_mode = operation_mode == "update"
    op_columns = [col.strip() for col in str(args.op_columns or "").split(",") if col.strip()]
    data_type = (args.data_type or "land_info").strip().lower()
    store_mode_raw = str(args.store or "auto").strip().lower()
    # Default to land_info_slim to reduce disk usage while keeping indexed PNU query performance.
    store_mode = "land_info_slim" if store_mode_raw == "auto" else store_mode_raw
    inserted_total = 0
    processed_files = 0
    release_is_active = False

    requested_workers = int(args.workers or 0)
    worker_count = _resolve_worker_count(requested_workers)
    temp_split_dir: Any | None = None
    split_meta_entries: list[dict[str, Any]] = []
    # Splitting a large CSV into shards can temporarily duplicate the entire dataset on disk (in /tmp).
    # Default to "no split" for safety; enable explicitly if you have enough free space.
    split_enabled = _truthy_env("LAND_INFO_SPLIT_ENABLED", default=False)
    if split_enabled and (not is_update_mode) and len(files) == 1 and worker_count > 1:
        files, temp_split_dir, split_meta_entries = _prepare_parallel_input_files(files, worker_count)
    parallel_full_mode = (not is_update_mode) and worker_count > 1 and len(files) > 1
    # KV upsert path uses psycopg executemany in tight loops; we've seen hard-crash behavior in
    # multi-threaded mode on some macOS setups. Keep it single-threaded by default.
    kv_parallel_enabled = _truthy_env("LAND_INFO_PNU_KV_PARALLEL", default=False)
    parallel_full_mode_kv = parallel_full_mode and kv_parallel_enabled

    try:
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data_type, is_active FROM cadastral_release WHERE id = %s", (release_id,))
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(f"release not found: {release_id}")
                data_type = str(row[0] or data_type or "land_info").strip().lower()
                release_is_active = bool(row[1])

            if store_mode == "land_info_slim":
                _ensure_land_info_slim_table(conn)
                with conn.transaction():
                    update_release_status(conn, release_id, "IMPORTING")
                    if args.job_id is not None:
                        update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                        reset_job_worker_progress(conn, args.job_id, files)

                if args.truncate_release:
                    with conn.transaction():
                        conn.execute("DELETE FROM public.land_info_record WHERE release_id = %s", (release_id,))

                import_fn = _import_update_file_land_info_slim if is_update_mode else _import_full_file_land_info_slim
                if parallel_full_mode:
                    print(
                        f"[INFO] full 병렬 적재 시작(land_info_slim): workers={worker_count}, files={len(files)}, batch_size={batch_size}, executor=thread",
                        flush=True,
                    )
                    futures: dict[concurrent.futures.Future[dict[str, Any]], Path] = {}
                    executor: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
                    try:
                        for path in files:
                            future = executor.submit(
                                import_fn,
                                db_url,
                                release_id,
                                data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
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
                            print(
                                f"[OK] {result.get('file', path.name)} 처리 완료 (누적 rows: {inserted_total})",
                                flush=True,
                            )
                    finally:
                        executor.shutdown(wait=True, cancel_futures=True)
                else:
                    for path in files:
                        if is_update_mode:
                            result = import_fn(
                                db_url,
                                release_id,
                                data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
                                op_columns,
                            )
                        else:
                            result = import_fn(
                                db_url,
                                release_id,
                                data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
                            )
                        inserted_total += int(result.get("inserted_rows") or 0)
                        processed_files += 1
                        print(
                            f"[OK] {result.get('file', path.name)} 완료 (누적 rows: {inserted_total})",
                            flush=True,
                        )

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM public.land_info_record
                        WHERE release_id = %s
                        """,
                        (release_id,),
                    )
                    row = cur.fetchone()
                    final_records = int(row[0] or 0) if row else 0

                with conn.transaction():
                    if args.activate_on_complete:
                        activate_release(conn, release_id, data_type, final_records)
                    else:
                        final_status = (
                            "ACTIVE"
                            if (args.mark_ready and release_is_active)
                            else ("READY" if args.mark_ready else "IMPORTING")
                        )
                        update_release_status(conn, release_id, final_status, records_count=final_records)

                    if args.job_id is not None:
                        update_job(
                            conn,
                            args.job_id,
                            status="SUCCEEDED",
                            inserted_rows=inserted_total,
                            error_message="",
                        )

                if split_meta_entries:
                    source_row_count = sum(int(item.get("source_row_count") or 0) for item in split_meta_entries)
                    shard_row_total = sum(
                        sum(int(count) for count in (item.get("shard_row_counts") or []))
                        for item in split_meta_entries
                    )
                    split_status = "OK" if source_row_count == shard_row_total else "MISMATCH"
                    print(
                        f"[INFO] 분할 검증: sources={len(split_meta_entries)}, "
                        f"source_rows={source_row_count}, "
                        f"shard_rows_total={shard_row_total}, "
                        f"status={split_status}",
                        flush=True,
                    )

                print(
                    f"완료(land_info_slim): release_id={release_id}, files={processed_files}, rows={inserted_total}, keys={final_records}",
                    flush=True,
                )
                return 0

            if store_mode == "pnu_kv":
                _ensure_dataset_pnu_kv_table(conn)
                with conn.transaction():
                    update_release_status(conn, release_id, "IMPORTING")
                    if args.job_id is not None:
                        update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                        reset_job_worker_progress(conn, args.job_id, files)

                if args.truncate_release:
                    with conn.transaction():
                        delete_kv_release_rows(conn, release_id, data_type)

                import_fn = _import_update_file_pnu_kv if is_update_mode else _import_full_file_pnu_kv
                if parallel_full_mode_kv:
                    print(
                        f"[INFO] full 병렬 적재 시작(pnu_kv): workers={worker_count}, files={len(files)}, batch_size={batch_size}, executor=thread",
                        flush=True,
                    )
                    futures: dict[concurrent.futures.Future[dict[str, Any]], Path] = {}
                    executor: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor(
                        max_workers=worker_count
                    )
                    try:
                        for path in files:
                            future = executor.submit(
                                import_fn,
                                db_url,
                                release_id,
                                data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
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
                            print(
                                f"[OK] {result.get('file', path.name)} 처리 완료 (누적 rows: {inserted_total})",
                                flush=True,
                            )
                    finally:
                        executor.shutdown(wait=True, cancel_futures=True)
                else:
                    if parallel_full_mode and not kv_parallel_enabled:
                        print(
                            "[INFO] pnu_kv 병렬 적재는 기본 비활성화입니다. "
                            "필요시 LAND_INFO_PNU_KV_PARALLEL=1 로 켜세요.",
                            flush=True,
                        )
                    for path in files:
                        if is_update_mode:
                            result = import_fn(
                                db_url,
                                release_id,
                                data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
                                op_columns,
                            )
                        else:
                            result = import_fn(
                                db_url,
                                release_id,
                                data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
                            )
                        inserted_total += int(result.get("inserted_rows") or 0)
                        processed_files += 1
                        print(
                            f"[OK] {result.get('file', path.name)} 완료 (누적 rows: {inserted_total})",
                            flush=True,
                        )

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM dataset_pnu_kv
                        WHERE release_id = %s
                          AND data_type = %s
                        """,
                        (release_id, data_type),
                    )
                    row = cur.fetchone()
                    final_records = int(row[0] or 0) if row else 0

                with conn.transaction():
                    if args.activate_on_complete:
                        activate_release(conn, release_id, data_type, final_records)
                    else:
                        final_status = (
                            "ACTIVE"
                            if (args.mark_ready and release_is_active)
                            else ("READY" if args.mark_ready else "IMPORTING")
                        )
                        update_release_status(conn, release_id, final_status, records_count=final_records)

                    if args.job_id is not None:
                        update_job(
                            conn,
                            args.job_id,
                            status="SUCCEEDED",
                            inserted_rows=inserted_total,
                            error_message="",
                        )

                if split_meta_entries:
                    source_row_count = sum(int(item.get("source_row_count") or 0) for item in split_meta_entries)
                    shard_row_total = sum(
                        sum(int(count) for count in (item.get("shard_row_counts") or []))
                        for item in split_meta_entries
                    )
                    split_status = "OK" if source_row_count == shard_row_total else "MISMATCH"
                    print(
                        f"[INFO] 분할 검증: sources={len(split_meta_entries)}, "
                        f"source_rows={source_row_count}, "
                        f"shard_rows_total={shard_row_total}, "
                        f"status={split_status}",
                        flush=True,
                    )

                print(
                    f"완료(pnu_kv): release_id={release_id}, files={processed_files}, rows={inserted_total}, keys={final_records}",
                    flush=True,
                )
                return 0
            schema_registry = DatasetSchemaRegistry(conn, data_type)

            with conn.transaction():
                schema_registry.ensure_release_partition(release_id)
                update_release_status(conn, release_id, "IMPORTING")
                if args.job_id is not None:
                    update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                    reset_job_worker_progress(conn, args.job_id, files)

            if args.truncate_release:
                with conn.transaction():
                    conn.execute("DELETE FROM dataset_record WHERE release_id = %s", (release_id,))

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
                            args.job_id,
                            progress_step,
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
                    dataset_code = _dataset_code_from_name(path)
                    store_source_metadata = _land_info_store_source_metadata()
                    row_source_file = path.name if store_source_metadata else None
                    rows: list[tuple[Any, ...]] = []
                    pending_delete_pnus: set[str] = set()
                    cleared_pnus: set[str] = set()
                    reported_rows = 0
                    with conn.transaction():
                        mark_worker_running(conn, args.job_id, path.name, "single-runner")

                    def flush_rows() -> None:
                        nonlocal inserted_total
                        nonlocal reported_rows
                        if not rows and not pending_delete_pnus:
                            return
                        with conn.transaction():
                            if is_update_mode:
                                delete_pnus = sorted(set(pending_delete_pnus))
                                if delete_pnus:
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
                                    cleared_pnus.update(delete_pnus)

                                upsert_all = {str(row[5]) for row in rows if row[5]}
                                upsert_pnus = sorted(pnu for pnu in upsert_all if pnu not in cleared_pnus)
                                if upsert_pnus:
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
                                    cleared_pnus.update(upsert_pnus)
                            if rows:
                                insert_batch(conn, rows)
                                inserted_now = len(rows)
                                inserted_total += inserted_now
                                reported_rows += inserted_now
                            if reported_rows >= progress_step:
                                increment_job_progress(conn, args.job_id, inserted_rows_delta=reported_rows)
                                increment_worker_progress(conn, args.job_id, path.name, processed_rows_delta=reported_rows)
                                reported_rows = 0
                        rows.clear()
                        pending_delete_pnus.clear()

                    for row_no, payload in _iter_dict_rows(path):
                        pnu = _extract_pnu(payload)
                        if not pnu:
                            continue
                        if is_update_mode:
                            op = _parse_operation(payload, op_columns)
                            if op == "delete":
                                pending_delete_pnus.add(pnu)
                                if len(pending_delete_pnus) >= batch_size:
                                    flush_rows()
                                continue
                        schema_id, payload_values = schema_registry.encode_payload(dataset_code, payload)
                        rows.append(
                            (
                                release_id,
                                data_type,
                                dataset_code,
                                row_source_file,
                                row_no if store_source_metadata else None,
                                pnu,
                                schema_id,
                                payload_values,
                                None,
                                None,
                            )
                        )
                        if len(rows) >= batch_size:
                            flush_rows()

                    if rows or pending_delete_pnus:
                        flush_rows()

                    with conn.transaction():
                        if reported_rows > 0:
                            increment_job_progress(conn, args.job_id, inserted_rows_delta=reported_rows)
                            increment_worker_progress(conn, args.job_id, path.name, processed_rows_delta=reported_rows)
                        increment_job_progress(conn, args.job_id, processed_files_delta=1)
                        mark_worker_finished(conn, args.job_id, path.name, "SUCCEEDED")

                    processed_files += 1
                    print(f"[OK] {path.name} 완료 (누적 rows: {inserted_total})", flush=True)

            with conn.transaction():
                if args.activate_on_complete:
                    final_records = inserted_total
                    if is_update_mode:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT COUNT(*)
                                FROM dataset_record
                                WHERE release_id = %s
                                  AND data_type = %s
                                """,
                                (release_id, data_type),
                            )
                            count_row = cur.fetchone()
                            final_records = int(count_row[0] or 0) if count_row else 0
                    activate_release(conn, release_id, data_type, final_records)
                else:
                    final_status = "ACTIVE" if (args.mark_ready and release_is_active) else ("READY" if args.mark_ready else "IMPORTING")
                    final_records = inserted_total
                    if is_update_mode:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT COUNT(*)
                                FROM dataset_record
                                WHERE release_id = %s
                                  AND data_type = %s
                                """,
                                (release_id, data_type),
                            )
                            count_row = cur.fetchone()
                            final_records = int(count_row[0] or 0) if count_row else 0
                    update_release_status(conn, release_id, final_status, records_count=final_records)
                if args.job_id is not None:
                    update_job(
                        conn,
                        args.job_id,
                        status="SUCCEEDED",
                        inserted_rows=inserted_total,
                        error_message="",
                    )
            if split_meta_entries:
                source_row_count = sum(int(item.get("source_row_count") or 0) for item in split_meta_entries)
                shard_row_total = sum(
                    sum(int(count) for count in (item.get("shard_row_counts") or []))
                    for item in split_meta_entries
                )
                split_status = "OK" if source_row_count == shard_row_total else "MISMATCH"
                print(
                    f"[INFO] 분할 검증: sources={len(split_meta_entries)}, "
                    f"source_rows={source_row_count}, "
                    f"shard_rows_total={shard_row_total}, "
                    f"status={split_status}",
                    flush=True,
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

    print(f"완료: release_id={release_id}, files={processed_files}, rows={inserted_total}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
