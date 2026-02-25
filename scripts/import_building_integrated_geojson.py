#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import glob
import hashlib
import json
import os
import sys
import tempfile
import threading
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from dataset_record_store import DatasetSchemaRegistry
from dataset_pnu_kv_store import clear_array_dataset_code
from dataset_pnu_kv_store import delete_release_rows as delete_kv_release_rows
from dataset_pnu_kv_store import upsert_array_payloads as upsert_kv_array_payloads

try:
    import ijson
except ImportError as exc:  # pragma: no cover
    raise SystemExit("ijson이 필요합니다. api 컨테이너 재빌드 후 실행하세요.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GIS건물통합정보 GeoJSON 적재 (기본: dataset_record; 선택: dataset_pnu_kv)"
    )
    parser.add_argument("--data-type", default="building_integrated_info")
    parser.add_argument(
        "--store",
        default=os.getenv("DATASET_STORE_MODE", "auto"),
        choices=("auto", "pnu_kv", "dataset_record"),
    )
    parser.add_argument("--release-id", type=int, required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--pattern", default="AL_D010*.json")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("BUILDING_INTEGRATED_INFO_IMPORT_WORKERS", "0") or "0"),
    )
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--truncate-release", action="store_true")
    parser.add_argument("--operation-mode", default="full", choices=("full", "update"))
    parser.add_argument(
        "--op-columns",
        default="작업구분,변경구분,변동구분,갱신구분,op,operation,crud,A8,a8,A29,a29",
    )
    parser.add_argument("--mark-ready", action="store_true", default=False)
    parser.add_argument("--activate-on-complete", action="store_true", default=False)
    return parser.parse_args()


def iter_features(path: Path):
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
    token = stem.split("_")
    # AL_D010(정기본) + CH_D010(변경본)을 같은 코드(AL_D010)로 정규화해 update 시 동일 버킷에 반영.
    if len(token) >= 2 and token[1].startswith("D"):
        prefix = token[0]
        if prefix in {"AL", "CH"}:
            return f"AL_{token[1]}"
        return f"{prefix}_{token[1]}"
    return stem[:40]


def _normalize_op_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace(" ", "").replace("_", "").replace("-", "")
    return text


def _parse_operation(payload: dict[str, Any], op_columns: list[str]) -> str:
    add_tokens = {"add", "insert", "create", "new", "추가", "신규", "등록", "a", "i", "c"}
    update_tokens = {"update", "modify", "수정", "변경", "정정", "갱신", "u", "m"}
    delete_tokens = {"delete", "remove", "삭제", "말소", "d", "r"}
    # Update 소스별로 작업구분 위치가 달라 A8/a8, A29/a29를 fallback으로 함께 검사.
    seen: set[str] = set()
    for key in [*op_columns, "A8", "a8", "A29", "a29"]:
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


def _extract_pnu(payload: dict[str, Any]) -> str:
    for key in ("A2", "고유번호", "pnu"):
        if key not in payload:
            continue
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    return ""


def _extract_delete_lookup_pair(payload: dict[str, Any]) -> tuple[str, str] | None:
    # building_integrated_info update delete 행은 A2(PNU)가 비고 A0/A1 식별자만 오는 경우가 많음.
    a0 = str(payload.get("A0") or "").strip()
    a1 = str(payload.get("A1") or "").strip()
    if not a0 or not a1:
        return None
    return (a0, a1)


def _delete_dataset_record_by_lookup_pairs(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
    dataset_code: str,
    delete_lookup_pairs: set[tuple[str, str]],
) -> int:
    safe_pairs = sorted(
        {
            (str(a0).strip(), str(a1).strip())
            for (a0, a1) in delete_lookup_pairs
            if str(a0).strip() and str(a1).strip()
        }
    )
    if not safe_pairs:
        return 0

    a0_values = [pair[0] for pair in safe_pairs]
    a1_values = [pair[1] for pair in safe_pairs]
    result = conn.execute(
        """
        DELETE FROM dataset_record dr
        USING dataset_schema ds,
              unnest(%s::text[], %s::text[]) AS lk(a0, a1)
        WHERE dr.schema_id = ds.id
          AND dr.release_id = %s
          AND dr.data_type = %s
          AND dr.dataset_code = %s
          AND COALESCE(
            dr.payload ->> 'A0',
            CASE
              WHEN array_position(ds.columns, 'A0') IS NULL THEN NULL
              ELSE dr.payload_values ->> (array_position(ds.columns, 'A0') - 1)
            END
          ) = lk.a0
          AND COALESCE(
            dr.payload ->> 'A1',
            CASE
              WHEN array_position(ds.columns, 'A1') IS NULL THEN NULL
              ELSE dr.payload_values ->> (array_position(ds.columns, 'A1') - 1)
            END
          ) = lk.a1
        """,
        (
            a0_values,
            a1_values,
            int(release_id),
            str(data_type or "").strip().lower(),
            str(dataset_code),
        ),
    )
    return int(result.rowcount or 0)


def _resolve_delete_pnus_from_pnu_kv(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
    dataset_code: str,
    delete_lookup_pairs: set[tuple[str, str]],
) -> tuple[set[str], int]:
    safe_pairs = sorted(
        {
            (str(a0).strip(), str(a1).strip())
            for (a0, a1) in delete_lookup_pairs
            if str(a0).strip() and str(a1).strip()
        }
    )
    if not safe_pairs:
        return set(), 0
    a0_values = [pair[0] for pair in safe_pairs]
    a1_values = [pair[1] for pair in safe_pairs]

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH lookup_pairs AS (
              SELECT *
              FROM unnest(%s::text[], %s::text[]) AS lk(a0, a1)
            )
            SELECT DISTINCT
              kv.pnu,
              lp.a0,
              lp.a1
            FROM lookup_pairs lp
            CROSS JOIN dataset_pnu_kv kv
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(kv.payload) = 'array' THEN kv.payload
                ELSE '[]'::jsonb
              END
            ) AS elem
            WHERE kv.release_id = %s
              AND kv.data_type = %s
              AND COALESCE(elem ->> 'dataset_code', '') = %s
              AND COALESCE(elem -> 'payload' ->> 'A0', '') = lp.a0
              AND COALESCE(elem -> 'payload' ->> 'A1', '') = lp.a1
            """,
            (
                a0_values,
                a1_values,
                int(release_id),
                str(data_type or "").strip().lower(),
                str(dataset_code),
            ),
        )
        rows = cur.fetchall()

    pnus = {str(row[0]).strip() for row in rows if row and row[0] is not None and str(row[0]).strip()}
    resolved_pairs = {
        (str(row[1]).strip(), str(row[2]).strip())
        for row in rows
        if row
        and len(row) > 2
        and row[1] is not None
        and row[2] is not None
        and str(row[1]).strip()
        and str(row[2]).strip()
    }
    return pnus, len(resolved_pairs)


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
            COPY dataset_record (
              release_id, data_type, dataset_code, source_file, row_no, pnu,
              schema_id, payload_values, payload, geometry
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


def _split_geojson_file_round_robin(
    source: Path,
    split_root: Path,
    worker_count: int,
) -> tuple[list[Path], dict[str, Any]]:
    shards_per_file = max(
        2,
        min(
            worker_count,
            _parse_positive_int_env("GEOJSON_SPLIT_SHARDS_PER_FILE", worker_count),
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

    temp_dir = tempfile.TemporaryDirectory(prefix="building_integrated_split_")
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
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    dataset_code = _dataset_code_from_name(path)
    source_file = path.name
    worker_name = threading.current_thread().name
    rows: list[tuple[Any, ...]] = []
    inserted_rows = 0
    reported_rows = 0

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
            row_no = 0
            for feature in iter_features(path):
                row_no += 1
                geometry = feature.get("geometry")
                if geometry is not None and not isinstance(geometry, dict):
                    geometry = None
                props = feature.get("properties")
                props = props if isinstance(props, dict) else {}
                pnu_raw = props.get("A2") or props.get("고유번호") or props.get("pnu")
                pnu = str(pnu_raw).strip() if pnu_raw is not None else ""
                if not pnu:
                    continue

                schema_id, payload_values = schema_registry.encode_payload(
                    dataset_code,
                    props,
                    json_default=_json_default,
                )
                rows.append(
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
                        schema_registry.encode_geometry(geometry, json_default=_json_default),
                    )
                )

                if len(rows) >= batch_size:
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
    worker_name = threading.current_thread().name
    records_by_pnu: dict[str, list[dict[str, Any]]] = {}
    buffered_records = 0
    inserted_rows = 0
    reported_rows = 0

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
            row_no = 0
            for feature in iter_features(path):
                row_no += 1
                geometry = feature.get("geometry")
                if geometry is not None and not isinstance(geometry, dict):
                    geometry = None
                props = feature.get("properties")
                props = props if isinstance(props, dict) else {}
                pnu_raw = props.get("A2") or props.get("고유번호") or props.get("pnu")
                pnu = str(pnu_raw).strip() if pnu_raw is not None else ""
                if not pnu:
                    continue

                records_by_pnu.setdefault(pnu, []).append(
                    {
                        "dataset_code": dataset_code,
                        "payload": props,
                        "geometry": geometry,
                        "source_file": source_file,
                        "row_no": row_no,
                    }
                )
                buffered_records += 1
                if buffered_records >= batch_size:
                    flush(conn)

            flush(conn)
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
    worker_name = threading.current_thread().name
    records_by_pnu: dict[str, list[dict[str, Any]]] = {}
    pending_delete_pnus: set[str] = set()
    pending_delete_lookup_pairs: set[tuple[str, str]] = set()
    cleared_pnus: set[str] = set()
    buffered_records = 0
    inserted_rows = 0
    reported_rows = 0
    delete_ops = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_records
        nonlocal inserted_rows
        nonlocal reported_rows
        if not records_by_pnu and not pending_delete_pnus and not pending_delete_lookup_pairs:
            return

        delete_pnus_set = {pnu for pnu in pending_delete_pnus if pnu}
        if pending_delete_lookup_pairs:
            resolved_pnus, resolved_keys = _resolve_delete_pnus_from_pnu_kv(
                conn,
                release_id=release_id,
                data_type=data_type,
                dataset_code=dataset_code,
                delete_lookup_pairs=pending_delete_lookup_pairs,
            )
            delete_pnus_set.update(resolved_pnus)
            unresolved = len(pending_delete_lookup_pairs) - int(resolved_keys)
            if unresolved > 0:
                print(
                    f"[WARN] update delete lookup miss(pnu_kv): source={source_file}, dataset_code={dataset_code}, unresolved_pairs={unresolved}",
                    flush=True,
                )
        delete_pnus = sorted(delete_pnus_set)
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
        pending_delete_lookup_pairs.clear()
        buffered_records = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            row_no = 0
            for feature in iter_features(path):
                row_no += 1
                geometry = feature.get("geometry")
                if geometry is not None and not isinstance(geometry, dict):
                    geometry = None
                props = feature.get("properties")
                props = props if isinstance(props, dict) else {}
                op = _parse_operation(props, op_columns)
                pnu = _extract_pnu(props)
                if op == "delete":
                    delete_ops += 1
                    if pnu:
                        pending_delete_pnus.add(pnu)
                        removed = records_by_pnu.pop(pnu, None)
                        if removed:
                            buffered_records -= len(removed)
                    else:
                        lookup_pair = _extract_delete_lookup_pair(props)
                        if lookup_pair:
                            pending_delete_lookup_pairs.add(lookup_pair)
                    if (len(pending_delete_pnus) + len(pending_delete_lookup_pairs)) >= batch_size:
                        flush(conn)
                    continue

                if not pnu:
                    continue

                records_by_pnu.setdefault(pnu, []).append(
                    {
                        "dataset_code": dataset_code,
                        "payload": props,
                        "geometry": geometry,
                        "source_file": source_file,
                        "row_no": row_no,
                    }
                )
                buffered_records += 1
                if buffered_records >= batch_size:
                    flush(conn)

            flush(conn)
            if row_no > 0 and inserted_rows == 0 and delete_ops <= 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, read_rows={row_no}, inserted_rows={inserted_rows}"
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
    data_type = (args.data_type or "building_integrated_info").strip().lower()
    store_mode_raw = str(args.store or "auto").strip().lower()
    # Default to dataset_record to keep geometry/payload compressed and avoid TOAST bloat from KV upserts.
    store_mode = "dataset_record" if store_mode_raw == "auto" else store_mode_raw
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
                data_type = str(row[0] or data_type or "building_integrated_info").strip().lower()
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

            if store_mode == "pnu_kv":
                with conn.transaction():
                    update_release_status(conn, release_id, "IMPORTING")
                    if args.job_id is not None:
                        update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                        reset_job_worker_progress(conn, args.job_id, files)

                if args.truncate_release:
                    with conn.transaction():
                        delete_kv_release_rows(conn, release_id, data_type)
                        conn.execute(
                            """
                            DELETE FROM dataset_import_file
                            WHERE release_id = %s
                              AND data_type = %s
                            """,
                            (release_id, data_type),
                        )

                import_fn = _import_update_file_pnu_kv if is_update_mode else _import_full_file_pnu_kv
                if parallel_full_mode:
                    print(
                        f"[INFO] full 병렬 적재 시작(pnu_kv): workers={worker_count}, files={len(files)}, batch_size={batch_size}, executor=thread",
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
                        if is_update_mode:
                            file_size = 0
                            with contextlib.suppress(Exception):
                                file_size = int(path.stat().st_size)
                            with conn.transaction():
                                record_update_file_name(
                                    conn,
                                    release_id=release_id,
                                    data_type=data_type,
                                    file_name=path.name,
                                    file_size=file_size,
                                )
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
                    source_feature_count = sum(
                        int(item.get("source_feature_count") or 0) for item in split_meta_entries
                    )
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
                    rows: list[tuple[Any, ...]] = []
                    pending_delete_pnus: set[str] = set()
                    pending_delete_lookup_pairs: set[tuple[str, str]] = set()
                    cleared_pnus: set[str] = set()
                    row_no = 0
                    reported_rows = 0
                    with conn.transaction():
                        mark_worker_running(conn, args.job_id, path.name, "single-runner")

                    def flush_rows() -> None:
                        nonlocal inserted_total
                        nonlocal reported_rows
                        if not rows and not pending_delete_pnus and not pending_delete_lookup_pairs:
                            return
                        with conn.transaction():
                            if is_update_mode:
                                delete_pnus_set = set(pending_delete_pnus)
                                if pending_delete_lookup_pairs:
                                    deleted_rows = _delete_dataset_record_by_lookup_pairs(
                                        conn,
                                        release_id=release_id,
                                        data_type=data_type,
                                        dataset_code=dataset_code,
                                        delete_lookup_pairs=pending_delete_lookup_pairs,
                                    )
                                    if deleted_rows <= 0:
                                        print(
                                            f"[WARN] update delete lookup miss(dataset_record): source={path.name}, dataset_code={dataset_code}, unresolved_pairs={len(pending_delete_lookup_pairs)}",
                                            flush=True,
                                        )
                                delete_pnus = sorted(delete_pnus_set)
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
                        pending_delete_lookup_pairs.clear()

                    for feature in iter_features(path):
                        row_no += 1
                        geometry = feature.get("geometry")
                        if geometry is not None and not isinstance(geometry, dict):
                            geometry = None
                        props = feature.get("properties")
                        props = props if isinstance(props, dict) else {}
                        pnu = _extract_pnu(props)
                        if is_update_mode:
                            op = _parse_operation(props, op_columns)
                            if op == "delete":
                                if pnu:
                                    pending_delete_pnus.add(pnu)
                                else:
                                    lookup_pair = _extract_delete_lookup_pair(props)
                                    if lookup_pair:
                                        pending_delete_lookup_pairs.add(lookup_pair)
                                if (len(pending_delete_pnus) + len(pending_delete_lookup_pairs)) >= batch_size:
                                    flush_rows()
                                continue

                        if not pnu:
                            continue

                        schema_id, payload_values = schema_registry.encode_payload(
                            dataset_code,
                            props,
                            json_default=_json_default,
                        )
                        rows.append(
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
                                schema_registry.encode_geometry(geometry, json_default=_json_default),
                            )
                        )

                        if len(rows) >= batch_size:
                            flush_rows()

                    if rows or pending_delete_pnus or pending_delete_lookup_pairs:
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
