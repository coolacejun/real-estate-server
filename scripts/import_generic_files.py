#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path
from typing import Any

import psycopg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generic importer (파일 메타 적재)")
    parser.add_argument("--data-type", default="generic")
    parser.add_argument("--release-id", type=int, required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--pattern", default="*")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--truncate-release", action="store_true")
    parser.add_argument("--operation-mode", default="full", choices=("full", "update"))
    parser.add_argument("--mark-ready", action="store_true", default=False)
    parser.add_argument("--activate-on-complete", action="store_true", default=False)
    return parser.parse_args()


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


def insert_file_batch(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dataset_import_file (
              release_id, data_type, file_name, file_size
            )
            VALUES (%s, %s, %s, %s)
            """,
            rows,
        )


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
    data_type = (args.data_type or "generic").strip().lower()
    processed_files = 0
    inserted_total = 0
    release_is_active = False
    can_write_dataset_file = True

    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT data_type, is_active FROM cadastral_release WHERE id = %s", (release_id,))
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(f"release not found: {release_id}")
                data_type = str(row[0] or data_type or "generic")
                release_is_active = bool(row[1])

            with conn.transaction():
                update_release_status(conn, release_id, "IMPORTING")
                if args.job_id is not None:
                    update_job(conn, args.job_id, status="RUNNING", total_files=len(files))

            if args.truncate_release:
                try:
                    with conn.transaction():
                        conn.execute("DELETE FROM dataset_import_file WHERE release_id = %s", (release_id,))
                except Exception:
                    can_write_dataset_file = False

            for path in files:
                processed_files += 1
                inserted_total += 1
                if can_write_dataset_file:
                    try:
                        with conn.transaction():
                            insert_file_batch(
                                conn,
                                [(release_id, data_type, path.name, path.stat().st_size)],
                            )
                    except Exception:
                        can_write_dataset_file = False

                with conn.transaction():
                    if args.job_id is not None:
                        update_job(
                            conn,
                            args.job_id,
                            processed_files=processed_files,
                            inserted_rows=inserted_total,
                        )
                print(f"[OK] {path.name} 처리 완료 (누적 files: {inserted_total})", flush=True)

            with conn.transaction():
                if args.activate_on_complete:
                    activate_release(conn, release_id, data_type, inserted_total)
                else:
                    final_status = "ACTIVE" if (args.mark_ready and release_is_active) else ("READY" if args.mark_ready else "IMPORTING")
                    update_release_status(conn, release_id, final_status, records_count=inserted_total)

                if args.job_id is not None:
                    update_job(
                        conn,
                        args.job_id,
                        status="SUCCEEDED",
                        inserted_rows=inserted_total,
                        error_message="",
                    )

    except Exception as exc:
        error_text = str(exc)
        with psycopg.connect(db_url) as conn:
            with conn.transaction():
                update_release_status(conn, release_id, "FAILED")
                if args.job_id is not None:
                    update_job(conn, args.job_id, status="FAILED", error_message=error_text)
        raise

    print(f"완료: release_id={release_id}, files={processed_files}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
