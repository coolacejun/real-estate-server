#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import fnmatch
import glob
import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import unicodedata
from pathlib import Path
from typing import Any

import psycopg
from dataset_record_store import DatasetSchemaRegistry
from dataset_pnu_kv_store import clear_building_info_bucket
from dataset_pnu_kv_store import delete_release_rows as delete_kv_release_rows
from dataset_pnu_kv_store import upsert_building_info_payloads as upsert_building_info_kv_payloads


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "건축물정보 텍스트 적재. "
            "권장: building_info_line(라인 테이블) + building_info_lookup, "
            "대안: dataset_pnu_kv(pnu_kv) 또는 dataset_record."
        )
    )
    parser.add_argument("--data-type", default="building_info")
    parser.add_argument(
        "--store",
        default=os.getenv("DATASET_STORE_MODE", "auto"),
        choices=("auto", "line_table", "pnu_kv", "dataset_record"),
    )
    parser.add_argument("--release-id", type=int, required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--pattern", default="*.txt")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=int(os.getenv("BUILDING_INFO_IMPORT_WORKERS", "0") or "0"))
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--truncate-release", action="store_true")
    parser.add_argument("--merge-by-pnu", action="store_true")
    parser.add_argument("--operation-mode", default="full", choices=("full", "update"))
    parser.add_argument(
        "--op-columns",
        default="작업구분,변경구분,변동구분,갱신구분,op,operation,crud,A8,a8",
    )
    parser.add_argument("--mark-ready", action="store_true", default=False)
    parser.add_argument("--activate-on-complete", action="store_true", default=False)
    return parser.parse_args()


def detect_category(path: Path) -> str:
    name = unicodedata.normalize("NFC", path.name)
    if "총괄표제부" in name:
        return "total"
    if "전유공용면적" in name:
        return "room"
    if "층별개요" in name:
        return "floor"
    if "표제부" in name:
        return "single"
    return "single"


def decode_line(raw: bytes) -> str:
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "ignore")


def _digits(value: str) -> str:
    return re.sub(r"[^0-9]", "", value or "")


def _pnu_part(value: str, size: int) -> str:
    digits = _digits(value)
    if not digits:
        return "0" * size
    return digits[-size:].zfill(size)


_CATEGORY_PNU_INDEXES: dict[str, tuple[int, int, int, int, int]] = {
    "total": (10, 11, 12, 13, 14),
    "single": (8, 9, 10, 11, 12),
    "floor": (4, 5, 6, 7, 8),
    "room": (8, 9, 10, 11, 12),
}

_CATEGORY_COLUMNS: dict[str, list[str]] = {
    "total": [
        "관리_건축물대장_PK", "대장_구분_코드", "대장_구분_코드_명", "대장_종류_코드", "대장_종류_코드_명",
        "신_구_대장_구분_코드", "신_구_대장_구분_코드_명", "대지_위치", "도로명_대지_위치", "건물_명",
        "시군구_코드", "법정동_코드", "대지_구분_코드", "번", "지", "특수지_명", "블록", "로트", "외필지_수",
        "새주소_도로_코드", "새주소_법정동_코드", "새주소_지상지하_코드", "새주소_본_번", "새주소_부_번",
        "대지_면적(㎡)", "건축_면적(㎡)", "건폐_율(%)", "연면적(㎡)", "용적_률_산정_연면적(㎡)", "용적_률(%)",
        "주_용도_코드", "주_용도_코드_명", "기타_용도", "세대_수(세대)", "가구_수(가구)", "주_건축물_수",
        "부속_건축물_수", "부속_건축물_면적(㎡)", "총_주차_수", "옥내_기계식_대수(대)", "옥내_기계식_면적(㎡)",
        "옥외_기계식_대수(대)", "옥외_기계식_면적(㎡)", "옥내_자주식_대수(대)", "옥내_자주식_면적(㎡)",
        "옥외_자주식_대수(대)", "옥외_자주식_면적(㎡)", "허가_일", "착공_일", "사용승인_일", "허가번호_년",
        "허가번호_기관_코드", "허가번호_기관_코드_명", "허가번호_구분_코드", "허가번호_구분_코드_명", "호_수(호)",
        "에너지효율_등급", "에너지절감_율", "에너지_EPI점수", "친환경_건축물_등급", "친환경_건축물_인증점수",
        "지능형_건축물_등급", "지능형_건축물_인증점수", "생성_일자",
    ],
    "single": [
        "관리_건축물대장_PK", "대장_구분_코드", "대장_구분_코드_명", "대장_종류_코드", "대장_종류_코드_명",
        "대지_위치", "도로명_대지_위치", "건물_명", "시군구_코드", "법정동_코드", "대지_구분_코드", "번", "지",
        "특수지_명", "블록", "로트", "외필지_수", "새주소_도로_코드", "새주소_법정동_코드", "새주소_지상지하_코드",
        "새주소_본_번", "새주소_부_번", "동_명", "주_부속_구분_코드", "주_부속_구분_코드_명", "대지_면적(㎡)",
        "건축_면적(㎡)", "건폐_율(%)", "연면적(㎡)", "용적_률_산정_연면적(㎡)", "용적_률(%)", "구조_코드",
        "구조_코드_명", "기타_구조", "주_용도_코드", "주_용도_코드_명", "기타_용도", "지붕_코드", "지붕_코드_명",
        "기타_지붕", "세대_수(세대)", "가구_수(가구)", "높이(m)", "지상_층_수", "지하_층_수", "승용_승강기_수",
        "비상용_승강기_수", "부속_건축물_수", "부속_건축물_면적(㎡)", "총_동_연면적(㎡)", "옥내_기계식_대수(대)",
        "옥내_기계식_면적(㎡)", "옥외_기계식_대수(대)", "옥외_기계식_면적(㎡)", "옥내_자주식_대수(대)",
        "옥내_자주식_면적(㎡)", "옥외_자주식_대수(대)", "옥외_자주식_면적(㎡)", "허가_일", "착공_일", "사용승인_일",
        "허가번호_년", "허가번호_기관_코드", "허가번호_기관_코드_명", "허가번호_구분_코드", "허가번호_구분_코드_명",
        "호_수(호)", "에너지효율_등급", "에너지절감_율", "에너지_EPI점수", "친환경_건축물_등급",
        "친환경_건축물_인증점수", "지능형_건축물_등급", "지능형_건축물_인증점수", "생성_일자",
        "내진_설계_적용_여부", "내진_능력",
    ],
    "floor": [
        "관리_건축물대장_PK", "대지_위치", "도로명_대지_위치", "건물_명", "시군구_코드", "법정동_코드",
        "대지_구분_코드", "번", "지", "특수지_명", "블록", "로트", "새주소_도로_코드", "새주소_법정동_코드",
        "새주소_지상지하_코드", "새주소_본_번", "새주소_부_번", "동_명", "층_구분_코드", "층_구분_코드_명",
        "층_번호", "층_번호_명", "구조_코드", "구조_코드_명", "기타_구조", "주_용도_코드", "주_용도_코드_명",
        "기타_용도", "면적(㎡)", "주_부속_구분_코드", "주_부속_구분_코드_명", "면적_제외_여부", "생성_일자",
    ],
    "room": [
        "관리_건축물대장_PK", "대장_구분_코드", "대장_구분_코드_명", "대장_종류_코드", "대장_종류_코드_명",
        "대지_위치", "도로명_대지_위치", "건물_명", "시군구_코드", "법정동_코드", "대지_구분_코드", "번", "지",
        "특수지_명", "블록", "로트", "새주소_도로_코드", "새주소_법정동_코드", "새주소_지상지하_코드",
        "새주소_본_번", "새주소_부_번", "동_명", "호_명", "층_구분_코드", "층_구분_코드_명", "층_번호",
        "전유_공용_구분_코드", "전유_공용_구분_코드_명", "주_부속_구분_코드", "주_부속_구분_코드_명", "층_번호_명",
        "구조_코드", "구조_코드_명", "기타_구조", "주_용도_코드", "주_용도_코드_명", "기타_용도", "면적(㎡)",
        "생성_일자",
    ],
}


def _build_pnu_by_index(parts: list[str], indexes: tuple[int, int, int, int, int]) -> str | None:
    if max(indexes) >= len(parts):
        return None
    sigungu = _pnu_part(parts[indexes[0]], 5)
    bjd = _pnu_part(parts[indexes[1]], 5)
    land_type = _pnu_part(parts[indexes[2]], 1)
    bun = _pnu_part(parts[indexes[3]], 4)
    ji = _pnu_part(parts[indexes[4]], 4)
    if sigungu == "00000" or bjd == "00000":
        return None
    return f"{sigungu}{bjd}{land_type}{bun}{ji}"


def _build_pnu_fallback(parts: list[str]) -> str | None:
    for idx in range(0, max(0, len(parts) - 4)):
        candidate = _build_pnu_by_index(parts, (idx, idx + 1, idx + 2, idx + 3, idx + 4))
        if candidate and candidate[:2] in {"11", "26", "27", "28", "29", "30", "31", "36", "41", "42", "43", "44", "45", "46", "47", "48", "50"}:
            return candidate
    return None


def build_pnu(parts: list[str], category: str) -> str | None:
    indexes = _CATEGORY_PNU_INDEXES.get(category)
    if indexes:
        pnu = _build_pnu_by_index(parts, indexes)
        if pnu:
            return pnu
    return _build_pnu_fallback(parts)


def _dataset_code_from_category(category: str) -> str:
    mapping = {
        "total": "building_info_total",
        "single": "building_info_single",
        "floor": "building_info_floor",
        "room": "building_info_room",
    }
    return mapping.get(category, "building_info")


# PNU KV stores trimmed bucket lines to minimize DB size and match the API client parsers.
_KV_BUCKET_COLUMNS: dict[str, list[str]] = {
    "total": [
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
        "관리_건축물대장_PK",
        "대장_구분_코드",
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


def _kv_line_from_payload(payload: dict[str, Any], bucket: str) -> str:
    columns = _KV_BUCKET_COLUMNS.get(bucket, [])
    parts: list[str] = []
    for col in columns:
        value = payload.get(col, "")
        parts.append("" if value is None else str(value))
    return "|".join(parts)


def _build_payload(parts: list[str], category: str) -> dict[str, str]:
    columns = _CATEGORY_COLUMNS.get(category, [])
    payload: dict[str, str] = {}
    for idx, value in enumerate(parts):
        if idx < len(columns):
            key = columns[idx]
        else:
            key = f"col_{idx + 1}"
        payload[key] = value
    payload["_category"] = category
    return payload


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

    # Fallback: update files for building_info can include extra columns without headers.
    # In that case, our payload uses "col_N" keys. Scan a few trailing columns for CRUD tokens.
    tail_cols: list[tuple[int, Any]] = []
    for key, value in payload.items():
        if not isinstance(key, str) or not key.startswith("col_"):
            continue
        try:
            idx = int(key.split("_", 1)[1])
        except Exception:
            continue
        tail_cols.append((idx, value))
    if tail_cols:
        tail_cols.sort(reverse=True)
        for _, value in tail_cols[:12]:
            token = _normalize_op_text(value)
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


def _normalize_area_text(value: Any) -> str:
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


def _lookup_violation_raw(payload: dict[str, Any]) -> str | None:
    for key in ("A20", "위반건축물여부", "위반여부", "violation"):
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _lookup_violation_flag(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    if normalized in {"Y", "1", "TRUE", "T"}:
        return True
    if normalized in {"N", "0", "FALSE", "F"}:
        return False
    return None


def _build_lookup_row(
    release_id: int,
    pnu: str,
    dataset_code: str,
    payload: dict[str, Any],
    source_file: str,
    row_no: int,
) -> tuple[Any, ...] | None:
    mgmt_pk = str(payload.get("관리_건축물대장_PK") or payload.get("A1") or "").strip()
    if not mgmt_pk:
        return None
    building_name = str(payload.get("건물_명") or payload.get("건물명") or "").strip()
    dong_name = str(payload.get("동_명") or payload.get("동명") or "").strip()
    area_text = _normalize_area_text(
        payload.get("건축_면적(㎡)")
        or payload.get("면적(㎡)")
        or payload.get("건축면적")
        or payload.get("면적")
    )
    violation_raw = _lookup_violation_raw(payload)
    name_for_norm = building_name or dong_name
    building_name_norm = _normalize_building_name(name_for_norm)
    return (
        release_id,
        pnu,
        dataset_code,
        mgmt_pk,
        building_name or None,
        dong_name or None,
        building_name_norm or None,
        area_text or None,
        violation_raw,
        _lookup_violation_flag(violation_raw),
        source_file,
        row_no,
    )


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
            """
        ) as copy:
            for row in rows:
                copy.write_row(row)


def insert_lookup_batch(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY building_info_lookup (
              release_id, pnu, dataset_code, building_mgmt_pk,
              building_name, dong_name, building_name_norm, area_text,
              violation_raw, is_violation, source_file, row_no
            )
            FROM STDIN
            """
        ) as copy:
            for row in rows:
                copy.write_row(row)


def insert_line_batch(conn: psycopg.Connection, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY building_info_line (
              release_id, pnu, category, line, source_file, line_no
            )
            FROM STDIN
            """
        ) as copy:
            for row in rows:
                copy.write_row(row)


def _prepare_merge_temp_table(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS _building_info_merge_touched_scope (
          pnu TEXT NOT NULL,
          category TEXT NOT NULL,
          PRIMARY KEY (pnu, category)
        )
        ON COMMIT PRESERVE ROWS
        """
    )


def _register_new_scope_pnus(conn: psycopg.Connection, category: str, pnus: list[str]) -> list[str]:
    if not pnus:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO _building_info_merge_touched_scope (pnu, category)
            SELECT DISTINCT pnu, %s
            FROM unnest(%s::text[]) AS t(pnu)
            ON CONFLICT (pnu, category) DO NOTHING
            RETURNING pnu
            """,
            (category, pnus),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _activate_release(conn: psycopg.Connection, release_id: int, data_type: str, records_count: int) -> None:
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


def _resolve_worker_count(requested: int, file_count: int) -> int:
    if file_count < 1:
        return 1
    return max(1, min(requested, file_count))


def _parse_positive_int_env(name: str, default: int = 0) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return value if value > 0 else default


def _total_input_size_bytes(files: list[Path]) -> int:
    total = 0
    for path in files:
        try:
            total += int(path.stat().st_size)
        except Exception:
            continue
    return total


def _fallback_auto_worker_count(cpu_count: int, auto_max_workers: int, total_size_bytes: int) -> int:
    cpu = max(1, int(cpu_count))
    cap = max(1, int(auto_max_workers))
    size_gb = float(total_size_bytes) / float(1024 ** 3)

    if size_gb <= 1.0:
        base = max(2, int(round(cpu * 0.4)))
    elif size_gb <= 5.0:
        base = max(4, int(round(cpu * 0.55)))
    else:
        base = max(6, int(round(cpu * 0.7)))

    return max(1, min(base, cap))


def _load_worker_history_stats(
    conn: psycopg.Connection,
    data_type: str,
    limit: int = 24,
) -> list[dict[str, Any]]:
    query = """
    WITH per_job AS (
      SELECT
        j.id AS job_id,
        j.inserted_rows::double precision AS inserted_rows,
        GREATEST(EXTRACT(EPOCH FROM (j.finished_at - j.started_at)), 1.0) AS elapsed_sec,
        COALESCE(
          MAX(
            CASE
              WHEN COALESCE(w.worker_name, '') ~ 'ThreadPoolExecutor-0_[0-9]+'
                THEN COALESCE(substring(w.worker_name from 'ThreadPoolExecutor-0_([0-9]+)')::int, 0) + 1
              WHEN COALESCE(w.worker_name, '') <> '' THEN 1
              ELSE NULL
            END
          ),
          1
        ) AS worker_count
      FROM cadastral_import_job j
      LEFT JOIN cadastral_release r ON r.id = j.release_id
      LEFT JOIN cadastral_import_job_worker w ON w.job_id = j.id
      WHERE j.data_type = %s
        AND j.status = 'SUCCEEDED'
        AND j.inserted_rows > 0
        AND j.started_at IS NOT NULL
        AND j.finished_at IS NOT NULL
        AND COALESCE(r.metadata->>'operation_mode', 'full') = 'full'
      GROUP BY j.id, j.inserted_rows, j.started_at, j.finished_at
    ),
    scored AS (
      SELECT
        worker_count,
        (inserted_rows / elapsed_sec) AS rows_per_sec
      FROM per_job
      WHERE worker_count >= 1
    )
    SELECT
      worker_count,
      COUNT(*) AS sample_count,
      AVG(rows_per_sec) AS avg_rows_per_sec,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY rows_per_sec) AS median_rows_per_sec
    FROM scored
    GROUP BY worker_count
    ORDER BY median_rows_per_sec DESC NULLS LAST, sample_count DESC, worker_count ASC
    LIMIT %s
    """

    with conn.cursor() as cur:
        cur.execute(query, (data_type, int(limit)))
        rows = cur.fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        worker_count = int(row[0] or 1)
        sample_count = int(row[1] or 0)
        avg_rows_per_sec = float(row[2] or 0.0)
        median_rows_per_sec = float(row[3] or 0.0)
        result.append(
            {
                "worker_count": worker_count,
                "sample_count": sample_count,
                "avg_rows_per_sec": avg_rows_per_sec,
                "median_rows_per_sec": median_rows_per_sec,
            }
        )
    return result


def _select_worker_plan(
    db_url: str,
    source_files: list[Path],
    requested_workers: int,
    data_type: str,
) -> dict[str, Any]:
    cpu_count = max(1, int(os.cpu_count() or 1))
    env_auto_max = _parse_positive_int_env("BUILDING_INFO_IMPORT_AUTO_MAX_WORKERS", 0)
    auto_max_workers = cpu_count if env_auto_max <= 0 else max(1, min(cpu_count, env_auto_max))
    total_size_bytes = _total_input_size_bytes(source_files)

    if requested_workers > 0:
        return {
            "worker_count": max(1, int(requested_workers)),
            "mode": "fixed",
            "cpu_count": cpu_count,
            "auto_max_workers": auto_max_workers,
            "total_size_bytes": total_size_bytes,
            "history_rows": [],
        }

    fallback_workers = _fallback_auto_worker_count(
        cpu_count=cpu_count,
        auto_max_workers=auto_max_workers,
        total_size_bytes=total_size_bytes,
    )
    chosen_workers = fallback_workers
    chosen_mode = "fallback"
    history_rows: list[dict[str, Any]] = []

    try:
        with psycopg.connect(db_url) as conn:
            history_rows = _load_worker_history_stats(conn, data_type=data_type, limit=24)
    except Exception:
        history_rows = []

    if history_rows:
        eligible = [
            row
            for row in history_rows
            if row["worker_count"] <= auto_max_workers and row["sample_count"] >= 2
        ]
        if eligible:
            best_row = max(
                eligible,
                key=lambda row: (float(row["median_rows_per_sec"]), int(row["sample_count"])),
            )
            threshold = float(best_row["median_rows_per_sec"]) * 0.95
            near_best = [row for row in eligible if float(row["median_rows_per_sec"]) >= threshold]
            chosen_workers = min(int(row["worker_count"]) for row in near_best)
            chosen_mode = "history"
        else:
            lite = [
                row for row in history_rows
                if row["worker_count"] <= auto_max_workers and row["sample_count"] >= 1
            ]
            if lite:
                top = lite[0]
                blended = int(round((int(top["worker_count"]) + fallback_workers) / 2))
                chosen_workers = max(1, min(blended, auto_max_workers))
                chosen_mode = "history-lite"

    return {
        "worker_count": max(1, int(chosen_workers)),
        "mode": chosen_mode,
        "cpu_count": cpu_count,
        "auto_max_workers": auto_max_workers,
        "total_size_bytes": total_size_bytes,
        "fallback_workers": fallback_workers,
        "history_rows": history_rows,
    }


def _split_large_file_round_robin(
    source_file: Path,
    worker_count: int,
    split_dir: Path,
) -> tuple[list[Path], dict[str, Any]]:
    if worker_count <= 1:
        return [source_file], {"source_line_count": 0, "shard_line_counts": []}

    split_dir.mkdir(parents=True, exist_ok=True)
    suffix = source_file.suffix or ".txt"
    stem = source_file.stem
    source_token = hashlib.sha1(str(source_file.resolve()).encode("utf-8")).hexdigest()[:8]

    shard_paths: list[Path] = [
        split_dir / f"{stem}__{source_token}__shard_{idx + 1:03d}{suffix}"
        for idx in range(worker_count)
    ]
    shard_line_counts = [0 for _ in range(worker_count)]
    handles = [path.open("wb") for path in shard_paths]
    source_line_count = 0

    try:
        with source_file.open("rb") as src:
            for line_idx, raw in enumerate(src):
                source_line_count += 1
                target_idx = line_idx % worker_count
                handles[target_idx].write(raw)
                shard_line_counts[target_idx] += 1
    finally:
        for handle in handles:
            with contextlib.suppress(Exception):
                handle.close()

    non_empty_shards: list[Path] = []
    non_empty_counts: list[int] = []
    for shard_path, line_count in zip(shard_paths, shard_line_counts):
        if line_count > 0:
            non_empty_shards.append(shard_path)
            non_empty_counts.append(int(line_count))
        else:
            with contextlib.suppress(Exception):
                shard_path.unlink()

    return (
        non_empty_shards or [source_file],
        {
            "source_file": source_file.name,
            "source_line_count": int(source_line_count),
            "shard_line_counts": non_empty_counts,
            "shard_count": len(non_empty_counts),
        },
    )


def _prepare_parallel_input_files(
    files: list[Path],
    worker_count: int,
    operation_mode: str,
    scope_replace_mode: bool,
) -> tuple[list[Path], Any | None, list[dict[str, Any]]]:
    if (
        operation_mode != "full"
        or scope_replace_mode
        or worker_count <= 1
    ):
        return files, None, []

    temp_dir = tempfile.TemporaryDirectory(prefix="building_info_split_")
    split_dir = Path(temp_dir.name)
    all_split_files: list[Path] = []
    split_meta_entries: list[dict[str, Any]] = []

    for source_file in files:
        split_files, split_meta = _split_large_file_round_robin(
            source_file=source_file,
            worker_count=worker_count,
            split_dir=split_dir,
        )
        all_split_files.extend(split_files)
        split_meta_entries.append(split_meta)
        print(
            f"[INFO] 파일 분할: source={source_file.name}, shards={len(split_files)}",
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
    release_data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    category = detect_category(path)
    dataset_code = _dataset_code_from_category(category)
    source_file = path.name
    worker_name = threading.current_thread().name
    dataset_rows: list[tuple[Any, ...]] = []
    lookup_rows: list[tuple[Any, ...]] = []
    inserted_rows = 0
    reported_rows = 0
    scanned_lines = 0
    blank_skips = 0
    pnu_skips = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal inserted_rows
        nonlocal reported_rows
        if not dataset_rows and not lookup_rows:
            return
        with conn.transaction():
            if dataset_rows:
                insert_dataset_batch(conn, dataset_rows)
                inserted_now = len(dataset_rows)
                inserted_rows += inserted_now
                reported_rows += inserted_now
            if lookup_rows:
                insert_lookup_batch(conn, lookup_rows)
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        dataset_rows.clear()
        lookup_rows.clear()

    with psycopg.connect(db_url, autocommit=True) as conn:
        schema_registry = DatasetSchemaRegistry(conn, release_data_type)
        with conn.transaction():
            schema_registry.ensure_release_partition(release_id)
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            with path.open("rb") as fp:
                for line_no, raw in enumerate(fp, start=1):
                    scanned_lines += 1
                    line = decode_line(raw).strip()
                    if not line:
                        blank_skips += 1
                        continue
                    parts = line.split("|")
                    pnu = build_pnu(parts, category)
                    if not pnu:
                        pnu_skips += 1
                        continue
                    payload = _build_payload(parts, category)
                    schema_id, payload_values = schema_registry.encode_payload(dataset_code, payload)
                    dataset_rows.append(
                        (
                            release_id,
                            release_data_type,
                            dataset_code,
                            source_file,
                            line_no,
                            pnu,
                            schema_id,
                            payload_values,
                            None,
                            None,
                        )
                    )
                    lookup_row = _build_lookup_row(
                        release_id=release_id,
                        pnu=pnu,
                        dataset_code=dataset_code,
                        payload=payload,
                        source_file=source_file,
                        row_no=line_no,
                    )
                    if lookup_row is not None:
                        lookup_rows.append(lookup_row)
                    if len(dataset_rows) >= batch_size:
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
        "category": category,
        "inserted_rows": inserted_rows,
        "scanned_lines": scanned_lines,
        "blank_skips": blank_skips,
        "pnu_skips": pnu_skips,
        "delete_skips": 0,
    }


def _import_full_file_pnu_kv(
    db_url: str,
    release_id: int,
    release_data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    category = detect_category(path)
    source_file = path.name
    worker_name = threading.current_thread().name
    payload_by_pnu: dict[str, dict[str, Any]] = {}
    buffered_lines = 0
    inserted_rows = 0
    reported_rows = 0
    scanned_lines = 0
    blank_skips = 0
    pnu_skips = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_lines
        nonlocal inserted_rows
        nonlocal reported_rows
        if not payload_by_pnu:
            return
        with conn.transaction():
            upsert_building_info_kv_payloads(
                conn,
                release_id=release_id,
                data_type=release_data_type,
                payload_by_pnu=payload_by_pnu,
            )
            inserted_rows += buffered_lines
            reported_rows += buffered_lines
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        payload_by_pnu.clear()
        buffered_lines = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            with path.open("rb") as fp:
                for line_no, raw in enumerate(fp, start=1):
                    scanned_lines += 1
                    line = decode_line(raw).strip()
                    if not line:
                        blank_skips += 1
                        continue
                    parts = line.split("|")
                    pnu = build_pnu(parts, category)
                    if not pnu:
                        pnu_skips += 1
                        continue
                    payload = _build_payload(parts, category)
                    bucket_line = _kv_line_from_payload(payload, category)
                    if not bucket_line:
                        continue

                    entry = payload_by_pnu.setdefault(pnu, {})
                    entry.setdefault(category, []).append(bucket_line)

                    mgmt_pk = str(payload.get("관리_건축물대장_PK") or payload.get("A1") or "").strip()
                    violation_raw = _lookup_violation_raw(payload)
                    if mgmt_pk and violation_raw:
                        entry.setdefault("violation_by_key", {})[mgmt_pk] = violation_raw
                        flag = _lookup_violation_flag(violation_raw)
                        if flag is not None:
                            entry.setdefault("is_violation_by_key", {})[mgmt_pk] = bool(flag)

                    buffered_lines += 1
                    if buffered_lines >= batch_size:
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
        "category": category,
        "inserted_rows": inserted_rows,
        "scanned_lines": scanned_lines,
        "blank_skips": blank_skips,
        "pnu_skips": pnu_skips,
        "delete_skips": 0,
    }


def _import_update_file_pnu_kv(
    db_url: str,
    release_id: int,
    release_data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
    op_columns: list[str],
) -> dict[str, Any]:
    category = detect_category(path)
    source_file = path.name
    worker_name = threading.current_thread().name
    payload_by_pnu: dict[str, dict[str, Any]] = {}
    pending_delete_pnus: set[str] = set()
    cleared_pnus: set[str] = set()
    buffered_lines = 0
    inserted_rows = 0
    reported_rows = 0
    scanned_lines = 0
    blank_skips = 0
    pnu_skips = 0
    delete_skips = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_lines
        nonlocal inserted_rows
        nonlocal reported_rows
        if not payload_by_pnu and not pending_delete_pnus:
            return

        delete_pnus = sorted({pnu for pnu in pending_delete_pnus if pnu})
        with conn.transaction():
            if delete_pnus:
                clear_building_info_bucket(
                    conn,
                    release_id=release_id,
                    data_type=release_data_type,
                    bucket=category,
                    pnus=delete_pnus,
                )
                cleared_pnus.update(delete_pnus)

            upsert_all = {pnu for pnu in payload_by_pnu.keys() if pnu}
            upsert_clear_pnus = sorted(pnu for pnu in upsert_all if pnu not in cleared_pnus)
            if upsert_clear_pnus:
                clear_building_info_bucket(
                    conn,
                    release_id=release_id,
                    data_type=release_data_type,
                    bucket=category,
                    pnus=upsert_clear_pnus,
                )
                cleared_pnus.update(upsert_clear_pnus)

            if payload_by_pnu:
                upsert_building_info_kv_payloads(
                    conn,
                    release_id=release_id,
                    data_type=release_data_type,
                    payload_by_pnu=payload_by_pnu,
                )
                inserted_rows += buffered_lines
                reported_rows += buffered_lines
                if reported_rows >= progress_step:
                    increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                    increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                    reported_rows = 0

        payload_by_pnu.clear()
        pending_delete_pnus.clear()
        buffered_lines = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            with path.open("rb") as fp:
                for line_no, raw in enumerate(fp, start=1):
                    scanned_lines += 1
                    line = decode_line(raw).strip()
                    if not line:
                        blank_skips += 1
                        continue
                    parts = line.split("|")
                    pnu = build_pnu(parts, category)
                    if not pnu:
                        pnu_skips += 1
                        continue

                    payload = _build_payload(parts, category)
                    op = _parse_operation(payload, op_columns)
                    if op == "delete":
                        delete_skips += 1
                        pending_delete_pnus.add(pnu)
                        removed = payload_by_pnu.pop(pnu, None)
                        if removed and isinstance(removed.get(category), list):
                            buffered_lines -= len(removed.get(category) or [])
                        if len(pending_delete_pnus) >= batch_size:
                            flush(conn)
                        continue

                    bucket_line = _kv_line_from_payload(payload, category)
                    if not bucket_line:
                        continue

                    entry = payload_by_pnu.setdefault(pnu, {})
                    entry.setdefault(category, []).append(bucket_line)

                    mgmt_pk = str(payload.get("관리_건축물대장_PK") or payload.get("A1") or "").strip()
                    violation_raw = _lookup_violation_raw(payload)
                    if mgmt_pk and violation_raw:
                        entry.setdefault("violation_by_key", {})[mgmt_pk] = violation_raw
                        flag = _lookup_violation_flag(violation_raw)
                        if flag is not None:
                            entry.setdefault("is_violation_by_key", {})[mgmt_pk] = bool(flag)

                    buffered_lines += 1
                    if buffered_lines >= batch_size:
                        flush(conn)

            flush(conn)
            if scanned_lines > 0 and inserted_rows == 0 and delete_skips <= 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, scanned_lines={scanned_lines}, inserted_rows={inserted_rows}"
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
        "category": category,
        "inserted_rows": inserted_rows,
        "scanned_lines": scanned_lines,
        "blank_skips": blank_skips,
        "pnu_skips": pnu_skips,
        "delete_skips": delete_skips,
    }


def _kv_line_from_parts(
    parts: list[str],
    *,
    category: str,
    index_by_col: dict[str, int],
) -> str:
    cols = _KV_BUCKET_COLUMNS.get(category, [])
    if not cols:
        return ""
    out: list[str] = []
    for col in cols:
        idx = index_by_col.get(col)
        if idx is None or idx < 0 or idx >= len(parts):
            out.append("")
            continue
        value = parts[idx]
        out.append("" if value is None else str(value))
    return "|".join(out)


def _lookup_row_from_parts(
    *,
    release_id: int,
    pnu: str,
    dataset_code: str,
    parts: list[str],
    index_by_col: dict[str, int],
    source_file: str,
    row_no: int,
) -> tuple[Any, ...] | None:
    # NOTE: building_info_lookup is used for fast candidate matching and (optional) violation flags.
    # Only keep rows where we can safely extract a stable management PK.
    pk_idx = index_by_col.get("관리_건축물대장_PK")
    mgmt_pk = ""
    if pk_idx is not None and 0 <= pk_idx < len(parts):
        mgmt_pk = str(parts[pk_idx] or "").strip()
    if not mgmt_pk:
        return None

    def _get_first(*names: str) -> str:
        for name in names:
            idx = index_by_col.get(name)
            if idx is None or idx < 0 or idx >= len(parts):
                continue
            value = str(parts[idx] or "").strip()
            if value:
                return value
        return ""

    building_name = _get_first("건물_명", "건물명")
    dong_name = _get_first("동_명", "동명")
    area_raw = _get_first("건축_면적(㎡)", "면적(㎡)", "건축면적", "면적")
    area_text = _normalize_area_text(area_raw)
    # building_info 원본에 위반여부 컬럼이 항상 있는 것은 아니므로, 없으면 NULL로 둡니다.
    violation_raw = ""
    for key in ("A20", "위반건축물여부", "위반여부", "violation"):
        value = _get_first(key)
        if value:
            violation_raw = value
            break
    violation_raw_or_none = violation_raw if violation_raw else None
    name_for_norm = building_name or dong_name
    building_name_norm = _normalize_building_name(name_for_norm)
    return (
        int(release_id),
        str(pnu),
        str(dataset_code),
        mgmt_pk,
        building_name or None,
        dong_name or None,
        building_name_norm or None,
        area_text or None,
        violation_raw_or_none,
        _lookup_violation_flag(violation_raw_or_none),
        str(source_file),
        int(row_no),
    )


def _parse_operation_from_parts(parts: list[str]) -> str:
    # Update files for building_info are often headerless; scan trailing fields for CRUD tokens.
    add_tokens = {"add", "insert", "create", "new", "추가", "신규", "등록", "a", "i", "c"}
    update_tokens = {"update", "modify", "수정", "변경", "정정", "갱신", "u", "m"}
    delete_tokens = {"delete", "remove", "삭제", "말소", "d", "r"}
    for value in reversed(parts[-12:]):
        token = _normalize_op_text(value)
        if not token:
            continue
        if token in delete_tokens:
            return "delete"
        if token in add_tokens:
            return "add"
        if token in update_tokens:
            return "update"
    return "none"


def _import_full_file_line_table(
    db_url: str,
    release_id: int,
    release_data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    category = detect_category(path)
    dataset_code = _dataset_code_from_category(category)
    source_file = path.name
    worker_name = threading.current_thread().name

    columns = _CATEGORY_COLUMNS.get(category, [])
    index_by_col = {name: idx for idx, name in enumerate(columns)}

    line_rows: list[tuple[Any, ...]] = []
    lookup_rows: list[tuple[Any, ...]] = []
    buffered_lines = 0
    inserted_rows = 0
    reported_rows = 0
    scanned_lines = 0
    blank_skips = 0
    pnu_skips = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_lines
        nonlocal inserted_rows
        nonlocal reported_rows
        if not line_rows and not lookup_rows:
            return
        with conn.transaction():
            if line_rows:
                insert_line_batch(conn, line_rows)
            if lookup_rows:
                insert_lookup_batch(conn, lookup_rows)
            inserted_rows += buffered_lines
            reported_rows += buffered_lines
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0
        line_rows.clear()
        lookup_rows.clear()
        buffered_lines = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            with path.open("rb") as fp:
                for line_no, raw in enumerate(fp, start=1):
                    scanned_lines += 1
                    line = decode_line(raw).strip()
                    if not line:
                        blank_skips += 1
                        continue
                    parts = line.split("|")
                    pnu = build_pnu(parts, category)
                    if not pnu:
                        pnu_skips += 1
                        continue

                    bucket_line = _kv_line_from_parts(parts, category=category, index_by_col=index_by_col)
                    if bucket_line:
                        line_rows.append(
                            (int(release_id), str(pnu), category, bucket_line, source_file, int(line_no))
                        )
                        buffered_lines += 1

                    # building_info_lookup는 single/total만 유지해 용량을 줄입니다.
                    if category in {"single", "total"}:
                        lookup_row = _lookup_row_from_parts(
                            release_id=release_id,
                            pnu=pnu,
                            dataset_code=dataset_code,
                            parts=parts,
                            index_by_col=index_by_col,
                            source_file=source_file,
                            row_no=line_no,
                        )
                        if lookup_row is not None:
                            lookup_rows.append(lookup_row)

                    if buffered_lines >= batch_size:
                        flush(conn)

            flush(conn)
            if scanned_lines > 0 and inserted_rows == 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, scanned_lines={scanned_lines}, inserted_rows={inserted_rows}"
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
        "category": category,
        "inserted_rows": inserted_rows,
        "scanned_lines": scanned_lines,
        "blank_skips": blank_skips,
        "pnu_skips": pnu_skips,
        "delete_skips": 0,
    }


def _import_full_file_line_table_scope_replace(
    db_url: str,
    release_id: int,
    release_data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
) -> dict[str, Any]:
    category = detect_category(path)
    dataset_code = _dataset_code_from_category(category)
    source_file = path.name
    worker_name = threading.current_thread().name

    columns = _CATEGORY_COLUMNS.get(category, [])
    index_by_col = {name: idx for idx, name in enumerate(columns)}

    line_rows: list[tuple[Any, ...]] = []
    lookup_rows: list[tuple[Any, ...]] = []
    buffered_lines = 0
    inserted_rows = 0
    reported_rows = 0
    scanned_lines = 0
    blank_skips = 0
    pnu_skips = 0
    cleared_pnus: set[str] = set()

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_lines
        nonlocal inserted_rows
        nonlocal reported_rows
        if not line_rows and not lookup_rows:
            return

        batch_pnus = sorted({str(row[1]) for row in line_rows if row[1]})
        to_clear = [pnu for pnu in batch_pnus if pnu and pnu not in cleared_pnus]

        with conn.transaction():
            if to_clear:
                conn.execute(
                    """
                    DELETE FROM building_info_line
                    WHERE release_id = %s
                      AND category = %s
                      AND pnu = ANY(%s)
                    """,
                    (int(release_id), category, to_clear),
                )
                conn.execute(
                    """
                    DELETE FROM building_info_lookup
                    WHERE release_id = %s
                      AND dataset_code = %s
                      AND pnu = ANY(%s)
                    """,
                    (int(release_id), dataset_code, to_clear),
                )
                cleared_pnus.update(to_clear)

            if line_rows:
                insert_line_batch(conn, line_rows)
            if lookup_rows:
                insert_lookup_batch(conn, lookup_rows)

            inserted_rows += buffered_lines
            reported_rows += buffered_lines
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0

        line_rows.clear()
        lookup_rows.clear()
        buffered_lines = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            with path.open("rb") as fp:
                for line_no, raw in enumerate(fp, start=1):
                    scanned_lines += 1
                    line = decode_line(raw).strip()
                    if not line:
                        blank_skips += 1
                        continue
                    parts = line.split("|")
                    pnu = build_pnu(parts, category)
                    if not pnu:
                        pnu_skips += 1
                        continue

                    bucket_line = _kv_line_from_parts(parts, category=category, index_by_col=index_by_col)
                    if bucket_line:
                        line_rows.append(
                            (int(release_id), str(pnu), category, bucket_line, source_file, int(line_no))
                        )
                        buffered_lines += 1

                    if category in {"single", "total"}:
                        lookup_row = _lookup_row_from_parts(
                            release_id=release_id,
                            pnu=pnu,
                            dataset_code=dataset_code,
                            parts=parts,
                            index_by_col=index_by_col,
                            source_file=source_file,
                            row_no=line_no,
                        )
                        if lookup_row is not None:
                            lookup_rows.append(lookup_row)

                    if buffered_lines >= batch_size:
                        flush(conn)

            flush(conn)
            if scanned_lines > 0 and inserted_rows == 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, scanned_lines={scanned_lines}, inserted_rows={inserted_rows}"
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
        "category": category,
        "inserted_rows": inserted_rows,
        "scanned_lines": scanned_lines,
        "blank_skips": blank_skips,
        "pnu_skips": pnu_skips,
        "delete_skips": 0,
    }


def _import_update_file_line_table(
    db_url: str,
    release_id: int,
    release_data_type: str,
    path: Path,
    batch_size: int,
    job_id: int | None,
    progress_step: int,
    op_columns: list[str],
) -> dict[str, Any]:
    del op_columns  # update files are commonly headerless; we scan tail fields instead.
    category = detect_category(path)
    dataset_code = _dataset_code_from_category(category)
    source_file = path.name
    worker_name = threading.current_thread().name

    columns = _CATEGORY_COLUMNS.get(category, [])
    index_by_col = {name: idx for idx, name in enumerate(columns)}

    line_rows_by_pnu: dict[str, list[tuple[Any, ...]]] = {}
    lookup_rows_by_pnu: dict[str, list[tuple[Any, ...]]] = {}
    pending_delete_pnus: set[str] = set()
    cleared_pnus: set[str] = set()

    buffered_lines = 0
    inserted_rows = 0
    reported_rows = 0
    scanned_lines = 0
    blank_skips = 0
    pnu_skips = 0
    delete_skips = 0

    def flush(conn: psycopg.Connection) -> None:
        nonlocal buffered_lines
        nonlocal inserted_rows
        nonlocal reported_rows
        nonlocal delete_skips
        if not line_rows_by_pnu and not lookup_rows_by_pnu and not pending_delete_pnus:
            return

        delete_pnus = sorted({pnu for pnu in pending_delete_pnus if pnu})
        batch_pnus = sorted({pnu for pnu in line_rows_by_pnu.keys() if pnu})
        upsert_clear = [pnu for pnu in batch_pnus if pnu not in cleared_pnus]

        line_rows: list[tuple[Any, ...]] = []
        for pnu, rows in line_rows_by_pnu.items():
            if not pnu or not rows:
                continue
            line_rows.extend(rows)
        lookup_rows: list[tuple[Any, ...]] = []
        for pnu, rows in lookup_rows_by_pnu.items():
            if not pnu or not rows:
                continue
            lookup_rows.extend(rows)

        with conn.transaction():
            if delete_pnus:
                conn.execute(
                    """
                    DELETE FROM building_info_line
                    WHERE release_id = %s
                      AND category = %s
                      AND pnu = ANY(%s)
                    """,
                    (int(release_id), category, delete_pnus),
                )
                conn.execute(
                    """
                    DELETE FROM building_info_lookup
                    WHERE release_id = %s
                      AND dataset_code = %s
                      AND pnu = ANY(%s)
                    """,
                    (int(release_id), dataset_code, delete_pnus),
                )
                cleared_pnus.update(delete_pnus)

            if upsert_clear:
                conn.execute(
                    """
                    DELETE FROM building_info_line
                    WHERE release_id = %s
                      AND category = %s
                      AND pnu = ANY(%s)
                    """,
                    (int(release_id), category, upsert_clear),
                )
                conn.execute(
                    """
                    DELETE FROM building_info_lookup
                    WHERE release_id = %s
                      AND dataset_code = %s
                      AND pnu = ANY(%s)
                    """,
                    (int(release_id), dataset_code, upsert_clear),
                )
                cleared_pnus.update(upsert_clear)

            if line_rows:
                insert_line_batch(conn, line_rows)
            if lookup_rows:
                insert_lookup_batch(conn, lookup_rows)

            inserted_rows += buffered_lines
            reported_rows += buffered_lines
            if reported_rows >= progress_step:
                increment_job_progress(conn, job_id, inserted_rows_delta=reported_rows)
                increment_worker_progress(conn, job_id, source_file, processed_rows_delta=reported_rows)
                reported_rows = 0

        line_rows_by_pnu.clear()
        lookup_rows_by_pnu.clear()
        pending_delete_pnus.clear()
        buffered_lines = 0

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.transaction():
            mark_worker_running(conn, job_id, source_file, worker_name)

        try:
            with path.open("rb") as fp:
                for line_no, raw in enumerate(fp, start=1):
                    scanned_lines += 1
                    line = decode_line(raw).strip()
                    if not line:
                        blank_skips += 1
                        continue
                    parts = line.split("|")
                    pnu = build_pnu(parts, category)
                    if not pnu:
                        pnu_skips += 1
                        continue

                    op = _parse_operation_from_parts(parts)
                    if op == "delete":
                        delete_skips += 1
                        pending_delete_pnus.add(pnu)
                        # Drop any pending upserts for the same PNU in this buffer.
                        removed = line_rows_by_pnu.pop(pnu, None)
                        if removed:
                            buffered_lines -= len(removed)
                        lookup_rows_by_pnu.pop(pnu, None)
                        if len(pending_delete_pnus) >= batch_size:
                            flush(conn)
                        continue

                    bucket_line = _kv_line_from_parts(parts, category=category, index_by_col=index_by_col)
                    if not bucket_line:
                        continue
                    line_rows_by_pnu.setdefault(pnu, []).append(
                        (int(release_id), str(pnu), category, bucket_line, source_file, int(line_no))
                    )
                    buffered_lines += 1

                    if category in {"single", "total"}:
                        lookup_row = _lookup_row_from_parts(
                            release_id=release_id,
                            pnu=pnu,
                            dataset_code=dataset_code,
                            parts=parts,
                            index_by_col=index_by_col,
                            source_file=source_file,
                            row_no=line_no,
                        )
                        if lookup_row is not None:
                            lookup_rows_by_pnu.setdefault(pnu, []).append(lookup_row)

                    if buffered_lines >= batch_size:
                        flush(conn)

            flush(conn)
            if scanned_lines > 0 and inserted_rows == 0 and delete_skips <= 0:
                raise RuntimeError(
                    f"pnu 추출 실패: source={source_file}, scanned_lines={scanned_lines}, inserted_rows={inserted_rows}"
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
        "category": category,
        "inserted_rows": inserted_rows,
        "scanned_lines": scanned_lines,
        "blank_skips": blank_skips,
        "pnu_skips": pnu_skips,
        "delete_skips": delete_skips,
    }


def main() -> int:
    args = parse_args()
    db_url = args.db_url.strip()
    if not db_url:
        raise SystemExit("DATABASE_URL이 필요합니다. --db-url 또는 환경변수로 전달하세요.")

    source_dir = Path(args.source_dir)
    files = sorted(Path(p).resolve() for p in glob.glob(str(source_dir / args.pattern)))
    if not files:
        lower_pattern = args.pattern.lower()
        files = sorted(
            p.resolve()
            for p in source_dir.iterdir()
            if p.is_file() and fnmatch.fnmatch(p.name.lower(), lower_pattern)
        )
    if not files:
        raise SystemExit(f"대상 파일이 없습니다: {source_dir}/{args.pattern}")

    release_id = args.release_id
    batch_size = max(100, args.batch_size)
    inserted_total = 0
    processed_files = 0
    scanned_lines_total = 0
    blank_skips_total = 0
    pnu_skips_total = 0
    delete_skips_total = 0
    release_data_type = args.data_type or "building_info"
    progress_step = max(5000, batch_size)
    last_reported_rows = 0
    requested_workers = int(args.workers or 0)
    worker_plan = _select_worker_plan(
        db_url=db_url,
        source_files=files,
        requested_workers=requested_workers,
        data_type=release_data_type,
    )
    worker_count = int(worker_plan["worker_count"])
    total_size_gb = float(worker_plan.get("total_size_bytes") or 0) / float(1024 ** 3)
    print(
        "[INFO] 워커 결정: "
        f"mode={worker_plan.get('mode')}, "
        f"workers={worker_count}, "
        f"cpu={worker_plan.get('cpu_count')}, "
        f"auto_max={worker_plan.get('auto_max_workers')}, "
        f"input_size_gb={total_size_gb:.2f}",
        flush=True,
    )
    if worker_plan.get("mode") in {"history", "history-lite"}:
        history_rows = worker_plan.get("history_rows") or []
        preview = ", ".join(
            f"{row.get('worker_count')}w/{float(row.get('median_rows_per_sec') or 0):.0f}rps/{int(row.get('sample_count') or 0)}s"
            for row in history_rows[:4]
        )
        if preview:
            print(f"[INFO] 워커 히스토리 상위: {preview}", flush=True)
    operation_mode = (args.operation_mode or "full").strip().lower()
    is_update_mode = operation_mode == "update"
    op_columns = [col.strip() for col in str(args.op_columns or "").split(",") if col.strip()]
    store_mode_raw = str(args.store or "auto").strip().lower()
    # For building_info, pnu_kv storage can explode TOAST size due to large per-PNU upserts.
    # Default to an insert-only line table.
    store_mode = "line_table" if store_mode_raw == "auto" else store_mode_raw
    if store_mode == "pnu_kv" and bool(args.merge_by_pnu):
        raise SystemExit("pnu_kv 저장 모드는 merge-by-pnu 모드를 지원하지 않습니다. full 모드로 적재하세요.")
    merge_by_pnu_mode = (
        bool(args.merge_by_pnu)
        and store_mode != "pnu_kv"
        and not bool(args.truncate_release)
        and not is_update_mode
    )
    scope_replace_mode = merge_by_pnu_mode or is_update_mode
    temp_split_dir: Any | None = None
    split_meta_entries: list[dict[str, Any]] = []
    files, temp_split_dir, split_meta_entries = _prepare_parallel_input_files(
        files=files,
        worker_count=worker_count,
        operation_mode=operation_mode,
        scope_replace_mode=scope_replace_mode,
    )
    worker_count = _resolve_worker_count(worker_count, len(files))
    parallel_full_mode = (operation_mode == "full") and not scope_replace_mode and worker_count > 1 and len(files) > 1

    try:
        with psycopg.connect(db_url, autocommit=True) as conn:
            schema_registry = DatasetSchemaRegistry(conn, release_data_type)
            with conn.cursor() as cur:
                cur.execute("SELECT data_type, is_active FROM cadastral_release WHERE id = %s", (release_id,))
                release_row = cur.fetchone()
                if not release_row:
                    raise RuntimeError(f"release not found: {release_id}")
                release_data_type = str(release_row[0] or release_data_type or "building_info").strip().lower()
                release_is_active = bool(release_row[1])

            if store_mode == "pnu_kv":
                with conn.transaction():
                    update_release_status(conn, release_id, "IMPORTING")
                    if args.job_id is not None:
                        update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                        reset_job_worker_progress(conn, args.job_id, files)

                if args.truncate_release:
                    with conn.transaction():
                        delete_kv_release_rows(conn, release_id, release_data_type)
                        # Legacy table cleanup (not used in pnu_kv mode).
                        conn.execute("DELETE FROM building_info_lookup WHERE release_id = %s", (release_id,))

                import_fn = _import_update_file_pnu_kv if is_update_mode else _import_full_file_pnu_kv
                if parallel_full_mode:
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
                                release_data_type,
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
                            scanned_lines_total += int(result.get("scanned_lines") or 0)
                            blank_skips_total += int(result.get("blank_skips") or 0)
                            pnu_skips_total += int(result.get("pnu_skips") or 0)
                            delete_skips_total += int(result.get("delete_skips") or 0)
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
                                release_data_type,
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
                                release_data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
                            )
                        inserted_total += int(result.get("inserted_rows") or 0)
                        scanned_lines_total += int(result.get("scanned_lines") or 0)
                        blank_skips_total += int(result.get("blank_skips") or 0)
                        pnu_skips_total += int(result.get("pnu_skips") or 0)
                        delete_skips_total += int(result.get("delete_skips") or 0)
                        processed_files += 1
                        print(
                            f"[OK] {result.get('file', path.name)} 처리 완료 (누적 rows: {inserted_total})",
                            flush=True,
                        )

                expected_insert_total = max(
                    0,
                    int(scanned_lines_total)
                    - int(blank_skips_total)
                    - int(pnu_skips_total)
                    - int(delete_skips_total),
                )
                print(
                    "[CHECK] 무결성 요약(pnu_kv): "
                    f"scanned={scanned_lines_total}, "
                    f"blank_skip={blank_skips_total}, "
                    f"pnu_skip={pnu_skips_total}, "
                    f"delete_skip={delete_skips_total}, "
                    f"expected_insert={expected_insert_total}, "
                    f"inserted={inserted_total}",
                    flush=True,
                )
                if expected_insert_total == inserted_total:
                    print("[CHECK] insert count 검증: OK", flush=True)
                else:
                    print("[WARN] insert count 검증: MISMATCH", flush=True)

                if split_meta_entries:
                    source_line_count = sum(
                        int(item.get("source_line_count") or 0) for item in split_meta_entries
                    )
                    shard_line_total = sum(
                        sum(int(count) for count in (item.get("shard_line_counts") or []))
                        for item in split_meta_entries
                    )
                    split_status = "OK" if source_line_count == shard_line_total else "MISMATCH"
                    print(
                        "[CHECK] 분할 무결성: "
                        f"sources={len(split_meta_entries)}, "
                        f"source_lines={source_line_count}, "
                        f"shard_lines_total={shard_line_total}, "
                        f"status={split_status}",
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
                        (release_id, release_data_type),
                    )
                    row = cur.fetchone()
                    final_keys = int(row[0] or 0) if row else 0

                with conn.transaction():
                    if args.activate_on_complete:
                        _activate_release(conn, release_id, release_data_type, final_keys)
                    else:
                        final_status = (
                            "ACTIVE"
                            if (args.mark_ready and release_is_active)
                            else ("READY" if args.mark_ready else "IMPORTING")
                        )
                        update_release_status(conn, release_id, final_status, records_count=final_keys)

                    if args.job_id is not None:
                        update_job(
                            conn,
                            args.job_id,
                            status="SUCCEEDED",
                            inserted_rows=inserted_total,
                            error_message="",
                        )

                print(
                    f"완료(pnu_kv): release_id={release_id}, files={processed_files}, inserted_rows={inserted_total}, keys={final_keys}",
                    flush=True,
                )
                return 0

            if store_mode == "line_table":
                with conn.transaction():
                    update_release_status(conn, release_id, "IMPORTING")
                    if args.job_id is not None:
                        update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                        reset_job_worker_progress(conn, args.job_id, files)

                if args.truncate_release:
                    with conn.transaction():
                        # Clear all possible storages for safety (only one is expected to be used).
                        conn.execute("DELETE FROM building_info_line WHERE release_id = %s", (release_id,))
                        conn.execute("DELETE FROM building_info_lookup WHERE release_id = %s", (release_id,))
                        conn.execute("DELETE FROM dataset_record WHERE release_id = %s", (release_id,))
                        delete_kv_release_rows(conn, release_id, release_data_type)

                if is_update_mode:
                    import_fn = _import_update_file_line_table
                elif scope_replace_mode:
                    import_fn = _import_full_file_line_table_scope_replace
                else:
                    import_fn = _import_full_file_line_table

                if parallel_full_mode:
                    print(
                        f"[INFO] full 병렬 적재 시작(line_table): workers={worker_count}, files={len(files)}, batch_size={batch_size}, executor=thread",
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
                                release_data_type,
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
                            scanned_lines_total += int(result.get("scanned_lines") or 0)
                            blank_skips_total += int(result.get("blank_skips") or 0)
                            pnu_skips_total += int(result.get("pnu_skips") or 0)
                            delete_skips_total += int(result.get("delete_skips") or 0)
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
                                release_data_type,
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
                                release_data_type,
                                path,
                                batch_size,
                                args.job_id,
                                progress_step,
                            )
                        inserted_total += int(result.get("inserted_rows") or 0)
                        scanned_lines_total += int(result.get("scanned_lines") or 0)
                        blank_skips_total += int(result.get("blank_skips") or 0)
                        pnu_skips_total += int(result.get("pnu_skips") or 0)
                        delete_skips_total += int(result.get("delete_skips") or 0)
                        processed_files += 1
                        print(
                            f"[OK] {result.get('file', path.name)} 처리 완료 (누적 rows: {inserted_total})",
                            flush=True,
                        )

                expected_insert_total = max(
                    0,
                    int(scanned_lines_total)
                    - int(blank_skips_total)
                    - int(pnu_skips_total)
                    - int(delete_skips_total),
                )
                print(
                    "[CHECK] 무결성 요약(line_table): "
                    f"scanned={scanned_lines_total}, "
                    f"blank_skip={blank_skips_total}, "
                    f"pnu_skip={pnu_skips_total}, "
                    f"delete_skip={delete_skips_total}, "
                    f"expected_insert={expected_insert_total}, "
                    f"inserted={inserted_total}",
                    flush=True,
                )
                if expected_insert_total == inserted_total:
                    print("[CHECK] insert count 검증: OK", flush=True)
                else:
                    print("[WARN] insert count 검증: MISMATCH", flush=True)

                release_records_count = inserted_total
                if scope_replace_mode:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) FROM building_info_line WHERE release_id = %s",
                            (release_id,),
                        )
                        row = cur.fetchone()
                        release_records_count = int(row[0] or 0) if row else 0

                with conn.transaction():
                    if args.activate_on_complete:
                        _activate_release(conn, release_id, release_data_type, release_records_count)
                    else:
                        final_status = (
                            "ACTIVE"
                            if (args.mark_ready and release_is_active)
                            else ("READY" if args.mark_ready else "IMPORTING")
                        )
                        update_release_status(conn, release_id, final_status, records_count=release_records_count)

                    if args.job_id is not None:
                        update_job(
                            conn,
                            args.job_id,
                            status="SUCCEEDED",
                            inserted_rows=inserted_total,
                            error_message="",
                        )

                print(
                    f"완료(line_table): release_id={release_id}, files={processed_files}, inserted_rows={inserted_total}, rows={release_records_count}",
                    flush=True,
                )
                return 0

            schema_registry = DatasetSchemaRegistry(conn, release_data_type)

            with conn.transaction():
                schema_registry.ensure_release_partition(release_id)
                update_release_status(conn, release_id, "IMPORTING")
                if args.job_id is not None:
                    update_job(conn, args.job_id, status="RUNNING", total_files=len(files))
                    reset_job_worker_progress(conn, args.job_id, files)

            if args.truncate_release:
                with conn.transaction():
                    conn.execute("DELETE FROM dataset_record WHERE release_id = %s", (release_id,))
                    conn.execute("DELETE FROM building_info_lookup WHERE release_id = %s", (release_id,))
            elif merge_by_pnu_mode:
                with conn.transaction():
                    _prepare_merge_temp_table(conn)

            if parallel_full_mode:
                print(
                    f"[INFO] full 병렬 적재 시작: workers={worker_count}, files={len(files)}, batch_size={batch_size}, executor=thread",
                    flush=True,
                )
                futures: dict[concurrent.futures.Future[dict[str, Any]], Path] = {}
                executor: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=worker_count
                )
                try:
                    for path in files:
                        future = executor.submit(
                            _import_full_file,
                            db_url,
                            release_id,
                            release_data_type,
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
                        scanned_lines_total += int(result.get("scanned_lines") or 0)
                        blank_skips_total += int(result.get("blank_skips") or 0)
                        pnu_skips_total += int(result.get("pnu_skips") or 0)
                        delete_skips_total += int(result.get("delete_skips") or 0)
                        processed_files += 1
                        print(
                            f"[OK] {result.get('file', path.name)} 처리 완료 (누적 rows: {inserted_total})",
                            flush=True,
                        )
                finally:
                    executor.shutdown(wait=True, cancel_futures=True)

            def flush_batch(
                dataset_rows: list[tuple[Any, ...]],
                lookup_rows: list[tuple[Any, ...]],
                category: str,
                dataset_code: str,
                pending_delete_pnus: set[str],
            ) -> None:
                nonlocal inserted_total
                nonlocal last_reported_rows

                if not dataset_rows and not lookup_rows and not pending_delete_pnus:
                    return

                with conn.transaction():
                    if scope_replace_mode:
                        delete_targets: list[str] = []
                        if pending_delete_pnus:
                            if merge_by_pnu_mode:
                                delete_targets = _register_new_scope_pnus(
                                    conn,
                                    category,
                                    sorted(pending_delete_pnus),
                                )
                            else:
                                delete_targets = sorted(set(pending_delete_pnus))
                        if delete_targets:
                            conn.execute(
                                """
                                DELETE FROM dataset_record
                                WHERE release_id = %s
                                  AND data_type = %s
                                  AND dataset_code = %s
                                  AND pnu = ANY(%s)
                                """,
                                (release_id, release_data_type, dataset_code, delete_targets),
                            )
                            conn.execute(
                                """
                                DELETE FROM building_info_lookup
                                WHERE release_id = %s
                                  AND dataset_code = %s
                                  AND pnu = ANY(%s)
                                """,
                                (release_id, dataset_code, delete_targets),
                            )
                        pending_delete_pnus.clear()

                        batch_pnus = sorted({str(row[5]) for row in dataset_rows if row[5]})
                        if batch_pnus:
                            if merge_by_pnu_mode:
                                upsert_pnus = _register_new_scope_pnus(conn, category, batch_pnus)
                            else:
                                upsert_pnus = batch_pnus
                            if upsert_pnus:
                                conn.execute(
                                    """
                                    DELETE FROM dataset_record
                                    WHERE release_id = %s
                                      AND data_type = %s
                                      AND dataset_code = %s
                                      AND pnu = ANY(%s)
                                    """,
                                    (release_id, release_data_type, dataset_code, upsert_pnus),
                                )
                                conn.execute(
                                    """
                                    DELETE FROM building_info_lookup
                                    WHERE release_id = %s
                                      AND dataset_code = %s
                                      AND pnu = ANY(%s)
                                    """,
                                    (release_id, dataset_code, upsert_pnus),
                                )
                    else:
                        pending_delete_pnus.clear()

                    if dataset_rows:
                        insert_dataset_batch(conn, dataset_rows)
                    if lookup_rows:
                        insert_lookup_batch(conn, lookup_rows)

                inserted_total += len(dataset_rows)
                dataset_rows.clear()
                lookup_rows.clear()
                if args.job_id is not None and inserted_total - last_reported_rows >= progress_step:
                    with conn.transaction():
                        update_job(
                            conn,
                            args.job_id,
                            processed_files=processed_files,
                            inserted_rows=inserted_total,
                        )
                    last_reported_rows = inserted_total

            if not parallel_full_mode:
                for path in files:
                    category = detect_category(path)
                    dataset_rows: list[tuple[Any, ...]] = []
                    lookup_rows: list[tuple[Any, ...]] = []
                    pending_delete_pnus: set[str] = set()
                    dataset_code = _dataset_code_from_category(category)
                    file_rows_before = inserted_total
                    with conn.transaction():
                        mark_worker_running(conn, args.job_id, path.name, "single-runner")

                    with path.open("rb") as fp:
                        for line_no, raw in enumerate(fp, start=1):
                            scanned_lines_total += 1
                            line = decode_line(raw).strip()
                            if not line:
                                blank_skips_total += 1
                                continue
                            parts = line.split("|")
                            pnu = build_pnu(parts, category)
                            if not pnu:
                                pnu_skips_total += 1
                                continue
                            payload = _build_payload(parts, category)
                            if is_update_mode:
                                op = _parse_operation(payload, op_columns)
                                if op == "delete":
                                    delete_skips_total += 1
                                    pending_delete_pnus.add(pnu)
                                    if len(pending_delete_pnus) >= batch_size:
                                        flush_batch(dataset_rows, lookup_rows, category, dataset_code, pending_delete_pnus)
                                    continue
                            schema_id, payload_values = schema_registry.encode_payload(dataset_code, payload)
                            dataset_rows.append(
                                (
                                    release_id,
                                    release_data_type,
                                    dataset_code,
                                    path.name,
                                    line_no,
                                    pnu,
                                    schema_id,
                                    payload_values,
                                    None,
                                    None,
                                )
                            )
                            lookup_row = _build_lookup_row(
                                release_id=release_id,
                                pnu=pnu,
                                dataset_code=dataset_code,
                                payload=payload,
                                source_file=path.name,
                                row_no=line_no,
                            )
                            if lookup_row is not None:
                                lookup_rows.append(lookup_row)
                            if len(dataset_rows) >= batch_size:
                                flush_batch(dataset_rows, lookup_rows, category, dataset_code, pending_delete_pnus)

                    if dataset_rows or lookup_rows or pending_delete_pnus:
                        flush_batch(dataset_rows, lookup_rows, category, dataset_code, pending_delete_pnus)

                    processed_files += 1
                    with conn.transaction():
                        if args.job_id is not None:
                            update_job(
                                conn,
                                args.job_id,
                                processed_files=processed_files,
                                inserted_rows=inserted_total,
                            )
                            file_rows_now = max(0, inserted_total - file_rows_before)
                            if file_rows_now > 0:
                                increment_worker_progress(
                                    conn,
                                    args.job_id,
                                    path.name,
                                    processed_rows_delta=file_rows_now,
                                )
                            mark_worker_finished(conn, args.job_id, path.name, "SUCCEEDED")
                    print(f"[OK] {path.name} 처리 완료 (누적 rows: {inserted_total})", flush=True)

            expected_insert_total = max(
                0,
                int(scanned_lines_total) - int(blank_skips_total) - int(pnu_skips_total) - int(delete_skips_total),
            )
            print(
                "[CHECK] 무결성 요약: "
                f"scanned={scanned_lines_total}, "
                f"blank_skip={blank_skips_total}, "
                f"pnu_skip={pnu_skips_total}, "
                f"delete_skip={delete_skips_total}, "
                f"expected_insert={expected_insert_total}, "
                f"inserted={inserted_total}",
                flush=True,
            )
            if expected_insert_total == inserted_total:
                print("[CHECK] insert count 검증: OK", flush=True)
            else:
                print("[WARN] insert count 검증: MISMATCH", flush=True)

            if split_meta_entries:
                source_line_count = sum(int(item.get("source_line_count") or 0) for item in split_meta_entries)
                shard_line_total = sum(
                    sum(int(count) for count in (item.get("shard_line_counts") or []))
                    for item in split_meta_entries
                )
                split_status = "OK" if source_line_count == shard_line_total else "MISMATCH"
                print(
                    "[CHECK] 분할 무결성: "
                    f"sources={len(split_meta_entries)}, "
                    f"source_lines={source_line_count}, "
                    f"shard_lines_total={shard_line_total}, "
                    f"status={split_status}",
                    flush=True,
                )

            release_records_count = inserted_total
            if scope_replace_mode:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM dataset_record
                        WHERE release_id = %s
                          AND data_type = %s
                        """,
                        (release_id, release_data_type),
                    )
                    count_row = cur.fetchone()
                    release_records_count = int(count_row[0] or 0) if count_row else 0

            with conn.transaction():
                if args.activate_on_complete:
                    _activate_release(conn, release_id, release_data_type, release_records_count)
                else:
                    final_status = "ACTIVE" if (args.mark_ready and release_is_active) else ("READY" if args.mark_ready else "IMPORTING")
                    update_release_status(conn, release_id, final_status, records_count=release_records_count)

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

    print(f"완료: release_id={release_id}, files={processed_files}, inserted_rows={inserted_total}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
