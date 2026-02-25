#!/usr/bin/env python3
from __future__ import annotations

import json
from typing import Any

import psycopg
from decimal import Decimal


def _json_dumps(obj: Any) -> str:
    # Compact JSON to reduce DB size; UTF-8 for Korean text.
    def _default(value: Any) -> Any:
        if isinstance(value, Decimal):
            if value == value.to_integral_value():
                return int(value)
            return float(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=_default)


def delete_release_rows(conn: psycopg.Connection, release_id: int, data_type: str) -> None:
    conn.execute(
        """
        DELETE FROM dataset_pnu_kv
        WHERE release_id = %s
          AND data_type = %s
        """,
        (int(release_id), str(data_type or "").strip().lower()),
    )


def upsert_array_payloads(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
    records_by_pnu: dict[str, list[dict[str, Any]]],
) -> None:
    """Upsert PNU KV where payload is a JSON array. Arrays are appended on conflict."""
    if not records_by_pnu:
        return
    rows: list[tuple[Any, ...]] = []
    normalized_type = str(data_type or "").strip().lower()
    for pnu, records in records_by_pnu.items():
        if not pnu or not records:
            continue
        rows.append((int(release_id), normalized_type, str(pnu), _json_dumps(records)))
    # Sort by PNU to enforce a consistent lock ordering across concurrent upserts.
    # This reduces the chance of deadlocks when multiple workers upsert overlapping PNUs.
    rows.sort(key=lambda row: row[2])
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dataset_pnu_kv (release_id, data_type, pnu, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (release_id, data_type, pnu)
            DO UPDATE SET payload = dataset_pnu_kv.payload || EXCLUDED.payload
            """,
            rows,
        )


def upsert_building_info_payloads(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
    payload_by_pnu: dict[str, dict[str, Any]],
) -> None:
    """Upsert building_info KV payloads.

    Expected payload keys (all optional):
    - total/single/floor/room: list[str]
    - violation_by_key: dict[str, str]
    - is_violation_by_key: dict[str, bool]
    """
    if not payload_by_pnu:
        return
    rows: list[tuple[Any, ...]] = []
    normalized_type = str(data_type or "").strip().lower()
    for pnu, payload in payload_by_pnu.items():
        if not pnu or not payload:
            continue
        rows.append((int(release_id), normalized_type, str(pnu), _json_dumps(payload)))
    # Sort by PNU to enforce a consistent lock ordering across concurrent upserts.
    rows.sort(key=lambda row: row[2])
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dataset_pnu_kv (release_id, data_type, pnu, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (release_id, data_type, pnu)
            DO UPDATE SET payload = jsonb_strip_nulls(
              jsonb_build_object(
                'total',
                  CASE
                    WHEN (dataset_pnu_kv.payload ? 'total') OR (EXCLUDED.payload ? 'total') THEN
                      COALESCE(dataset_pnu_kv.payload->'total', '[]'::jsonb) || COALESCE(EXCLUDED.payload->'total', '[]'::jsonb)
                    ELSE NULL
                  END,
                'single',
                  CASE
                    WHEN (dataset_pnu_kv.payload ? 'single') OR (EXCLUDED.payload ? 'single') THEN
                      COALESCE(dataset_pnu_kv.payload->'single', '[]'::jsonb) || COALESCE(EXCLUDED.payload->'single', '[]'::jsonb)
                    ELSE NULL
                  END,
                'floor',
                  CASE
                    WHEN (dataset_pnu_kv.payload ? 'floor') OR (EXCLUDED.payload ? 'floor') THEN
                      COALESCE(dataset_pnu_kv.payload->'floor', '[]'::jsonb) || COALESCE(EXCLUDED.payload->'floor', '[]'::jsonb)
                    ELSE NULL
                  END,
                'room',
                  CASE
                    WHEN (dataset_pnu_kv.payload ? 'room') OR (EXCLUDED.payload ? 'room') THEN
                      COALESCE(dataset_pnu_kv.payload->'room', '[]'::jsonb) || COALESCE(EXCLUDED.payload->'room', '[]'::jsonb)
                    ELSE NULL
                  END,
                'violation_by_key',
                  CASE
                    WHEN (dataset_pnu_kv.payload ? 'violation_by_key') OR (EXCLUDED.payload ? 'violation_by_key') THEN
                      COALESCE(dataset_pnu_kv.payload->'violation_by_key', '{}'::jsonb) || COALESCE(EXCLUDED.payload->'violation_by_key', '{}'::jsonb)
                    ELSE NULL
                  END,
                'is_violation_by_key',
                  CASE
                    WHEN (dataset_pnu_kv.payload ? 'is_violation_by_key') OR (EXCLUDED.payload ? 'is_violation_by_key') THEN
                      COALESCE(dataset_pnu_kv.payload->'is_violation_by_key', '{}'::jsonb) || COALESCE(EXCLUDED.payload->'is_violation_by_key', '{}'::jsonb)
                    ELSE NULL
                  END
              )
            )
            """,
            rows,
        )


def clear_array_dataset_code(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
    dataset_code: str,
    pnus: list[str] | set[str] | tuple[str, ...],
    delete_empty_rows: bool = True,
) -> None:
    """Remove dataset_code entries from JSON array payloads for given PNUs.

    This is used to support update mode for pnu_kv storage:
    - clear existing dataset_code rows once per PNU, then append new rows.
    - apply delete operations by clearing dataset_code rows for the PNU.
    """
    normalized_type = str(data_type or "").strip().lower()
    normalized_code = str(dataset_code or "").strip()
    if not normalized_type or not normalized_code:
        return
    safe_pnus = sorted({str(pnu).strip() for pnu in pnus or [] if str(pnu).strip()})
    if not safe_pnus:
        return

    conn.execute(
        """
        UPDATE dataset_pnu_kv
        SET payload = COALESCE(
          (
            SELECT jsonb_agg(elem)
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(dataset_pnu_kv.payload) = 'array' THEN dataset_pnu_kv.payload
                ELSE '[]'::jsonb
              END
            ) AS elem
            WHERE COALESCE(elem->>'dataset_code', '') <> %s
          ),
          '[]'::jsonb
        )
        WHERE release_id = %s
          AND data_type = %s
          AND pnu = ANY(%s)
        """,
        (normalized_code, int(release_id), normalized_type, safe_pnus),
    )

    if delete_empty_rows:
        conn.execute(
            """
            DELETE FROM dataset_pnu_kv
            WHERE release_id = %s
              AND data_type = %s
              AND pnu = ANY(%s)
              AND payload = '[]'::jsonb
            """,
            (int(release_id), normalized_type, safe_pnus),
        )


def clear_building_info_bucket(
    conn: psycopg.Connection,
    *,
    release_id: int,
    data_type: str,
    bucket: str,
    pnus: list[str] | set[str] | tuple[str, ...],
) -> None:
    """Remove a building_info bucket key (total/single/floor/room) for given PNUs."""
    normalized_type = str(data_type or "").strip().lower()
    normalized_bucket = str(bucket or "").strip()
    if not normalized_type or not normalized_bucket:
        return
    safe_pnus = sorted({str(pnu).strip() for pnu in pnus or [] if str(pnu).strip()})
    if not safe_pnus:
        return
    conn.execute(
        """
        UPDATE dataset_pnu_kv
        SET payload = jsonb_strip_nulls(
          (
            CASE
              WHEN jsonb_typeof(dataset_pnu_kv.payload) = 'object' THEN dataset_pnu_kv.payload
              ELSE '{}'::jsonb
            END
          ) - %s
        )
        WHERE release_id = %s
          AND data_type = %s
          AND pnu = ANY(%s)
        """,
        (normalized_bucket, int(release_id), normalized_type, safe_pnus),
    )
