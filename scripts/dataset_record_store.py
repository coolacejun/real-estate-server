#!/usr/bin/env python3
from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
from typing import Any, Callable

import psycopg

JsonDefault = Callable[[Any], Any]


class DatasetSchemaRegistry:
    def __init__(self, conn: psycopg.Connection, data_type: str) -> None:
        self._conn = conn
        self._data_type = str(data_type or "").strip().lower()
        self._data_type_suffix = self._normalize_data_type_suffix(self._data_type)
        self._cache: dict[tuple[str, str], int] = {}
        self._compress_payload_values = self._parse_bool_option(
            "DATASET_PAYLOAD_VALUES_COMPRESS",
            True,
        )
        self._compress_min_bytes = self._parse_positive_int_option(
            "DATASET_PAYLOAD_VALUES_COMPRESS_MIN_BYTES",
            256,
        )
        self._compress_level = max(
            1,
            min(
                9,
                self._parse_positive_int_option(
                    "DATASET_PAYLOAD_VALUES_COMPRESS_LEVEL",
                    6,
                ),
            ),
        )
        self._compress_geometry = self._parse_bool_option(
            "DATASET_GEOMETRY_COMPRESS",
            True,
        )
        self._compress_geometry_min_bytes = self._parse_positive_int_option(
            "DATASET_GEOMETRY_COMPRESS_MIN_BYTES",
            512,
        )
        self._compress_geometry_level = max(
            1,
            min(
                9,
                self._parse_positive_int_option(
                    "DATASET_GEOMETRY_COMPRESS_LEVEL",
                    6,
                ),
            ),
        )

    def ensure_release_partition(self, release_id: int) -> None:
        self._conn.execute(
            "SELECT ensure_dataset_record_partition(%s)",
            (int(release_id),),
        )

    def schema_id(self, dataset_code: str, columns: list[str]) -> int:
        normalized_code = str(dataset_code or "").strip()
        normalized_columns = [str(col) for col in columns]
        schema_hash = hashlib.sha1(
            "\x1f".join(normalized_columns).encode("utf-8")
        ).hexdigest()
        cache_key = (normalized_code, schema_hash)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM dataset_schema
                WHERE data_type = %s
                  AND dataset_code = %s
                  AND schema_hash = %s
                LIMIT 1
                """,
                (self._data_type, normalized_code, schema_hash),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                schema_id = int(row[0])
                self._cache[cache_key] = schema_id
                return schema_id

            cur.execute(
                """
                INSERT INTO dataset_schema (data_type, dataset_code, schema_hash, columns)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (data_type, dataset_code, schema_hash)
                DO UPDATE SET columns = EXCLUDED.columns
                RETURNING id
                """,
                (self._data_type, normalized_code, schema_hash, normalized_columns),
            )
            created = cur.fetchone()
            if not created or created[0] is None:
                raise RuntimeError("failed to resolve dataset schema id")
            schema_id = int(created[0])
            self._cache[cache_key] = schema_id
            return schema_id

    def encode_payload(
        self,
        dataset_code: str,
        payload: dict[str, Any],
        *,
        json_default: JsonDefault | None = None,
    ) -> tuple[int, str]:
        columns = list(payload.keys())
        schema_id = self.schema_id(dataset_code, columns)
        values = [payload.get(column) for column in columns]
        kwargs: dict[str, Any] = {"ensure_ascii": False, "separators": (",", ":")}
        if json_default is not None:
            kwargs["default"] = json_default
        json_values = json.dumps(values, **kwargs)
        if not self._compress_payload_values:
            return schema_id, json_values

        raw = json_values.encode("utf-8", "ignore")
        if len(raw) < self._compress_min_bytes:
            return schema_id, json_values

        compressed = gzip.compress(raw, compresslevel=self._compress_level)
        if len(compressed) >= len(raw):
            return schema_id, json_values

        marker = "gz:" + base64.b64encode(compressed).decode("ascii")
        return schema_id, json.dumps(marker, ensure_ascii=False)

    def encode_geometry(
        self,
        geometry: dict[str, Any] | None,
        *,
        json_default: JsonDefault | None = None,
    ) -> str | None:
        if not geometry:
            return None
        kwargs: dict[str, Any] = {"ensure_ascii": False, "separators": (",", ":")}
        if json_default is not None:
            kwargs["default"] = json_default
        json_geometry = json.dumps(geometry, **kwargs)
        if not self._compress_geometry:
            return json_geometry

        raw = json_geometry.encode("utf-8", "ignore")
        if len(raw) < self._compress_geometry_min_bytes:
            return json_geometry

        compressed = gzip.compress(raw, compresslevel=self._compress_geometry_level)
        if len(compressed) >= len(raw):
            return json_geometry

        marker = "gz:" + base64.b64encode(compressed).decode("ascii")
        return json.dumps(marker, ensure_ascii=False)

    def _option(self, name: str, default: str) -> str:
        if self._data_type_suffix:
            raw = str(os.getenv(f"{name}_{self._data_type_suffix}", "") or "").strip()
            if raw:
                return raw
        raw = str(os.getenv(name, "") or "").strip()
        if raw:
            return raw
        return default

    def _parse_bool_option(self, name: str, default: bool) -> bool:
        raw = self._option(name, "1" if default else "0").strip().lower()
        return raw not in {"0", "false", "off", "no"}

    def _parse_positive_int_option(self, name: str, default: int) -> int:
        raw = self._option(name, "").strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except Exception:
            return default
        return value if value > 0 else default

    @staticmethod
    def _normalize_data_type_suffix(data_type: str) -> str:
        text = str(data_type or "").strip().upper()
        if not text:
            return ""
        chars = [ch if ch.isalnum() else "_" for ch in text]
        normalized = "".join(chars).strip("_")
        while "__" in normalized:
            normalized = normalized.replace("__", "_")
        return normalized
