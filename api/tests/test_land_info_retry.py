from __future__ import annotations

import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app import main


class LandInfoRetryApiTest(unittest.TestCase):
    token = "test-worker-token"
    worker_id = "test-worker"

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.direct_dir = Path(self.temp_dir.name)
        self.env = patch.dict(
            os.environ,
            {
                "LAND_INFO_DIRECT_WORKER_DIR": str(self.direct_dir),
                "LAND_INFO_WORKER_TOKEN": self.token,
            },
        )
        self.env.start()

    def tearDown(self) -> None:
        self.env.stop()
        self.temp_dir.cleanup()

    @property
    def headers(self) -> dict[str, str]:
        return {
            "X-Worker-Id": self.worker_id,
            "X-Worker-Token": self.token,
        }

    def _parent_data(self, *, status: str = "completed_with_failures") -> dict[str, object]:
        return {
            "request_id": "land_info_update_54d02241715e6e65",
            "created_at": "2026-08-23T13:31:37+09:00",
            "updated_at": "2026-08-23T13:31:43+09:00",
            "status": status,
            "data_type": "land_info",
            "operation_mode": "full",
            "source": "vworld",
            "source_signature": "2cd37979719739f6b6ac09cc814f32ebd723fc6f5e0a771a7e52167531f12ba7",
            "changed_source_signature": "54d02241715e6e652fe6b99edd6a7ca542249f47bd73f4d242f6af687738ae88",
            "snapshot_key": "AL_D155=2026-08-09",
            "activate": True,
            "test_mode": False,
            "expected_count": 1,
            "items": [
                {
                    "file_id": "AL_D155_11_20260809",
                    "dataset_code": "AL_D155",
                    "ds_file_id": "123",
                    "file_no": "11",
                    "base_date": "2026-08-09",
                }
            ],
            "component_dataset_codes": ["AL_D155"],
            "component_data_types": ["land_info_al_d155"],
            "component_status": {"AL_D155": {"expected_count": 1}},
            "source_catalog": {"source": "vworld", "operation_mode": "full"},
            "created_by_worker": "original-worker",
            "claimed_by": "original-worker",
            "claimed_at": "2026-08-23T13:31:38+09:00",
            "file_statuses": {"AL_D155_11_20260809": {"status": "failed"}},
            "uploaded_count": 0,
            "failed_count": 1,
            "worker_completed_at": "2026-08-23T13:31:43+09:00",
        }

    def _write_request(self, data: dict[str, object]) -> Path:
        requests_dir = self.direct_dir / "requests"
        requests_dir.mkdir(parents=True, exist_ok=True)
        path = requests_dir / f"{data['request_id']}.json"
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _post_retry(self, request_id: str, body: dict[str, object] | None = None) -> dict[str, object]:
        return main.worker_land_info_retry_request(
            request_id,
            body or {},
            x_worker_id=self.worker_id,
            x_worker_token=self.token,
        )

    def test_retry_requires_worker_auth_and_preserves_parent_bytes(self) -> None:
        parent_path = self._write_request(self._parent_data())
        before = parent_path.read_bytes()

        with self.assertRaises(HTTPException) as raised:
            main.worker_land_info_retry_request(
                "land_info_update_54d02241715e6e65",
                {},
                x_worker_id=None,
                x_worker_token=None,
            )

        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(parent_path.read_bytes(), before)
        self.assertEqual(len(list((self.direct_dir / "requests").glob("*.json"))), 1)

    def test_retry_creates_worker_visible_request_and_is_idempotent(self) -> None:
        parent_path = self._write_request(self._parent_data())
        before = parent_path.read_bytes()

        first = self._post_retry(
            "land_info_update_54d02241715e6e65",
            {"reason": "operator requested retry"},
        )

        first_data = first["data"]
        self.assertTrue(first_data["created"])
        retry_id = first_data["request_id"]
        retry_path = self.direct_dir / "requests" / f"{retry_id}.json"
        retry = json.loads(retry_path.read_text(encoding="utf-8"))
        self.assertEqual(retry["status"], "requested")
        self.assertIs(retry["force_redownload"], True)
        self.assertEqual(retry["parent_request_id"], "land_info_update_54d02241715e6e65")
        self.assertEqual(retry["retry_root_request_id"], "land_info_update_54d02241715e6e65")
        self.assertEqual(retry["retry_seq"], 1)
        self.assertEqual(retry["retry_source_status"], "completed_with_failures")
        self.assertEqual(retry["created_by_worker"], self.worker_id)
        self.assertEqual(retry["retry_reason"], "operator requested retry")
        self.assertNotIn("claimed_by", retry)
        self.assertNotIn("file_statuses", retry)
        self.assertNotIn("failed_count", retry)
        self.assertEqual(parent_path.read_bytes(), before)

        next_response = main.worker_land_info_next_request(
            worker_id="",
            x_worker_id=self.worker_id,
            x_worker_token=self.token,
        )
        self.assertEqual(next_response["data"]["request_id"], retry_id)

        second = self._post_retry("land_info_update_54d02241715e6e65")
        self.assertFalse(second["data"]["created"])
        self.assertEqual(second["data"]["reason"], "existing_retry")
        self.assertEqual(second["data"]["request_id"], retry_id)
        self.assertEqual(len(list((self.direct_dir / "requests").glob("*.json"))), 2)
        self.assertEqual(parent_path.read_bytes(), before)

    def test_concurrent_retries_create_exactly_one_child(self) -> None:
        self._write_request(self._parent_data())

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda _: main._create_land_info_retry_request(
                        "land_info_update_54d02241715e6e65",
                        worker_id=self.worker_id,
                        retry_reason=None,
                    ),
                    range(8),
                )
            )

        self.assertEqual(sum(bool(result["created"]) for result in results), 1)
        self.assertEqual(len({str(result["request_id"]) for result in results}), 1)
        self.assertEqual(len(list((self.direct_dir / "requests").glob("*.json"))), 2)

    def test_retry_chain_requires_retrying_the_terminal_child(self) -> None:
        self._write_request(self._parent_data())
        first = self._post_retry("land_info_update_54d02241715e6e65")["data"]
        first_path = self.direct_dir / "requests" / f"{first['request_id']}.json"
        first_request = json.loads(first_path.read_text(encoding="utf-8"))
        first_request["status"] = "server_failed"
        first_path.write_text(json.dumps(first_request), encoding="utf-8")

        root_repeat = self._post_retry("land_info_update_54d02241715e6e65")["data"]
        self.assertFalse(root_repeat["created"])
        self.assertEqual(root_repeat["request_id"], first["request_id"])

        second = self._post_retry(str(first["request_id"]))["data"]
        self.assertTrue(second["created"])
        self.assertEqual(second["request"]["parent_request_id"], first["request_id"])
        self.assertEqual(second["request"]["retry_root_request_id"], "land_info_update_54d02241715e6e65")
        self.assertEqual(second["request"]["retry_seq"], 2)

    def test_chain_wide_runnable_retry_blocks_another_branch(self) -> None:
        self._write_request(self._parent_data())
        first = self._post_retry("land_info_update_54d02241715e6e65")["data"]
        sibling = self._parent_data(status="server_failed")
        sibling["request_id"] = "land_info_retry_terminal_sibling"
        sibling["parent_request_id"] = "older_retry_parent"
        sibling["retry_root_request_id"] = "land_info_update_54d02241715e6e65"
        sibling["retry_seq"] = 9
        self._write_request(sibling)

        response = self._post_retry("land_info_retry_terminal_sibling")

        data = response["data"]
        self.assertFalse(data["created"])
        self.assertEqual(data["reason"], "existing_runnable_retry")
        self.assertEqual(data["request_id"], first["request_id"])

    def test_retry_validates_status_path_and_body(self) -> None:
        self._write_request(self._parent_data(status="requested"))

        with self.assertRaises(HTTPException) as status_error:
            self._post_retry("land_info_update_54d02241715e6e65")
        self.assertEqual(status_error.exception.status_code, 409)
        with self.assertRaises(HTTPException) as body_error:
            self._post_retry("land_info_update_54d02241715e6e65", {"force_redownload": True})
        self.assertEqual(body_error.exception.status_code, 400)
        with self.assertRaises(HTTPException) as path_error:
            self._post_retry("land info update")
        self.assertEqual(path_error.exception.status_code, 400)

    def test_existing_ensure_contract_still_creates_and_reuses_request(self) -> None:
        item = {
            "file_id": "AL_D155_11_20260809",
            "dataset_code": "AL_D155",
            "ds_file_id": "123",
            "file_no": "11",
            "base_date": "2026-08-09",
        }
        body = {
            "hostname": "test-host",
            "version": "test-version",
            "source_catalog": {
                "source": "vworld",
                "operation_mode": "full",
                "items": [item],
            },
        }
        with (
            patch.object(main, "_active_land_info_release_metadata", return_value=None),
            patch.object(main, "_active_land_info_component_releases", return_value={}),
        ):
            first = main.worker_land_info_ensure_update(
                body,
                x_worker_id=self.worker_id,
                x_worker_token=self.token,
            )
            second = main.worker_land_info_ensure_update(
                body,
                x_worker_id=self.worker_id,
                x_worker_token=self.token,
            )

        self.assertTrue(first["data"]["created"])
        self.assertFalse(second["data"]["created"])
        self.assertEqual(second["data"]["reason"], "existing_requested")
        self.assertEqual(first["data"]["request_id"], second["data"]["request_id"])


if __name__ == "__main__":
    unittest.main()
