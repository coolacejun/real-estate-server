from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from threading import BoundedSemaphore, Event
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.platform import environment, routes
from app.platform.body_limit import RequestBodyLimitMiddleware
from app.platform.config import PlatformSettings
from app.platform.reports import (
    _renderer_report_payload, materialize_assets, render_pdf,
    validate_canonical_report,
)


FIXTURE = Path(__file__).resolve().parents[1] / "contracts/golden/web-a4-canonical-v3.json"


def v3_report() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["report"]


class CanonicalV3Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.settings = replace(PlatformSettings.from_env(), report_asset_dir=Path(self.temp.name))

    def test_normalized_hash_and_all_profiles_preserve_semantic_versions(self):
        fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        for version in ("web-a4-canonical-v2", "web-a4-canonical-v3"):
            for profile in (None, "web-a4-v1", "ios-a4-v1", "android-a4-v1"):
                with self.subTest(version=version, profile=profile):
                    report = v3_report()
                    report["rendererVersion"] = version
                    canonical = validate_canonical_report(report, requested_profile=profile)
                    self.assertEqual(canonical.response_renderer_version, version)
                    self.assertEqual(canonical.report["pages"], report["pages"])
                    self.assertEqual(canonical.renderer_profile, profile or "web-a4-v1")
                    if version.endswith("v3"):
                        self.assertEqual(canonical.content_hash, fixture["expected"]["contentHash"])

    def test_final_and_preview_send_identical_inline_content_including_environment_photo(self):
        canonical = validate_canonical_report(v3_report())
        preview = _renderer_report_payload(self.settings, canonical, None)
        stored, assets = materialize_assets(self.settings, user_id="fixture-owner", report=canonical.report)
        archive = replace(canonical, report=stored)
        final = _renderer_report_payload(self.settings, archive, [asset.as_json() for asset in assets])
        self.assertEqual(preview, final)
        self.assertNotIn("assetBundle", final)
        self.assertNotIn("asset://", json.dumps(final))
        self.assertEqual(final["contentHash"], canonical.content_hash)
        self.assertEqual(len(assets), 3)
        photo = next(page for page in stored["pages"] if page["pageKey"] == "environment")["environmentPhoto"]
        self.assertTrue(photo["src"].startswith("asset://"))
        self.assertNotIn("data:image", json.dumps(stored))

    def test_archive_tampering_and_missing_manifest_fail_before_renderer(self):
        canonical = validate_canonical_report(v3_report())
        stored, assets = materialize_assets(self.settings, user_id="owner", report=canonical.report)
        archive = replace(canonical, report=stored)
        with self.assertRaises(HTTPException) as missing:
            _renderer_report_payload(self.settings, archive, [])
        self.assertEqual(missing.exception.status_code, 410)
        manifest = [asset.as_json() for asset in assets]
        (self.settings.report_asset_dir / assets[0].storage_key).write_bytes(b"corrupt")
        with patch("app.platform.reports._request_renderer_pdf") as request:
            with self.assertRaises(HTTPException) as corrupt:
                render_pdf(self.settings, archive, asset_manifest=manifest)
        self.assertEqual(corrupt.exception.status_code, 410)
        request.assert_not_called()

    def test_semantic_contract_rejects_unsafe_data_limits_and_hash_mismatch(self):
        reports = []
        for key in ("html", "editedHtml", "script", "isArchivedSnapshot", "__proto__"):
            report = v3_report()
            report["pages"][1][key] = "untrusted"
            reports.append(report)
        for version in (True, "1", 2):
            report = v3_report()
            report["schemaVersion"] = version
            reports.append(report)
        for number in (float("nan"), float("inf")):
            report = v3_report()
            report["pages"][1]["sourceRows"] = {"bad": number}
            reports.append(report)
        duplicate = v3_report()
        duplicate["pages"][1]["pageKey"] = "cover"
        reports.append(duplicate)
        too_many = v3_report()
        too_many["pages"] *= 7
        reports.append(too_many)
        unknown = v3_report()
        unknown["rendererVersion"] = "web-a4-canonical-v99"
        reports.append(unknown)
        for report in reports:
            with self.subTest(report=report["rendererVersion"]), self.assertRaises(HTTPException):
                validate_canonical_report(report, requested_profile="web-a4-v1")
        with self.assertRaises(HTTPException):
            validate_canonical_report(v3_report(), expected_content_hash="f" * 64)

    def test_remote_environment_photo_and_asset_count_are_bounded(self):
        report = v3_report()
        photo = next(page for page in report["pages"] if page["pageKey"] == "environment")["environmentPhoto"]
        photo["src"] = "http://169.254.169.254/metadata"
        with patch("app.platform.reports._request_renderer_pdf") as request:
            with self.assertRaises(HTTPException):
                render_pdf(self.settings, validate_canonical_report(report))
        request.assert_not_called()
        with self.assertRaises(HTTPException) as limited:
            _renderer_report_payload(replace(self.settings, report_asset_max_count=2),
                                     validate_canonical_report(v3_report()), None)
        self.assertEqual(limited.exception.status_code, 413)

    def test_preview_is_stateless_and_validates_hash(self):
        app = FastAPI()
        app.include_router(routes.router)
        with TestClient(app) as client, patch.object(routes, "_settings", return_value=self.settings), \
             patch.object(routes, "_rate_limit"), patch.object(routes, "_session", return_value={"user_id": "owner"}), \
             patch.object(routes, "render_pdf", return_value=b"%PDF-test"), \
             patch.object(routes, "begin_final_usage") as begin, \
             patch.object(routes, "complete_final_usage") as complete, \
             patch.object(routes, "materialize_assets") as materialize:
            response = client.post("/api/mobile/v1/reports/preview", json={"report": v3_report()})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["x-report-renderer-version"], "web-a4-canonical-v3")
            self.assertEqual(response.headers["x-report-artifact-sha256"], hashlib.sha256(response.content).hexdigest())
            mismatch = client.post("/api/mobile/v1/reports/preview", json={"report": v3_report(), "contentHash": "f" * 64})
            self.assertEqual(mismatch.status_code, 422)
            begin.assert_not_called()
            complete.assert_not_called()
            materialize.assert_not_called()


class EnvironmentSafetyTest(unittest.TestCase):
    def test_public_endpoint_defaults_disabled_and_enforces_body_limit(self):
        with patch.dict(os.environ, {"ENVIRONMENT_ANALYSIS_ENABLED": "false"}):
            settings = PlatformSettings.from_env()
        self.assertFalse(settings.environment_analysis_enabled)
        app = FastAPI()
        app.add_middleware(RequestBodyLimitMiddleware)
        app.include_router(routes.router)
        with TestClient(app) as client, patch.object(routes, "_settings", return_value=settings), \
             patch.object(routes, "analyze_environment") as analyze:
            self.assertEqual(client.post("/api/v1/environment-analysis", json={}).status_code, 503)
            self.assertEqual(client.post("/api/v1/environment-analysis", content=b" " * 16385).status_code, 413)
            analyze.assert_not_called()

    def test_active_analyses_are_bounded(self):
        with patch.object(environment, "_analysis_slots", BoundedSemaphore(0)), \
             patch.object(environment, "_analyze_environment") as analyze:
            with self.assertRaises(HTTPException) as caught:
                environment.analyze_environment(None, {})
            self.assertEqual(caught.exception.status_code, 503)
            analyze.assert_not_called()

    def test_timed_out_jobs_keep_slots_until_reads_finish(self):
        release = Event()
        started = Event()
        slots = BoundedSemaphore(1)

        def slow_read():
            started.set()
            release.wait(5)

        with patch.object(environment, "_category_slots", slots):
            future = environment._submit_category(slow_read)
            try:
                self.assertTrue(started.wait(2))
                self.assertFalse(future.cancel())
                with self.assertRaises(HTTPException):
                    environment._submit_category(lambda: None)
            finally:
                release.set()
                future.result(timeout=2)
            # Callback may finish just after result(); acquiring with a deadline
            # proves the slot is eventually released without timing sleeps.
            self.assertTrue(slots.acquire(timeout=2))
            slots.release()

    def test_slow_category_returns_partial_result_without_late_mutation(self):
        from test_environment_analysis import EnvironmentAnalysisTest

        fixtures = EnvironmentAnalysisTest()
        release = Event()
        finished = Event()
        original_load = environment._load

        def slow_park(path):
            if path.parent.name == "parks":
                release.wait(5)
                try:
                    return original_load(path)
                finally:
                    finished.set()
            return original_load(path)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixtures._fixture(root)
            settings = replace(PlatformSettings.from_env(), environment_data_dir=root)
            with patch.object(environment, "_load", side_effect=slow_park), \
                 patch.object(environment, "CATEGORY_TIMEOUT_SECONDS", 0.25):
                try:
                    result = environment.analyze_environment(settings, fixtures._request())
                    snapshot = copy.deepcopy(result)
                    self.assertTrue(result["partial"])
                    self.assertIn({"category": "parks", "code": "dataset_timeout", "retryable": True}, result["errors"])
                finally:
                    release.set()
                self.assertTrue(finished.wait(2))
                self.assertEqual(result, snapshot)


if __name__ == "__main__":
    unittest.main()
