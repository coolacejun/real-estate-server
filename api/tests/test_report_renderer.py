from __future__ import annotations

import base64
import json
import tempfile
import unittest
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from PIL import Image

from app.platform.config import PlatformSettings
from app.platform.reports import (
    RENDERER_PROFILES,
    _broker_sections,
    _floor_rows,
    _page_image_references,
    _renderer_endpoint,
    _render_reportlab_pdf,
    materialize_assets,
    render_pdf,
    validate_canonical_report,
)


FAKE_CHROMIUM_PDF = (
    b"%PDF-1.7\n/Producer (Skia/PDF m151)\n"
    + b"/Type /Page\n" * 120
    + b"0" * 30_000
    + b"\n%%EOF"
)


def _png_data_uri() -> str:
    buffer = BytesIO()
    Image.new("RGB", (320, 180), (44, 105, 141)).save(buffer, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode("ascii")


def sample_report(image_uri: str, *, profile: str = "android-a4-v1") -> dict[str, object]:
    floor_rows = [
        {
            "floor": f"지상 {index}층",
            "usage": "업무시설 및 근린생활시설",
            "area": f"{120 + index}.50",
            "structure": "철근콘크리트구조",
            "display": f"지상 {index}층 · 업무시설 · {120 + index}.50㎡ · 철근콘크리트구조",
        }
        for index in range(1, 61)
    ]
    broker_values = {
        "landArea": "432.10",
        "landCategory": "대",
        "useApprovalDate": "2020-06-15",
        "officialBuildingUse": "업무시설",
        "actualUse": "업무시설",
        "structure": "철근콘크리트구조",
        "direction": "남향(주출입구 기준)",
        "seismicApply": "적용됨",
        "seismicAbility": "VII-0.176g",
        "violationStatus": "해당 없음",
        "violationDetail": "건축물대장상 기재 없음",
        "useArea": "제2종일반주거지역",
        "useDistrict": "중요시설물보호지구",
        "buildingCoverageLimit": "60%",
        "floorAreaRatioLimit": "200%",
        "landTransactionPermit": False,
        "parkingType": "자주식",
        "parkingDetail": "총 12대",
        "bus": '{"name":"테스트 정류장","mode":"도보","minutes":4}',
        "rail": {"name": "테스트역", "mode": "도보", "minutes": 9},
        "elementarySchool": {"name": "테스트초등학교", "minutes": 7},
        "futureField": "기존 보관본의 미지 필드",
    }
    broker_rows = [
        [
            {"label": key, "value": value, "span": 3}
            for key, value in list(broker_values.items())[index : index + 2]
        ]
        for index in range(0, len(broker_values), 2)
    ]
    return {
        "schemaVersion": 1,
        "rendererVersion": profile,
        "mappingVersion": "mobile-canonical-v2",
        "title": "서울 테스트 부동산 종합 보고서",
        "address": "서울특별시 중구 세종대로 110",
        "includedItems": ["cover", "building", "land", "cadastre", "ai", "broker", "opinion"],
        "pages": [
            {
                "pageKey": "cover",
                "layout": "cover",
                "title": "부동산 종합 보고서",
                "address": "서울특별시 중구 세종대로 110",
                "placeName": "테스트 빌딩",
                "createdAt": "2026-09-04",
            },
            {
                "pageKey": "building:1",
                "reportType": "building",
                "layout": "property-report",
                "title": "테스트 빌딩 일반건축물",
                "address": "서울특별시 중구 세종대로 110",
                "reportRows": [
                    [
                        {"label": "구분", "value": "일반건축물", "span": 3},
                        {"label": "명칭", "value": "테스트 빌딩", "span": 3},
                    ],
                    [{"divider": True}],
                    *[
                        [{"label": "층별개요", "value": row["display"], "span": 3}]
                        for row in floor_rows
                    ],
                ],
                "sourceRows": {"registerKindName": "일반", "floors": floor_rows},
            },
            {
                "pageKey": "land",
                "reportType": "land",
                "layout": "property-report",
                "title": "토지정보",
                "address": "서울특별시 중구 세종대로 110",
                "reportRows": [
                    [
                        {"label": "지목", "value": "대", "span": 3},
                        {"label": "면적", "value": "432.10㎡", "span": 3},
                    ],
                    [{"label": "토지이용상황", "value": "업무용", "span": 6}],
                ],
            },
            {
                "pageKey": "cadastre",
                "reportType": "cadastre",
                "layout": "property-report",
                "title": "지적도",
                "address": "서울특별시 중구 세종대로 110",
                "reportRows": [[{"label": "번지", "value": "110", "span": 6}]],
                "mapImage": image_uri,
            },
            {
                "pageKey": "ai",
                "reportType": "ai",
                "layout": "property-report",
                "title": "주변환경분석",
                "address": "서울특별시 중구 세종대로 110",
                "reportRows": [[{"label": "대중교통", "value": "지하철역 도보 9분", "span": 6}]],
                "environmentDataNotice": "공공데이터와 지도 분석 결과이며 현장 확인이 필요합니다.",
            },
            {
                "pageKey": "broker:1",
                "reportType": "broker",
                "layout": "broker-disclosure",
                "title": "건물·토지 설명용",
                "address": "서울특별시 중구 세종대로 110",
                "brokerRows": broker_rows,
            },
            {
                "pageKey": "opinion",
                "layout": "opinion",
                "title": "설명 및 의견",
                "opinionText": "현장 확인 결과 채광이 양호합니다.\n계약 전 권리관계를 다시 확인하세요.",
                "opinionImages": [
                    {"id": "photo-1", "name": "현장 전경", "src": image_uri},
                    image_uri,
                ],
            },
            {
                "pageKey": "enforcement",
                "layout": "property-report",
                "title": "위반건축물 검토",
                "address": "서울특별시 중구 세종대로 110",
                "enforcementSnapshot": {
                    "isViolation": False,
                    "status": "검토 완료",
                    "description": "제공된 자료 기준으로 확인",
                    "futureCheck": "계약 전 최신 대장 재확인",
                },
            },
        ],
    }


class ReportRendererTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.settings = replace(
            PlatformSettings.from_env(),
            report_asset_dir=Path(self.temporary.name),
            report_font_path="/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        )
        self.image_uri = _png_data_uri()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_mobile_schema_drift_is_accepted_and_assets_are_materialized(self) -> None:
        report = sample_report(self.image_uri)
        canonical = validate_canonical_report(report)
        self.assertIsInstance(canonical.report["pages"][1]["sourceRows"], dict)
        self.assertIsInstance(canonical.report["pages"][6]["opinionImages"][0], dict)

        sanitized, assets = materialize_assets(self.settings, user_id="renderer-test", report=report)
        self.assertEqual(len(assets), 1)
        opinion = sanitized["pages"][6]["opinionImages"]
        self.assertTrue(opinion[0]["src"].startswith("asset://"))
        self.assertEqual(opinion[0]["src"], opinion[1])

    def test_adapters_preserve_structured_floors_broker_sections_and_photo_maps(self) -> None:
        report = sample_report(self.image_uri)
        building = report["pages"][1]
        broker = report["pages"][5]
        opinion = report["pages"][6]

        floors = _floor_rows(building)
        self.assertEqual(floors[0], ("지상 1층", "업무시설 및 근린생활시설", "121.50㎡ / 철근콘크리트구조"))
        self.assertEqual(len(floors), 60)
        fallback = _floor_rows(
            {
                "reportRows": [
                    [{"label": "층별개요", "value": "지상 2층 · 업무시설 · 98.2㎡ · 철근콘크리트구조", "span": 6}]
                ]
            }
        )
        self.assertEqual(fallback, [("지상 2층", "업무시설", "98.2㎡ · 철근콘크리트구조")])

        sections = _broker_sections(broker)
        rows = {label: value for _, section_rows in sections for label, value in section_rows}
        self.assertEqual(rows["면적(㎡)"], "432.10")
        self.assertEqual(rows["버스"], "테스트 정류장 (도보 4분)")
        self.assertEqual(rows["지하철"], "테스트역 (도보 9분)")
        self.assertEqual(rows["추가 정보 1"], "기존 보관본의 미지 필드")

        references = _page_image_references(opinion)
        self.assertEqual([label for label, _ in references], ["현장 전경", "의견 사진 2"])

    def test_all_renderer_profiles_build_long_table_pdf(self) -> None:
        with patch("app.platform.reports._request_renderer_pdf", return_value=FAKE_CHROMIUM_PDF) as request:
            for profile in RENDERER_PROFILES:
                with self.subTest(profile=profile):
                    report = sample_report(self.image_uri, profile=profile)
                    sanitized, assets = materialize_assets(
                        self.settings, user_id=f"renderer-{profile}", report=report
                    )
                    canonical = validate_canonical_report(sanitized)
                    rendered = render_pdf(
                        self.settings,
                        canonical,
                        asset_manifest=[asset.as_json() for asset in assets],
                    )
                    self.assertTrue(rendered.startswith(b"%PDF-"))
                    self.assertGreater(len(rendered), 20_000)
            self.assertEqual(request.call_count, len(RENDERER_PROFILES))

    def test_renderer_failure_never_falls_back_to_reportlab(self) -> None:
        canonical = validate_canonical_report(sample_report(self.image_uri))
        failure = HTTPException(status_code=503, detail="renderer unavailable")
        with patch("app.platform.reports._request_renderer_pdf", side_effect=failure), patch(
            "app.platform.reports._render_reportlab_pdf", wraps=_render_reportlab_pdf
        ) as legacy:
            with self.assertRaises(HTTPException) as caught:
                render_pdf(self.settings, canonical)
        self.assertIs(caught.exception, failure)
        legacy.assert_not_called()

    def test_renderer_endpoint_is_exactly_allowlisted(self) -> None:
        settings = replace(
            self.settings,
            report_renderer_url="http://169.254.169.254/api/internal/mobile-report-pdf",
            report_renderer_allowed_hosts=("web",),
        )
        with self.assertRaises(HTTPException) as caught:
            _renderer_endpoint(settings)
        self.assertEqual(caught.exception.status_code, 503)

    def test_remote_image_is_rejected_before_internal_request(self) -> None:
        canonical = validate_canonical_report(sample_report("https://example.invalid/map.png"))
        with patch("app.platform.reports._request_renderer_pdf") as request:
            with self.assertRaises(HTTPException) as caught:
                render_pdf(self.settings, canonical)
        self.assertEqual(caught.exception.status_code, 422)
        request.assert_not_called()

    def test_invalid_inline_image_is_rejected(self) -> None:
        report = sample_report("data:image/png;base64,not-valid-base64")
        with self.assertRaises(HTTPException) as caught:
            materialize_assets(self.settings, user_id="renderer-invalid", report=report)
        self.assertEqual(caught.exception.status_code, 422)

    def test_checked_in_golden_profiles_still_render(self) -> None:
        contracts = Path(__file__).resolve().parents[1] / "contracts"
        with patch("app.platform.reports._request_renderer_pdf", return_value=FAKE_CHROMIUM_PDF):
            for fixture_path in sorted((contracts / "golden").glob("*.json")):
                with self.subTest(fixture=fixture_path.name):
                    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
                    canonical = validate_canonical_report(
                        fixture["report"], requested_profile=fixture["rendererProfile"]
                    )
                    self.assertEqual(canonical.content_hash, fixture["expected"]["contentHash"])
                    rendered = render_pdf(self.settings, canonical)
                    self.assertGreaterEqual(len(rendered), fixture["expected"]["minimumPdfBytes"])


if __name__ == "__main__":
    unittest.main()
