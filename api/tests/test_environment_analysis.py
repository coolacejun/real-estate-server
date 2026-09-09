from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException

from app.platform.environment import Point, _nearby, analyze_environment


class EnvironmentAnalysisTest(unittest.TestCase):
    def _fixture(self, root: Path) -> None:
        (root / "bus-stops").mkdir(parents=True)
        (root / "amenities").mkdir()
        (root / "parks").mkdir()
        (root / "bus-stops" / "seoul.csv").write_text(
            "id,name,type,address,lat,lng,dataDate\n"
            "b1,시청.정류장,버스정류장,서울,37.5005,127.0,2026-01-01\n"
            "b2,시청·정류장,버스정류장,서울,37.5006,127.0,2026-01-01\n",
            encoding="utf-8",
        )
        (root / "rail-stations.csv").write_text(
            "id,name,type,address,lat,lng,dataDate\n"
            "r1,서울역,철도역,서울,37.51,127.0,2024-04-01\n",
            encoding="utf-8",
        )
        (root / "subway-stations.csv").write_text(
            "id,name,type,category,detail,address,lat,lng,dataDate\n"
            "s1,시청,도시철도역,1호선,1호선,서울,37.501,127.0,2025-04-08\n"
            "s1,시청,도시철도역,1호선,1호선,서울,37.501,127.0,2025-04-08\n",
            encoding="utf-8",
        )
        (root / "amenities" / "seoul.csv").write_text(
            "id,name,type,address,lat,lng,category\n"
            "a1,테스트약국,약국,서울,37.5005,127.0,약국\n"
            "a2,테스트골프연습장,생활편의,서울,37.5006,127.0,기타\n",
            encoding="utf-8",
        )
        (root / "parks" / "seoul.csv").write_text(
            "id,name,type,address,lat,lng,area,dataDate\n"
            "p1,테스트공원,근린공원,서울,37.501,127.0,1000,2026-04-13\n",
            encoding="utf-8",
        )
        (root / "schools.csv").write_text(
            "학교ID,학교명,학교급구분,운영상태,소재지지번주소,소재지도로명주소,위도,경도,데이터기준일자\n"
            "e1,테스트초,초등학교,운영,서울,서울로 1,37.501,127.0,2026-03-20\n"
            "m1,테스트중,중학교,운영,서울,서울로 2,37.502,127.0,2026-03-20\n"
            "h1,테스트고,고등학교,운영,서울,서울로 3,37.503,127.0,2026-03-20\n"
            "x1,폐교,초등학교,폐교,서울,서울로 4,37.5005,127.0,2026-03-20\n",
            encoding="utf-8",
        )
        (root / "security-lights.json").write_text(
            json.dumps(
                {
                    "source": "보안등",
                    "generatedAt": "2026-07-04T14:42:18.809Z",
                    "items": [[37.5005, 127.0, 2]],
                }
            ),
            encoding="utf-8",
        )
        (root / "cctv.json").write_text(
            json.dumps(
                {
                    "source": "CCTV",
                    "generatedAt": "2026-07-04T14:54:26.747Z",
                    "purposes": ["생활방범"],
                    "items": [[37.5005, 127.0, 3, 0]],
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _request() -> dict[str, object]:
        return {
            "location": {"lat": 37.5, "lng": 127.0, "crs": "EPSG:4326"},
            "address": {"parcel": "주소 없이도 PNU로 지역 확인", "road": ""},
            "parcelId": "1156011000100140011",
            "radiusProfile": "web-v1",
            "calculationVersion": "environment-web-v1",
        }

    def test_web_v1_contract_reads_all_seven_sources_and_keeps_legacy_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._fixture(root)
            result = analyze_environment(
                SimpleNamespace(environment_data_dir=root), self._request()
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["partial"])
            categories = result["categories"]
            self.assertEqual(categories["bus"]["radiusMeters"], 700)
            self.assertEqual(categories["rail"]["radiusMeters"], 3000)
            self.assertEqual(categories["schools"]["radiusMeters"], 1500)
            self.assertEqual(categories["bus"]["total"], 2)
            self.assertEqual(len(categories["bus"]["nearest"]), 1)
            self.assertEqual(categories["rail"]["total"], 2)
            self.assertEqual(categories["amenities"]["total"], 1)
            self.assertEqual(categories["schools"]["total"], 3)
            self.assertEqual(categories["schools"]["elementary"]["count"], 1)
            self.assertEqual(categories["schools"]["middle"]["count"], 1)
            self.assertEqual(categories["schools"]["high"]["count"], 1)
            self.assertEqual(
                {item["level"] for item in categories["schools"]["nearest"]},
                {"초등학교", "중학교", "고등학교"},
            )
            self.assertEqual(categories["securityLights"]["total"], 2)
            self.assertEqual(categories["cctv"]["total"], 3)
            self.assertTrue(
                {"traffic", "school", "convenience", "park", "streetlight"}
                <= set(categories)
            )
            self.assertEqual(
                [row["label"] for row in result["reportRows"]],
                [
                    "대상지",
                    "도로명주소",
                    "교통환경",
                    "학군환경",
                    "생활편의시설",
                    "공원환경",
                    "보안등",
                    "CCTV",
                ],
            )
            self.assertEqual(
                {source["category"] for source in result["sources"]},
                {"schools", "bus", "rail", "parks", "amenities", "securityLights", "cctv"},
            )
            self.assertEqual(
                {source["file"] for source in result["sources"]},
                {
                    "schools.csv",
                    "bus-stops/seoul.csv",
                    "rail-stations.csv",
                    "subway-stations.csv",
                    "parks/seoul.csv",
                    "amenities/seoul.csv",
                    "security-lights.json",
                    "cctv.json",
                },
            )

            (root / "parks" / "seoul.csv").unlink()
            partial = analyze_environment(
                SimpleNamespace(environment_data_dir=root), self._request()
            )
            self.assertTrue(partial["partial"])
            self.assertEqual(partial["categories"]["parks"]["status"], "error")
            self.assertIn(
                {"category": "parks", "code": "dataset_unavailable", "retryable": True},
                partial["errors"],
            )

    def test_invalid_inputs_and_complete_dataset_failure_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            settings = SimpleNamespace(environment_data_dir=Path(directory))
            with self.assertRaises(HTTPException) as invalid_coordinate:
                analyze_environment(
                    settings,
                    {
                        **self._request(),
                        "location": {"lat": "nan", "lng": 127.0, "crs": "EPSG:4326"},
                    },
                )
            self.assertEqual(invalid_coordinate.exception.status_code, 422)

            with self.assertRaises(HTTPException) as unavailable:
                analyze_environment(settings, self._request())
            self.assertEqual(unavailable.exception.status_code, 503)

    def test_radius_boundary_uses_haversine_distance(self) -> None:
        latitude = 37.5
        inside = Point(latitude + (699 / 111_320), 127.0, "inside", "bus")
        outside = Point(latitude + (701 / 111_320), 127.0, "outside", "bus")
        matches = _nearby((inside, outside), latitude, 127.0, 700)
        self.assertEqual([point.name for _, point in matches], ["inside"])


if __name__ == "__main__":
    unittest.main()
