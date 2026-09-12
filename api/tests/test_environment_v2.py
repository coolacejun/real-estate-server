import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.platform.environment import analyze_environment, shared_environment
import test_environment_analysis as fixtures


class EnvironmentV2Test(unittest.TestCase):
    def test_web_and_server_share_exact_source_and_results(self):
        web = Path(__file__).resolve().parents[2] / 'web'
        self.assertEqual((web/'environment_analysis.py').read_bytes(), Path(shared_environment.__file__).read_bytes())
        spec = importlib.util.spec_from_file_location('web_environment_parity', web/'environment_analysis.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            fixture=fixtures.EnvironmentAnalysisTest()
            fixture._fixture(root)
            pins=shared_environment.dataset_identity(root,'seoul')
            self.enterContext(patch.object(shared_environment,'_pinned_digests',return_value=pins))
            self.enterContext(patch.object(module,'_pinned_digests',return_value=pins))
            request={**fixture._request(),'calculationVersion':'environment-web-v2'}
            web_result=module.analyze_environment_request(request,data_root=root)
            api_result=analyze_environment(SimpleNamespace(environment_data_dir=root),request)
            for key in ('calculationVersion','radiusProfile','categories','reportRows','sources','datasetFingerprint','datasetFiles'):
                self.assertEqual(web_result[key],api_result[key],key)
            text=' '.join(row['value'] for row in api_result['reportRows'])
            for expected in ('반경 300m','500m','설치 지점은 1곳','생활방범 3대','입니다.'):
                self.assertIn(expected,text)
            self.assertEqual(api_result['calculationVersion'],'environment-web-v2')
            # Old clients retain their v1 schema and aliases, never relabelled v2.
            old=analyze_environment(SimpleNamespace(environment_data_dir=root),fixture._request())
            self.assertEqual(old['calculationVersion'],'environment-web-v1')
            self.assertIn('traffic',old['categories'])
            self.assertEqual(old['categories']['cctv']['total'],api_result['categories']['cctv']['total'])

    def test_changed_data_is_reloaded_and_changes_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);fixture=fixtures.EnvironmentAnalysisTest();fixture._fixture(root)
            pins=shared_environment.dataset_identity(root,'seoul')
            self.enterContext(patch.object(shared_environment,'_pinned_digests',return_value=pins))
            request={**fixture._request(),'calculationVersion':'environment-web-v2'}
            settings=SimpleNamespace(environment_data_dir=root)
            before=analyze_environment(settings,request)
            bus=root/'bus-stops/seoul.csv'
            bus.write_text(bus.read_text(encoding='utf-8')+'b3,새정류장,버스정류장,서울,37.5007,127.0,2026-01-02\n',encoding='utf-8')
            from fastapi import HTTPException
            with self.assertRaises(HTTPException) as stale:
                analyze_environment(settings,request)
            self.assertEqual(stale.exception.status_code,503)
            # Simulate a reviewed matching dataset release, then require reload.
            pins.update(shared_environment.dataset_identity(root,'seoul'))
            after=analyze_environment(settings,request)
            self.assertNotEqual(before['datasetFingerprint'],after['datasetFingerprint'])
            self.assertEqual(after['categories']['bus']['total'],before['categories']['bus']['total']+1)


if __name__=='__main__': unittest.main()
