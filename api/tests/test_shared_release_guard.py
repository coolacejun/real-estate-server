from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest
import tempfile
import hashlib
import json
import os
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('shared_release_guard', Path(__file__).resolve().parents[2] / 'scripts/check_shared_release.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class SharedReleaseGuardTest(unittest.TestCase):
    def test_hook_repository_environment_cannot_redirect_web_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            server = Path(directory) / 'server'
            web = server / 'web'
            web.mkdir(parents=True)
            for repo in (server, web):
                guard.git(repo, 'init', '--quiet')
                guard.git(repo, '-c', 'user.name=Release Guard Test',
                          '-c', 'user.email=guard@example.invalid', 'commit',
                          '--quiet', '--allow-empty', '-m', repo.name)
            server_head = guard.git(server, 'rev-parse', 'HEAD')
            web_head = guard.git(web, 'rev-parse', 'HEAD')
            self.assertNotEqual(server_head, web_head)
            inherited = {'GIT_DIR': str(server / '.git'),
                         'GIT_WORK_TREE': str(server),
                         'GIT_INDEX_FILE': str(server / '.git/index'),
                         'GIT_COMMON_DIR': str(server / '.git')}
            with patch.dict(os.environ, inherited):
                self.assertEqual(guard.git(server, 'rev-parse', 'HEAD'), server_head)
                self.assertEqual(guard.git(web, 'rev-parse', 'HEAD'), web_head)
                (web / 'uncommitted.txt').write_text('dirty web checkout', encoding='utf-8')
                self.assertEqual(guard.git(web, 'status', '--porcelain'), '?? uncommitted.txt')

    def test_environment_code_and_dataset_drift_block_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);web=root/'web';api=root/'api/app/platform'
            (web/'data').mkdir(parents=True);api.mkdir(parents=True)
            (web/'environment_analysis.py').write_bytes(b'shared calculator')
            (api/'shared_environment.py').write_bytes(b'shared calculator')
            data=web/'data/rail-stations.csv';data.write_bytes(b'pinned rail dataset')
            manifest=json.dumps({'calculationVersion':'environment-web-v2','files':{'rail-stations.csv':hashlib.sha256(data.read_bytes()).hexdigest()}}).encode()
            for parent in (api,web): (parent/'environment-data-manifest.json').write_bytes(manifest)
            guard.verify_environment_contract(root,web)
            (api/'shared_environment.py').write_bytes(b'different formatter')
            with self.assertRaises(guard.ReleaseBlocked):guard.verify_environment_contract(root,web)
            (api/'shared_environment.py').write_bytes(b'shared calculator')
            data.write_bytes(b'unreviewed rail dataset')
            with self.assertRaises(guard.ReleaseBlocked):guard.verify_environment_contract(root,web)

    def test_unpublished_wrong_commit_or_wrong_ref_blocks(self):
        pin = 'a' * 40
        for listing in ('', f"{'b'*40}\t{guard.WEB_BRANCH}", f'{pin}\trefs/heads/main'):
            with self.subTest(listing=listing), self.assertRaises(guard.ReleaseBlocked):
                guard.verify_remote_pin(pin, listing)

    def test_exact_published_pin_passes(self):
        pin = 'a' * 40
        guard.verify_remote_pin(pin, f'{pin}\t{guard.WEB_BRANCH}\n')

    def test_different_server_commit_and_dirty_checkout_block_before_remote_lookup(self):
        with patch.object(guard, 'git', side_effect=['a' * 40, 'b' * 40]) as git:
            with self.assertRaises(guard.ReleaseBlocked):
                guard.check(Path('.'), server_ref='b' * 40)
            self.assertEqual(git.call_count, 2)
        with patch.object(guard, 'git', side_effect=['a' * 40, 'a' * 40, ' M web']) as git:
            with self.assertRaises(guard.ReleaseBlocked):
                guard.check(Path('.'))
            self.assertEqual(git.call_count, 3)


if __name__ == '__main__':
    unittest.main()
