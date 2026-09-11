from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('shared_release_guard', Path(__file__).resolve().parents[2] / 'scripts/check_shared_release.py')
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class SharedReleaseGuardTest(unittest.TestCase):
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
