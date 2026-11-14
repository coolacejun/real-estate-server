"""Synthetic policy tests only: no Docker commands or production data access."""
import copy
import unittest
import sys
import time

from external_server_recovery import Controller, DiskUnavailable, ManualReview, Backend, DOCKER
from unittest.mock import patch


class FakeBackend:
    def __init__(self):
        self.media = 'original-attachment'
        self.available = True
        self.review = False
        self.is_healthy = True
        self.vm = 'Broken'
        self.actions = []
        self.fail = False

    def guard(self):
        if not self.available:
            raise DiskUnavailable('Original disk missing/wrong UUID.')
        if self.review:
            raise ManualReview('Original disk-image identity changed.')
        return {'media': self.media}

    def healthy(self):
        return self.is_healthy

    def vm_state(self):
        return self.vm

    def recover(self, media, restart_vm):
        self.actions.append((media, restart_vm))
        if self.fail:
            raise RuntimeError('Synthetic recovery failure.')
        self.is_healthy = True


class RecoveryPolicyTests(unittest.TestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.state = {}
        self.saved = []
        self.events = []
        self.c = Controller(self.backend, self.state,
                            lambda s: self.saved.append(copy.deepcopy(s)),
                            lambda kind, reason: self.events.append(kind))

    def tick(self, now):
        with patch('external_server_recovery.time.time', return_value=now):
            self.c.tick(now)

    def stable(self):
        self.tick(1000)
        self.tick(1030)
        self.tick(1060)

    def test_healthy_server_is_never_restarted(self):
        self.stable()
        self.assertEqual(self.state['status'], 'healthy')
        self.assertEqual(self.backend.actions, [])

    def test_missing_or_wrong_disk_never_starts_vm(self):
        self.backend.available = False
        for now in (1000, 1030, 1060):
            self.tick(now)
        self.assertEqual(self.backend.actions, [])
        self.assertEqual(self.events, ['disk_unavailable'])

    def test_changed_image_requires_manual_review(self):
        self.backend.review = True
        self.tick(1000)
        self.assertTrue(self.state['blocked'])
        self.assertFalse(self.backend.actions)

    def test_recovery_waits_full_stability_period(self):
        self.backend.is_healthy = False
        self.tick(1000)
        self.tick(1030)
        self.assertFalse(self.backend.actions)
        self.tick(1060)
        self.assertEqual(self.backend.actions, [('original-attachment', True)])

    def test_brief_reconnection_changes_registry_id(self):
        self.tick(1000)
        self.tick(1030)
        self.backend.media = 'reattached'
        self.backend.is_healthy = False
        self.tick(1060)
        self.tick(1090)
        self.assertFalse(self.backend.actions)
        self.tick(1120)
        self.assertEqual(self.backend.actions, [('reattached', True)])

    def test_long_gap_restarts_stability_timer(self):
        self.tick(1000)
        self.backend.is_healthy = False
        self.tick(1200)
        self.assertFalse(self.backend.actions)

    def test_clock_reversal_restarts_stability_timer(self):
        self.tick(1000)
        self.backend.is_healthy = False
        self.tick(900)
        self.assertFalse(self.backend.actions)

    def test_disconnection_does_not_reset_failure_budget(self):
        self.state.update(consecutive_failures=2, attempts=[900, 950])
        self.backend.available = False
        self.tick(1000)
        self.assertEqual(self.state['consecutive_failures'], 2)
        self.assertEqual(self.state['attempts'], [900, 950])

    def test_three_failures_block_and_cooldown_is_enforced(self):
        self.backend.is_healthy = False
        self.backend.fail = True
        self.stable()
        self.tick(1090)
        self.assertEqual(len(self.backend.actions), 1)
        # Keep monitoring through cooldown so it represents stable attachment.
        for now in range(1120, 1400, 30):
            self.tick(now)
        self.tick(1420)
        self.assertEqual(len(self.backend.actions), 2)
        for now in range(1450, 2050, 30):
            self.tick(now)
        self.tick(2050)
        self.assertEqual(len(self.backend.actions), 3)
        self.assertTrue(self.state['blocked'])
        self.tick(2080)
        self.assertEqual(len(self.backend.actions), 3)
        self.assertEqual(self.events.count('blocked'), 1)

    def test_block_persists_after_one_hour_and_reconnect(self):
        self.state.update(blocked=True, block_reason='Retry limit reached.')
        self.tick(1000)
        self.backend.media = 'new-attachment'
        self.tick(5000)
        self.assertFalse(self.backend.actions)
        self.assertTrue(self.state['blocked'])

    def test_three_successful_restarts_still_limit_flapping(self):
        self.state.update(attempts=[800, 850, 900])
        self.backend.is_healthy = False
        self.stable()
        self.assertTrue(self.state['blocked'])
        self.assertFalse(self.backend.actions)

    def test_success_resets_consecutive_failures_but_not_hourly_budget(self):
        self.state.update(consecutive_failures=1, attempts=[900])
        self.backend.is_healthy = False
        self.stable()
        self.assertEqual(self.state['consecutive_failures'], 0)
        self.assertEqual(self.state['attempts'], [900, 1060])
        self.assertEqual(self.events, ['recovered'])

    def test_failure_budget_is_persisted_before_recovery(self):
        self.backend.is_healthy = False
        def recover(*args):
            self.assertEqual(self.saved[-1]['attempts'], [1060])
            self.assertEqual(self.saved[-1]['consecutive_failures'], 1)
        self.backend.recover = recover
        self.stable()

    def test_running_vm_without_reattachment_only_repairs_existing_containers(self):
        self.stable()
        self.backend.is_healthy = False
        self.backend.vm = 'Running'
        self.tick(1090)
        self.assertEqual(self.backend.actions, [('original-attachment', False)])

    def test_health_inventory_conflict_blocks_before_any_start(self):
        def healthy():
            raise ManualReview('Deployment changed.')
        self.backend.healthy = healthy
        self.stable()
        self.assertTrue(self.state['blocked'])
        self.assertFalse(self.backend.actions)

    def test_docker_endpoint_is_explicit_not_default_context(self):
        self.assertEqual(DOCKER[1], '--host')
        self.assertEqual(DOCKER[2], 'unix:///Volumes/DB_EXTERNAL/colima-target/external-pg/docker.sock')

    def test_running_vm_is_not_force_killed_after_failed_graceful_stop(self):
        backend = Backend({})
        with patch.object(backend, 'guard', return_value={'media': 'safe'}), \
             patch.object(backend, 'safe_stale_pids'), \
             patch.object(backend, 'vm_state', return_value='Running'), \
             patch.object(backend, 'run', return_value='') as run:
            with self.assertRaises(ManualReview):
                backend.recover('safe', True)
            self.assertFalse(any('--force' in c.args[0] for c in run.call_args_list))
            self.assertFalse(any('start' in c.args[0] for c in run.call_args_list))

    def test_disk_loss_stops_recovery_before_any_command(self):
        backend = Backend({})
        with patch.object(backend, 'guard', side_effect=DiskUnavailable('Unplugged')), \
             patch.object(backend, 'run') as run:
            with self.assertRaises(DiskUnavailable):
                backend.recover('safe', True)
            run.assert_not_called()

    def test_limactl_timeouts_count_as_three_failures_and_block(self):
        self.backend.is_healthy = False
        self.backend.vm_state = lambda: (_ for _ in ()).throw(TimeoutError('limactl timed out'))
        self.stable()
        self.assertEqual(self.state['consecutive_failures'], 1)
        cooldown = self.state['next_attempt_at']
        for now in range(1090, 1360, 30):
            self.tick(now)
            self.assertEqual(self.state['next_attempt_at'], cooldown)
        self.tick(1360)
        self.assertEqual(self.state['consecutive_failures'], 2)
        for now in range(1390, 1960, 30):
            self.tick(now)
        self.tick(1960)
        self.assertTrue(self.state['blocked'])
        self.assertEqual(self.state['consecutive_failures'], 3)
        self.assertEqual(len(self.state['attempts']), 3)
        self.assertFalse(self.backend.actions)
        self.tick(2050)
        self.assertEqual(self.events.count('blocked'), 1)

    def test_cooldown_prevents_even_vm_state_inspection(self):
        self.backend.is_healthy = False
        self.state.update(media=self.backend.media, stable_since=1000, checked_at=1060,
                          next_attempt_at=1500, attempts=[1000], consecutive_failures=1)
        with patch.object(self.backend, 'vm_state') as inspect_vm:
            self.tick(1090)
            inspect_vm.assert_not_called()
        self.assertEqual(self.state['next_attempt_at'], 1500)
        self.assertEqual(self.state['consecutive_failures'], 1)

    def test_cli_return_does_not_wait_for_detached_child_output_eof(self):
        backend = Backend({})
        program = ('import subprocess,sys; '
                   'subprocess.Popen([sys.executable,"-c","import time;time.sleep(2)"],'
                   'stdout=sys.stdout,stderr=sys.stderr); print("finished",flush=True)')
        start = time.monotonic()
        result = backend.run([sys.executable, '-c', program], timeout=1)
        self.assertEqual(result.strip(), 'finished')
        self.assertLess(time.monotonic() - start, 1)

    def test_actual_cli_timeout_still_stops_the_child(self):
        with self.assertRaises(TimeoutError):
            Backend({}).run([sys.executable, '-c', 'import time;time.sleep(5)'], timeout=0.2)


if __name__ == '__main__':
    unittest.main()
