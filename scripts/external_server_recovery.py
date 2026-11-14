"""Bounded recovery of the existing external-pg instance; never provision data.

Launchd runs one check every 30 seconds. Capture/refresh the baseline only after
an authorized deployment, while the original PostgreSQL is healthy. After a
retry-limit block, inspect the disk/cable and use --reset-retries explicitly.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import plistlib
import signal
import subprocess
import tempfile
import time
import urllib.request
import uuid

MOUNT = Path('/Volumes/DB_EXTERNAL')
VOLUME_UUID = '36ACB1AF-432F-4404-9ED1-FD1ACF907F1E'
COLIMA_HOME = MOUNT / 'colima-target'
LIMA_HOME = COLIMA_HOME / '_lima'
VM = LIMA_HOME / 'colima-external-pg'
STATE_DIR = Path('/Users/jun/Library/Application Support/BuildingLand/server-recovery')
DOCKER = ['/opt/homebrew/bin/docker', '--host', f'unix://{COLIMA_HOME}/external-pg/docker.sock']
CONTAINERS = ('postgres', 'redis', 'api', 'web', 'traefik', 'cadastral-sync',
              'building-hub-sync', 'land-movement-sync', 'building-integrated-sync')
FILES = (COLIMA_HOME / 'external-pg/colima.yaml', VM / 'lima.yaml', VM / 'diffdisk',
         LIMA_HOME / '_disks/colima-external-pg/datadisk')
STABLE_SECONDS = 60
RETRY_WINDOW = 3600
MAX_ATTEMPTS = 3


class ManualReview(RuntimeError):
    pass


class DiskUnavailable(RuntimeError):
    pass


def atomic_json(path: Path, value: dict) -> None:
    fd, name = tempfile.mkstemp(prefix=path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def read_json(path: Path, *, required=False) -> dict:
    if not path.exists() and not required:
        return {}
    # Corrupt/missing baselines must never silently become a fresh retry budget.
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ManualReview('Recovery state is invalid; operator review required.')
    return value


class Backend:
    def __init__(self, baseline: dict):
        self.baseline = baseline
        self.env = {**os.environ, 'COLIMA_HOME': str(COLIMA_HOME), 'LIMA_HOME': str(LIMA_HOME)}
        self.env.pop('DOCKER_CONTEXT', None)

    def run(self, args, *, timeout=10, check=True, guard=None):
        # Lima can spawn detached SSH helpers inheriting its output handles.
        # Waiting for PIPE EOF then waits for those helpers, not for limactl.
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            if guard:
                guard()
            process = subprocess.Popen(args, env=self.env, stdout=output,
                                       stderr=errors, start_new_session=True)
            deadline = time.monotonic() + timeout
            last_guard = time.monotonic()
            try:
                while process.poll() is None:
                    now = time.monotonic()
                    if guard and now - last_guard >= 2:
                        guard()
                        last_guard = now
                    remaining = deadline - now
                    if remaining <= 0:
                        raise TimeoutError(f'{Path(args[0]).name} timed out')
                    time.sleep(min(0.1, remaining))
                if guard:
                    guard()
                if check and process.returncode:
                    raise RuntimeError(f'{Path(args[0]).name} failed ({process.returncode})')
                output.seek(0)
                return output.read().decode('utf-8', errors='replace')
            except BaseException:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                raise

    def guard(self):
        try:
            info = plistlib.loads(self.run(['/usr/sbin/diskutil', 'info', '-plist', str(MOUNT)]).encode())
            media = plistlib.loads(self.run(['/usr/sbin/ioreg', '-a', '-r', '-n', 'DB_EXTERNAL', '-d', '1']).encode())
        except (RuntimeError, TimeoutError, ValueError, plistlib.InvalidFileException) as exc:
            raise DiskUnavailable('Original external disk is not readable.') from exc
        if info.get('VolumeUUID') != VOLUME_UUID or info.get('MountPoint') != str(MOUNT) or not os.path.ismount(MOUNT):
            raise DiskUnavailable('Original mounted volume UUID is not present; no fallback allowed.')
        node = next((r for r in media if r.get('UUID') == VOLUME_UUID), None)
        if not node or not node.get('IORegistryEntryID'):
            raise DiskUnavailable('Cannot confirm the original disk attachment.')
        for path in FILES:
            if not path.is_file() or path.stat().st_size == 0:
                raise ManualReview('An existing VM configuration/image is missing; refusing provisioning.')
        images = {str(p): {'inode': p.stat().st_ino, 'size': p.stat().st_size} for p in FILES[-2:]}
        if self.baseline and images != self.baseline['images']:
            raise ManualReview('Existing VM disk-image identity changed; refusing automatic start.')
        # Registry IDs change even when a brief unplug/replug happens between checks.
        return {'media': str(node['IORegistryEntryID']), 'images': images}

    def inventory(self):
        rows = json.loads(self.run(DOCKER + ['inspect', *CONTAINERS], timeout=10))
        found = {r['Name'].lstrip('/'): r for r in rows}
        if set(found) != set(CONTAINERS):
            raise ManualReview('An existing deployed container is missing; no container creation allowed.')
        for row in found.values():
            if row['Config'].get('Labels', {}).get('com.docker.compose.project') != 'server':
                raise ManualReview('Unexpected container project; refusing recovery.')
        if self.baseline and {k: r['Id'] for k, r in found.items()} != self.baseline['containers']:
            raise ManualReview('Deployment inventory changed; refresh the baseline after review.')
        mount = next((m for m in found['postgres']['Mounts'] if m['Destination'] == '/var/lib/postgresql/data'), None)
        if not mount or mount.get('Type') != 'volume':
            raise ManualReview('Expected existing PostgreSQL volume is unavailable.')
        if self.baseline and mount.get('Name') != self.baseline['postgres_volume']:
            raise ManualReview('PostgreSQL volume changed; refusing recovery.')
        return found

    def db_identity(self):
        result = self.run(DOCKER + ['exec', 'postgres', 'sh', '-c',
            'test -s "$PGDATA/PG_VERSION" && test -s "$PGDATA/global/pg_control" && '
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc '
            '"SELECT system_identifier::text FROM pg_control_system()"'], timeout=10).strip()
        if not result.isdigit():
            raise ManualReview('Cannot confirm the existing PostgreSQL cluster.')
        if self.baseline and result != self.baseline['database_system_id']:
            raise ManualReview('PostgreSQL cluster identity changed; operator review required.')
        return result

    def healthy(self):
        try:
            rows = self.inventory()
            if any(not r['State']['Running'] for r in rows.values()):
                return False
            self.db_identity()
            if self.run(DOCKER + ['exec', 'redis', 'redis-cli', 'ping'], timeout=5).strip() != 'PONG':
                return False
            self.run(DOCKER + ['exec', 'api', 'python', '-c',
                'import urllib.request; assert urllib.request.urlopen("http://127.0.0.1:8000/health",timeout=5).status==200'], timeout=8)
            request = urllib.request.Request('http://127.0.0.1:18080/', headers={'Host': 'building-land.com'})
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=5) as response:
                return response.status == 200
        except ManualReview:
            raise
        except Exception:
            return False

    def vm_state(self):
        output = self.run(['/opt/homebrew/bin/limactl', 'list', '--json', 'colima-external-pg'], timeout=10)
        rows = [json.loads(line) for line in output.splitlines() if line.strip()]
        row = next((r for r in rows if r.get('name') == 'colima-external-pg'), None)
        if row is None:
            raise ManualReview('Existing Lima instance is missing; refusing automatic creation.')
        return row['status']

    def safe_stale_pids(self):
        for path in (VM / 'ha.pid', VM / 'vz.pid'):
            if not path.exists():
                continue
            pid = int(path.read_text().strip())
            command = self.run(['/bin/ps', '-p', str(pid), '-o', 'command='], check=False).strip()
            if command and (str(VM) not in command or 'limactl' not in command):
                raise ManualReview('A stale VM PID belongs to another process; refusing to kill it.')

    def recover(self, media: str, restart_vm: bool):
        def guard():
            if self.guard()['media'] != media:
                raise DiskUnavailable('Disk attachment changed during recovery; operation stopped.')
        guard()
        if restart_vm:
            self.safe_stale_pids()
            self.run(['/opt/homebrew/bin/colima', 'stop', '--profile', 'external-pg'],
                     timeout=120, check=False, guard=guard)
            if self.vm_state() == 'Running':
                raise ManualReview('Graceful VM stop failed; automatic force-stop of a running VM is prohibited.')
            self.safe_stale_pids()
            self.run(['/opt/homebrew/bin/limactl', 'stop', '--force', 'colima-external-pg'],
                     timeout=30, check=False, guard=guard)
            if self.vm_state() != 'Stopped':
                raise ManualReview('Existing VM did not stop safely.')
            lines = self.run(['/bin/ps', '-axo', 'pid=,command=']).splitlines()
            expected = f'ssh: {VM}/ssh.sock [mux]'
            for line in lines:
                pid, _, command = line.strip().partition(' ')
                if command.strip() == expected:
                    guard()
                    if self.run(['/bin/ps', '-p', pid, '-o', 'command='], check=False).strip() == expected:
                        os.kill(int(pid), signal.SIGTERM)
            guard()
            # Guards are continuously rechecked; no create/delete/prune/compose/migrate commands exist here.
            self.run(['/opt/homebrew/bin/colima', 'start', '--profile', 'external-pg', '--activate=false'],
                     timeout=240, guard=guard)
        guard()
        rows = self.inventory()
        for name in CONTAINERS:
            if not rows[name]['State']['Running']:
                guard()
                self.run(DOCKER + ['start', rows[name]['Id']], timeout=30, guard=guard)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            guard()
            if self.healthy():
                return
            time.sleep(3)
        raise RuntimeError('Existing server health checks did not recover.')


class Controller:
    def __init__(self, backend, state, save, event):
        self.backend, self.state, self.save, self.event = backend, state, save, event

    def status(self, name, reason=''):
        changed = self.state.get('status') != name or self.state.get('reason') != reason
        self.state.update(status=name, reason=reason)
        self.save(self.state)
        if changed:
            logging.getLogger('server-recovery').info('status %s: %s', name, reason)
        if changed and name in {'disk_unavailable', 'blocked', 'recovered'}:
            self.event(name, reason)

    def tick(self, now):
        s = self.state
        last_tick = s.get('checked_at', now)
        s['checked_at'] = now
        try:
            attachment = self.backend.guard()
        except DiskUnavailable as exc:
            s.update(stable_since=None, media=None, needs_vm_restart=True)
            self.status('disk_unavailable', str(exc))
            return
        except ManualReview as exc:
            s['blocked'] = True
            self.status('blocked', str(exc))
            return
        if s.get('media') != attachment['media'] or now - last_tick > 90 or now < last_tick:
            s.update(media=attachment['media'], stable_since=now, needs_vm_restart=True)
        if s.get('blocked'):
            self.status('blocked', s.get('block_reason', s.get('reason', 'Manual retry reset required.')))
            return
        if now - s.get('stable_since', now) < STABLE_SECONDS:
            self.status('waiting_stable_disk', 'Waiting for 60 seconds of stable original-disk attachment.')
            return
        try:
            if self.backend.healthy():
                s.update(consecutive_failures=0, needs_vm_restart=False)
                self.status('healthy')
                return
            s['attempts'] = [t for t in s.get('attempts', []) if now - t < RETRY_WINDOW]
            if len(s['attempts']) >= MAX_ATTEMPTS or s.get('consecutive_failures', 0) >= MAX_ATTEMPTS:
                raise ManualReview('Retry limit reached (3 attempts/hour or 3 consecutive failures); manual review required.')
            if now < s.get('next_attempt_at', 0):
                self.status('retry_wait', 'Recovery retry is cooling down.')
                return
            s['attempts'].append(now)
            s['consecutive_failures'] = s.get('consecutive_failures', 0) + 1
            self.status('recovering', 'Recovering only the existing instance and deployed containers.')
            # State inspection itself is part of this durable, bounded attempt.
            # A timeout cannot bypass the failure counter or extend every cooldown.
            vm_state = self.backend.vm_state()
            restart_vm = s.get('needs_vm_restart', False) or vm_state in ('Broken', 'Stopped')
            if vm_state not in ('Running', 'Broken', 'Stopped'):
                raise ManualReview('Unknown VM state; automatic recovery stopped.')
            self.backend.recover(attachment['media'], restart_vm)
            s.update(consecutive_failures=0, next_attempt_at=0, needs_vm_restart=False,
                     checked_at=time.time(), last_recovered_at=time.time())
            self.status('recovered', 'Existing server and PostgreSQL identity verified.')
        except ManualReview as exc:
            s.update(blocked=True, block_reason=str(exc))
            self.status('blocked', str(exc))
        except Exception as exc:
            if s.get('consecutive_failures', 0) >= MAX_ATTEMPTS:
                s.update(blocked=True, block_reason='Three consecutive recovery failures; manual review required.')
                self.status('blocked', s['block_reason'])
            else:
                s['next_attempt_at'] = now + 300 * (2 ** max(0, s.get('consecutive_failures', 1) - 1))
                self.status('retry_wait', f'{type(exc).__name__}: {exc}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-baseline', action='store_true')
    parser.add_argument('--reset-retries', action='store_true')
    parser.add_argument('--retry-now', action='store_true',
                        help='Operator-requested check: bypass cooldown only; preserve all failure limits and disk guards.')
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--test-notification', action='store_true')
    args = parser.parse_args()
    STATE_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(STATE_DIR, 0o700)
    log = logging.getLogger('server-recovery')
    log.setLevel(logging.INFO)
    handler = RotatingFileHandler(STATE_DIR / 'recovery.log', maxBytes=2_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(handler)
    def event(kind, reason):
        record = {'id': str(uuid.uuid4()), 'at': time.time(), 'kind': kind, 'reason': reason}
        with (STATE_DIR / 'events.jsonl').open('a') as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + '\n')
        messages = {
            'blocked': '서버 자동 복구가 안전 검사 또는 재시도 제한으로 중단됐습니다. 이 채팅에서 상태를 확인해 주세요.',
            'recovered': '기존 서버 자동 복구와 DB 확인을 완료했습니다.',
            'disk_unavailable': '원래 외장 디스크를 확인할 수 없어 서버 기동을 보류합니다.',
            'notification_test': '서버 자동 복구 알림 테스트입니다. 실제 장애가 아닙니다.',
        }
        script = 'on run argv\n display notification (item 1 of argv) with title "건물과토지 서버"\nend run'
        try:
            result = subprocess.run(['/usr/bin/osascript', '-e', script, messages[kind]],
                                    capture_output=True, timeout=10)
            log.info('event %s; notification request exit=%s', kind, result.returncode)
        except Exception:
            # Notification delivery must not change a successful recovery into a failure.
            log.exception('Native notification failed; event retained for Codex notification')
    with (STATE_DIR / 'lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        state_file, baseline_file = STATE_DIR / 'state.json', STATE_DIR / 'baseline.json'
        try:
            baseline = read_json(baseline_file, required=not args.capture_baseline)
            state = read_json(state_file)
            if args.status:
                print(json.dumps(state, ensure_ascii=False, indent=2))
                return
            backend = Backend(baseline)
            if args.test_notification:
                event('notification_test', 'Operator-requested notification test.')
                return
            if args.capture_baseline:
                # Explicit deployment refresh may accept new container IDs, but
                # cannot adopt another disk image or another PostgreSQL cluster.
                backend = Backend({})
                attachment = backend.guard()
                if not backend.healthy():
                    raise ManualReview('Capture requires the original healthy server; baseline was not changed.')
                rows = backend.inventory()
                mount = next(m for m in rows['postgres']['Mounts'] if m['Destination'] == '/var/lib/postgresql/data')
                new = {'version': 1, 'volume_uuid': VOLUME_UUID, 'images': attachment['images'],
                       'containers': {k: r['Id'] for k, r in rows.items()},
                       'postgres_volume': mount['Name'], 'database_system_id': backend.db_identity()}
                if baseline and (new['images'] != baseline['images'] or
                                 new['database_system_id'] != baseline['database_system_id'] or
                                 new['postgres_volume'] != baseline['postgres_volume']):
                    raise ManualReview('Baseline refresh cannot replace the original disk/database.')
                atomic_json(baseline_file, new)
                print('Captured the original healthy VM/container/database baseline.')
                return
            if args.reset_retries:
                backend.guard()
                state.update(blocked=False, attempts=[], consecutive_failures=0, next_attempt_at=0,
                             stable_since=None, media=None, status='waiting_stable_disk', reason='Operator reset.')
                state.pop('block_reason', None)
                atomic_json(state_file, state)
                print('Retry limit reset; stable-disk and original-data guards remain mandatory.')
                return
            if args.retry_now:
                state['next_attempt_at'] = 0
            Controller(backend, state, lambda s: atomic_json(state_file, s), event).tick(time.time())
        except Exception as exc:
            # Never run destructive recovery after an unreadable/missing state file.
            log.exception('Recovery check aborted')
            atomic_json(STATE_DIR / 'fault.json', {'at': time.time(), 'error': f'{type(exc).__name__}: {exc}'})
            print(f'Recovery check aborted: {type(exc).__name__}', flush=True)
            raise SystemExit(1)


if __name__ == '__main__':
    main()
