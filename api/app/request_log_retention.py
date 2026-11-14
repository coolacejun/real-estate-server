"""Seven UTC calendar days of request/error logs, including idle applications.

API uses RetentionFileHandler; `run` captures tunnel stdout/stderr. A host
launchd job calls `clean` even while applications are stopped. No log backups.
"""
from __future__ import annotations
import argparse
import datetime as dt
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path
import signal
import subprocess
import threading
import time

DAYS = 7


def clean(base: Path, now: float | None = None) -> int:
    now = time.time() if now is None else now
    # A five-minute safety margin covers the one-minute scheduling interval.
    expiry_now = now + 300
    today = dt.datetime.fromtimestamp(expiry_now, dt.timezone.utc).date()
    cutoff = today - dt.timedelta(days=DAYS - 1)
    removed = 0
    for path in base.parent.glob(base.name + '.*'):
        if path.is_symlink() or not path.is_file():
            continue
        try:
            date = dt.date.fromisoformat(path.name[len(base.name) + 1:])
        except ValueError:
            # Old size-based rotations have no trustworthy per-record date.
            # They are removed explicitly once during migration, not guessed.
            continue
        if date < cutoff:
            path.unlink(missing_ok=True)
            removed += 1
    if base.is_file() and not base.is_symlink() and base.stat().st_mtime <= expiry_now - DAYS * 86400:
        base.unlink(missing_ok=True)
        removed += 1
    return removed


class RetentionFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, maxBytes=0, backupCount=6, encoding='utf-8', **kwargs):
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        clean(path)
        super().__init__(path, when='midnight', interval=1, backupCount=DAYS - 1,
                         encoding=encoding, utc=True, delay=True)
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._maintain, daemon=True)
        self._worker.start()

    def _maintain(self):
        while not self._stop.wait(60):
            self.acquire()
            try:
                if self.shouldRollover(None):
                    self.doRollover()
                clean(Path(self.baseFilename))
            except Exception:
                # Never recursively send maintenance errors to this handler.
                logging.getLogger('log-retention-maintenance').exception('Log cleanup failed')
            finally:
                self.release()

    def close(self):
        self._stop.set()
        super().close()


def run(base: Path, command: list[str]) -> int:
    os.umask(0o077)
    handler = RetentionFileHandler(base)
    handler.setFormatter(logging.Formatter('%(asctime)sZ %(message)s'))
    handler.formatter.converter = time.gmtime
    logger = logging.Logger('captured-process')
    logger.addHandler(handler)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               start_new_session=True)
    def forward(signum, _):
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            pass
    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    assert process.stdout is not None
    for raw in iter(process.stdout.readline, b''):
        logger.info(raw.decode('utf-8', 'replace').rstrip('\n'))
    result = process.wait()
    handler.close()
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['clean', 'run'])
    parser.add_argument('paths', nargs='+')
    args = parser.parse_args()
    if args.mode == 'clean':
        for name in args.paths:
            clean(Path(name))
    else:
        command = args.paths[1:]
        if command[:1] == ['--']:
            command.pop(0)
        raise SystemExit(run(Path(args.paths[0]), command))
