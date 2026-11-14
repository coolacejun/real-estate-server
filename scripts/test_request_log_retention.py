import datetime as dt
import importlib.util
import logging
import os
from pathlib import Path
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location('retention', Path(__file__).resolve().parents[1] / 'api/app/request_log_retention.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)

class RetentionTests(unittest.TestCase):
    def test_calendar_cutoff_and_idle_base(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root) / 'server.log'
            now = dt.datetime(2026, 10, 19, tzinfo=dt.timezone.utc).timestamp()
            for day in ('2026-10-11', '2026-10-12', '2026-10-13', '2026-10-18'):
                Path(str(base) + '.' + day).write_text('private test data')
            base.write_text('idle')
            os.utime(base, (now - 8*86400, now - 8*86400))
            self.assertEqual(r.clean(base, now), 3)
            self.assertFalse(base.exists())
            self.assertEqual(sorted(p.name for p in Path(root).iterdir()),
                             ['server.log.2026-10-13', 'server.log.2026-10-18'])
    def test_handler_rollover_without_new_requests(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root) / 'server.log'
            h = r.RetentionFileHandler(base)
            try:
                h.emit(logging.makeLogRecord({'msg': 'before'}))
                h.rolloverAt = int(time.time()) - 1
                self.assertTrue(h.shouldRollover(None))
                h.doRollover()
                h.emit(logging.makeLogRecord({'msg': 'after'}))
                self.assertEqual(base.read_text().strip(), 'after')
                self.assertTrue(list(Path(root).glob('server.log.*')))
            finally:
                h.close()

if __name__ == '__main__':
    unittest.main()
