"""Run archive contracts using a disposable loopback PostgreSQL cluster only."""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import unittest
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--postgres-bin', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--pattern', default='test_shared_archives.py')
    parser.add_argument('--web-repo', type=Path, required=True)
    parser.add_argument('--case', action='append', help='Run only this fully qualified unittest case')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    args.work_dir.mkdir(parents=True, exist_ok=True)
    suffix = '.exe' if os.name == 'nt' else ''
    flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
    with tempfile.TemporaryDirectory(prefix='archive-contract-', dir=args.work_dir) as temporary:
        root = Path(temporary).resolve()
        assert root.parent == args.work_dir.resolve()
        tempfile.tempdir = str(root)
        data = root / 'pgdata'
        password = secrets.token_urlsafe(24)
        password_file = root / 'synthetic-test-password'
        password_file.write_text(password, encoding='ascii')
        def execute(name, *arguments):
            # A file prevents daemon children from retaining captured pipe handles.
            log = root / f'{name}-command.log'
            with log.open('wb') as output:
                result = subprocess.run([str(args.postgres_bin / f'{name}{suffix}'), *map(str, arguments)],
                    stdout=output, stderr=subprocess.STDOUT, creationflags=flags, timeout=60)
            if result.returncode:
                failure_log = args.work_dir / f'{name}-failure.log'
                failure_log.write_bytes(log.read_bytes() + ((root / 'pg.log').read_bytes() if (root / 'pg.log').exists() else b''))
                raise RuntimeError(f'{name} failed; see {failure_log}')
        execute('initdb', '-D', data, '-U', 'archive_contract', '-A', 'scram-sha-256',
                '--pwfile', password_file, '--encoding=UTF8', '--locale=C')
        with socket.socket() as port_socket:
            port_socket.bind(('127.0.0.1', 0))
            port = port_socket.getsockname()[1]
        started = False
        renderer_server = None
        try:
            execute('pg_ctl', '-D', data, '-l', root / 'pg.log', '-o', f'-h 127.0.0.1 -p {port}', '-w', 'start')
            started = True
            os.environ.update(DATABASE_URL=f'postgresql://archive_contract:{password}@127.0.0.1:{port}/postgres',
                APP_ENV='test', STORE_VERIFIER_MODE='fake', REPORT_ASSET_DIR=str(root / 'assets'),
                SHARED_WEB_REPO=str(args.web_repo.resolve()), PLATFORM_API_BASE_URL='')
            import psycopg
            with psycopg.connect(os.environ['DATABASE_URL'], autocommit=True) as connection:
                for name in ('009_mobile_platform.sql', '010_mobile_auth_hardening.sql', '011_shared_archives.sql', '012_identity_connections.sql'):
                    connection.execute((repo / 'db' / name).read_text(encoding='utf-8'))
            sys.path.insert(0, str(repo / 'api'))
            if args.pattern != 'test_shared_archives.py':
                if os.name == 'nt':
                    import types
                    import msvcrt
                    adapter = types.ModuleType('fcntl')
                    adapter.LOCK_EX, adapter.LOCK_UN = 2, 8
                    def flock(fd, operation):
                        os.lseek(fd, 0, os.SEEK_SET)
                        msvcrt.locking(fd, msvcrt.LK_LOCK if operation == adapter.LOCK_EX else msvcrt.LK_UNLCK, 1)
                    adapter.flock = flock
                    sys.modules['fcntl'] = adapter
                sys.path.insert(0, str(args.web_repo.resolve()))
                import mobile_report_renderer as renderer
                renderer.load_verified_manifest()
                os.environ['SERVER_LOG_FILE'] = str(root / 'api.log')
                os.environ['PLATFORM_INTERNAL_SERVICE_TOKEN'] = 'test-internal-token-with-sufficient-length'
                class RendererHandler(BaseHTTPRequestHandler):
                    def do_POST(self):
                        if self.path != '/api/internal/mobile-report-pdf' or self.headers.get('X-Internal-Service-Token') != os.environ['PLATFORM_INTERNAL_SERVICE_TOKEN']:
                            self.send_error(403)
                            return
                        size = int(self.headers.get('Content-Length', '0'))
                        if not 0 < size <= 100663296:
                            self.send_error(413)
                            return
                        try:
                            data = renderer.render_mobile_report_pdf(json.loads(self.rfile.read(size)))
                            status = 200
                        except renderer.CanonicalRenderError as exc:
                            data, status = json.dumps({'detail': exc.code}).encode(), exc.status
                        self.send_response(status)
                        self.send_header('X-Report-Renderer', 'chromium-skia')
                        self.send_header('Content-Length', str(len(data)))
                        self.end_headers()
                        self.wfile.write(data)
                    def log_message(self, *args): pass
                renderer_server = ThreadingHTTPServer(('127.0.0.1', 0), RendererHandler)
                threading.Thread(target=renderer_server.serve_forever, daemon=True).start()
                os.environ['MOBILE_REPORT_RENDERER_URL'] = f'http://127.0.0.1:{renderer_server.server_port}/api/internal/mobile-report-pdf'
                os.environ['MOBILE_REPORT_RENDERER_ALLOWED_HOSTS'] = '127.0.0.1,web'
                # Test modules shared by the web repo must resolve to the API suite.
                sys.path.insert(0, str(repo / 'api/tests'))
            suite = unittest.defaultTestLoader.discover(str(repo / 'api/tests'), pattern=args.pattern)
            if args.case:
                suite = unittest.defaultTestLoader.loadTestsFromNames(args.case)
            log_path = args.work_dir / 'shared-archive-tests.log'
            with log_path.open('w', encoding='utf-8') as log:
                result = unittest.TextTestRunner(stream=log, verbosity=2).run(suite)
            print(json.dumps({'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors),
                              'skipped': len(result.skipped), 'log': str(log_path)}), flush=True)
            for case, error in result.failures + result.errors:
                print(f'{case.id()}: {error.splitlines()[-1]}', flush=True)
            return 0 if result.wasSuccessful() else 1
        finally:
            if renderer_server:
                renderer_server.shutdown()
                renderer_server.server_close()
            if started:
                execute('pg_ctl', '-D', data, '-m', 'fast', '-w', 'stop')
            logging.shutdown()
            tempfile.tempdir = None


if __name__ == '__main__':
    raise SystemExit(main())
