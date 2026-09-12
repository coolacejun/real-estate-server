"""Check the committed web pin and refuse publishing an unpublished dependency.

Default mode performs read-only ls-remote against the canonical web repository.
--local-only validates a checkout but is never sufficient authorization to push.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

WEB_REMOTE = 'https://github.com/noriddori-jpg/real_estate_web.git'
WEB_IDENTITIES = {WEB_REMOTE, 'git@github.com:noriddori-jpg/real_estate_web.git'}
WEB_BRANCH = 'refs/heads/work/shared-archives-20260911'


class ReleaseBlocked(RuntimeError):
    pass


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(['git', '-c', f'safe.directory={repo.as_posix()}', *args], cwd=repo,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=45)
    if result.returncode:
        raise ReleaseBlocked(f'Git check failed: {args[0]} (exit {result.returncode})')
    return result.stdout.strip()


def verify_manifest(web: Path) -> None:
    manifest = json.loads((web / 'report-renderer-manifest.json').read_text(encoding='utf-8'))
    if manifest.get('version') != 'web-a4-shared-v3-20260913':
        raise ReleaseBlocked('Web V3 renderer bundle is not compatible')
    if not {'canonical_v3_contract.py', 'mobile_report_renderer.py', 'script.js', 'environment_analysis.py', 'client-bridge.js'} <= set(manifest.get('assets', {})):
        raise ReleaseBlocked('Web V3 renderer assets are missing')
    for group, prefix in (('assets', ''), ('fonts', 'assets/fonts/pretendard/')):
        for name, digest in manifest[group].items():
            target = (web / prefix / name).resolve()
            if not target.is_relative_to(web.resolve()) or hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                raise ReleaseBlocked('Web renderer manifest does not match checkout bytes')
    contract = (web / 'canonical_v3_contract.py').read_text(encoding='utf-8')
    if 'RENDERER_VERSION = "web-a4-canonical-v3"' not in contract:
        raise ReleaseBlocked('Web semantic report contract is not V3')


def verify_environment_contract(repo: Path, web: Path) -> None:
    if (repo / 'api/app/platform/shared_environment.py').read_bytes() != (web / 'environment_analysis.py').read_bytes():
        raise ReleaseBlocked('Server environment calculations differ from the pinned web source')
    if (repo / 'api/app/platform/environment-data-manifest.json').read_bytes() != (web / 'environment-data-manifest.json').read_bytes():
        raise ReleaseBlocked('Server environment datasets differ from the pinned web contract')
    data_manifest = json.loads((web / 'environment-data-manifest.json').read_text(encoding='utf-8'))
    if data_manifest.get('calculationVersion') != 'environment-web-v2':
        raise ReleaseBlocked('Environment data contract version is not V2')
    for name, digest in data_manifest['files'].items():
        path = (web / 'data' / name).resolve()
        if not path.is_relative_to((web / 'data').resolve()) or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ReleaseBlocked('Pinned environment dataset bytes are inconsistent')


def verify_remote_pin(pin: str, listing: str) -> None:
    rows = [line.split() for line in listing.splitlines() if line.strip()]
    if rows != [[pin, WEB_BRANCH]]:
        raise ReleaseBlocked('Publish and verify the matching web feature branch before pushing the server pin')


def check(repo: Path, *, local_only: bool = False, server_ref: str = 'HEAD') -> dict:
    if not re.fullmatch(r'(?:HEAD|[0-9a-f]{40})', server_ref):
        raise ReleaseBlocked('A checked-out commit SHA or HEAD is required')
    current = git(repo, 'rev-parse', 'HEAD')
    if git(repo, 'rev-parse', server_ref) != current:
        raise ReleaseBlocked('Check out the server commit being published first')
    if git(repo, 'status', '--porcelain', '--untracked-files=no'):
        raise ReleaseBlocked('Commit server and submodule changes before publication')
    module_url = git(repo, 'config', '--file', '.gitmodules', '--get', 'submodule.web.url')
    if module_url not in WEB_IDENTITIES:
        raise ReleaseBlocked('Unexpected web submodule repository')
    entry = git(repo, 'ls-tree', server_ref, 'web').split()
    if len(entry) != 4 or entry[0:2] != ['160000', 'commit'] or entry[3] != 'web':
        raise ReleaseBlocked('A committed web submodule pin is required')
    pin = entry[2]
    web = repo / 'web'
    if not (web / '.git').exists() or git(web, 'rev-parse', 'HEAD') != pin:
        raise ReleaseBlocked('Initialize and check out the exact pinned web commit')
    if git(web, 'remote', 'get-url', 'origin') not in WEB_IDENTITIES:
        raise ReleaseBlocked('Unexpected checked-out web remote')
    if git(web, 'status', '--porcelain', '--untracked-files=no'):
        raise ReleaseBlocked('Pinned web checkout is modified')
    verify_manifest(web)
    verify_environment_contract(repo, web)
    if not local_only:
        verify_remote_pin(pin, git(repo, 'ls-remote', WEB_REMOTE, WEB_BRANCH))
    return {'serverCommit': current, 'webCommit': pin, 'webRemote': WEB_REMOTE,
            'webBranch': WEB_BRANCH, 'localContract': 'verified',
            'remotePublished': not local_only}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--server-ref', default='HEAD')
    parser.add_argument('--local-only', action='store_true')
    args = parser.parse_args()
    try:
        print(json.dumps(check(args.repo.resolve(), local_only=args.local_only, server_ref=args.server_ref)))
        return 0
    except (ReleaseBlocked, OSError, ValueError, subprocess.TimeoutExpired) as error:
        print(f'BLOCKED: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
