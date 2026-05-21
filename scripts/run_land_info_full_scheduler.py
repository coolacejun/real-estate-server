#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run land_info full worker/import cycle on an interval.")
    parser.add_argument("--repo-root", default=str(repo_root))
    parser.add_argument(
        "--base-dir",
        default=os.getenv("LAND_INFO_SYNC_BASE_DIR", str(repo_root / "토지정보/auto")),
    )
    parser.add_argument("--worker-dir", default=os.getenv("LAND_INFO_WORKER_DIR", "/worker/land-info-worker"))
    parser.add_argument("--api-base", default=os.getenv("BUILDING_LAND_API_BASE", os.getenv("LAND_INFO_SYNC_API_BASE", "http://localhost")))
    parser.add_argument("--env-file", default=str(repo_root / ".env"))
    parser.add_argument("--check-interval", type=float, default=float(os.getenv("LAND_INFO_SYNC_CHECK_INTERVAL_SECONDS", "86400") or "86400"))
    parser.add_argument("--pending-interval", type=float, default=float(os.getenv("LAND_INFO_SYNC_PENDING_INTERVAL_SECONDS", "600") or "600"))
    parser.add_argument("--retry-interval", type=float, default=float(os.getenv("LAND_INFO_SYNC_RETRY_SECONDS", "3600") or "3600"))
    parser.add_argument("--initial-delay", type=float, default=float(os.getenv("LAND_INFO_SYNC_INITIAL_DELAY_SECONDS", "0") or "0"))
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("LAND_INFO_SYNC_POLL_INTERVAL_SECONDS", "30") or "30"))
    parser.add_argument("--import-timeout", type=float, default=float(os.getenv("LAND_INFO_SYNC_IMPORT_TIMEOUT_SECONDS", "86400") or "86400"))
    parser.add_argument("--stable-seconds", type=float, default=float(os.getenv("LAND_INFO_SYNC_STABLE_SECONDS", "60") or "60"))
    return parser.parse_args()


def sleep_with_log(seconds: float, reason: str) -> None:
    safe_seconds = max(1.0, float(seconds))
    wake_at = dt.datetime.now().astimezone() + dt.timedelta(seconds=safe_seconds)
    print(
        f"[land-info-scheduler] sleep {int(safe_seconds)}s until {wake_at.isoformat(timespec='seconds')} reason={reason}",
        flush=True,
    )
    time.sleep(safe_seconds)


def run_cycle_once(args: argparse.Namespace) -> int:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().with_name("run_land_info_full_cycle.py")),
        "--repo-root",
        args.repo_root,
        "--base-dir",
        args.base_dir,
        "--worker-dir",
        args.worker_dir,
        "--api-base",
        args.api_base,
        "--env-file",
        args.env_file,
        "--poll-interval",
        str(args.poll_interval),
        "--import-timeout",
        str(args.import_timeout),
        "--stable-seconds",
        str(args.stable_seconds),
    ]
    return subprocess.run(cmd, cwd=args.repo_root).returncode


def latest_cycle_status(base_dir: Path) -> str:
    path = base_dir / "cycle_manifest.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return ""
    latest = data.get("latest") if isinstance(data, dict) else None
    if not isinstance(latest, dict):
        return ""
    return str(latest.get("status") or "")


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base_dir)
    if args.initial_delay > 0:
        sleep_with_log(args.initial_delay, "initial_delay")
    while True:
        try:
            rc = run_cycle_once(args)
            status = latest_cycle_status(base_dir)
            if rc == 0 and status == "waiting_worker":
                sleep_with_log(args.pending_interval, "waiting_worker")
            elif rc == 0:
                sleep_with_log(args.check_interval, f"cycle_complete status={status or 'unknown'}")
            else:
                sleep_with_log(args.retry_interval, f"cycle_failed status={status or 'unknown'}")
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[land-info-scheduler] failed: {exc}", file=sys.stderr, flush=True)
            sleep_with_log(args.retry_interval, "scheduler_error")


if __name__ == "__main__":
    raise SystemExit(main())
