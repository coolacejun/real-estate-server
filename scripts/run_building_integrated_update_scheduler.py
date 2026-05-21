#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run building integrated CH update cycle on an interval.")
    parser.add_argument("--repo-root", default=str(repo_root))
    parser.add_argument(
        "--base-dir",
        default=os.getenv("BUILDING_INTEGRATED_SYNC_BASE_DIR", str(repo_root / "data/source/building_integrated_info/auto")),
    )
    parser.add_argument("--api-base", default=os.getenv("BUILDING_LAND_API_BASE", os.getenv("BUILDING_INTEGRATED_SYNC_API_BASE", "http://localhost")))
    parser.add_argument("--env-file", default=str(repo_root / ".env"))
    parser.add_argument("--check-interval", type=float, default=float(os.getenv("BUILDING_INTEGRATED_SYNC_CHECK_INTERVAL_SECONDS", "86400") or "86400"))
    parser.add_argument("--retry-interval", type=float, default=float(os.getenv("BUILDING_INTEGRATED_SYNC_RETRY_SECONDS", "3600") or "3600"))
    parser.add_argument("--initial-delay", type=float, default=float(os.getenv("BUILDING_INTEGRATED_SYNC_INITIAL_DELAY_SECONDS", "0") or "0"))
    parser.add_argument("--poll-interval", type=float, default=float(os.getenv("BUILDING_INTEGRATED_SYNC_POLL_INTERVAL_SECONDS", "30") or "30"))
    parser.add_argument("--import-timeout", type=float, default=float(os.getenv("BUILDING_INTEGRATED_SYNC_IMPORT_TIMEOUT_SECONDS", "14400") or "14400"))
    parser.add_argument("--max-files", type=int, default=int(os.getenv("BUILDING_INTEGRATED_SYNC_MAX_FILES", "0") or "0"))
    return parser.parse_args()


def sleep_with_log(seconds: float, reason: str) -> None:
    safe_seconds = max(1.0, float(seconds))
    wake_at = dt.datetime.now().astimezone() + dt.timedelta(seconds=safe_seconds)
    print(
        f"[building-integrated-scheduler] sleep {int(safe_seconds)}s until {wake_at.isoformat(timespec='seconds')} reason={reason}",
        flush=True,
    )
    time.sleep(safe_seconds)


def run_cycle_once(args: argparse.Namespace) -> int:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().with_name("run_building_integrated_update_cycle.py")),
        "--repo-root",
        args.repo_root,
        "--base-dir",
        args.base_dir,
        "--api-base",
        args.api_base,
        "--env-file",
        args.env_file,
        "--poll-interval",
        str(args.poll_interval),
        "--import-timeout",
        str(args.import_timeout),
    ]
    if args.max_files > 0:
        cmd.extend(["--max-files", str(args.max_files)])
    return subprocess.run(cmd, cwd=args.repo_root).returncode


def main() -> int:
    args = parse_args()
    if args.initial_delay > 0:
        sleep_with_log(args.initial_delay, "initial_delay")
    while True:
        try:
            rc = run_cycle_once(args)
            if rc == 0:
                sleep_with_log(args.check_interval, "cycle_complete")
            else:
                sleep_with_log(args.retry_interval, "cycle_failed")
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[building-integrated-scheduler] failed: {exc}", file=sys.stderr, flush=True)
            sleep_with_log(args.retry_interval, "scheduler_error")


if __name__ == "__main__":
    raise SystemExit(main())
