#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import run_building_hub_cycle as cycle


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Run building_info cycle around the expected monthly upload window."
    )
    parser.add_argument("--repo-root", default=str(repo_root))
    parser.add_argument(
        "--base-dir",
        default=os.getenv("BUILDING_HUB_SYNC_DIR", str(repo_root / "data/source/building_info_hub")),
    )
    parser.add_argument(
        "--visible-source-dir",
        default=str(repo_root / "data/source/building_info"),
    )
    parser.add_argument("--api-base", default=os.getenv("BUILDING_LAND_API_BASE", "http://localhost"))
    parser.add_argument("--env-file", default=str(repo_root / ".env"))
    parser.add_argument("--upload-day", type=int, default=20)
    parser.add_argument("--upload-check-hour", type=int, default=13)
    parser.add_argument("--upload-window-days", type=float, default=7)
    parser.add_argument("--window-check-interval", type=float, default=6 * 60 * 60)
    parser.add_argument("--late-check-interval", type=float, default=24 * 60 * 60)
    parser.add_argument("--retry-interval", type=float, default=60 * 60)
    parser.add_argument("--poll-interval", type=float, default=30.0)
    parser.add_argument("--import-timeout", type=float, default=4 * 60 * 60)
    parser.add_argument("--initial-delay", type=float, default=0.0)
    return parser.parse_args()


def parse_timestamp(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    with contextlib.suppress(Exception):
        parsed = dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
        return parsed
    return None


def active_release(api_base: str, token: str) -> dict[str, Any] | None:
    releases = cycle.list_building_releases(api_base, token)
    return cycle.active_release_from_list(releases)


def add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    zero_based = (year * 12 + (month - 1)) + delta
    return zero_based // 12, zero_based % 12 + 1


def release_data_month(release: dict[str, Any]) -> tuple[int, int] | None:
    candidates: list[str] = []
    for key in ("version", "source_name"):
        value = release.get(key)
        if value:
            candidates.append(str(value))
    metadata = release.get("metadata")
    if isinstance(metadata, dict):
        for key in ("version", "source_name", "latest_month", "data_month"):
            value = metadata.get(key)
            if value:
                candidates.append(str(value))

    patterns = (
        re.compile(r"hub-(\d{4})(\d{2})"),
        re.compile(r"(\d{4})-(\d{1,2})"),
        re.compile(r"(\d{4})년\s*(\d{1,2})월"),
    )
    for text in candidates:
        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            year = int(match.group(1))
            month = int(match.group(2))
            if 1 <= month <= 12:
                return year, month
    return None


def fallback_next_upload_start(now: dt.datetime, upload_day: int, upload_hour: int) -> dt.datetime:
    day = max(1, min(28, int(upload_day)))
    hour = max(0, min(23, int(upload_hour)))
    start = now.replace(day=day, hour=hour, minute=0, second=0, microsecond=0)
    if start <= now:
        year, month = add_months(start.year, start.month, 1)
        start = start.replace(year=year, month=month)
    return start


def expected_upload_start(release: dict[str, Any], args: argparse.Namespace, now: dt.datetime) -> dt.datetime:
    data_month = release_data_month(release)
    if not data_month:
        return fallback_next_upload_start(now, args.upload_day, args.upload_check_hour)

    upload_year, upload_month = add_months(data_month[0], data_month[1], 2)
    day = max(1, min(28, int(args.upload_day)))
    hour = max(0, min(23, int(args.upload_check_hour)))
    return dt.datetime(upload_year, upload_month, day, hour, 0, 0, tzinfo=now.tzinfo)


def sleep_with_log(seconds: float, reason: str) -> None:
    safe_seconds = max(1.0, float(seconds))
    wake_at = dt.datetime.now().astimezone() + dt.timedelta(seconds=safe_seconds)
    print(
        f"[scheduler] sleep {int(safe_seconds)}s until {wake_at.isoformat(timespec='seconds')} reason={reason}",
        flush=True,
    )
    time.sleep(safe_seconds)


def run_cycle_once(args: argparse.Namespace) -> int:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().with_name("run_building_hub_cycle.py")),
        "--repo-root",
        args.repo_root,
        "--base-dir",
        args.base_dir,
        "--visible-source-dir",
        args.visible_source_dir,
        "--api-base",
        args.api_base,
        "--env-file",
        args.env_file,
        "--poll-interval",
        str(args.poll_interval),
        "--import-timeout",
        str(args.import_timeout),
    ]
    return subprocess.run(cmd, cwd=args.repo_root).returncode


def load_latest_cycle_status(base_dir: Path) -> str:
    manifest = base_dir / "cycle_manifest.json"
    with contextlib.suppress(Exception):
        data = json.loads(manifest.read_text(encoding="utf-8"))
        latest = data.get("latest")
        if isinstance(latest, dict):
            return str(latest.get("status") or "")
    return ""


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    api_base = args.api_base.rstrip("/")
    env = cycle.load_env_file(Path(args.env_file))
    token = os.getenv("ADMIN_TOKEN", env.get("ADMIN_TOKEN", ""))

    if args.initial_delay > 0:
        sleep_with_log(args.initial_delay, "initial_delay")

    while True:
        try:
            release = active_release(api_base, token)
            now = dt.datetime.now().astimezone()
            if release:
                upload_start = expected_upload_start(release, args, now)
                upload_end = upload_start + dt.timedelta(days=max(1.0, args.upload_window_days))
                if now < upload_start:
                    remaining = (upload_start - now).total_seconds()
                    print(
                        "[scheduler] next expected building_info upload window "
                        f"id={release.get('id')} version={release.get('version')} "
                        f"start={upload_start.isoformat(timespec='seconds')}",
                        flush=True,
                    )
                    sleep_with_log(remaining, "before_expected_upload")
                    continue
            else:
                print("[scheduler] no active building_info release; running cycle now", flush=True)

            rc = run_cycle_once(args)
            status = load_latest_cycle_status(base_dir)
            if rc != 0:
                sleep_with_log(args.retry_interval, f"cycle_failed status={status or 'unknown'}")
                continue
            if status == "imported":
                continue
            if release:
                interval = args.window_check_interval if now <= upload_end else args.late_check_interval
                reason = "within_upload_window" if now <= upload_end else "after_upload_window"
                sleep_with_log(interval, f"{reason} status={status or 'unknown'}")
            else:
                sleep_with_log(args.late_check_interval, f"checked status={status or 'unknown'}")
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[scheduler] failed: {exc}", file=sys.stderr, flush=True)
            sleep_with_log(args.retry_interval, "scheduler_error")


if __name__ == "__main__":
    raise SystemExit(main())
