#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete SMB land_info ZIP originals after a verified import.")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--smb-downloads", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    parser.add_argument("--deadline-hours", type=float, default=36.0)
    return parser.parse_args()


def write_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(message, encoding="utf-8")


def main() -> int:
    args = parse_args()
    manifest_path = Path(args.manifest)
    smb_downloads = Path(args.smb_downloads)
    log_path = Path(args.log)
    deadline = time.time() + max(1.0, float(args.deadline_hours)) * 60 * 60
    poll_seconds = max(10.0, float(args.poll_seconds))

    while time.time() < deadline:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            latest = payload.get("latest") if isinstance(payload, dict) else None
            if not isinstance(latest, dict):
                raise RuntimeError("manifest latest object not found")
            status = str(latest.get("status") or "").strip().lower()
            if status == "imported":
                deleted: list[str] = []
                missing: list[str] = []
                for raw_path in latest.get("worker_zip_files") or []:
                    name = Path(str(raw_path)).name
                    if not name:
                        continue
                    target = smb_downloads / name
                    if target.exists():
                        target.unlink()
                        deleted.append(name)
                    else:
                        missing.append(name)
                write_log(
                    log_path,
                    "completed\n"
                    f"deleted={len(deleted)}\n"
                    f"missing={len(missing)}\n"
                    + "\n".join(deleted),
                )
                return 0
            if status == "failed":
                write_log(log_path, "stopped: import failed\n" + json.dumps(latest, ensure_ascii=False)[:4000])
                return 1
            write_log(log_path, f"waiting: status={status or 'unknown'}")
        except Exception as exc:
            write_log(log_path, f"waiting: {exc}")
        time.sleep(poll_seconds)

    write_log(log_path, "timeout waiting for imported status")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
