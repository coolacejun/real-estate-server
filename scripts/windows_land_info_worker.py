#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as dt
import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


DEFAULT_BUTTON_TEXTS = [
    "확인",
    "예",
    "폴더선택",
    "폴더 선택",
    "찾아보기",
    "시작",
    "다운로드",
    "저장",
    "열기",
    "계속",
    "OK",
    "Yes",
    "Browse",
    "Select Folder",
    "Start",
    "Download",
    "Save",
    "Open",
    "Continue",
]

ONE_SHOT_BUTTON_KEYWORDS = [
    "폴더선택",
    "폴더 선택",
    "찾아보기",
    "시작",
    "다운로드",
    "저장",
    "browse",
    "select folder",
    "start",
    "download",
    "save",
]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    default_root = script_dir.parent
    parser = argparse.ArgumentParser(description="VWorld land_info RaonK Windows download worker.")
    parser.add_argument("--root", default=os.getenv("LAND_INFO_WORKER_ROOT", str(default_root)))
    parser.add_argument("--config", default=os.getenv("LAND_INFO_WORKER_CONFIG", ""))
    parser.add_argument("--request", default=os.getenv("LAND_INFO_WORKER_REQUEST", ""))
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-redownload", action="store_true")
    parser.add_argument("--keep-browser-open", action="store_true")
    parser.add_argument("--timeout-minutes", type=float, default=float(os.getenv("LAND_INFO_WORKER_TIMEOUT_MINUTES", "720") or "720"))
    parser.add_argument("--stable-seconds", type=float, default=float(os.getenv("LAND_INFO_WORKER_STABLE_SECONDS", "90") or "90"))
    parser.add_argument("--trigger-gap-seconds", type=float, default=float(os.getenv("LAND_INFO_WORKER_TRIGGER_GAP_SECONDS", "20") or "20"))
    parser.add_argument("--ui-click-seconds", type=float, default=float(os.getenv("LAND_INFO_WORKER_UI_CLICK_SECONDS", "90") or "90"))
    return parser.parse_args()


def worker_paths(root: Path) -> dict[str, Path]:
    return {
        "root": root,
        "requests": root / "requests",
        "downloads": root / "downloads",
        "manifests": root / "manifests",
        "logs": root / "logs",
        "profile": root / "browser-profile",
    }


def load_config(args: argparse.Namespace, paths: dict[str, Path]) -> dict[str, Any]:
    if args.config:
        config_path = Path(args.config)
    else:
        worker_config_path = paths["root"] / "worker" / "worker_config.json"
        legacy_config_path = paths["root"] / "worker_config.json"
        config_path = worker_config_path if worker_config_path.exists() else legacy_config_path
    config = load_json(config_path)
    config.setdefault("vworld_user_id", os.getenv("VWORLD_USER_ID", ""))
    config.setdefault("vworld_user_password", os.getenv("VWORLD_USER_PASSWORD", ""))
    config.setdefault("browser_channel", os.getenv("LAND_INFO_WORKER_BROWSER_CHANNEL", "msedge"))
    config.setdefault("download_dir", str(paths["downloads"]))
    config.setdefault("set_download_path_in_raonk", False)
    config.setdefault("button_texts", DEFAULT_BUTTON_TEXTS)
    config.setdefault("trigger_mode", "selection_url")
    config["_config_path"] = str(config_path)
    return config


def select_request(args: argparse.Namespace, paths: dict[str, Path]) -> Path:
    if args.request:
        return Path(args.request)
    latest = paths["requests"] / "latest_land_info_full_request.json"
    if latest.exists():
        return latest
    candidates = sorted(paths["requests"].glob("land_info_full_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise RuntimeError(f"request manifest not found: {paths['requests']}")
    return candidates[0]


def expected_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for item in items:
        code = str(item.get("dataset_code") or "").strip()
        if code:
            counts[code] += 1
    return dict(counts)


def item_date_compact(item: dict[str, Any]) -> str:
    base_date = str(item.get("base_date") or "").strip()
    return base_date.replace("-", "")


def matching_zip_files(download_dir: Path, items: list[dict[str, Any]], stable_seconds: float) -> tuple[list[Path], dict[str, Any]]:
    expected_by_code = expected_counts(items)
    date_by_code: dict[str, str] = {}
    for item in items:
        code = str(item.get("dataset_code") or "").strip()
        if code:
            date_by_code[code] = item_date_compact(item)

    found_by_code: dict[str, list[Path]] = {code: [] for code in expected_by_code}
    now = time.time()
    for path in download_dir.rglob("*.zip"):
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat.st_size <= 0:
            continue
        if now - stat.st_mtime < stable_seconds:
            continue
        name = path.name.upper()
        for code, compact in date_by_code.items():
            if re.fullmatch(rf"{re.escape(code)}_[0-9A-Z]+_{re.escape(compact)}\.ZIP", name):
                found_by_code.setdefault(code, []).append(path)
                break

    selected: list[Path] = []
    found_counts: dict[str, int] = {}
    missing_counts: dict[str, int] = {}
    for code, expected in expected_by_code.items():
        paths = sorted({p.resolve() for p in found_by_code.get(code, [])})
        found_counts[code] = len(paths)
        if len(paths) < expected:
            missing_counts[code] = expected - len(paths)
        selected.extend(paths[:expected])
    return selected, {
        "expected_counts": expected_by_code,
        "found_counts": found_counts,
        "missing_counts": missing_counts,
    }


def selection_urls(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[str(item.get("dataset_code") or "unknown")].append(item)

    out: list[dict[str, Any]] = []
    for code in sorted(grouped):
        group = grouped[code]
        ds_values: list[str] = []
        page_id = str(group[0].get("page_id") or "")
        for item in group:
            ds_file_id = str(item.get("ds_file_id") or "").strip()
            file_no = str(item.get("file_no") or "").strip()
            if ds_file_id and file_no:
                ds_values.append(ds_file_id + file_no)
        url = "https://www.vworld.kr/dtmk/downloadDtnaResourceFile.do?" + urlencode({"ds_file_sq": ",".join(ds_values)})
        list_url = "https://www.vworld.kr/dtmk/dtmk_ntads_s002.do?" + urlencode(
            {
                "pageIndex": "1",
                "gidmCd": "01",
                "gidsCd": "0108",
                "sortType": "00",
                "svcCde": "NA",
                "dsId": page_id,
                "dataSetSeq": page_id,
                "listPageIndex": "1",
                "datPageIndex": "1",
                "datPageSize": "100",
                "pageSize": "100",
                "pageUnit": "100",
                "fileGbnCd": "AL",
                "formatSelect": "CSV",
            }
        )
        out.append({"dataset_code": code, "count": len(group), "url": url, "referer": list_url})
    return out


def click_raonk_windows(
    button_texts: list[str],
    seconds: float,
    download_dir: Path,
    set_download_path: bool,
) -> list[str]:
    events: list[str] = []
    try:
        from pywinauto import Desktop
    except Exception as exc:
        return [f"pywinauto unavailable: {exc}"]

    deadline = time.time() + max(0.0, seconds)
    targets = [text.lower() for text in button_texts if text]
    one_shot_targets = [text.lower() for text in ONE_SHOT_BUTTON_KEYWORDS]
    clicked_once: set[str] = set()
    window_keywords = ["raon", "k", "download", "다운로드", "저장", "폴더", "folder", "vworld", "브이월드"]
    while time.time() < deadline:
        clicked = False
        try:
            windows = Desktop(backend="uia").windows()
        except Exception:
            windows = []
        for window in windows:
            try:
                title = str(window.window_text() or "")
            except Exception:
                title = ""
            title_l = title.lower()
            if not any(keyword in title_l for keyword in window_keywords):
                continue
            try:
                if set_download_path:
                    edits = window.descendants(control_type="Edit")
                    for edit in edits:
                        try:
                            text = str(edit.window_text() or "")
                            if text and (":\\" in text or "\\\\" in text):
                                continue
                            if edit.is_enabled():
                                edit.set_edit_text(str(download_dir))
                                events.append(f"set path: {title}")
                        except Exception:
                            continue
                buttons = window.descendants(control_type="Button")
            except Exception:
                buttons = []
            for button in buttons:
                try:
                    label = str(button.window_text() or "").strip()
                    if not label:
                        continue
                    label_l = label.lower()
                    if any(target in label_l for target in targets) and button.is_enabled():
                        click_key = f"{title_l}|{label_l}"
                        one_shot = any(target in label_l for target in one_shot_targets)
                        if one_shot and click_key in clicked_once:
                            continue
                        button.click_input()
                        if one_shot:
                            clicked_once.add(click_key)
                        events.append(f"clicked: {title} / {label}")
                        clicked = True
                        time.sleep(1.0)
                        break
                except Exception:
                    continue
            if clicked:
                break
        time.sleep(0.8 if clicked else 1.5)
    return events


def run_browser_flow(
    request: dict[str, Any],
    config: dict[str, Any],
    args: argparse.Namespace,
    paths: dict[str, Path],
) -> dict[str, Any]:
    user_id = str(config.get("vworld_user_id") or "").strip()
    password = str(config.get("vworld_user_password") or "").strip()

    items = [item for item in request.get("items") or [] if isinstance(item, dict)]
    groups = selection_urls(items)
    if not groups:
        raise RuntimeError("no downloadable groups in request manifest")

    paths["downloads"].mkdir(parents=True, exist_ok=True)
    paths["profile"].mkdir(parents=True, exist_ok=True)
    events: list[str] = []

    if args.dry_run:
        return {"groups": groups, "events": ["dry_run"]}

    try:
        print("[worker] loading playwright", flush=True)
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError("playwright is not installed. Run install_windows_land_info_worker.ps1 first.") from exc
    if not user_id or not password:
        raise RuntimeError("vworld_user_id/vworld_user_password are required in worker_config.json or environment")

    with sync_playwright() as p:
        channel = str(config.get("browser_channel") or "msedge").strip() or "msedge"
        launch_kwargs: dict[str, Any] = {
            "headless": bool(args.headless),
            "accept_downloads": True,
            "downloads_path": str(paths["downloads"]),
            "args": ["--disable-popup-blocking"],
        }
        try:
            print(f"[worker] opening browser channel={channel}", flush=True)
            context = p.chromium.launch_persistent_context(str(paths["profile"]), channel=channel, **launch_kwargs)
        except Exception:
            print("[worker] opening bundled chromium", flush=True)
            context = p.chromium.launch_persistent_context(str(paths["profile"]), **launch_kwargs)
        page = context.pages[0] if context.pages else context.new_page()
        page.set_default_timeout(60000)

        first_referer = groups[0]["referer"]
        print("[worker] loading vworld page", flush=True)
        page.goto(first_referer, wait_until="domcontentloaded")
        print("[worker] logging in", flush=True)
        login_result = page.evaluate(
            """
            async ({userId, password}) => {
              const body = new URLSearchParams({
                usrIdeE: btoa(unescape(encodeURIComponent(userId))),
                usrPwdE: btoa(unescape(encodeURIComponent(password))),
                nextUrl: ""
              });
              const res = await fetch("/v4po_usrlogin_a004.do", {
                method: "POST",
                headers: {
                  "X-Requested-With": "XMLHttpRequest",
                  "Content-Type": "application/x-www-form-urlencoded"
                },
                body
              });
              return await res.json();
            }
            """,
            {"userId": user_id, "password": password},
        )
        result = ((login_result or {}).get("resultMap") or {}).get("result")
        if result != "success":
            message = ((login_result or {}).get("resultMap") or {}).get("msg") or login_result
            raise RuntimeError(f"VWorld login failed: {message}")
        events.append("login success")

        for group in groups:
            code = group["dataset_code"]
            print(f"[worker] trigger {code} count={group['count']}", flush=True)
            page.goto(group["referer"], wait_until="domcontentloaded")
            page.evaluate(
                """
                (url) => {
                  const frame = document.createElement("iframe");
                  frame.style.display = "none";
                  frame.src = url;
                  document.body.appendChild(frame);
                }
                """,
                group["url"],
            )
            events.append(f"triggered {code} count={group['count']}")
            events.extend(click_raonk_windows(
                list(config.get("button_texts") or DEFAULT_BUTTON_TEXTS),
                args.ui_click_seconds,
                paths["downloads"],
                bool(config.get("set_download_path_in_raonk")),
            ))
            time.sleep(max(1.0, args.trigger_gap_seconds))

        if not args.keep_browser_open:
            context.close()

    return {"groups": groups, "events": events}


def build_completed_manifest(request: dict[str, Any], files: list[Path], status: str, extra: dict[str, Any]) -> dict[str, Any]:
    entries = []
    for path in sorted(files):
        try:
            stat = path.stat()
        except OSError:
            continue
        entries.append(
            {
                "file_name": path.name,
                "path": str(path),
                "size": int(stat.st_size),
                "modified_at": dt.datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(timespec="seconds"),
            }
        )
    return {
        "request_id": request.get("request_id"),
        "status": status,
        "created_at": now_iso(),
        "snapshot_key": request.get("snapshot_key"),
        "expected_count": request.get("expected_count"),
        "files": entries,
        **extra,
    }


def process_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL

            process_query_limited_information = 0x1000
            handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            return ctypes.get_last_error() == 5
        except Exception:
            return True
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True


def read_lock_pid(lock_path: Path) -> int:
    try:
        first_line = lock_path.read_text(encoding="utf-8", errors="replace").splitlines()[0]
        return int(first_line.strip())
    except Exception:
        return 0


def acquire_lock(lock_path: Path) -> int | None:
    try:
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, f"{os.getpid()}\n{now_iso()}\n".encode("utf-8"))
        return lock_fd
    except FileExistsError:
        pid = read_lock_pid(lock_path)
        if pid and not process_exists(pid):
            print(f"[worker] removing stale lock pid={pid}: {lock_path}", flush=True)
            with contextlib.suppress(Exception):
                lock_path.unlink()
            lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(lock_fd, f"{os.getpid()}\n{now_iso()}\n".encode("utf-8"))
            return lock_fd
        print(f"[worker] lock exists, skip: {lock_path}", flush=True)
        return None


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    paths = worker_paths(root)
    for path in paths.values():
        if path.suffix:
            continue
        path.mkdir(parents=True, exist_ok=True)

    lock_path = paths["root"] / ".land_info_worker.lock"
    lock_fd: int | None = None
    lock_fd = acquire_lock(lock_path)
    if lock_fd is None:
        return 0

    config = load_config(args, paths)
    try:
        print(f"[worker] config={config.get('_config_path')}", flush=True)
        request_path = select_request(args, paths)
        request = load_json(request_path)
        if not request:
            raise SystemExit(f"invalid request manifest: {request_path}")
        request_id = str(request.get("request_id") or request_path.stem)
        items = [item for item in request.get("items") or [] if isinstance(item, dict)]
        if not items:
            raise SystemExit(f"request has no items: {request_path}")

        print(f"[worker] request={request_id} items={len(items)} root={root}", flush=True)
        completed_manifest_path = paths["manifests"] / f"{request_id}.completed.json"
        completed_manifest = load_json(completed_manifest_path)
        if completed_manifest and not args.force_redownload:
            print(
                f"[worker] completed manifest exists, skip redownload: {completed_manifest_path}",
                flush=True,
            )
            return 0

        files, counts = matching_zip_files(paths["downloads"], items, args.stable_seconds)
        if not counts.get("missing_counts"):
            manifest = build_completed_manifest(request, files, "completed", {"counts": counts, "note": "already complete"})
            write_json(paths["manifests"] / f"{request_id}.completed.json", manifest)
            print(f"[worker] already complete files={len(files)}", flush=True)
            return 0

        status = "completed"
        extra: dict[str, Any] = {"counts_before": counts}
        try:
            extra["browser"] = run_browser_flow(request, config, args, paths)
        except Exception as exc:
            status = "failed"
            extra["error"] = str(exc)
            print(f"[worker] failed: {exc}", file=sys.stderr, flush=True)

        if args.dry_run:
            files, counts = matching_zip_files(paths["downloads"], items, args.stable_seconds)
            extra["counts_after"] = counts
            manifest = build_completed_manifest(request, files, "dry_run", extra)
            write_json(paths["manifests"] / f"{request_id}.dry_run.json", manifest)
            print(f"[worker] dry_run groups={len(extra.get('browser', {}).get('groups', []))}", flush=True)
            return 0

        deadline = time.time() + max(60.0, args.timeout_minutes * 60.0)
        while time.time() < deadline:
            files, counts = matching_zip_files(paths["downloads"], items, args.stable_seconds)
            if not counts.get("missing_counts"):
                status = "completed"
                break
            print(f"[worker] waiting downloads missing={counts.get('missing_counts')}", flush=True)
            time.sleep(60)

        if counts.get("missing_counts") and status != "failed":
            status = "waiting"
        extra["counts_after"] = counts
        manifest = build_completed_manifest(request, files, status, extra)
        suffix = "completed" if status == "completed" else status
        write_json(paths["manifests"] / f"{request_id}.{suffix}.json", manifest)
        print(f"[worker] status={status} files={len(files)}", flush=True)
        return 0 if status == "completed" else 1
    finally:
        if lock_fd is not None:
            with contextlib.suppress(Exception):
                os.close(lock_fd)
        with contextlib.suppress(Exception):
            lock_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
