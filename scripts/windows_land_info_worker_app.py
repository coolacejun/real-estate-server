#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, X, BooleanVar, StringVar, Tk, messagebox
from tkinter import ttk
from typing import Any


TASK_NAME = "LandInfoWorker"
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
CREATE_NEW_PROCESS_GROUP = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
DATASET_LABELS = {
    "AL_D155": "토지이용계획",
    "AL_D157": "토지이동",
    "AL_D161": "토지소유",
    "AL_D195": "토지특성",
}


def decode_text(data: bytes) -> str:
    for encoding in ("utf-16", "utf-8", "cp949", "euc-kr"):
        try:
            return data.decode(encoding)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def read_tail(path: Path, max_bytes: int = 80_000) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as handle:
        try:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes), os.SEEK_SET)
        except OSError:
            pass
        return decode_text(handle.read())


def now_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def hidden_subprocess_flags(extra: int = 0) -> int:
    return CREATE_NO_WINDOW | extra


class LandInfoWorkerApp:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.worker_dir = root_dir / "worker"
        self.requests_dir = root_dir / "requests"
        self.downloads_dir = root_dir / "downloads"
        self.manifests_dir = root_dir / "manifests"
        self.logs_dir = root_dir / "logs"
        self.lock_path = root_dir / ".land_info_worker.lock"
        self.current_process: subprocess.Popen[bytes] | None = None

        self.window = Tk()
        self.window.title("토지정보 다운로드 워커")
        self.window.geometry("1120x780")
        self.window.minsize(920, 640)

        self.status_var = StringVar(value="상태 확인 중")
        self.request_var = StringVar(value="-")
        self.snapshot_var = StringVar(value="-")
        self.task_var = StringVar(value="-")
        self.log_var = StringVar(value="-")
        self.autorefresh_var = BooleanVar(value=True)

        self._build_ui()
        self.refresh()
        self._tick()

    def _build_ui(self) -> None:
        main = ttk.Frame(self.window, padding=12)
        main.pack(fill=BOTH, expand=True)

        header = ttk.Frame(main)
        header.pack(fill=X)
        ttk.Label(header, text="토지정보 다운로드 워커", font=("Segoe UI", 18, "bold")).pack(side=LEFT)
        ttk.Label(header, textvariable=self.status_var, font=("Segoe UI", 11)).pack(side=RIGHT)

        summary = ttk.LabelFrame(main, text="요청 상태", padding=10)
        summary.pack(fill=X, pady=(12, 8))

        self.progress = ttk.Progressbar(summary, orient="horizontal", mode="determinate", maximum=100)
        self.progress.pack(fill=X, pady=(0, 8))

        grid = ttk.Frame(summary)
        grid.pack(fill=X)
        ttk.Label(grid, text="요청").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=2)
        ttk.Label(grid, textvariable=self.request_var).grid(row=0, column=1, sticky="w", pady=2)
        ttk.Label(grid, text="스냅샷").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=2)
        ttk.Label(grid, textvariable=self.snapshot_var).grid(row=1, column=1, sticky="w", pady=2)
        ttk.Label(grid, text="예약 작업").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=2)
        ttk.Label(grid, textvariable=self.task_var).grid(row=2, column=1, sticky="w", pady=2)
        ttk.Label(grid, text="최신 로그").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=2)
        ttk.Label(grid, textvariable=self.log_var).grid(row=3, column=1, sticky="w", pady=2)
        grid.columnconfigure(1, weight=1)

        buttons = ttk.Frame(main)
        buttons.pack(fill=X, pady=(0, 8))
        ttk.Button(buttons, text="새로고침", command=self.refresh).pack(side=LEFT, padx=(0, 6))
        ttk.Button(buttons, text="수동 실행", command=self.start_worker).pack(side=LEFT, padx=6)
        ttk.Button(buttons, text="예약 즉시 실행", command=self.run_task_now).pack(side=LEFT, padx=6)
        ttk.Button(buttons, text="중단", command=self.stop_worker).pack(side=LEFT, padx=6)
        ttk.Button(buttons, text="완전히 종료", command=self.full_shutdown).pack(side=LEFT, padx=6)
        ttk.Button(buttons, text="예약 켜기", command=lambda: self.change_task(True)).pack(side=LEFT, padx=6)
        ttk.Button(buttons, text="예약 끄기", command=lambda: self.change_task(False)).pack(side=LEFT, padx=6)
        ttk.Button(buttons, text="잠금 삭제", command=self.delete_lock).pack(side=LEFT, padx=6)
        ttk.Button(buttons, text="다운로드 폴더", command=lambda: self.open_path(self.downloads_dir)).pack(side=RIGHT, padx=(6, 0))
        ttk.Button(buttons, text="로그 폴더", command=lambda: self.open_path(self.logs_dir)).pack(side=RIGHT, padx=6)
        ttk.Checkbutton(buttons, text="자동 새로고침", variable=self.autorefresh_var).pack(side=RIGHT, padx=10)

        body = ttk.PanedWindow(main, orient="vertical")
        body.pack(fill=BOTH, expand=True)

        table_frame = ttk.LabelFrame(body, text="파일 수신 현황", padding=8)
        body.add(table_frame, weight=1)

        self.tree = ttk.Treeview(
            table_frame,
            columns=("name", "base_date", "expected", "zip", "partial", "missing", "size", "state"),
            show="headings",
            height=8,
        )
        columns = [
            ("name", "데이터", 150),
            ("base_date", "기준일", 100),
            ("expected", "필요", 70),
            ("zip", "완료 ZIP", 80),
            ("partial", "다운로드중", 90),
            ("missing", "남음", 70),
            ("size", "용량", 90),
            ("state", "상태", 120),
        ]
        for key, label, width in columns:
            self.tree.heading(key, text=label)
            self.tree.column(key, width=width, anchor="center")
        self.tree.pack(fill=BOTH, expand=True)

        log_frame = ttk.LabelFrame(body, text="최신 로그", padding=8)
        body.add(log_frame, weight=2)
        self.log_text = ttk.Frame(log_frame)
        self.log_box = None
        import tkinter as tk

        self.log_box = tk.Text(log_frame, wrap="word", height=18, font=("Consolas", 10))
        scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_box.yview)
        self.log_box.configure(yscrollcommand=scroll.set)
        self.log_box.pack(side=LEFT, fill=BOTH, expand=True)
        scroll.pack(side=RIGHT, fill="y")

    def request_path(self) -> Path:
        latest = self.requests_dir / "latest_land_info_full_request.json"
        if latest.exists():
            return latest
        candidates = sorted(self.requests_dir.glob("land_info_full_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0] if candidates else latest

    def latest_log(self) -> Path | None:
        candidates = sorted(self.logs_dir.glob("land_info_worker_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0] if candidates else None

    def expected(self, request: dict[str, Any]) -> tuple[Counter[str], dict[str, str]]:
        counts: Counter[str] = Counter()
        dates: dict[str, str] = {}
        for item in request.get("items") or []:
            if not isinstance(item, dict):
                continue
            code = str(item.get("dataset_code") or "").strip()
            if not code:
                continue
            counts[code] += 1
            dates[code] = str(item.get("base_date") or "").strip()
        return counts, dates

    def download_counts(self) -> tuple[Counter[str], Counter[str], defaultdict[str, int]]:
        zips: Counter[str] = Counter()
        partials: Counter[str] = Counter()
        bytes_by_code: defaultdict[str, int] = defaultdict(int)
        for path in self.downloads_dir.glob("*"):
            if not path.is_file():
                continue
            name = path.name.upper()
            match = re.match(r"^(AL_D\d+)_", name)
            if not match:
                continue
            code = match.group(1)
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            if name.endswith(".ZIP"):
                zips[code] += 1
                bytes_by_code[code] += size
            elif ".PARTIAL" in name or name.endswith(".RAON"):
                partials[code] += 1
        return zips, partials, bytes_by_code

    def completed_manifest(self, request_id: str) -> dict[str, Any]:
        if not request_id:
            return {}
        return load_json(self.manifests_dir / f"{request_id}.completed.json")

    def task_status(self) -> str:
        try:
            proc = subprocess.run(
                ["schtasks", "/Query", "/TN", TASK_NAME, "/FO", "LIST", "/V"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=8,
                check=False,
                creationflags=hidden_subprocess_flags(),
            )
        except Exception as exc:
            return f"확인 실패: {exc}"
        text = decode_text(proc.stdout)
        if proc.returncode != 0:
            return "등록 안 됨"
        lines = []
        for raw in text.splitlines():
            if any(key in raw for key in ("Status:", "상태:", "Next Run Time:", "다음 실행 시간:", "Task To Run:", "실행할 작업:")):
                lines.append(raw.strip())
        return " | ".join(lines[:3]) if lines else "등록됨"

    def lock_status(self) -> str:
        if self.current_process and self.current_process.poll() is None:
            return f"수동 실행 중 pid={self.current_process.pid}"
        if not self.lock_path.exists():
            return "대기"
        text = self.lock_path.read_text(encoding="utf-8", errors="replace").strip().replace("\n", " / ")
        return f"실행 중 또는 잠금 있음: {text}"

    def refresh(self) -> None:
        request = load_json(self.request_path())
        request_id = str(request.get("request_id") or "")
        snapshot = str(request.get("snapshot_key") or "-")
        expected, dates = self.expected(request)
        zips, partials, bytes_by_code = self.download_counts()
        completed = self.completed_manifest(request_id)

        total_expected = sum(expected.values())
        total_zip = sum(min(zips[code], expected[code]) for code in expected)
        percent = int((total_zip / total_expected) * 100) if total_expected else 0
        self.progress["value"] = percent

        state = "완료" if completed and total_zip >= total_expected and total_expected else self.lock_status()
        self.status_var.set(f"{state} · {total_zip}/{total_expected} ({percent}%)")
        self.request_var.set(request_id or "요청 파일 없음")
        self.snapshot_var.set(snapshot)
        self.task_var.set(self.task_status())

        for item in self.tree.get_children():
            self.tree.delete(item)

        for code in sorted(expected):
            done = min(zips[code], expected[code])
            missing = max(0, expected[code] - done)
            if missing == 0 and partials[code] == 0:
                row_state = "완료"
            elif partials[code]:
                row_state = "다운로드 중"
            elif done:
                row_state = "일부 수신"
            else:
                row_state = "대기"
            self.tree.insert(
                "",
                END,
                values=(
                    f"{DATASET_LABELS.get(code, code)} ({code})",
                    dates.get(code, "-"),
                    expected[code],
                    zips[code],
                    partials[code],
                    missing,
                    self.format_bytes(bytes_by_code[code]),
                    row_state,
                ),
            )

        latest = self.latest_log()
        if latest:
            self.log_var.set(f"{latest.name} · {dt.datetime.fromtimestamp(latest.stat().st_mtime).strftime('%H:%M:%S')}")
            log_text = read_tail(latest)
        else:
            self.log_var.set("로그 없음")
            log_text = ""
        if self.log_box is not None:
            self.log_box.delete("1.0", END)
            self.log_box.insert(END, log_text)
            self.log_box.see(END)

    @staticmethod
    def format_bytes(size: int) -> str:
        value = float(size)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if value < 1024 or unit == "TB":
                return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
            value /= 1024
        return f"{size} B"

    def run_command(self, command: list[str], title: str) -> None:
        try:
            proc = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=15,
                check=False,
                creationflags=hidden_subprocess_flags(),
            )
            output = decode_text(proc.stdout).strip()
            if proc.returncode == 0:
                self.status_var.set(f"{title} 완료")
            else:
                messagebox.showwarning(title, output or f"exit={proc.returncode}")
        except Exception as exc:
            messagebox.showerror(title, str(exc))
        self.refresh()

    def run_quiet(self, command: list[str], timeout: int = 15) -> str:
        try:
            proc = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout,
                check=False,
                creationflags=hidden_subprocess_flags(),
            )
            return decode_text(proc.stdout).strip()
        except Exception as exc:
            return str(exc)

    def start_worker(self) -> None:
        if self.current_process and self.current_process.poll() is None:
            messagebox.showinfo("수동 실행", "이미 이 앱에서 실행한 워커가 동작 중입니다.")
            return
        python = self.worker_dir / ".venv" / "Scripts" / "LandInfoWorkerRunner.exe"
        if not python.exists():
            python = self.worker_dir / ".venv" / "Scripts" / "python.exe"
        script = self.worker_dir / "windows_land_info_worker.py"
        if not python.exists() or not script.exists():
            messagebox.showerror("수동 실행", "Python venv 또는 워커 스크립트를 찾을 수 없습니다. setup_windows_land_info_worker.cmd를 먼저 실행하세요.")
            return
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.logs_dir / f"land_info_worker_gui_{now_stamp()}.log"
        try:
            handle = log_path.open("wb")
            self.current_process = subprocess.Popen(
                [str(python), str(script), "--root", str(self.root_dir)],
                stdout=handle,
                stderr=subprocess.STDOUT,
                cwd=str(self.root_dir),
                creationflags=hidden_subprocess_flags(CREATE_NEW_PROCESS_GROUP),
            )
            self.status_var.set(f"수동 실행 시작 pid={self.current_process.pid}")
        except Exception as exc:
            messagebox.showerror("수동 실행", str(exc))
        self.refresh()

    def stop_worker(self) -> None:
        self.run_command(["schtasks", "/End", "/TN", TASK_NAME], "예약 실행 중단")
        if self.current_process and self.current_process.poll() is None:
            try:
                self.current_process.terminate()
                time.sleep(1)
                if self.current_process.poll() is None:
                    self.current_process.kill()
            except Exception as exc:
                messagebox.showwarning("수동 실행 중단", str(exc))
        self.refresh()

    def full_shutdown(self) -> None:
        if not messagebox.askyesno(
            "완전히 종료",
            "예약 작업을 끄고, 실행 중인 토지정보 워커를 종료하고, 잠금 파일을 삭제합니다. 계속할까요?",
        ):
            return

        notes = []
        notes.append(self.run_quiet(["schtasks", "/Change", "/TN", TASK_NAME, "/DISABLE"]))
        notes.append(self.run_quiet(["schtasks", "/End", "/TN", TASK_NAME]))

        if self.current_process and self.current_process.poll() is None:
            try:
                self.current_process.terminate()
                time.sleep(1)
                if self.current_process.poll() is None:
                    self.current_process.kill()
                notes.append("GUI-launched worker stopped.")
            except Exception as exc:
                notes.append(f"GUI-launched worker stop failed: {exc}")

        notes.append(self.stop_external_worker_processes())

        if self.lock_path.exists():
            try:
                self.lock_path.unlink()
                notes.append("Lock file removed.")
            except Exception as exc:
                notes.append(f"Lock remove failed: {exc}")

        self.status_var.set("완전히 종료됨")
        self.refresh()

    def stop_external_worker_processes(self) -> str:
        script_name = "windows_land_info_worker.py"
        app_name = "windows_land_info_worker_app.py"
        powershell = (
            "$ErrorActionPreference='SilentlyContinue'; "
            f"Get-CimInstance Win32_Process | "
            f"Where-Object {{ $_.CommandLine -like '*{script_name}*' -and $_.CommandLine -notlike '*{app_name}*' }} | "
            "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
        )
        return self.run_quiet(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", powershell])

    def run_task_now(self) -> None:
        self.run_command(["schtasks", "/Run", "/TN", TASK_NAME], "예약 즉시 실행")

    def change_task(self, enabled: bool) -> None:
        flag = "/ENABLE" if enabled else "/DISABLE"
        title = "예약 켜기" if enabled else "예약 끄기"
        self.run_command(["schtasks", "/Change", "/TN", TASK_NAME, flag], title)

    def delete_lock(self) -> None:
        if not self.lock_path.exists():
            self.refresh()
            return
        if not messagebox.askyesno("잠금 삭제", "실행 중인 워커가 없을 때만 삭제해야 합니다. 잠금 파일을 삭제할까요?"):
            return
        try:
            self.lock_path.unlink()
        except Exception as exc:
            messagebox.showerror("잠금 삭제", str(exc))
        self.refresh()

    def open_path(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        try:
            os.startfile(str(path))  # type: ignore[attr-defined]
        except Exception as exc:
            messagebox.showerror("폴더 열기", str(exc))

    def _tick(self) -> None:
        if self.autorefresh_var.get():
            self.refresh()
        self.window.after(5000, self._tick)

    def run(self) -> None:
        self.window.mainloop()


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    root_dir = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else script_dir.parent
    app = LandInfoWorkerApp(root_dir)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
