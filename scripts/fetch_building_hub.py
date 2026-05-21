#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import hashlib
import html
import json
import os
import re
import shutil
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any


HUB_LIST_URL = "https://www.hub.go.kr/portal/opn/lps/idx-lgcpt-pvsn-srvc-list.do"
HUB_DOWNLOAD_URL = "https://www.hub.go.kr/cmm/fms/fileOpnDown.do"
USER_AGENT = "Mozilla/5.0 (compatible; building-land-sync/1.0)"
BUILDING_REGISTER_GROUP = "03"

TARGET_TASKS: dict[str, str] = {
    "0302": "총괄표제부",
    "0303": "표제부",
    "0304": "층별개요",
    "0306": "전유공용면적",
}


@dataclass
class HubItem:
    group_code: str
    task_code: str
    task_name: str
    month: str
    file_id: str
    source_title: str
    expected_category: str
    expected_filename: str
    content_length: int | None = None
    content_disposition: str | None = None
    remote_filename: str | None = None
    raw_zip_path: str | None = None
    extracted_files: list[str] | None = None
    staging_files: list[str] | None = None
    sha256: str | None = None
    status: str = "discovered"
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="건축HUB 건축물대장 최신 4종 다운로드/압축해제/staging 준비"
    )
    parser.add_argument(
        "--base-dir",
        default=os.getenv("BUILDING_HUB_SYNC_DIR", "data/source/building_info_hub"),
        help="raw/extracted/staging/manifest를 저장할 기준 디렉터리",
    )
    parser.add_argument(
        "--page-count",
        type=int,
        default=80,
        help="건축HUB 목록 페이지에서 한 번에 요청할 항목 수",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="zip 파일을 실제 다운로드합니다. 없으면 목록과 다운로드 헤더만 확인합니다.",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="다운로드된 zip을 압축해제하고 staging 파일을 준비합니다.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="이미 받은 zip/staging 파일이 있어도 다시 씁니다.",
    )
    parser.add_argument(
        "--skip-probe",
        action="store_true",
        help="다운로드 헤더 확인을 생략합니다.",
    )
    parser.add_argument(
        "--manifest",
        default="manifest.json",
        help="base-dir 아래에 기록할 manifest 파일명",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP 요청 타임아웃(초)",
    )
    return parser.parse_args()


def _request(
    opener: urllib.request.OpenerDirector,
    url: str,
    *,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 60.0,
) -> urllib.response.addinfourl:
    request_headers = {
        "User-Agent": USER_AGENT,
        "Referer": HUB_LIST_URL,
    }
    if headers:
        request_headers.update(headers)
    req = urllib.request.Request(url, data=data, headers=request_headers)
    return opener.open(req, timeout=timeout)


def fetch_list_page(opener: urllib.request.OpenerDirector, page_count: int, timeout: float) -> tuple[str, str]:
    query = urllib.parse.urlencode(
        {
            "opnLgcptTaskSeCd": BUILDING_REGISTER_GROUP,
            "pageCountPerPage": str(max(40, page_count)),
        }
    )
    with _request(opener, f"{HUB_LIST_URL}?{query}", timeout=timeout) as resp:
        raw = resp.read()
    text = raw.decode("utf-8", "replace")
    csrf = extract_csrf(text)
    if not csrf:
        raise RuntimeError("건축HUB 목록 페이지에서 _csrf 토큰을 찾지 못했습니다.")
    return text, csrf


def extract_csrf(text: str) -> str:
    for pattern in (
        r'name=["\']_csrf["\']\s+value=["\']([^"\']+)["\']',
        r'value=["\']([^"\']+)["\']\s+name=["\']_csrf["\']',
        r'<meta\s+name=["\']_csrf["\']\s+content=["\']([^"\']+)["\']',
    ):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return html.unescape(match.group(1))
    return ""


def parse_month(title: str) -> str:
    match = re.search(r"(\d{4})년\s*(\d{1,2})월", title)
    if not match:
        return ""
    return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}"


def category_for_task(task_code: str) -> str:
    return {
        "0302": "total",
        "0303": "single",
        "0304": "floor",
        "0306": "room",
    }.get(task_code, "single")


def expected_filename_for_task(task_code: str) -> str:
    name = TARGET_TASKS.get(task_code, task_code)
    return f"{name}.txt"


def strip_tags(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value)
    return unicodedata.normalize("NFC", value).strip()


def discover_latest_items(list_html: str) -> list[HubItem]:
    # Each card carries a title and fnDownloadPop(group, task, file_id). Keep the
    # regex local to cards so older months do not accidentally pair with a new ID.
    card_pattern = re.compile(r"<li\b(?P<body>.*?)</li>", re.IGNORECASE | re.DOTALL)
    title_pattern = re.compile(r'<p\s+class=["\']tit["\']>(?P<title>.*?)</p>', re.IGNORECASE | re.DOTALL)
    download_pattern = re.compile(
        r"fnDownloadPop\(\s*['\"](?P<group>\d+)['\"]\s*,\s*['\"](?P<task>\d+)['\"]\s*,\s*['\"](?P<file>OPN\d+)['\"]\s*\)",
        re.IGNORECASE,
    )
    candidates: list[HubItem] = []
    for card_match in card_pattern.finditer(list_html):
        body = card_match.group("body")
        title_match = title_pattern.search(body)
        download_match = download_pattern.search(body)
        if not title_match or not download_match:
            continue
        group_code = download_match.group("group")
        task_code = download_match.group("task")
        if group_code != BUILDING_REGISTER_GROUP or task_code not in TARGET_TASKS:
            continue
        source_title = strip_tags(title_match.group("title"))
        month = parse_month(source_title)
        if not month:
            continue
        task_name = TARGET_TASKS[task_code]
        # Guard against similarly named non-target cards.
        if task_name not in source_title:
            continue
        candidates.append(
            HubItem(
                group_code=group_code,
                task_code=task_code,
                task_name=task_name,
                month=month,
                file_id=download_match.group("file"),
                source_title=source_title,
                expected_category=category_for_task(task_code),
                expected_filename=expected_filename_for_task(task_code),
            )
        )

    latest_by_task: dict[str, HubItem] = {}
    for item in candidates:
        current = latest_by_task.get(item.task_code)
        if current is None or item.month > current.month:
            latest_by_task[item.task_code] = item

    return [latest_by_task[code] for code in TARGET_TASKS if code in latest_by_task]


def _download_form_data(csrf: str, file_id: str) -> bytes:
    return urllib.parse.urlencode({"srvrFileNm": file_id, "_csrf": csrf}).encode("utf-8")


def _remote_filename(content_disposition: str | None) -> str | None:
    if not content_disposition:
        return None
    match = re.search(r"filename\*=UTF-8''([^;]+)", content_disposition, re.IGNORECASE)
    if match:
        return urllib.parse.unquote(match.group(1)).strip('"')
    match = re.search(r"filename=([^;]+)", content_disposition, re.IGNORECASE)
    if match:
        return urllib.parse.unquote_plus(match.group(1).strip().strip('"'))
    return None


def probe_download(
    opener: urllib.request.OpenerDirector,
    csrf: str,
    item: HubItem,
    timeout: float,
) -> None:
    data = _download_form_data(csrf, item.file_id)
    with _request(
        opener,
        HUB_DOWNLOAD_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
    ) as resp:
        headers = resp.info()
        item.content_disposition = headers.get("Content-Disposition")
        item.remote_filename = _remote_filename(item.content_disposition)
        length = headers.get("Content-Length")
        item.content_length = int(length) if length and length.isdigit() else None
        # Open + close is enough to validate headers without consuming the body.


def safe_filename(value: str) -> str:
    value = unicodedata.normalize("NFC", value)
    value = re.sub(r"[\\/:\0]+", "_", value)
    return value.strip() or "download.zip"


def download_zip(
    opener: urllib.request.OpenerDirector,
    csrf: str,
    item: HubItem,
    raw_dir: Path,
    timeout: float,
    force: bool,
) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    remote_name = item.remote_filename or f"{item.file_id}_{item.task_name}_{item.month}.zip"
    target = raw_dir / safe_filename(remote_name)
    item.raw_zip_path = str(target)
    if target.exists() and not force:
        if not zipfile.is_zipfile(target):
            print(f"[WARN] {item.task_name}: remove invalid cached zip {target}", flush=True)
            target.unlink()
        else:
            if item.content_length is not None and target.stat().st_size != item.content_length:
                print(
                    f"[WARN] {item.task_name}: remove size-mismatched cached zip "
                    f"{target} expected={item.content_length} actual={target.stat().st_size}",
                    flush=True,
                )
                target.unlink()
            else:
                item.sha256 = sha256_file(target)
                item.status = "downloaded"
                print(f"[SKIP] {item.task_name}: already downloaded {target}", flush=True)
                return target

    tmp = target.with_suffix(target.suffix + ".part")
    if tmp.exists():
        tmp.unlink()

    print(
        f"[DOWNLOAD] {item.task_name} {item.month} file_id={item.file_id} "
        f"expected={format_bytes(item.content_length)}",
        flush=True,
    )
    data = _download_form_data(csrf, item.file_id)
    digest = hashlib.sha256()
    downloaded = 0
    last_report = 0
    with _request(
        opener,
        HUB_DOWNLOAD_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=timeout,
    ) as resp:
        headers = resp.info()
        item.content_disposition = headers.get("Content-Disposition")
        item.remote_filename = _remote_filename(item.content_disposition) or item.remote_filename
        length = headers.get("Content-Length")
        item.content_length = int(length) if length and length.isdigit() else item.content_length
        with tmp.open("wb") as fp:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                fp.write(chunk)
                digest.update(chunk)
                downloaded += len(chunk)
                if downloaded - last_report >= 512 * 1024 * 1024:
                    print(
                        f"[DOWNLOAD] {item.task_name}: {format_bytes(downloaded)}"
                        f"/{format_bytes(item.content_length)}",
                        flush=True,
                    )
                    last_report = downloaded

    if item.content_length is not None and tmp.stat().st_size != item.content_length:
        raise RuntimeError(
            f"download size mismatch: expected={item.content_length}, actual={tmp.stat().st_size}"
        )
    item.sha256 = digest.hexdigest()
    tmp.replace(target)
    item.raw_zip_path = str(target)
    item.status = "downloaded"
    print(f"[OK] {item.task_name}: downloaded {target} ({format_bytes(target.stat().st_size)})", flush=True)
    return target


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def is_zip_safe(zip_file: zipfile.ZipFile) -> bool:
    for info in zip_file.infolist():
        name = info.filename
        if not name or name.startswith("/") or "\\" in name:
            return False
        parts = Path(name).parts
        if any(part == ".." for part in parts):
            return False
    return True


def extract_zip(item: HubItem, zip_path: Path, extract_dir: Path, force: bool) -> list[Path]:
    target_dir = extract_dir / item.task_code
    if target_dir.exists() and force:
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    print(f"[EXTRACT] {item.task_name}: {zip_path} -> {target_dir}", flush=True)
    with zipfile.ZipFile(zip_path) as zf:
        if not is_zip_safe(zf):
            raise RuntimeError(f"unsafe zip paths: {zip_path}")
        if not any(not info.is_dir() for info in zf.infolist()):
            raise RuntimeError(f"zip has no files: {zip_path}")
        if not any(info.filename.lower().endswith(".txt") for info in zf.infolist() if not info.is_dir()):
            raise RuntimeError(f"zip has no txt files: {zip_path}")
        if force or not any(target_dir.iterdir()):
            zf.extractall(target_dir)

    files = sorted(path for path in target_dir.rglob("*") if path.is_file())
    item.extracted_files = [str(path) for path in files]
    print(f"[OK] {item.task_name}: extracted_files={len(files)}", flush=True)
    return files


def first_nonempty_line(path: Path, max_bytes: int = 1024 * 1024) -> str:
    with path.open("rb") as fp:
        read = 0
        while read < max_bytes:
            raw = fp.readline()
            if not raw:
                break
            read += len(raw)
            if raw.strip():
                for enc in ("utf-8", "cp949", "euc-kr"):
                    try:
                        return raw.decode(enc).strip()
                    except UnicodeDecodeError:
                        continue
                return raw.decode("utf-8", "ignore").strip()
    return ""


def validate_txt_for_import(item: HubItem, files: list[Path]) -> dict[str, Any]:
    txt_files = [path for path in files if path.suffix.lower() == ".txt"]
    result: dict[str, Any] = {
        "expected_category": item.expected_category,
        "txt_count": len(txt_files),
        "compatible": False,
        "messages": [],
    }
    if not txt_files:
        result["messages"].append("압축 해제 결과에 .txt 파일이 없습니다.")
        return result

    sample = first_nonempty_line(txt_files[0])
    if not sample:
        result["messages"].append(f"샘플 라인을 읽지 못했습니다: {txt_files[0].name}")
        return result
    parts = sample.split("|")
    result["sample_file"] = str(txt_files[0])
    result["sample_columns"] = len(parts)
    result["sample_preview"] = sample[:240]
    if len(parts) < 10:
        result["messages"].append("구분자 '|' 기준 컬럼 수가 너무 적습니다.")
        return result
    result["compatible"] = True
    result["messages"].append("현재 import_building_info_text.py의 | 구분 텍스트 입력과 호환됩니다.")
    return result


def prepare_staging(item: HubItem, files: list[Path], staging_dir: Path, force: bool) -> list[Path]:
    staging_dir.mkdir(parents=True, exist_ok=True)
    target = staging_dir / item.expected_filename
    if target.exists() and force:
        target.unlink()
    if target.exists() and not force:
        item.staging_files = [str(target)]
        return [target]

    txt_files = [path for path in files if path.suffix.lower() == ".txt"]
    if not txt_files:
        raise RuntimeError(f"no txt file to stage for {item.task_name}")
    # Prefer the largest txt in case the zip contains readme or split byproducts.
    source = max(txt_files, key=lambda path: path.stat().st_size)
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)
    item.staging_files = [str(target)]
    print(f"[STAGE] {item.task_name}: {source} -> {target}", flush=True)
    return [target]


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"runs": []}
    try:
        with path.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
        return data if isinstance(data, dict) else {"runs": []}
    except Exception:
        return {"runs": []}


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2, sort_keys=True)
        fp.write("\n")
    tmp.replace(path)


def format_bytes(value: int | None) -> str:
    if value is None:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f}{unit}" if unit != "B" else f"{int(amount)}B"
        amount /= 1024
    return str(value)


def main() -> int:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()
    raw_root = base_dir / "raw"
    extracted_root = base_dir / "extracted"
    staging_root = base_dir / "staging" / "full"
    manifest_path = base_dir / args.manifest

    jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    list_html, csrf = fetch_list_page(opener, args.page_count, args.timeout)
    items = discover_latest_items(list_html)
    missing = [name for code, name in TARGET_TASKS.items() if code not in {item.task_code for item in items}]
    if missing:
        raise SystemExit(f"최신 항목을 모두 찾지 못했습니다: {', '.join(missing)}")

    if not args.skip_probe:
        for item in items:
            try:
                probe_download(opener, csrf, item, args.timeout)
            except Exception as exc:
                item.error = f"probe failed: {exc}"

    latest_month = max(item.month for item in items)
    raw_dir = raw_root / latest_month
    extracted_dir = extracted_root / latest_month

    validations: dict[str, Any] = {}
    for item in items:
        try:
            if args.download:
                zip_path = download_zip(opener, csrf, item, raw_dir, args.timeout, args.force)
                if args.extract:
                    extracted_files = extract_zip(item, zip_path, extracted_dir, args.force)
                    validations[item.task_code] = validate_txt_for_import(item, extracted_files)
                    prepare_staging(item, extracted_files, staging_root, args.force)
                    item.status = "staged"
            elif args.extract:
                zip_candidates = sorted(raw_dir.glob(f"*{item.task_name}*.zip"))
                if not zip_candidates:
                    raise RuntimeError(f"downloaded zip not found for extraction: {item.task_name}")
                extracted_files = extract_zip(item, zip_candidates[-1], extracted_dir, args.force)
                validations[item.task_code] = validate_txt_for_import(item, extracted_files)
                prepare_staging(item, extracted_files, staging_root, args.force)
                item.status = "staged"
        except Exception as exc:
            item.status = "failed"
            item.error = str(exc)

    run = {
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "source": "hub.go.kr",
        "group_code": BUILDING_REGISTER_GROUP,
        "latest_month": latest_month,
        "download": bool(args.download),
        "extract": bool(args.extract),
        "items": [asdict(item) for item in items],
        "validations": validations,
    }
    manifest = load_manifest(manifest_path)
    manifest["latest"] = run
    runs = manifest.get("runs")
    if not isinstance(runs, list):
        runs = []
    runs.append(run)
    manifest["runs"] = runs[-50:]
    write_manifest(manifest_path, manifest)

    print(f"manifest={manifest_path}")
    print(f"latest_month={latest_month}")
    for item in items:
        print(
            " - "
            f"{item.task_name} {item.month} file_id={item.file_id} "
            f"size={format_bytes(item.content_length)} status={item.status}"
        )
        if item.error:
            print(f"   error={item.error}", file=sys.stderr)
    if args.extract:
        compatible = all(v.get("compatible") for v in validations.values()) and len(validations) == len(TARGET_TASKS)
        print(f"import_compatible={'yes' if compatible else 'no'}")
        print(f"staging_dir={staging_root}")

    failed = [item for item in items if item.status == "failed"]
    return 1 if failed else 0


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        raise SystemExit(main())
    raise SystemExit(130)
