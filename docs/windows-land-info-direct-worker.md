# Windows Land Info Direct Upload Worker Spec

This document is the handoff spec for the Windows Codex session that uses a direct server API worker for land-info updates.

## Goal

The Windows worker owns update discovery and file download. The server-side land-info scheduler is not used.

Current flow:

1. The Windows worker checks the latest VWorld land-info files.
2. The worker sends that catalog to the server and asks which files are needed.
3. The server returns only the files that are not already reflected in active data.
4. The worker downloads the needed files with the official VWorld/RaonK flow.
5. After download completion, the worker uploads the downloaded ZIP files to the server by chunked API.
6. The worker reports progress and heartbeat.
7. The worker deletes local ZIPs only after the server confirms upload acceptance.

The server remains responsible for unzip, CSV import, DB verification, active release switch, previous release cleanup, and server-side source cleanup.

## Important Rules

- Do not use SMB for the new direct worker path.
- Do not redownload a completed request unless the server explicitly sends a new request or `force_redownload=true`.
- Keep the existing RaonK/browser automation behavior, including login.
- Keep the local lock file so scheduled runs cannot overlap.
- Uploads must be resumable/idempotent.
- The worker must keep a local manifest so it can recover after restart.
- Do not delete local ZIP until the server returns file status `accepted` or `processed`.
- Test mode must use the same poll/download/upload loop as production, but the server request will set `test_mode=true` and `activate=false`.

## Windows Paths

Default root:

```text
C:\land-info-worker
```

Recommended directories:

```text
C:\land-info-worker\worker
C:\land-info-worker\downloads
C:\land-info-worker\manifests
C:\land-info-worker\logs
C:\land-info-worker\browser-profile
```

Direct worker config:

```text
C:\land-info-worker\worker\worker_config.json
```

Add these fields to the existing config:

```json
{
  "server_api_base": "https://YOUR_SERVER_DOMAIN",
  "worker_id": "NUCBOX_M6",
  "worker_token": "SET_A_SERVER_ISSUED_TOKEN",
  "poll_interval_seconds": 600,
  "upload_chunk_bytes": 8388608,
  "delete_local_after_server_accept": true,
  "client_initiated_update": true,
  "server_update_ensure_path": "/v1/worker/land-info/updates/ensure",
  "source_catalog_cache_seconds": 21600,
  "download_dir": "C:\\land-info-worker\\downloads",
  "set_download_path_in_raonk": false,
  "browser_channel": "msedge"
}
```

## Auth

All worker API calls must send:

```http
X-Worker-Id: NUCBOX_M6
X-Worker-Token: <worker_token>
```

The token must not be printed to logs.

## API Contract

### 1. Heartbeat

```http
POST /v1/worker/land-info/heartbeat
```

Request:

```json
{
  "worker_id": "NUCBOX_M6",
  "hostname": "NUCBOX_M6",
  "version": "direct-upload-0.1",
  "status": "idle",
  "current_request_id": null,
  "current_file_id": null,
  "download_dir": "C:\\land-info-worker\\downloads",
  "free_bytes": 1234567890,
  "message": "polling"
}
```

Response:

```json
{
  "ok": true,
  "server_time": "2026-05-20T22:00:00+09:00"
}
```

### 2. Ensure Latest Update

The worker should call this before polling. The worker discovers VWorld's latest 4-dataset catalog locally and sends the catalog to the server. The server compares each dataset code with its own active component release and creates a request containing only changed components.

Component mapping:

```text
AL_D155 -> land_info_al_d155 -> 토지이용계획
AL_D157 -> land_info_al_d157 -> 토지이동
AL_D161 -> land_info_al_d161 -> 토지소유
AL_D195 -> land_info_al_d195 -> 토지특성
```

```http
POST /v1/worker/land-info/updates/ensure
```

Request:

```json
{
  "worker_id": "NUCBOX_M6",
  "hostname": "NUCBOX_M6",
  "version": "direct-upload-0.1",
  "activate": true,
  "test_mode": false,
  "source_catalog": {
    "source": "vworld",
    "data_type": "land_info",
    "operation_mode": "full",
    "signature": "ee3183856b059c3c...",
    "expected_count": 68,
    "datasets": [],
    "items": []
  }
}
```

Response when the active release already matches:

```json
{
  "ok": true,
  "data": {
    "created": false,
    "request_created": false,
    "reason": "already_active",
    "request_id": null,
    "component_status": {
      "AL_D155": {
        "data_type": "land_info_al_d155",
        "dataset_name": "토지이용계획",
        "up_to_date": true,
        "expected_count": 17
      }
    }
  }
}
```

Response when a request is created or already pending:

```json
{
  "ok": true,
  "data": {
    "created": true,
    "request_created": true,
    "request_id": "land_info_update_ee3183856b059c3c",
    "reason": "new_source_catalog",
    "changed_dataset_codes": ["AL_D155", "AL_D157"],
    "request": {
      "request_id": "land_info_update_ee3183856b059c3c",
      "status": "requested",
      "data_type": "land_info",
      "operation_mode": "full",
      "expected_count": 34,
      "component_dataset_codes": ["AL_D155", "AL_D157"],
      "component_data_types": ["land_info_al_d155", "land_info_al_d157"],
      "items": []
    }
  }
}
```

The worker can pass `--skip-update-check` to bypass this endpoint and only poll existing server requests.

### 3. Poll Next Request

```http
GET /v1/worker/land-info/requests/next?worker_id=NUCBOX_M6
```

Response when idle:

```json
{
  "ok": true,
  "data": null
}
```

Response with request:

```json
{
  "ok": true,
  "data": {
    "request_id": "land_info_update_15520260_9b428aad7442",
    "data_type": "land_info",
    "operation_mode": "full",
    "test_mode": false,
    "activate": true,
    "force_redownload": false,
    "expected_count": 17,
    "component_dataset_codes": ["AL_D155"],
    "component_data_types": ["land_info_al_d155"],
    "items": [
      {
        "file_id": "AL_D155_11_20260509",
        "dataset_key": "land_use_plan",
        "dataset_name": "토지이용계획",
        "dataset_code": "AL_D155",
        "page_id": "14",
        "base_date": "2026-05-09",
        "updated_date": "2026-05-12",
        "file_no": "1234",
        "ds_file_id": "20171128DS00148",
        "expected_glob": "AL_D155_11_20260509.zip",
        "size_bytes": 123456789
      }
    ]
  }
}
```

### 3. Claim Request

```http
POST /v1/worker/land-info/requests/{request_id}/claim
```

Request:

```json
{
  "worker_id": "NUCBOX_M6"
}
```

Response:

```json
{
  "ok": true,
  "data": {
    "claimed": true,
    "request_id": "land_info_update_15520260_9b428aad7442"
  }
}
```

If another worker claimed it:

```json
{
  "ok": false,
  "error": "already_claimed"
}
```

### 4. File Status

```http
POST /v1/worker/land-info/requests/{request_id}/files/{file_id}/status
```

Request examples:

```json
{
  "status": "download_started",
  "message": "triggered RaonK"
}
```

```json
{
  "status": "downloaded",
  "local_path": "C:\\land-info-worker\\downloads\\AL_D155_11_20260509.zip",
  "file_name": "AL_D155_11_20260509.zip",
  "file_size": 123456789,
  "sha256": "..."
}
```

```json
{
  "status": "failed",
  "error": "download timeout"
}
```

Allowed file statuses:

```text
pending
download_started
downloaded
upload_started
uploading
uploaded
accepted
processed
failed
```

### 5. Init Upload

```http
POST /v1/worker/land-info/uploads/init
```

Request:

```json
{
  "request_id": "land_info_update_15520260_9b428aad7442",
  "file_id": "AL_D155_11_20260509",
  "file_name": "AL_D155_11_20260509.zip",
  "file_size": 123456789,
  "sha256": "...",
  "chunk_size": 8388608
}
```

Response:

```json
{
  "ok": true,
  "data": {
    "upload_id": "upl_abc123",
    "received_chunks": [],
    "received_bytes": 0,
    "status": "uploading"
  }
}
```

If upload already completed:

```json
{
  "ok": true,
  "data": {
    "upload_id": "upl_abc123",
    "status": "uploaded",
    "already_uploaded": true
  }
}
```

### 6. Upload Chunk

```http
PUT /v1/worker/land-info/uploads/{upload_id}/chunks/{chunk_index}
Content-Type: application/octet-stream
X-Chunk-Offset: 0
X-Chunk-Size: 8388608
```

Response:

```json
{
  "ok": true,
  "data": {
    "upload_id": "upl_abc123",
    "chunk_index": 0,
    "received_bytes": 8388608
  }
}
```

Rules:

- Chunks are zero-indexed.
- Re-uploading the same chunk must be safe.
- Worker should skip chunks listed in `received_chunks`.
- Worker should retry transient HTTP/network errors with backoff.

### 7. Complete Upload

```http
POST /v1/worker/land-info/uploads/{upload_id}/complete
```

Request:

```json
{
  "sha256": "...",
  "file_size": 123456789
}
```

Response:

```json
{
  "ok": true,
  "data": {
    "upload_id": "upl_abc123",
    "status": "accepted",
    "server_path": "/data/source/land_info/uploads/request/file.zip",
    "sha256_verified": true,
    "zip_verified": true
  }
}
```

The worker may delete its local ZIP when status is `accepted` or later.

### 8. Complete Request

```http
POST /v1/worker/land-info/requests/{request_id}/complete
```

Request:

```json
{
  "worker_id": "NUCBOX_M6",
  "uploaded_count": 68,
  "failed_count": 0
}
```

Response:

```json
{
  "ok": true,
  "data": {
    "request_id": "land_info_update_15520260_9b428aad7442",
    "status": "server_processing",
    "processor_started": true
  }
}
```

When `failed_count` is `0`, the API starts the server-side direct processor in the background. The processor groups accepted ZIPs by dataset code, imports each component into its own data type, verifies recorded files and rows, activates the component release, deletes processed ZIP/staging files, and removes the old monolithic `land_info` release only after all four component releases are active.

## Worker Algorithm

Pseudo-flow:

```text
acquire .land_info_worker.lock
load config
heartbeat(status=idle)
request = GET /requests/next
if no request: exit 0
POST /requests/{id}/claim
save local manifest
for each item:
  if local file exists and sha256/zip valid:
    skip download
  else:
    trigger VWorld/RaonK download
    wait until matching ZIP exists and stable
    verify zip
    compute sha256
  POST file status downloaded
  POST uploads/init
  PUT missing chunks
  POST uploads/{id}/complete
  if server status accepted and delete_local_after_server_accept:
    delete local ZIP
  POST file status accepted
POST request complete
heartbeat(status=idle)
release lock
```

## Test Mode

The Windows worker should not need a separate path for test mode. It should follow the same loop. The server request controls behavior:

```json
{
  "test_mode": true,
  "activate": false,
  "expected_count": 1,
  "items": [...]
}
```

Expected worker behavior in test mode:

- Poll normally.
- Claim normally.
- Download normally.
- Upload normally.
- Delete local ZIP only if server returns `accepted` and config allows it.
- Do not decide whether DB release is active. That is server responsibility.

## Commands For Windows Codex

Open PowerShell or CMD on the Windows machine.

### Install / refresh dependencies

```bat
C:\land-info-worker\worker\install_windows_land_info_worker.cmd
```

### Edit config

```powershell
notepad C:\land-info-worker\worker\worker_config.json
```

Add `server_api_base`, `worker_id`, and `worker_token`.

### One-time manual run

```bat
C:\land-info-worker\worker\run_windows_land_info_worker.cmd
```

For the new direct worker, Windows Codex should add one of these commands:

```bat
C:\land-info-worker\worker\run_windows_land_info_direct_worker.cmd --once
```

or:

```powershell
C:\land-info-worker\worker\.venv\Scripts\LandInfoWorkerRunner.exe C:\land-info-worker\worker\windows_land_info_direct_worker.py --root C:\land-info-worker --once
```

### Register scheduled task

Use a separate task name so the SMB worker and direct worker do not conflict:

```bat
schtasks /Create /TN LandInfoDirectWorker /SC MINUTE /MO 10 /TR "C:\land-info-worker\worker\run_windows_land_info_direct_worker.cmd" /RU %USERDOMAIN%\%USERNAME% /IT /F
```

### Run scheduled task now

```bat
schtasks /Run /TN LandInfoDirectWorker
```

### Stop scheduled task

```bat
schtasks /End /TN LandInfoDirectWorker
```

### Disable scheduled task

```bat
schtasks /Change /TN LandInfoDirectWorker /DISABLE
```

### Enable scheduled task

```bat
schtasks /Change /TN LandInfoDirectWorker /ENABLE
```

### Check logs

```powershell
Get-ChildItem C:\land-info-worker\logs | Sort-Object LastWriteTime -Descending | Select-Object -First 5
Get-Content -Tail 120 C:\land-info-worker\logs\land_info_direct_worker_YYYYMMDD_HHMMSS.log
```

### Check local manifests

```powershell
Get-ChildItem C:\land-info-worker\manifests | Sort-Object LastWriteTime -Descending | Select-Object -First 10
```

## Implementation Notes For Windows Codex

Recommended new files:

```text
C:\land-info-worker\worker\windows_land_info_direct_worker.py
C:\land-info-worker\worker\run_windows_land_info_direct_worker.ps1
C:\land-info-worker\worker\run_windows_land_info_direct_worker.cmd
C:\land-info-worker\worker\register_windows_land_info_direct_worker_task.ps1
C:\land-info-worker\worker\register_windows_land_info_direct_worker_task.cmd
```

The direct worker can reuse from `windows_land_info_worker.py`:

- config loading
- request item grouping
- VWorld login
- RaonK clicking
- local lock handling
- stable ZIP detection
- GUI/status ideas

New code needed:

- API client with `X-Worker-Id` and `X-Worker-Token`
- chunked upload with resume
- local per-request manifest
- request claim/completion reporting
- heartbeat reporting

## Acceptance Test

1. Server creates a test request with `test_mode=true`, `activate=false`, `expected_count=1`.
2. Scheduled `LandInfoDirectWorker` runs without manual button.
3. Worker claims request.
4. Worker downloads one ZIP through RaonK.
5. Worker uploads ZIP to server by chunks.
6. Server returns `accepted`.
7. Worker reports request complete.
8. Server processes accepted ZIPs by component:
   - `AL_D155` -> `land_info_al_d155`
   - `AL_D157` -> `land_info_al_d157`
   - `AL_D161` -> `land_info_al_d161`
   - `AL_D195` -> `land_info_al_d195`
9. Worker deletes local ZIP only after server acceptance.
10. Admin page shows:
   - worker online
   - request claimed
   - file uploaded
   - server verification passed
   - changed component releases activated independently
