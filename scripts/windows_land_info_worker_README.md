# Land Info Windows Worker

Run on the Windows PC where RaonK download works.

## One-time setup

1. Copy or use the shared folder as `C:\land-info-worker`.
2. Edit `C:\land-info-worker\worker\worker_config.json`.
3. Fill `vworld_user_id` and `vworld_user_password`.
   If RaonK already uses `C:\land-info-worker\downloads` as its default folder, keep `set_download_path_in_raonk` as `false`.
4. Open PowerShell as the logged-in desktop user.
5. Run:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
C:\land-info-worker\worker\install_windows_land_info_worker.ps1 -Root C:\land-info-worker
```

If you are using `cmd.exe`, run this instead:

```bat
C:\land-info-worker\worker\install_windows_land_info_worker.cmd
```

## Manual run

```powershell
C:\land-info-worker\worker\run_windows_land_info_worker.ps1 -Root C:\land-info-worker
```

From `cmd.exe`:

```bat
C:\land-info-worker\worker\run_windows_land_info_worker.cmd
```

## Status app

Use the desktop status app when you want to see request progress, per-dataset ZIP counts, latest logs, and scheduled task controls.

```bat
C:\land-info-worker\worker\launch_windows_land_info_worker_app.cmd
```

## Register scheduled task

UI automation needs an interactive desktop session. Keep this Windows user logged in.

```powershell
C:\land-info-worker\worker\register_windows_land_info_worker_task.ps1 -Root C:\land-info-worker -IntervalMinutes 10
```

From `cmd.exe`:

```bat
C:\land-info-worker\worker\register_windows_land_info_worker_task.cmd
```

Or install and register together:

```bat
C:\land-info-worker\worker\setup_windows_land_info_worker.cmd
```

The worker reads `requests/latest_land_info_full_request.json`, triggers VWorld selection downloads through Edge/RaonK, waits for ZIP files in `downloads`, and writes `manifests/{request_id}.completed.json` when all expected files are present.

Once a completed manifest exists for a request, later scheduled runs skip that same request even if the original ZIP files were deleted after import verification. To intentionally download the same request again, delete the completed manifest or run `windows_land_info_worker.py --force-redownload`.

## Task Manager process names

After setup, the status app and worker use named Python launchers for easier monitoring:

```text
LandInfoWorkerStatus.exe
LandInfoWorkerRunner.exe
```

Run setup again if these names do not appear yet.
