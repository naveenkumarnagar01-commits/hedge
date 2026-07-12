# Run this once as Administrator to register StraddleBot + Dashboard as startup tasks.
# Right-click PowerShell -> "Run as administrator", then:
#   Set-ExecutionPolicy -Scope Process Bypass; .\register_autostart.ps1

$BOT_DIR  = 'c:\Users\Admin\OneDrive\Desktop\hedge_platform_backup_20260528_1724'
$BOT_BAT  = "$BOT_DIR\start_straddle.bat"
$DASH_BAT = "$BOT_DIR\start_dashboard.bat"

# -- Detect Python: prefer venv, fall back to system --
$VENV_PY   = "$BOT_DIR\.venv\Scripts\python.exe"
$SYSTEM_PY = 'C:\Program Files\Python312\python.exe'
if (Test-Path $VENV_PY) {
    $PY = $VENV_PY
} elseif (Test-Path $SYSTEM_PY) {
    $PY = $SYSTEM_PY
} else {
    $PY = (Get-Command python -ErrorAction SilentlyContinue).Source
}
Write-Host "Using Python: $PY" -ForegroundColor Cyan

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0) `
    -RestartCount 5 `
    -RestartInterval (New-TimeSpan -Minutes 2) `
    -StartWhenAvailable

# -- Register Bot task --
$botAction = New-ScheduledTaskAction `
    -Execute 'cmd.exe' `
    -Argument "/c `"$BOT_BAT`"" `
    -WorkingDirectory $BOT_DIR
$trigger = New-ScheduledTaskTrigger -AtStartup

Register-ScheduledTask -TaskName "StraddleBot" `
    -Action $botAction `
    -Trigger $trigger `
    -Settings $settings `
    -RunLevel Highest `
    -Force
Write-Host "StraddleBot task registered." -ForegroundColor Green

# -- Register Dashboard task (30s delay so bot starts first) --
$dashTrigger = New-ScheduledTaskTrigger -AtStartup
$dashTrigger.Delay = 'PT30S'   # 30-second delay after startup

$dashAction = New-ScheduledTaskAction `
    -Execute 'cmd.exe' `
    -Argument "/c `"$DASH_BAT`"" `
    -WorkingDirectory $BOT_DIR

Register-ScheduledTask -TaskName "StradDashboard" `
    -Action $dashAction `
    -Trigger $dashTrigger `
    -Settings $settings `
    -RunLevel Highest `
    -Force
Write-Host "StradDashboard task registered (starts 30s after boot)." -ForegroundColor Green

Write-Host ""
Write-Host "Both tasks will auto-start on next reboot." -ForegroundColor Yellow
Write-Host "To start now without rebooting:" -ForegroundColor Yellow
Write-Host "  Start-ScheduledTask -TaskName StraddleBot" -ForegroundColor Cyan
Write-Host "  Start-ScheduledTask -TaskName StradDashboard" -ForegroundColor Cyan
