param(
  [double]$Hours = 6,
  [string]$LogPath = ""
)

$ErrorActionPreference = "Stop"

Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class SleepGuard {
  [DllImport("kernel32.dll")]
  public static extern uint SetThreadExecutionState(uint esFlags);
}
"@

[uint32]$ES_CONTINUOUS = [uint32]2147483648
[uint32]$ES_SYSTEM_REQUIRED = [uint32]1
$deadline = (Get-Date).AddHours($Hours)

function Write-GuardLog {
  param([string]$Message)
  $line = "[{0}] {1}" -f (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ssZ"), $Message
  Write-Output $line
  if ($LogPath) {
    Add-Content -Path $LogPath -Value $line
  }
}

Write-GuardLog "prevent_sleep starting until $($deadline.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ssZ'))"
try {
  while ((Get-Date) -lt $deadline) {
    [void][SleepGuard]::SetThreadExecutionState($ES_CONTINUOUS -bor $ES_SYSTEM_REQUIRED)
    Start-Sleep -Seconds 30
  }
}
finally {
  [void][SleepGuard]::SetThreadExecutionState($ES_CONTINUOUS)
  Write-GuardLog "prevent_sleep stopped"
}
