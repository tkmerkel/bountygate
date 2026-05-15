# Runs the selector-drift detector against every baseline trace in
# traces/drift_baseline/ and pages BG_DISCORD_WEBHOOK_URL on any drift or
# error. Intended for Windows Task Scheduler (weekly cadence).
#
# Exit codes:
#   0  all baselines clean
#   1  one or more baselines drifted (Discord alerted)
#   2  one or more baselines errored or webhook missing (Discord alerted if possible)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $RepoRoot

$EnvFile = Join-Path $RepoRoot ".env"
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match '^\s*([^#=][^=]*)=(.*)$') {
            $name  = $Matches[1].Trim()
            $value = $Matches[2].Trim().Trim('"').Trim("'")
            Set-Item -Path "env:$name" -Value $value
        }
    }
}

$Webhook = $env:BG_DISCORD_WEBHOOK_URL
if (-not $Webhook) {
    Write-Error "BG_DISCORD_WEBHOOK_URL not set; cannot alert."
    exit 2
}

function Send-Discord([string]$content) {
    $body = @{ content = $content } | ConvertTo-Json -Compress
    try {
        Invoke-RestMethod -Uri $Webhook -Method Post `
            -ContentType "application/json" -Body $body | Out-Null
    } catch {
        Write-Warning "Discord post failed: $_"
    }
}

$BaselineDir = Join-Path $RepoRoot "traces\drift_baseline"
if (-not (Test-Path $BaselineDir)) {
    Send-Discord "WARNING drift_check: baseline dir missing ($BaselineDir)"
    exit 2
}

$Traces = Get-ChildItem -Path $BaselineDir -Filter "*.jsonl" -File
if ($Traces.Count -eq 0) {
    Write-Warning "No baseline traces in $BaselineDir; nothing to check."
    exit 0
}

$DriftHits = @()
$Errors    = @()

foreach ($trace in $Traces) {
    Write-Host "checking: $($trace.Name)"
    $output = & python -m toolkit.recorder_cli drift --trace $trace.FullName --json 2>&1
    $code = $LASTEXITCODE
    switch ($code) {
        0 { Write-Host "  clean" }
        1 {
            $book = "?"; $market = "?"; $count = -1
            try {
                $parsed = ($output | Out-String | ConvertFrom-Json)
                $book   = $parsed.book
                $market = $parsed.market
                $count  = @($parsed.diffs).Count
            } catch {}
            $DriftHits += [pscustomobject]@{
                file = $trace.Name; book = $book; market = $market; diffCount = $count
            }
            Write-Host "  DRIFT: $book/$market ($count field(s))"
        }
        default {
            $Errors += [pscustomobject]@{
                file = $trace.Name; output = ($output | Out-String).Trim()
            }
            Write-Host "  ERROR (exit $code)"
        }
    }
}

if ($DriftHits.Count -eq 0 -and $Errors.Count -eq 0) {
    Send-Discord "INFO drift_check: $($Traces.Count) baseline(s) clean"
    exit 0
}

$lines = @()
if ($DriftHits.Count -gt 0) {
    $lines += "WARNING drift_check: $($DriftHits.Count) market(s) drifted"
    foreach ($d in $DriftHits) {
        $lines += "  - $($d.book)/$($d.market) ($($d.diffCount) field(s)) — $($d.file)"
    }
    $lines += "Fix: re-record + codegen --save --overwrite for each drifted market."
}
if ($Errors.Count -gt 0) {
    $lines += "WARNING drift_check: $($Errors.Count) trace(s) errored"
    foreach ($e in $Errors) {
        $lines += "  - $($e.file)"
    }
}
Send-Discord ($lines -join "`n")

if ($DriftHits.Count -gt 0) { exit 1 } else { exit 2 }
