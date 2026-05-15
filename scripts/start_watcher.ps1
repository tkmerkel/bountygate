# Start the video-feedback-loop watcher session.
# Loops claude -p until the review.pending queue drains. Each invocation
# processes one recording; the loop here drives chaining since Stop-hook
# exit-2 chaining doesn't work in headless (-p) mode.

$ErrorActionPreference = "Stop"

$watcherDir = "C:\Users\tkmer\bountygate\watcher"
$promptFile = Join-Path $watcherDir "INITIAL_PROMPT.md"
$auditRoot  = "C:\Users\tkmer\bountygate\arbitrage_executor\audit_logs"
$maxRuns    = 50

if (-not (Test-Path $promptFile)) {
    Write-Error "Initial prompt not found at $promptFile"
    exit 1
}

$prompt = Get-Content -Path $promptFile -Raw -Encoding UTF8

Set-Location $watcherDir

$i = 0
while ($i -lt $maxRuns) {
    $count = 0
    if (Test-Path $auditRoot) {
        $pending = @(Get-ChildItem -Path $auditRoot -Recurse -Filter "review.pending" -File -ErrorAction SilentlyContinue)
        $count = $pending.Count
    }

    if ($count -eq 0) {
        Write-Host ("[watcher] queue drained after {0} iteration(s)." -f $i)
        exit 0
    }

    $iter = $i + 1
    Write-Host ("[watcher] {0} pending; iteration {1}/{2}..." -f $count, $iter, $maxRuns)

    & claude -p $prompt
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        Write-Host ("[watcher] claude exited {0} - stopping loop." -f $code)
        exit $code
    }

    $i++
}

Write-Host ("[watcher] hit max iteration cap ({0}). Re-run if needed." -f $maxRuns)
exit 0
