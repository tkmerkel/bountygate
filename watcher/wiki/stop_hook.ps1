# watcher/wiki/stop_hook.ps1
# Claude Code stop hook: if any wiki/.pending/* files remain, re-invoke the session.
# Mirrors the existing watcher/stop_hook.ps1 pattern for the review-watcher.

$repoRoot = (& git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { exit 0 }

$pendingDir = Join-Path $repoRoot "wiki\.pending"
if (-not (Test-Path $pendingDir)) { exit 0 }

$remaining = @(Get-ChildItem $pendingDir -File -ErrorAction SilentlyContinue)
if ($remaining.Count -gt 0) {
    Write-Host "[wiki-stop-hook] $($remaining.Count) pending files remain — re-invoking session"
    & (Join-Path $repoRoot "scripts\start_wiki_watcher.ps1")
}
