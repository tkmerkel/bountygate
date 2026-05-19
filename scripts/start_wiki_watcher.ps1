# scripts/start_wiki_watcher.ps1
# Launches a Claude Code session that drains wiki/.pending/ via the
# /wiki:sync skill. Mirrors scripts/start_watcher.ps1 for the review-watcher.
#
# Prerequisites:
# - .env at repo root with DATABASE_URL set (loaded by python-dotenv inside
#   watcher_heartbeat module via the conftest pattern; for raw invocations
#   from this script, source the env first).
# - `claude` CLI on PATH (Claude Code).

$ErrorActionPreference = "Stop"
$repoRoot = (& git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { throw "Not in a git repo." }

Set-Location $repoRoot

# Load .env into the current shell so DATABASE_URL is available to the
# python -c heartbeat calls inside the watcher loop.
$envFile = Join-Path $repoRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.+?)\s*$' -and -not $_.StartsWith('#')) {
            [Environment]::SetEnvironmentVariable($matches[1], $matches[2].Trim('"'), 'Process')
        }
    }
}

# Ensure app/shared/python is on PYTHONPATH for bountygate.watcher_heartbeat imports.
$shared = Join-Path $repoRoot "app\shared\python"
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$shared;$env:PYTHONPATH" } else { $shared }

$prompt = Get-Content (Join-Path $repoRoot "watcher\wiki\INITIAL_PROMPT.md") -Raw
$stopHook = Join-Path $repoRoot "watcher\wiki\stop_hook.ps1"

Write-Host "Starting wiki-watcher Claude Code session..."
& claude --prompt $prompt --stop-hook $stopHook
