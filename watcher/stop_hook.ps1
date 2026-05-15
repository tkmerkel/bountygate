# Stop hook for the video-feedback-loop watcher session.
# If any review.pending markers remain under audit_logs/, exit 2 and tell
# Claude to continue processing. Otherwise exit 0 (allow stop).

$ErrorActionPreference = "SilentlyContinue"
$auditRoot = "C:\Users\tkmer\bountygate\arbitrage_executor\audit_logs"

if (-not (Test-Path $auditRoot)) {
    exit 0
}

$pending = Get-ChildItem -Path $auditRoot -Recurse -Filter "review.pending" -File
$count = ($pending | Measure-Object).Count

if ($count -gt 0) {
    [Console]::Error.WriteLine("$count review.pending file(s) remain under audit_logs/. Process the next oldest one: read its sibling opportunity_info.json and recording.mp4, run /watch:watch with the review prompt, write review.md, append to dashboard/data.json, then delete review.pending and create review.done.")
    exit 2
}

exit 0
