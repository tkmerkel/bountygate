# scripts/install_wiki_hook.ps1
# Installs the wiki post-commit hook into the local .git/hooks/post-commit.
# Re-run safely; overwrites any prior install.

$ErrorActionPreference = "Stop"
$repoRoot = (& git rev-parse --show-toplevel).Trim()
if (-not $repoRoot) { throw "Not in a git repo." }

$hooksDir = Join-Path $repoRoot ".git\hooks"
if (-not (Test-Path $hooksDir)) { throw "Hooks dir missing: $hooksDir" }

$hookPath = Join-Path $hooksDir "post-commit"
$hookContent = @'
#!/bin/sh
# Auto-installed by scripts/install_wiki_hook.ps1
exec python "$(git rev-parse --show-toplevel)/scripts/wiki_hook.py" "$@"
'@

# Write LF line endings (sh shebang requires unix EOLs).
[System.IO.File]::WriteAllText($hookPath, $hookContent.Replace("`r`n", "`n"), [System.Text.UTF8Encoding]::new($false))
Write-Host "Installed post-commit hook at $hookPath"
Write-Host "Test: make a commit touching a wiki/<slug>.md's `watches:` source; wiki/.pending/ will be populated."
