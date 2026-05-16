#!/bin/bash
# scripts/install_wiki_hook.sh — git-bash / WSL / Linux installer.
set -e
HOOK_PATH="$(git rev-parse --show-toplevel)/.git/hooks/post-commit"
cat > "$HOOK_PATH" <<'EOF'
#!/bin/sh
exec python "$(git rev-parse --show-toplevel)/scripts/wiki_hook.py" "$@"
EOF
chmod +x "$HOOK_PATH"
echo "Installed post-commit hook at $HOOK_PATH"
