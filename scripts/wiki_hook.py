#!/usr/bin/env python3
"""Git post-commit hook. After every commit:

1. Compute changed files (HEAD~1..HEAD).
2. For each wiki/*.md, parse front-matter `watches:` list.
3. If any watched path appears in the changed files, touch wiki/.pending/{slug}.
4. Cap fan-out at MAX_FANOUT pages (avoid the whole wiki on a merge commit).
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from typing import List

_FM_RE = re.compile(r"^---\n(.*?)\n---", re.DOTALL)
_LIST_ITEM_RE = re.compile(r"^\s*-\s+(.+)$")

MAX_FANOUT = 5


def parse_watches(md_path: Path) -> List[str]:
    """Return the list of source file paths under the page's `watches:` key."""
    text = md_path.read_text(encoding="utf-8")
    m = _FM_RE.match(text)
    if not m:
        return []
    fm = m.group(1)
    out: List[str] = []
    in_watches = False
    for line in fm.splitlines():
        stripped = line.rstrip()
        if stripped.startswith("watches:"):
            in_watches = True
            continue
        if in_watches:
            li = _LIST_ITEM_RE.match(line)
            if li:
                out.append(li.group(1).strip())
            elif stripped and not line.startswith(" "):
                break
    return out


def affected_pages(wiki_dir: Path, changed_files: List[str]) -> List[str]:
    """Return slugs (alphabetically) whose `watches:` intersects changed_files."""
    changed = set(changed_files)
    affected: List[str] = []
    if not wiki_dir.exists():
        return affected
    for md in sorted(wiki_dir.glob("*.md")):
        if md.name.startswith("_"):
            continue
        watches = parse_watches(md)
        if any(w in changed for w in watches):
            affected.append(md.stem)
    return affected


def _changed_files() -> List[str]:
    try:
        res = subprocess.run(
            ["git", "diff", "--name-only", "HEAD~1..HEAD"],
            capture_output=True, text=True, check=False,
        )
    except FileNotFoundError:
        return []
    return [l.strip() for l in res.stdout.splitlines() if l.strip()]


def main(argv: List[str]) -> int:
    force = "--force" in argv
    repo_root_res = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=False,
    )
    if repo_root_res.returncode != 0:
        print("[wiki-hook] not a git repo")
        return 0
    repo_root = Path(repo_root_res.stdout.strip())
    wiki = repo_root / "wiki"
    pending = wiki / ".pending"
    pending.mkdir(parents=True, exist_ok=True)

    changed = _changed_files()
    pages = affected_pages(wiki, changed)
    if not pages:
        print("[wiki-hook] no affected pages")
        return 0
    if len(pages) > MAX_FANOUT and not force:
        print(f"[wiki-hook] {len(pages)} pages affected (> {MAX_FANOUT}). "
              f"Refusing without --force. Pages: {pages}")
        return 0
    for slug in pages:
        (pending / slug).touch()
    print(f"[wiki-hook] marked {len(pages)} pending: {pages}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
