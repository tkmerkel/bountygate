---
name: wiki:sync
description: |
  Regenerate one wiki page from its `watches:` source files. Invoked by the
  wiki-watcher loop when `wiki/.pending/<slug>` appears. Takes one positional
  arg: the page slug (filename under wiki/<slug>.md).
---

# /wiki:sync — regenerate a wiki page

You are syncing **wiki/{{slug}}.md** so its contents match the current code in the files declared under its `watches:` front-matter.

## Procedure

1. **Read the page.** Read `wiki/{{slug}}.md`. Parse the YAML front-matter and note the `watches:` list.

2. **Read every watched file.** For each path in `watches:`, use the Read tool — never just grep. You need full content for accurate diagram regeneration.

3. **Regenerate the body.** Update the markdown body to reflect the current code reality:
   - **Prose**: rewrite intros to match what the code actually does today. Preserve any lines marked `<!-- preserve -->`.
   - **Mermaid blocks** (` ```mermaid ` fences): regenerate the diagram from the current call graph / state transitions / queue states in the watched code.
   - **`:::reactflow` blocks**: regenerate the `nodes`, `edges`, `layers` JSON to match the current decision gates and state machine in the watched code. Preserve `id` and `endpoint` and the `layers` definitions; refresh `nodes`/`edges` and node positions only if the structure has changed.

4. **Granularity bar (important).** Enumerate every meaningful UI interaction (navigate, wait-for-element, dismiss-modal, click-search, type-query, click-submit, etc.) as a separate node. Do NOT collapse multiple UI interactions into one node. The renderer handles dense layouts; readability is preserved via the layer-toggle legend, not via coarse nodes.

5. **Update front-matter.** Set:
   - `updated_at: <now in ISO 8601 UTC>` — example: `2026-05-16T22:18:34Z`
   - `generated_by: /wiki:sync`
   - Leave `title`, `slug`, `watches:` unchanged unless the source file list legitimately needs to change.

6. **Write the new page.** Overwrite `wiki/{{slug}}.md` in place using the Write tool.

7. **Move the signal file.** Create `wiki/.done/` if it doesn't exist. Then move `wiki/.pending/{{slug}}` → `wiki/.done/{{slug}}` (use `mv` in bash or `Move-Item` in PowerShell).

## Constraints

- **Do NOT git commit.** The user reviews the diff and commits manually.
- **Idempotent.** Running `/wiki:sync <slug>` twice in a row on the same slug with no source changes between runs must produce identical output. Don't introduce timestamps in the body (only in front-matter).
- **Stable ordering.** Sort node IDs alphabetically. Sort edges by `id`. This keeps git diffs minimal.
- **No new pages.** If `wiki/{{slug}}.md` doesn't exist, exit with an error message. Page creation is a manual act.

## Output

Print a one-line summary at the end:

```
[wiki:sync] regenerated wiki/<slug>.md (<N> nodes, <M> edges, <K> mermaid blocks)
```
