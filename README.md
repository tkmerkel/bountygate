# bountygate

**Pivoting** from arb execution to a read-only prediction-market **analytics aggregator**
(in progress, 2026-06-05). The arb-execution layer was archived at git tag
`arb-execution-final` and removed from `main`.

## Current contents
- `airflow/dags/` — analytics DAGs (`bg_*`), currently paused/dark pending the rewrite.
- `app/shared/python/bountygate/analytics/` — kept analytics lib (devig/ev/kelly/clv/consensus/signals).
- `db/` — SQL migrations + reference data.
- `scripts/inventory/` — DAG pre-scan + inventory workflow.

## Planning artifacts
- Spec: `docs/superpowers/specs/2026-06-05-decommission-arb-inventory-blueprint-design.md`
- Inventory: `docs/superpowers/inventory/`
- Blueprint + downstream spec queue: `docs/superpowers/specs/2026-06-05-target-architecture-blueprint.md`

## Recover the archived arb bot
`git checkout arb-execution-final` (full execution layer + kalshi at freeze point).
The Heroku Postgres pre-pivot backup: `C:\Users\tkmer\_archive\bountygate-pre-pivot.dump`
(111 MB custom-format `pg_dump`, restore with `pg_restore`). The bot's execution audit
trail (screenshots/recordings): `C:\Users\tkmer\_archive\arbitrage_executor_audit_logs\`.
