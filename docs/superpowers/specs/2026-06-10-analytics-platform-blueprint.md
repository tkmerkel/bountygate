# Analytics Platform Blueprint — Public Product + Modeling Ladder

**Date:** 2026-06-10
**Status:** Blueprint — approved. Frames the next spec queue; each stage gets its own
brainstorm → spec → plan → build cycle.
**Supersedes scope of:** the frontend item left open by
`2026-06-05-target-architecture-blueprint.md` (its §5 queue is complete through spec #5).

The MVP pipeline is live end-to-end: connectors (Kalshi / Polymarket / The Odds API) → raw
partitioned snapshots → normalized markets/outcomes/events → marts (cross-market prices, edge
signals, market history) → FastAPI read API → vanilla-JS two-tab page. This blueprint defines the
ambitious end-state on top of it: a **public analytics product** fed by a **three-tier modeling
ladder**, and decomposes it into an ordered build queue.

## Locked decisions

| Decision | Choice |
|---|---|
| Purpose | **Public analytics product** — credibility- and presentation-first |
| Audience | **Mixed: prediction-market traders + sports bettors** |
| Model depth | **Full ladder, staged**: market-derived → fundamental → ML |
| Sports | **MLB + NBA + NHL** (the sports the enrichment feeds already cover); football later |
| Frontend | **React/Next.js rebuild** (App Router, TypeScript, Tailwind, Recharts); vanilla MVP retired once ported |
| Infra | **Heroku core (Postgres + FastAPI) + Vercel frontend**; Airflow + ML training stay on the local machine |
| Access | **Free + public, no auth**; a seam reserved for accounts/alerts later, none built now |
| Data budget | **Stay within current Odds API plan**; history accumulates capture-forward; buying historical snapshots is an explicit later option |

## 1. Product definition

A public, free, credibility-first sports market analytics site. The marquee promise: **"what's
the fair price, and who's off it"** — across Kalshi, Polymarket, and sportsbooks, with our own
models as an independent voice and a public calibration track record proving honesty.

Product surfaces (end-state):

1. **Cross-venue divergence** — today's view professionalized: sortable/filterable, sparkline
   history of how each divergence evolved.
2. **Fair odds screen** — per game-side: devigged, sharpness-weighted consensus fair probability;
   every venue's price beside it; edge % highlighted. The bettor's EV screen and the trader's
   mispricing screen are the same table with different columns emphasized.
3. **Line movement** — time-series charts per market from snapshot history: open → now →
   (post-close) closing line, all venues overlaid.
4. **Game pages** — one URL per event (the SEO surface): all venue prices, movement chart, model
   forecast, weather/injury context, post-game result + who-was-right.
5. **Model hub** — model probabilities vs. market consensus per slate; model cards explaining
   each model.
6. **Calibration & accountability** — public reliability curves, Brier/log scores for our models
   *and* each venue; book sharpness leaderboard ranked by closing-line accuracy. The credibility
   engine for everything else.

## 2. The modeling ladder

New package: `app/shared/python/bountygate/models/` — pure functions + fitted-artifact loaders,
unit-tested, called from DAGs. Three tiers, built in order. **Every tier writes the same shape**:
probabilities into a common `model_predictions` table, so the frontend and calibration engine
never care which tier produced a number. Tier 1's contribution is the consensus itself —
per-venue/per-method detail lands in `fair_prices`, and the blended consensus is registered as a
prediction source (e.g. `model_key='consensus_v1'`) in `model_predictions` alongside Tier 2/3
models.

### Tier 1 — Market-derived

- Proper devig: multiplicative, power, and Shin's method; per-book fair probabilities (the
  current `analytics/devig.py` is the seed, extended).
- **Sharpness-weighted consensus**: books weighted by measured closing-line accuracy. Bootstrap
  with the existing Pinnacle-anchored weighting, then learn weights from accumulated CLV data.
- **Closing-line capture**: a DAG snapshotting final pre-game prices per event — ground truth for
  CLV, sharpness ratings, and ML labels. Capture-forward starts the training-data clock now.
- Line-movement features: open-to-now delta, velocity, steam detection (cross-book synchronized
  moves).
- Calibration engine: scores every prediction source (venues and our models) against `bg_results`
  outcomes.

### Tier 2 — Fundamental models

- **NBA / NHL**: Elo-family ratings with home advantage, rest, and back-to-back adjustments
  → P(win).
- **NHL totals/ML**: bivariate Poisson on goals.
- **MLB**: rating model adjusted for **starting pitcher** (MLB StatsAPI probables) — the single
  biggest MLB factor; park factors and weather (Open-Meteo wind/temp for totals) as adjustments.
- Nightly rating updates from `bg_results`; per-slate predictions each morning.

### Tier 3 — ML layer

- Feature store: per-event wide feature tables (market features from Tier 1, fundamentals from
  Tier 2, enrichment: injuries, weather, rest, travel).
- LightGBM models predicting outcome probability and **market residual** (where the close moves
  from here / where consensus is wrong), trained walk-forward.
- Backtest harness with strict temporal splits. **Ship gate:** an ML model reaches the public
  site only when it beats the market-consensus baseline on held-out log loss.
- Lightweight registry: `model_versions` table + versioned joblib artifacts in-repo (no MLflow).

## 3. Architecture & data flow

```
connectors (exists)          models/ lib (new)              web
Kalshi ─┐                    tier1: devig·consensus·clv     FastAPI (Heroku)
Poly ───┼→ raw snapshots →   tier2: elo·poisson·mlb     →   /fair-odds /movement
Odds ───┘  (partitioned)     tier3: features·lgbm           /models /calibration
              ↓                      ↓                      /events/{id} /sharpness
           normalize         model_predictions,                   ↓ JSON
           markets/events    fair_prices, venue_sharpness,  Next.js (Vercel)
              ↓              features_*, calibration marts  divergence · fair odds
           enrichment                                       movement · game pages
           results/injuries/weather (exists)                model hub · calibration
```

- **DB migrations** (continuing 011+): `fair_prices` (event/outcome/method/prob/captured_at),
  `closing_lines`, `model_versions` + `model_predictions`, `venue_sharpness`,
  `features_{sport}`, plus calibration/movement marts. Same raw → normalized → marts discipline;
  history tables stay partitioned.
- **DAGs** (new): `build_fair_odds` (follows ingest), `capture_closing_lines` (pre-commence),
  `score_results` (post-resolution: CLV, calibration, sharpness), `update_ratings` (nightly),
  `predict_slate` (morning), `train_ml` (weekly, local machine).
- **API**: FastAPI remains the single read surface and expands per product surface. CORS opens to
  the Vercel origin (or Next.js rewrites proxy `/api/*` — decided in that stage's spec).
- **Frontend**: Next.js App Router + TypeScript + Tailwind; Recharts for charts; Pixel Augusta
  tokens (`app/design_handoff_bountygate_dashboard/`) as the theme seed. SSR/ISR on game pages
  and screens for shareability/SEO. Vercel deploys with preview branches.

## 4. Build order (the spec queue)

1. **Quant core (Tier 1)** — devig/consensus/fair-odds mart, closing-line capture, CLV +
   calibration engines, new API endpoints. Pure-function heavy, fully unit-testable; every later
   stage depends on it. Includes the Odds API credit-budget check, capture gap
   detection/alerting, and the DB retention policy.
2. **Next.js foundation** — scaffold, theme, Vercel deploy; port the two MVP views; add the fair
   odds screen + movement charts. The site becomes "a product"; the vanilla page retires.
3. **Fundamental models (Tier 2)** — ratings + Poisson + MLB pitcher model, model registry;
   model hub + calibration pages go live.
4. **Game pages + sharpness leaderboard** — the SEO/editorial surface; enrichment data surfaced.
5. **ML layer (Tier 3)** — feature store, training/backtest pipeline; ML predictions join the
   model hub when they beat baseline.
6. **(Reserved seam)** accounts/alerts — schema and API designed not to preclude it; nothing
   built now.

Each stage ships a visible public capability; stages 1 and 2 are the only hard ordering
dependency (everything needs the quant core; the product needs the frontend). 3–5 could in
principle reorder, but the listed order maximizes credibility-per-effort.

## 5. Testing & quality bars

- **Model lib**: pure functions; unit tests with known-answer fixtures (e.g., Shin devig against
  published examples); golden-file backtest outputs.
- **Pipelines**: DAG import smoke (pattern exists), idempotent re-runs, append + dedup-on-hash.
- **API**: contract tests per endpoint (FastAPI TestClient; pattern exists).
- **Frontend**: Playwright smoke per page against a seeded API.
- **Standing quality gate**: the calibration page doubles as regression detection — a model whose
  rolling Brier degrades gets flagged; ML models must beat the consensus baseline before public
  exposure.

## 6. Risks & constraints

- **Training-data depth**: capture-forward means thin ML training data for months. Tiers 1–2
  don't need training history; buying Odds API historical snapshots stays an explicit later
  option if Tier 3 needs it sooner.
- **Odds API credits**: the sharp-book toggle and added sports raise burn; stage 1 includes a
  credit budget check before widening coverage.
- **Local Airflow uptime**: closing-line capture is time-sensitive; missed windows degrade CLV
  data. Stage 1 includes capture gap detection/alerting.
- **Heroku Postgres growth**: feature tables and history accumulate; partitioning exists; stage 1
  sets the retention policy.

## 7. Explicitly deferred

Accounts/auth/alerts (seam only), monetization, NFL/CFB/CBB sports, player-prop modeling,
MLflow-style experiment tracking, live in-game odds, and any bet execution (the platform is
analytics-only; the executor remains a separate, frozen system).
