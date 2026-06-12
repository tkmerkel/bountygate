# The Daily Hedge — bountygate frontend

Next.js (App Router, TS, Tailwind v4) public product shell. Pixel Augusta theme.

## Develop

    # API (repo root; needs DATABASE_URL in env)
    py -3.12 -m uvicorn app.web.main:app --port 8000
    # frontend
    cd app/frontend && npm run dev      # http://localhost:3000

`/api/*` is rewritten to `API_BASE_URL` (default `http://localhost:8000`).

## Test

    npm run e2e     # boots seeded sqlite FastAPI + dev server, 5 smokes

## Build

    npm run build

## Deploy (Vercel)

1. Import the GitHub repo in Vercel; set **Root Directory** to `app/frontend`.
2. Set env var `API_BASE_URL` to the Heroku API origin (e.g. `https://bountygate.herokuapp.com`).
3. Deploy. Previews get the same env var.
