-- Add consensus-quality / confidence indicators to the good-bets board so the
-- dashboard can show how trustworthy each +EV play is: how many independent
-- books posted a two-way price (the consensus sample size) and whether the fair
-- probability is Pinnacle-anchored ('pinnacle') or from the wide-book consensus
-- fallback ('consensus'); arbs are 'arb', CLV rows are 'clv'.
ALTER TABLE mart_good_bets ADD COLUMN IF NOT EXISTS n_two_way_books integer;
ALTER TABLE mart_good_bets ADD COLUMN IF NOT EXISTS fair_source text;
