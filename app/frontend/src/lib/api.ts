"use client";

import { useCallback, useEffect, useState } from "react";

export type FairOddsRow = {
  event_id: string;
  sport_key: string | null;
  commence_time: string | null;
  home_team: string | null;
  away_team: string | null;
  market_type: string;
  outcome_name: string;
  consensus_prob: number | null;
  best_price: number | null;
  best_bookmaker: string | null;
  edge: number | null;
  computed_at: string | null;
};

export type MovementPoint = {
  market_type: string;
  bookmaker: string;
  outcome_name: string;
  decimal_price: number | null;
  captured_at: string;
};

export type ClosingLine = {
  event_id: string;
  market_type: string;
  bookmaker: string;
  outcome_name: string;
  decimal_price: number | null;
  fair_prob: number | null;
  captured_at: string | null;
  staleness_minutes: number | null;
};

export type CrossMarketRow = {
  question_key: string;
  captured_at: string | null;
  kalshi_prob: number | null;
  polymarket_prob: number | null;
  sportsbook_consensus_prob: number | null;
  max_spread: number | null;
};

export type MarketRow = {
  market_id: string;
  venue_key: string | null;
  external_id: string | null;
  title: string | null;
  category: string | null;
  status: string | null;
  open_time: string | null;
  close_time: string | null;
  resolved_outcome: string | null;
  resolution_time: string | null;
  updated_at: string | null;
};

export type ArbRow = {
  opportunity_hash: string;
  first_detected_at: string | null;
  last_seen_at: string | null;
  kind: string;
  pairing: string;
  event_id: string | null;
  sport_key: string | null;
  home_team: string | null;
  away_team: string | null;
  commence_time: string | null;
  market_segment: string | null;
  player_name: string | null;
  line: number | null;
  pairing_type: string | null;
  leg_a_kind: string | null;
  leg_a_source: string | null;
  leg_a_outcome: string | null;
  leg_a_point: number | null;
  leg_a_price: number;
  leg_a_stake: number | null;
  leg_b_kind: string | null;
  leg_b_source: string | null;
  leg_b_outcome: string | null;
  leg_b_point: number | null;
  leg_b_price: number;
  leg_b_stake: number | null;
  payout: number | null;
  arb_ev: number | null;
  roi: number;
  fee_adjusted_roi: number;
  hours_until_commence: number | null;
  details: Record<string, unknown> | null;
};

export type PropRow = {
  event_id: string;
  sport_key: string | null;
  commence_time: string | null;
  home_team: string | null;
  away_team: string | null;
  market_key: string;
  player_name: string | null;
  line: number | null;
  side: string | null;
  bookmaker: string | null;
  decimal_price: number | null;
  captured_at: string | null;
};

export type ApiState<T> = {
  data: T | null;
  error: string | null;
  loading: boolean;
  reload: () => void;
};

export function useApi<T>(path: string): ApiState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const reload = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(path)
      .then(async (resp) => {
        if (!resp.ok) throw new Error(`status ${resp.status}`);
        return (await resp.json()) as T;
      })
      .then((body) => {
        if (!cancelled) setData(body);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [path, tick]);

  return { data, error, loading, reload };
}
