from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from typing import List

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset

from bountygate.utils.db_connection import fetch_data, insert_data, execute_raw_sql


STAGE_TABLE_ODDS = "bg_unified_lines_stage_odds"
TARGET_TABLE = "bg_arbitrage_player_props"
TARGET_TABLE_ALT = "bg_arbitrage_player_props_alt"

HISTORY_TABLE = "bg_arbitrage_player_props_history"
HISTORY_TABLE_ALT = "bg_arbitrage_player_props_alt_history"

# Schedule trigger asset (created upstream).
odds_player_props_staged_asset = Asset("odds_player_props_staged")

# Completion asset for downstream DAGs.
player_props_arbitrage_complete_asset = Asset("bg_arbitrage_player_props_complete")

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1336061346088751104/G630NXDcVY6ZqejMM-pyIZVDgazNAUKG0rAiDWZ3oBi-2TkR2qoGkte-1fX0HaKyT5_Q"
DISCORD_WEBHOOK_ENV_VAR = "BG_DISCORD_WEBHOOK_URL"
HIGH_VALUE_ROI_THRESHOLD = 0.0075
BOT_EXECUTION_TASK_COUNT = 2

# Markets excluded from bot execution — debug separately, remove once working.
MARKET_BLACKLIST = [
	"pitcher_strikeouts",
	"pitcher_strikeouts_alternate"
]


def _send_discord_message(message: str) -> None:
	url = DISCORD_WEBHOOK_URL
	if not url:
		return

	content = (message or "").strip()
	if not content:
		return

	# Discord webhook content limit is 2000 chars; stay safely under.
	content = content[:1900]

	payload = {"content": content}
	headers = {"Content-Type": "application/json"}

	try:
		response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=10)
		if response.status_code != 204:
			print(
				f"Discord webhook send failed: status={response.status_code} body={response.text}"
			)
	except Exception as exc:
		print(f"Discord webhook send raised: {exc}")


def _format_discord_opportunities_message(df: pd.DataFrame, *, label: str) -> str:
	if df is None or df.empty:
		return ""

	working = df.copy()
	working["roi"] = pd.to_numeric(working.get("roi"), errors="coerce")
	working.sort_values(by=["roi"], ascending=False, inplace=True)

	lines: List[str] = []
	lines.append(f"====== +++++++++++++++ ======")
	lines.append(f"====== {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ======")
	lines.append(f"New high-value opportunity ({label})")
	lines.append(f"Count: {int(len(working))}")

	max_rows = 8
	for idx, row in working.head(max_rows).reset_index(drop=True).iterrows():
		roi = row.get("roi")
		roi_pct = f"{float(roi) * 100:.2f}%" if pd.notna(roi) else ""
		hours = row.get("hours_until_commence")
		hours_str = f"{float(hours):.2f}h" if pd.notna(hours) else ""
		commence = row.get("commence_time")
		commence_str = (
			pd.to_datetime(commence, errors="coerce", utc=True).strftime("%Y-%m-%d %H:%MZ")
			if pd.notna(commence)
			else ""
		)

		sport = str(row.get("sport_title") or row.get("sport_key") or "").strip()
		away = str(row.get("away_team") or "").strip()
		home = str(row.get("home_team") or "").strip()
		matchup = f"{away} @ {home}".strip(" @")

		player = str(row.get("player_name") or "").strip()
		market = str(row.get("market_key") or "").strip()
		line_value = row.get("under_line") if "under_line" in working.columns else row.get("line")
		line_str = (
			f"{float(line_value):g}" if pd.notna(pd.to_numeric(line_value, errors="coerce")) else ""
		)

		under_book = str(row.get("under_bookmaker_key") or "").strip()
		over_book = str(row.get("over_bookmaker_key") or "").strip()
		under_price = row.get("under_price")
		over_price = row.get("over_price")
		under_price_str = f"{float(under_price):.4g}" if pd.notna(under_price) else ""
		over_price_str = f"{float(over_price):.4g}" if pd.notna(over_price) else ""

		arb_ev = row.get("arb_ev")
		total_wager = row.get("total_wager")
		ev_str = f"EV ${float(arb_ev):.2f} on ${float(total_wager):.2f}" if pd.notna(arb_ev) and pd.notna(total_wager) else ""

		header_bits = " | ".join([bit for bit in [sport, matchup, commence_str, hours_str] if bit])
		prop_bits = " ".join([bit for bit in [player, market, line_str] if bit]).strip()
		price_bits = " | ".join(
			[bit for bit in [
				f"U {under_book} {under_price_str}".strip(),
				f"O {over_book} {over_price_str}".strip(),
			] if bit]
		)
		metric_bits = " | ".join([bit for bit in [f"ROI {roi_pct}" if roi_pct else "", ev_str] if bit])

		lines.append(f"{idx + 1}) {header_bits}")
		if prop_bits:
			lines.append(prop_bits)
		if price_bits:
			lines.append(price_bits)
		if metric_bits:
			lines.append(metric_bits)
		lines.append("")

	if len(working) > max_rows:
		lines.append(f"(+{len(working) - max_rows} more)")

	return "\n".join(lines).strip()


def _build_opportunity_key(df: pd.DataFrame) -> pd.Series:
	"""Stable key: event_id-player_name-market_key-line-under_bookmaker-over_bookmaker-under_price-over_price."""
	if df is None or df.empty:
		return pd.Series(dtype="string")

	def _fmt_num(series: pd.Series, *, decimals: int) -> pd.Series:
		numeric = pd.to_numeric(series, errors="coerce")
		rounded = numeric.round(decimals)
		return rounded.map(lambda value: f"{value:.{decimals}f}" if pd.notna(value) else "")

	# Prefer under_line for "line" (it equals over_line by construction).
	line_value = df["under_line"] if "under_line" in df.columns else df.get("line")
	parts = [
		df.get("event_id", pd.Series([""] * len(df))).astype("string").fillna(""),
		df.get("player_name", pd.Series([""] * len(df))).astype("string").fillna(""),
		df.get("market_key", pd.Series([""] * len(df))).astype("string").fillna(""),
		_fmt_num(line_value, decimals=3) if line_value is not None else pd.Series([""] * len(df)),
		df.get("under_bookmaker_key", pd.Series([""] * len(df))).astype("string").fillna(""),
		df.get("over_bookmaker_key", pd.Series([""] * len(df))).astype("string").fillna(""),
		_fmt_num(df.get("under_price"), decimals=6) if "under_price" in df.columns else pd.Series([""] * len(df)),
		_fmt_num(df.get("over_price"), decimals=6) if "over_price" in df.columns else pd.Series([""] * len(df)),
	]

	key = parts[0]
	for part in parts[1:]:
		key = key.str.cat(part.astype("string"), sep="|")
	return key


def build_player_props_arbitrage(
	df: pd.DataFrame,
	*,
	base_wager: float = 100.0,
	exclude_market_substring: str = "alternate",
) -> pd.DataFrame:
	"""Build arbitrage rows from staged OddsAPI player props.

	Expects decimal odds in `price` and two-sided markets with `outcome` in
	{"Over", "Under"} (case-insensitive).

	Output mirrors the structure of `bg_unified_arbitrage` plus:
	- `roi` = arb_ev / total_wager
	- `hours_until_commence`
	"""

	if df is None or df.empty:
		return pd.DataFrame()

	working = df.copy()

	required_columns: List[str] = [
		"event_id",
		"player_name",
		"market_key",
		"line",
		"outcome",
		"price",
		"bookmaker_key",
		"commence_time",
	]
	for column in required_columns:
		if column not in working.columns:
			working[column] = pd.NA

	working["market_key"] = working["market_key"].astype("string")
	if exclude_market_substring:
		working = working[
			~working["market_key"].str.contains(exclude_market_substring, case=False, na=False)
		].copy()

	working["outcome"] = working["outcome"].astype("string").str.strip().str.lower()
	working = working[working["outcome"].isin(["over", "under"])].copy()

	for text_col in ("event_id", "player_name", "market_key", "bookmaker_key"):
		working[text_col] = (
			working[text_col]
			.astype("string")
			.str.strip()
			.replace("", pd.NA)
			.replace("None", pd.NA)
		)

	working["line"] = pd.to_numeric(working["line"], errors="coerce")
	working["price"] = pd.to_numeric(working["price"], errors="coerce")
	working["commence_time"] = pd.to_datetime(working["commence_time"], errors="coerce", utc=True)

	working.dropna(
		subset=["event_id", "player_name", "market_key", "line", "outcome", "price", "bookmaker_key"],
		inplace=True,
	)

	# Defensive: decimal odds must be > 0 to size wagers.
	working = working[working["price"] > 0].copy()
	if working.empty:
		return pd.DataFrame()

	# Keep event-level fields for convenience.
	event_fields = [
		"sport_key",
		"sport_title",
		"commence_time",
		"home_team",
		"away_team",
		"fetched_at_utc",
	]
	for column in event_fields:
		if column not in working.columns:
			working[column] = pd.NA

	working["fetched_at_utc"] = pd.to_datetime(working["fetched_at_utc"], errors="coerce", utc=True)

	event_info = (
		working[["event_id", *event_fields]]
		.sort_values(["event_id", "fetched_at_utc"], ascending=[True, False])
		.drop_duplicates(subset=["event_id"], keep="first")
	)

	# Pairing keys. Include `line` so under/over must match on the same numeric line.
	group_cols = ["event_id", "player_name", "market_key", "line"]

	def _pick_extreme(*, side: str, highest: bool) -> pd.DataFrame:
		side_df = working[working["outcome"] == side].copy()
		if side_df.empty:
			return pd.DataFrame(columns=group_cols + [f"{side}_bookmaker_key", f"{side}_price"])

		side_df.sort_values(
			by=group_cols + ["price"],
			ascending=[True] * len(group_cols) + ([False] if highest else [True]),
			inplace=True,
		)
		extreme = side_df.drop_duplicates(subset=group_cols, keep="first")
		return extreme[group_cols + ["bookmaker_key", "price"]].rename(
			columns={"bookmaker_key": f"{side}_bookmaker_key", "price": f"{side}_price"}
		)

	best_under = _pick_extreme(side="under", highest=True)
	best_over = _pick_extreme(side="over", highest=True)
	worst_under = _pick_extreme(side="under", highest=False)[
		group_cols + ["under_bookmaker_key", "under_price"]
	].rename(columns={"under_bookmaker_key": "under_bookmaker_key_low", "under_price": "under_price_low"})
	worst_over = _pick_extreme(side="over", highest=False)[
		group_cols + ["over_bookmaker_key", "over_price"]
	].rename(columns={"over_bookmaker_key": "over_bookmaker_key_low", "over_price": "over_price_low"})

	merged = best_under.merge(best_over, on=group_cols, how="inner")
	merged = merged.merge(worst_under, on=group_cols, how="left")
	merged = merged.merge(worst_over, on=group_cols, how="left")

	if merged.empty:
		return pd.DataFrame()

	# Since `line` is part of the pairing key, under_line == over_line by construction.
	merged["under_line"] = merged["line"]
	merged["over_line"] = merged["line"]
	merged.drop(columns=["line"], inplace=True)

	wager_under = float(base_wager)
	merged["wager_under"] = wager_under
	merged["payout"] = (merged["wager_under"] * merged["under_price"]).round(2)

	# Size the opposite bet so its payout matches `payout`.
	merged["wager_over"] = (merged["payout"] / merged["over_price"]).round(2)
	merged["payout_over"] = merged["payout"]

	merged["total_wager"] = (merged["wager_under"] + merged["wager_over"]).round(2)
	merged["arb_ev"] = (merged["payout"] - merged["total_wager"]).round(2)

	merged["roi"] = (merged["arb_ev"] / merged["total_wager"]).round(6)

	merged = merged.merge(event_info, on="event_id", how="left")

	now_utc = pd.Timestamp.now(tz="UTC")
	commence = pd.to_datetime(merged.get("commence_time"), errors="coerce", utc=True)
	merged["hours_until_commence"] = ((commence - now_utc).dt.total_seconds() / 3600.0).round(2)

	# Make output a bit nicer/consistent.
	merged.sort_values(by=["arb_ev"], ascending=False, inplace=True)
	merged.reset_index(drop=True, inplace=True)

	return merged


def build_player_props_arbitrage_alt(
	df: pd.DataFrame,
	*,
	base_wager: float = 100.0,
	alternate_suffix: str = "_alternate",
) -> pd.DataFrame:
	"""Build arbitrage rows pairing base-market Unders with alternate-market Overs.

	Rule:
	- Under comes from `market_key` = {market}
	- Over comes from `market_key` = {market}_alternate

	This allows valid matches even when the two sides are posted under different
	market keys.
	"""

	if df is None or df.empty:
		return pd.DataFrame()

	working = df.copy()

	required_columns: List[str] = [
		"event_id",
		"player_name",
		"market_key",
		"line",
		"outcome",
		"price",
		"bookmaker_key",
		"commence_time",
	]
	for column in required_columns:
		if column not in working.columns:
			working[column] = pd.NA

	working["market_key"] = working["market_key"].astype("string").str.strip().replace("", pd.NA)
	working["outcome"] = working["outcome"].astype("string").str.strip().str.lower()
	working = working[working["outcome"].isin(["over", "under"])].copy()

	for text_col in ("event_id", "player_name", "bookmaker_key"):
		working[text_col] = (
			working[text_col]
			.astype("string")
			.str.strip()
			.replace("", pd.NA)
			.replace("None", pd.NA)
		)

	working["line"] = pd.to_numeric(working["line"], errors="coerce")
	working["price"] = pd.to_numeric(working["price"], errors="coerce")
	working["commence_time"] = pd.to_datetime(working["commence_time"], errors="coerce", utc=True)

	working.dropna(
		subset=["event_id", "player_name", "market_key", "line", "outcome", "price", "bookmaker_key"],
		inplace=True,
	)

	working = working[working["price"] > 0].copy()
	if working.empty:
		return pd.DataFrame()

	# Keep event-level fields for convenience.
	event_fields = [
		"sport_key",
		"sport_title",
		"commence_time",
		"home_team",
		"away_team",
		"fetched_at_utc",
	]
	for column in event_fields:
		if column not in working.columns:
			working[column] = pd.NA

	working["fetched_at_utc"] = pd.to_datetime(working["fetched_at_utc"], errors="coerce", utc=True)
	if "market_key" in working.columns:
		working["market_key"] = working["market_key"].astype("string")

	# Build a base market key used for pairing.
	working["_base_market_key"] = working["market_key"].astype("string")
	alt_mask = working["_base_market_key"].str.endswith(alternate_suffix, na=False)
	if alternate_suffix:
		working.loc[alt_mask, "_base_market_key"] = working.loc[alt_mask, "_base_market_key"].str.slice(
			stop=-len(alternate_suffix)
		)

	event_info = (
		working[["event_id", *event_fields]]
		.sort_values(["event_id", "fetched_at_utc"], ascending=[True, False])
		.drop_duplicates(subset=["event_id"], keep="first")
	)

	# Pairing keys. Include `line` so under/over must match on the same numeric line.
	group_cols = ["event_id", "player_name", "_base_market_key", "line"]

	def _pick_extreme_under(*, highest: bool) -> pd.DataFrame:
		side_df = working[
			(working["outcome"] == "under")
			& (~working["market_key"].str.endswith(alternate_suffix, na=False))
		].copy()
		if side_df.empty:
			return pd.DataFrame(
				columns=group_cols + ["under_market_key", "under_bookmaker_key", "under_price"]
			)
		side_df.sort_values(
			by=group_cols + ["price"],
			ascending=[True] * len(group_cols) + ([False] if highest else [True]),
			inplace=True,
		)
		extreme = side_df.drop_duplicates(subset=group_cols, keep="first")
		return extreme[group_cols + ["market_key", "bookmaker_key", "price"]].rename(
			columns={
				"market_key": "under_market_key",
				"bookmaker_key": "under_bookmaker_key",
				"price": "under_price",
			}
		)

	def _pick_extreme_over_alt(*, highest: bool) -> pd.DataFrame:
		side_df = working[
			(working["outcome"] == "over")
			& (working["market_key"].str.endswith(alternate_suffix, na=False))
		].copy()
		if side_df.empty:
			return pd.DataFrame(
				columns=group_cols + ["over_market_key", "over_bookmaker_key", "over_price"]
			)
		side_df.sort_values(
			by=group_cols + ["price"],
			ascending=[True] * len(group_cols) + ([False] if highest else [True]),
			inplace=True,
		)
		extreme = side_df.drop_duplicates(subset=group_cols, keep="first")
		return extreme[group_cols + ["market_key", "bookmaker_key", "price"]].rename(
			columns={
				"market_key": "over_market_key",
				"bookmaker_key": "over_bookmaker_key",
				"price": "over_price",
			}
		)

	best_under = _pick_extreme_under(highest=True)
	best_over = _pick_extreme_over_alt(highest=True)
	if best_under.empty or best_over.empty:
		return pd.DataFrame()

	worst_under = _pick_extreme_under(highest=False)[group_cols + ["under_bookmaker_key", "under_price"]].rename(
		columns={"under_bookmaker_key": "under_bookmaker_key_low", "under_price": "under_price_low"}
	)
	worst_over = _pick_extreme_over_alt(highest=False)[group_cols + ["over_bookmaker_key", "over_price"]].rename(
		columns={"over_bookmaker_key": "over_bookmaker_key_low", "over_price": "over_price_low"}
	)

	merged = best_under.merge(best_over, on=group_cols, how="inner")
	merged = merged.merge(worst_under, on=group_cols, how="left")
	merged = merged.merge(worst_over, on=group_cols, how="left")

	if merged.empty:
		return pd.DataFrame()

	# Since `line` is part of the pairing key, under_line == over_line by construction.
	merged["under_line"] = merged["line"]
	merged["over_line"] = merged["line"]
	merged.drop(columns=["line"], inplace=True)

	wager_under = float(base_wager)
	merged["wager_under"] = wager_under
	merged["payout"] = (merged["wager_under"] * merged["under_price"]).round(2)
	merged["wager_over"] = (merged["payout"] / merged["over_price"]).round(2)
	merged["payout_over"] = merged["payout"]
	merged["total_wager"] = (merged["wager_under"] + merged["wager_over"]).round(2)
	merged["arb_ev"] = (merged["payout"] - merged["total_wager"]).round(2)
	merged["roi"] = (merged["arb_ev"] / merged["total_wager"]).round(6)

	merged = merged.merge(event_info, on="event_id", how="left")

	now_utc = pd.Timestamp.now(tz="UTC")
	commence = pd.to_datetime(merged.get("commence_time"), errors="coerce", utc=True)
	merged["hours_until_commence"] = ((commence - now_utc).dt.total_seconds() / 3600.0).round(2)

	# Convenience: keep the base market key visible.
	merged.rename(columns={"_base_market_key": "market_key"}, inplace=True)

	merged.sort_values(by=["arb_ev"], ascending=False, inplace=True)
	merged.reset_index(drop=True, inplace=True)
	return merged


def _fetch_staged_odds() -> pd.DataFrame:
	"""Pull staged player-prop odds, excluding bookmakers we don't pair against."""
	df = fetch_data(
		f"SELECT * FROM {STAGE_TABLE_ODDS} "
		f"WHERE bookmaker_key NOT IN ('williamhill_us', 'draftkings', 'fanatics', 'underdog', 'betonlineag', 'betrivers', 'bovada')"
	)
	if df is None or df.empty:
		return pd.DataFrame()
	df = df.copy()
	if "commence_time" in df.columns:
		df["commence_time"] = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
	return df


def _append_history_and_alert(df: pd.DataFrame, history_table: str) -> int:
	"""Filter to new high-ROI rows, append to history table, fire Discord on high-value."""
	if df is None or df.empty:
		return 0

	working = df.copy()
	if "roi" not in working.columns or "hours_until_commence" not in working.columns:
		return 0

	working["roi"] = pd.to_numeric(working["roi"], errors="coerce")
	working = working[working["roi"] > 0.0075].copy()

	working["hours_until_commence"] = pd.to_numeric(working["hours_until_commence"], errors="coerce")
	working = working[working["hours_until_commence"].notna()].copy()
	working = working[working["hours_until_commence"] >= 0.0].copy()
	if working.empty:
		return 0

	working["opportunity_key"] = _build_opportunity_key(working)
	working.dropna(subset=["opportunity_key"], inplace=True)
	if working.empty:
		return 0

	try:
		existing = fetch_data(f"SELECT opportunity_key FROM {history_table}")
		existing_keys = (
			set(existing["opportunity_key"].astype("string").fillna("").tolist())
			if not existing.empty
			else set()
		)
	except Exception:
		existing_keys = set()

	new_rows = working[~working["opportunity_key"].isin(existing_keys)].copy()
	if new_rows.empty:
		return 0

	insert_data(new_rows, history_table, if_exists="append")

	high_value = new_rows[
		pd.to_numeric(new_rows.get("roi"), errors="coerce") > HIGH_VALUE_ROI_THRESHOLD
	].copy()
	if not high_value.empty:
		_send_discord_message(
			_format_discord_opportunities_message(high_value, label=history_table)
		)
	return int(len(new_rows))


@dag(
	dag_id="bg_arbitrage_player_props",
	description="Compute player-prop arbitrage from staged OddsAPI lines",
	schedule=[odds_player_props_staged_asset],
	start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
	catchup=False,
	max_active_runs=1,
	tags=["arbitrage", "player-props", "odds"],
)
def bg_arbitrage_player_props_pipeline() -> None:
	# DataFrames are not pushed through XCom — Airflow 3's serializer rejects them.
	# Each task pulls the stage table itself and writes results directly to Postgres,
	# matching the pattern in bg_unified_dag.py.

	@task(outlets=[player_props_arbitrage_complete_asset])
	def process_player_props_arbitrage() -> int:
		staged = _fetch_staged_odds()
		if staged.empty:
			raise AirflowSkipException("No staged odds rows")

		arbitrage = build_player_props_arbitrage(
			staged, base_wager=100.0, exclude_market_substring="alternate"
		)
		_append_history_and_alert(arbitrage, HISTORY_TABLE)

		if arbitrage is None or arbitrage.empty:
			raise AirflowSkipException("No player-props arbitrage records to load")
		filtered = (
			arbitrage[~arbitrage["market_key"].isin(MARKET_BLACKLIST)].copy()
			if MARKET_BLACKLIST
			else arbitrage
		)
		if filtered.empty:
			raise AirflowSkipException("All player-props records filtered by blacklist")
		insert_data(filtered, TARGET_TABLE, if_exists="replace")
		return int(len(filtered))

	@task()
	def process_player_props_arbitrage_alt() -> int:
		staged = _fetch_staged_odds()
		if staged.empty:
			raise AirflowSkipException("No staged odds rows")

		arbitrage_alt = build_player_props_arbitrage_alt(
			staged, base_wager=100.0, alternate_suffix="_alternate"
		)
		_append_history_and_alert(arbitrage_alt, HISTORY_TABLE_ALT)

		if arbitrage_alt is None or arbitrage_alt.empty:
			raise AirflowSkipException("No alternate-market player-props arbitrage records to load")
		filtered = (
			arbitrage_alt[~arbitrage_alt["market_key"].isin(MARKET_BLACKLIST)].copy()
			if MARKET_BLACKLIST
			else arbitrage_alt
		)
		if filtered.empty:
			raise AirflowSkipException("All alternate-market records filtered by blacklist")
		insert_data(filtered, TARGET_TABLE_ALT, if_exists="replace")
		return int(len(filtered))

	@task()
	def trigger_bot_execution(_loaded: int, _alt_loaded: int) -> None:
		"""Insert PENDING tasks into bot_execution_queue so the local worker picks them up."""
		for _ in range(BOT_EXECUTION_TASK_COUNT):
			execute_raw_sql(
				"INSERT INTO bot_execution_queue (status) VALUES ('PENDING')",
				fetch_results=False,
			)

	loaded = process_player_props_arbitrage()
	alt_loaded = process_player_props_arbitrage_alt()
	trigger_bot_execution(loaded, alt_loaded)


dag = bg_arbitrage_player_props_pipeline()

