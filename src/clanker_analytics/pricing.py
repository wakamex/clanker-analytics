"""API-equivalent prices for token rows and subscription plans, from model-prices."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import tempfile
import tomllib
from typing import Iterator

import duckdb
import model_prices

# Provider hints for resolving each tool's model names; other tools report names that
# resolve without one, such as "opencode/glm-5.3".
TOOL_PROVIDERS = {
    "Agy": "google",
    "Claude Code": "anthropic",
    "Codex": "openai",
    "DeepSeek Harness": "deepseek",
    "Gemini": "google",
    "Grok": "xai",
    "Muse": "meta",
    "Zcode": "zai",
}

# The lab whose plan each tool's usage package reports.
PLAN_PROVIDERS = {
    "Claude Code": "anthropic",
    "Codex": "openai",
    "Gemini": "google",
    "Zcode": "zai",
}


# Raised when the rules in price_tokens change, so cached prices are recomputed.
PRICING_RULES = 2


def pricing_basis() -> str:
    """Identify the price data."""
    return model_prices.pricing_basis()


def cache_pricing_key() -> str:
    """Identify the price data and pricing rules, so cached prices are recomputed when
    either changes."""
    return f"{pricing_basis()}; rules {PRICING_RULES}"


def refresh_prices() -> None:
    """Use the newest published price data, downloading it when the local copy is a day old.
    A failed download keeps the local copy and warns."""
    model_prices.refresh()


# Tool names accepted in plan settings, for the tools whose usage package reports a plan.
PLAN_KEYS = {"claude": "Claude Code", "codex": "Codex", "gemini": "Gemini", "zcode": "Zcode"}

CONFIG_PATH = (Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
               / "clanker-analytics" / "config.toml")


# Google's plan tier ids. Google names both Ultra tiers "Google AI Ultra"; only the id tells
# them apart. g1-ultra-lite-tier is the $100 tier. g1-ultra-tier, which may be the $200
# tier, has not been seen from a live account, so it stays the unpriced "ultra".
GOOGLE_TIERS = {"g1-pro-tier": "pro", "g1-ultra-lite-tier": "ultra_100", "g1-ultra-tier": "ultra"}


def normalize_plan(plan: str) -> str:
    """A usage package's plan name as the model-prices plan id. Google plans are reported
    as "Google AI Ultra (g1-ultra-lite-tier)", the name followed by the tier id."""
    tier = re.fullmatch(r".*\((g1-[a-z-]+)\)", plan.strip())
    if tier and tier.group(1) in GOOGLE_TIERS:
        return GOOGLE_TIERS[tier.group(1)]
    if plan in GOOGLE_TIERS:
        return GOOGLE_TIERS[plan]
    return plan.replace("default_claude_", "")


def plan_price(tool: str, plan: str, at: datetime | str | None = None) -> float:
    """Monthly price of a detected plan, or 0 when model-prices has no price for it."""
    provider = PLAN_PROVIDERS.get(tool)
    found = model_prices.plan(provider, plan, at=at) if provider else None
    return found.usd_per_month if found else 0


def plan_choice(key: str, value: object) -> tuple[str, tuple[str, float]]:
    """Resolve a plan setting, an model-prices plan id or a monthly USD price, for a tool."""
    tool = PLAN_KEYS.get(key.lower())
    if tool is None:
        raise ValueError(f"no plan setting for {key!r}; use one of {', '.join(PLAN_KEYS)}")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        price = float(value)
    else:
        try:
            price = float(str(value))
        except ValueError:
            price = plan_price(tool, str(value))
            if not price:
                known = ", ".join(model_prices.plan_ids(PLAN_PROVIDERS[tool]))
                raise ValueError(f"no {key} plan {value!r}; use one of {known}, "
                                 "or a monthly USD price") from None
            return tool, (str(value), price)
    if price <= 0:
        raise ValueError(f"{key} plan price must be positive")
    return tool, ("custom", price)


def configured_plans(path: Path = CONFIG_PATH) -> dict[str, tuple[str, float]]:
    """Plans named in the [plans] table of the config file, keyed by tool."""
    if not path.exists():
        return {}
    settings = tomllib.loads(path.read_text()).get("plans", {})
    try:
        return dict(plan_choice(key, value) for key, value in settings.items())
    except ValueError as error:
        raise ValueError(f"{path}: {error}") from None


# Columns holding each row's cost when it ran, split by token type, then the row's whole
# prompt priced as uncached input and as cache reads, which bound what caching achieved.
COST_PARTS = ("input", "output", "cache_read", "cache_write", "cache_write_1h")
PROMPT_BOUNDS = ("prompt_uncached", "prompt_read")


def _price_rows(rows: list[tuple], today: datetime) -> Iterator[tuple]:
    """Yield (rowid, cost when run, cost at today's prices, the model whose rates stood in
    for an estimated price, the cost when run split into COST_PARTS, then PROMPT_BOUNDS)
    for each fetched row.

    A model sold with no per-token price, such as Muse Wonder Full, which Meta sells only
    in Muse Code subscriptions, is priced at the rates model-prices estimates for it."""
    for rowid, tool, model, timestamp, date, uncached, output, written, read, written_1h in rows:
        at = timestamp or date
        historical = current = estimated_from = None
        parts: tuple = (None,) * (len(COST_PARTS) + len(PROMPT_BOUNDS))
        if model and at:
            # cache_write_tokens counts both cache lifetimes; the one-hour part is priced apart.
            written_1h = written_1h or 0
            usage = {
                "input_tokens": uncached or 0, "output_tokens": output or 0,
                "cache_write_tokens": max((written or 0) - written_1h, 0),
                "cache_write_1h_tokens": written_1h, "cache_read_tokens": read or 0,
            }
            try:
                found = model_prices.rates(model, provider=TOOL_PROVIDERS.get(tool), at=at,
                                           estimate=True)
                if found is not None:
                    estimated_from = found.estimated_from
                    breakdown = found.breakdown(**usage)
                    historical = breakdown.total
                    prompt = sum(usage[key] for key in (
                        "input_tokens", "cache_read_tokens", "cache_write_tokens",
                        "cache_write_1h_tokens"))
                    parts = (*(getattr(breakdown, part) for part in COST_PARTS),
                             found.breakdown(input_tokens=prompt).input,
                             found.breakdown(cache_read_tokens=prompt).cache_read)
                current = model_prices.cost(model, provider=TOOL_PROVIDERS.get(tool),
                                          at=at, prices_at=today, estimate=True, **usage)
            except ValueError:
                pass
        yield rowid, historical, current, estimated_from, *parts


def price_tokens(db: duckdb.DuckDBPyConnection, all_rows: bool) -> None:
    """Price unpriced rows, or every row, at the rates in effect when each request ran
    (api_cost_usd) and at today's price list (current_api_cost_usd).

    AOP runs whose model model-prices cannot price keep the cost AOP recorded.
    """
    columns = {row[0] for row in db.sql("DESCRIBE tokens").fetchall()}
    for column, kind in (("api_cost_usd", "DOUBLE"), ("current_api_cost_usd", "DOUBLE"),
                         ("pricing", "VARCHAR"), ("estimated_from", "VARCHAR"),
                         ("cache_write_1h_tokens", "INT"),
                         ("model_assumed", "BOOLEAN"),
                         *((f"api_{part}_usd", "DOUBLE")
                           for part in (*COST_PARTS, *PROMPT_BOUNDS))):
        if column not in columns:
            db.execute(f"ALTER TABLE tokens ADD COLUMN {column} {kind}")
    selected = "true" if all_rows else "pricing IS NULL"
    rows = db.execute(f"""
        SELECT rowid, tool, model, timestamp::VARCHAR, date::VARCHAR, input_tokens,
               output_tokens, cache_write_tokens, cache_read_tokens, cache_write_1h_tokens
        FROM tokens WHERE {selected}
    """).fetchall()
    # Pricing in Python and loading the results as CSV is far faster than a per-row
    # DuckDB Python function.
    if not rows:
        return
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "prices.csv"
        with path.open("w", newline="") as stream:
            csv.writer(stream).writerows(_price_rows(rows, datetime.now(timezone.utc)))
        db.execute(f"""
            UPDATE tokens SET api_cost_usd = prices.historical,
                              current_api_cost_usd = prices.current,
                              estimated_from = prices.estimated_from,
                              {", ".join(f"api_{part}_usd = prices.{part}"
                                         for part in (*COST_PARTS, *PROMPT_BOUNDS))}
            FROM read_csv('{path.as_posix()}', header = false, auto_detect = false,
                          delim = ',', columns = {{
                'id': 'BIGINT', 'historical': 'DOUBLE', 'current': 'DOUBLE',
                'estimated_from': 'VARCHAR',
                {", ".join(f"'{part}': 'DOUBLE'" for part in (*COST_PARTS, *PROMPT_BOUNDS))}}}) prices
            WHERE tokens.rowid = prices.id
        """)
    # An AOP run's recorded cost prices each of its model calls at that call's own model,
    # which a result's total usage under one model cannot reproduce, so it is the run's
    # cost. Its parts and its cost at today's prices are model-prices' own, scaled to it.
    scale = "cost_usd / api_cost_usd"
    recorded = "source_kind = 'aop' AND cost_usd IS NOT NULL"
    db.execute(f"""
        UPDATE tokens SET
            pricing = CASE
                WHEN {recorded} THEN 'aop'
                WHEN api_cost_usd IS NOT NULL AND model_assumed THEN 'assumed model'
                WHEN api_cost_usd IS NOT NULL AND estimated_from IS NOT NULL THEN 'estimated'
                WHEN api_cost_usd IS NOT NULL THEN 'model-prices'
                ELSE 'unpriced'
            END,
            api_cost_usd = CASE WHEN {recorded} THEN cost_usd ELSE api_cost_usd END,
            current_api_cost_usd = CASE
                WHEN {recorded} AND api_cost_usd > 0 THEN current_api_cost_usd * {scale}
                WHEN {recorded} THEN cost_usd
                ELSE current_api_cost_usd END,
            estimated_from = CASE WHEN {recorded} THEN NULL ELSE estimated_from END,
            {", ".join(f"""api_{part}_usd = CASE
                WHEN {recorded} AND api_cost_usd > 0 THEN api_{part}_usd * {scale}
                WHEN {recorded} THEN NULL ELSE api_{part}_usd END"""
                       for part in (*COST_PARTS, *PROMPT_BOUNDS))}
        WHERE {selected}
    """)
