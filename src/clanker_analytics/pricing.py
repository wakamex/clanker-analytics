"""API-equivalent prices for token rows and subscription plans, from llm-prices."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from typing import Iterator

import duckdb
import llm_prices

# Provider hints for resolving each tool's model names; other tools report names that
# resolve without one, such as "opencode/glm-5.3".
TOOL_PROVIDERS = {
    "Agy": "google",
    "Claude Code": "anthropic",
    "Codex": "openai",
    "DeepSeek Harness": "deepseek",
    "Gemini": "google",
    "Grok": "xai",
    "Zcode": "zai",
}

# The lab whose plan each tool's usage package reports.
PLAN_PROVIDERS = {
    "Claude Code": "anthropic",
    "Codex": "openai",
    "Gemini": "google",
    "Zcode": "zai",
}


def pricing_basis() -> str:
    """Identify the price data, so cached prices are recomputed when it changes."""
    return llm_prices.pricing_basis()


def plan_price(tool: str, plan: str, at: datetime | str | None = None) -> float:
    """Monthly price of a detected plan, or 0 when llm-prices has no price for it."""
    provider = PLAN_PROVIDERS.get(tool)
    found = llm_prices.plan(provider, plan, at=at) if provider else None
    return found.usd_per_month if found else 0


def _price_rows(rows: list[tuple], today: datetime) -> Iterator[tuple]:
    """Yield (rowid, cost when run, cost at today's prices) for each fetched row."""
    for rowid, tool, model, timestamp, date, uncached, output, written, read in rows:
        at = timestamp or date
        historical = current = None
        if model and at:
            usage = {
                "input_tokens": uncached or 0, "output_tokens": output or 0,
                "cache_write_tokens": written or 0, "cache_read_tokens": read or 0,
            }
            try:
                historical = llm_prices.cost(model, provider=TOOL_PROVIDERS.get(tool),
                                             at=at, **usage)
                current = llm_prices.cost(model, provider=TOOL_PROVIDERS.get(tool),
                                          at=at, prices_at=today, **usage)
            except ValueError:
                pass
        yield rowid, historical, current


def price_tokens(db: duckdb.DuckDBPyConnection, all_rows: bool) -> None:
    """Price unpriced rows, or every row, at the rates in effect when each request ran
    (api_cost_usd) and at today's price list (current_api_cost_usd).

    AOP runs whose model llm-prices cannot price keep the cost AOP recorded.
    """
    columns = {row[0] for row in db.sql("DESCRIBE tokens").fetchall()}
    for column, kind in (("api_cost_usd", "DOUBLE"), ("current_api_cost_usd", "DOUBLE"),
                         ("pricing", "VARCHAR")):
        if column not in columns:
            db.execute(f"ALTER TABLE tokens ADD COLUMN {column} {kind}")
    selected = "true" if all_rows else "pricing IS NULL"
    rows = db.execute(f"""
        SELECT rowid, tool, model, timestamp::VARCHAR, date::VARCHAR, input_tokens,
               output_tokens, cache_write_tokens, cache_read_tokens
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
                              current_api_cost_usd = prices.current
            FROM read_csv('{path.as_posix()}', header = false, auto_detect = false,
                          delim = ',', columns = {{
                'id': 'BIGINT', 'historical': 'DOUBLE', 'current': 'DOUBLE'}}) prices
            WHERE tokens.rowid = prices.id
        """)
    db.execute(f"""
        UPDATE tokens SET
            pricing = CASE
                WHEN api_cost_usd IS NOT NULL THEN 'llm-prices'
                WHEN source_kind = 'aop' AND cost_usd IS NOT NULL THEN 'aop'
                ELSE 'unpriced'
            END,
            api_cost_usd = coalesce(
                api_cost_usd, CASE WHEN source_kind = 'aop' THEN cost_usd END),
            current_api_cost_usd = coalesce(
                current_api_cost_usd, CASE WHEN source_kind = 'aop' THEN cost_usd END)
        WHERE {selected}
    """)
