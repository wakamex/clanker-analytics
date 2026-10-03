"""API-equivalent prices for token rows and subscription plans, from model-prices."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import os
from pathlib import Path
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
    return model_prices.pricing_basis()


# Tool names accepted in plan settings, for the tools whose usage package reports a plan.
PLAN_KEYS = {"claude": "Claude Code", "codex": "Codex", "gemini": "Gemini", "zcode": "Zcode"}

CONFIG_PATH = (Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
               / "clanker-analytics" / "config.toml")


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


def _price_rows(rows: list[tuple], today: datetime) -> Iterator[tuple]:
    """Yield (rowid, cost when run, cost at today's prices) for each fetched row."""
    for rowid, tool, model, timestamp, date, uncached, output, written, read, written_1h in rows:
        at = timestamp or date
        historical = current = None
        if model and at:
            # cache_write_tokens counts both cache lifetimes; the one-hour part is priced apart.
            written_1h = written_1h or 0
            usage = {
                "input_tokens": uncached or 0, "output_tokens": output or 0,
                "cache_write_tokens": max((written or 0) - written_1h, 0),
                "cache_write_1h_tokens": written_1h, "cache_read_tokens": read or 0,
            }
            try:
                historical = model_prices.cost(model, provider=TOOL_PROVIDERS.get(tool),
                                             at=at, **usage)
                current = model_prices.cost(model, provider=TOOL_PROVIDERS.get(tool),
                                          at=at, prices_at=today, **usage)
            except ValueError:
                pass
        yield rowid, historical, current


def price_tokens(db: duckdb.DuckDBPyConnection, all_rows: bool) -> None:
    """Price unpriced rows, or every row, at the rates in effect when each request ran
    (api_cost_usd) and at today's price list (current_api_cost_usd).

    AOP runs whose model model-prices cannot price keep the cost AOP recorded.
    """
    columns = {row[0] for row in db.sql("DESCRIBE tokens").fetchall()}
    for column, kind in (("api_cost_usd", "DOUBLE"), ("current_api_cost_usd", "DOUBLE"),
                         ("pricing", "VARCHAR"), ("cache_write_1h_tokens", "INT")):
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
                              current_api_cost_usd = prices.current
            FROM read_csv('{path.as_posix()}', header = false, auto_detect = false,
                          delim = ',', columns = {{
                'id': 'BIGINT', 'historical': 'DOUBLE', 'current': 'DOUBLE'}}) prices
            WHERE tokens.rowid = prices.id
        """)
    db.execute(f"""
        UPDATE tokens SET
            pricing = CASE
                WHEN api_cost_usd IS NOT NULL THEN 'model-prices'
                WHEN source_kind = 'aop' AND cost_usd IS NOT NULL THEN 'aop'
                ELSE 'unpriced'
            END,
            api_cost_usd = coalesce(
                api_cost_usd, CASE WHEN source_kind = 'aop' THEN cost_usd END),
            current_api_cost_usd = coalesce(
                current_api_cost_usd, CASE WHEN source_kind = 'aop' THEN cost_usd END)
        WHERE {selected}
    """)
