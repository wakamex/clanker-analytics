"""Explain what changed in usage between two periods, and how well prompt caching worked.

Both reports read the tokens table. A request is a cold start when it writes nearly its
whole prompt to the cache: it found nothing cached, so it paid the cache-write price for
tokens a warm cache would have served at the read price.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb

from clanker_analytics.pricing import COST_PARTS

# A cold start writes at least this share of its prompt and at least this many tokens;
# smaller prompts are too short to cache.
COLD_WRITE_SHARE = 0.8
COLD_MIN_TOKENS = 20_000
# Seconds a cache entry survives without use. Claude Code writes the main conversation to
# Anthropic's one-hour cache and subagents to the five-minute one; OpenAI's GPT-5.6 and
# later keep entries for 30 minutes.
TTL_SECONDS = {"Claude Code": 300, "Codex": 1800}
ONE_HOUR = 3600

DIMENSIONS = {
    "tool": "tool",
    "project": "project",
    "thread": "execution_type",
    "session": "project || ' ' || CASE WHEN session LIKE 'rollout-%' THEN right(session, 8) ELSE left(session, 8) END",
}


def _requests_view(db: duckdb.DuckDBPyConnection) -> None:
    """Each request with its gap since the previous one in the same transcript, and
    whether and why it was a cold start."""
    ttl = " ".join(f"WHEN tool = '{tool}' THEN {seconds}" for tool, seconds in TTL_SECONDS.items())
    db.execute(f"""
        CREATE OR REPLACE TEMP VIEW requests AS
        WITH ordered AS (
            SELECT *,
                   try_cast(timestamp AS TIMESTAMPTZ) AS ts,
                   coalesce(input_tokens, 0) + coalesce(cache_read_tokens, 0)
                     + coalesce(cache_write_tokens, 0) AS prompt_tokens,
                   CASE WHEN coalesce(cache_write_1h_tokens, 0) > 0 THEN {ONE_HOUR}
                        {ttl} ELSE 300 END AS ttl_seconds
            FROM tokens
        ), gaps AS (
            SELECT *,
                   epoch(ts - lag(ts) OVER (PARTITION BY source_file ORDER BY ts)) AS gap_seconds
            FROM ordered
        )
        SELECT *,
               CASE
                   WHEN coalesce(cache_write_tokens, 0) < {COLD_MIN_TOKENS}
                     OR cache_write_tokens < {COLD_WRITE_SHARE} * prompt_tokens THEN NULL
                   WHEN gap_seconds IS NULL THEN 'first request'
                   WHEN gap_seconds > ttl_seconds THEN 'expired while idle'
                   ELSE 'no idle gap'
               END AS cold_start,
               coalesce(api_cache_write_usd, 0) + coalesce(api_cache_write_1h_usd, 0)
                 AS write_cost
        FROM gaps
    """)


def _periods(recent: timedelta, baseline: timedelta, now: datetime) -> list[tuple[str, datetime, datetime]]:
    return [("baseline", now - recent - baseline, now - recent), ("recent", now - recent, now)]


def build_diagnosis(db: duckdb.DuckDBPyConnection, recent: timedelta, baseline: timedelta,
                    now: datetime | None = None, limit: int = 8) -> dict[str, Any]:
    """Compare a recent period with the one before it, per day so lengths may differ."""
    now = now or datetime.now(timezone.utc)
    _requests_view(db)
    periods = _periods(recent, baseline, now)
    case = " ".join(f"WHEN ts >= '{start.isoformat()}' AND ts < '{end.isoformat()}' THEN '{name}'"
                    for name, start, end in periods)
    days = {name: (end - start).total_seconds() / 86400 for name, start, end in periods}
    db.execute(f"""
        CREATE OR REPLACE TEMP VIEW periods AS
        SELECT *, CASE {case} END AS period FROM requests
    """)
    summary = {}
    for name, _, _ in periods:
        row = db.execute("""
            SELECT coalesce(sum(api_cost_usd), 0), coalesce(sum(output_tokens), 0),
                   coalesce(sum(cache_read_tokens), 0), coalesce(sum(prompt_tokens), 0),
                   coalesce(sum(cache_write_tokens), 0), count(cold_start),
                   coalesce(sum(write_cost) FILTER (WHERE cold_start IS NOT NULL), 0), count(*)
            FROM periods WHERE period = ?
        """, [name]).fetchone()
        cost, output, read, prompt, written, colds, cold_cost, requests = row
        per_day = days[name]
        summary[name] = {
            "days": per_day,
            "requests_per_day": requests / per_day,
            "cost_per_day": cost / per_day,
            "output_per_day": output / per_day,
            "cost_per_million_output": cost / output * 1e6 if output else None,
            "cache_hit_share": read / prompt if prompt else None,
            "cache_writes_per_day": written / per_day,
            "cold_starts_per_day": colds / per_day,
            "cold_start_cost_per_day": cold_cost / per_day,
        }
    changes = {}
    for label, expression in DIMENSIONS.items():
        rows = db.execute(f"""
            SELECT {expression} AS key,
                   coalesce(sum(api_cost_usd) FILTER (WHERE period = 'baseline'), 0),
                   coalesce(sum(api_cost_usd) FILTER (WHERE period = 'recent'), 0)
            FROM periods WHERE period IS NOT NULL GROUP BY 1
        """).fetchall()
        changes[label] = _ranked(rows, days, limit)
    parts = ", ".join(
        f"coalesce(sum(api_{part}_usd) FILTER (WHERE period = '{name}'), 0)"
        for name, _, _ in periods for part in COST_PARTS)
    totals = db.execute(f"""
        SELECT {parts},
               coalesce(sum(api_cost_usd) FILTER (WHERE period = 'baseline'), 0),
               coalesce(sum(api_cost_usd) FILTER (WHERE period = 'recent'), 0)
        FROM periods
    """).fetchone()
    width = len(COST_PARTS)
    by_part = [(part.replace("_", " "), totals[i], totals[width + i]) for i, part in enumerate(COST_PARTS)]
    # Costs without a token split, such as AOP's own records for unpriced models.
    by_part.append(("other", totals[-2] - sum(b for _, b, _ in by_part),
                    totals[-1] - sum(r for _, _, r in by_part)))
    changes["token type"] = _ranked(by_part, days, limit)
    return {"periods": {name: (start.isoformat(), end.isoformat()) for name, start, end in periods},
            "summary": summary, "changes": changes}


def _ranked(rows: list[tuple], days: dict[str, float], limit: int) -> list[dict[str, Any]]:
    ranked = [{"key": key or "(none)", "baseline_per_day": base / days["baseline"],
               "recent_per_day": recent / days["recent"]} for key, base, recent in rows]
    for item in ranked:
        item["change_per_day"] = item["recent_per_day"] - item["baseline_per_day"]
    ranked.sort(key=lambda item: abs(item["change_per_day"]), reverse=True)
    return [item for item in ranked[:limit] if abs(item["change_per_day"]) >= 0.005]


def build_cache_health(db: duckdb.DuckDBPyConnection, since: datetime, limit: int = 12) -> dict[str, Any]:
    """How much prompt caching saved, what misses cost, and why caches went cold."""
    _requests_view(db)
    db.execute(f"""
        CREATE OR REPLACE TEMP VIEW cache_window AS
        SELECT *, coalesce(api_input_usd, 0) + coalesce(api_cache_read_usd, 0) + write_cost
                    AS prompt_cost
        FROM requests
        WHERE ts >= '{since.isoformat()}' AND api_prompt_uncached_usd IS NOT NULL
          AND prompt_tokens > 0
    """)
    metrics = """
        count(*) AS requests,
        sum(prompt_tokens) AS prompt_tokens,
        sum(cache_read_tokens) / nullif(sum(prompt_tokens), 0) AS hit_share,
        1 - sum(prompt_cost) / nullif(sum(api_prompt_uncached_usd), 0) AS discount,
        1 - sum(api_prompt_read_usd) / nullif(sum(api_prompt_uncached_usd), 0) AS best_discount,
        sum(prompt_cost) - sum(api_prompt_read_usd) AS miss_cost,
        sum(cache_write_1h_tokens) / nullif(sum(cache_write_tokens), 0) AS one_hour_share,
        sum(cache_read_tokens) / nullif(sum(cache_write_tokens), 0) AS reads_per_write,
        count(cold_start) FILTER (WHERE cold_start = 'first request') AS cold_first,
        count(cold_start) FILTER (WHERE cold_start = 'expired while idle') AS cold_expired,
        count(cold_start) FILTER (WHERE cold_start = 'no idle gap') AS cold_other,
        coalesce(sum(write_cost) FILTER (WHERE cold_start = 'expired while idle'), 0)
          AS expired_cost
    """
    def rows(group: str, order: str = "miss_cost DESC", extra: str = "") -> list[dict[str, Any]]:
        result = db.execute(f"SELECT {group} AS key, {metrics} {extra} FROM cache_window "
                            f"GROUP BY 1 ORDER BY {order} LIMIT {limit}")
        names = [column[0] for column in result.description]
        return [dict(zip(names, row)) for row in result.fetchall()]

    return {
        "since": since.isoformat(),
        "by_thread": rows("tool || ' ' || execution_type"),
        "by_session": rows("project || ' ' || CASE WHEN session LIKE 'rollout-%' THEN right(session, 8) ELSE left(session, 8) END || ' ' || execution_type"),
        "one_hour_subagents": _simulate_one_hour_subagents(db),
    }


def _simulate_one_hour_subagents(db: duckdb.DuckDBPyConnection) -> dict[str, float] | None:
    """Claude Code subagent prompt cost if subagents used the one-hour cache.

    Cold starts after idling five minutes to an hour would have read the cache instead,
    and every remaining cache write would cost 2x input instead of 1.25x.
    """
    row = db.execute(f"""
        SELECT sum(prompt_cost),
               sum(CASE WHEN cold_start = 'expired while idle' AND gap_seconds <= {ONE_HOUR}
                        THEN api_prompt_read_usd
                        ELSE coalesce(api_input_usd, 0) + coalesce(api_cache_read_usd, 0)
                             + 2 * cache_write_tokens * api_prompt_uncached_usd / prompt_tokens
                   END)
        FROM cache_window
        WHERE tool = 'Claude Code' AND execution_type = 'subagent'
    """).fetchone()
    if not row or not row[0]:
        return None
    return {"actual": row[0], "one_hour": row[1]}


def _money(value: float | None) -> str:
    if value is None:
        return "-"
    return f"${value:,.0f}" if abs(value) >= 100 else f"${value:,.2f}"


def _share(value: float | None) -> str:
    return "-" if value is None else f"{value:.1%}"


def _table(headers: list[str], rows: list[list[str]]) -> None:
    widths = [max(len(str(cell)) for cell in column) for column in zip(headers, *rows)]
    for row in [headers, *rows]:
        print("  ".join(str(cell).ljust(width) if i == 0 else str(cell).rjust(width)
                        for i, (cell, width) in enumerate(zip(row, widths))))


def print_diagnosis(report: dict[str, Any]) -> None:
    base, recent = report["summary"]["baseline"], report["summary"]["recent"]
    print(f"Recent {recent['days']:g} days compared with the {base['days']:g} days before, per day.\n")
    lines = [
        ("API-equivalent cost", _money, "cost_per_day"),
        ("Output tokens", lambda v: f"{v / 1e6:,.2f}M", "output_per_day"),
        ("Cost per million output tokens", _money, "cost_per_million_output"),
        ("Requests", lambda v: f"{v:,.0f}", "requests_per_day"),
        ("Prompt tokens read from cache", _share, "cache_hit_share"),
        ("Cache writes", lambda v: f"{v / 1e6:,.1f}M", "cache_writes_per_day"),
        ("Cold starts", lambda v: f"{v:,.0f}", "cold_starts_per_day"),
        ("Cold-start cache writes", _money, "cold_start_cost_per_day"),
    ]
    _table(["Per day", "Before", "Recent"],
           [[label, fmt(base[key]), fmt(recent[key])] for label, fmt, key in lines])
    for label, items in report["changes"].items():
        if not items:
            continue
        print(f"\nCost change per day by {label}:")
        _table([label.capitalize(), "Before", "Recent", "Change"],
               [[item["key"], _money(item["baseline_per_day"]), _money(item["recent_per_day"]),
                 ("+" if item["change_per_day"] >= 0 else "-") + _money(abs(item["change_per_day"]))]
                for item in items])


def print_cache_health(report: dict[str, Any]) -> None:
    print(f"Prompt caching since {report['since'][:16].replace('T', ' ')} UTC.\n"
          "Discount: what caching saved on prompt tokens, against the most a fully warm "
          "cache could save. Miss cost: what was paid beyond a fully warm cache.\n")
    headers = ["", "Requests", "Read from cache", "Discount", "Best", "Miss cost",
               "1h writes", "Reads/write", "Cold: first", "Expired", "Other", "Expired cost"]

    def row(item: dict[str, Any]) -> list[str]:
        return [item["key"], f"{item['requests']:,}", _share(item["hit_share"]),
                _share(item["discount"]), _share(item["best_discount"]), _money(item["miss_cost"]),
                _share(item["one_hour_share"]),
                "-" if item["reads_per_write"] is None else f"{item['reads_per_write']:.0f}",
                f"{item['cold_first']:,}", f"{item['cold_expired']:,}", f"{item['cold_other']:,}",
                _money(item["expired_cost"])]

    _table(["By tool and thread", *headers[1:]], [row(item) for item in report["by_thread"]])
    print()
    _table(["Sessions by miss cost", *headers[1:]], [row(item) for item in report["by_session"]])
    simulation = report["one_hour_subagents"]
    if simulation:
        saved = simulation["actual"] - simulation["one_hour"]
        print(f"\nClaude Code subagents paid {_money(simulation['actual'])} for prompt tokens. "
              f"With the one-hour cache (subagentPromptCacheTtl \"1h\") they would have paid about "
              f"{_money(simulation['one_hour'])}, {'saving' if saved >= 0 else 'costing'} "
              f"{_money(abs(saved))} ({abs(saved) / simulation['actual']:.0%}).")


def build_throughput(db: duckdb.DuckDBPyConnection, since: datetime,
                     now: datetime | None = None) -> dict[str, Any]:
    """Output tokens per second by UTC day, with how many transcripts produced them at once.

    Each transcript, a main conversation or a subagent, counts as one stream while it
    makes requests within an hour. AOP runs are recorded as one summary at completion, so
    they are counted as finished runs rather than streams.
    """
    now = now or datetime.now(timezone.utc)
    db.execute(f"""
        CREATE OR REPLACE TEMP VIEW hourly AS
        SELECT date_trunc('hour', timezone('UTC', ts)) AS hour, sum(output_tokens) AS output,
               count(DISTINCT source_file) FILTER (WHERE source_kind = 'native') AS streams,
               count(DISTINCT source_file) FILTER (WHERE source_kind = 'aop') AS aop_runs,
               sum(output_tokens) FILTER (WHERE execution_type = 'subagent') AS subagent_output
        FROM (SELECT *, try_cast(timestamp AS TIMESTAMPTZ) AS ts FROM tokens)
        WHERE ts >= '{since.isoformat()}' AND ts < '{now.isoformat()}'
        GROUP BY 1
    """)
    days = []
    for day, output, subagent, busiest, active, median_streams, peak, aop_runs in db.execute("""
        SELECT date_trunc('day', hour)::DATE, sum(output), coalesce(sum(subagent_output), 0),
               max(output), count(*) FILTER (WHERE output > 0),
               median(streams) FILTER (WHERE output > 0), max(streams), sum(aop_runs)
        FROM hourly GROUP BY 1 ORDER BY 1
    """).fetchall():
        start = max(datetime(day.year, day.month, day.day, tzinfo=timezone.utc), since)
        end = min(datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(days=1), now)
        seconds = max((end - start).total_seconds(), 1)
        days.append({
            "date": day.isoformat(), "output_tokens": output,
            "tokens_per_second": output / seconds, "busiest_hour_tokens_per_second": busiest / 3600,
            "active_hours": active, "typical_streams": median_streams, "peak_streams": peak,
            "subagent_output_share": subagent / output if output else None,
            "aop_runs": aop_runs,
            "covered_hours": seconds / 3600,
        })
    return {"since": since.isoformat(), "days": days}


def print_throughput(report: dict[str, Any]) -> None:
    print("Output tokens per second by UTC day, averaged over the whole day including idle "
          "time. Streams are transcripts, main conversations or subagents, making requests "
          "in the same hour.\n")
    rows = []
    for day in report["days"]:
        partial = "" if day["covered_hours"] >= 23.99 else f" ({day['covered_hours']:.0f}h)"
        rows.append([
            day["date"] + partial, f"{day['output_tokens'] / 1e6:,.2f}M",
            f"{day['tokens_per_second']:,.0f}", f"{day['busiest_hour_tokens_per_second']:,.0f}",
            f"{day['active_hours']}", f"{day['typical_streams'] or 0:.0f}", f"{day['peak_streams']}",
            _share(day["subagent_output_share"]), f"{day['aop_runs']:,}",
        ])
    _table(["Day", "Output", "Tokens/s", "Busiest hour", "Active hours", "Typical streams",
            "Peak streams", "From subagents", "AOP runs"], rows)
