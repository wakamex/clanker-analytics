#!/usr/bin/env python3
"""AI coding tool token analytics powered by DuckDB."""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import duckdb

HOME = os.path.expanduser("~").replace("\\", "/")

# Plan detection from cached usage-limits.json files
PLAN_COSTS = {
    # Claude
    "pro": 20, "max_5x": 100, "max_20x": 200,
    # Codex / ChatGPT (handled separately — "pro" means $200 for Codex)
    "plus": 20,
    # Gemini (normalized from g1-pro-tier, g1-ultra-tier)
    "pro": 20, "ultra": 250, "free": 0,
}

def detect_plans() -> dict[str, tuple[str, int]]:
    """Detect subscription plans by shelling out to usage tools. Returns {tool: (plan_name, monthly_cost)}."""
    import subprocess
    plans = {}

    cmds = {
        "Claude Code": [sys.executable, "-m", "ccusage", "json"],
        "Codex": [sys.executable, "-m", "codex_cli_usage", "json"],
        "Gemini": [sys.executable, "-m", "gemini_cli_usage", "json"],
    }
    for tool, cmd in cmds.items():
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                continue
            data = json.loads(result.stdout)
            plan = (data.get("plan")
                    or data.get("account_quota", {}).get("user_tier")
                    or "unknown")
            # Normalize display names
            plan = (plan.replace("default_claude_", "")
                        .replace("g1-pro-tier", "pro")
                        .replace("g1-ultra-tier", "ultra"))
            if tool == "Codex" and plan == "pro":
                cost = 200
            elif tool == "Codex" and plan == "plus":
                cost = 20
            else:
                cost = PLAN_COSTS.get(plan, 0)
            plans[tool] = (plan, cost)
        except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError, OSError):
            continue

    return plans
CACHE_DIR = Path.home() / ".cache" / "clanker-analytics"
CACHE_FILE = CACHE_DIR / "tokens.parquet"

SOURCE_DIRS = [
    Path(HOME) / ".claude" / "projects",
    Path(HOME) / ".codex" / "sessions",
    Path(HOME) / ".gemini" / "tmp",
]

# On Windows, discover WSL home paths for additional data sources
_WSL_HOMES: list[str] = []
if sys.platform == "win32":
    import subprocess as _sp
    try:
        _r = _sp.run(["wsl", "-l", "-q"], capture_output=True, timeout=5)
        if _r.returncode == 0:
            for distro in _r.stdout.decode("utf-16-le", errors="ignore").strip().splitlines():
                distro = distro.strip()
                if not distro:
                    continue
                try:
                    _u = _sp.run(["wsl", "-d", distro, "ls", "/home"],
                                 capture_output=True, text=True, timeout=5)
                    for user in (_u.stdout.strip().splitlines() if _u.returncode == 0 else []):
                        user = user.strip()
                        if user:
                            _WSL_HOMES.append(f"//wsl$/{distro}/home/{user}")
                except (OSError, _sp.TimeoutExpired):
                    pass
    except (FileNotFoundError, OSError, _sp.TimeoutExpired):
        pass

CLAUDE_SQL = f"""
SELECT
    'Claude Code' as tool,
    lower(coalesce(nullif(split_part(replace(cwd, '\\', '/'), '/', -1), ''),
             regexp_extract(replace(filename, '\\', '/'), 'projects/([^/]+)/', 1))) as project,
    cast(sessionId as VARCHAR) as session,
    timestamp[:10] as date,
    cast(message.model as VARCHAR) as model,
    coalesce(cast(message.usage.input_tokens as INTEGER), 0) as input_tokens,
    coalesce(cast(message.usage.output_tokens as INTEGER), 0) as output_tokens,
    coalesce(cast(message.usage.cache_creation_input_tokens as INTEGER), 0) as cache_write_tokens,
    coalesce(cast(message.usage.cache_read_input_tokens as INTEGER), 0) as cache_read_tokens,
    coalesce(cast(message.usage.input_tokens as INTEGER), 0)
      + coalesce(cast(message.usage.output_tokens as INTEGER), 0)
      + coalesce(cast(message.usage.cache_creation_input_tokens as INTEGER), 0)
      + coalesce(cast(message.usage.cache_read_input_tokens as INTEGER), 0) as total_tokens
FROM read_json('{HOME}/.claude/projects/*/*.jsonl',
    format='newline_delimited', filename=true, union_by_name=true,
    ignore_errors=true, maximum_depth=3, maximum_object_size=67108864)
WHERE type = 'assistant'
  AND (isSidechain IS NULL OR isSidechain = false)
  AND message.model != '<synthetic>'
  AND message.usage IS NOT NULL
  AND timestamp IS NOT NULL
  AND NOT contains(replace(filename, '\\', '/'), '/subagents/')
"""

CODEX_SQL = f"""
WITH raw AS (
    SELECT filename, type, timestamp, payload
    FROM read_json('{HOME}/.codex/sessions/**/*.jsonl',
        format='newline_delimited', filename=true, union_by_name=true,
        ignore_errors=true, maximum_depth=4, maximum_object_size=67108864)
    WHERE type IN ('session_meta', 'event_msg')
),
projects AS (
    SELECT filename, split_part(trim(cast(payload.cwd as VARCHAR), '"'), '/', -1) as project
    FROM raw WHERE type = 'session_meta'
),
token_entries AS (
    SELECT
        r.filename, r.timestamp,
        cast(r.payload.info.total_token_usage.total_tokens as BIGINT) as cum_total,
        cast(r.payload.info.last_token_usage.total_tokens as BIGINT) as last_total,
        coalesce(cast(r.payload.info.last_token_usage.input_tokens as INTEGER), 0) as input_tokens,
        coalesce(cast(r.payload.info.last_token_usage.output_tokens as INTEGER), 0) as output_tokens,
        coalesce(cast(r.payload.info.last_token_usage.cached_input_tokens as INTEGER), 0) as cache_read_tokens,
        LAG(cast(r.payload.info.total_token_usage.total_tokens as BIGINT))
            OVER (PARTITION BY r.filename ORDER BY r.timestamp) as prev_cum
    FROM raw r
    WHERE r.type = 'event_msg'
      AND trim(cast(r.payload.type as VARCHAR), '"') = 'token_count'
      AND r.payload.info IS NOT NULL
      AND r.timestamp IS NOT NULL
)
SELECT
    'Codex' as tool,
    lower(coalesce(p.project, regexp_extract(t.filename, '([^/]+)[.]jsonl', 1))) as project,
    regexp_extract(t.filename, '([^/]+)[.]jsonl', 1) as session,
    t.timestamp[:10] as date,
    '' as model,
    t.input_tokens, t.output_tokens, 0 as cache_write_tokens, t.cache_read_tokens,
    CASE
        WHEN t.last_total > 0 THEN t.last_total
        WHEN t.cum_total IS NOT NULL AND t.prev_cum IS NOT NULL THEN t.cum_total - t.prev_cum
        ELSE 0
    END as total_tokens
FROM token_entries t
LEFT JOIN projects p ON t.filename = p.filename
WHERE (t.cum_total IS NULL OR t.cum_total != coalesce(t.prev_cum, -1))
  AND CASE
        WHEN t.last_total > 0 THEN t.last_total
        WHEN t.cum_total IS NOT NULL AND t.prev_cum IS NOT NULL THEN t.cum_total - t.prev_cum
        ELSE 0
      END > 0
"""

GEMINI_SQL = f"""
WITH raw AS (
    SELECT filename,
           regexp_extract(replace(filename, '\\', '/'), 'tmp/([^/]+)/', 1) as project_raw,
           cast(sessionId as VARCHAR) as session,
           unnest(messages) as m
    FROM read_json('{HOME}/.gemini/tmp/*/chats/*.json',
        format='auto', filename=true, union_by_name=true,
        ignore_errors=true, maximum_depth=5, maximum_object_size=67108864)
)
SELECT
    'Gemini' as tool,
    CASE WHEN length(project_raw) = 64 AND regexp_matches(project_raw, '^[0-9a-f]+$')
         THEN project_raw[:8] ELSE lower(project_raw) END as project,
    session,
    m.timestamp[:10] as date,
    cast(m.model as VARCHAR) as model,
    coalesce(m.tokens.input::INT, 0) as input_tokens,
    coalesce(m.tokens.output::INT, 0) as output_tokens,
    0 as cache_write_tokens,
    coalesce(m.tokens.cached::INT, 0) as cache_read_tokens,
    coalesce(m.tokens.total::INT, 0) as total_tokens
FROM raw
WHERE m.tokens IS NOT NULL
  AND m.tokens.total > 0
"""

SOURCES = {
    "claude": ("Claude Code", CLAUDE_SQL),
    "codex": ("Codex", CODEX_SQL),
    "gemini": ("Gemini", GEMINI_SQL),
}

COST_PER_ROW = """
    CASE
        WHEN tool = 'Codex' THEN
            (input_tokens * 1.25 + cache_write_tokens * 1.25
             + cache_read_tokens * 0.125 + output_tokens * 10.0) / 1e6
        WHEN tool = 'Gemini' THEN CASE
            WHEN model LIKE '%2.5%' THEN
                (input_tokens * 1.25 + cache_read_tokens * 0.125
                 + output_tokens * 10.0) / 1e6
            ELSE
                (input_tokens * 2.0 + cache_read_tokens * 0.50
                 + output_tokens * 12.0) / 1e6
        END
        ELSE CASE
            WHEN model LIKE '%opus%' THEN
                (input_tokens * 5.0 + cache_write_tokens * 6.25
                 + cache_read_tokens * 0.50 + output_tokens * 25.0) / 1e6
            WHEN model LIKE '%haiku%' THEN
                (input_tokens * 1.0 + cache_write_tokens * 1.25
                 + cache_read_tokens * 0.10 + output_tokens * 5.0) / 1e6
            ELSE
                (input_tokens * 3.0 + cache_write_tokens * 3.75
                 + cache_read_tokens * 0.30 + output_tokens * 15.0) / 1e6
        END
    END
"""

COST_EXPR = f"fmtcost(sum({COST_PER_ROW}))"

SUMMARY_COLS = f"""
    count(*)::INT as turns,
    fmt(sum(total_tokens)) as total,
    fmt(sum(total_tokens) - 0.9 * sum(cache_read_tokens)) as billable,
    fmt(sum(output_tokens)) as output,
    lpad(printf('%.0f%%', 100.0 * sum(cache_read_tokens) / greatest(sum(total_tokens) - sum(output_tokens), 1)), 4, ' ') as "cache",
    {COST_EXPR} as "api_cost",
    sum(total_tokens)::BIGINT as _sort
"""

QUERIES = {
    "project": f"""
        SELECT * EXCLUDE (_sort) FROM (
            SELECT '*' as project, '*' as tool, {SUMMARY_COLS},
                   min(date) as first_seen, max(date) as last_seen
            FROM tokens
            UNION ALL
            SELECT '*' as project, tool, {SUMMARY_COLS},
                   min(date) as first_seen, max(date) as last_seen
            FROM tokens GROUP BY tool
            UNION ALL
            SELECT project, tool, {SUMMARY_COLS},
                   min(date) as first_seen, max(date) as last_seen
            FROM tokens GROUP BY project, tool
        ) ORDER BY (project = '*' AND tool = '*') DESC, (project = '*') DESC, _sort DESC
        LIMIT {{limit}}
    """,
    "date": f"""
        SELECT * EXCLUDE (_sort) FROM (
            SELECT '*' as date, '*' as tool, {SUMMARY_COLS}
            FROM tokens
            UNION ALL
            SELECT '*' as date, tool, {SUMMARY_COLS}
            FROM tokens GROUP BY tool
            UNION ALL
            SELECT date, tool, {SUMMARY_COLS}
            FROM tokens GROUP BY date, tool
        ) ORDER BY (date = '*' AND tool = '*') DESC, (date = '*') DESC, date DESC, _sort DESC
        LIMIT {{limit}}
    """,
    "model": f"""
        SELECT * EXCLUDE (_sort) FROM (
            SELECT '*' as model, '*' as tool, {SUMMARY_COLS}
            FROM tokens
            UNION ALL
            SELECT '*' as model, tool, {SUMMARY_COLS}
            FROM tokens GROUP BY tool
            UNION ALL
            SELECT model, tool, {SUMMARY_COLS}
            FROM tokens WHERE model != '' GROUP BY model, tool
        ) ORDER BY (model = '*' AND tool = '*') DESC, (model = '*') DESC, _sort DESC
    """,
    "session": f"""
        SELECT * EXCLUDE (_sort) FROM (
            SELECT '*' as tool, '*' as project, '*' as session, {SUMMARY_COLS},
                   min(date) as date
            FROM tokens
            UNION ALL
            SELECT tool, '*' as project, '*' as session, {SUMMARY_COLS},
                   min(date) as date
            FROM tokens GROUP BY tool
            UNION ALL
            SELECT tool, project, session, {SUMMARY_COLS},
                   min(date) as date
            FROM tokens GROUP BY tool, project, session
        ) ORDER BY (tool = '*' AND project = '*') DESC, (project = '*') DESC, _sort DESC
        LIMIT {{limit}}
    """,
}


def fmt(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


def sources_mtime() -> float:
    """Newest mtime across all source JSONL files."""
    newest = 0.0
    all_dirs = list(SOURCE_DIRS)
    for wsl_home in _WSL_HOMES:
        all_dirs.extend([
            Path(wsl_home) / ".claude" / "projects",
            Path(wsl_home) / ".codex" / "sessions",
            Path(wsl_home) / ".gemini" / "tmp",
        ])
    for d in all_dirs:
        try:
            if not d.exists():
                continue
            for f in d.rglob("*.jsonl"):
                mt = f.stat().st_mtime
                if mt > newest:
                    newest = mt
        except OSError:
            continue
    return newest


def register_macros(db: duckdb.DuckDBPyConnection) -> None:
    """Register formatting macros."""
    db.execute("""
        CREATE MACRO fmt(n) AS
        lpad(CASE
            WHEN n >= 1e9  THEN printf('%.1fB', n / 1e9)
            WHEN n >= 1e6  THEN printf('%.1fM', n / 1e6)
            WHEN n >= 1e3  THEN printf('%.1fk', n / 1e3)
            ELSE cast(n AS VARCHAR)
        END, 7, ' ')
    """)
    db.execute("""
        CREATE MACRO fmtcost(n) AS
        CASE
            WHEN n < 10   THEN printf('$%.2f', n)
            WHEN n < 100  THEN printf('$%.1f', n)
            ELSE printf('$%.0f', n)
        END
    """)


def load_tokens(db: duckdb.DuckDBPyConnection, refresh: bool) -> None:
    """Load tokens table from cache or rebuild from source files."""
    if not refresh and CACHE_FILE.exists():
        cache_mt = CACHE_FILE.stat().st_mtime
        if sources_mtime() <= cache_mt:
            db.execute(f"CREATE TABLE tokens AS FROM '{CACHE_FILE.as_posix()}'")
            print("  (cached)", file=sys.stderr)
            return

    # Rebuild from source, skipping tools with no data
    parts = []
    all_sources = list(SOURCES.values())
    # On Windows, also try WSL home directories
    for wsl_home in _WSL_HOMES:
        for name, sql in SOURCES.values():
            all_sources.append((name, sql.replace(HOME, wsl_home)))
    for name, sql in all_sources:
        try:
            db.sql(f"SELECT 1 FROM ({sql}) LIMIT 0")
            parts.append(sql)
        except duckdb.IOException:
            continue

    if parts:
        db.execute("CREATE TABLE tokens AS " + " UNION ALL ".join(f"({p})" for p in parts))
    else:
        db.execute("CREATE TABLE tokens (tool VARCHAR, project VARCHAR, session VARCHAR, date VARCHAR, model VARCHAR, input_tokens INT, output_tokens INT, cache_write_tokens INT, cache_read_tokens INT, total_tokens BIGINT)")

    row_count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
    if row_count > 0:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        db.execute(f"COPY tokens TO '{CACHE_FILE.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"  cached {row_count} rows → {CACHE_FILE}", file=sys.stderr)


def _get_version() -> str:
    try:
        from importlib.metadata import version
        return version("clanker-analytics")
    except Exception:
        return "dev"


def main():
    parser = argparse.ArgumentParser(description="AI coding tool token analytics")
    parser.add_argument("--version", action="version", version=f"%(prog)s {_get_version()}")
    parser.add_argument("--by", choices=list(QUERIES), default="project",
                        help="Group results by (default: project)")
    parser.add_argument("--tool", choices=[*SOURCES, "all"], default="all",
                        help="Which tool to analyze (default: all)")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max rows to display (default: 50)")
    parser.add_argument("--sql", type=str,
                        help="Run custom SQL against the 'tokens' table")
    parser.add_argument("--since", type=str,
                        help="Only include data since date (e.g. 24h, 7d, 2026-03-01)")
    parser.add_argument("--chart", action="store_true", default=True,
                        help="Generate PNG chart (default)")
    parser.add_argument("--table", action="store_true",
                        help="Show table instead of chart")
    parser.add_argument("--share", action="store_true",
                        help="Generate PNG chart, copy to clipboard, and open X")
    cost_group = parser.add_mutually_exclusive_group()
    cost_group.add_argument("--monthly", action="store_true",
                            help="Show full monthly subscription cost")
    cost_group.add_argument("--prorated", action="store_true",
                            help="Show pro-rated subscription cost for the period")
    parser.add_argument("--refresh", action="store_true",
                        help="Force rebuild of cache from source files")
    args = parser.parse_args()

    db = duckdb.connect()
    register_macros(db)
    load_tokens(db, args.refresh)

    row_count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
    if row_count == 0:
        print("No data found.")
        return 1

    # Apply tool filter by narrowing the table
    if args.tool != "all":
        tool_name = SOURCES[args.tool][0]
        db.execute(f"CREATE OR REPLACE VIEW tokens_all AS SELECT * FROM tokens")
        db.execute(f"DELETE FROM tokens WHERE tool != '{tool_name}'")

    # Apply --since filter
    if args.since:
        m = re.fullmatch(r'(\d+)([hdw])', args.since)
        if m:
            n, unit = int(m.group(1)), m.group(2)
            interval = {'h': 'HOUR', 'd': 'DAY', 'w': 'WEEK'}[unit]
            db.execute(f"DELETE FROM tokens WHERE date < (current_date - INTERVAL {n} {interval})::DATE::VARCHAR")
        else:
            db.execute(f"DELETE FROM tokens WHERE date < '{args.since}'")

    if args.sql:
        db.sql(args.sql).show(max_rows=100)
        return 0

    if args.table:
        db.sql(QUERIES[args.by].format(limit=args.limit)).show(max_rows=100)
        return 0

    # Default: chart mode
    from clanker_analytics.share import generate, copy_and_open
    since = args.since or "7d"
    if not args.since:
        # Apply default --since 7d
        db.execute("DELETE FROM tokens WHERE date < (current_date - INTERVAL 7 DAY)::DATE::VARCHAR")
    plans = detect_plans()
    cost_mode = "monthly" if args.monthly else ("prorated" if args.prorated else "auto")
    path = generate(db, since, plans, cost_mode)
    if not path:
        pass
    elif args.share:
        total_cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        sub_cost = sum(c for _, c in plans.values())
        copy_and_open(path, total_cost or 0, since, sub_cost, cost_mode)
    else:
        print(f"  Card saved to {path}")


if __name__ == "__main__":
    main()
