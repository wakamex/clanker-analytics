#!/usr/bin/env python3
"""AI coding tool token analytics powered by DuckDB."""

import argparse
import os
import sys
from pathlib import Path

import duckdb

HOME = os.path.expanduser("~")
CACHE_DIR = Path.home() / ".cache" / "clanker-analytics"
CACHE_FILE = CACHE_DIR / "tokens.parquet"

SOURCE_DIRS = [
    Path(HOME) / ".claude" / "projects",
    Path(HOME) / ".codex" / "sessions",
]

CLAUDE_SQL = f"""
SELECT
    'Claude Code' as tool,
    coalesce(nullif(split_part(cwd, '/', -1), ''),
             regexp_extract(filename, 'projects/([^/]+)/', 1)) as project,
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
  AND NOT contains(filename, '/subagents/')
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
    coalesce(p.project, regexp_extract(t.filename, '([^/]+)[.]jsonl', 1)) as project,
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

SOURCES = {
    "claude": ("Claude Code", CLAUDE_SQL),
    "codex": ("Codex", CODEX_SQL),
}

QUERIES = {
    "project": """
        SELECT project, tool,
               count(*)::INT as turns,
               fmt(sum(total_tokens)) as total,
               fmt(sum(input_tokens)) as input,
               fmt(sum(output_tokens)) as output,
               min(date) as first_seen,
               max(date) as last_seen
        FROM tokens
        GROUP BY project, tool
        ORDER BY sum(total_tokens) DESC
        LIMIT {limit}
    """,
    "date": """
        SELECT date, tool,
               fmt(sum(total_tokens)) as total,
               count(*)::INT as turns
        FROM tokens
        GROUP BY date, tool
        ORDER BY date DESC
        LIMIT {limit}
    """,
    "model": """
        SELECT model, tool,
               count(*)::INT as turns,
               fmt(sum(total_tokens)) as total
        FROM tokens
        WHERE model != ''
        GROUP BY model, tool
        ORDER BY sum(total_tokens) DESC
    """,
    "session": """
        SELECT tool, project, session,
               fmt(sum(total_tokens)) as total,
               count(*)::INT as turns,
               min(date) as date
        FROM tokens
        GROUP BY tool, project, session
        ORDER BY sum(total_tokens) DESC
        LIMIT {limit}
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
    for d in SOURCE_DIRS:
        if not d.exists():
            continue
        for f in d.rglob("*.jsonl"):
            mt = f.stat().st_mtime
            if mt > newest:
                newest = mt
    return newest


def register_fmt(db: duckdb.DuckDBPyConnection) -> None:
    """Register a human-readable token formatter as a DuckDB macro."""
    db.execute("""
        CREATE MACRO fmt(n) AS
        CASE
            WHEN n >= 1e9  THEN printf('%.1fB', n / 1e9)
            WHEN n >= 1e6  THEN printf('%.1fM', n / 1e6)
            WHEN n >= 1e3  THEN printf('%.1fk', n / 1e3)
            ELSE cast(n AS VARCHAR)
        END
    """)


def load_tokens(db: duckdb.DuckDBPyConnection, refresh: bool) -> None:
    """Load tokens table from cache or rebuild from source files."""
    if not refresh and CACHE_FILE.exists():
        cache_mt = CACHE_FILE.stat().st_mtime
        if sources_mtime() <= cache_mt:
            db.execute(f"CREATE TABLE tokens AS FROM '{CACHE_FILE}'")
            print("  (cached)", file=sys.stderr)
            return

    # Rebuild from source
    parts = []
    for name, sql in SOURCES.values():
        parts.append(sql)

    db.execute("CREATE TABLE tokens AS " + " UNION ALL ".join(f"({p})" for p in parts))

    row_count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
    if row_count > 0:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        db.execute(f"COPY tokens TO '{CACHE_FILE}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"  cached {row_count} rows → {CACHE_FILE}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="AI coding tool token analytics")
    parser.add_argument("--by", choices=list(QUERIES), default="project",
                        help="Group results by (default: project)")
    parser.add_argument("--tool", choices=[*SOURCES, "all"], default="all",
                        help="Which tool to analyze (default: all)")
    parser.add_argument("--limit", type=int, default=30,
                        help="Max rows to display (default: 30)")
    parser.add_argument("--sql", type=str,
                        help="Run custom SQL against the 'tokens' table")
    parser.add_argument("--refresh", action="store_true",
                        help="Force rebuild of cache from source files")
    args = parser.parse_args()

    db = duckdb.connect()
    register_fmt(db)
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

    if args.sql:
        db.sql(args.sql).show()
        return 0

    db.sql(QUERIES[args.by].format(limit=args.limit)).show()

    print()
    for tool, projects, total in db.sql("""
        SELECT tool, count(DISTINCT project)::INT, sum(total_tokens)::BIGINT
        FROM tokens GROUP BY tool
    """).fetchall():
        print(f"{tool}: {fmt(total)} across {projects} projects")


if __name__ == "__main__":
    main()
