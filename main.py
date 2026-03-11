#!/usr/bin/env python3
"""AI coding tool token analytics powered by DuckDB."""

import argparse
import json
import sys
from pathlib import Path

import duckdb


def collect_claude() -> list[dict]:
    """Parse Claude Code JSONL logs into token records."""
    records = []
    projects_dir = Path.home() / ".claude" / "projects"
    if not projects_dir.exists():
        return records

    for f in projects_dir.glob("*/*.jsonl"):
        if "subagents" in f.parts:
            continue
        dir_name = f.parent.name
        with open(f) as fh:
            for line in fh:
                if '"assistant"' not in line or '"usage"' not in line:
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("type") != "assistant" or obj.get("isSidechain"):
                        continue
                    msg = obj.get("message") or {}
                    if msg.get("model") == "<synthetic>":
                        continue
                    u = msg.get("usage") or {}
                    ts = obj.get("timestamp")
                    if not u or not ts:
                        continue
                    inp = u.get("input_tokens") or 0
                    out = u.get("output_tokens") or 0
                    cache_w = u.get("cache_creation_input_tokens") or 0
                    cache_r = u.get("cache_read_input_tokens") or 0
                    total = inp + out + cache_w + cache_r
                    if total <= 0:
                        continue
                    cwd = obj.get("cwd", "")
                    project = Path(cwd).name if cwd else dir_name
                    records.append({
                        "tool": "Claude Code",
                        "project": project,
                        "session": obj.get("sessionId", ""),
                        "date": ts[:10],
                        "model": msg.get("model", ""),
                        "input_tokens": inp,
                        "output_tokens": out,
                        "cache_write_tokens": cache_w,
                        "cache_read_tokens": cache_r,
                        "total_tokens": total,
                    })
                except (json.JSONDecodeError, KeyError):
                    continue
    return records


COLLECTORS = {
    "claude": collect_claude,
}

QUERIES = {
    "project": """
        SELECT project, tool,
               count(*)::INT as turns,
               sum(total_tokens)::BIGINT as total,
               sum(input_tokens)::BIGINT as input,
               sum(output_tokens)::BIGINT as output,
               min(date) as first_seen,
               max(date) as last_seen
        FROM tokens
        GROUP BY project, tool
        ORDER BY total DESC
        LIMIT {limit}
    """,
    "date": """
        SELECT date, tool,
               sum(total_tokens)::BIGINT as total,
               count(*)::INT as turns
        FROM tokens
        GROUP BY date, tool
        ORDER BY date DESC
        LIMIT {limit}
    """,
    "model": """
        SELECT model, tool,
               count(*)::INT as turns,
               sum(total_tokens)::BIGINT as total
        FROM tokens
        WHERE model != ''
        GROUP BY model, tool
        ORDER BY total DESC
    """,
    "session": """
        SELECT tool, project, session,
               sum(total_tokens)::BIGINT as total,
               count(*)::INT as turns,
               min(date) as date
        FROM tokens
        GROUP BY tool, project, session
        ORDER BY total DESC
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


def main():
    parser = argparse.ArgumentParser(description="AI coding tool token analytics")
    parser.add_argument("--by", choices=list(QUERIES), default="project",
                        help="Group results by (default: project)")
    parser.add_argument("--tool", choices=[*COLLECTORS, "all"], default="all",
                        help="Which tool to analyze (default: all)")
    parser.add_argument("--limit", type=int, default=30,
                        help="Max rows to display (default: 30)")
    parser.add_argument("--sql", type=str,
                        help="Run custom SQL against the 'tokens' table")
    args = parser.parse_args()

    records = []
    tools = COLLECTORS if args.tool == "all" else {args.tool: COLLECTORS[args.tool]}
    for name, fn in tools.items():
        rows = fn()
        records.extend(rows)
        print(f"  {name}: {len(rows)} entries", file=sys.stderr)

    if not records:
        print("No data found.")
        return 1

    db = duckdb.connect()
    cols = list(records[0].keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_defs = ", ".join(f"{c} VARCHAR" if isinstance(records[0][c], str) else f"{c} INTEGER"
                         for c in cols)
    db.execute(f"CREATE TABLE tokens ({col_defs})")
    db.executemany(f"INSERT INTO tokens VALUES ({placeholders})",
                   [tuple(r.values()) for r in records])

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
