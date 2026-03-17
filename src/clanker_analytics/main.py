#!/usr/bin/env python3
"""AI coding tool token analytics powered by DuckDB."""

import argparse
import cProfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import io
import json
import os
import pstats
import re
import sys
import time
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

    def fetch_plan(tool: str, cmd: list[str]) -> tuple[str, tuple[str, int] | None]:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                return tool, None
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
            return tool, (plan, cost)
        except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError, OSError):
            return tool, None

    with ThreadPoolExecutor(max_workers=len(cmds)) as executor:
        futures = [executor.submit(fetch_plan, tool, cmd) for tool, cmd in cmds.items()]
        for future in as_completed(futures):
            tool, result = future.result()
            if result is not None:
                plans[tool] = result

    return plans
CACHE_DIR = Path.home() / ".cache" / "clanker-analytics"
CACHE_FILE = CACHE_DIR / "tokens.parquet"
CACHE_META_FILE = CACHE_DIR / "tokens-meta.json"
CACHE_SCHEMA_VERSION = 2

SOURCE_TREES = [
    ("Claude Code", Path(HOME) / ".claude" / "projects", "*.jsonl"),
    ("Codex", Path(HOME) / ".codex" / "sessions", "*.jsonl"),
    ("Gemini", Path(HOME) / ".gemini" / "tmp", "chats/*.json"),
]

TOKEN_SCHEMA = """
    tool VARCHAR,
    project VARCHAR,
    session VARCHAR,
    date VARCHAR,
    model VARCHAR,
    input_tokens INT,
    output_tokens INT,
    cache_write_tokens INT,
    cache_read_tokens INT,
    total_tokens BIGINT,
    source_file VARCHAR
"""

TOKEN_INSERT_COLUMNS = """
    tool, project, session, date, model,
    input_tokens, output_tokens, cache_write_tokens, cache_read_tokens,
    total_tokens, source_file
"""

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

def _sql_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _sql_file_list(paths: list[str]) -> str:
    if len(paths) == 1:
        return _sql_literal(paths[0])
    return "[" + ", ".join(_sql_literal(p) for p in paths) + "]"


def _claude_sql(source_expr: str) -> str:
    return f"""
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
      + coalesce(cast(message.usage.cache_read_input_tokens as INTEGER), 0) as total_tokens,
    replace(filename, '\\', '/') as source_file
FROM read_json({source_expr},
    format='newline_delimited', filename=true, union_by_name=true,
    ignore_errors=true, maximum_depth=3, maximum_object_size=67108864)
WHERE type = 'assistant'
  AND (isSidechain IS NULL OR isSidechain = false)
  AND message.model != '<synthetic>'
  AND message.usage IS NOT NULL
  AND timestamp IS NOT NULL
  AND NOT contains(replace(filename, '\\', '/'), '/subagents/')
"""


def _codex_sql(source_expr: str) -> str:
    return f"""
WITH raw AS (
    SELECT replace(filename, '\\', '/') as filename, type, timestamp, payload
    FROM read_json({source_expr},
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
    END as total_tokens,
    t.filename as source_file
FROM token_entries t
LEFT JOIN projects p ON t.filename = p.filename
WHERE (t.cum_total IS NULL OR t.cum_total != coalesce(t.prev_cum, -1))
  AND CASE
        WHEN t.last_total > 0 THEN t.last_total
        WHEN t.cum_total IS NOT NULL AND t.prev_cum IS NOT NULL THEN t.cum_total - t.prev_cum
        ELSE 0
      END > 0
"""


def _gemini_sql(source_expr: str) -> str:
    return f"""
WITH raw AS (
    SELECT replace(filename, '\\', '/') as filename,
           regexp_extract(replace(filename, '\\', '/'), 'tmp/([^/]+)/', 1) as project_raw,
           cast(sessionId as VARCHAR) as session,
           unnest(messages) as m
    FROM read_json({source_expr},
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
    coalesce(m.tokens.total::INT, 0) as total_tokens,
    filename as source_file
FROM raw
WHERE m.tokens IS NOT NULL
  AND m.tokens.total > 0
"""


SOURCES = {
    "claude": ("Claude Code", _claude_sql),
    "codex": ("Codex", _codex_sql),
    "gemini": ("Gemini", _gemini_sql),
}

COST_PER_ROW = """
    CASE
        WHEN tool = 'Codex' THEN
            (input_tokens * 1.25 + cache_write_tokens * 1.25
             + cache_read_tokens * 0.125 + output_tokens * 10.0) / 1e6
        WHEN tool = 'Gemini' THEN CASE
            WHEN model LIKE '%flash%' THEN
                (input_tokens * 0.15 + cache_read_tokens * 0.0375
                 + output_tokens * 0.60) / 1e6
            WHEN model LIKE '%2.5%pro%' THEN
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


@dataclass
class TimingSample:
    label: str
    seconds: float
    detail: str = ""


class DebugTimer:
    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self.started = time.perf_counter()
        self.samples: list[TimingSample] = []
        self.notes: list[str] = []

    @contextmanager
    def span(self, label: str, detail: str | None = None):
        started = time.perf_counter()
        try:
            yield
        finally:
            self.record(label, time.perf_counter() - started, detail)

    def record(self, label: str, seconds: float, detail: str | None = None) -> None:
        if not self.enabled:
            return
        self.samples.append(TimingSample(label, seconds, detail or ""))

    def note(self, message: str) -> None:
        if self.enabled:
            self.notes.append(message)

    def report(self) -> None:
        if not self.enabled:
            return
        total = time.perf_counter() - self.started
        print("\n[debug] timing summary", file=sys.stderr)
        for note in self.notes:
            print(f"[debug] note: {note}", file=sys.stderr)
        for sample in self.samples:
            detail = f" ({sample.detail})" if sample.detail else ""
            print(f"[debug] {sample.label:<26} {sample.seconds:7.3f}s{detail}", file=sys.stderr)
        print(f"[debug] {'total':<26} {total:7.3f}s", file=sys.stderr)


def _fmt_debug_ts(ts: float) -> str:
    if ts <= 0:
        return "n/a"
    return datetime.fromtimestamp(ts).isoformat(sep=" ", timespec="seconds")


def _print_profile(profile: cProfile.Profile, limit: int = 30) -> None:
    stream = io.StringIO()
    stats = pstats.Stats(profile, stream=stream).strip_dirs().sort_stats("cumulative")
    stats.print_stats(limit)
    print("\n[profile] top functions by cumulative time", file=sys.stderr)
    print(stream.getvalue().rstrip(), file=sys.stderr)


def fmt(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}k"
    return str(n)


@dataclass(frozen=True)
class SourceSnapshot:
    tool: str
    mtime_ns: int
    size: int

    def to_json(self) -> dict[str, int | str]:
        return {
            "tool": self.tool,
            "mtime_ns": self.mtime_ns,
            "size": self.size,
        }


def _iter_source_trees() -> list[tuple[str, Path, str]]:
    trees = list(SOURCE_TREES)
    for wsl_home in _WSL_HOMES:
        trees.extend([
            ("Claude Code", Path(wsl_home) / ".claude" / "projects", "*.jsonl"),
            ("Codex", Path(wsl_home) / ".codex" / "sessions", "*.jsonl"),
            ("Gemini", Path(wsl_home) / ".gemini" / "tmp", "chats/*.json"),
        ])
    return trees


def scan_source_files() -> tuple[dict[str, SourceSnapshot], int, float]:
    files: dict[str, SourceSnapshot] = {}
    dir_count = 0
    started = time.perf_counter()
    for tool, root, pattern in _iter_source_trees():
        try:
            if not root.exists():
                continue
            dir_count += 1
            for path in root.rglob(pattern):
                stat = path.stat()
                files[path.as_posix()] = SourceSnapshot(
                    tool=tool,
                    mtime_ns=stat.st_mtime_ns,
                    size=stat.st_size,
                )
        except OSError:
            continue
    return files, dir_count, time.perf_counter() - started


def sources_mtime() -> tuple[float, int, int, float]:
    """Newest mtime across all source files plus basic scan stats."""
    files, dir_count, elapsed = scan_source_files()
    newest = max((meta.mtime_ns / 1e9 for meta in files.values()), default=0.0)
    return newest, len(files), dir_count, elapsed


def _load_cache_meta() -> tuple[int, dict[str, SourceSnapshot]] | None:
    if not CACHE_META_FILE.exists():
        return None
    try:
        data = json.loads(CACHE_META_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return None

    try:
        version = int(data.get("version", 0))
        raw_files = data.get("files", {})
        files = {
            path: SourceSnapshot(
                tool=str(meta["tool"]),
                mtime_ns=int(meta["mtime_ns"]),
                size=int(meta["size"]),
            )
            for path, meta in raw_files.items()
        }
    except (KeyError, TypeError, ValueError):
        return None
    return version, files


def _write_cache_meta(files: dict[str, SourceSnapshot]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": CACHE_SCHEMA_VERSION,
        "files": {path: meta.to_json() for path, meta in sorted(files.items())},
    }
    CACHE_META_FILE.write_text(json.dumps(payload, separators=(",", ":")))


def _empty_tokens_table(db: duckdb.DuckDBPyConnection) -> None:
    db.execute(f"CREATE TABLE tokens ({TOKEN_SCHEMA})")


def _table_has_source_file(db: duckdb.DuckDBPyConnection) -> bool:
    cols = {row[0] for row in db.sql("DESCRIBE tokens").fetchall()}
    return "source_file" in cols


def _group_files_by_tool(files: dict[str, SourceSnapshot]) -> dict[str, list[str]]:
    grouped = {tool_name: [] for tool_name, _ in SOURCES.values()}
    for path, meta in files.items():
        grouped.setdefault(meta.tool, []).append(path)
    for paths in grouped.values():
        paths.sort()
    return grouped


def _build_source_sql(grouped_files: dict[str, list[str]]) -> list[str]:
    parts = []
    for tool_name, builder in SOURCES.values():
        paths = grouped_files.get(tool_name, [])
        if paths:
            parts.append(builder(_sql_file_list(paths)))
    return parts


def _write_cache(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                 timer: DebugTimer, action: str, detail: str = "") -> int:
    with timer.span("count token rows"):
        row_count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with timer.span("write parquet cache", CACHE_FILE.as_posix()):
        db.execute(f"COPY tokens TO '{CACHE_FILE.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    _write_cache_meta(files)
    detail_part = f"{detail}; " if detail else ""
    print(f"  {action}: {detail_part}{row_count} rows → {CACHE_FILE}", file=sys.stderr)
    return row_count


def _rebuild_tokens(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                    timer: DebugTimer) -> None:
    parts = _build_source_sql(_group_files_by_tool(files))
    if parts:
        with timer.span("build tokens table", f"{len(parts)} sources"):
            db.execute("CREATE TABLE tokens AS " + " UNION ALL ".join(f"({part})" for part in parts))
    else:
        _empty_tokens_table(db)


def _delete_source_files(db: duckdb.DuckDBPyConnection, paths: list[str],
                         timer: DebugTimer) -> None:
    if not paths:
        return
    with timer.span("drop changed rows", f"{len(paths)} files"):
        db.execute(f"DELETE FROM tokens WHERE source_file IN ({', '.join(_sql_literal(p) for p in paths)})")


def _append_source_files(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                         timer: DebugTimer) -> None:
    if not files:
        return
    parts = _build_source_sql(_group_files_by_tool(files))
    if not parts:
        return
    with timer.span("append changed files", f"{len(files)} files"):
        db.execute(
            f"INSERT INTO tokens ({TOKEN_INSERT_COLUMNS}) "
            + " UNION ALL ".join(f"SELECT * FROM ({part})" for part in parts)
        )


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


def load_tokens(db: duckdb.DuckDBPyConnection, refresh: bool,
                timing: DebugTimer | None = None) -> None:
    """Load tokens table from cache or rebuild from source files."""
    timer = timing or DebugTimer(False)
    files, dir_count, scan_seconds = scan_source_files()
    timer.record("scan source mtimes", scan_seconds,
                 f"{len(files)} files in {dir_count} dirs")

    if refresh:
        timer.note("cache refresh forced by --refresh")
        _rebuild_tokens(db, files, timer)
        _write_cache(db, files, timer, "rebuilt cache", "--refresh")
        return

    meta = _load_cache_meta()
    cache_mt = CACHE_FILE.stat().st_mtime if CACHE_FILE.exists() else 0.0
    newest_source = max((meta.mtime_ns / 1e9 for meta in files.values()), default=0.0)

    if CACHE_FILE.exists() and meta and meta[0] == CACHE_SCHEMA_VERSION:
        _, cached_files = meta
        changed = sorted(
            path for path, snapshot in files.items()
            if cached_files.get(path) != snapshot
        )
        deleted = sorted(path for path in cached_files if path not in files)
        if not changed and not deleted:
            with timer.span("load parquet cache", CACHE_FILE.as_posix()):
                db.execute(f"CREATE TABLE tokens AS FROM '{CACHE_FILE.as_posix()}'")
            print("  (cached)", file=sys.stderr)
            timer.note(
                f"cache hit: {len(files)} source files unchanged since cache {_fmt_debug_ts(cache_mt)}"
            )
            return

        with timer.span("load parquet cache", CACHE_FILE.as_posix()):
            db.execute(f"CREATE TABLE tokens AS FROM '{CACHE_FILE.as_posix()}'")
        if not _table_has_source_file(db):
            timer.note("cache schema missing source_file; rebuilding")
            db.execute("DROP TABLE tokens")
            _rebuild_tokens(db, files, timer)
            _write_cache(db, files, timer, "rebuilt cache", "schema migration")
            return

        timer.note(
            f"incremental update: {len(changed)} changed, {len(deleted)} deleted; newest source {_fmt_debug_ts(newest_source)}"
        )
        _delete_source_files(db, changed + deleted, timer)
        _append_source_files(db, {path: files[path] for path in changed}, timer)
        _write_cache(
            db,
            files,
            timer,
            "updated cache",
            f"{len(changed)} changed, {len(deleted)} deleted",
        )
        return

    if CACHE_FILE.exists():
        if meta is None:
            timer.note("cache metadata missing or invalid; rebuilding")
        else:
            timer.note(f"cache metadata version {meta[0]} != {CACHE_SCHEMA_VERSION}; rebuilding")
        timer.note(
            f"cache stale: newest source {_fmt_debug_ts(newest_source)} > cache {_fmt_debug_ts(cache_mt)}"
        )
    else:
        timer.note(f"cache missing: {CACHE_FILE}")

    _rebuild_tokens(db, files, timer)
    _write_cache(db, files, timer, "rebuilt cache")


def _get_version() -> str:
    try:
        from importlib.metadata import version
        return version("clanker-analytics")
    except Exception:
        return "dev"


def _run(args: argparse.Namespace, timing: DebugTimer | None = None) -> int:
    timer = timing or DebugTimer(False)
    db = duckdb.connect()
    with timer.span("register macros"):
        register_macros(db)
    with timer.span("load tokens"):
        load_tokens(db, args.refresh, timer)

    with timer.span("count rows"):
        row_count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
    if row_count == 0:
        print("No data found.")
        return 1

    # Apply tool filter by narrowing the table
    if args.tool != "all":
        tool_name = SOURCES[args.tool][0]
        with timer.span("filter tool", tool_name):
            db.execute(f"CREATE OR REPLACE VIEW tokens_all AS SELECT * FROM tokens")
            db.execute(f"DELETE FROM tokens WHERE tool != '{tool_name}'")

    # Apply --since filter
    if args.since:
        with timer.span("filter since", args.since):
            m = re.fullmatch(r'(\d+)([hdw])', args.since)
            if m:
                n, unit = int(m.group(1)), m.group(2)
                interval = {'h': 'HOUR', 'd': 'DAY', 'w': 'WEEK'}[unit]
                db.execute(f"DELETE FROM tokens WHERE date < (current_date - INTERVAL {n} {interval})::DATE::VARCHAR")
            else:
                db.execute(f"DELETE FROM tokens WHERE date < '{args.since}'")

    if args.sql:
        with timer.span("run custom sql"):
            db.sql(args.sql).show(max_rows=100)
        return 0

    if args.table:
        with timer.span("render table", args.by):
            db.sql(QUERIES[args.by].format(limit=args.limit)).show(max_rows=100)
        return 0

    # Default: chart mode
    with timer.span("import share module"):
        from clanker_analytics.share import generate, copy_and_open
    since = args.since or "7d"
    if not args.since:
        # Apply default --since 7d
        with timer.span("apply default since", since):
            db.execute("DELETE FROM tokens WHERE date < (current_date - INTERVAL 7 DAY)::DATE::VARCHAR")
    with timer.span("detect plans"):
        plans = detect_plans()
    cost_mode = "monthly" if args.monthly else ("prorated" if args.prorated else "auto")
    with timer.span("generate chart", since):
        path = generate(db, since, plans, cost_mode)
    if not path:
        pass
    elif args.share:
        with timer.span("share card"):
            total_cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
            sub_cost = sum(c for _, c in plans.values())
            copy_and_open(path, total_cost or 0, since, sub_cost, cost_mode)
    else:
        print(f"  Card saved to {path}")
    return 0


def main(argv: list[str] | None = None):
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
    parser.add_argument("--debug-timing", action="store_true",
                        help="Print execution timings and cache decisions to stderr")
    parser.add_argument("--profile", action="store_true",
                        help="Print a cProfile summary to stderr")
    args = parser.parse_args(argv)

    timer = DebugTimer(args.debug_timing)
    profiler = cProfile.Profile() if args.profile else None
    try:
        if profiler:
            profiler.enable()
        return _run(args, timer)
    finally:
        if profiler:
            profiler.disable()
            _print_profile(profiler)
        timer.report()


if __name__ == "__main__":
    main()
