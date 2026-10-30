#!/usr/bin/env python3
"""AI coding tool token analytics powered by DuckDB."""

import argparse
import cProfile
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import io
import json
import os
import pstats
import re
import sqlite3
import stat
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

import duckdb

from clanker_analytics.pricing import (
    cache_pricing_key, configured_plans, normalize_plan, plan_choice, plan_price, price_tokens,
    pricing_basis,
    refresh_prices,
)

HOME = os.path.expanduser("~").replace("\\", "/")

def detect_plans() -> dict[str, tuple[str, float]]:
    """Detect subscription plans from clanker-usage. Returns {tool: (plan_name, monthly_cost)}."""
    import subprocess
    from clanker_analytics.quota import usage_document
    plans = {}

    cmds = {"Claude Code": "claude", "Codex": "codex", "Gemini": "gemini", ZCODE_TOOL: "zcode"}

    def fetch_plan(tool: str, provider: str) -> tuple[str, tuple[str, float] | None]:
        try:
            data = usage_document(provider, timeout=10)
        except (RuntimeError, subprocess.TimeoutExpired, OSError):
            return tool, None
        # Only a fresh reading confirms the plan; null means no plan.
        if data.get("status") not in ("live", "cached") or not data.get("plan"):
            return tool, None
        plan = normalize_plan(data["plan"])
        return tool, (plan, plan_price(tool, plan))

    with ThreadPoolExecutor(max_workers=len(cmds) + 1) as executor:
        futures = [executor.submit(fetch_plan, tool, cmd) for tool, cmd in cmds.items()]
        # Antigravity reports the same Google AI plan, which covers a Gemini CLI that signs
        # in with an API key and so reports none.
        futures.append(executor.submit(fetch_plan, "Gemini", "agy"))
        for future in as_completed(futures):
            tool, result = future.result()
            if result is not None and (tool not in plans or not plans[tool][1]):
                plans[tool] = result

    return plans
def plans_for(args: argparse.Namespace) -> dict[str, tuple[str, float]]:
    """Detected plans, overridden by the config file and then by --plan."""
    plans = detect_plans()
    try:
        plans.update(configured_plans())
    except (ValueError, OSError) as error:
        raise SystemExit(f"error: {error}") from None
    plans.update(dict(args.plan))
    return plans


CACHE_DIR = Path.home() / ".cache" / "clanker-analytics"
CACHE_FILE = CACHE_DIR / "tokens.parquet"
CACHE_META_FILE = CACHE_DIR / "tokens-meta.json"
CACHE_SCHEMA_VERSION = 31

AGY_METADATA_TOOL = "Agy metadata"
# AOP 0.3.0 and later copies every result it writes here as <run_id>.json, so a run's
# usage outlives the project directory that holds its .aop.
AOP_RESULTS = (Path(os.environ.get("XDG_STATE_HOME", Path(HOME) / ".local" / "state"))
               / "aop" / "results")
AOP_RESULT_TOOL = "AOP result"
ZCODE_TOOL = "Zcode"
ZCODE_USAGE_TABLE = "zcode_model_usage"

MUSE_TOOL = "Muse"


def _gemini_home() -> Path:
    return Path(os.environ.get("GEMINI_CLI_HOME") or HOME).expanduser()


def _claude_config_dirs() -> list[Path]:
    """Claude Code homes other than ~/.claude: CLAUDE_CONFIG_DIR, and the config
    directories added to clanker-usage with `clanker-usage claude configure --config-dir`,
    each of which holds its own account's transcripts."""
    config = Path(os.environ.get("XDG_CONFIG_HOME", Path(HOME) / ".config"))
    try:
        listed = json.loads((config / "clanker-usage" / "config.json").read_text())["claude"]["config_dirs"]
    except (OSError, ValueError, KeyError, TypeError):
        listed = []
    selected = [os.environ.get("CLAUDE_CONFIG_DIR"), *(listed if isinstance(listed, list) else [])]
    homes: dict[Path, Path] = {(Path(HOME) / ".claude").resolve(): Path(HOME) / ".claude"}
    for directory in selected:
        if isinstance(directory, str) and directory:
            homes.setdefault(Path(directory).expanduser().resolve(), Path(directory).expanduser())
    return list(homes.values())[1:]


def _home_trees() -> list[tuple[str, Path, str]]:
    """Each source's (tool, directory, glob) under this machine's home, following the
    variables each harness resolves its home with."""
    codex = Path(os.environ.get("CODEX_HOME") or Path(HOME) / ".codex").expanduser()
    data = Path(os.environ.get("XDG_DATA_HOME") or Path(HOME) / ".local" / "share").expanduser()
    return [
        ("Claude Code", Path(HOME) / ".claude" / "projects", "*.jsonl"),
        *(("Claude Code", home / "projects", "*.jsonl") for home in _claude_config_dirs()),
        ("Codex", codex / "sessions", "*.jsonl"),
        # Codex moves a conversation's log here when it is archived.
        ("Codex", codex / "archived_sessions", "*.jsonl"),
        # Gemini CLI keeps its home in GEMINI_CLI_HOME when set, and writes chats as JSON
        # Lines since April 2026, after a single JSON document each before.
        ("Gemini", _gemini_home() / ".gemini" / "tmp", "chats/*.json"),
        ("Gemini", _gemini_home() / ".gemini" / "tmp", "chats/*.jsonl"),
        ("Agy", Path(HOME) / ".gemini" / "antigravity-cli" / "brain",
         "*/.system_generated/logs/transcript_full.jsonl"),
        (AGY_METADATA_TOOL, Path(HOME) / ".gemini" / "antigravity-cli" / "cache",
         "conversation_metadata.json"),
        (AOP_RESULT_TOOL, AOP_RESULTS, ""),
        (ZCODE_TOOL, Path(HOME) / ".zcode" / "cli" / "db", "db.sqlite"),
        # A session's log and each subagent's, under sessions/YYYY/MM/DD/<session>/.
        (MUSE_TOOL, data / "muse" / "sessions", "session.jsonl"),
    ]


SOURCE_TREES = _home_trees()

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
    cache_write_1h_tokens INT,
    total_tokens BIGINT,
    execution_type VARCHAR,
    project_path VARCHAR,
    token_count_type VARCHAR,
    turn_count INTEGER,
    retained_tokens BIGINT,
    source_kind VARCHAR,
    cost_usd DOUBLE,
    source_file VARCHAR,
    timestamp VARCHAR,
    rate_mode VARCHAR,
    quota_used_pct DOUBLE,
    quota_resets_at BIGINT,
    quota_window_minutes INTEGER,
    quota_limit_id VARCHAR,
    quota_plan VARCHAR,
    message_id VARCHAR
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


def _utc_timestamp(expr: str) -> str:
    """SQL for a timestamp as UTC text ending in Z, whatever the session time zone.

    Logs record UTC, but DuckDB detects some batches as zone-less TIMESTAMP values, and
    a zone-less value would otherwise be read in the local time zone.
    """
    text = f"cast({expr} as VARCHAR)"
    return (f"strftime(timezone('UTC', try_cast(CASE WHEN regexp_matches({text}, "
            f"'(Z|[+-][0-9]{{2}}:?[0-9]{{2}})$') THEN {text} ELSE {text} || 'Z' END "
            f"AS TIMESTAMPTZ)), '%Y-%m-%dT%H:%M:%S.%gZ')")


# The transcript fields the Claude query reads. Declaring them stops DuckDB from inferring
# a schema from sampled files, which fails for some mixes of transcripts and other records.
CLAUDE_COLUMNS = """{
    'type': 'VARCHAR', 'timestamp': 'VARCHAR', 'sessionId': 'VARCHAR',
    'isSidechain': 'BOOLEAN', 'cwd': 'VARCHAR', 'requestId': 'VARCHAR',
    'message': 'STRUCT(id VARCHAR, model VARCHAR, usage STRUCT(input_tokens BIGINT, output_tokens BIGINT,
        cache_creation_input_tokens BIGINT, cache_read_input_tokens BIGINT,
        cache_creation STRUCT(ephemeral_1h_input_tokens BIGINT)))'
}"""


def _claude_sql(source_expr: str) -> str:
    return f"""
SELECT
    'Claude Code' as tool,
    lower(coalesce(nullif(split_part(replace(cwd, '\\', '/'), '/', -1), ''),
             regexp_extract(replace(filename, '\\', '/'), 'projects/([^/]+)/', 1))) as project,
    cast(sessionId as VARCHAR) as session,
    left(cast(timestamp as VARCHAR), 10) as date,
    cast(message.model as VARCHAR) as model,
    coalesce(cast(message.usage.input_tokens as INTEGER), 0) as input_tokens,
    coalesce(cast(message.usage.output_tokens as INTEGER), 0) as output_tokens,
    coalesce(cast(message.usage.cache_creation_input_tokens as INTEGER), 0) as cache_write_tokens,
    coalesce(cast(message.usage.cache_read_input_tokens as INTEGER), 0) as cache_read_tokens,
    -- The part of cache_write_tokens written to the one-hour cache, which main threads use;
    -- absent from transcripts older than the split.
    coalesce(cast(message.usage.cache_creation.ephemeral_1h_input_tokens as INTEGER), 0)
        as cache_write_1h_tokens,
    coalesce(cast(message.usage.input_tokens as INTEGER), 0)
      + coalesce(cast(message.usage.output_tokens as INTEGER), 0)
      + coalesce(cast(message.usage.cache_creation_input_tokens as INTEGER), 0)
      + coalesce(cast(message.usage.cache_read_input_tokens as INTEGER), 0) as total_tokens,
    CASE
        WHEN coalesce(isSidechain, false)
          OR contains(replace(filename, '\\', '/'), '/subagents/')
          OR contains(replace(cwd, '\\', '/'), '/.aop/worktrees/')
        THEN 'subagent'
        ELSE 'interactive'
    END as execution_type,
    replace(cwd, '\\', '/') as project_path,
    'exact' as token_count_type,
    1 as turn_count,
    NULL::BIGINT as retained_tokens,
    'native' as source_kind,
    NULL::DOUBLE as cost_usd,
    replace(filename, '\\', '/') as source_file,
    {_utc_timestamp("timestamp")} as timestamp,
    '' as rate_mode,
    NULL::DOUBLE as quota_used_pct,
    NULL::BIGINT as quota_resets_at,
    NULL::INTEGER as quota_window_minutes,
    NULL::VARCHAR as quota_limit_id,
    -- One API response; Claude Code writes a line for each of its content blocks.
    coalesce(cast(message.id as VARCHAR), cast(requestId as VARCHAR)) as message_id
FROM read_json({source_expr},
    format='newline_delimited', filename=true, columns={CLAUDE_COLUMNS},
    ignore_errors=true, maximum_object_size=67108864)
WHERE type = 'assistant'
  AND message.model != '<synthetic>'
  AND message.usage IS NOT NULL
  AND timestamp IS NOT NULL
  AND NOT contains(replace(filename, '\\', '/'), '/subagents/agent-acompact-')
"""


def _codex_sql(source_expr: str) -> str:
    return f"""
WITH raw AS (
    SELECT
        replace(filename, '\\', '/') as filename,
        json_extract_string(json, '$.type') as type,
        json_extract_string(json, '$.timestamp') as timestamp,
        json_extract(json, '$.payload') as payload_json
    FROM read_json_objects({source_expr},
        format='newline_delimited', filename=true, ignore_errors=true,
        maximum_object_size=67108864)
    WHERE json_extract_string(json, '$.type')
          IN ('session_meta', 'event_msg', 'turn_context')
),
projects AS (
    SELECT
        filename,
        try_cast(timestamp AS TIMESTAMPTZ) as session_started_at,
        json_extract_string(payload_json, '$.forked_from_id') IS NOT NULL
          OR json_extract_string(payload_json, '$.parent_thread_id') IS NOT NULL as is_fork,
        json_extract_string(payload_json, '$.cwd') as project_path,
        split_part(json_extract_string(payload_json, '$.cwd'), '/', -1) as project,
        CASE
            WHEN contains(json_extract_string(payload_json, '$.cwd'), '/.aop/worktrees/')
              OR json_extract(payload_json, '$.source.subagent') IS NOT NULL
              OR json_extract_string(payload_json, '$.thread_source') = 'subagent'
              OR json_extract_string(payload_json, '$.parent_thread_id') IS NOT NULL
              OR json_extract_string(payload_json, '$.agent_path') IS NOT NULL
            THEN 'subagent'
            WHEN json_extract_string(payload_json, '$.originator') = 'codex_exec'
              OR json_extract_string(payload_json, '$.source') = 'exec'
            THEN 'exec'
            WHEN json_extract_string(payload_json, '$.thread_source') = 'user'
              OR json_extract_string(payload_json, '$.originator') IN ('codex-tui', 'codex_cli_rs')
              OR json_extract_string(payload_json, '$.source') = 'cli'
            THEN 'interactive'
            ELSE 'unknown'
        END as execution_type
    FROM raw WHERE type = 'session_meta'
    QUALIFY row_number() OVER (
        PARTITION BY filename
        ORDER BY contains(filename, coalesce(
                     json_extract_string(payload_json, '$.id'), '')) DESC,
                 timestamp ASC
    ) = 1
),
timeline AS (
    SELECT
        *,
        CASE
            WHEN type = 'turn_context'
            THEN json_extract_string(payload_json, '$.model')
            WHEN type = 'event_msg'
              AND json_extract_string(payload_json, '$.type') = 'thread_settings_applied'
            THEN json_extract_string(payload_json, '$.thread_settings.model')
        END as model_event,
        CASE
            WHEN type = 'event_msg'
              AND json_extract_string(payload_json, '$.type') = 'thread_settings_applied'
            THEN json_extract_string(payload_json, '$.thread_settings.service_tier')
        END as rate_mode_event
    FROM raw
),
enriched AS (
    SELECT
        *,
        -- Before a session's first model event, such as a fork replaying its parent's
        -- history, take the first model the session names.
        coalesce(
            last_value(model_event IGNORE NULLS) OVER (
                PARTITION BY filename ORDER BY timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            first_value(model_event IGNORE NULLS) OVER (
                PARTITION BY filename ORDER BY timestamp
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            )
        ) as model,
        last_value(rate_mode_event IGNORE NULLS) OVER (
            PARTITION BY filename ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as rate_mode
    FROM timeline
),
replay_seconds AS (
    SELECT
        r.filename,
        date_trunc('second', try_cast(r.timestamp AS TIMESTAMPTZ)) as replay_second,
        count(*) as event_count,
        max(try_cast(r.timestamp AS TIMESTAMPTZ)) as cutoff
    FROM enriched r
    JOIN projects p ON r.filename = p.filename
    WHERE p.is_fork
      AND r.type = 'event_msg'
      AND json_extract_string(r.payload_json, '$.type') = 'token_count'
      AND try_cast(r.timestamp AS TIMESTAMPTZ)
          BETWEEN p.session_started_at AND p.session_started_at + INTERVAL '5 seconds'
    GROUP BY r.filename, replay_second
    HAVING count(*) >= 10
),
replay_cutoffs AS (
    SELECT filename, max(cutoff) as cutoff
    FROM replay_seconds
    GROUP BY filename
),
token_entries AS (
    SELECT
        r.filename, r.timestamp, r.model, r.rate_mode,
        try_cast(json_extract_string(
            r.payload_json, '$.info.total_token_usage.total_tokens') as BIGINT) as cum_total,
        try_cast(json_extract_string(
            r.payload_json, '$.info.last_token_usage.total_tokens') as BIGINT) as last_total,
        greatest(
            coalesce(try_cast(json_extract_string(
                r.payload_json, '$.info.last_token_usage.input_tokens') as INTEGER), 0)
            - coalesce(try_cast(json_extract_string(
                r.payload_json, '$.info.last_token_usage.cached_input_tokens') as INTEGER), 0)
            - coalesce(try_cast(json_extract_string(
                r.payload_json, '$.info.last_token_usage.cache_write_input_tokens') as INTEGER), 0),
            0
        ) as input_tokens,
        coalesce(try_cast(json_extract_string(
            r.payload_json, '$.info.last_token_usage.output_tokens') as INTEGER), 0)
            as output_tokens,
        coalesce(try_cast(json_extract_string(
            r.payload_json, '$.info.last_token_usage.cache_write_input_tokens') as INTEGER), 0)
            as cache_write_tokens,
        coalesce(try_cast(json_extract_string(
            r.payload_json, '$.info.last_token_usage.cached_input_tokens') as INTEGER), 0)
            as cache_read_tokens,
        CASE
            WHEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.primary.window_minutes') as INTEGER) = 10080
            THEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.primary.used_percent') as DOUBLE)
            WHEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.secondary.window_minutes') as INTEGER) = 10080
            THEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.secondary.used_percent') as DOUBLE)
        END as quota_used_pct,
        CASE
            WHEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.primary.window_minutes') as INTEGER) = 10080
            THEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.primary.resets_at') as BIGINT)
            WHEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.secondary.window_minutes') as INTEGER) = 10080
            THEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.secondary.resets_at') as BIGINT)
        END as quota_resets_at,
        CASE
            WHEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.primary.window_minutes') as INTEGER) = 10080
              OR try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.secondary.window_minutes') as INTEGER) = 10080
            THEN 10080
        END as quota_window_minutes,
        CASE
            WHEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.primary.window_minutes') as INTEGER) = 10080
              OR try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.secondary.window_minutes') as INTEGER) = 10080
            THEN json_extract_string(r.payload_json, '$.rate_limits.limit_id')
        END as quota_limit_id,
        CASE
            WHEN try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.primary.window_minutes') as INTEGER) = 10080
              OR try_cast(json_extract_string(
                r.payload_json, '$.rate_limits.secondary.window_minutes') as INTEGER) = 10080
            THEN json_extract_string(r.payload_json, '$.rate_limits.plan_type')
        END as quota_plan,
        LAG(try_cast(json_extract_string(
            r.payload_json, '$.info.total_token_usage.total_tokens') as BIGINT))
            OVER (PARTITION BY r.filename ORDER BY r.timestamp) as prev_cum
    FROM enriched r
    LEFT JOIN replay_cutoffs replay ON r.filename = replay.filename
    WHERE r.type = 'event_msg'
      AND json_extract_string(r.payload_json, '$.type') = 'token_count'
      AND json_extract(r.payload_json, '$.info') IS NOT NULL
      AND r.timestamp IS NOT NULL
      AND (replay.cutoff IS NULL
           OR try_cast(r.timestamp AS TIMESTAMPTZ) > replay.cutoff)
)
SELECT
    'Codex' as tool,
    lower(coalesce(p.project, regexp_extract(t.filename, '([^/]+)[.]jsonl', 1))) as project,
    -- The thread id ending the rollout file name, which AOP also records for the run.
    coalesce(nullif(regexp_extract(t.filename, '([0-9a-f]{{8}}(-[0-9a-f]{{4}}){{3}}-[0-9a-f]{{12}})[.]jsonl$', 1), ''),
             regexp_extract(t.filename, '([^/]+)[.]jsonl', 1)) as session,
    left(cast(t.timestamp as VARCHAR), 10) as date,
    coalesce(t.model, '') as model,
    t.input_tokens, t.output_tokens, t.cache_write_tokens, t.cache_read_tokens,
    CASE
        WHEN t.last_total > 0 THEN t.last_total
        WHEN t.cum_total IS NOT NULL AND t.prev_cum IS NOT NULL THEN t.cum_total - t.prev_cum
        ELSE 0
    END as total_tokens,
    coalesce(p.execution_type, 'unknown') as execution_type,
    coalesce(p.project_path, p.project,
             regexp_extract(t.filename, '([^/]+)[.]jsonl', 1)) as project_path,
    'exact' as token_count_type,
    1 as turn_count,
    NULL::BIGINT as retained_tokens,
    'native' as source_kind,
    NULL::DOUBLE as cost_usd,
    t.filename as source_file,
    {_utc_timestamp("t.timestamp")} as timestamp,
    coalesce(t.rate_mode, 'default') as rate_mode,
    t.quota_used_pct,
    t.quota_resets_at,
    t.quota_window_minutes,
    t.quota_limit_id,
    t.quota_plan
FROM token_entries t
LEFT JOIN projects p ON t.filename = p.filename
WHERE (t.cum_total IS NULL OR t.cum_total != coalesce(t.prev_cum, -1))
  AND CASE
        WHEN t.last_total > 0 THEN t.last_total
        WHEN t.cum_total IS NOT NULL AND t.prev_cum IS NOT NULL THEN t.cum_total - t.prev_cum
        ELSE 0
      END > 0
"""


# Gemini CLI names a project's directory under tmp/ by the name projects.json gives the
# project path, or in older homes by the path's SHA-256, shortened here to 8 characters.
GEMINI_PROJECT = """CASE WHEN length(project_raw) = 64 AND regexp_matches(project_raw, '^[0-9a-f]+$')
         THEN left(cast(project_raw as VARCHAR), 8) ELSE lower(cast(project_raw as VARCHAR)) END"""


def _gemini_jsonl_sql(source_expr: str) -> str:
    """Gemini CLI chats as JSON Lines: a session line, then one line per message. A message
    Gemini CLI rewrites is appended again with the same id, so its last line counts;
    lines that only $set session fields carry no usage."""
    return f"""
WITH lines AS (
    SELECT replace(filename, '\\', '/') as filename, json, row_number() OVER () as line
    FROM read_json_objects({source_expr}, format='newline_delimited', filename=true,
                           ignore_errors=true, maximum_object_size=67108864)
),
sessions AS (
    SELECT filename, arg_min(json_extract_string(json, '$.sessionId'), line) as session
    FROM lines WHERE json_extract_string(json, '$.sessionId') IS NOT NULL
    GROUP BY filename
),
messages AS (
    SELECT filename, json as m FROM lines
    WHERE json_extract_string(json, '$.type') = 'gemini'
      AND json_extract(json, '$.tokens') IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY filename, json_extract_string(json, '$.id') ORDER BY line DESC) = 1
),
raw AS (
    SELECT messages.filename,
           regexp_extract(messages.filename, '.*/tmp/([^/]+)/chats/', 1) as project_raw,
           session, m,
           json_extract_string(m, '$.timestamp') as ts,
           coalesce(try_cast(json_extract_string(m, '$.tokens.input') as INT), 0) as input,
           coalesce(try_cast(json_extract_string(m, '$.tokens.output') as INT), 0) as output,
           coalesce(try_cast(json_extract_string(m, '$.tokens.cached') as INT), 0) as cached,
           coalesce(try_cast(json_extract_string(m, '$.tokens.total') as INT), 0) as total
    FROM messages LEFT JOIN sessions USING (filename)
)
SELECT
    'Gemini' as tool,
    {GEMINI_PROJECT} as project,
    session,
    left(ts, 10) as date,
    json_extract_string(m, '$.model') as model,
    input as input_tokens,
    output as output_tokens,
    0 as cache_write_tokens,
    cached as cache_read_tokens,
    total as total_tokens,
    'interactive' as execution_type,
    {GEMINI_PROJECT} as project_path,
    'exact' as token_count_type,
    1 as turn_count,
    NULL::BIGINT as retained_tokens,
    'native' as source_kind,
    NULL::DOUBLE as cost_usd,
    filename as source_file,
    {_utc_timestamp("ts")} as timestamp,
    '' as rate_mode,
    NULL::DOUBLE as quota_used_pct,
    NULL::BIGINT as quota_resets_at,
    NULL::INTEGER as quota_window_minutes,
    NULL::VARCHAR as quota_limit_id
FROM raw
WHERE total > 0
"""


def _gemini_sql(source_expr: str) -> str:
    return f"""
WITH raw AS (
    SELECT replace(filename, '\\', '/') as filename,
           regexp_extract(replace(filename, '\\', '/'), '.*/tmp/([^/]+)/chats/', 1) as project_raw,
           cast(sessionId as VARCHAR) as session,
           unnest(messages) as m
    FROM read_json({source_expr},
        format='auto', filename=true, union_by_name=true,
        ignore_errors=true, maximum_depth=5, maximum_object_size=67108864)
)
SELECT
    'Gemini' as tool,
    {GEMINI_PROJECT} as project,
    session,
    left(cast(m.timestamp as VARCHAR), 10) as date,
    cast(m.model as VARCHAR) as model,
    coalesce(m.tokens.input::INT, 0) as input_tokens,
    coalesce(m.tokens.output::INT, 0) as output_tokens,
    0 as cache_write_tokens,
    coalesce(m.tokens.cached::INT, 0) as cache_read_tokens,
    coalesce(m.tokens.total::INT, 0) as total_tokens,
    'interactive' as execution_type,
    {GEMINI_PROJECT} as project_path,
    'exact' as token_count_type,
    1 as turn_count,
    NULL::BIGINT as retained_tokens,
    'native' as source_kind,
    NULL::DOUBLE as cost_usd,
    filename as source_file,
    {_utc_timestamp("m.timestamp")} as timestamp,
    '' as rate_mode,
    NULL::DOUBLE as quota_used_pct,
    NULL::BIGINT as quota_resets_at,
    NULL::INTEGER as quota_window_minutes,
    NULL::VARCHAR as quota_limit_id
FROM raw
WHERE m.tokens IS NOT NULL
  AND m.tokens.total > 0
"""


def _agy_metadata_paths() -> list[str]:
    homes = [HOME, *_WSL_HOMES]
    paths = [
        Path(home) / ".gemini" / "antigravity-cli" / "cache" / "conversation_metadata.json"
        for home in homes
    ]
    return [path.as_posix() for path in paths if path.exists()]


AGY_MODELS_TABLE = "agy_conversation_models"
# In each generation's metadata protobuf, field 19 (tag 0x9a 0x01) is the serving model
# name, a length-prefixed string directly after the request's 16-character hex hash;
# matching the hash too keeps the same tag inside message content from matching. Most
# records end with field 21 (tag 0xaa 0x01), the display name of the model the user
# selected, such as "Gemini 3.5 Flash (High)". Serving names can be aliases:
# gemini-default and gemini-3-flash-a both ran Gemini 3.5 Flash (High).
AGY_SERVED_FIELD = re.compile(rb"[0-9a-f]{16}\x9a\x01([\x01-\x7f])")
AGY_SELECTED_FIELD = re.compile(rb"\xaa\x01([\x01-\x7f])([\x20-\x7e]+)\Z")
# Antigravity also notes each model choice in the conversation, which covers sessions
# whose records lack the display name field.
AGY_SELECTION_SETTING = re.compile(
    rb"changed setting `Model Selection` from [^`\n]*? to "
    rb"((?:Gemini|Claude|GPT)[\w .-]*?(?: \(\w+\))?)\. "
)


AGY_USAGE_TABLE = "agy_generation_usage"


def _protobuf_fields(data: bytes) -> list[tuple[int, int | bytes]]:
    """The top-level (field, value) pairs of a protobuf message: varints as ints and
    length-delimited fields as bytes. Fixed-width fields are skipped. A malformed
    message gives the fields read before the error."""
    fields: list[tuple[int, int | bytes]] = []
    position = 0

    def varint() -> int:
        nonlocal position
        value = shift = 0
        while True:
            byte = data[position]
            position += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if byte < 0x80:
                return value

    try:
        while position < len(data):
            key = varint()
            field, wire = key >> 3, key & 7
            if wire == 0:
                fields.append((field, varint()))
            elif wire == 2:
                length = varint()
                fields.append((field, data[position:position + length]))
                position += length
            elif wire in (1, 5):
                position += 8 if wire == 1 else 4
            else:
                break
    except IndexError:
        pass
    return fields


def _agy_generation_usage(data: bytes) -> tuple[int, int, int, int] | None:
    """A model call's step index, uncached input, cache reads and output from its
    generation metadata, or None when it recorded no usage.

    Field 1 holds the call: its field 4 is the usage Antigravity also prints for the step,
    with uncached input in field 2, cache reads in field 5 and output including thinking
    in field 3, and its field 20 entries are key and value pairs naming last_step_index.
    """
    call = next((value for field, value in _protobuf_fields(data)
                 if field == 1 and isinstance(value, bytes)), None)
    if call is None:
        return None
    usage = step = None
    for field, value in _protobuf_fields(call):
        if field == 4 and isinstance(value, bytes):
            usage = {f: v for f, v in _protobuf_fields(value) if isinstance(v, int)}
        elif field == 20 and isinstance(value, bytes):
            entry = dict(_protobuf_fields(value))
            if entry.get(1) == b"last_step_index" and isinstance(entry.get(2), bytes):
                try:
                    step = int(entry[2])
                except ValueError:
                    pass
    if not usage or 2 not in usage:
        return None
    return step if step is not None else -1, usage.get(2, 0), usage.get(5, 0), usage.get(3, 0)


def _agy_slug(display_name: str) -> str:
    """Antigravity's slug for a display name: "Gemini 3.5 Flash (High)" is gemini-3.5-flash-high."""
    return "-".join(display_name.lower().replace("(", "").replace(")", "").split())


def _load_agy_models(db: duckdb.DuckDBPyConnection, transcript_paths: list[str]) -> None:
    """Record each session's model, and each model call's usage, from Antigravity's
    conversation database.

    Transcripts do not name the model; each model call's generation metadata in
    conversations/<session>.db does. A session's model is the selected model most of its
    calls recorded, else the last model selection noted in the conversation, else the
    serving name most of its calls used.
    """
    rows = []
    usage_rows = []
    for transcript in transcript_paths:
        path = Path(transcript)
        session = path.parents[2].name
        database = path.parents[4] / "conversations" / f"{session}.db"
        if not database.exists():
            continue
        selected: dict[str, int] = {}
        served: dict[str, int] = {}
        selection = None
        try:
            source = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)
            try:
                for (data,) in source.execute("SELECT data FROM gen_metadata ORDER BY idx"):
                    data = data or b""
                    usage = _agy_generation_usage(data)
                    if usage:
                        usage_rows.append((session, *usage))
                    display = AGY_SELECTED_FIELD.search(data)
                    if display and display.group(1)[0] == len(display.group(2)):
                        name = _agy_slug(display.group(2).decode())
                        selected[name] = selected.get(name, 0) + 1
                    serving = AGY_SERVED_FIELD.search(data)
                    if serving:
                        end = serving.end() + serving.group(1)[0]
                        name = data[serving.end():end].decode(errors="replace")
                        served[name] = served.get(name, 0) + 1
                    for choice in AGY_SELECTION_SETTING.findall(data):
                        selection = _agy_slug(choice.decode())
            finally:
                source.close()
        except sqlite3.Error as error:
            print(f"  warning: could not read Antigravity models from {database}: {error}",
                  file=sys.stderr)
        model = (max(selected, key=selected.get) if selected
                 else selection or (max(served, key=served.get) if served else None))
        if model:
            rows.append((session, model))
    db.execute(f"CREATE OR REPLACE TEMP TABLE {AGY_MODELS_TABLE} (session VARCHAR, model VARCHAR)")
    if rows:
        db.executemany(f"INSERT INTO {AGY_MODELS_TABLE} VALUES (?, ?)", rows)
    db.execute(f"""CREATE OR REPLACE TEMP TABLE {AGY_USAGE_TABLE} (session VARCHAR,
                   step_index BIGINT, input BIGINT, cache_read BIGINT, output BIGINT)""")
    if usage_rows:
        db.executemany(f"INSERT INTO {AGY_USAGE_TABLE} VALUES (?, ?, ?, ?, ?)", usage_rows)


def _agy_sql(source_expr: str, metadata_expr: str | None = None,
             models_table: str | None = None, usage_table: str | None = None) -> str:
    """Load top-level Antigravity sessions with each model call's usage.

    models_table maps sessions to models when transcripts do not name them. usage_table
    holds the exact usage of each call that the session's conversation database recorded;
    a call is timed by the transcript step it ended on. A session without recorded usage
    is estimated from its transcript, charging each response the whole transcript so far
    as uncached input.
    """
    if metadata_expr is None:
        metadata_paths = _agy_metadata_paths()
        metadata_expr = _sql_file_list(metadata_paths) if metadata_paths else None

    session_filter = "m.session IS NOT NULL" if metadata_expr else "true"
    if metadata_expr:
        metadata_cte = f"""
metadata_raw AS (
    SELECT entry.value as conversation
    FROM read_json_objects({metadata_expr}, format='auto', ignore_errors=true) source,
         json_each(source.json, '$.conversations') entry
),
metadata AS (
    SELECT
        json_extract_string(conversation, '$.summary.ID') as session,
        regexp_replace(
            json_extract_string(conversation, '$.summary.WorkspaceURIs[0]'),
            '^file://', ''
        ) as workspace_path
    FROM metadata_raw
    WHERE json_extract_string(conversation, '$.summary.ID') IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY json_extract_string(conversation, '$.summary.ID')
        ORDER BY json_extract_string(conversation, '$.summary.WorkspaceURIs[0]') IS NOT NULL DESC
    ) = 1
),
"""
    else:
        metadata_cte = """
metadata AS (
    SELECT NULL::VARCHAR as session, NULL::VARCHAR as workspace_path WHERE false
),
"""

    models_cte = (
        f"conversation_models AS (SELECT session, model FROM {models_table}),"
        if models_table else
        "conversation_models AS (SELECT NULL::VARCHAR as session, NULL::VARCHAR as model WHERE false),"
    )
    usage_cte = (
        f"generation_usage AS (SELECT * FROM {usage_table}),"
        if usage_table else
        """generation_usage AS (SELECT NULL::VARCHAR as session, NULL::BIGINT as step_index,
               NULL::BIGINT as input, NULL::BIGINT as cache_read, NULL::BIGINT as output
           WHERE false),"""
    )

    return f"""
WITH raw AS (
    SELECT replace(filename, '\\', '/') as filename, json as event_json
    FROM read_json_objects({source_expr},
        format='newline_delimited', filename=true, ignore_errors=true)
),
events AS (
    SELECT DISTINCT
        filename,
        event_json,
        try_cast(json_extract_string(event_json, '$.step_index') as BIGINT) as step_index,
        json_extract_string(event_json, '$.type') as type,
        json_extract_string(event_json, '$.status') as status,
        json_extract_string(event_json, '$.created_at') as created_at,
        coalesce(
            json_extract_string(event_json, '$.tool_calls[0].args.Cwd'),
            json_extract_string(event_json, '$.tool_calls[0].args.cwd'),
            json_extract_string(event_json, '$.tool_calls[0].args.WorkspaceRoot'),
            json_extract_string(event_json, '$.cwd')
        ) as observed_path,
        coalesce(
            json_extract_string(event_json, '$.model'),
            json_extract_string(event_json, '$.modelVersion'),
            json_extract_string(event_json, '$.usageMetadata.model')
        ) as model,
        try_cast(coalesce(
            json_extract_string(event_json, '$.usageMetadata.promptTokenCount'),
            json_extract_string(event_json, '$.usage_metadata.prompt_token_count'),
            json_extract_string(event_json, '$.usage.prompt_tokens'),
            json_extract_string(event_json, '$.usage.input_tokens'),
            json_extract_string(event_json, '$.usage.inputTokens')
        ) as BIGINT) as exact_input_raw,
        try_cast(coalesce(
            json_extract_string(event_json, '$.usageMetadata.cachedContentTokenCount'),
            json_extract_string(event_json, '$.usage_metadata.cached_content_token_count'),
            json_extract_string(event_json, '$.usage.cache_read_tokens'),
            json_extract_string(event_json, '$.usage.cached_input_tokens')
        ) as BIGINT) as exact_cache,
        try_cast(coalesce(
            json_extract_string(event_json, '$.usageMetadata.candidatesTokenCount'),
            json_extract_string(event_json, '$.usage_metadata.candidates_token_count'),
            json_extract_string(event_json, '$.usage.completion_tokens'),
            json_extract_string(event_json, '$.usage.output_tokens'),
            json_extract_string(event_json, '$.usage.outputTokens')
        ) as BIGINT) as exact_output_raw,
        try_cast(coalesce(
            json_extract_string(event_json, '$.usageMetadata.thoughtsTokenCount'),
            json_extract_string(event_json, '$.usage_metadata.thoughts_token_count')
        ) as BIGINT) as exact_thinking,
        try_cast(coalesce(
            json_extract_string(event_json, '$.usageMetadata.totalTokenCount'),
            json_extract_string(event_json, '$.usage_metadata.total_token_count'),
            json_extract_string(event_json, '$.usage.total_tokens'),
            json_extract_string(event_json, '$.usage.totalTokens')
        ) as BIGINT) as exact_total,
        length(coalesce(json_extract_string(event_json, '$.content'), ''))
        + length(coalesce(json_extract_string(event_json, '$.thinking'), ''))
        + length(coalesce(cast(json_extract(event_json, '$.tool_calls') as VARCHAR), ''))
            as retained_chars
    FROM raw
),
{metadata_cte}
{models_cte}
{usage_cte}
session_meta AS (
    SELECT
        filename,
        regexp_extract(filename, '/brain/([^/]+)/', 1) as session,
        arg_min(observed_path, coalesce(step_index, 9223372036854775807))
            FILTER (WHERE observed_path IS NOT NULL) as observed_path,
        arg_min(model, coalesce(step_index, 9223372036854775807))
            FILTER (WHERE model IS NOT NULL) as model
    FROM events
    GROUP BY filename
),
sessions AS (
    SELECT
        s.filename,
        s.session,
        coalesce(nullif(m.workspace_path, ''), s.observed_path) as project_path,
        coalesce(s.model, c.model) as model
    FROM session_meta s
    LEFT JOIN metadata m USING (session)
    LEFT JOIN conversation_models c USING (session)
    WHERE {session_filter}
),
ordered AS (
    SELECT
        e.*,
        coalesce(sum(e.retained_chars) OVER (
            PARTITION BY e.filename
            ORDER BY e.step_index NULLS LAST, e.created_at, e.event_json
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::BIGINT as context_chars
    FROM events e
    JOIN sessions s USING (filename)
    WHERE e.status = 'DONE'
      AND e.created_at IS NOT NULL
      AND e.type != 'CONVERSATION_HISTORY'
),
turns AS (
    SELECT
        filename,
        left(cast(created_at as VARCHAR), 10) as date,
        {_utc_timestamp("created_at")} as timestamp,
        exact_input_raw IS NOT NULL AND exact_output_raw IS NOT NULL as is_exact,
        CASE WHEN is_exact THEN
            greatest(exact_input_raw - coalesce(exact_cache, 0), 0)
        ELSE ceil(context_chars / 4.0) END::BIGINT as input_tokens,
        CASE WHEN is_exact THEN
            exact_output_raw + coalesce(exact_thinking, 0)
        ELSE ceil(retained_chars / 4.0) END::BIGINT as output_tokens,
        CASE WHEN is_exact THEN coalesce(exact_cache, 0) ELSE 0 END::BIGINT
            as cache_read_tokens,
        CASE WHEN is_exact THEN
            coalesce(exact_total, exact_input_raw + exact_output_raw
                     + coalesce(exact_thinking, 0))
        ELSE ceil(context_chars / 4.0) + ceil(retained_chars / 4.0)
        END::BIGINT as total_tokens
    FROM ordered
    WHERE type = 'PLANNER_RESPONSE'
      AND filename NOT IN (SELECT filename FROM sessions
                           WHERE session IN (SELECT session FROM generation_usage))
    UNION ALL
    SELECT
        s.filename,
        left(cast(step.created_at as VARCHAR), 10) as date,
        {_utc_timestamp("step.created_at")} as timestamp,
        true as is_exact,
        u.input as input_tokens,
        u.output as output_tokens,
        u.cache_read as cache_read_tokens,
        u.input + u.cache_read + u.output as total_tokens
    FROM generation_usage u
    JOIN sessions s USING (session)
    ASOF LEFT JOIN (
        SELECT filename, step_index, min(created_at) as created_at FROM events
        WHERE step_index IS NOT NULL AND created_at IS NOT NULL GROUP BY ALL
    ) step ON step.filename = s.filename AND u.step_index >= step.step_index
    WHERE u.input + u.cache_read + u.output > 0
),
retained_daily AS (
    SELECT
        e.filename,
        left(cast(e.created_at as VARCHAR), 10) as date,
        ceil(sum(e.retained_chars) / 4.0)::BIGINT as retained_tokens
    FROM events e
    JOIN sessions s USING (filename)
    WHERE e.status = 'DONE'
      AND e.created_at IS NOT NULL
      AND e.type != 'CONVERSATION_HISTORY'
    GROUP BY e.filename, left(cast(e.created_at as VARCHAR), 10)
),
turn_rows AS (
    SELECT
        t.*,
        CASE WHEN row_number() OVER (
            PARTITION BY t.filename, t.date ORDER BY t.timestamp
        ) = 1 THEN r.retained_tokens END as retained_tokens
    FROM turns t
    LEFT JOIN retained_daily r USING (filename, date)
)
SELECT
    'Agy' as tool,
    lower(coalesce(
        nullif(CASE WHEN contains(replace(m.project_path, '\\', '/'), '/.aop/worktrees/')
                    THEN split_part(split_part(replace(m.project_path, '\\', '/'), '/.aop/', 1), '/', -1)
                    ELSE split_part(replace(m.project_path, '\\', '/'), '/', -1)
               END, ''),
                   'antigravity')) as project,
    m.session,
    t.date,
    coalesce(m.model, '') as model,
    t.input_tokens::INT as input_tokens,
    t.output_tokens::INT as output_tokens,
    0 as cache_write_tokens,
    t.cache_read_tokens::INT as cache_read_tokens,
    t.total_tokens as total_tokens,
    CASE WHEN contains(replace(m.project_path, '\\', '/'), '/.aop/worktrees/')
         THEN 'subagent' ELSE 'interactive' END as execution_type,
    coalesce(replace(m.project_path, '\\', '/'), 'antigravity') as project_path,
    CASE WHEN t.is_exact THEN 'exact' ELSE 'estimated_processed' END as token_count_type,
    1 as turn_count,
    t.retained_tokens,
    'native' as source_kind,
    NULL::DOUBLE as cost_usd,
    t.filename as source_file,
    t.timestamp,
    '' as rate_mode,
    NULL::DOUBLE as quota_used_pct,
    NULL::BIGINT as quota_resets_at,
    NULL::INTEGER as quota_window_minutes,
    NULL::VARCHAR as quota_limit_id
FROM turn_rows t
JOIN sessions m USING (filename)
WHERE t.total_tokens > 0
"""


AOP_USAGE_SCHEMA = "aop-token-usage-v3"


def _aop_sql(source_expr: str) -> str:
    """Load the per-run usage Agent Orchestration Process records in v3 results."""
    return f"""
WITH raw AS (
    SELECT replace(filename, '\\', '/') as filename, json as result_json
    FROM read_json_objects({source_expr}, format='auto', filename=true,
                           ignore_errors=true)
),
stored AS (
    SELECT
        filename,
        json_extract_string(result_json, '$.usage_schema') as usage_schema,
        json_extract_string(result_json, '$.accounting_status') as accounting_status,
        json_extract(result_json, '$.usage') IS NOT NULL as has_usage,
        lower(json_extract_string(result_json, '$.provider')) as provider,
        json_extract_string(result_json, '$.inference_provider') as inference_provider,
        json_extract_string(result_json, '$.run_id') as run_id,
        json_extract_string(result_json, '$.session_id') as session_id,
        json_extract_string(result_json, '$.task') as task,
        json_extract_string(result_json, '$.model') as model,
        {_utc_timestamp("""coalesce(
            json_extract_string(result_json, '$.started_at'),
            json_extract_string(result_json, '$.finished_at'))""")} as timestamp,
        greatest(coalesce(try_cast(json_extract_string(
            result_json, '$.usage.input_tokens') as BIGINT), 0), 0) as raw_input,
        greatest(coalesce(try_cast(json_extract_string(
            result_json, '$.usage.cached_input_tokens') as BIGINT), 0), 0) as cached_input,
        greatest(coalesce(try_cast(json_extract_string(
            result_json, '$.usage.cache_write_input_tokens') as BIGINT), 0), 0)
            as cache_write_input,
        greatest(coalesce(try_cast(json_extract_string(
            result_json, '$.usage.cache_write_1h_input_tokens') as BIGINT), 0), 0)
            as cache_write_1h_input,
        greatest(coalesce(try_cast(json_extract_string(
            result_json, '$.usage.output_tokens') as BIGINT), 0), 0) as raw_output,
        greatest(coalesce(try_cast(json_extract_string(
            result_json, '$.usage.reasoning_output_tokens') as BIGINT), 0), 0)
            as reasoning_output,
        try_cast(json_extract_string(
            result_json, '$.calculated_cost.amount_usd') as DOUBLE) as cost_usd,
        -- The project is the directory that owns the run's .aop.
        regexp_extract(json_extract_string(result_json, '$.state_root'),
                       '^(.*)/[.]aop/?$', 1) as owner_path
    FROM raw
),
normalized AS (
    -- Input and output are totals; cached input, cache writes, and reasoning output are subsets.
    SELECT *, raw_input as normalized_input, raw_output as processed_output
    FROM stored
    WHERE CASE
        WHEN usage_schema != '{AOP_USAGE_SCHEMA}' OR usage_schema IS NULL
             THEN error('unsupported AOP token usage schema; run aop migrate-results')
        WHEN owner_path IS NULL OR owner_path = ''
             THEN error('AOP result without state_root; run aop migrate-results')
        WHEN accounting_status IN ('complete', 'partial') AND has_usage THEN true
        WHEN accounting_status IN ('complete', 'partial', 'unavailable') THEN false
        ELSE error('unsupported AOP accounting status')
    END
),
tokens AS (
    SELECT
        *,
        greatest(normalized_input - cached_input - cache_write_input, 0) as uncached_input,
        least(cache_write_1h_input, cache_write_input) as cache_write_1h
    FROM normalized
)
SELECT
    CASE provider
        WHEN 'agy' THEN 'Agy'
        WHEN 'claude' THEN 'Claude Code'
        WHEN 'codex' THEN 'Codex'
        WHEN 'cursor' THEN 'Cursor'
        WHEN 'devin' THEN 'Devin'
        WHEN 'dsh' THEN 'DeepSeek Harness'
        WHEN 'grok' THEN 'Grok'
        WHEN 'hermes' THEN 'Hermes'
        WHEN 'opencode' THEN 'OpenCode'
        WHEN 'zcode' THEN 'Zcode'
        WHEN 'muse' THEN 'Muse'
        ELSE coalesce(nullif(provider, ''), 'AOP')
    END as tool,
    lower(regexp_extract(owner_path, '([^/]+)$', 1)) as project,
    coalesce(nullif(session_id, ''), run_id) as session,
    left(timestamp, 10) as date,
    coalesce(model, '') as model,
    uncached_input::INT as input_tokens,
    processed_output::INT as output_tokens,
    cache_write_input::INT as cache_write_tokens,
    cached_input::INT as cache_read_tokens,
    (uncached_input + cached_input + cache_write_input + processed_output)::BIGINT
        as total_tokens,
    'subagent' as execution_type,
    owner_path || '/.aop/worktrees/' || coalesce(nullif(task, ''), run_id)
        as project_path,
    'exact' as token_count_type,
    1 as turn_count,
    NULL::BIGINT as retained_tokens,
    'aop' as source_kind,
    cost_usd,
    filename as source_file,
    timestamp,
    '' as rate_mode,
    NULL::DOUBLE as quota_used_pct,
    NULL::BIGINT as quota_resets_at,
    NULL::INTEGER as quota_window_minutes,
    NULL::VARCHAR as quota_limit_id,
    cache_write_1h::INT as cache_write_1h_tokens
FROM tokens
WHERE timestamp IS NOT NULL
  AND run_id IS NOT NULL
  AND uncached_input + cached_input + cache_write_input + processed_output > 0
"""


ZCODE_USAGE_COLUMNS = """
    source_file VARCHAR,
    session_id VARCHAR,
    parent_session_id VARCHAR,
    directory VARCHAR,
    model_id VARCHAR,
    charged_at BIGINT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    cache_write_tokens BIGINT,
    cache_read_tokens BIGINT,
    error_message VARCHAR
"""

ZCODE_USAGE_QUERY = """
    SELECT m.session_id, s.parent_id, s.directory, m.model_id,
           coalesce(m.completed_at, m.started_at),
           m.input_tokens, m.output_tokens, m.cache_creation_input_tokens,
           m.cache_read_input_tokens,
           CASE WHEN m.error_type = 'rate_limited' THEN m.error_message END
    FROM model_usage m LEFT JOIN session s ON s.id = m.session_id
    WHERE m.input_tokens + m.output_tokens > 0 OR m.error_type = 'rate_limited'
"""

# Z.ai reports quota resets in China Standard Time: the reset in each observed limit
# message is exactly one window after the first request that opened that window.
ZAI_LIMIT_MESSAGE_UTC_OFFSET_HOURS = 8


def _load_zcode_usage(db: duckdb.DuckDBPyConnection, paths: list[str]) -> None:
    """Copy per-request usage from Zcode's SQLite databases into DuckDB."""
    rows = []
    for path in paths:
        try:
            source = sqlite3.connect(f"{Path(path).as_uri()}?mode=ro", uri=True)
            try:
                rows.extend((path, *row) for row in source.execute(ZCODE_USAGE_QUERY))
            finally:
                source.close()
        except sqlite3.Error as error:
            print(f"  warning: could not read Zcode usage from {path}: {error}", file=sys.stderr)
    db.execute(f"CREATE OR REPLACE TEMP TABLE {ZCODE_USAGE_TABLE} ({ZCODE_USAGE_COLUMNS})")
    if rows:
        db.executemany(
            f"INSERT INTO {ZCODE_USAGE_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )


def _muse_parent_logs(paths: list[str]) -> list[str]:
    """The session logs of the subagent logs in paths, which name the session's workspace."""
    parents = {path.split("/subagent/")[0] + "/session.jsonl" for path in paths if "/subagent/" in path}
    return sorted(parent for parent in parents if parent not in paths and os.path.exists(parent))


# Muse runs its skill reminder in the main process, so the reminder's calls have no log
# or model_completed event of their own; the proxied requests ask for this model.
MUSE_REMINDER_MODEL = "muse-spark-1.3"


def _muse_subagents(paths: list[str]) -> list[str]:
    """The ids of the subagents whose logs sit beside the session logs in paths."""
    return sorted({child.name for path in paths if "/subagent/" not in path
                   for child in (Path(path).parent / "subagent").glob("*")
                   if (child / "session.jsonl").exists()})


def _muse_sql(source_expr: str) -> str:
    """Muse Code usage: one row per model call. A call records a model_completed event in
    its session's or subagent's log; an automated approval review records its usage once,
    as automated_review_completed in the session's log. The session's log also attributes
    every call to its owner; reminder calls of owners without a log of their own, the
    in-process skill reminder, are counted from those records. Input counts include
    cached input."""
    return f"""
WITH lines AS (
    SELECT replace(filename, '\\', '/') as filename, json,
           regexp_extract(replace(filename, '\\', '/'),
                          '/sessions/[0-9]{{4}}/[0-9]{{2}}/[0-9]{{2}}/([^/]+)/', 1) as session
    FROM read_json_objects({source_expr}, format='newline_delimited', filename=true,
                           ignore_errors=true, maximum_object_size=67108864)
),
workspaces AS (
    SELECT session, max(json_extract_string(json, '$.payload.record.workspace_root')) as workspace
    FROM lines GROUP BY session
),
calls AS (
    SELECT filename, session,
        json_extract_string(json, '$.payload.event.kind') as kind,
        -- A model call names its model; a review records it as an object with model_id.
        coalesce(json_extract_string(json, '$.payload.event.model.model_id'),
                 json_extract_string(json, '$.payload.event.model')) as model,
        try_cast(json_extract_string(json, '$.recorded_at') as BIGINT) as recorded_us,
        coalesce(try_cast(json_extract_string(json, '$.payload.event.usage.input_tokens') as BIGINT), 0) as input,
        coalesce(try_cast(json_extract_string(json, '$.payload.event.usage.cached_tokens') as BIGINT),
                 try_cast(json_extract_string(json, '$.payload.event.usage.cached_input_tokens') as BIGINT),
                 0) as cached,
        coalesce(try_cast(json_extract_string(json, '$.payload.event.usage.cache_write_tokens') as BIGINT), 0)
            as cache_write,
        coalesce(try_cast(json_extract_string(json, '$.payload.event.usage.output_tokens') as BIGINT), 0) as output
    FROM lines
    WHERE json_extract_string(json, '$.payload_type') = 'runtime.session'
      AND (json_extract_string(json, '$.payload.event.kind') = 'model_completed'
           OR (json_extract_string(json, '$.payload.event.kind') = 'automated_review_completed'
               AND filename NOT LIKE '%/subagent/%'))
      AND json_extract(json, '$.payload.event.usage') IS NOT NULL
    UNION ALL
    SELECT filename, session, 'reminder' as kind, '{MUSE_REMINDER_MODEL}' as model,
        try_cast(json_extract_string(json, '$.recorded_at') as BIGINT) as recorded_us,
        coalesce(try_cast(json_extract_string(json, '$.payload.event.record.quantity.input_tokens') as BIGINT), 0),
        coalesce(try_cast(json_extract_string(json, '$.payload.event.record.quantity.cached_tokens') as BIGINT), 0),
        0,
        coalesce(try_cast(json_extract_string(json, '$.payload.event.record.quantity.output_tokens') as BIGINT), 0)
    FROM lines
    WHERE json_extract_string(json, '$.payload.event.kind') = 'goal_usage_attribution'
      AND json_extract_string(json, '$.payload.event.record.usage_family') = 'provider'
      AND json_extract_string(json, '$.payload.event.record.owner.owner_type') = 'reminder_child'
      AND coalesce(json_extract_string(json, '$.payload.event.record.quantity.reported'), 'true') = 'true'
      AND filename NOT LIKE '%/subagent/%'
      AND json_extract_string(json, '$.payload.event.record.owner.owner_id')
          NOT IN (SELECT path FROM muse_subagents)
),
timed AS (
    SELECT calls.*, strftime(make_timestamp(recorded_us), '%Y-%m-%dT%H:%M:%S.%gZ') as ts,
           workspace
    FROM calls LEFT JOIN workspaces USING (session)
)
SELECT
    'Muse' as tool,
    lower(coalesce(regexp_extract(workspace, '([^/]+)/?$', 1), 'muse')) as project,
    session,
    left(ts, 10) as date,
    coalesce(model, '') as model,
    greatest(input - cached - cache_write, 0)::INT as input_tokens,
    output::INT as output_tokens,
    cache_write::INT as cache_write_tokens,
    cached::INT as cache_read_tokens,
    (input + output)::BIGINT as total_tokens,
    CASE WHEN filename LIKE '%/subagent/%' OR kind IN ('automated_review_completed', 'reminder')
         THEN 'subagent' ELSE 'interactive' END as execution_type,
    workspace as project_path,
    'exact' as token_count_type,
    1 as turn_count,
    NULL::BIGINT as retained_tokens,
    'native' as source_kind,
    NULL::DOUBLE as cost_usd,
    filename as source_file,
    ts as timestamp,
    '' as rate_mode,
    NULL::DOUBLE as quota_used_pct,
    NULL::BIGINT as quota_resets_at,
    NULL::INTEGER as quota_window_minutes,
    NULL::VARCHAR as quota_limit_id
FROM timed
WHERE input + output > 0
  AND filename IN (SELECT path FROM muse_logs)
"""


def _zcode_sql(source_expr: str) -> str:
    """Normalize requests that _load_zcode_usage copied from the source databases.

    A rejected request whose message says a usage limit was reached becomes a zero-token
    row carrying an exact 100% quota observation for that window.
    """
    # Z.ai starts a quota window within a second of the first request's completion,
    # so requests are timed when they finish.
    # Zcode input and total counts include cache reads and writes, like OpenAI usage.
    return f"""
WITH usage AS (
    SELECT
        *,
        regexp_extract(error_message,
            'Usage limit reached for ([0-9]+) (hour|day|week)', ['count', 'unit']) as limit_window,
        try_strptime(regexp_extract(error_message,
            'reset at ([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}})', 1),
            '%Y-%m-%d %H:%M:%S') as limit_reset_local
    FROM {ZCODE_USAGE_TABLE}
),
limits AS (
    SELECT
        *,
        try_cast(limit_window.count AS INTEGER)
          * CASE limit_window.unit WHEN 'hour' THEN 60 WHEN 'day' THEN 1440
                                   WHEN 'week' THEN 10080 END as limit_minutes,
        epoch(limit_reset_local)::BIGINT
          - {ZAI_LIMIT_MESSAGE_UTC_OFFSET_HOURS} * 3600 as limit_resets_at
    FROM usage
)
SELECT
    '{ZCODE_TOOL}' as tool,
    lower(regexp_extract(replace(directory, '\\', '/'), '([^/]+)/?$', 1)) as project,
    session_id as session,
    strftime(epoch_ms(charged_at), '%Y-%m-%d') as date,
    'zai/' || lower(model_id) as model,
    greatest(input_tokens - cache_read_tokens - cache_write_tokens, 0)::INT as input_tokens,
    output_tokens::INT as output_tokens,
    cache_write_tokens::INT as cache_write_tokens,
    cache_read_tokens::INT as cache_read_tokens,
    (greatest(input_tokens, cache_read_tokens + cache_write_tokens)
      + output_tokens)::BIGINT as total_tokens,
    CASE WHEN parent_session_id IS NOT NULL THEN 'subagent' ELSE 'interactive' END
        as execution_type,
    replace(directory, '\\', '/') as project_path,
    'exact' as token_count_type,
    CASE WHEN error_message IS NULL THEN 1 ELSE 0 END as turn_count,
    NULL::BIGINT as retained_tokens,
    'native' as source_kind,
    NULL::DOUBLE as cost_usd,
    source_file,
    strftime(epoch_ms(charged_at), '%Y-%m-%dT%H:%M:%S.%gZ') as timestamp,
    '' as rate_mode,
    CASE WHEN limit_resets_at IS NOT NULL THEN 100.0 END as quota_used_pct,
    CASE WHEN limit_minutes IS NOT NULL THEN limit_resets_at END as quota_resets_at,
    CASE WHEN limit_resets_at IS NOT NULL THEN limit_minutes END as quota_window_minutes,
    CASE WHEN limit_resets_at IS NOT NULL AND limit_minutes IS NOT NULL
         THEN limit_window.count || left(limit_window.unit, 1) END as quota_limit_id
FROM limits
WHERE error_message IS NULL
   OR (limit_resets_at IS NOT NULL AND limit_minutes IS NOT NULL)
"""


SOURCES = {
    "claude": ("Claude Code", _claude_sql),
    "codex": ("Codex", _codex_sql),
    "gemini": ("Gemini", _gemini_sql),
    "agy": ("Agy", _agy_sql),
    "aop": (AOP_RESULT_TOOL, _aop_sql),
    "zcode": (ZCODE_TOOL, _zcode_sql),
    "muse": (MUSE_TOOL, _muse_sql),
}

# API-equivalent cost of each row, priced by model-prices when the cache is built. Rows
# of models without a price count as zero; their pricing column says "unpriced".
COST_PER_ROW = "coalesce(api_cost_usd, 0)"

PRICE_BASES = {
    "historical": "rates in effect when each request ran",
    "current": "current rates applied to every request",
}

COST_EXPR = f"fmtcost(sum({COST_PER_ROW}))"

SUMMARY_COLS = f"""
    sum(turn_count)::INT as turns,
    fmt(sum(total_tokens)) as total,
    fmt(sum(total_tokens) - 0.9 * sum(cache_read_tokens)) as billable,
    fmt(sum(output_tokens)) as output,
    lpad(printf('%.0f%%', 100.0 * sum(cache_read_tokens) / greatest(sum(total_tokens) - sum(output_tokens), 1)), 4, ' ') as "cache",
    {COST_EXPR} as "api_cost",
    CASE WHEN count(*) FILTER (WHERE token_count_type != 'exact') > 0
         THEN 'processed estimate' ELSE 'exact' END as count_basis,
    CASE WHEN count(retained_tokens) > 0 THEN fmt(sum(retained_tokens)) ELSE '' END as retained_text,
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
    "execution": f"""
        SELECT * EXCLUDE (_sort) FROM (
            SELECT '*' as execution_type, '*' as tool, {SUMMARY_COLS}
            FROM tokens
            UNION ALL
            SELECT '*' as execution_type, tool, {SUMMARY_COLS}
            FROM tokens GROUP BY tool
            UNION ALL
            SELECT execution_type, tool, {SUMMARY_COLS}
            FROM tokens GROUP BY execution_type, tool
        ) ORDER BY (execution_type = '*' AND tool = '*') DESC,
                   (execution_type = '*') DESC, _sort DESC
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


# Logs that only ever grow, so a changed one is read from where the last read stopped.
APPEND_ONLY_TOOLS = ("Claude Code", "Codex")
# Bytes before a read position whose hash shows the log was not rewritten since.
CHECKPOINT_BYTES = 4096
# Codex replays a fork's history within this many seconds of the session start.
CODEX_REPLAY_SECONDS = 5
CODEX_LINE_TYPES = re.compile(rb'"type":\s*"([a-z_]+)"')
CODEX_TIMESTAMP = re.compile(rb'"timestamp":\s*"([^"]+)"')
CODEX_INFO = re.compile(rb'"info":\s*\{')
CODEX_SERVICE_TIER = re.compile(rb'"service_tier":\s*"')


@dataclass(frozen=True)
class ReadPosition:
    """Where the last read of an append-only log stopped.

    `offset` ends the last complete line read, `checkpoint` hashes the bytes before it,
    and `context` holds the byte ranges of the earlier Codex lines its parser needs to
    read the lines after `offset` the way it reads the whole log: the session headers,
    the latest model and service tier events, and the latest running token total.
    """
    offset: int
    checkpoint: str
    context: tuple[tuple[int, int], ...] = ()

    def to_json(self) -> dict:
        return {"offset": self.offset, "checkpoint": self.checkpoint,
                "context": [list(span) for span in self.context]}


def _checkpoint(source, offset: int) -> str:
    start = max(0, offset - CHECKPOINT_BYTES)
    source.seek(start)
    return hashlib.sha256(source.read(offset - start)).hexdigest()


def _complete_end(source, start: int, size: int) -> int:
    """The end of the last complete line between start and size."""
    end = size
    while end > start:
        chunk_start = max(start, end - 65536)
        source.seek(chunk_start)
        newline = source.read(end - chunk_start).rfind(b"\n")
        if newline >= 0:
            return chunk_start + newline + 1
        end = chunk_start
    return start


def _codex_line_kind(line: bytes) -> str | None:
    """Which kind of context line this Codex line is, if any."""
    types = CODEX_LINE_TYPES.findall(line[:400])
    if not types:
        return None
    if types[0] in (b"session_meta", b"turn_context"):
        return types[0].decode()
    if types[0] != b"event_msg" or len(types) < 2:
        return None
    if types[1] == b"token_count" and CODEX_INFO.search(line):
        return "token_count"
    if types[1] == b"thread_settings_applied":
        return "tier" if CODEX_SERVICE_TIER.search(line) else "settings"
    return None


def _codex_timestamp(line: bytes) -> datetime | None:
    match = CODEX_TIMESTAMP.search(line[:200])
    try:
        return datetime.fromisoformat(match.group(1).decode()) if match else None
    except ValueError:
        return None


def _codex_context_line(line: bytes) -> bytes:
    """A context token count keeps only its running total, so it adds no row itself."""
    record = json.loads(line)
    record["payload"]["info"].pop("last_token_usage", None)
    record["payload"].pop("rate_limits", None)
    return json.dumps(record, separators=(",", ":")).encode() + b"\n"


def _read_line(source, span: tuple[int, int]) -> bytes:
    source.seek(span[0])
    return source.read(span[1] - span[0])


def _stage_log(path: str, tool: str, position: ReadPosition | None,
               mirror: Path) -> tuple[ReadPosition, bool]:
    """Copy a log's unread complete lines to `mirror`, with any context its parser needs.

    Returns the new read position and whether the log was copied from its start, whose
    cached rows the copy then replaces.
    """
    mirror.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "rb") as source, open(mirror, "wb") as out:
        size = os.fstat(source.fileno()).st_size
        start = 0
        if (position and position.offset <= size
                and _checkpoint(source, position.offset) == position.checkpoint):
            start = position.offset
        end = _complete_end(source, start, size)
        context: dict = {}
        if tool == "Codex" and start:
            for span in position.context:
                line = _read_line(source, span)
                kind = _codex_line_kind(line)
                context[(kind, span[0]) if kind == "session_meta" else kind] = (span, line)
            if not _codex_can_resume(context, _read_line(source, (start, end))[:400]):
                start, context = 0, {}
        if tool == "Codex":
            for span, line in sorted(context.values()):
                out.write(_codex_context_line(line) if _codex_line_kind(line) == "token_count"
                          else line)
            source.seek(start)
            offset = start
            while offset < end:
                line = source.readline()
                if not line or offset + len(line) > end:
                    break
                out.write(line)
                kind = _codex_line_kind(line)
                if kind:
                    span = (offset, offset + len(line))
                    context[(kind, offset) if kind == "session_meta" else kind] = (span, line)
                offset += len(line)
        else:
            source.seek(start)
            remaining = end - start
            while remaining:
                chunk = source.read(min(remaining, 1 << 20))
                if not chunk:
                    break
                out.write(chunk)
                remaining -= len(chunk)
        spans = tuple(sorted(span for span, _ in context.values()))
        return ReadPosition(end, _checkpoint(source, end), spans), start == 0


def _codex_can_resume(context: dict, first_new_line: bytes) -> bool:
    """Whether the lines after a read position parse as they would within the whole log.

    The parser gives lines before a session's first model event the first model that
    follows, and drops a fork's replayed history from the session's first seconds; both
    need the whole log when the earlier lines hold no model or end inside that window.
    """
    if "turn_context" not in context and "settings" not in context and "tier" not in context:
        return False
    starts = [_codex_timestamp(line) for key, (_, line) in context.items()
              if isinstance(key, tuple)]
    starts = [start for start in starts if start]
    if not starts:
        return True
    window_end = max(starts) + timedelta(seconds=CODEX_REPLAY_SECONDS)
    for line in (context.get("token_count", (None, b""))[1], first_new_line):
        stamp = _codex_timestamp(line) if line else None
        if stamp and stamp <= window_end:
            return False
    return True


def _positions_after_full_read(files: dict[str, "SourceSnapshot"]) -> dict[str, ReadPosition]:
    """Read positions for the Claude logs a rebuild read whole and unchanged.

    Codex logs get theirs on their next update, which records their context lines.
    """
    positions = {}
    for path, snapshot in files.items():
        if snapshot.tool != "Claude Code":
            continue
        try:
            with open(path, "rb") as source:
                status = os.fstat(source.fileno())
                if (status.st_mtime_ns, status.st_size) != (snapshot.mtime_ns, snapshot.size):
                    continue
                end = _complete_end(source, 0, status.st_size)
                positions[path] = ReadPosition(end, _checkpoint(source, end))
        except OSError:
            continue
    return positions


def _iter_source_trees() -> list[tuple[str, Path, str]]:
    trees = list(SOURCE_TREES)
    for wsl_home in _WSL_HOMES:
        trees.extend([
            ("Claude Code", Path(wsl_home) / ".claude" / "projects", "*.jsonl"),
            ("Codex", Path(wsl_home) / ".codex" / "sessions", "*.jsonl"),
            ("Codex", Path(wsl_home) / ".codex" / "archived_sessions", "*.jsonl"),
            ("Gemini", Path(wsl_home) / ".gemini" / "tmp", "chats/*.json"),
            ("Gemini", Path(wsl_home) / ".gemini" / "tmp", "chats/*.jsonl"),
            ("Agy", Path(wsl_home) / ".gemini" / "antigravity-cli" / "brain",
             "*/.system_generated/logs/transcript_full.jsonl"),
            (AGY_METADATA_TOOL,
             Path(wsl_home) / ".gemini" / "antigravity-cli" / "cache",
             "conversation_metadata.json"),
            (ZCODE_TOOL, Path(wsl_home) / ".zcode" / "cli" / "db", "db.sqlite"),
        ])
    return trees


def _aop_results(archive: Path) -> dict[str, os.stat_result]:
    """The current result of every run in AOP's archive, with its file status. The
    archive's top level holds one record per run; replaced originals sit in a
    subdirectory and are not read."""
    results: dict[str, os.stat_result] = {}
    try:
        entries = os.scandir(archive)
    except OSError:
        return results
    with entries:
        for entry in entries:
            if not entry.name.endswith(".json"):
                continue
            try:
                status = entry.stat()
            except OSError:
                continue
            if stat.S_ISREG(status.st_mode):
                results[Path(entry.path).as_posix()] = status
    return results


def scan_source_files() -> tuple[dict[str, SourceSnapshot], int, float]:
    files: dict[str, SourceSnapshot] = {}
    dir_count = 0
    started = time.perf_counter()
    for tool, root, pattern in _iter_source_trees():
        try:
            if tool == AOP_RESULT_TOOL:
                results = _aop_results(root)
                dir_count += 1
                for path, status in results.items():
                    files[path] = SourceSnapshot(
                        tool=tool, mtime_ns=status.st_mtime_ns, size=status.st_size)
                continue
            if not root.exists():
                continue
            dir_count += 1
            for path in root.rglob(pattern):
                # AOP keeps run logs in .aop/runs, which can sit inside a tool's own tree
                # (Claude memory folders); they are not that tool's transcripts.
                if ".aop" in path.relative_to(root).parts:
                    continue
                # Claude Code keeps workflow journals beside subagent transcripts, which
                # are always agent-<id>.jsonl.
                if (tool == "Claude Code" and "subagents" in path.relative_to(root).parts
                        and not path.name.startswith("agent-")):
                    continue
                stats = [path.stat()]
                if tool == ZCODE_TOOL:
                    # SQLite commits land in the write-ahead log before checkpointing.
                    wal = path.with_name(path.name + "-wal")
                    if wal.exists():
                        stats.append(wal.stat())
                files[path.as_posix()] = SourceSnapshot(
                    tool=tool,
                    mtime_ns=max(stat.st_mtime_ns for stat in stats),
                    size=sum(stat.st_size for stat in stats),
                )
        except OSError:
            continue
    return files, dir_count, time.perf_counter() - started


def sources_mtime() -> tuple[float, int, int, float]:
    """Newest mtime across all source files plus basic scan stats."""
    files, dir_count, elapsed = scan_source_files()
    newest = max((meta.mtime_ns / 1e9 for meta in files.values()), default=0.0)
    return newest, len(files), dir_count, elapsed


def _load_cache_meta() -> tuple[int, dict[str, SourceSnapshot], str,
                                dict[str, ReadPosition], set[str] | None] | None:
    """The cache's version, known files, pricing basis, read positions, and the known files
    the scan before it did not find, or None when the cache did not record them."""
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
        positions = {
            path: ReadPosition(int(position["offset"]), str(position["checkpoint"]),
                               tuple(tuple(map(int, span)) for span in position["context"]))
            for path, position in data.get("positions", {}).items()
        }
        missing = set(map(str, data["missing"])) if "missing" in data else None
    except (KeyError, TypeError, ValueError):
        return None
    return version, files, str(data.get("pricing_basis", "")), positions, missing


def _write_cache_meta(files: dict[str, SourceSnapshot],
                      positions: dict[str, ReadPosition], missing: Iterable[str] = ()) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": CACHE_SCHEMA_VERSION,
        "pricing_basis": cache_pricing_key(),
        "files": {path: meta.to_json() for path, meta in sorted(files.items())},
        "positions": {path: position.to_json() for path, position in sorted(positions.items())},
        "missing": sorted(missing),
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


AOP_BATCH = 5000
CODEX_BATCH_BYTES = 2 << 30


def _source_batches(files: dict[str, SourceSnapshot]) -> list[dict[str, list[str]]]:
    """Files grouped by tool, with the two large sources split into batches to bound
    memory. Parsing AOP results in one query takes about 110 KB per result, so they go
    AOP_BATCH at a time. Parsing Codex logs takes about 1.25 times their size, so they go
    in batches of up to CODEX_BATCH_BYTES; each log is parsed independently of the rest.
    """
    grouped = _group_files_by_tool(files)
    aop = grouped.pop(AOP_RESULT_TOOL, [])
    batches = [grouped, *({AOP_RESULT_TOOL: aop[start:start + AOP_BATCH]}
                          for start in range(0, len(aop), AOP_BATCH))]
    logs: list[str] = []
    size = 0
    for path in grouped.pop("Codex", []):
        if logs and size + files[path].size > CODEX_BATCH_BYTES:
            batches.append({"Codex": logs})
            logs, size = [], 0
        logs.append(path)
        size += files[path].size
    if logs:
        batches.append({"Codex": logs})
    return batches


def _skipping_deleted(read, grouped: dict[str, list[str]]):
    """Run read(grouped), and when DuckDB cannot open a file, run it again without the
    files deleted since the scan. Benchmark harnesses remove whole AOP run directories
    seconds after a turn ends, so a listed file can be gone by the time it is read."""
    while True:
        try:
            return read(grouped)
        except duckdb.IOException:
            present = {tool: [path for path in paths if os.path.exists(path)]
                       for tool, paths in grouped.items()}
            if present == grouped:
                raise
            grouped = present


def _insert_source_files(db: duckdb.DuckDBPyConnection,
                         files: dict[str, SourceSnapshot]) -> None:
    def insert(grouped: dict[str, list[str]]) -> None:
        parts = _build_source_sql(db, grouped)
        if parts:
            db.execute("INSERT INTO tokens BY NAME "
                       + " UNION ALL BY NAME ".join(f"SELECT * FROM ({part})" for part in parts))

    for grouped in _source_batches(files):
        _skipping_deleted(insert, grouped)


def _build_source_sql(db: duckdb.DuckDBPyConnection,
                      grouped_files: dict[str, list[str]]) -> list[str]:
    parts = []
    for tool_name, builder in SOURCES.values():
        paths = grouped_files.get(tool_name, [])
        if paths:
            if tool_name == ZCODE_TOOL:
                _load_zcode_usage(db, paths)
            if tool_name == MUSE_TOOL:
                # A subagent's log names no workspace; its session's log does, so that
                # is read alongside without adding its rows.
                _path_table(db, "muse_logs", paths)
                _path_table(db, "muse_subagents", _muse_subagents(paths))
                parts.append(builder(_sql_file_list([*paths, *_muse_parent_logs(paths)])))
                continue
            if tool_name == "Gemini":
                # Chats in the older single-document format and in JSON Lines.
                for builder, chats in ((_gemini_sql, [p for p in paths if not p.endswith(".jsonl")]),
                                       (_gemini_jsonl_sql, [p for p in paths if p.endswith(".jsonl")])):
                    if chats:
                        parts.append(builder(_sql_file_list(chats)))
                continue
            if tool_name == "Agy":
                _load_agy_models(db, paths)
                parts.append(builder(_sql_file_list(paths), models_table=AGY_MODELS_TABLE,
                                     usage_table=AGY_USAGE_TABLE))
                continue
            parts.append(builder(_sql_file_list(paths)))
    return parts


def _expand_dependency_changes(
        files: dict[str, SourceSnapshot],
        changed: list[str],
) -> list[str]:
    dependencies = [files.get(path) for path in changed]
    if any(snapshot and snapshot.tool == AGY_METADATA_TOOL for snapshot in dependencies):
        changed = [
            *changed,
            *(path for path, snapshot in files.items() if snapshot.tool == "Agy"),
        ]
    return sorted(set(changed))


def _write_cache(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                 positions: dict[str, ReadPosition], timer: DebugTimer, action: str,
                 detail: str = "", missing: Iterable[str] = ()) -> int:
    with timer.span("count token rows"):
        row_count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with timer.span("write parquet cache", CACHE_FILE.as_posix()):
        db.execute(f"COPY tokens TO '{CACHE_FILE.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    _write_cache_meta(files, positions, missing)
    detail_part = f"{detail}; " if detail else ""
    print(f"  {action}: {detail_part}{row_count} rows → {CACHE_FILE}", file=sys.stderr)
    return row_count


def _unmigrated_aop_results(db: duckdb.DuckDBPyConnection,
                            files: dict[str, SourceSnapshot]) -> set[str]:
    """AOP results that are not v3 or name no state_root, whose cached usage is kept until
    AOP migrates them."""
    def read(grouped: dict[str, list[str]]) -> list[tuple[str]]:
        if not grouped.get(AOP_RESULT_TOOL):
            return []
        return db.execute(f"""
            SELECT replace(filename, '\\', '/')
            FROM read_json_objects({_sql_file_list(grouped[AOP_RESULT_TOOL])}, format='auto',
                                   filename=true, ignore_errors=true)
            WHERE coalesce(json_extract_string(json, '$.usage_schema'), '') != '{AOP_USAGE_SCHEMA}'
               OR json_extract_string(json, '$.state_root') IS NULL
        """).fetchall()

    unmigrated = set()
    for grouped in _source_batches(files):
        unmigrated.update(row[0] for row in _skipping_deleted(read, grouped))
    if unmigrated:
        print(f"  warning: {len(unmigrated)} AOP results are not {AOP_USAGE_SCHEMA} with a "
              "state_root; their cached usage is kept. Run aop migrate-results.", file=sys.stderr)
    return unmigrated


def _path_table(db: duckdb.DuckDBPyConnection, name: str, paths: list[str]) -> None:
    """A temporary table of paths. A list literal loads 200,000 paths in about a second,
    where a bound list takes 18 and a row at a time about a minute."""
    literals = ", ".join(_sql_literal(path) for path in paths)
    db.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS "
               f"SELECT unnest([{literals}]::VARCHAR[]) AS path")


def _rebuild_tokens(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                    timer: DebugTimer) -> set[str]:
    """Rebuild the table from source files, returning the AOP results left unread."""
    unmigrated = _unmigrated_aop_results(db, files)
    files = {path: snapshot for path, snapshot in files.items() if path not in unmigrated}
    _empty_tokens_table(db)
    with timer.span("build tokens table", f"{len(files)} files"):
        _insert_source_files(db, files)
    _carry_over_vanished_rows(db, files, timer)
    _drop_other_copies_of_aop_runs(db, list(files), timer)
    _drop_moved_codex_logs(db, files, timer)
    _count_claude_messages_once(db, timer)
    _name_claude_projects(db, timer)
    _deduplicate_aop(db, timer)
    with timer.span("price rows"):
        price_tokens(db, all_rows=True)
    return unmigrated


def _carry_over_vanished_rows(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                              timer: DebugTimer) -> None:
    """Keep the cached rows of source files this scan did not find.

    Tools prune their own logs, and AOP results are found relative to the working
    directory, so the cache can hold the only record of that usage.
    """
    if not CACHE_FILE.exists():
        return
    cache = _sql_literal(CACHE_FILE.as_posix())
    cached = {row[0] for row in db.sql(f"DESCRIBE SELECT * FROM read_parquet({cache})").fetchall()}
    if "source_file" not in cached:
        return
    current = {row[0] for row in db.sql("DESCRIBE tokens").fetchall()}
    columns = ", ".join(f'"{name}"' for name in sorted(cached & current))
    with timer.span("carry over vanished sources"):
        _path_table(db, "scanned_sources", list(files))
        db.execute(f"""
            INSERT INTO tokens BY NAME
            SELECT {columns} FROM read_parquet({cache})
            WHERE source_file IS NOT NULL
              AND source_file NOT IN (SELECT path FROM scanned_sources)
        """)
        db.execute("DROP TABLE scanned_sources")


AOP_RUN_ID_PATTERNS = (r"/[.]aop/runs/([^/]+)/result[.]json$", r"/aop/results/([^/]+)[.]json$")


def _aop_run_id(path: str) -> str:
    """The run ID in the path of an AOP result, or '' for any other file."""
    for pattern in AOP_RUN_ID_PATTERNS:
        if match := re.search(pattern, path):
            return match.group(1)
    return ""


def _aop_run_id_sql(column: str) -> str:
    return "coalesce(" + ", ".join(
        f"nullif(regexp_extract({column}, '{pattern}', 1), '')"
        for pattern in AOP_RUN_ID_PATTERNS) + ")"


def _drop_other_copies_of_aop_runs(db: duckdb.DuckDBPyConnection, read: list[str],
                                   timer: DebugTimer) -> None:
    """Count each AOP run once when its results were copied under another path.

    A run is identified by its run ID, which names both its archived result and its run
    directory under .aop/runs. Rows read just now from `read` replace the rows of the same
    runs from any other path, such as kept rows of a run directory read before AOP
    archived its results.
    """
    read = [path for path in read if _aop_run_id(path)]
    if not read:
        return
    run = _aop_run_id_sql("source_file")
    with timer.span("drop other copies of AOP runs"):
        _path_table(db, "read_results", read)
        db.execute(f"""
            DELETE FROM tokens
            WHERE source_kind = 'aop'
              AND source_file NOT IN (SELECT path FROM read_results)
              AND {run} IN (SELECT {_aop_run_id_sql("path")} FROM read_results)
        """)
        # Copies read together keep the first path.
        db.execute(f"""
            DELETE FROM tokens
            WHERE source_kind = 'aop' AND source_file IN (
                SELECT source_file FROM (
                    SELECT source_file, min(source_file) OVER (PARTITION BY run) AS kept
                    FROM (SELECT DISTINCT source_file, {run} AS run FROM tokens
                          WHERE source_kind = 'aop'
                            AND source_file IN (SELECT path FROM read_results)))
                WHERE source_file != kept)
        """)
        db.execute("DROP TABLE read_results")


def _drop_moved_codex_logs(db: duckdb.DuckDBPyConnection, read: dict[str, SourceSnapshot],
                           timer: DebugTimer) -> None:
    """Count a Codex log once after Codex moves it from sessions to archived_sessions.

    A log keeps its file name, which ends in its thread id, so rows read just now replace
    the kept rows of a log with the same name at another path.
    """
    logs = [path for path, snapshot in read.items() if snapshot.tool == "Codex"]
    if not logs:
        return
    name = "regexp_extract(source_file, '[^/]+$')"
    with timer.span("drop moved Codex logs"):
        _path_table(db, "read_logs", logs)
        db.execute(f"""
            DELETE FROM tokens
            WHERE tool = 'Codex' AND coalesce(source_kind, 'native') = 'native'
              AND source_file NOT IN (SELECT path FROM read_logs)
              AND {name} IN (SELECT regexp_extract(path, '[^/]+$') FROM read_logs)
        """)
        db.execute("DROP TABLE read_logs")


def _count_claude_messages_once(db: duckdb.DuckDBPyConnection, timer: DebugTimer) -> None:
    """Keep one row per Claude Code API response.

    Claude Code writes a transcript line for each content block of a response, each with
    the response's usage, and only output_tokens grows on the later lines. A forked or
    resumed session copies earlier responses into its own transcript. So each response
    keeps its row with the most output, from the first transcript by path that has it.
    """
    with timer.span("count Claude messages once"):
        db.execute("""
            DELETE FROM tokens WHERE rowid IN (
                SELECT rowid FROM (
                    SELECT rowid, row_number() OVER (
                        PARTITION BY message_id
                        ORDER BY output_tokens DESC, source_file, timestamp) AS copy
                    FROM tokens
                    WHERE tool = 'Claude Code' AND message_id IS NOT NULL
                      AND coalesce(source_kind, 'native') = 'native')
                WHERE copy > 1)
        """)


def _name_claude_projects(db: duckdb.DuckDBPyConnection, timer: DebugTimer) -> None:
    """Name each Claude Code session's project after the directory it was launched in.

    Claude Code stores a session under ~/.claude/projects/<launch directory with every
    character other than a letter or digit replaced by '-'>. The launch directory is the
    ancestor of any of the folder's request directories that encodes to that name, so
    requests made in a subdirectory or another repository keep the session's project.
    """
    with timer.span("name Claude projects"):
        db.execute("""
            WITH requests AS (
                SELECT DISTINCT regexp_extract(source_file, '/projects/([^/]+)/', 1) AS folder,
                       project_path AS cwd
                FROM tokens
                WHERE tool = 'Claude Code' AND coalesce(source_kind, 'native') = 'native'
                  AND project_path IS NOT NULL
            ),
            launch AS (
                SELECT folder, min(dir) AS dir FROM (
                    SELECT folder, unnest(list_filter(
                        list_transform(range(2, len(string_split(cwd, '/')) + 1),
                                       i -> array_to_string(string_split(cwd, '/')[1:i], '/')),
                        dir -> regexp_replace(dir, '[^a-zA-Z0-9]', '-', 'g') = folder)) AS dir
                    FROM requests)
                GROUP BY folder
            )
            UPDATE tokens SET project = lower(split_part(launch.dir, '/', -1))
            FROM launch
            WHERE tokens.tool = 'Claude Code' AND coalesce(tokens.source_kind, 'native') = 'native'
              AND regexp_extract(tokens.source_file, '/projects/([^/]+)/', 1) = launch.folder
        """)


def _deduplicate_aop(db: duckdb.DuckDBPyConnection, timer: DebugTimer) -> None:
    """Prefer normalized AOP usage over overlapping provider-native sessions."""
    columns = {row[0] for row in db.sql("DESCRIBE tokens").fetchall()}
    if "source_kind" not in columns:
        return
    if "model_assumed" not in columns:
        db.execute("ALTER TABLE tokens ADD COLUMN model_assumed BOOLEAN")
    with timer.span("deduplicate AOP sessions"):
        # A run started without a model records none; take the model the session's
        # native log says it ran, by most tokens, before those rows are dropped.
        db.execute("""
            UPDATE tokens SET model = native.model
            FROM (
                SELECT tool, session, arg_max(model, tokens) AS model
                FROM (
                    SELECT tool, session, model, sum(total_tokens) AS tokens
                    FROM tokens
                    WHERE coalesce(source_kind, 'native') != 'aop' AND coalesce(model, '') != ''
                    GROUP BY ALL
                )
                GROUP BY ALL
            ) AS native
            WHERE tokens.source_kind = 'aop' AND coalesce(tokens.model, '') = ''
              AND tokens.tool = native.tool AND tokens.session = native.session
        """)
        # Without a native log, assume the tool's default was the model it ran most that
        # day, and mark the model as assumed.
        db.execute("""
            UPDATE tokens SET model = daily.model, model_assumed = true
            FROM (
                SELECT tool, date, arg_max(model, tokens) AS model
                FROM (
                    SELECT tool, date, model, sum(total_tokens) AS tokens
                    FROM tokens
                    WHERE coalesce(source_kind, 'native') != 'aop' AND coalesce(model, '') != ''
                    GROUP BY ALL
                )
                GROUP BY ALL
            ) AS daily
            WHERE tokens.source_kind = 'aop' AND coalesce(tokens.model, '') = ''
              AND tokens.tool = daily.tool AND tokens.date = daily.date
        """)
        db.execute("""
            DELETE FROM tokens
            WHERE coalesce(source_kind, 'native') != 'aop'
              AND session IS NOT NULL
              AND (tool, session) IN (
                  SELECT tool, session FROM tokens WHERE source_kind = 'aop'
              )
        """)


def _aop_change_requires_rebuild(
    cached_files: dict[str, SourceSnapshot], changed: list[str]
) -> bool:
    """A changed retained result may release a previously shadowed native session."""
    return any(
        cached_files.get(path)
        and cached_files[path].tool == AOP_RESULT_TOOL
        for path in changed
    )


def _delete_source_files(db: duckdb.DuckDBPyConnection, paths: list[str],
                         timer: DebugTimer) -> None:
    if not paths:
        return
    with timer.span("drop changed rows", f"{len(paths)} files"):
        db.execute(f"DELETE FROM tokens WHERE source_file IN ({', '.join(_sql_literal(p) for p in paths)})")


def _append_source_files(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                         timer: DebugTimer, real_paths: dict[str, str] | None = None) -> None:
    """Parse files into the table; rows from a copy at a key of `real_paths` name the
    file it was copied from."""
    if not files:
        return
    with timer.span("append changed files", f"{len(files)} files"):
        _insert_source_files(db, files)
        if real_paths:
            db.execute("CREATE OR REPLACE TEMP TABLE copied_logs (copy VARCHAR, path VARCHAR)")
            db.executemany("INSERT INTO copied_logs VALUES (?, ?)", list(real_paths.items()))
            db.execute("""
                UPDATE tokens SET source_file = copied_logs.path FROM copied_logs
                WHERE tokens.source_file = copied_logs.copy
            """)
            db.execute("DROP TABLE copied_logs")


def _read_changed_files(db: duckdb.DuckDBPyConnection, files: dict[str, SourceSnapshot],
                        positions: dict[str, ReadPosition], timer: DebugTimer) -> None:
    """Replace the rows of changed files, reading append-only logs from their positions.

    Each log's unread lines are copied beside the cache under a path ending in the log's
    own, so the parsers derive the same sessions and projects from its name.
    """
    logs = {path: snapshot for path, snapshot in files.items()
            if snapshot.tool in APPEND_ONLY_TOOLS}
    # A file deleted since the scan keeps its cached rows rather than losing them unread.
    others = {path: snapshot for path, snapshot in files.items()
              if path not in logs and os.path.exists(path)}
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=CACHE_DIR, prefix="new-lines-") as copies:
        staged: dict[str, SourceSnapshot] = {}
        real_paths: dict[str, str] = {}
        from_start = []
        with timer.span("copy new log lines", f"{len(logs)} logs"):
            for index, (path, snapshot) in enumerate(sorted(logs.items())):
                mirror = Path(copies, str(index), path.lstrip("/").replace(":", ""))
                try:
                    positions[path], whole = _stage_log(path, snapshot.tool,
                                                        positions.get(path), mirror)
                except OSError:
                    # Gone or unreadable since the scan: its cached rows stay.
                    positions.pop(path, None)
                    continue
                if whole:
                    from_start.append(path)
                if mirror.stat().st_size:
                    staged[mirror.as_posix()] = snapshot
                    real_paths[mirror.as_posix()] = path
        _delete_source_files(db, [*others, *from_start], timer)
        _append_source_files(db, {**others, **staged}, timer, real_paths)


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

    meta = _load_cache_meta()
    positions = dict(meta[3]) if meta else {}
    # Known files this scan did not find; their rows are kept. Only those missing since the
    # last cache write are reported, since most stay missing for good.
    missing = sorted(path for path in (meta[1] if meta else {}) if path not in files)
    previously_missing = meta[4] if meta else None
    newly_missing = ([] if previously_missing is None
                     else [path for path in missing if path not in previously_missing])

    def with_missing(detail: str) -> str:
        if not newly_missing:
            return detail
        note = f"{len(newly_missing)} files no longer on disk, their rows kept"
        return f"{detail}, {note}" if detail else note

    def rebuild(detail: str = "") -> None:
        unread = _rebuild_tokens(db, files, timer)
        _write_cache(db, known(unread), _positions_after_full_read(files), timer,
                     "rebuilt cache", with_missing(detail), missing)

    def known(unread: set[str]) -> dict[str, SourceSnapshot]:
        # Files a scan no longer finds, or could not read, stay known with their last
        # snapshot, so their rows are kept and a file that reappears unchanged is not
        # loaded again.
        return {**(meta[1] if meta else {}),
                **{path: snapshot for path, snapshot in files.items() if path not in unread}}

    if refresh:
        timer.note("cache refresh forced by --refresh")
        rebuild("--refresh")
        return

    cache_mt = CACHE_FILE.stat().st_mtime if CACHE_FILE.exists() else 0.0
    newest_source = max((meta.mtime_ns / 1e9 for meta in files.values()), default=0.0)

    if CACHE_FILE.exists() and meta and meta[0] == CACHE_SCHEMA_VERSION:
        _, cached_files, cached_basis, _, _ = meta
        # Price data changed since the cache was written: reprice every row.
        reprice = cached_basis != cache_pricing_key()
        changed = sorted(
            path for path, snapshot in files.items()
            if cached_files.get(path) != snapshot
        )
        changed = _expand_dependency_changes(files, changed)
        if not changed:
            with timer.span("load parquet cache", CACHE_FILE.as_posix()):
                db.execute(f"CREATE TABLE tokens AS FROM '{CACHE_FILE.as_posix()}'")
            if reprice:
                with timer.span("reprice rows", pricing_basis()):
                    price_tokens(db, all_rows=True)
                _write_cache(db, known(set()), positions, timer, "repriced cache",
                             with_missing(pricing_basis()), missing)
                return
            print("  (cached)", file=sys.stderr)
            timer.note(
                f"cache hit: {len(files)} source files unchanged since cache {_fmt_debug_ts(cache_mt)}"
            )
            return

        if _aop_change_requires_rebuild(cached_files, changed):
            timer.note("retained AOP result changed; rebuilding for precedence")
            rebuild("AOP result changed")
            return

        with timer.span("load parquet cache", CACHE_FILE.as_posix()):
            db.execute(f"CREATE TABLE tokens AS FROM '{CACHE_FILE.as_posix()}'")
        if not _table_has_source_file(db):
            timer.note("cache schema missing source_file; rebuilding")
            db.execute("DROP TABLE tokens")
            rebuild("schema migration")
            return

        timer.note(
            f"incremental update: {len(changed)} changed, {len(missing)} known files missing and kept; newest source {_fmt_debug_ts(newest_source)}"
        )
        unread = _unmigrated_aop_results(db, {path: files[path] for path in changed})
        changed = [path for path in changed if path not in unread]
        _read_changed_files(db, {path: files[path] for path in changed}, positions, timer)
        _drop_other_copies_of_aop_runs(db, changed, timer)
        _drop_moved_codex_logs(db, {path: files[path] for path in changed}, timer)
        _count_claude_messages_once(db, timer)
        _name_claude_projects(db, timer)
        _deduplicate_aop(db, timer)
        with timer.span("price rows"):
            price_tokens(db, all_rows=reprice)
        _write_cache(
            db,
            known(unread),
            positions,
            timer,
            "updated cache",
            with_missing(f"{len(changed)} changed"),
            missing,
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

    rebuild()


def _get_version() -> str:
    try:
        from importlib.metadata import version
        return version("clanker-analytics")
    except Exception:
        return "dev"


def _run(args: argparse.Namespace, timing: DebugTimer | None = None) -> int:
    timer = timing or DebugTimer(False)

    if args.command == "record-quota":
        from clanker_analytics.quota import QUOTA_PROVIDERS, record_quota
        inserted, errors = record_quota(Path(args.quota_db))
        print(f"Recorded {inserted} new quota samples in {Path(args.quota_db).expanduser()}")
        for error in errors:
            print(f"  warning: {error}", file=sys.stderr)
        return 1 if len(errors) == len(QUOTA_PROVIDERS) else 0

    db = duckdb.connect()
    with timer.span("register macros"):
        register_macros(db)
    with timer.span("refresh prices"):
        refresh_prices()
    with timer.span("load tokens"):
        load_tokens(db, args.refresh, timer)
    if args.prices == "current":
        # Reprice every request from today's price list; the on-disk cache keeps both.
        db.execute("UPDATE tokens SET api_cost_usd = current_api_cost_usd")
    basis = f"{pricing_basis()}, {PRICE_BASES[args.prices]}"

    if args.command == "pace":
        from clanker_analytics.quota import build_report, connect, print_report
        from clanker_analytics.quota import DEFAULT_LOOKBACKS
        lookbacks = args.periods or list(DEFAULT_LOOKBACKS)
        with connect(Path(args.quota_db)) as quota_db:
            report = build_report(
                quota_db, db, lookbacks, COST_PER_ROW,
                include_inactive=args.all_buckets, pricing_basis=basis,
            )
        if args.json:
            print(json.dumps(report, indent=2))
        elif report["buckets"]:
            print_report(report, show_model_mix=args.model_mix)
        elif report["sample_bucket_count"]:
            print(
                "No active complete pace lookbacks yet. "
                "Keep the quota recorder running or use pace --all-buckets."
            )
            return 1
        else:
            print("No quota samples found. Run clanker-analytics record-quota first.")
            return 1
        return 0

    if args.command in ("quota-history", "value"):
        from clanker_analytics.quota import (
            account_labels, build_quota_history, build_value_report, connect, print_quota_history,
            print_value_report,
        )
        with connect(Path(args.quota_db)) as quota_db:
            report = build_quota_history(db, COST_PER_ROW, basis, quota_db)
            labels = account_labels(quota_db)
        if args.command == "value":
            with timer.span("detect plans"):
                plans = plans_for(args)
            report = build_value_report(report, plans, {tool for tool, _ in args.plan}, labels)
            if args.json:
                print(json.dumps(report, indent=2))
            elif report["tools"]:
                print_value_report(report)
            else:
                print("No weekly quota periods with local usage found.")
                return 1
            return 0
        if args.json:
            print(json.dumps(report, indent=2))
        elif report["periods"]:
            print_quota_history(report)
        else:
            print("No quota observations found in local logs or recorded snapshots.")
            return 1
        return 0

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
            if args.tool == "aop":
                db.execute("DELETE FROM tokens WHERE source_kind != 'aop'")
            else:
                db.execute(f"DELETE FROM tokens WHERE tool != {_sql_literal(tool_name)}")

    if args.command in ("diagnose", "cache", "throughput"):
        from clanker_analytics.diagnose import (
            build_cache_health, build_diagnosis, build_throughput, print_cache_health,
            print_diagnosis, print_throughput,
        )
        now = datetime.now(timezone.utc)
        recent = _since_delta(args.since, now, default="3d" if args.command == "diagnose" else "7d")
        if args.command == "diagnose":
            report = build_diagnosis(db, recent, timedelta(seconds=args.vs or 7 * 86400), now,
                                     limit=min(args.limit, 10))
            report["pricing_basis"] = basis
            print(json.dumps(report, indent=2) if args.json else "", end="")
            if not args.json:
                print_diagnosis(report)
        elif args.command == "throughput":
            report = build_throughput(db, now - recent, now)
            print(json.dumps(report, indent=2) if args.json else "", end="")
            if not args.json:
                print_throughput(report)
        else:
            report = build_cache_health(db, now - recent, limit=min(args.limit, 10))
            report["pricing_basis"] = basis
            print(json.dumps(report, indent=2, default=str) if args.json else "", end="")
            if not args.json:
                print_cache_health(report)
        return 0

    # Views over a period default to the last 7 days; SQL and regime see all history.
    if args.command in ("table", "chart", "share") and not args.since:
        args.since = "7d"
    if args.since:
        with timer.span("filter since", args.since):
            m = re.fullmatch(r'(\d+)([hdw])', args.since)
            if m:
                n, unit = int(m.group(1)), m.group(2)
                interval = {'h': 'HOUR', 'd': 'DAY', 'w': 'WEEK'}[unit]
                db.execute(f"DELETE FROM tokens WHERE date < (current_date - INTERVAL {n} {interval})::DATE::VARCHAR")
            else:
                db.execute(f"DELETE FROM tokens WHERE date < '{args.since}'")

    if args.command == "sql":
        with timer.span("run custom sql"):
            result = db.sql(args.query)
            # Statements such as COPY return no rows to show.
            if result is not None:
                result.show(max_rows=100)
        return 0

    if args.command == "regime":
        from clanker_analytics.regime import detect_and_plot
        path = detect_and_plot(db, args.since, args.tool if args.tool != "all" else None,
                               output=args.output)
        if path:
            print(f"  Card saved to {path}")
        return 0

    if args.command == "table":
        with timer.span("render table", args.by):
            db.sql(QUERIES[args.by].format(limit=args.limit)).show(max_rows=100)
        print(f"pricing: {basis}")
        return 0

    # chart or share
    with timer.span("import share module"):
        from clanker_analytics.share import generate, copy_and_open
    since = args.since
    with timer.span("detect plans"):
        plans = plans_for(args)
    cost_mode = "monthly" if args.monthly else ("prorated" if args.prorated else "auto")
    with timer.span("generate chart", since):
        path = generate(db, since, plans, cost_mode, output=args.output)
    if not path:
        pass
    elif args.command == "share":
        with timer.span("share card"):
            total_cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
            sub_cost = sum(c for _, c in plans.values())
            copy_and_open(path, total_cost or 0, since, sub_cost, cost_mode)
    else:
        print(f"  Card saved to {path}")
    return 0


def _since_delta(since: str | None, now: datetime, default: str) -> timedelta:
    """A --since value, either a duration such as 3d or a start date, as a span ending now."""
    from clanker_analytics.quota import parse_duration
    value = since or default
    try:
        return timedelta(seconds=parse_duration(value))
    except ValueError:
        start = datetime.fromisoformat(value)
        return now - (start if start.tzinfo else start.replace(tzinfo=timezone.utc))


# Options that chose a mode before commands existed, and the command each maps to.
LEGACY_MODES = {
    "--record-quota": "record-quota", "--pace": "pace", "--quota-history": "quota-history",
    "--sql": "sql", "--regime": "regime", "--table": "table", "--share": "share",
    "--chart": "chart",
}


def _modernize_argv(argv: list[str], commands: set[str]) -> list[str]:
    """Rewrite the option-style interface of 0.4 into a command line, with a warning.

    A bare invocation, or one that starts with options, runs the default table.
    """
    if argv and (argv[0] in commands or argv[0] in ("-h", "--help", "--version")):
        return argv
    modes = [arg for arg in argv if arg in LEGACY_MODES]
    legacy = modes or any(arg in ("--lookback", "--quota-json") for arg in argv)
    command = LEGACY_MODES[modes[0]] if modes else "table"
    rest, periods = [], []
    args = iter(argv)
    for arg in args:
        if arg in LEGACY_MODES:
            if arg == "--sql":
                rest.append(next(args, ""))
        elif arg == "--lookback":
            periods.append(next(args, ""))
        elif arg == "--quota-json":
            rest.append("--json")
        else:
            rest.append(arg)
    modern = [command, *periods, *rest]
    if legacy:
        print(f"warning: options such as {modes[0] if modes else '--lookback'} are deprecated "
              f"and will be removed; run: clanker-analytics {' '.join(modern)}", file=sys.stderr)
    return modern


def main(argv: list[str] | None = None):
    from clanker_analytics.quota import DEFAULT_DB_PATH, parse_duration

    def duration(value: str) -> int:
        try:
            return parse_duration(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError(str(error)) from error

    def plan_setting(value: str) -> tuple[str, tuple[str, float]]:
        key, separator, choice = value.partition("=")
        if not separator:
            raise argparse.ArgumentTypeError("plan must be TOOL=PLAN or TOOL=USD, such as codex=pro_200")
        try:
            return plan_choice(key, choice)
        except ValueError as error:
            raise argparse.ArgumentTypeError(str(error)) from None

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--prices", choices=list(PRICE_BASES), default="historical",
                        help="Price requests at the rates in effect when they ran "
                             "(historical, default) or at today's rates (current)")
    common.add_argument("--refresh", action="store_true",
                        help="Rebuild the token cache from source files first")
    common.add_argument("--debug-timing", action="store_true",
                        help="Print execution timings and cache decisions to stderr")
    common.add_argument("--profile", action="store_true",
                        help="Print a cProfile summary to stderr")
    usage = argparse.ArgumentParser(add_help=False)
    usage.add_argument("--tool", choices=[*SOURCES, "all"], default="all",
                       help="Which tool to analyze (default: all)")
    usage.add_argument("--since", type=str,
                       help="Only include data since a duration or date, such as 24h, 7d or 2026-03-01")
    quota_db = argparse.ArgumentParser(add_help=False)
    quota_db.add_argument("--quota-db", default=str(DEFAULT_DB_PATH),
                          help=f"Quota history database (default: {DEFAULT_DB_PATH})")
    as_json = argparse.ArgumentParser(add_help=False)
    as_json.add_argument("--json", action="store_true", help="Print the report as JSON")
    limit = argparse.ArgumentParser(add_help=False)
    limit.add_argument("--limit", type=int, default=50, help="Max rows to display (default: 50)")
    output = argparse.ArgumentParser(add_help=False)
    output.add_argument("-o", "--output", type=lambda value: Path(value).expanduser(),
                        metavar="PATH",
                        help="Where to save the PNG (default: in ~/.cache/clanker-analytics/)")
    plans = argparse.ArgumentParser(add_help=False)
    plans.add_argument("--plan", action="append", type=plan_setting, default=[],
                       metavar="TOOL=PLAN|USD",
                       help="Subscription plan for this run, as a model-prices plan id such as "
                            "codex=pro_200 or a monthly price such as zcode=18; overrides "
                            "the [plans] table in ~/.config/clanker-analytics/config.toml")

    parser = argparse.ArgumentParser(
        description="AI coding tool token analytics. Without a command, runs table.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {_get_version()}")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)

    def command(name: str, help: str, *parents: argparse.ArgumentParser) -> argparse.ArgumentParser:
        return commands.add_parser(name, help=help, description=help, parents=[common, *parents])

    table = command("table", "Tokens and cost grouped by project, date, model, session or "
                             "execution, over --since (default 7d)", usage, limit)
    table.add_argument("--by", choices=list(QUERIES), default="project",
                       help="Group results by (default: project)")
    for name, help in (("chart", "Save a PNG chart of cost by tool per day, or per week beyond two months, over --since (default 7d)"),
                       ("share", "Save the chart, copy it to the clipboard, and open X to post it")):
        chart = command(name, help, usage, plans, output)
        cost = chart.add_mutually_exclusive_group()
        cost.add_argument("--monthly", action="store_true",
                          help="Show the full monthly subscription cost")
        cost.add_argument("--prorated", action="store_true",
                          help="Show the subscription cost pro-rated to the period")
    command("regime", "Detect and plot changes in the cache hit rate over --since", usage, output)
    sql = command("sql", "Run SQL against the 'tokens' table, over --since if given", usage)
    sql.add_argument("query", help="SQL to run, such as \"SELECT count(*) FROM tokens\"")
    diagnose = command("diagnose", "What changed in the --since period (default 3d) against "
                                   "the --vs period before it", usage, limit, as_json)
    diagnose.add_argument("--vs", type=duration, metavar="DURATION",
                          help="The baseline period before --since (default: 7d)")
    command("cache", "Prompt-cache health over --since (default 7d)", usage, limit, as_json)
    command("throughput", "Output tokens per second by day over --since (default 7d)",
            usage, as_json)
    pace = command("pace", "Compare quota-delta and local token-derived pace", quota_db, as_json)
    pace.add_argument("periods", nargs="*", type=duration, metavar="PERIOD",
                      help="Pace periods such as 3h or 7h (default: the standard set)")
    pace.add_argument("--all-buckets", action="store_true",
                      help="Include quota buckets with no activity in the periods")
    pace.add_argument("--model-mix", action="store_true", help="Include the model mix")
    command("value", "What each subscription's latest week is worth at API prices",
            quota_db, as_json, plans)
    command("quota-history", "Audit API-equivalent usage across every observed fixed quota "
                             "period", quota_db, as_json)
    command("record-quota", "Fetch and retain current provider quota percentages", quota_db)

    argv = sys.argv[1:] if argv is None else list(argv)
    args = parser.parse_args(_modernize_argv(argv, set(commands.choices)))

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
