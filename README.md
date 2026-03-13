# clanker-analytics

Token usage analytics for AI coding tools. Reads local session logs and shows per-project breakdowns using DuckDB.

Supports **Claude Code** and **Codex**.

## Install

```
uv tool install clanker-analytics
```

Or run directly from the repo:

```
uv run clanker-analytics
```

## Usage

```
clanker-analytics                        # ranked by project (default)
clanker-analytics --by date              # daily totals
clanker-analytics --by model             # per-model breakdown
clanker-analytics --by session           # per-session breakdown
clanker-analytics --tool claude          # Claude Code only
clanker-analytics --tool codex           # Codex only
clanker-analytics --refresh              # force cache rebuild
clanker-analytics --sql "SELECT ..."     # custom SQL against 'tokens' table
```

## Example output

```
┌──────────────────┬─────────────┬───────┬─────────┬─────────┬─────────┬─────────┬────────────┬────────────┐
│     project      │    tool     │ turns │  total  │  input  │ output  │  cache  │ first_seen │ last_seen  │
├──────────────────┼─────────────┼───────┼─────────┼─────────┼─────────┼─────────┼────────────┼────────────┤
│ tao              │ Codex       │  7729 │    1.4B │    1.3B │    3.0M │  96%    │ 2026-03-07 │ 2026-03-12 │
│ gitrep           │ Claude Code │ 11949 │    1.1B │    1.1B │    1.0M │  98%    │ 2026-02-17 │ 2026-03-13 │
│ tao-auto         │ Codex       │  6253 │  903.2M │  901.5M │    1.7M │  97%    │ 2026-03-10 │ 2026-03-13 │
│ frame-modernized │ Claude Code │  9218 │  883.4M │  882.1M │    1.3M │  97%    │ 2026-02-24 │ 2026-03-06 │
│ tao-sprites      │ Claude Code │  7924 │  800.4M │  798.4M │    1.9M │  98%    │ 2026-03-09 │ 2026-03-13 │
└──────────────────┴─────────────┴───────┴─────────┴─────────┴─────────┴─────────┴────────────┴────────────┘

Codex: 5.7B across 43 projects
Claude Code: 7.7B across 94 projects
```

## How it works

DuckDB reads JSONL session logs directly from `~/.claude/projects/` and `~/.codex/sessions/` — no Python JSON parsing. Results are cached to `~/.cache/clanker-analytics/tokens.parquet` (ZSTD compressed) and auto-invalidated when source files change.

## Columns

- **total** — input + output + cache write + cache read tokens
- **input** — total minus output (includes cache tokens)
- **output** — output tokens only
- **cache** — cache read hits as % of input tokens

## Requirements

Python 3.13+, DuckDB 1.5+.
