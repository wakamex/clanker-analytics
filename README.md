# clanker-analytics

Token usage analytics for AI coding tools. Reads local session logs and shows per-project breakdowns using DuckDB.

Supports **Claude Code**, **Codex**, and **Gemini CLI**.

## Install

```
uv tool install clanker-analytics
```

Or run without installing:

```
uvx clanker-analytics
```

## Usage

```
clanker-analytics                        # ranked by project (default)
clanker-analytics --since 24h            # last 24 hours (also: 7d, 2w, 2026-03-01)
clanker-analytics --by date              # daily totals
clanker-analytics --by model             # per-model breakdown
clanker-analytics --by session           # per-session breakdown
clanker-analytics --tool claude          # Claude Code only
clanker-analytics --tool codex           # Codex only
clanker-analytics --tool gemini          # Gemini CLI only
clanker-analytics --chart                # generate PNG chart
clanker-analytics --share                # generate chart + open X compose
clanker-analytics --refresh              # force cache rebuild
clanker-analytics --sql "SELECT ..."     # custom SQL against 'tokens' table
```

## How it works

DuckDB reads session logs directly from `~/.claude/projects/`, `~/.codex/sessions/`, and `~/.gemini/tmp/` — no Python JSON parsing. Results are cached to `~/.cache/clanker-analytics/tokens.parquet` (ZSTD compressed) and auto-invalidated when source files change.

## Columns

- **total** — all tokens processed (input + output + cache write + cache read)
- **billable** — total minus the 90% cache read discount
- **output** — output tokens only
- **cache** — cache read hits as % of input tokens
- **api_cost** — estimated cost at API rates

## API cost calculation

The `api_cost` and `billable` columns use published API pricing. Cache reads are 0.1x the input token price for all three providers:

| | Input | Cache read | Cache write | Output |
|---|---|---|---|---|
| Claude Sonnet | $3/MTok | $0.30/MTok | $3.75/MTok | $15/MTok |
| Claude Opus | $5/MTok | $0.50/MTok | $6.25/MTok | $25/MTok |
| GPT-5 | $1.25/MTok | $0.125/MTok | (auto) | $10/MTok |
| Gemini 2.5 Pro | $1.25/MTok | $0.315/MTok | (auto) | $10/MTok |

Sources: [Anthropic pricing](https://docs.anthropic.com/en/docs/about-claude/pricing), [OpenAI pricing](https://openai.com/api/pricing/), [Google AI pricing](https://ai.google.dev/gemini-api/docs/pricing)

## Chart colors

Brand colors used in `--chart` / `--share` output:

| Tool | Color | Source |
|---|---|---|
| Claude Code | `#d97757` | [Anthropic brand guidelines](https://github.com/anthropics/skills/blob/main/skills/brand-guidelines/SKILL.md) |
| Codex | `#10a37f` | [OpenAI brand](https://openai.com) |
| Gemini | `#4285f4` | [Google brand](https://about.google/brand-resource-center/) |

## Requirements

Python 3.13+, DuckDB 1.5+, matplotlib 3.9+.
