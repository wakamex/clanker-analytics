# clanker-analytics

Token usage analytics for AI coding tools. Reads local session logs and shows per-project breakdowns using DuckDB.

Supports **Claude Code**, **Codex**, and **Gemini CLI**.

![clanker-analytics chart](share.png)

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

## Table view

Use `--table` for a tabular breakdown:

```
$ clanker-analytics --table --since 7d --limit 10
┌─────────────┬─────────────┬───────┬─────────┬──────────┬─────────┬─────────┬──────────┬────────────┬────────────┐
│   project   │    tool     │ turns │  total  │ billable │ output  │  cache  │ api_cost │ first_seen │ last_seen  │
├─────────────┼─────────────┼───────┼─────────┼──────────┼─────────┼─────────┼──────────┼────────────┼────────────┤
│ *           │ *           │ 60060 │    9.1B │    1.2B  │   18.3M │  97%    │ $9802    │ 2026-03-10 │ 2026-03-17 │
│ *           │ Codex       │ 37129 │    5.5B │  753.1M  │   13.8M │  96%    │ $7662    │ 2026-03-10 │ 2026-03-17 │
│ *           │ Claude Code │ 20303 │    3.6B │  417.9M  │    4.3M │  98%    │ $2089    │ 2026-03-10 │ 2026-03-17 │
│ *           │ Gemini      │  2628 │   36.4M │   18.2M  │  205.2k │  56%    │ $50.7    │ 2026-03-10 │ 2026-03-17 │
│ tao-auto    │ Codex       │ 12587 │    1.9B │  250.9M  │    4.1M │  97%    │ $2694    │ 2026-03-10 │ 2026-03-16 │
│ tao         │ Codex       │ 10670 │    1.8B │  245.0M  │    4.5M │  96%    │ $2470    │ 2026-03-10 │ 2026-03-16 │
│ wezterm     │ Claude Code │  2237 │    1.2B │  132.5M  │  410.3k │  99%    │ $692     │ 2026-03-13 │ 2026-03-15 │
│ wildlands   │ Codex       │  5515 │  714.8M │   94.8M  │    1.7M │  97%    │ $994     │ 2026-03-14 │ 2026-03-16 │
│ tao-perf    │ Codex       │  4960 │  673.0M │   86.4M  │    1.8M │  97%    │ $939     │ 2026-03-10 │ 2026-03-12 │
│ tao-sprites │ Claude Code │  5954 │  609.5M │   72.1M  │    1.4M │  98%    │ $247     │ 2026-03-10 │ 2026-03-16 │
└─────────────┴─────────────┴───────┴─────────┴──────────┴─────────┴─────────┴──────────┴────────────┴────────────┘
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

## Environmental impact estimates

The `--chart` / `--share` output shows estimated environmental impact per million tokens:

| Metric | Per 1M tokens | Source |
|---|---|---|
| Electricity | 0.6 kWh | [Epoch AI](https://epoch.ai/gradient-updates/how-much-energy-does-chatgpt-use), [arxiv:2505.09598](https://arxiv.org/abs/2505.09598) |
| Water | 1 liter | [Li & Ren (2023)](https://cacm.acm.org/sustainability-and-computing/making-ai-less-thirsty/), adjusted for modern models |
| CO2 | 90 g | [Ritchie (2025)](https://hannahritchie.substack.com/p/ai-footprint-august-2025) |

These are rough estimates — actual impact varies 10-100x depending on model, hardware, and data center location. No provider publishes official per-token figures.

## Chart colors

Brand colors used in `--chart` / `--share` output:

| Tool | Color | Source |
|---|---|---|
| Claude Code | `#d97757` | [Anthropic brand guidelines](https://github.com/anthropics/skills/blob/main/skills/brand-guidelines/SKILL.md) |
| Codex | `#10a37f` | [OpenAI brand](https://openai.com) |
| Gemini | `#4285f4` | [Google brand](https://about.google/brand-resource-center/) |

## Requirements

Python 3.13+, DuckDB 1.5+, matplotlib 3.9+.

Tested on Linux, macOS, and Windows (including WSL data auto-discovery).
