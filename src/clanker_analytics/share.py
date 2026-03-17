"""Generate shareable PNG card with usage chart."""

import subprocess
import sys
import urllib.parse
from pathlib import Path

import duckdb
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# Website palette
BG = "#040506"
TEXT = "#b6aa99"
ACCENT = "#ff9800"
DIM = "#6b5d4f"
LIGHT = "#e0e0e0"

TOOL_COLORS = {
    "Claude Code": "#d97757",
    "Codex": "#10a37f",
    "Gemini": "#4285f4",
}

COST_SQL = """
    CASE
        WHEN tool = 'Codex' THEN
            (input_tokens * 1.25 + cache_write_tokens * 1.25
             + cache_read_tokens * 0.125 + output_tokens * 10.0) / 1e6
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

FONT_PATH = Path(__file__).parent / "D2CodingLigature.ttf"
OUTPUT = Path.home() / ".cache" / "clanker-analytics" / "share.png"


def _font(size: int, bold: bool = False) -> dict:
    return {"fontproperties": fm.FontProperties(fname=FONT_PATH, size=size,
                                                weight="bold" if bold else "normal")}


def _fmt_cost(n: float) -> str:
    if n >= 1000:
        return f"${n:,.0f}"
    if n >= 100:
        return f"${n:.0f}"
    if n >= 10:
        return f"${n:.1f}"
    return f"${n:.2f}"


def _fmt_tokens(n: int) -> str:
    if n >= 1_000_000_000:
        v = n / 1e9
        return f"{v:.0f}B" if v == int(v) else f"{v:.1f}B"
    if n >= 1_000_000:
        v = n / 1e6
        return f"{v:.0f}M" if v == int(v) else f"{v:.1f}M"
    if n >= 1_000:
        v = n / 1e3
        return f"{v:.0f}k" if v == int(v) else f"{v:.1f}k"
    return str(n)


def _short_date(d: str) -> str:
    """'2026-03-15' -> 'Mar 15'"""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    parts = d.split("-")
    return f"{months[int(parts[1]) - 1]} {int(parts[2])}"


def generate(db: duckdb.DuckDBPyConnection, since_label: str | None,
             plans: dict | None = None) -> Path:
    """Generate share card PNG. Returns path to output file."""
    plt.rcParams['text.parse_math'] = False

    # Get totals
    totals = db.sql(f"""
        SELECT sum(total_tokens)::BIGINT,
               (sum(total_tokens) - 0.9 * sum(cache_read_tokens))::BIGINT,
               count(DISTINCT tool)::INT,
               count(DISTINCT project)::INT,
               sum({COST_SQL})
        FROM tokens
    """).fetchone()
    total_tokens, billable_tokens, n_tools, n_projects, total_cost = totals
    total_tokens = total_tokens or 0
    billable_tokens = billable_tokens or 0
    total_cost = total_cost or 0
    if total_tokens == 0:
        print("  No data in selected range.")
        return OUTPUT

    # Get api cost per tool
    tool_costs = db.sql(f"""
        SELECT tool, sum({COST_SQL}) as cost
        FROM tokens GROUP BY tool ORDER BY cost DESC
    """).fetchall()

    # Get chart data — use per-project bars for short ranges, daily area for longer
    n_dates = db.sql("SELECT count(DISTINCT date) FROM tokens").fetchone()[0]
    use_bars = n_dates <= 3

    if use_bars:
        chart_data = db.sql(f"""
            WITH top AS (
                SELECT project FROM tokens
                GROUP BY project ORDER BY sum({COST_SQL}) DESC LIMIT 10
            )
            SELECT t.project, t.tool, sum({COST_SQL}) as cost
            FROM tokens t JOIN top ON t.project = top.project
            GROUP BY t.project, t.tool
            ORDER BY sum({COST_SQL}) DESC
        """).fetchall()
        projects = list(dict.fromkeys(p for p, _, _ in chart_data))
        tools = sorted(set(t for _, t, _ in chart_data))
        tool_proj = {t: {} for t in tools}
        for p, t, cost in chart_data:
            tool_proj[t][p] = cost
    else:
        daily = db.sql(f"""
            SELECT date, tool, sum({COST_SQL}) as cost
            FROM tokens GROUP BY date, tool ORDER BY date
        """).fetchall()
        tools = sorted(set(t for _, t, _ in daily))
        dates = sorted(set(d for d, _, _ in daily))
        tool_data = {t: {} for t in tools}
        for d, t, cost in daily:
            tool_data[t][d] = cost

    # Build figure
    fig, ax = plt.subplots(figsize=(12, 6.75), dpi=100)
    fig.set_facecolor(BG)
    ax.set_facecolor(BG)

    if use_bars and projects:
        x = np.arange(len(projects))
        bar_width = 0.6
        bottoms = np.zeros(len(projects))
        for t in tools:
            vals = np.array([tool_proj[t].get(p, 0) for p in projects], dtype=float)
            color = TOOL_COLORS.get(t, "#c4862c")
            ax.bar(x, vals, bar_width, bottom=bottoms, color=color, alpha=0.8)
            bottoms += vals
        ax.set_xticks(x)
        ax.set_xticklabels(projects, rotation=0, ha="center", **_font(9))
    elif not use_bars and dates:
        x = np.arange(len(dates))
        bottoms = np.zeros(len(dates))
        for t in tools:
            vals = np.array([tool_data[t].get(d, 0) for d in dates], dtype=float)
            color = TOOL_COLORS.get(t, "#c4862c")
            ax.fill_between(x, bottoms, bottoms + vals,
                            color=color, alpha=0.8, linewidth=0)
            ax.plot(x, bottoms + vals, color=color, alpha=0.9, linewidth=1)
            bottoms += vals

        if len(dates) <= 14:
            ax.set_xticks(x)
            ax.set_xticklabels([_short_date(d) for d in dates],
                               rotation=0, ha="center", **_font(9))
        else:
            step = max(1, len(dates) // 10)
            ticks = list(range(0, len(dates), step))
            ax.set_xticks(ticks)
            ax.set_xticklabels([_short_date(dates[i]) for i in ticks],
                               rotation=0, ha="center", **_font(9))

    # Y axis as dollars
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: _fmt_cost(v) if v > 0 else ""))

    ax.tick_params(colors=DIM, which="both")
    for label in ax.get_yticklabels():
        label.set_fontproperties(fm.FontProperties(fname=FONT_PATH, size=9))
        label.set_color(DIM)
    for label in ax.get_xticklabels():
        label.set_color(DIM)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="y", color=DIM, alpha=0.3, linewidth=0.5)

    # No legend — tool names in subtitle are color-coded instead

    # Headline: the savings angle
    period = "today" if since_label in ("24h", "1d") else (
        "this week" if since_label in ("7d", "1w") else "")
    sub_cost = sum(c for _, c in (plans or {}).values())
    sub_label = f" for ${sub_cost}/mo" if sub_cost else ""
    headline = f"{_fmt_cost(total_cost)} of AI compute{f' {period}' if period else ''}{sub_label}"
    fig.text(0.05, 0.95, headline, color=LIGHT, **_font(22, bold=True),
             ha="left", va="top")

    # Subtitle: colored tool names as legend
    renderer = fig.canvas.get_renderer()
    fig_width = fig.get_window_extent(renderer=renderer).width
    x_pos = 0.05
    for i, (t, c) in enumerate(tool_costs):
        if i > 0:
            sep = fig.text(x_pos, 0.89, "  |  ", color=DIM, **_font(11),
                           ha="left", va="top")
            fig.canvas.draw()
            x_pos += sep.get_window_extent(renderer=renderer).width / fig_width
        # Tool name in its color, with plan if known
        color = TOOL_COLORS.get(t, "#c4862c")
        plan_info = plans.get(t) if plans else None
        label = f"{t} ({plan_info[0]})" if plan_info else t
        name_txt = fig.text(x_pos, 0.89, label, color=color, **_font(11, bold=True),
                            ha="left", va="top")
        fig.canvas.draw()
        x_pos += name_txt.get_window_extent(renderer=renderer).width / fig_width
        # Cost in default text color
        cost_txt = fig.text(x_pos, 0.89, f" {_fmt_cost(c)}", color=TEXT, **_font(11),
                            ha="left", va="top")
        fig.canvas.draw()
        x_pos += cost_txt.get_window_extent(renderer=renderer).width / fig_width

    fig.text(x_pos, 0.89, f"  |  {n_projects} projects", color=DIM, **_font(11),
             ha="left", va="top")

    # Top-right: command + token count
    since_arg = f" --since {since_label}" if since_label else ""
    fig.text(0.95, 0.95, f"uvx clanker-analytics{since_arg} --chart", color=DIM,
             **_font(11), ha="right", va="top")
    token_line = f"{_fmt_tokens(total_tokens)} tokens ({_fmt_tokens(billable_tokens)} billable)"
    fig.text(0.95, 0.90, token_line, color=DIM, **_font(11), ha="right", va="top")

    plt.tight_layout(rect=[0, 0, 1, 0.85])
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)

    return OUTPUT


def copy_and_open(path: Path, total_cost: float, since_label: str | None,
                  sub_cost: int = 0):
    """Copy PNG to clipboard and open X compose window."""
    # Copy to clipboard
    copied = False
    if sys.platform == "darwin":
        copied = subprocess.run(["osascript", "-e",
                        f'set the clipboard to (read (POSIX file "{path}") as «class PNGf»)'],
                       check=False, capture_output=True).returncode == 0
    elif sys.platform == "win32":
        try:
            # PowerShell: Set-Clipboard with image
            copied = subprocess.run(
                ["powershell", "-Command",
                 f"Add-Type -AssemblyName System.Windows.Forms; "
                 f"[System.Windows.Forms.Clipboard]::SetImage("
                 f"[System.Drawing.Image]::FromFile('{path}'))"],
                check=False, capture_output=True).returncode == 0
        except FileNotFoundError:
            pass
    elif sys.platform == "linux":
        for cmd in [["wl-copy", "--type", "image/png"],
                    ["xclip", "-selection", "clipboard", "-t", "image/png"]]:
            try:
                with open(path, "rb") as f:
                    if subprocess.run(cmd, stdin=f, check=False, capture_output=True).returncode == 0:
                        copied = True
                        break
            except FileNotFoundError:
                continue

    period = "today" if since_label in ("24h", "1d") else (
        "this week" if since_label in ("7d", "1w") else "")
    sub_label = f" for ${sub_cost}/mo" if sub_cost else ""
    text = f"{_fmt_cost(total_cost)} of AI compute {period}{sub_label}\nuvx clanker-analytics"

    url = "https://x.com/intent/tweet?" + urllib.parse.urlencode({"text": text})
    print(f"\n  Card saved to {path}")
    if copied:
        print(f"  Copied to clipboard — paste into tweet")

    opened = False
    if sys.platform == "darwin":
        opened = subprocess.run(["open", url], check=False, capture_output=True).returncode == 0
    elif sys.platform == "win32":
        opened = subprocess.run(["cmd", "/c", "start", url], check=False, capture_output=True).returncode == 0
    elif sys.platform == "linux":
        opened = subprocess.run(["xdg-open", url], check=False, capture_output=True).returncode == 0

    if opened:
        print(f"  Opened X compose window")
    else:
        print(f"  Share on X: {url}")
