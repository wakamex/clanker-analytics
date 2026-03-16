"""Generate shareable PNG card with usage chart."""

import subprocess
import sys
from datetime import datetime
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
DIM = "#3d3428"
LIGHT = "#e0e0e0"

TOOL_COLORS = {
    "Claude Code": ACCENT,
    "Codex": "#6b8f5e",
}

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


def generate(db: duckdb.DuckDBPyConnection, since_label: str | None) -> Path:
    """Generate share card PNG. Returns path to output file."""
    # Get totals
    totals = db.sql("""
        SELECT sum(total_tokens)::BIGINT,
               (sum(total_tokens) - 0.9 * sum(cache_read_tokens))::BIGINT,
               count(DISTINCT tool)::INT,
               count(DISTINCT project)::INT
        FROM tokens
    """).fetchone()
    total_tokens, billable_tokens, n_tools, n_projects = totals

    # Get api cost per tool
    tool_costs = db.sql("""
        SELECT tool, sum(
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
        ) as cost
        FROM tokens GROUP BY tool ORDER BY cost DESC
    """).fetchall()
    total_cost = sum(c for _, c in tool_costs)

    # Get daily data per tool for chart
    daily = db.sql("""
        SELECT date, tool, sum(total_tokens)::BIGINT as tokens
        FROM tokens
        GROUP BY date, tool
        ORDER BY date
    """).fetchall()

    # Pivot into per-tool series
    tools = sorted(set(t for _, t, _ in daily))
    dates_set = sorted(set(d for d, _, _ in daily))
    tool_data = {t: {} for t in tools}
    for d, t, tok in daily:
        tool_data[t][d] = tok

    dates = dates_set
    series = {}
    for t in tools:
        series[t] = [tool_data[t].get(d, 0) for d in dates]

    # Build figure
    fig, ax = plt.subplots(figsize=(12, 6.75), dpi=100)
    fig.set_facecolor(BG)
    ax.set_facecolor(BG)

    # Stacked area chart
    if dates and any(any(v > 0 for v in s) for s in series.values()):
        x = np.arange(len(dates))
        bottoms = np.zeros(len(dates))
        for t in tools:
            vals = np.array(series[t], dtype=float)
            color = TOOL_COLORS.get(t, "#c4862c")
            ax.fill_between(x, bottoms, bottoms + vals, label=t,
                            color=color, alpha=0.8, linewidth=0)
            ax.plot(x, bottoms + vals, color=color, alpha=0.9, linewidth=1)
            bottoms += vals

        # X axis labels
        if len(dates) <= 14:
            ax.set_xticks(x)
            ax.set_xticklabels(dates, rotation=45, ha="right", **_font(9))
        else:
            step = max(1, len(dates) // 10)
            ticks = list(range(0, len(dates), step))
            ax.set_xticks(ticks)
            ax.set_xticklabels([dates[i] for i in ticks], rotation=45, ha="right",
                               **_font(9))

        # Y axis formatting
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(
            lambda v, _: _fmt_tokens(int(v))))

    ax.tick_params(colors=DIM, which="both")
    for label in ax.get_yticklabels():
        label.set_fontproperties(fm.FontProperties(fname=FONT_PATH, size=9))
        label.set_color(DIM)
    for label in ax.get_xticklabels():
        label.set_color(DIM)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="y", color=DIM, alpha=0.3, linewidth=0.5)

    # Legend
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        leg = ax.legend(handles, labels, loc="lower left",
                        frameon=False, prop=fm.FontProperties(fname=FONT_PATH, size=10))
        for t in leg.get_texts():
            t.set_color(TEXT)

    # Headline text overlay
    period = f" today" if since_label in ("24h", "1d") else (
        f" this week" if since_label in ("7d", "1w") else "")

    headline = f"{_fmt_cost(total_cost)} of AI tokens burned{period}"
    fig.text(0.05, 0.95, headline, color=LIGHT, **_font(22, bold=True),
             ha="left", va="top")

    # Subtitle - render each tool separately to avoid font spacing issues
    x_pos = 0.05
    for i, (t, c) in enumerate(tool_costs):
        label = f"{t}: {_fmt_cost(c)}"
        if i > 0:
            fig.text(x_pos, 0.89, "  |  ", color=DIM, **_font(11), ha="left", va="top")
            x_pos += 0.04
        txt = fig.text(x_pos, 0.89, label, color=TEXT, **_font(11), ha="left", va="top")
        fig.canvas.draw()
        bbox = txt.get_window_extent(renderer=fig.canvas.get_renderer())
        x_pos += bbox.width / fig.get_window_extent().width + 0.005
    fig.text(x_pos, 0.89, f"  |  {n_projects} projects", color=DIM, **_font(11),
             ha="left", va="top")

    # Token count in corner
    token_line = f"{_fmt_tokens(total_tokens)} tokens ({_fmt_tokens(billable_tokens)} billable)"
    fig.text(0.95, 0.95, token_line, color=DIM, **_font(10), ha="right", va="top")

    # Watermark
    fig.text(0.95, 0.02, "clanker-analytics", color=DIM, **_font(9),
             ha="right", va="bottom")

    plt.tight_layout(rect=[0, 0, 1, 0.85])
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)

    return OUTPUT


def copy_and_open(path: Path, total_cost: float, since_label: str | None):
    """Copy PNG to clipboard and open X compose window."""
    # Copy to clipboard
    copied = False
    if sys.platform == "darwin":
        copied = subprocess.run(["osascript", "-e",
                        f'set the clipboard to (read (POSIX file "{path}") as «class PNGf»)'],
                       check=False, capture_output=True).returncode == 0
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
    text = f"burned {_fmt_cost(total_cost)} of AI tokens {period}\nclanker-analytics --since {since_label or '24h'}"

    import urllib.parse
    url = "https://x.com/intent/tweet?" + urllib.parse.urlencode({"text": text})
    print(f"\n  Card saved to {path}")
    if copied:
        print(f"  Copied to clipboard")
    print(f"  Opening X: {url}")

    if sys.platform == "darwin":
        subprocess.run(["open", url], check=False)
    elif sys.platform == "linux":
        subprocess.run(["xdg-open", url], check=False)
