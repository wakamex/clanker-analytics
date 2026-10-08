"""Generate shareable PNG card with usage chart."""

import subprocess
import sys
from datetime import date, timedelta
import urllib.parse
from pathlib import Path

import duckdb
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# Website palette
BG = "#040506"
TEXT = "#d4c8b8"
ACCENT = "#ff9800"
DIM = "#6b5d4f"
LIGHT = "#e0e0e0"

TOOL_COLORS = {
    "Claude Code": "#d97757",
    "Codex": "#10a37f",
    "Gemini": "#4285f4",
    "Agy": "#a142f4",
    "Cursor": "#7c5cff",
    "Devin": "#7cae5f",
    "DeepSeek Harness": "#4d6bfe",
    "Grok": "#c0c0c0",
    "Hermes": "#ff6b35",
    "OpenCode": "#f0b429",
}
# Ranges longer than this many days draw one bar per week instead of per day.
DAILY_BAR_DAYS = 62
# Tools under 1% of the period's cost share one segment and one legend entry.
OTHER = "Other"
OTHER_COLOR = "#7a7a7a"

from clanker_analytics.main import COST_PER_ROW as COST_SQL

FONT_PATH = Path(__file__).parent / "ClankerMono.ttf"
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


def _color(tool: str) -> str:
    return OTHER_COLOR if tool == OTHER else TOOL_COLORS.get(tool, "#c4862c")


def _stack(ax, x, series: dict[str, np.ndarray], width: float,
           partial: int | None = None, horizontal: bool = False) -> None:
    """Stacked bars, first series at the bottom, with a background-colored gap between
    segments so each tool's share stays legible. The bar at `partial`, a period still in
    progress, is hatched."""
    bottoms = np.zeros(len(x))
    plt.rcParams["hatch.color"] = BG
    plt.rcParams["hatch.linewidth"] = 1.5
    for t, vals in series.items():
        draw = ax.barh if horizontal else ax.bar
        bars = draw(x, vals, width, bottoms, color=_color(t), edgecolor=BG, linewidth=1)
        if partial is not None:
            bars[partial].set_hatch("//")
        bottoms += vals


def _plot_periods(ax, periods: list[date], step: int,
                  series: dict[str, np.ndarray]) -> str | None:
    """Cost as one stacked bar per day, or per week starting Monday when step is 7.

    Returns a note explaining the hatched bar of a period still in progress, if any."""
    x = np.arange(len(periods))
    today = date.today()
    partial = len(periods) - 1 if periods[-1] + timedelta(step) > today else None
    _stack(ax, x, series, width=0.8, partial=partial)
    if step == 7:
        # A label at the first week of each month.
        ticks = []
        for i, d in enumerate(periods):
            # Skip a month start too close to the previous label to fit beside it.
            if (i == 0 or d.month != periods[i - 1].month) and (not ticks or i - ticks[-1] > 2):
                ticks.append(i)
        labels = [periods[i].strftime("%b '%y" if periods[i].month == 1 or i == ticks[0]
                                      else "%b") for i in ticks]
    else:
        # A label for every day up to two weeks, then one each Monday.
        ticks = (list(x) if len(periods) <= 14
                 else [i for i, d in enumerate(periods) if d.weekday() == 0])
        labels = [_short_date(periods[i].isoformat()) for i in ticks]
    ax.set_xticks(ticks)
    ax.set_xticklabels(labels, rotation=0, ha="center", **_font(11))
    ax.set_xlim(-0.6, len(periods) - 0.4)
    if partial is not None:
        return f"hatched: {'today' if step == 1 else 'this week'} so far"
    return None


def generate(db: duckdb.DuckDBPyConnection, since_label: str | None,
             plans: dict | None = None, cost_mode: str = "auto",
             output: Path | None = None) -> Path:
    """Generate share card PNG at `output`, by default OUTPUT. Returns its path."""
    plt.rcParams['text.parse_math'] = False

    # Get totals
    totals = db.sql(f"""
        SELECT sum(total_tokens)::BIGINT,
               (sum(total_tokens) - 0.9 * sum(cache_read_tokens))::BIGINT,
               count(DISTINCT tool)::INT,
               count(DISTINCT project)::INT,
               sum({COST_SQL}),
               count(*) FILTER (WHERE token_count_type != 'exact') > 0
        FROM tokens
    """).fetchone()
    total_tokens, billable_tokens, n_tools, n_projects, total_cost, has_estimates = totals
    total_tokens = total_tokens or 0
    billable_tokens = billable_tokens or 0
    total_cost = total_cost or 0
    if total_tokens == 0:
        print("  No data in selected range.")
        return None

    # Get api cost per tool
    tool_costs = db.sql(f"""
        SELECT tool, sum({COST_SQL}) as cost,
               count(*) FILTER (WHERE token_count_type != 'exact') > 0 as estimated
        FROM tokens GROUP BY tool ORDER BY cost DESC
    """).fetchall()

    # Largest tools first, at the bottom of each stack; the rest fold into Other.
    shown = [item for item in tool_costs if item[1] >= 0.01 * total_cost] or tool_costs[:1]
    folded = tool_costs[len(shown):]
    order = [t for t, _, _ in shown] + ([OTHER] if folded else [])
    group = {t: (t if t in order else OTHER) for t, _, _ in tool_costs}

    # Get chart data — use per-project bars for short ranges, daily bars for longer
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
        tool_proj = {t: {} for t in order}
        for p, t, cost in chart_data:
            tool_proj[group[t]][p] = tool_proj[group[t]].get(p, 0) + cost
    else:
        daily = db.sql(f"""
            SELECT date, tool, sum({COST_SQL}) as cost
            FROM tokens GROUP BY date, tool ORDER BY date
        """).fetchall()
        # Every day or week from the first to the last, so idle ones show as gaps.
        first, last = (date.fromisoformat(d) for d in (daily[0][0], daily[-1][0]))
        step = 1 if (last - first).days < DAILY_BAR_DAYS else 7
        first -= timedelta((first.weekday() if step == 7 else 0))
        periods = [first + timedelta(n) for n in range(0, (last - first).days + 1, step)]
        series = {t: np.zeros(len(periods)) for t in order}
        for d, t, cost in daily:
            series[group[t]][(date.fromisoformat(d) - first).days // step] += cost

    # Build figure
    fig, ax = plt.subplots(figsize=(12, 6.75), dpi=100)
    fig.set_facecolor(BG)
    ax.set_facecolor(BG)

    note = None
    if use_bars and projects:
        # Horizontal, so project names of any length get their own column.
        y = np.arange(len(projects))
        _stack(ax, y, {t: np.array([tool_proj[t].get(p, 0) for p in projects], dtype=float)
                       for t in order}, width=0.6, horizontal=True)
        ax.set_yticks(y)
        ax.set_yticklabels(projects, **_font(11))
        ax.invert_yaxis()
    elif not use_bars:
        note = _plot_periods(ax, periods, step, series)

    # Value axis as dollars, whole unless the ticks fall between them
    value_axis = ax.xaxis if use_bars else ax.yaxis
    value_axis.set_major_formatter(ticker.FuncFormatter(
        lambda v, _: (f"${v:,.0f}" if v == round(v) else f"${v:,.2f}") if v > 0 else ""))

    ax.tick_params(colors=TEXT, which="both")
    for label in [*ax.get_yticklabels(), *ax.get_xticklabels()]:
        label.set_fontproperties(fm.FontProperties(fname=FONT_PATH, size=11))
        label.set_color(TEXT)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="x" if use_bars else "y", color=DIM, alpha=0.3, linewidth=0.5)

    # No legend — tool names in subtitle are color-coded instead

    # Headline: the savings angle
    period = "today" if since_label in ("24h", "1d") else (
        "this week" if since_label in ("7d", "1w") else "")
    sub_cost = sum(c for _, c in (plans or {}).values())

    # If usage < subscription: show monthly (funny: overpaying)
    # If usage >= subscription: pro-rate to period (funny: insane value)
    if sub_cost:
        # Parse days from since_label
        days = 30  # default to monthly
        if since_label:
            import re as _re
            m = _re.fullmatch(r'(\d+)([hdw])', since_label)
            if m:
                n, unit = int(m.group(1)), m.group(2)
                days = {"h": n / 24, "d": n, "w": n * 7}[unit]

        prorated = sub_cost * days / 30
        if cost_mode == "monthly" or (cost_mode == "auto" and total_cost < sub_cost):
            sub_label = f" for ${sub_cost}/mo"
        else:
            sub_label = f" for {_fmt_cost(prorated)}"
    else:
        sub_label = ""
    # Environmental impact
    kwh = billable_tokens * 0.6 / 1e6
    liters = billable_tokens * 1.0 / 1e6
    co2_kg = billable_tokens * 90 / 1e9

    env_parts = []
    if kwh >= 1:
        env_parts.append(f"{kwh:,.0f}kWh")
    elif kwh >= 0.01:
        env_parts.append(f"{kwh:.1f}kWh")
    if liters >= 1000:
        env_parts.append(f"{liters / 1000:,.1f}m\u00b3")
    elif liters >= 1:
        env_parts.append(f"{liters:,.0f}L")
    if co2_kg >= 1:
        env_parts.append(f"{co2_kg:,.0f}kg")
    elif co2_kg >= 0.01:
        env_parts.append(f"{co2_kg:.1f}kg")

    # Giant headline: dollar amount + eco stats on same line
    renderer = fig.canvas.get_renderer()
    fig_width = fig.get_window_extent(renderer=renderer).width

    cost_label = ("~" if has_estimates else "") + _fmt_cost(total_cost)
    cost_txt = fig.text(0.05, 0.97, cost_label, color=LIGHT,
                        **_font(42, bold=True), ha="left", va="top")
    fig.canvas.draw()
    x_after_cost = 0.05 + cost_txt.get_window_extent(renderer=renderer).width / fig_width + 0.02
    if env_parts:
        from matplotlib.offsetbox import OffsetImage, AnnotationBbox
        from matplotlib.image import imread as mpl_imread
        emoji_dir = Path(__file__).parent / "emoji"
        emoji_map = {"kWh": "zap.png", "m\u00b3": "sweat_droplets.png", "L": "sweat_droplets.png", "kg": "factory.png"}

        ex = x_after_cost
        for part in env_parts:
            # Find which emoji to use
            emoji_file = None
            for suffix, fname in emoji_map.items():
                if suffix in part:
                    emoji_file = emoji_dir / fname
                    break

            if emoji_file and emoji_file.exists():
                img = mpl_imread(str(emoji_file))
                im = OffsetImage(img, zoom=0.45)
                ex += 0.01  # space before emoji
                ab = AnnotationBbox(im, (ex, 0.955), xycoords='figure fraction',
                                    frameon=False, box_alignment=(0, 0.5))
                fig.add_artist(ab)
                ex += 0.03

            txt = fig.text(ex, 0.97, part, color=TEXT,
                           **_font(42, bold=True), ha="left", va="top")
            fig.canvas.draw()
            ex += txt.get_window_extent(renderer=renderer).width / fig_width + 0.015

    # Second line: context
    multiplier = ""
    if sub_cost and total_cost >= sub_cost:
        ratio = total_cost / (sub_cost * days / 30) if days else 0
        if ratio >= 2:
            multiplier = f" ({ratio:.0f}x)"
    estimate_note = " (includes processed-token estimates)" if has_estimates else ""
    context = f"of AI compute{f' {period}' if period else ''}{sub_label}{multiplier}{estimate_note}"
    fig.text(0.05, 0.87, context, color=TEXT, **_font(14),
             ha="left", va="top")

    # Third line: colored tool names as legend, in stack order, with Other last.
    x_pos = 0.05
    for i, (t, c, estimated) in enumerate(shown):
        if i > 0:
            sep = fig.text(x_pos, 0.825, "  |  ", color=TEXT, **_font(12),
                           ha="left", va="top")
            fig.canvas.draw()
            x_pos += sep.get_window_extent(renderer=renderer).width / fig_width
        color = TOOL_COLORS.get(t, "#c4862c")
        plan_info = plans.get(t) if plans else None
        details = []
        if plan_info:
            details.append(plan_info[0])
        if estimated:
            details.append("processed estimate")
        label = f"{t} ({', '.join(details)})" if details else t
        name_txt = fig.text(x_pos, 0.825, label, color=color, **_font(12, bold=True),
                            ha="left", va="top")
        fig.canvas.draw()
        x_pos += name_txt.get_window_extent(renderer=renderer).width / fig_width
        cost_prefix = "~" if estimated else ""
        cost_txt = fig.text(x_pos, 0.825, f" {cost_prefix}{_fmt_cost(c)}", color=TEXT, **_font(12),
                            ha="left", va="top")
        fig.canvas.draw()
        x_pos += cost_txt.get_window_extent(renderer=renderer).width / fig_width

    if folded:
        for text, color, bold in (("  |  ", TEXT, False),
                                  (f"{OTHER} ({len(folded)} tools)", OTHER_COLOR, True),
                                  (f" {_fmt_cost(sum(c for _, c, _ in folded))}", TEXT, False)):
            part = fig.text(x_pos, 0.825, text, color=color, **_font(12, bold=bold),
                            ha="left", va="top")
            fig.canvas.draw()
            x_pos += part.get_window_extent(renderer=renderer).width / fig_width
    fig.text(x_pos, 0.825, f"  |  {n_projects} projects", color=TEXT, **_font(12),
             ha="left", va="top")

    # Footer: the command that draws this chart, with --since when it is not the default
    cmd = "uvx clanker-analytics chart"
    if since_label and since_label != "7d":
        cmd += f" --since {since_label}"
    fig.text(0.95, 0.01, cmd, color=DIM, **_font(11), ha="right", va="bottom")
    if note:
        fig.text(0.05, 0.01, note, color=DIM, **_font(11), ha="left", va="bottom")

    plt.tight_layout(rect=[0, 0.04, 1, 0.78])
    output = output or OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)

    return output


def copy_and_open(path: Path, total_cost: float, since_label: str | None,
                  sub_cost: int = 0, cost_mode: str = "auto"):
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
    if sub_cost:
        import re as _re
        days = 30
        if since_label:
            m = _re.fullmatch(r'(\d+)([hdw])', since_label)
            if m:
                n, unit = int(m.group(1)), m.group(2)
                days = {"h": n / 24, "d": n, "w": n * 7}[unit]
        prorated = sub_cost * days / 30
        if (total_cost or 0) < sub_cost:
            sub_label = f" for ${sub_cost}/mo"
        else:
            sub_label = f" for {_fmt_cost(prorated)}"
    else:
        sub_label = ""
    text = f"{_fmt_cost(total_cost or 0)} of AI compute {period}{sub_label}\nuvx clanker-analytics"

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
