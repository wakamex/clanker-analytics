"""Detect and visualize cache rate regime changes."""

import math
from pathlib import Path

import duckdb
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

FONT_PATH = Path(__file__).parent / "D2CodingLigature.ttf"
OUTPUT = Path.home() / ".cache" / "clanker-analytics" / "regime.png"

BG = "#040506"
TEXT = "#d4c8b8"
DIM = "#6b5d4f"
LIGHT = "#e0e0e0"
ACCENT = "#ff9800"
RED = "#e74c3c"
GREEN = "#2ecc71"


def _font(size: int, bold: bool = False) -> dict:
    return {"fontproperties": fm.FontProperties(fname=FONT_PATH, size=size,
                                                weight="bold" if bold else "normal")}


def _short_date(d: str) -> str:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    parts = d.split("-")
    return f"{months[int(parts[1]) - 1]} {int(parts[2])}"


def detect_and_plot(db: duckdb.DuckDBPyConnection, since_label: str | None) -> Path | None:
    """Detect cache rate regime change and generate a chart."""
    plt.rcParams['text.parse_math'] = False

    # Get daily cache rates per tool
    daily = db.sql("""
        SELECT date, tool,
               100.0 * sum(cache_read_tokens) / greatest(sum(total_tokens - output_tokens), 1) as cache_pct,
               count(*) as turns,
               sum(total_tokens)::BIGINT as total
        FROM tokens
        GROUP BY date, tool
        ORDER BY date
    """).fetchall()

    if len(daily) < 4:
        print("  Not enough data for regime detection (need at least 4 days).")
        return None

    # Aggregate across tools for overall cache rate
    overall = db.sql("""
        SELECT date,
               100.0 * sum(cache_read_tokens) / greatest(sum(total_tokens - output_tokens), 1) as cache_pct,
               count(*) as turns
        FROM tokens
        GROUP BY date
        ORDER BY date
    """).fetchall()

    dates = [r[0] for r in overall]
    cache_pcts = [r[1] for r in overall]
    turns = [r[2] for r in overall]

    if len(dates) < 4:
        print("  Not enough data for regime detection.")
        return None

    # Find optimal changepoint: maximize t-statistic
    best_t = 0
    best_idx = -1
    best_stats = None

    for i in range(2, len(dates) - 2):
        before = cache_pcts[:i]
        after = cache_pcts[i:]

        n_b, n_a = len(before), len(after)
        mean_b = sum(before) / n_b
        mean_a = sum(after) / n_a

        var_b = sum((x - mean_b) ** 2 for x in before) / max(n_b - 1, 1)
        var_a = sum((x - mean_a) ** 2 for x in after) / max(n_a - 1, 1)

        se = math.sqrt(var_b / n_b + var_a / n_a) if (var_b + var_a) > 0 else 1
        t = abs(mean_b - mean_a) / se

        if t > best_t:
            best_t = t
            best_idx = i
            best_stats = {
                "date": dates[i],
                "mean_before": mean_b,
                "mean_after": mean_a,
                "n_before": n_b,
                "n_after": n_a,
                "t_stat": t,
                "drop": mean_b - mean_a,
            }

    if not best_stats or best_t < 1.96:
        print(f"  No significant regime change detected (best t={best_t:.1f}).")
        # Still plot the cache rate over time
        best_stats = None

    # Build figure
    fig, ax = plt.subplots(figsize=(12, 6.75), dpi=100)
    fig.set_facecolor(BG)
    ax.set_facecolor(BG)

    x = np.arange(len(dates))

    # Plot cache rate as area fill
    ax.fill_between(x, cache_pcts, alpha=0.3, color=ACCENT)
    ax.plot(x, cache_pcts, color=ACCENT, linewidth=2, label="cache hit rate")

    # Plot turn volume as faint bars
    max_turns = max(turns)
    turn_scale = max(cache_pcts) / max_turns if max_turns > 0 else 1
    scaled_turns = [t * turn_scale * 0.3 for t in turns]
    ax.bar(x, scaled_turns, alpha=0.15, color=TEXT, width=0.8)

    # Mark the changepoint
    if best_stats:
        cp = best_idx
        ax.axvline(x=cp, color=RED, linestyle="--", linewidth=2, alpha=0.8)

        # Shade before/after regions
        ax.axhspan(best_stats["mean_before"] - 0.5, best_stats["mean_before"] + 0.5,
                    xmin=0, xmax=cp / len(dates), color=GREEN, alpha=0.15)
        ax.axhline(y=best_stats["mean_before"], xmax=cp / len(dates),
                    color=GREEN, linestyle="-", linewidth=1.5, alpha=0.6)
        ax.axhline(y=best_stats["mean_after"], xmin=cp / len(dates),
                    color=RED, linestyle="-", linewidth=1.5, alpha=0.6)

    # X axis
    if len(dates) <= 14:
        ax.set_xticks(x)
        ax.set_xticklabels([_short_date(d) for d in dates], rotation=0, ha="center", **_font(11))
    else:
        step = max(1, len(dates) // 12)
        ticks = list(range(0, len(dates), step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([_short_date(dates[i]) for i in ticks], rotation=0, ha="center", **_font(11))

    # Y axis
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylim(min(cache_pcts) - 3, 101)

    ax.tick_params(colors=TEXT, which="both")
    for label in ax.get_yticklabels():
        label.set_fontproperties(fm.FontProperties(fname=FONT_PATH, size=11))
        label.set_color(TEXT)
    for label in ax.get_xticklabels():
        label.set_color(TEXT)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.grid(axis="y", color=DIM, alpha=0.3, linewidth=0.5)

    # Headline
    if best_stats:
        headline = f"cache rate dropped {best_stats['drop']:.1f}% on {_short_date(best_stats['date'])}"
        fig.text(0.05, 0.97, headline, color=LIGHT, **_font(28, bold=True),
                 ha="left", va="top")

        # p-value from z-score (two-tailed)
        z = best_t
        # Approximate p-value using the complementary error function
        p_val = math.erfc(z / math.sqrt(2))
        if p_val < 0.001:
            p_str = f"p={p_val:.1e}"
        elif p_val < 0.01:
            p_str = f"p={p_val:.3f}"
        else:
            p_str = f"p={p_val:.2f}"

        detail = (f"{best_stats['mean_before']:.1f}% \u2192 {best_stats['mean_after']:.1f}%"
                  f"  z={z:.1f}  {p_str}"
                  f"  ({best_stats['n_before']} days before, {best_stats['n_after']} after)")
        fig.text(0.05, 0.91, detail, color=TEXT, **_font(13), ha="left", va="top")
    else:
        fig.text(0.05, 0.97, "cache rate stable", color=GREEN, **_font(28, bold=True),
                 ha="left", va="top")
        mean = sum(cache_pcts) / len(cache_pcts)
        fig.text(0.05, 0.91, f"avg {mean:.1f}% over {len(dates)} days", color=TEXT,
                 **_font(13), ha="left", va="top")

    # Watermark
    fig.text(0.95, 0.97, "uvx clanker-analytics --regime", color=DIM,
             **_font(11), ha="right", va="top")

    plt.tight_layout(rect=[0, 0, 1, 0.85])
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)

    return OUTPUT
