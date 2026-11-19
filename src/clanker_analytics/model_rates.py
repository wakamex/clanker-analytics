"""API dollars of each model's usage that move a quota window by one percentage point.

A provider's accounts on one plan share a price per point, so a window's readings across all
of them are fitted together against all of the tool's local usage: no request needs to be
attributed to an account. Readings are whole points that trail usage by a minute or more, so
the interval between two changes of a reading holds one point of rise but a noisy share of
the usage behind it, which would bias every price upward. The sample unit is therefore a run
of consecutive such intervals holding a few points of rise, ended early by a reset, a full
window or a gap in the readings, which are left out. Each unit's rise is fitted by non-negative least
squares on each model's API dollars in it, plus the interval's hours for usage the local logs
do not see. Intervals are percentile ranges of a stationary block bootstrap whose expected
block length is chosen from the data (Politis and White 2004, with the 2009 correction).
"""

from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from clanker_analytics.quota import PROVIDER_TO_TOOL, _parse_time, _time_text
from clanker_analytics.rows import insert_rows

# Models with less than this share of the fitted usage are fitted together as "other".
MIN_MODEL_SHARE = 0.02
UNLOGGED = "unlogged usage, points per hour"
# Effort variants of a model, such as gemini-4-argon-high, are fitted as the model.
EFFORT_SUFFIX = "-(minimal|low|medium|high|xhigh|max)$"
RESET_TOLERANCE = timedelta(hours=1)
BOOTSTRAP_DRAWS = 1000


def account_readings(connection: sqlite3.Connection, provider: str, bucket_id: str,
                     start: datetime | None, end: datetime | None) -> dict[str, list[tuple]]:
    """Each account's (time, used percentage, reset time, plan) readings of one window."""
    series: dict[str, list[tuple]] = {}
    for account, at, used, resets, plan in connection.execute("""
            SELECT account_key, effective_at, used_pct, resets_at, plan FROM quota_samples
            WHERE provider = ? AND bucket_id = ? AND account_key != 'default'
            ORDER BY effective_at""", (provider, bucket_id)):
        at = _parse_time(at)
        if (start and at < start) or (end and at > end):
            continue
        series.setdefault(account, []).append((at, used, _parse_time(resets), plan))
    return series


def rise_intervals(series: dict[str, list[tuple]]) -> list[tuple[datetime, datetime, float]]:
    """(start, end, points risen across accounts) between consecutive reading changes."""
    changes = sorted({reading[0] for readings in series.values()
                      for previous, reading in zip(readings, readings[1:])
                      if reading[1] != previous[1] or reading[2] != previous[2]})
    if not changes:
        return []
    # The last reading of each account at or before each change.
    at_change: dict[str, list[tuple | None]] = {}
    for account, readings in series.items():
        position, latest, values = 0, None, []
        for moment in changes:
            while position < len(readings) and readings[position][0] <= moment:
                latest = readings[position]
                position += 1
            values.append(latest)
        at_change[account] = values
    intervals: list[tuple[datetime, datetime, float]] = []
    for index in range(len(changes) - 1):
        rise, valid = 0.0, True
        for values in at_change.values():
            before, after = values[index], values[index + 1]
            if before is None or after is None:
                continue
            resets_moved = (before[2] and after[2]
                            and abs(after[2] - before[2]) > RESET_TOLERANCE)
            if after[1] < before[1] or resets_moved or (after[1] >= 100 and after[1] > before[1]):
                valid = False
                break
            rise += after[1] - before[1]
        if valid:
            intervals.append((changes[index], changes[index + 1], rise))
    return intervals


def units(intervals: list[tuple], points: float) -> list[tuple[datetime, datetime, float]]:
    """Consecutive intervals merged until each unit holds at least points of rise. A unit
    ends at a left-out interval, so none spans a reset."""
    merged: list[tuple[datetime, datetime, float]] = []
    current = None
    for start, end, rise in intervals:
        if current and current[1] != start:
            merged.append(current)
            current = None
        current = (current[0], end, current[2] + rise) if current else (start, end, rise)
        if current[2] >= points:
            merged.append(current)
            current = None
    return merged + ([current] if current else [])


def interval_spend(token_db: Any, tool: str,
                   intervals: list[tuple]) -> tuple[list[str], np.ndarray]:
    """Each model's API dollars of the tool's usage within each interval."""
    token_db.execute("CREATE OR REPLACE TEMP TABLE rate_intervals (i INTEGER, s TIMESTAMPTZ, e TIMESTAMPTZ)")
    insert_rows(token_db, "rate_intervals", [(i, s, e) for i, (s, e, _) in enumerate(intervals)])
    rows = token_db.execute(f"""
        SELECT r.i, coalesce(nullif(regexp_replace(t.model, '{EFFORT_SUFFIX}', ''), ''), 'unknown'),
               sum(coalesce(t.api_cost_usd, 0))
        FROM tokens t JOIN rate_intervals r
          ON try_cast(t.timestamp AS TIMESTAMPTZ) > r.s
         AND try_cast(t.timestamp AS TIMESTAMPTZ) <= r.e
        WHERE t.tool = ?
        GROUP BY ALL
    """, [tool]).fetchall()
    totals: dict[str, float] = {}
    for _, model, usd in rows:
        totals[model] = totals.get(model, 0.0) + usd
    overall = sum(totals.values()) or 1.0
    kept = sorted((m for m, usd in totals.items() if usd / overall >= MIN_MODEL_SHARE),
                  key=lambda m: -totals[m])
    names = kept + (["other"] if len(kept) < len(totals) else [])
    spend = np.zeros((len(intervals), len(names)))
    for index, model, usd in rows:
        spend[index, names.index(model if model in kept else "other")] += usd
    return names, spend


def nnls(a: np.ndarray, b: np.ndarray, iterations: int = 200) -> np.ndarray:
    """Non-negative least squares by Lawson and Hanson's active-set method."""
    x = np.zeros(a.shape[1])
    passive = np.zeros(a.shape[1], dtype=bool)
    for _ in range(iterations):
        gradient = a.T @ (b - a @ x)
        candidates = ~passive & (gradient > 1e-12)
        if not candidates.any():
            break
        passive[np.argmax(np.where(candidates, gradient, -np.inf))] = True
        while True:
            z = np.zeros_like(x)
            z[passive] = np.linalg.lstsq(a[:, passive], b, rcond=None)[0]
            if (z[passive] > 0).all():
                x = z
                break
            negative = passive & (z <= 0)
            step = np.min(x[negative] / (x[negative] - z[negative]))
            x = x + step * (z - x)
            passive &= x > 1e-12
    return x


def block_length(series: np.ndarray) -> float:
    """Politis and White's expected block length for the stationary bootstrap."""
    n = len(series)
    centered = series - series.mean()
    variance = centered @ centered / n
    if n < 8 or variance == 0:
        return 1.0
    lags = min(n - 1, int(2 * math.sqrt(n)) + 10)
    acov = np.array([centered[:n - k] @ centered[k:] / n for k in range(lags + 1)])
    rho = acov / variance
    window = max(5, int(math.sqrt(math.log10(n))))
    bound = 2 * math.sqrt(math.log10(n) / n)
    m = next((k for k in range(1, lags - window + 1)
              if (np.abs(rho[k + 1:k + 1 + window]) < bound).all()), lags // 2)
    big_m = min(2 * m, lags)

    def flat_top(t: float) -> float:
        t = abs(t)
        return 1.0 if t <= 0.5 else 2 * (1 - t) if t <= 1 else 0.0

    g = sum(flat_top(k / big_m) * k * acov[k] for k in range(1, big_m + 1)) * 2
    spectrum = acov[0] + 2 * sum(flat_top(k / big_m) * acov[k] for k in range(1, big_m + 1))
    if spectrum <= 0:
        return 1.0
    return min(max((2 * g * g / (2 * spectrum * spectrum)) ** (1 / 3) * n ** (1 / 3), 1.0), n / 2)


def stationary_bootstrap(n: int, mean_block: float, rng: np.random.Generator) -> np.ndarray:
    """Indices of one stationary bootstrap resample: blocks of geometric length, wrapping."""
    starts_block = rng.random(n) < 1 / mean_block
    starts_block[0] = True
    block = np.cumsum(starts_block) - 1
    first = np.flatnonzero(starts_block)
    offset = np.arange(n) - first[block]
    return (rng.integers(n, size=len(first))[block] + offset) % n


def fit_window(token_db: Any, connection: sqlite3.Connection, provider: str, bucket_id: str,
               start: datetime | None, end: datetime | None, points: float = 5,
               seed: int = 0) -> dict[str, Any]:
    series = account_readings(connection, provider, bucket_id, start, end)
    intervals = units(rise_intervals(series), points)
    tool = PROVIDER_TO_TOOL[provider]
    result: dict[str, Any] = {
        "provider": provider, "tool": tool, "bucket_id": bucket_id,
        "from": _time_text(start) if start else None, "to": _time_text(end) if end else None,
        "accounts": {account: sorted({r[3] or "unrecorded" for r in readings})
                     for account, readings in series.items()},
        "points_per_unit": points,
        "units": len(intervals),
    }
    if len(intervals) < 20:
        result["models"] = []
        return result
    names, spend = interval_spend(token_db, tool, intervals)
    hours = np.array([(e - s).total_seconds() / 3600 for s, e, _ in intervals])
    a = np.column_stack([spend, hours])
    b = np.array([rise for _, _, rise in intervals])
    estimate = nnls(a, b)
    mean_block = block_length(b)
    rng = np.random.default_rng(seed)
    draws = np.array([nnls(a[i], b[i]) for i in (
        stationary_bootstrap(len(b), mean_block, rng) for _ in range(BOOTSTRAP_DRAWS))])
    low, high = np.percentile(draws, [2.5, 97.5], axis=0)

    def per_point(coefficient: float) -> float | None:
        return 1 / coefficient if coefficient > 0 else None

    result.update({
        "points": float(b.sum()),
        "api_usd": float(spend.sum()),
        "mean_block_units": mean_block,
        "models": [{
            "model": name,
            "api_usd": float(spend[:, index].sum()),
            "usd_per_point": per_point(estimate[index]),
            # A smaller coefficient is a higher price, so the bounds swap.
            "usd_per_point_low": per_point(high[index]),
            "usd_per_point_high": per_point(low[index]),
        } for index, name in enumerate(names)],
        "unlogged_points_per_hour": float(estimate[-1]),
        "unlogged_points_per_hour_range": [float(low[-1]), float(high[-1])],
    })
    return result


def print_fit(result: dict[str, Any]) -> None:
    span = f"{result['from'] or 'start'} to {result['to'] or 'now'}"
    plans = ", ".join(f"{a[:8]} {'/'.join(p)}" for a, p in result["accounts"].items())
    print(f"{result['tool']} {result['bucket_id']}, {span}: {result['units']} units of "
          f"{result['points_per_unit']:g} or more points; "
          f"accounts {plans}")
    if not result["models"]:
        print("  too few intervals to fit")
        return
    print(f"  {result['points']:.0f} points, ${result['api_usd']:,.0f} of local usage, "
          f"bootstrap blocks of {result['mean_block_units']:.1f} units")

    def money(value: float | None, missing: str) -> str:
        return f"${value:,.2f}" if value is not None else missing

    # No price means the fit found no rise from that model's usage.
    for model in result["models"]:
        print(f"  {model['model']:<28} ${model['api_usd']:>9,.0f}"
              f"  {money(model['usd_per_point'], 'no rise'):>10} per point"
              f"  (95% {money(model['usd_per_point_low'], 'no rise')} to"
              f" {money(model['usd_per_point_high'], 'unbounded')})")
    low, high = result["unlogged_points_per_hour_range"]
    print(f"  {UNLOGGED}: {result['unlogged_points_per_hour']:.3f} (95% {low:.3f} to {high:.3f})")
