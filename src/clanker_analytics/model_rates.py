"""API dollars of each model's usage that move a quota window by one percentage point.

A provider's accounts on one plan share a price per point, so a window's readings across all
of them are fitted together against all of the tool's local usage: no request needs to be
attributed to an account. Readings are whole points that trail usage by a minute or more, so
a short interval's rise is a noisy measure of its usage. The sample unit is therefore a run
of consecutive intervals between readings that holds enough usage for a few points of rise
and lasts long enough that little of its rise shows in the next unit.
Units are cut by usage and reading times alone, never by the rise they are fitted to, which
would bias every price. A unit ends where any account's reading falls, its reset time moves,
or it reads 100%; those intervals are left out. Rises that local usage does not explain, such
as use on the provider's website, are measured from idle time, the intervals with no local
usage in them or in the hours of readout lag before them, and that rate times each unit's hours is
taken off its rise. Fitted jointly with the models, such a term trades off against a model
that runs at all hours. Each unit's remaining rise is fitted by non-negative least squares
on each model's API dollars in it. Intervals are percentile ranges of a stationary block bootstrap whose expected
block length is chosen from the data (Politis and White 2004, with the 2009 correction).
Each model's fit is checked against the units it had to itself, and against fits of each
half of the window, which show a price that changed within it.
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
# Readings trail usage by up to an hour or more (Antigravity's most), so idle time starts
# this long after the last usage: on this host's Claude, Codex and Antigravity weekly
# readings, rises between 1 and 3 hours after usage still occur and none after 3 hours.
READOUT_LAG = timedelta(hours=3)
BOOTSTRAP_DRAWS = 1000
# A unit is a model's own, for the single-model check, when the model spent this share of it.
SINGLE_MODEL_SHARE = 0.97
# The share of a unit's rise that may show in the next unit, which sets how long units last.
LAG_SHARE = 0.1


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


def reading_intervals(series: dict[str, list[tuple]]) -> list[tuple[datetime, datetime, float, bool]]:
    """(start, end, points risen across accounts, usable) between consecutive reading times.
    An interval is unusable when any account's reading fell, its reset time moved, or it
    read 100% at either end, where further use may not register."""
    moments = sorted({reading[0] for readings in series.values() for reading in readings})
    # The last reading of each account at or before each moment.
    latest: dict[str, list[tuple | None]] = {}
    for account, readings in series.items():
        position, current, values = 0, None, []
        for moment in moments:
            while position < len(readings) and readings[position][0] <= moment:
                current = readings[position]
                position += 1
            values.append(current)
        latest[account] = values
    intervals = []
    for index in range(len(moments) - 1):
        rise, usable = 0.0, True
        for values in latest.values():
            before, after = values[index], values[index + 1]
            if before is None or after is None:
                continue
            resets_moved = (before[2] and after[2]
                            and abs(after[2] - before[2]) > RESET_TOLERANCE)
            if after[1] < before[1] or resets_moved or max(before[1], after[1]) >= 100:
                usable = False
            rise += after[1] - before[1]
        intervals.append((moments[index], moments[index + 1], rise, usable))
    return intervals


def units(intervals: list[tuple], spend: np.ndarray, threshold: float,
          duration: timedelta = timedelta(0)) -> tuple[list[tuple[datetime, datetime, float]], np.ndarray]:
    """Consecutive usable intervals merged until each unit holds at least threshold API
    dollars of usage and lasts at least duration, with each unit's rise and each model's
    dollars. A unit ends at an unusable interval, and one left short there is kept."""
    merged, rows = [], []
    current, dollars = None, None
    for (start, end, rise, usable), row in zip(intervals, spend):
        if not usable:
            if current:
                merged.append(current)
                rows.append(dollars)
            current = None
            continue
        if current:
            current, dollars = (current[0], end, current[2] + rise), dollars + row
        else:
            current, dollars = (start, end, rise), row.copy()
        if dollars.sum() >= threshold and current[1] - current[0] >= duration:
            merged.append(current)
            rows.append(dollars)
            current = None
    if current:
        merged.append(current)
        rows.append(dollars)
    return merged, np.array(rows).reshape(len(rows), spend.shape[1])


def interval_spend(token_db: Any, tool: str,
                   intervals: list[tuple]) -> tuple[list[str], np.ndarray]:
    """Each model's API dollars of the tool's usage within each interval."""
    token_db.execute("CREATE OR REPLACE TEMP TABLE rate_intervals (i INTEGER, s TIMESTAMPTZ, e TIMESTAMPTZ)")
    insert_rows(token_db, "rate_intervals", [(i, s, e) for i, (s, e, *_) in enumerate(intervals)])
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


def readout_lag(intervals: list[tuple], dollars: np.ndarray) -> timedelta:
    """How long readings trail usage on average: the mean time after the last usage at which
    the rise seen in the following idle READOUT_LAG showed, weighted by that rise."""
    trailing: list[tuple[timedelta, float]] = []
    last_use = None
    for (start, end, rise, usable), spent in zip(intervals, dollars):
        if spent > 0:
            last_use = end
        elif usable and last_use is not None and start - last_use < READOUT_LAG and rise > 0:
            trailing.append((end - last_use, rise))
    total = sum(rise for _, rise in trailing)
    if not total:
        return timedelta(0)
    return sum((age * rise for age, rise in trailing), timedelta(0)) / total


def idle_rate(intervals: list[tuple], dollars: np.ndarray) -> tuple[float, float]:
    """Points per hour risen while no local usage could still show, and the idle hours."""
    rise = hours = 0.0
    last_use = None
    for (start, end, interval_rise, usable), spent in zip(intervals, dollars):
        if spent > 0:
            last_use = end
        elif usable and (last_use is None or start - last_use >= READOUT_LAG):
            rise += interval_rise
            hours += (end - start).total_seconds() / 3600
    return (rise / hours if hours else 0.0), hours


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


def _ratio_range(dollars: np.ndarray, points: np.ndarray,
                 rng: np.random.Generator) -> tuple[float, float, float] | None:
    """Dollars per point over units in time order, with a stationary block bootstrap range,
    or None for fewer than five units or no rise."""
    if len(dollars) < 5 or points.sum() <= 0:
        return None
    block = block_length(points)
    draws = []
    for _ in range(BOOTSTRAP_DRAWS):
        pick = stationary_bootstrap(len(points), block, rng)
        if points[pick].sum() > 0:
            draws.append(dollars[pick].sum() / points[pick].sum())
    return (float(dollars.sum() / points.sum()), float(np.percentile(draws, 2.5)),
            float(np.percentile(draws, 97.5)))


def single_model_prices(elementary: list[tuple], elementary_dollars: np.ndarray,
                        unit_spans: list[tuple], spend: np.ndarray, rise: np.ndarray,
                        lag: timedelta, seed: int) -> list[dict[str, Any]]:
    """Each model's price from the units it had to itself, as a check on the joint fit.

    A unit counts for a model when the model spent at least SINGLE_MODEL_SHARE of the unit's
    usage and of the usage in the readout lag before it, whose rise can still show in the
    unit. The price is the units' dollars over their rise, with a stationary block bootstrap
    range over them in time order."""
    ends = np.array([end.timestamp() for _, end, *_ in elementary])
    cumulative = np.vstack([np.zeros(spend.shape[1]), np.cumsum(elementary_dollars, axis=0)])
    rng = np.random.default_rng(seed)
    checks = []
    for index in range(spend.shape[1]):
        own = []
        for unit, (start, _, _) in enumerate(unit_spans):
            before = (cumulative[np.searchsorted(ends, start.timestamp(), side="right")]
                      - cumulative[np.searchsorted(ends, (start - lag).timestamp(), side="right")])
            window = spend[unit] + before
            if spend[unit].sum() > 0 and window[index] >= SINGLE_MODEL_SHARE * window.sum():
                own.append(unit)
        check: dict[str, Any] = {"alone_units": len(own)}
        priced = _ratio_range(spend[own, index], rise[own], rng)
        if priced:
            check.update(zip(("alone_usd_per_point", "alone_usd_per_point_low",
                              "alone_usd_per_point_high"), priced))
        checks.append(check)
    return checks


def _joint_fit(a: np.ndarray, b: np.ndarray,
               rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    """Coefficients by non-negative least squares, their 95% bootstrap bounds, and the
    bootstrap's expected block length."""
    estimate = nnls(a, b)
    mean_block = block_length(b)
    draws = np.array([nnls(a[i], b[i]) for i in (
        stationary_bootstrap(len(b), mean_block, rng) for _ in range(BOOTSTRAP_DRAWS))])
    low, high = np.percentile(draws, [2.5, 97.5], axis=0)
    return estimate, low, high, mean_block


def _per_point(coefficient: float) -> float | None:
    return 1 / coefficient if coefficient > 0 else None


def fit_window(token_db: Any, connection: sqlite3.Connection, provider: str, bucket_id: str,
               start: datetime | None, end: datetime | None, points: float = 5,
               seed: int = 0) -> dict[str, Any]:
    series = account_readings(connection, provider, bucket_id, start, end)
    tool = PROVIDER_TO_TOOL[provider]
    intervals = reading_intervals(series)
    result: dict[str, Any] = {
        "provider": provider, "tool": tool, "bucket_id": bucket_id,
        "from": _time_text(start) if start else None, "to": _time_text(end) if end else None,
        "accounts": {account: sorted({r[3] or "unrecorded" for r in readings})
                     for account, readings in series.items()},
        "points_per_unit": points,
        "units": 0,
        "models": [],
    }
    if not intervals:
        return result
    names, interval_dollars = interval_spend(token_db, tool, intervals)
    usable = np.array([interval[3] for interval in intervals])
    rises = np.array([interval[2] for interval in intervals])
    if not usable.any() or rises[usable].sum() <= 0:
        return result
    drift, idle_hours = idle_rate(intervals, interval_dollars.sum(axis=1))
    lag = readout_lag(intervals, interval_dollars.sum(axis=1))
    # Enough usage for about points of rise at the window's overall price. A unit's rise
    # from usage in its last lag shows in the next unit, about lag over its length, so a
    # unit lasts long enough to keep that near LAG_SHARE.
    threshold = points * interval_dollars[usable].sum() / rises[usable].sum()
    elementary = intervals
    intervals, spend = units(intervals, interval_dollars, threshold, lag / LAG_SHARE)
    result["units"] = len(intervals)
    if len(intervals) < 20:
        return result
    hours = np.array([(e - s).total_seconds() / 3600 for s, e, *_ in intervals])
    a = spend
    b = np.array([rise for _, _, rise in intervals]) - drift * hours
    rng = np.random.default_rng(seed)
    estimate, low, high, mean_block = _joint_fit(a, b, rng)
    per_point = _per_point
    # Each half of the window, split where half its usage is reached, fitted on its own:
    # a model whose halves' ranges do not overlap changed price within the window.
    split = int(np.searchsorted(np.cumsum(spend.sum(axis=1)), spend.sum() / 2))
    halves: list[tuple | None] = []
    for part in (slice(0, split), slice(split, None)):
        halves.append(_joint_fit(a[part], b[part], rng) if len(b[part]) >= 10 else None)

    alone = single_model_prices(elementary, interval_dollars, intervals, spend, b, lag, seed)
    result.update({
        "readout_lag_minutes": lag.total_seconds() / 60,
        "points": float(sum(rise for _, _, rise in intervals)),
        "api_usd": float(spend.sum()),
        "mean_block_units": mean_block,
        "models": [{
            "model": name,
            "api_usd": float(spend[:, index].sum()),
            "usd_per_point": per_point(estimate[index]),
            # A smaller coefficient is a higher price, so the bounds swap.
            "usd_per_point_low": per_point(high[index]),
            "usd_per_point_high": per_point(low[index]),
            **alone[index],
            **({"halves": [{
                "from": _time_text(intervals[0 if part == 0 else split][0]),
                "api_usd": float(spend[slice(0, split) if part == 0 else slice(split, None), index].sum()),
                "usd_per_point": per_point(half[0][index]),
                "usd_per_point_low": per_point(half[2][index]),
                "usd_per_point_high": per_point(half[1][index]),
            } for part, half in enumerate(halves)]} if all(halves) else {}),
        } for index, name in enumerate(names)],
        "unlogged_points_per_hour": drift,
        "idle_hours": idle_hours,
    })
    return result


def disagrees(model: dict[str, Any]) -> bool:
    """Whether a model's joint-fit range and its single-model range do not overlap."""
    low = model["usd_per_point_low"] or 0.0
    high = model["usd_per_point_high"] if model["usd_per_point_high"] is not None else math.inf
    return high < model["alone_usd_per_point_low"] or low > model["alone_usd_per_point_high"]


def changed(model: dict[str, Any]) -> bool:
    """Whether a model's price ranges in the window's two halves do not overlap, among
    models with at least 5% of their usage in each half."""
    halves = model.get("halves")
    if not halves or min(h["api_usd"] for h in halves) < 0.05 * model["api_usd"]:
        return False
    bounds = [(h["usd_per_point_low"] or 0.0,
               h["usd_per_point_high"] if h["usd_per_point_high"] is not None else math.inf)
              for h in halves]
    return bounds[0][1] < bounds[1][0] or bounds[1][1] < bounds[0][0]


def print_fit(result: dict[str, Any]) -> None:
    span = f"{result['from'] or 'start'} to {result['to'] or 'now'}"
    plans = ", ".join(f"{a[:8]} {'/'.join(p)}" for a, p in result["accounts"].items())
    print(f"{result['tool']} {result['bucket_id']}, {span}: {result['units']} units of usage "
          f"worth about {result['points_per_unit']:g} points; "
          f"accounts {plans}")
    if not result["models"]:
        print("  too few units to fit")
        return
    print(f"  {result['points']:.0f} points, ${result['api_usd']:,.0f} of local usage, "
          f"bootstrap blocks of {result['mean_block_units']:.1f} units")

    def money(value: float | None, missing: str) -> str:
        return f"${value:,.2f}" if value is not None else missing

    print(f"  readings trail usage by {result['readout_lag_minutes']:.0f} minutes on average, "
          f"so each unit lasts at least {result['readout_lag_minutes'] / LAG_SHARE / 60:.1f} hours")
    # No price means the fit found no rise from that model's usage.
    for model in result["models"]:
        line = (f"  {model['model']:<28} ${model['api_usd']:>9,.0f}"
                f"  {money(model['usd_per_point'], 'no rise'):>10} per point"
                f"  (95% {money(model['usd_per_point_low'], 'no rise')} to"
                f" {money(model['usd_per_point_high'], 'unbounded')})")
        if "alone_usd_per_point" in model:
            line += (f"; alone in {model['alone_units']} units {money(model['alone_usd_per_point'], '')}"
                     f" ({money(model['alone_usd_per_point_low'], '')} to"
                     f" {money(model['alone_usd_per_point_high'], '')})")
            if disagrees(model):
                line += " DISAGREES"
        print(line)
        if changed(model):
            print("    CHANGED within the window: "
                  + ", then ".join(f"from {h['from'][:10]} {money(h['usd_per_point'], 'no rise')}"
                                   f" ({money(h['usd_per_point_low'], 'no rise')} to"
                                   f" {money(h['usd_per_point_high'], 'unbounded')})"
                                   for h in model["halves"])
                  + "; fit each stretch with --from and --to")
    print(f"  {UNLOGGED}, over {result['idle_hours']:.0f} idle hours: "
          f"{result['unlogged_points_per_hour']:.3f}")
