import math
from datetime import datetime, timedelta, timezone

import duckdb
import numpy as np

from clanker_analytics import model_rates
from clanker_analytics.quota import connect
from clanker_analytics.rows import insert_rows

from support import Case

START = datetime(2026, 10, 1, tzinfo=timezone.utc)
PRICES = {"claude-opus-5-5": 40.0, "claude-sonnet-5-5": 15.0}


def _text(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class TestModelRates(Case):
    def _pool(self):
        """Two accounts on one plan, each in use half of every day for three weeks, read
        every five minutes in whole points a minute behind their usage, with each hour's
        model mix drawn at random. Their weeks reset on different days."""
        rng = np.random.default_rng(1)
        quota = connect(self.tmp_path / "quota.sqlite3")
        tokens = duckdb.connect()
        tokens.execute("CREATE TABLE tokens (tool VARCHAR, model VARCHAR, timestamp VARCHAR, "
                       "api_cost_usd DOUBLE)")
        used = {"a": 0.0, "b": 0.0}
        resets = {"a": START + timedelta(days=7), "b": START + timedelta(days=10)}
        requests, samples = [], []
        for minute in range(0, 21 * 24 * 60, 5):
            now = START + timedelta(minutes=minute)
            account = "a" if now.hour < 12 else "b"
            for name in used:
                if now >= resets[name]:
                    used[name], resets[name] = 0.0, resets[name] + timedelta(days=7)
            # Readings show the points of usage up to a minute before.
            for name in used:
                samples.append((name, _text(now), math.floor(used[name]), _text(resets[name])))
            if minute % 60 == 0:
                # Each hour runs its own mix of the two models.
                mix = {model: rng.uniform(0, 1) for model in PRICES}
            for model, price in PRICES.items():
                if rng.random() < 0.6:
                    usd = 3 * rng.exponential(mix[model])
                    requests.append(("Claude Code", model, _text(now + timedelta(minutes=1)), usd))
                    used[account] += usd / price
        quota.executemany("""
            INSERT INTO quota_samples (provider, account_key, bucket_id, bucket_label, scope_type,
                scope_id, window_kind, window_seconds, effective_at, collected_at, used_pct,
                resets_at, source, source_sample_id, status, plan)
            VALUES ('anthropic', ?, '1w', 'Week', 'account', '', 'fixed', 604800, ?, ?, ?, ?,
                    'test', ?, 'ok', 'max_20x')""",
            [(name, at, at, pct, reset, f"{name}{at}") for name, at, pct, reset in samples])
        insert_rows(tokens, "tokens", requests)
        return quota, tokens

    def test_recovers_each_models_price_across_accounts_and_a_reset(self):
        quota, tokens = self._pool()

        result = model_rates.fit_window(tokens, quota, "anthropic", "1w", None, None)

        prices = {model["model"]: model for model in result["models"]}
        assert set(prices) == set(PRICES)
        for model, price in PRICES.items():
            assert abs(prices[model]["usd_per_point"] / price - 1) < 0.1, prices[model]
            assert prices[model]["usd_per_point_low"] <= price <= prices[model]["usd_per_point_high"]
        assert result["accounts"] == {"a": ["max_20x"], "b": ["max_20x"]}

    def test_units_are_cut_by_usage_and_stop_at_an_unusable_interval(self):
        at = [START + timedelta(hours=hour) for hour in range(6)]
        intervals = [(at[0], at[1], 1.0, True), (at[1], at[2], 0.0, True),
                     (at[2], at[3], 0.0, False), (at[3], at[4], 2.0, True),
                     (at[4], at[5], 1.0, True)]
        spend = np.array([[3.0, 1.0], [2.0, 0.0], [9.0, 9.0], [1.0, 1.0], [6.0, 0.0]])

        merged, dollars = model_rates.units(intervals, spend, 5)

        assert merged == [(at[0], at[2], 1.0), (at[3], at[5], 3.0)]
        assert dollars.tolist() == [[5.0, 1.0], [7.0, 1.0]]

    def test_intervals_across_a_reset_or_at_a_full_window_are_unusable(self):
        week = START + timedelta(days=7)
        later = week + timedelta(days=7)
        readings = {"a": [(START + timedelta(hours=hour), used, resets, None)
                          for hour, used, resets in ((0, 98, week), (1, 99, week), (2, 100, week),
                                                     (3, 3, later), (4, 4, later))]}

        assert [(rise, usable) for _, _, rise, usable in model_rates.reading_intervals(readings)] == [
            (1.0, True), (1.0, False), (-97.0, False), (1.0, True)]

    def test_idle_rate_counts_only_time_long_after_usage(self):
        at = [START + timedelta(hours=hour) for hour in range(9)]
        # Usage in the first hour, a lagging rise an hour later, then an idle rise.
        intervals = [(at[i], at[i + 1], rise, True)
                     for i, rise in enumerate([1.0, 0.0, 1.0, 0.0, 0.0, 0.5, 0.5, 0.0])]
        spent = np.array([5.0, 0, 0, 0, 0, 0, 0, 0])

        rate, hours = model_rates.idle_rate(intervals, spent)

        assert (rate, hours) == (0.25, 4.0)

    def test_readout_lag_is_the_rise_weighted_time_after_usage(self):
        at = [START + timedelta(minutes=10 * step) for step in range(6)]
        intervals = [(at[i], at[i + 1], rise, True) for i, rise in enumerate([0.0, 3.0, 1.0, 0.0, 0.0])]
        spent = np.array([5.0, 0, 0, 0, 0])

        # 3 points by 10 minutes after the usage ended and 1 by 20 minutes.
        assert model_rates.readout_lag(intervals, spent) == timedelta(minutes=12.5)

    def test_a_price_counts_as_changed_only_when_both_halves_disagree_and_hold_usage(self):
        def model(first, second, usd=(50.0, 50.0)):
            return {"api_usd": sum(usd), "halves": [
                {"api_usd": usd[0], "usd_per_point_low": first[0], "usd_per_point_high": first[1]},
                {"api_usd": usd[1], "usd_per_point_low": second[0], "usd_per_point_high": second[1]}]}

        assert model_rates.changed(model((1.0, 2.0), (3.0, None)))
        assert not model_rates.changed(model((1.0, 2.0), (1.5, 4.0)))
        assert not model_rates.changed(model((1.0, 2.0), (3.0, 4.0), usd=(99.0, 1.0)))

    def test_nnls_keeps_coefficients_non_negative(self):
        a = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
        b = np.array([2.0, -1.0, 1.0])

        x = model_rates.nnls(a, b)

        assert x[1] == 0 and abs(x[0] - 1.5) < 1e-9
