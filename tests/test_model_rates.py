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
        model mix drawn at random. The second account's week resets on day 10."""
        rng = np.random.default_rng(1)
        quota = connect(self.tmp_path / "quota.sqlite3")
        tokens = duckdb.connect()
        tokens.execute("CREATE TABLE tokens (tool VARCHAR, model VARCHAR, timestamp VARCHAR, "
                       "api_cost_usd DOUBLE)")
        used = {"a": 0.0, "b": 0.0}
        resets = {"a": START + timedelta(days=28), "b": START + timedelta(days=10)}
        requests, samples = [], []
        for minute in range(0, 21 * 24 * 60, 5):
            now = START + timedelta(minutes=minute)
            account = "a" if now.hour < 12 else "b"
            if now >= resets["b"]:
                used["b"], resets["b"] = 0.0, resets["b"] + timedelta(days=7)
            # Readings show the points of usage up to a minute before.
            for name in used:
                samples.append((name, _text(now), math.floor(used[name]), _text(resets[name])))
            if minute % 60 == 0:
                # Each hour runs its own mix of the two models.
                mix = {model: rng.uniform(0, 1) for model in PRICES}
            for model, price in PRICES.items():
                if rng.random() < 0.6:
                    usd = rng.exponential(mix[model])
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

    def test_units_stop_at_a_left_out_interval(self):
        at = [START + timedelta(hours=hour) for hour in range(6)]
        intervals = [(at[0], at[1], 1.0), (at[1], at[2], 1.0), (at[3], at[4], 1.0),
                     (at[4], at[5], 2.0)]

        assert model_rates.units(intervals, 2) == [(at[0], at[2], 2.0), (at[3], at[5], 3.0)]

    def test_intervals_across_a_reset_or_into_a_full_window_are_left_out(self):
        week = START + timedelta(days=7)
        readings = {"a": [(START, 98, week, None), (START + timedelta(hours=1), 99, week, None),
                          (START + timedelta(hours=2), 100, week, None),
                          (START + timedelta(hours=3), 3, week + timedelta(days=7), None),
                          (START + timedelta(hours=4), 4, week + timedelta(days=7), None)]}

        assert model_rates.rise_intervals(readings) == [
            (START + timedelta(hours=3), START + timedelta(hours=4), 1.0)]

    def test_nnls_keeps_coefficients_non_negative(self):
        a = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
        b = np.array([2.0, -1.0, 1.0])

        x = model_rates.nnls(a, b)

        assert x[1] == 0 and abs(x[0] - 1.5) < 1e-9
