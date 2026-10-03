"""Tests for quota snapshot recording and token pace comparison."""

from copy import deepcopy
from datetime import datetime, timezone
import json
import subprocess

import duckdb

from clanker_analytics import quota
from clanker_analytics.quota import (
    QuotaSample,
    build_quota_history,
    build_report,
    build_value_report,
    connect,
    insert_samples,
    normalize_antigravity,
    normalize_anthropic,
    normalize_openai,
    normalize_zai,
    parse_duration,
    print_report,
)

from support import Case, approx


def _sample(effective_at: str, used_pct: float) -> QuotaSample:
    return QuotaSample(
        provider="openai",
        account_key="default",
        bucket_id="primary",
        bucket_label="primary",
        scope_type="account",
        scope_id="",
        window_kind="unknown",
        window_seconds=259200,
        effective_at=effective_at,
        collected_at=effective_at,
        used_pct=used_pct,
        resets_at="2026-08-23T12:00:00Z",
        source="api",
        source_sample_id=f"primary-{effective_at}",
    )


def _claude_snapshot(when, used, resets_at, account=None):
    data = {"plan": "max_20x", "updated_at": when, "7d": {"pct": used, "resets_at": resets_at}}
    if account:
        data["account_key"] = account
    return normalize_anthropic(data, datetime.fromisoformat(when.replace("Z", "+00:00")))


def _history_period(tool, resets_at, used_pct, cost, tokens, pricing_flags,
                    window_minutes=10080):
    return {
        "tool": tool, "limit_id": "7d", "resets_at": resets_at, "used_pct": used_pct,
        "window_minutes": window_minutes, "total_tokens": tokens,
        "api_equivalent_cost_usd": cost,
        "full_quota_api_equivalent_cost_estimate_usd": cost * 100 / used_pct,
        "pricing_flags": pricing_flags,
    }


class TestQuota(Case):
    def test_parse_duration_accepts_arbitrary_compact_periods(self):
        assert parse_duration("3h") == 10800
        assert parse_duration("7h") == 25200
        assert parse_duration("2w") == 1209600

    def test_openai_adapter_avoids_legacy_alias_duplicates(self):
        data = {
            "updated_at": "2026-08-20T12:00:00Z",
            "source": "api",
            "primary": {"pct": 12, "window_secs": 604800, "resets_at": 1787335200},
            "7d": {"pct": 12, "window_secs": 604800, "resets_at": 1787335200},
            "additional": [{
                "name": "GPT-5.3-Codex-Spark",
                "primary": {"pct": 3, "window_secs": 18000, "resets_at": 1787335200},
            }],
        }

        samples = normalize_openai(data, datetime(2026, 8, 20, 12, tzinfo=timezone.utc))

        assert [sample.bucket_id for sample in samples] == [
            "primary",
            "additional:gpt-5-3-codex-spark:primary",
        ]
        assert samples[0].window_seconds == 604800
        assert samples[0].window_kind == "fixed"
        assert samples[1].scope_id == "GPT-5.3-Codex-Spark"

    def test_anthropic_adapter_classifies_windows_by_plan(self):
        for plan, expected in [
            ("ai", "fixed"),
            ("pro", "fixed"),
            ("max_20x", "fixed"),
            ("enterprise", "rolling"),
            ("enterprise_premium", "rolling"),
            ("unknown", "unknown"),
        ]:
            with self.subTest(plan=plan, expected=expected):
                data = {
                    "plan": plan,
                    "updated_at": "2026-08-20T12:00:00Z",
                    "7d": {
                        "pct": 20,
                        "resets_at": "2026-08-25T13:00:00.436637Z",
                    },
                }

                samples = normalize_anthropic(
                    data, datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
                )

                assert samples[0].window_kind == expected
                assert samples[0].resets_at == "2026-08-25T13:00:00Z"

    def test_claude_snapshots_record_the_logged_in_organization(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        config = tmp_path / "claude"
        config.mkdir()
        (config / ".claude.json").write_text(json.dumps(
            {"oauthAccount": {"organizationUuid": "org-second", "emailAddress": "x@example.com"}}))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(config))
        output = json.dumps({"plan": "max_20x", "updated_at": "2026-10-03T01:14:00Z",
                             "7d": {"pct": 1, "resets_at": "2026-10-09T14:00:00Z"}})
        monkeypatch.setattr(quota.subprocess, "run",
                            lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, output, ""))

        samples = quota.collect_provider("anthropic")

        assert {sample.account_key for sample in samples} == {"org-second"}

    def test_claude_snapshots_without_an_account_file_keep_the_default(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))

        assert quota.claude_account_key() is None

    def test_zai_adapter_maps_credit_windows_and_keeps_fractional_usage(self):
        data = {
            "plan": "lite",
            "source": "api",
            "updated_at": "2026-09-28T16:31:42.657428+00:00",
            "limits": [
                {"type": "CREDIT_LIMIT", "unit": 3, "number": 5, "used": 0, "limit": 2000,
                 "remaining": 2000, "pct": 0, "resets_at": None},
                {"type": "CREDIT_LIMIT", "unit": 6, "number": 1, "used": 2004, "limit": 10000,
                 "remaining": 7995, "pct": 20,
                 "resets_at": "2026-10-04T15:24:38.984000+00:00"},
            ],
            "stale": True,
        }

        session, week = normalize_zai(data, datetime(2026, 9, 30, tzinfo=timezone.utc))

        assert (session.bucket_id, session.window_seconds, session.resets_at) == ("5h", 18000, None)
        assert (week.bucket_id, week.window_seconds) == ("1w", 604800)
        assert week.used_pct == approx(20.04)
        assert week.resets_at == "2026-10-04T15:24:38.984000Z"
        assert week.effective_at == "2026-09-28T16:31:42.657428Z"
        assert week.window_kind == "fixed"

    def test_antigravity_adapter_classifies_reset_windows_as_fixed(self):
        data = {
            "updated_at": "2026-08-20T12:00:00Z",
            "quota_summary": {
                "groups": [{
                    "display_name": "Gemini Models",
                    "buckets": [
                        {
                            "bucket_id": "gemini-5h",
                            "window": "5h",
                            "remaining_pct": 75,
                            "reset_time": "2026-08-20T17:00:00Z",
                        },
                        {
                            "bucket_id": "gemini-weekly",
                            "window": "weekly",
                            "remaining_pct": 80,
                            "reset_time": "2026-08-24T20:00:00Z",
                        },
                    ],
                }],
            },
        }

        samples = normalize_antigravity(
            data, datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
        )

        assert [sample.window_kind for sample in samples] == ["fixed", "fixed"]
        assert [sample.window_seconds for sample in samples] == [18000, 604800]

    def test_fixed_window_lookback_does_not_cross_reset(self):
        tmp_path = self.tmp_path
        previous = QuotaSample(
            provider="antigravity", account_key="default", bucket_id="gemini-5h",
            bucket_label="Gemini Models 5h", scope_type="model_group",
            scope_id="Gemini Models", window_kind="fixed", window_seconds=18000,
            effective_at="2026-08-21T06:00:00Z",
            collected_at="2026-08-21T06:00:00Z", used_pct=40,
            resets_at="2026-08-21T07:00:00Z", source="api",
            source_sample_id="previous-cycle",
        )
        current = QuotaSample(
            provider="antigravity", account_key="default", bucket_id="gemini-5h",
            bucket_label="Gemini Models 5h", scope_type="model_group",
            scope_id="Gemini Models", window_kind="fixed", window_seconds=18000,
            effective_at="2026-08-21T12:00:00Z",
            collected_at="2026-08-21T12:00:00Z", used_pct=10,
            resets_at="2026-08-21T17:00:00Z", source="api",
            source_sample_id="current-cycle",
        )
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [previous, current])
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)
            report = build_report(
                quota_db, token_db, [parse_duration("6h")], "cost_usd",
                include_inactive=True,
            )

        bucket = report["buckets"][0]
        assert bucket["forecast_basis"] == "fixed_window"
        assert bucket["lookbacks"][0]["quality"] == "collecting_history"

    def test_pace_skips_a_login_that_was_switched_away_from(self):
        tmp_path = self.tmp_path
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [
                *_claude_snapshot("2026-10-03T01:00:00Z", 99, "2026-10-06T13:00:00Z", "org-one"),
                *_claude_snapshot("2026-10-03T06:00:00Z", 25, "2026-10-09T14:00:00Z", "org-two"),
            ])
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)
            report = build_report(quota_db, token_db, [parse_duration("6h")], "cost_usd",
                                  include_inactive=True)

        assert {bucket["account_key"] for bucket in report["buckets"]} == {"org-two"}

    def test_snapshot_insert_is_idempotent(self):
        tmp_path = self.tmp_path
        with connect(tmp_path / "quota.sqlite3") as db:
            assert insert_samples(db, [_sample("2026-08-21T06:00:00Z", 14)]) == 1
            assert insert_samples(db, [_sample("2026-08-21T06:00:00Z", 14)]) == 0

    def test_report_compares_quota_and_token_derived_pace(self):
        tmp_path = self.tmp_path
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [
                _sample("2026-08-21T06:00:00Z", 14),
                _sample("2026-08-21T12:00:00Z", 20),
            ])
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)
            token_db.executemany(
                "INSERT INTO tokens VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("Codex", "gpt-5.6-sol", "default", 1000, 4.0,
                     "2026-08-20T18:00:00Z"),
                    ("Codex", "gpt-5.6-terra", "default", 2000, 6.0,
                     "2026-08-21T07:00:00Z"),
                ],
            )

            report = build_report(quota_db, token_db, [parse_duration("6h")], "cost_usd")

        bucket = report["buckets"][0]
        interval = bucket["lookbacks"][0]
        assert bucket["token_calibration"]["api_usd_per_quota_percentage_point"] == 0.5
        assert interval["quota_delta_pp_per_hour"] == 1.0
        assert interval["token_derived_pp_per_hour"] == 2.0
        assert interval["model_mix"][0]["model"] == "gpt-5.6-terra"
        assert interval["quality"] == "full"
        assert bucket["forecast_basis"] == "reported_openai_reset"
        assert bucket["target_percentage_points_per_hour"] == approx(5 / 3)
        assert interval["observed_pace_source"] == "quota_delta"
        assert interval["uncapped_projected_used_pct"] == approx(68)
        assert interval["headroom_pct"] == approx(2 / 3 * 100)
        assert interval["guidance"] == "headroom 67%"

    def test_flat_quantized_quota_uses_token_derived_pace(self):
        tmp_path = self.tmp_path
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [
                _sample("2026-08-21T06:00:00Z", 20),
                _sample("2026-08-21T12:00:00Z", 20),
            ])
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)
            token_db.executemany(
                "INSERT INTO tokens VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("Codex", "gpt-5.6-sol", "default", 1000, 4.0,
                     "2026-08-20T18:00:00Z"),
                    ("Codex", "gpt-5.6-sol", "default", 1000, 2.0,
                     "2026-08-21T07:00:00Z"),
                ],
            )
            report = build_report(
                quota_db, token_db, [parse_duration("6h")], "cost_usd"
            )

        interval = report["buckets"][0]["lookbacks"][0]
        assert interval["quota_delta_pp_per_hour"] == 0
        assert interval["token_derived_pp_per_hour"] == approx(10 / 9)
        assert interval["observed_pace_source"] == "token_derived"

    def test_pace_table_columns_are_aligned(self):
        tmp_path = self.tmp_path
        capsys = self.capsys
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [
                _sample("2026-08-21T06:00:00Z", 14),
                _sample("2026-08-21T12:00:00Z", 20),
            ])
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)
            token_db.executemany(
                "INSERT INTO tokens VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("Codex", "gpt-5.6-sol", "default", 1000, 4.0,
                     "2026-08-20T18:00:00Z"),
                    ("Codex", "gpt-5.6-sol", "default", 1000, 6.0,
                     "2026-08-21T07:00:00Z"),
                ],
            )
            report = build_report(quota_db, token_db, [parse_duration("6h")], "cost_usd")

        agy_bucket = deepcopy(report["buckets"][0])
        agy_bucket.update({
            "provider": "antigravity",
            "bucket_label": "Gemini Models weekly",
            "scope_id": "Gemini Models",
            "window_seconds": 604800,
            "target_percentage_points_per_hour": None,
            "forecast_basis": None,
        })
        report["buckets"].append(agy_bucket)

        print_report(report)
        output = capsys.readouterr().out
        lines = output.splitlines()
        header = next(line for line in lines if "projected" in line)
        row = next(line for line in lines if line.startswith("codex"))
        for heading, value in (
            ("quota pp/h", "1.000"),
            ("token pp/h", "2.000"),
            ("local $/h", "$1.000"),
        ):
            assert header.index(heading) + len(heading) == row.index(value) + len(value)
        assert "quality" not in header
        assert "target pp/h" not in header
        assert "model mix" not in header
        assert "gpt-5.6-sol" not in output
        assert "target pace: codex Primary 1.667 quota pp/h" in output
        assert sum("projected" in line for line in lines) == 1
        assert any(line.startswith("agy") for line in lines)
        assert "Gemini (weekly)" in output
        assert (
            "Projection unavailable for agy Gemini (weekly): window semantics are unknown; "
            "quota pp/h is a net trend only."
        ) in output
        assert "Lookback coverage:" not in output

        print_report(report, show_model_mix=True)
        output = capsys.readouterr().out
        header = next(line for line in output.splitlines() if "projected" in line)
        assert "model mix" in header
        assert "gpt-5.6-sol 100%" in output

    def test_pace_table_hides_incomplete_lookbacks(self):
        tmp_path = self.tmp_path
        capsys = self.capsys
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [
                _sample("2026-08-21T06:00:00Z", 14),
                _sample("2026-08-21T12:00:00Z", 20),
            ])
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)
            report = build_report(
                quota_db,
                token_db,
                [parse_duration("4h"), parse_duration("8h")],
                "cost_usd",
                include_inactive=True,
            )

        print_report(report)
        output = capsys.readouterr().out
        assert "quality" not in output
        assert "Lookback coverage:" not in output
        assert "4h" not in output
        assert "8h" not in output
        assert output.rstrip().endswith("No complete lookbacks yet.")
        qualities = [interval["quality"] for interval in report["buckets"][0]["lookbacks"]]
        assert qualities == ["partial", "collecting_history"]

    def test_report_hides_inactive_buckets_by_default(self):
        tmp_path = self.tmp_path
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [
                _sample("2026-08-21T06:00:00Z", 14),
                _sample("2026-08-21T12:00:00Z", 14),
            ])
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)

            filtered = build_report(quota_db, token_db, [parse_duration("6h")], "cost_usd")
            complete = build_report(
                quota_db,
                token_db,
                [parse_duration("6h")],
                "cost_usd",
                include_inactive=True,
            )

        assert filtered["buckets"] == []
        assert filtered["hidden_bucket_count"] == 1
        assert complete["bucket_filter"] == "all"
        assert len(complete["buckets"]) == 1

    def test_quota_history_estimates_full_cost_from_last_period_observation(self):
        db = duckdb.connect()
        db.execute("""
            CREATE TABLE tokens (
                tool VARCHAR,
                model VARCHAR,
                rate_mode VARCHAR,
                source_kind VARCHAR,
                input_tokens BIGINT,
                output_tokens BIGINT,
                cache_write_tokens BIGINT,
                cache_read_tokens BIGINT,
                total_tokens BIGINT,
                cost_usd DOUBLE,
                timestamp VARCHAR,
                quota_used_pct DOUBLE,
                quota_resets_at BIGINT,
                quota_window_minutes INTEGER,
                quota_limit_id VARCHAR
            )
        """)
        first_reset = int(datetime(2026, 8, 20, tzinfo=timezone.utc).timestamp())
        second_reset = int(datetime(2026, 8, 27, tzinfo=timezone.utc).timestamp())
        unused_reset = int(datetime(2026, 9, 3, tzinfo=timezone.utc).timestamp())
        db.executemany(
            "INSERT INTO tokens VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("Codex", "gpt-5.6-sol", "default", "native", 10, 5, 0, 85, 100,
                 10.0, "2026-08-14T00:00:00Z", 20, first_reset, 10080, "codex"),
                ("Codex", "gpt-5.6-sol", "default", "native", 20, 5, 0, 75, 100,
                 15.0, "2026-08-19T23:00:00Z", 50, first_reset, 10080, "codex"),
                ("Codex", "gpt-5.6-sol", "default", "native", 0, 0, 0, 0, 0,
                 0.0, "2026-08-19T23:30:00Z", 50, first_reset + 20, 10080, "codex"),
                ("Codex", "gpt-5.6-sol", "default", "native", 10, 5, 0, 85, 100,
                 20.0, "2026-08-21T00:00:00Z", 25, second_reset, 10080, "codex"),
                ("Codex", "gpt-5.6-sol", "default", "native", 10, 5, 0, 85, 100,
                 5.0, "2026-08-28T00:00:00Z", 0, unused_reset, 10080, "codex"),
            ],
        )

        db.execute("ALTER TABLE tokens ADD COLUMN pricing VARCHAR DEFAULT 'model-prices'")
        report = build_quota_history(db, "cost_usd", "test-rate-card")

        first, second = report["periods"]
        assert len(report["periods"]) == 2
        assert report["pricing_basis"] == "test-rate-card"
        assert first["used_pct"] == 50
        assert first["api_equivalent_cost_usd"] == 25
        assert first["full_quota_api_equivalent_cost_estimate_usd"] == 50
        assert first["cached_input_pct"] == 160 / 190 * 100
        assert first["pricing_flags"] == []
        assert second["full_quota_api_equivalent_cost_estimate_usd"] == 80
        assert second["change_from_previous_estimate_pct"] == approx(60)

    def test_quota_history_combines_recorded_snapshots_and_embedded_limit_hits(self):
        tmp_path = self.tmp_path
        db = duckdb.connect()
        db.execute("""
            CREATE TABLE tokens (
                tool VARCHAR, model VARCHAR, rate_mode VARCHAR, source_kind VARCHAR,
                input_tokens BIGINT, output_tokens BIGINT, cache_write_tokens BIGINT,
                cache_read_tokens BIGINT, total_tokens BIGINT, cost_usd DOUBLE,
                timestamp VARCHAR, quota_used_pct DOUBLE, quota_resets_at BIGINT,
                quota_window_minutes INTEGER, quota_limit_id VARCHAR
            )
        """)
        zcode_reset = int(datetime(2026, 9, 27, 19, 11, 57, tzinfo=timezone.utc).timestamp())
        db.executemany(
            "INSERT INTO tokens VALUES (?, ?, '', 'native', 0, 0, 0, 0, 100, ?, ?, ?, ?, ?, ?)",
            [
                ("Zcode", "zai/glm-5.3", 3.0, "2026-09-27T14:12:00Z", None, None, None, None),
                ("Zcode", "zai/glm-5.3", 3.0, "2026-09-27T15:00:00Z", None, None, None, None),
                ("Zcode", "zai/glm-5.3", 0.0, "2026-09-27T15:21:03Z", 100, zcode_reset, 300, "5h"),
                ("Zcode", "zai/glm-5.3", 5.0, "2026-09-27T16:00:00Z", None, None, None, None),
                ("Claude Code", "claude-opus-5-5", 40.0, "2026-09-25T00:00:00Z",
                 None, None, None, None),
                ("Claude Code", "claude-fable-5-1", 10.0, "2026-09-25T00:00:00Z",
                 None, None, None, None),
            ],
        )
        with connect(tmp_path / "quota.sqlite3") as connection:
            insert_samples(connection, [
                *normalize_zai({
                    "updated_at": "2026-09-27T15:30:00Z",
                    "limits": [{"type": "CREDIT_LIMIT", "unit": 3, "number": 5, "used": 1000,
                                "limit": 2000, "pct": 50,
                                "resets_at": "2026-09-27T20:24:52Z"}],
                }, datetime(2026, 9, 27, 15, 30, tzinfo=timezone.utc)),
                *normalize_anthropic({
                    "plan": "max_20x",
                    "updated_at": "2026-09-26T00:00:00Z",
                    "7d": {"pct": 50, "resets_at": "2026-09-29T13:00:00Z"},
                    "7d_fable": {"pct": 20, "resets_at": "2026-09-29T13:00:00Z"},
                }, datetime(2026, 9, 26, tzinfo=timezone.utc)),
            ])
            db.execute("ALTER TABLE tokens ADD COLUMN pricing VARCHAR DEFAULT 'model-prices'")
            report = build_quota_history(db, "cost_usd", "test-rate-card", connection)

        periods = {(item["tool"], item["limit_id"], item["resets_at"]): item
                   for item in report["periods"]}
        assert report["report_type"] == "quota_history"
        hit = periods[("Zcode", "5h", "2026-09-27T19:11:57Z")]
        assert hit["used_pct"] == 100
        assert hit["window_start"] == "2026-09-27T14:11:57Z"
        assert hit["full_quota_api_equivalent_cost_estimate_usd"] == 6.0
        assert hit["change_from_previous_estimate_pct"] is None
        recorded = periods[("Zcode", "5h", "2026-09-27T20:24:52Z")]
        assert recorded["full_quota_api_equivalent_cost_estimate_usd"] == 0.0
        assert recorded["change_from_previous_estimate_pct"] == -100
        week = periods[("Claude Code", "7d", "2026-09-29T13:00:00Z")]
        assert week["full_quota_api_equivalent_cost_estimate_usd"] == 100
        fable = periods[("Claude Code", "7d_fable", "2026-09-29T13:00:00Z")]
        assert fable["full_quota_api_equivalent_cost_estimate_usd"] == 50
        assert [item["model"] for item in fable["model_mix"]] == ["claude-fable-5-1"]

    def test_two_claude_logins_get_separate_quota_periods_and_value(self):
        tmp_path = self.tmp_path
        db = duckdb.connect()
        db.execute("""
            CREATE TABLE tokens (
                tool VARCHAR, model VARCHAR, rate_mode VARCHAR, source_kind VARCHAR,
                input_tokens BIGINT, output_tokens BIGINT, cache_write_tokens BIGINT,
                cache_read_tokens BIGINT, total_tokens BIGINT, cost_usd DOUBLE,
                timestamp VARCHAR, quota_used_pct DOUBLE, quota_resets_at BIGINT,
                quota_window_minutes INTEGER, quota_limit_id VARCHAR, pricing VARCHAR
            )
        """)
        # $30 of usage on the first login, then $10 after switching to the second login,
        # whose week began before the switch.
        db.executemany(
            "INSERT INTO tokens VALUES ('Claude Code', 'claude-opus-5-5', '', 'native', "
            "0, 0, 0, 0, 100, ?, ?, NULL, NULL, NULL, NULL, 'model-prices')",
            [(30.0, "2026-10-02T20:00:00Z"), (10.0, "2026-10-03T05:00:00Z")],
        )
        with connect(tmp_path / "quota.sqlite3") as connection:
            insert_samples(connection, [
                # The first login, recorded before snapshots named their login.
                *_claude_snapshot("2026-10-03T01:00:00Z", 60, "2026-10-06T13:00:00Z"),
                # The second login: one snapshot before labeling, then a labeled one.
                *_claude_snapshot("2026-10-03T01:20:00Z", 5, "2026-10-09T14:00:00Z"),
                *_claude_snapshot("2026-10-03T06:00:00Z", 25, "2026-10-09T14:00:00Z", "org-two"),
            ])
            report = build_quota_history(db, "cost_usd", "test-rate-card", connection)
            relabeled = tuple(connection.execute(
                "SELECT account_key FROM quota_samples WHERE effective_at LIKE '2026-10-03T01:20%'"
            ).fetchone())

        assert relabeled == ("org-two",)
        weeks = {item["account"]: item for item in report["periods"] if item["limit_id"].endswith("7d")}
        assert set(weeks) == {"default", "org-two"}
        # Each login's week counts only its own usage, split at the first login's last snapshot.
        assert weeks["default"]["api_equivalent_cost_usd"] == 30
        assert weeks["org-two"]["api_equivalent_cost_usd"] == 10
        value = build_value_report(report, {"Claude Code": ("max_20x", 200.0)})
        assert [(item["account"], item["full_week_api_equivalent_usd"]) for item in value["tools"]] == [
            ("default", 50.0), ("org-two", 40.0)]

    def test_quota_history_counts_aop_runs_on_the_same_subscription(self):
        db = duckdb.connect()
        db.execute("""
            CREATE TABLE tokens (
                tool VARCHAR, model VARCHAR, rate_mode VARCHAR, source_kind VARCHAR,
                input_tokens BIGINT, output_tokens BIGINT, cache_write_tokens BIGINT,
                cache_read_tokens BIGINT, total_tokens BIGINT, cost_usd DOUBLE,
                timestamp VARCHAR, quota_used_pct DOUBLE, quota_resets_at BIGINT,
                quota_window_minutes INTEGER, quota_limit_id VARCHAR
            )
        """)
        reset = int(datetime(2026, 8, 20, tzinfo=timezone.utc).timestamp())
        db.executemany(
            "INSERT INTO tokens VALUES ('Codex', 'gpt-5.6-sol', '', ?, 0, 0, 0, 0, 100, ?, ?, ?, ?, ?, ?)",
            [
                ("native", 10.0, "2026-08-14T00:00:00Z", 40, reset, 10080, "codex"),
                ("aop", 10.0, "2026-08-15T00:00:00Z", None, None, None, None),
                ("aop", None, "2026-08-16T00:00:00Z", None, None, None, None),
                ("native", 0.0, "2026-08-19T00:00:00Z", 50, reset, 10080, "codex"),
            ],
        )

        db.execute("""
            ALTER TABLE tokens ADD COLUMN pricing VARCHAR;
            UPDATE tokens SET pricing = CASE WHEN cost_usd IS NULL THEN 'unpriced' ELSE 'model-prices' END;
        """)
        period, = build_quota_history(db, "cost_usd", "test-rate-card")["periods"]

        assert period["api_equivalent_cost_usd"] == 20
        assert period["total_tokens"] == 400
        assert period["full_quota_api_equivalent_cost_estimate_usd"] == 40
        assert {item["pricing"] for item in period["model_mix"]} == {"model-prices", "unpriced"}
        assert "unpriced_models" in period["pricing_flags"]

    def test_value_report_uses_the_latest_week_above_minimum_usage(self):
        history = {"pricing_basis": "test", "periods": [
            _history_period("Claude Code", "2026-09-22T13:00:00Z", 90, 900.0, 1_000_000_000, []),
            _history_period("Claude Code", "2026-09-29T13:00:00Z", 50, 1000.0, 1_000_000_000, []),
            _history_period("Claude Code", "2026-10-06T13:00:00Z", 15, 50.0, 10_000_000, []),
            _history_period("Claude Code", "2026-10-01T00:00:00Z", 100, 5.0, 1000, [],
                            window_minutes=300),
            _history_period("Agy", "2026-09-30T00:00:00Z", 20, 3.0, 5_000_000,
                            ["fallback_pricing"]),
            _history_period("Codex", "2026-09-30T00:00:00Z", 10, 3.0, 5_000_000, []),
        ]}

        report = build_value_report(history, {"Claude Code": ("max_20x", 200), "Gemini": ("ultra", 250)})

        agy, claude = report["tools"]
        assert claude["resets_at"] == "2026-09-29T13:00:00Z"
        assert claude["full_week_api_equivalent_usd"] == 2000
        assert claude["monthly_api_equivalent_usd"] == approx(2000 * 365.25 / 84)
        assert claude["api_equivalent_multiple"] == approx(2000 * 365.25 / 84 / 200)
        assert claude["api_usd_per_million_tokens"] == approx(1.0)
        assert claude["effective_usd_per_million_tokens"] == approx(
            1.0 / claude["api_equivalent_multiple"]
        )
        assert agy["plan"] == "ultra"
        assert agy["pricing_flags"] == ["fallback_pricing"]
