"""Tests for quota snapshot recording and token pace comparison."""

from copy import deepcopy
from datetime import datetime, timezone

import duckdb
import pytest

from clanker_analytics.quota import (
    QuotaSample,
    build_quota_history,
    build_report,
    connect,
    insert_samples,
    normalize_antigravity,
    normalize_anthropic,
    normalize_openai,
    parse_duration,
    print_report,
)


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


def test_parse_duration_accepts_arbitrary_compact_periods():
    assert parse_duration("3h") == 10800
    assert parse_duration("7h") == 25200
    assert parse_duration("2w") == 1209600


def test_openai_adapter_avoids_legacy_alias_duplicates():
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


@pytest.mark.parametrize(
    ("plan", "expected"),
    [
        ("ai", "fixed"),
        ("pro", "fixed"),
        ("max_20x", "fixed"),
        ("enterprise", "rolling"),
        ("enterprise_premium", "rolling"),
        ("unknown", "unknown"),
    ],
)
def test_anthropic_adapter_classifies_windows_by_plan(plan, expected):
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


def test_antigravity_adapter_classifies_reset_windows_as_fixed():
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


def test_fixed_window_lookback_does_not_cross_reset(tmp_path):
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


def test_snapshot_insert_is_idempotent(tmp_path):
    with connect(tmp_path / "quota.sqlite3") as db:
        assert insert_samples(db, [_sample("2026-08-21T06:00:00Z", 14)]) == 1
        assert insert_samples(db, [_sample("2026-08-21T06:00:00Z", 14)]) == 0


def test_report_compares_quota_and_token_derived_pace(tmp_path):
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
    assert bucket["target_percentage_points_per_hour"] == pytest.approx(5 / 3)
    assert interval["observed_pace_source"] == "quota_delta"
    assert interval["uncapped_projected_used_pct"] == pytest.approx(68)
    assert interval["headroom_pct"] == pytest.approx(2 / 3 * 100)
    assert interval["guidance"] == "headroom 67%"


def test_flat_quantized_quota_uses_token_derived_pace(tmp_path):
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
    assert interval["token_derived_pp_per_hour"] == pytest.approx(10 / 9)
    assert interval["observed_pace_source"] == "token_derived"


def test_pace_table_columns_are_aligned(tmp_path, capsys):
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


def test_pace_table_hides_incomplete_lookbacks(tmp_path, capsys):
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


def test_report_hides_inactive_buckets_by_default(tmp_path):
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


def test_quota_history_estimates_full_cost_from_last_period_observation():
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

    report = build_quota_history(db, "cost_usd", "test-rate-card")

    first, second = report["periods"]
    assert len(report["periods"]) == 2
    assert report["pricing_method"] == "constant_rate_card_applied_to_all_periods"
    assert first["used_pct"] == 50
    assert first["api_equivalent_cost_usd"] == 25
    assert first["full_quota_api_equivalent_cost_estimate_usd"] == 50
    assert first["cached_input_pct"] == 160 / 190 * 100
    assert first["quality"] == ["high_utilization"]
    assert second["full_quota_api_equivalent_cost_estimate_usd"] == 80
    assert second["change_from_previous_estimate_pct"] == pytest.approx(60)
    assert "early" not in second["quality"]
