"""Tests for quota snapshot recording and token pace comparison."""

from copy import deepcopy
from datetime import datetime, timezone
import json
import re
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
    normalize_usage_v1,
    parse_duration,
    print_report,
)

from support import Case, approx, raises


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


def _usage_samples(provider, when, windows, account=None, plan=None):
    """Samples from a usage-v1 document with these (name, seconds, scope, used_pct,
    resets_at) windows."""
    document = {
        "schema": "usage-v1", "provider": provider, "account": account, "plan": plan,
        "status": "live", "error": None, "retrieved_at": when, "details": {},
        "windows": [{"name": name, "seconds": seconds, "scope": scope, "used_pct": used,
                     "resets_at": resets_at}
                    for name, seconds, scope, used, resets_at in windows],
    }
    return normalize_usage_v1(provider, document, datetime.fromisoformat(when.replace("Z", "+00:00")))


def _claude_snapshot(when, used, resets_at, account=None):
    return _usage_samples("anthropic", when, [("weekly", 604800, None, used, resets_at)],
                          account, "max_20x")


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

    def test_claude_login_without_an_account_file_is_unknown(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))

        assert quota.claude_login() == (None, None)

    def test_usage_v1_document_gives_samples_for_each_window(self):
        # Printed by clanker-usage 0.4.0 for its recorded Z.ai quota response.
        document = {
            "schema": "usage-v1", "provider": "zcode", "account": "3f2a9c1e5b7d4a60",
            "plan": "lite", "status": "live", "error": None,
            "retrieved_at": "2026-09-05T17:00:00+00:00",
            "windows": [
                {"name": "5-hour", "seconds": 18000, "scope": None, "used_pct": 1,
                 "resets_at": None},
                {"name": "weekly", "seconds": 604800, "scope": None, "used_pct": 20,
                 "resets_at": "2026-09-11T03:38:39.995000+00:00"},
            ],
            "details": {"limits": [
                {"type": "CREDIT_LIMIT", "unit": 3, "number": 5, "used": 1, "limit": 100,
                 "remaining": 99},
                {"type": "CREDIT_LIMIT", "unit": 6, "number": 1, "used": 2004,
                 "limit": 10000, "remaining": 7996},
            ]},
        }
        self.monkeypatch.setattr(quota.subprocess, "run", lambda *args, **kwargs:
                                 subprocess.CompletedProcess(args, 0, json.dumps(document), ""))

        session, week = quota.collect_provider("zai")

        assert (session.provider, session.account_key, session.plan) == ("zai", "3f2a9c1e5b7d4a60", "lite")
        assert (session.bucket_id, session.window_seconds, session.resets_at) == ("5h", 18000, None)
        assert (week.bucket_id, week.window_seconds, week.used_pct) == ("1w", 604800, 20)
        assert week.resets_at == "2026-09-11T03:38:40Z"
        assert week.effective_at == "2026-09-05T17:00:00Z"
        assert (week.scope_type, week.window_kind) == ("account", "fixed")

        scoped = {**document, "windows": [{"name": "weekly", "seconds": 604800,
                                           "scope": "fable", "used_pct": 3, "resets_at": None}]}
        [sample] = quota.normalize_usage_v1("anthropic", scoped, datetime.now(timezone.utc))
        assert (sample.bucket_id, sample.scope_type, sample.scope_id) == ("fable:1w", "model", "fable")

    def test_unavailable_usage_v1_document_is_an_error(self):
        document = {"schema": "usage-v1", "provider": "zcode", "account": None, "plan": None,
                    "status": "unavailable", "error": "no ZAI_API_KEY found",
                    "retrieved_at": None, "windows": [], "details": {}}
        self.monkeypatch.setattr(quota.subprocess, "run", lambda *args, **kwargs:
                                 subprocess.CompletedProcess(args, 1, json.dumps(document), ""))

        with raises(RuntimeError, match="no ZAI_API_KEY found"):
            quota.collect_provider("zai")

    def test_antigravity_history_continues_under_usage_v1_names_and_account(self):
        path = self.tmp_path / "quota.sqlite3"
        old = [QuotaSample(
            provider="antigravity", account_key="default", bucket_id=bucket_id,
            bucket_label=label, scope_type="model_group", scope_id=scope,
            window_kind="fixed", window_seconds=seconds, effective_at="2026-10-18T12:00:00Z",
            collected_at="2026-10-18T12:00:00Z", used_pct=10, resets_at="2026-10-24T15:41:29Z",
            source="api", source_sample_id=bucket_id)
            for bucket_id, label, scope, seconds in [
                ("gemini-5h", "Gemini Models 5h", "Gemini Models", 18000),
                ("gemini-weekly", "Gemini Models weekly", "Gemini Models", 604800),
                ("3p-5h", "Claude and GPT models 5h", "Claude and GPT models", 18000),
                ("3p-weekly", "Claude and GPT models weekly", "Claude and GPT models", 604800)]]
        with connect(path) as db:
            insert_samples(db, old)
            # As stored before the rename.
            db.execute("PRAGMA user_version = 1")
        # Printed by agy-usage 0.4.0 on this host, with the account replaced.
        document = {
            "schema": "usage-v1", "provider": "agy", "account": "f90eba0000000000",
            "plan": "Google AI Ultra", "status": "live", "error": None,
            "retrieved_at": "2026-10-19T03:54:42+00:00", "balance": None, "details": {},
            "windows": [
                {"name": "weekly", "seconds": 604800, "scope": "Gemini",
                 "used_pct": 14.583313, "resets_at": "2026-10-24T15:41:29+00:00"},
                {"name": "5-hour", "seconds": 18000, "scope": "Gemini",
                 "used_pct": 2.176905, "resets_at": "2026-10-19T05:11:17+00:00"},
                {"name": "weekly", "seconds": 604800, "scope": "Claude and GPT",
                 "used_pct": 0, "resets_at": "2026-10-26T03:54:42+00:00"},
                {"name": "5-hour", "seconds": 18000, "scope": "Claude and GPT",
                 "used_pct": 0, "resets_at": "2026-10-19T08:54:42+00:00"},
            ],
        }
        self.monkeypatch.setattr(quota.subprocess, "run", lambda *args, **kwargs:
                                 subprocess.CompletedProcess(args, 0, json.dumps(document), ""))

        quota.record_quota(path, ["antigravity"])

        with connect(path) as db:
            series = db.execute("""
                SELECT account_key, bucket_id, bucket_label, scope_type, scope_id, count(*)
                FROM quota_samples GROUP BY 1, 2, 3, 4, 5 ORDER BY 2
            """).fetchall()
        assert [tuple(row) for row in series] == [
            ("f90eba0000000000", "Claude and GPT:1w", "Claude and GPT weekly", "model", "Claude and GPT", 2),
            ("f90eba0000000000", "Claude and GPT:5h", "Claude and GPT 5-hour", "model", "Claude and GPT", 2),
            ("f90eba0000000000", "Gemini:1w", "Gemini weekly", "model", "Gemini", 2),
            ("f90eba0000000000", "Gemini:5h", "Gemini 5-hour", "model", "Gemini", 2),
        ]

    def test_codex_history_is_renamed_by_window_length_and_scope(self):
        path = self.tmp_path / "quota.sqlite3"
        def sample(bucket_id, scope, seconds, effective_at):
            return QuotaSample(
                provider="openai", account_key="default", bucket_id=bucket_id,
                bucket_label=bucket_id, scope_type="model" if scope else "account",
                scope_id=scope, window_kind="fixed", window_seconds=seconds,
                effective_at=effective_at, collected_at=effective_at, used_pct=1,
                resets_at=None, source="api", source_sample_id=bucket_id + effective_at)
        with connect(path) as db:
            insert_samples(db, [
                # On one plan the weekly window was primary; on the next it is secondary.
                sample("primary", "", 604800, "2026-10-01T00:00:00Z"),
                sample("primary", "", 18000, "2026-10-17T00:00:00Z"),
                sample("secondary", "", 604800, "2026-10-17T00:00:00Z"),
                sample("additional:gpt-5-3-codex-spark:primary", "GPT-5.3-Codex-Spark", 18000,
                       "2026-09-01T00:00:00Z"),
            ])
            db.execute("PRAGMA user_version = 2")
        with connect(path) as db:
            rows = db.execute("""
                SELECT bucket_id, bucket_label, count(*) FROM quota_samples
                GROUP BY 1, 2 ORDER BY 1
            """).fetchall()
        assert [tuple(row) for row in rows] == [
            ("1w", "weekly", 2), ("5h", "5-hour", 1),
            ("GPT-5.3-Codex-Spark:5h", "GPT-5.3-Codex-Spark 5-hour", 1)]

    def test_claude_history_moves_to_usage_v1_names_and_account(self):
        path = self.tmp_path / "quota.sqlite3"
        old = _usage_samples("anthropic", "2026-10-18T12:00:00Z", [
            ("5-hour", 18000, None, 10, "2026-10-18T14:00:00Z"),
            ("weekly", 604800, None, 40, "2026-10-23T14:00:00Z"),
            ("weekly", 604800, "Fable", 1, "2026-10-23T14:00:00Z"),
        ], "org-one", "max_20x")
        with connect(path) as db:
            insert_samples(db, old)
            # As ccusage before 0.4.0 stored them.
            db.execute("""UPDATE quota_samples SET bucket_id = CASE bucket_id
                WHEN '5h' THEN 'session' WHEN '1w' THEN '7d' ELSE '7d_fable' END,
                scope_id = CASE WHEN scope_id = 'Fable' THEN 'fable' ELSE scope_id END""")
            db.execute("INSERT INTO quota_accounts VALUES ('anthropic', 'org-one', 'one@example.com')")
            db.execute("PRAGMA user_version = 3")
        # Printed by ccusage 0.4.0 on this host, with the account replaced.
        document = {
            "schema": "usage-v1", "provider": "claude", "account": "e7390f1f00000000",
            "plan": "max_20x", "status": "live", "error": None,
            "retrieved_at": "2026-10-19T04:08:01+00:00", "balance": None, "details": {},
            "windows": [
                {"name": "5-hour", "seconds": 18000, "scope": None, "used_pct": 22,
                 "resets_at": "2026-10-19T04:59:59.946776+00:00"},
                {"name": "weekly", "seconds": 604800, "scope": None, "used_pct": 42,
                 "resets_at": "2026-10-23T13:59:59.946796+00:00"},
                {"name": "weekly", "seconds": 604800, "scope": "Fable", "used_pct": 0,
                 "resets_at": "2026-10-23T14:00:00+00:00"},
            ],
        }
        self.monkeypatch.setattr(quota.subprocess, "run", lambda *args, **kwargs:
                                 subprocess.CompletedProcess(args, 0, json.dumps(document), ""))
        self.monkeypatch.setattr(quota, "claude_login", lambda: ("org-one", "one@example.com"))

        quota.record_quota(path, ["anthropic"])

        with connect(path) as db:
            series = db.execute("""
                SELECT account_key, bucket_id, scope_id, count(*), max(resets_at)
                FROM quota_samples GROUP BY 1, 2, 3 ORDER BY 2
            """).fetchall()
            labels = db.execute("SELECT * FROM quota_accounts").fetchall()
        assert [tuple(row) for row in series] == [
            ("e7390f1f00000000", "1w", "", 2, "2026-10-23T14:00:00Z"),
            ("e7390f1f00000000", "5h", "", 2, "2026-10-19T05:00:00Z"),
            ("e7390f1f00000000", "Fable:1w", "Fable", 2, "2026-10-23T14:00:00Z"),
        ]
        assert [tuple(row) for row in labels] == [("anthropic", "e7390f1f00000000", "one@example.com")]

    def test_unnamed_history_stays_unnamed_when_a_provider_has_two_accounts(self):
        path = self.tmp_path / "quota.sqlite3"
        def sample(account, effective_at):
            return QuotaSample(
                provider="zai", account_key=account, bucket_id="1w", bucket_label="1w",
                scope_type="account", scope_id="", window_kind="fixed", window_seconds=604800,
                effective_at=effective_at, collected_at=effective_at, used_pct=1,
                resets_at=None, source="api", source_sample_id=account + effective_at)
        with connect(path) as db:
            insert_samples(db, [sample("default", "2026-09-01T00:00:00Z"),
                                sample("first", "2026-09-02T00:00:00Z")])
            new = [sample("second", "2026-09-03T00:00:00Z")]
            insert_samples(db, new)
            assert quota.adopt_unnamed_history(db, new) == 0

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

    def test_pace_measures_each_account_over_its_logged_in_time(self):
        tmp_path = self.tmp_path
        capsys = self.capsys

        def snapshots(account, resets_at, readings):
            return [sample for when, used in readings
                    for sample in _claude_snapshot(when, used, resets_at, account)]

        with connect(tmp_path / "quota.sqlite3") as quota_db:
            insert_samples(quota_db, [
                # Logged in for 10 minutes, then switched away from with its week still open.
                *snapshots("org-one", "2026-10-13T13:00:00Z", [
                    ("2026-10-10T23:30:00Z", 98), ("2026-10-10T23:35:00Z", 99),
                    ("2026-10-10T23:40:00Z", 100)]),
                # A day into its week before its first login, then two 10-minute stretches
                # with five hours logged out between them.
                *snapshots("org-two", "2026-10-16T14:00:00Z", [
                    ("2026-10-10T23:45:00Z", 1), ("2026-10-10T23:50:00Z", 2),
                    ("2026-10-10T23:55:00Z", 3), ("2026-10-11T05:00:00Z", 3),
                    ("2026-10-11T05:05:00Z", 4), ("2026-10-11T05:10:00Z", 5)]),
            ])
            quota_db.execute(
                "INSERT INTO quota_accounts VALUES ('anthropic', 'org-two', 'second@example.com')")
            token_db = duckdb.connect()
            token_db.execute("""
                CREATE TABLE tokens (
                    tool VARCHAR, model VARCHAR, rate_mode VARCHAR,
                    total_tokens BIGINT, cost_usd DOUBLE, timestamp VARCHAR
                )
            """)
            report = build_report(quota_db, token_db, [parse_duration("5m")], "cost_usd",
                                  include_inactive=True)

        current = report["buckets"][0]
        assert current["account_label"] == "second"
        # 4 points in 20 logged-in minutes is 12 points an hour; even use is 100 in 168 hours.
        assert current["window_pace"]["logged_in_hours"] == approx(20 / 60)
        assert current["window_pace"]["quota_pp_per_hour"] == approx(12)
        assert current["window_pace"]["pace_ratio"] == approx(12 * 168 / 100)
        away = report["logged_out_buckets"][0]
        assert away["account_key"] == "org-one"
        assert away["logged_out_since"] == "2026-10-10T23:40:00Z"
        assert away["window_pace"]["quota_pp_per_hour"] == approx(12)

        print_report(report)
        output = capsys.readouterr().out
        assert re.search(r"claude\s+second\s+Week \(all models\)\s+5\.0%\s+0\.3\s+12\.000\s+"
                         r"20\.16x\s+logged in", output)
        assert re.search(r"claude\s+org-one\s+Week \(all models\)\s+100\.0%.*"
                         r"logged out since 2026-10-10 23:40Z", output)

    def test_recording_stores_the_claude_login_email(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        (tmp_path / ".claude.json").write_text(json.dumps(
            {"oauthAccount": {"organizationUuid": "org-second",
                              "emailAddress": "second@example.com"}}))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path))
        output = json.dumps({
            "schema": "usage-v1", "provider": "claude", "account": "account-second",
            "plan": "max_20x", "status": "live", "error": None,
            "retrieved_at": "2026-10-03T01:14:00Z", "details": {},
            "windows": [{"name": "weekly", "seconds": 604800, "scope": None, "used_pct": 1,
                         "resets_at": "2026-10-09T14:00:00Z"}]})
        commands = []
        def run(cmd, **kwargs):
            commands.append(cmd[1:])
            return subprocess.CompletedProcess(cmd, 0, output, "")
        monkeypatch.setattr(quota.subprocess, "run", run)

        quota.record_quota(tmp_path / "quota.sqlite3", ["anthropic"])

        assert commands == [["-m", "clanker_usage", "claude", "json"]]
        with connect(tmp_path / "quota.sqlite3") as quota_db:
            assert quota.account_labels(quota_db) == {("anthropic", "account-second"): "second"}

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
                *_usage_samples("zai", "2026-09-27T15:30:00Z", [
                    ("5-hour", 18000, None, 50, "2026-09-27T20:24:52Z")]),
                *_usage_samples("anthropic", "2026-09-26T00:00:00Z", [
                    ("weekly", 604800, None, 50, "2026-09-29T13:00:00Z"),
                    ("weekly", 604800, "Fable", 20, "2026-09-29T13:00:00Z"),
                ], plan="max_20x"),
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
        week = periods[("Claude Code", "1w", "2026-09-29T13:00:00Z")]
        assert week["full_quota_api_equivalent_cost_estimate_usd"] == 100
        fable = periods[("Claude Code", "Fable:1w", "2026-09-29T13:00:00Z")]
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
        weeks = {item["account"]: item for item in report["periods"] if item["limit_id"].split(":")[-1] == "1w"}
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

    def test_quota_history_names_the_plan_each_period_reported(self):
        tmp_path = self.tmp_path
        db = duckdb.connect()
        db.execute("""
            CREATE TABLE tokens (
                tool VARCHAR, model VARCHAR, rate_mode VARCHAR, source_kind VARCHAR,
                input_tokens BIGINT, output_tokens BIGINT, cache_write_tokens BIGINT,
                cache_read_tokens BIGINT, total_tokens BIGINT, cost_usd DOUBLE,
                timestamp VARCHAR, quota_used_pct DOUBLE, quota_resets_at BIGINT,
                quota_window_minutes INTEGER, quota_limit_id VARCHAR, quota_plan VARCHAR,
                pricing VARCHAR
            )
        """)
        first = int(datetime(2026, 10, 10, 5, 24, tzinfo=timezone.utc).timestamp())
        db.executemany(
            "INSERT INTO tokens VALUES ('Codex', 'gpt-5.5', '', 'native', 0, 0, 0, 0, 100, 1.0, "
            "?, ?, ?, 10080, 'codex', ?, 'model-prices')",
            [("2026-10-04T00:00:00Z", 20, first, "pro"),
             ("2026-10-06T00:00:00Z", 35, first, "pro"),
             ("2026-10-12T03:00:00Z", 5, first + 7 * 86400, "plus")],
        )
        with connect(tmp_path / "quota.sqlite3") as connection:
            insert_samples(connection, _claude_snapshot(
                "2026-10-05T00:00:00Z", 40, "2026-10-09T14:00:00Z", "org-one"))
            report = build_quota_history(db, "cost_usd", "test", connection)

        assert [(period["tool"], period["plan"]) for period in report["periods"]] == [
            ("Claude Code", "max_20x"), ("Codex", "pro"), ("Codex", "plus")]

    def test_value_report_prices_a_week_at_the_plan_it_ran_on(self):
        week = _history_period("Codex", "2026-10-10T05:24:00Z", 36, 510.0, 1_000_000_000, [])
        history = {"pricing_basis": "test", "periods": [{**week, "plan": "pro"}]}

        def plan(current, forced=()):
            item = build_value_report(history, {"Codex": current}, forced)["tools"][0]
            return item["plan"], item["plan_usd_per_month"]

        # A downgrade after the week ended does not reprice it.
        assert plan(("plus", 20)) == ("pro", 200)
        # Codex reports every Pro tier as "pro", so a configured tier names it exactly.
        assert plan(("pro_100", 100)) == ("pro_100", 100)
        # --plan applies to every week.
        assert plan(("plus", 20), forced={"Codex"}) == ("plus", 20)
        # A week whose snapshots named no plan uses the current one.
        history["periods"][0]["plan"] = None
        assert plan(("plus", 20)) == ("plus", 20)

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
