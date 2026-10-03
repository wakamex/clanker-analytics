from datetime import datetime, timedelta, timezone

import duckdb

from clanker_analytics.diagnose import build_cache_health, build_diagnosis, build_throughput
from clanker_analytics.main import TOKEN_SCHEMA
from clanker_analytics.pricing import price_tokens

from support import Case, approx

NOW = datetime(2026, 10, 3, 12, tzinfo=timezone.utc)


def _db(rows):
    """A priced tokens table; each row is (minutes before NOW, transcript, thread,
    input, cache write, one-hour cache write, cache read, output)."""
    db = duckdb.connect()
    db.execute(f"CREATE TABLE tokens ({TOKEN_SCHEMA})")
    for minutes, transcript, thread, uncached, written, written_1h, read, output in rows:
        ts = (NOW - timedelta(minutes=minutes)).isoformat()
        db.execute("""
            INSERT INTO tokens (tool, project, session, date, model, input_tokens, output_tokens,
                cache_write_tokens, cache_read_tokens, cache_write_1h_tokens, total_tokens,
                execution_type, source_kind, source_file, timestamp)
            VALUES ('Claude Code', 'demo', ?, ?, 'claude-opus-5-5', ?, ?, ?, ?, ?, 0, ?,
                    'native', ?, ?)
        """, [transcript, ts[:10], uncached, output, written, read, written_1h, thread,
              transcript, ts])
    price_tokens(db, all_rows=True)
    return db


class TestDiagnose(Case):
    def test_cold_starts_are_classified_by_the_gap_and_cache_lifetime(self):
        db = _db([
            # A subagent: its first request, a warm one, a cold start after idling 10 minutes
            # past its five-minute cache, and a cold start with no idle gap.
            (100, "sub", "subagent", 0, 300_000, 0, 0, 100),
            (99, "sub", "subagent", 0, 1_000, 0, 300_000, 100),
            (89, "sub", "subagent", 0, 301_000, 0, 0, 100),
            (88, "sub", "subagent", 0, 302_000, 0, 0, 100),
            # A main thread on the one-hour cache: a 30-minute gap keeps it warm, so a cold
            # start then has no idle gap to blame.
            (90, "main", "interactive", 0, 100_000, 100_000, 0, 100),
            (60, "main", "interactive", 0, 100_000, 100_000, 0, 100),
        ])

        report = build_cache_health(db, NOW - timedelta(days=1))
        threads = {item["key"]: item for item in report["by_thread"]}

        sub = threads["Claude Code subagent"]
        assert (sub["cold_first"], sub["cold_expired"], sub["cold_other"]) == (1, 1, 1)
        main = threads["Claude Code interactive"]
        assert (main["cold_first"], main["cold_expired"], main["cold_other"]) == (1, 0, 1)
        assert main["one_hour_share"] == 1.0
        # The warm subagent request read 300k tokens for each 1k it wrote.
        assert sub["reads_per_write"] == approx(300_000 / 904_000)

    def test_one_hour_subagent_simulation_turns_idle_cold_starts_into_reads(self):
        db = _db([
            (100, "sub", "subagent", 0, 300_000, 0, 0, 100),
            (80, "sub", "subagent", 0, 300_000, 0, 0, 100),
        ])

        simulation = build_cache_health(db, NOW - timedelta(days=1))["one_hour_subagents"]

        # Opus 5.5: five-minute writes cost $5/M, one-hour writes $8/M, reads $0.20/M.
        assert simulation["actual"] == approx(0.6 * 5)
        assert simulation["one_hour"] == approx(0.3 * 8 + 0.3 * 0.2)

    def test_diagnosis_compares_periods_per_day_and_ranks_the_largest_change(self):
        day = 24 * 60
        db = _db([
            # Baseline: one cheap request a day for two days before the recent day.
            (2 * day + 60, "a", "interactive", 0, 0, 0, 1_000_000, 0),
            (day + 60, "a", "interactive", 0, 0, 0, 1_000_000, 0),
            # Recent day: a subagent writing 1M tokens to its cache.
            (60, "b", "subagent", 0, 1_000_000, 0, 0, 0),
        ])

        report = build_diagnosis(db, timedelta(days=1), timedelta(days=2), NOW)

        assert report["summary"]["baseline"]["cost_per_day"] == approx(0.2)
        assert report["summary"]["recent"]["cost_per_day"] == approx(5.0)
        first = report["changes"]["thread"][0]
        assert (first["key"], first["change_per_day"]) == ("subagent", approx(5.0))
        token_types = {item["key"]: item["change_per_day"] for item in report["changes"]["token type"]}
        assert token_types["cache write"] == approx(5.0)
        assert token_types["cache read"] == approx(-0.2)

    def test_throughput_counts_concurrent_transcripts_and_aop_runs_separately(self):
        db = _db([
            (90, "main", "interactive", 0, 0, 0, 0, 3_600),
            (80, "sub", "subagent", 0, 0, 0, 0, 3_600),
        ])
        db.execute("""
            INSERT INTO tokens (tool, session, date, model, output_tokens, execution_type,
                source_kind, source_file, timestamp)
            VALUES ('OpenCode', 'run', '2026-10-03', 'opencode/glm-5.3', 0, 'subagent', 'aop',
                    '/runs/one/result.json', '2026-10-03T10:40:00+00:00')
        """)

        report = build_throughput(db, NOW - timedelta(hours=12), NOW)

        (day,) = report["days"]
        assert day["covered_hours"] == 12
        assert day["tokens_per_second"] == approx(7_200 / (12 * 3600))
        assert (day["peak_streams"], day["aop_runs"]) == (2, 1)
        assert day["subagent_output_share"] == 0.5
