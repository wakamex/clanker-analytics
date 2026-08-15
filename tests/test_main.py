"""Tests for clanker-analytics."""

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

import duckdb
import pytest

import clanker_analytics.main as main_mod
from clanker_analytics.main import COST_PER_ROW, fmt, register_macros, detect_plans


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(*rows):
    """Create a DuckDB with tokens table from row tuples.
    Each row: (tool, project, session, date, model, input, output, cache_write, cache_read, total)
    """
    db = duckdb.connect()
    register_macros(db)
    db.execute("""CREATE TABLE tokens (
        tool VARCHAR, project VARCHAR, session VARCHAR, date VARCHAR, model VARCHAR,
        input_tokens INT, output_tokens INT, cache_write_tokens INT, cache_read_tokens INT,
        total_tokens BIGINT, execution_type VARCHAR DEFAULT 'interactive',
        project_path VARCHAR, token_count_type VARCHAR DEFAULT 'exact',
        turn_count INT DEFAULT 1, retained_tokens BIGINT, source_file VARCHAR
    )""")
    for r in rows:
        db.execute("""
            INSERT INTO tokens (
                tool, project, session, date, model, input_tokens, output_tokens,
                cache_write_tokens, cache_read_tokens, total_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, r)
    return db


SONNET_ROW = ("Claude Code", "myproj", "s1", "2026-03-15", "claude-sonnet-4-20250514",
              1000, 200, 500, 8000, 9700)
OPUS_ROW = ("Claude Code", "myproj", "s1", "2026-03-16", "claude-opus-4-20250514",
            2000, 500, 1000, 15000, 18500)
CODEX_ROW = ("Codex", "codexproj", "s2", "2026-03-15", "",
             500, 100, 0, 4000, 4600)
GEMINI_ROW = ("Gemini", "gemproj", "s3", "2026-03-15", "gemini-2.5-pro",
              500, 50, 0, 200, 750)
HAIKU_ROW = ("Claude Code", "other", "s4", "2026-03-14", "claude-haiku-4-20250514",
             300, 50, 100, 2000, 2450)

ALL_ROWS = [SONNET_ROW, OPUS_ROW, CODEX_ROW, GEMINI_ROW, HAIKU_ROW]


# ===========================================================================
# Source loader tests
# ===========================================================================

class TestSourceLoaders:
    def test_codex_uses_own_session_metadata_once(self, tmp_path):
        session_id = "019fceda-3070-71e2-b533-349e787d37ac"
        path = tmp_path / f"rollout-2026-08-04T18-17-13-{session_id}.jsonl"
        rows = [
            {
                "timestamp": "2026-08-04T22:17:13.971Z",
                "type": "session_meta",
                "payload": {
                    "id": "019fc954-2955-73c0-9a5c-ed20691f7bd7",
                    "cwd": "/code/parent",
                },
            },
            {
                "timestamp": "2026-08-04T22:17:13.972Z",
                "type": "session_meta",
                "payload": {"id": session_id, "cwd": "/code/child"},
            },
            {
                "timestamp": "2026-08-04T22:17:14.000Z",
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {"total_tokens": 100},
                        "last_token_usage": {
                            "total_tokens": 100,
                            "input_tokens": 70,
                            "output_tokens": 10,
                            "cached_input_tokens": 20,
                        },
                    },
                },
            },
        ]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))

        db = duckdb.connect()
        result = db.sql(main_mod._codex_sql(main_mod._sql_literal(path.as_posix()))).fetchall()

        assert len(result) == 1
        assert result[0][1] == "child"
        assert result[0][9] == 100
        assert result[0][10] == "unknown"
        assert result[0][11] == "/code/child"

    @pytest.mark.parametrize(
        ("metadata", "expected"),
        [
            ({"thread_source": "user", "originator": "codex-tui", "source": "cli"},
             "interactive"),
            ({"originator": "codex_exec", "source": "exec"}, "exec"),
            ({"thread_source": "subagent", "source": {"subagent": {}}}, "subagent"),
            ({
                "cwd": "/code/project/.aop/worktrees/research-a",
                "originator": "codex_exec",
                "source": "exec",
            }, "subagent"),
        ],
    )
    def test_codex_execution_type(self, tmp_path, metadata, expected):
        path = tmp_path / f"{expected}.jsonl"
        rows = [
            {
                "timestamp": "2026-08-04T22:17:13.971Z",
                "type": "session_meta",
                "payload": {"id": expected, "cwd": "/code/project", **metadata},
            },
            {
                "timestamp": "2026-08-04T22:17:14.000Z",
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {"total_tokens": 100},
                        "last_token_usage": {
                            "total_tokens": 100,
                            "input_tokens": 70,
                            "output_tokens": 10,
                            "cached_input_tokens": 20,
                        },
                    },
                },
            },
        ]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))

        result = duckdb.sql(
            main_mod._codex_sql(main_mod._sql_literal(path.as_posix()))
        ).fetchone()

        assert result[10] == expected
        assert result[11] == metadata.get("cwd", "/code/project")

    def test_claude_includes_subagents_but_not_compaction_duplicates(self, tmp_path):
        root = tmp_path / "session.jsonl"
        subagents = tmp_path / "subagents"
        subagents.mkdir()
        subagent = subagents / "agent-worker.jsonl"
        compact = subagents / "agent-acompact-state.jsonl"

        def assistant(uuid, sidechain):
            return {
                "type": "assistant",
                "uuid": uuid,
                "sessionId": "session",
                "cwd": "/code/project",
                "timestamp": "2026-08-04T22:17:14.000Z",
                "isSidechain": sidechain,
                "message": {
                    "model": "claude-opus-4-6",
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "cache_creation_input_tokens": 0,
                        "cache_read_input_tokens": 0,
                    },
                },
            }

        root.write_text(json.dumps(assistant("root", False)) + "\n")
        subagent.write_text(json.dumps(assistant("worker", True)) + "\n")
        compact.write_text(json.dumps(assistant("compact", True)) + "\n")

        source = main_mod._sql_file_list([p.as_posix() for p in (root, subagent, compact)])
        rows = duckdb.sql(main_mod._claude_sql(source)).fetchall()

        assert sorted(row[10] for row in rows) == ["interactive", "subagent"]

    def test_agy_uses_exact_retained_usage_and_attributes_project(self, tmp_path):
        session = "11111111-2222-3333-4444-555555555555"
        path = tmp_path / "brain" / session / ".system_generated" / "logs"
        path.mkdir(parents=True)
        transcript = path / "transcript_full.jsonl"
        transcript.write_text(json.dumps({
            "step_index": 1,
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "DONE",
            "created_at": "2026-08-14T12:00:00Z",
            "content": "retained response text",
            "model": "gemini-3.5-flash",
            "tool_calls": [{"name": "run_command", "args": {"Cwd": "/code/project/subdir"}}],
            "usageMetadata": {
                "promptTokenCount": 100,
                "cachedContentTokenCount": 20,
                "candidatesTokenCount": 30,
                "thoughtsTokenCount": 10,
                "totalTokenCount": 140,
            },
        }) + "\n")

        metadata = tmp_path / "conversation_metadata.json"
        metadata.write_text(json.dumps({"conversations": {"display-id": {"summary": {
            "ID": session,
            "WorkspaceURIs": ["file:///code/project"],
        }}}}))

        row = duckdb.sql(main_mod._agy_sql(
            main_mod._sql_literal(transcript.as_posix()),
            main_mod._sql_literal(metadata.as_posix()),
        )).fetchone()

        assert row[1:5] == ("project", session, "2026-08-14", "gemini-3.5-flash")
        assert row[5:10] == (80, 40, 0, 20, 140)
        assert row[11:13] == ("/code/project", "exact")

    def test_agy_estimates_cumulative_turns_and_classifies_tool_results(self, tmp_path):
        session = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        transcript = tmp_path / "brain" / session / ".system_generated" / "logs" / "transcript_full.jsonl"
        transcript.parent.mkdir(parents=True)
        rows = [
            {
                "step_index": 0,
                "source": "SYSTEM",
                "type": "CONVERSATION_HISTORY",
                "status": "DONE",
                "created_at": "2026-08-14T12:00:00Z",
                "content": "x" * 400,
            },
            {
                "step_index": 1,
                "source": "USER_EXPLICIT",
                "type": "USER_INPUT",
                "status": "DONE",
                "created_at": "2026-08-14T12:00:01Z",
                "content": "12345678",
            },
            {
                "step_index": 2,
                "source": "MODEL",
                "type": "PLANNER_RESPONSE",
                "status": "DONE",
                "created_at": "2026-08-14T12:00:02Z",
                "content": "abcdefgh",
            },
            {
                "step_index": 3,
                "source": "MODEL",
                "type": "RUN_COMMAND",
                "status": "DONE",
                "created_at": "2026-08-14T12:00:03Z",
                "content": "tooltool",
            },
            {
                "step_index": 4,
                "source": "MODEL",
                "type": "PLANNER_RESPONSE",
                "status": "DONE",
                "created_at": "2026-08-14T12:00:04Z",
                "content": "ijklmnop",
            },
        ]
        transcript.write_text("".join(json.dumps(row) + "\n" for row in rows))

        result = duckdb.sql(
            main_mod._agy_sql(main_mod._sql_literal(transcript.as_posix()), "")
        ).fetchone()

        assert result[5:10] == (8, 4, 0, 0, 12)
        assert result[12] == "estimated_processed"
        assert result[13:15] == (2, 8)

    def test_agy_metadata_filters_internal_trajectories(self, tmp_path):
        brain = tmp_path / "brain"
        kept = "11111111-1111-1111-1111-111111111111"
        internal = "22222222-2222-2222-2222-222222222222"
        for session in (kept, internal):
            transcript = brain / session / ".system_generated" / "logs" / "transcript_full.jsonl"
            transcript.parent.mkdir(parents=True)
            transcript.write_text(json.dumps({
                "step_index": 1,
                "source": "MODEL",
                "type": "PLANNER_RESPONSE",
                "status": "DONE",
                "created_at": "2026-08-14T12:00:00Z",
                "content": "abcdefgh",
            }) + "\n")

        metadata = tmp_path / "conversation_metadata.json"
        metadata.write_text(json.dumps({"conversations": {"display-id": {"summary": {
            "ID": kept,
            "WorkspaceURIs": ["file:///code/project"],
        }}}}))

        rows = duckdb.sql(main_mod._agy_sql(
            main_mod._sql_literal((brain / "*" / ".system_generated" / "logs" / "transcript_full.jsonl").as_posix()),
            main_mod._sql_literal(metadata.as_posix()),
        )).fetchall()

        assert [row[2] for row in rows] == [kept]

    def test_gemini_timestamp_schema_shape_is_cast_before_slicing(self, tmp_path):
        chat = tmp_path / "tmp" / ("a" * 64) / "chats" / "session.json"
        chat.parent.mkdir(parents=True)
        chat.write_text(json.dumps({
            "sessionId": "session",
            "messages": [{
                "timestamp": "2026-08-14T12:00:00Z",
                "model": "gemini-flash",
                "tokens": {"input": 10, "output": 2, "cached": 3, "total": 15},
            }],
        }))

        row = duckdb.sql(main_mod._gemini_sql(main_mod._sql_literal(chat.as_posix()))).fetchone()

        assert row[2:4] == ("session", "2026-08-14")


# ===========================================================================
# fmt() tests
# ===========================================================================

class TestFmt:
    def test_billions(self):
        assert fmt(1_500_000_000) == "1.5B"

    def test_millions(self):
        assert fmt(883_400_000) == "883.4M"

    def test_thousands(self):
        assert fmt(24_100) == "24.1k"

    def test_small(self):
        assert fmt(42) == "42"

    def test_zero(self):
        assert fmt(0) == "0"

    def test_exact_billion(self):
        assert fmt(1_000_000_000) == "1.0B"

    def test_exact_million(self):
        assert fmt(1_000_000) == "1.0M"

    def test_exact_thousand(self):
        assert fmt(1_000) == "1.0k"


# ===========================================================================
# share.py formatter tests
# ===========================================================================

class TestShareFormatters:
    def test_fmt_cost_large(self):
        from clanker_analytics.share import _fmt_cost
        assert _fmt_cost(3841.5) == "$3,842"

    def test_fmt_cost_hundreds(self):
        from clanker_analytics.share import _fmt_cost
        result = _fmt_cost(91.15)
        assert result.startswith("$91.")

    def test_fmt_cost_tens(self):
        from clanker_analytics.share import _fmt_cost
        assert _fmt_cost(8.60) == "$8.60"

    def test_fmt_cost_small(self):
        from clanker_analytics.share import _fmt_cost
        assert _fmt_cost(0.49) == "$0.49"

    def test_fmt_cost_zero(self):
        from clanker_analytics.share import _fmt_cost
        assert _fmt_cost(0) == "$0.00"

    def test_fmt_cost_boundary_1000(self):
        from clanker_analytics.share import _fmt_cost
        assert _fmt_cost(1000) == "$1,000"

    def test_fmt_cost_boundary_100(self):
        from clanker_analytics.share import _fmt_cost
        assert _fmt_cost(100) == "$100"

    def test_fmt_cost_boundary_10(self):
        from clanker_analytics.share import _fmt_cost
        assert _fmt_cost(10) == "$10.0"

    def test_fmt_tokens_billions(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(1_500_000_000) == "1.5B"

    def test_fmt_tokens_exact_billion(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(2_000_000_000) == "2B"

    def test_fmt_tokens_millions(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(465_600_000) == "465.6M"

    def test_fmt_tokens_exact_million(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(3_000_000) == "3M"

    def test_fmt_tokens_thousands(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(12_500) == "12.5k"

    def test_fmt_tokens_exact_thousand(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(5_000) == "5k"

    def test_fmt_tokens_small(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(42) == "42"

    def test_fmt_tokens_zero(self):
        from clanker_analytics.share import _fmt_tokens
        assert _fmt_tokens(0) == "0"

    def test_short_date(self):
        from clanker_analytics.share import _short_date
        assert _short_date("2026-03-15") == "Mar 15"

    def test_short_date_january(self):
        from clanker_analytics.share import _short_date
        assert _short_date("2026-01-01") == "Jan 1"

    def test_short_date_december(self):
        from clanker_analytics.share import _short_date
        assert _short_date("2026-12-31") == "Dec 31"


# ===========================================================================
# Token counting / data shape tests
# ===========================================================================

class TestTokenCounting:
    def test_all_tools_present(self):
        db = _make_db(*ALL_ROWS)
        tools = sorted(r[0] for r in db.sql("SELECT DISTINCT tool FROM tokens").fetchall())
        assert tools == ["Claude Code", "Codex", "Gemini"]

    def test_total_tokens(self):
        db = _make_db(*ALL_ROWS)
        total = db.sql("SELECT sum(total_tokens)::BIGINT FROM tokens").fetchone()[0]
        assert total == 9700 + 18500 + 4600 + 750 + 2450

    def test_per_tool_totals(self):
        db = _make_db(*ALL_ROWS)
        claude = db.sql("SELECT sum(total_tokens)::BIGINT FROM tokens WHERE tool='Claude Code'").fetchone()[0]
        assert claude == 9700 + 18500 + 2450

    def test_project_names(self):
        db = _make_db(*ALL_ROWS)
        projects = sorted(r[0] for r in db.sql("SELECT DISTINCT project FROM tokens").fetchall())
        assert projects == ["codexproj", "gemproj", "myproj", "other"]

    def test_date_filtering(self):
        db = _make_db(*ALL_ROWS)
        db.execute("DELETE FROM tokens WHERE date < '2026-03-15'")
        dates = sorted(r[0] for r in db.sql("SELECT DISTINCT date FROM tokens").fetchall())
        assert "2026-03-14" not in dates

    def test_tool_filtering(self):
        db = _make_db(*ALL_ROWS)
        db.execute("DELETE FROM tokens WHERE tool != 'Claude Code'")
        tools = [r[0] for r in db.sql("SELECT DISTINCT tool FROM tokens").fetchall()]
        assert tools == ["Claude Code"]

    def test_model_values(self):
        db = _make_db(*ALL_ROWS)
        models = sorted(r[0] for r in db.sql(
            "SELECT DISTINCT model FROM tokens WHERE model != ''"
        ).fetchall())
        assert "claude-opus-4-20250514" in models
        assert "claude-sonnet-4-20250514" in models
        assert "gemini-2.5-pro" in models

    def test_billable_less_than_total(self):
        db = _make_db(*ALL_ROWS)
        row = db.sql("""
            SELECT sum(total_tokens)::BIGINT,
                   (sum(total_tokens) - 0.9 * sum(cache_read_tokens))::BIGINT
            FROM tokens
        """).fetchone()
        assert row[1] < row[0]
        assert row[1] > 0


# ===========================================================================
# Cost calculation tests
# ===========================================================================

class TestCostCalculation:
    def test_sonnet_cost(self):
        db = _make_db(SONNET_ROW)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        expected = (1000 * 3.0 + 500 * 3.75 + 8000 * 0.30 + 200 * 15.0) / 1e6
        assert abs(cost - expected) < 0.0001

    def test_opus_cost(self):
        db = _make_db(OPUS_ROW)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        expected = (2000 * 5.0 + 1000 * 6.25 + 15000 * 0.50 + 500 * 25.0) / 1e6
        assert abs(cost - expected) < 0.0001

    def test_haiku_cost(self):
        db = _make_db(HAIKU_ROW)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        expected = (300 * 1.0 + 100 * 1.25 + 2000 * 0.10 + 50 * 5.0) / 1e6
        assert abs(cost - expected) < 0.0001

    def test_codex_cost(self):
        db = _make_db(CODEX_ROW)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        expected = (500 * 1.25 + 0 * 1.25 + 4000 * 0.125 + 100 * 10.0) / 1e6
        assert abs(cost - expected) < 0.0001

    def test_gemini_cost(self):
        db = _make_db(GEMINI_ROW)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        expected = (500 * 1.25 + 200 * 0.315 + 50 * 10.0) / 1e6
        assert abs(cost - expected) < 0.0001

    def test_total_cost_all_tools(self):
        db = _make_db(*ALL_ROWS)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        assert cost > 0

    def test_null_cost_on_empty(self):
        db = _make_db()
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        assert cost is None


# ===========================================================================
# DuckDB macro tests
# ===========================================================================

class TestMacros:
    def test_fmt_macro(self):
        db = _make_db(SONNET_ROW)
        result = db.sql("SELECT fmt(9700)").fetchone()[0]
        assert "9.7k" in result

    def test_fmtcost_macro_small(self):
        db = _make_db(SONNET_ROW)
        result = db.sql("SELECT fmtcost(8.5)").fetchone()[0]
        assert result == "$8.50"

    def test_fmtcost_macro_medium(self):
        db = _make_db(SONNET_ROW)
        result = db.sql("SELECT fmtcost(91.1)").fetchone()[0]
        assert result == "$91.1"

    def test_fmtcost_macro_large(self):
        db = _make_db(SONNET_ROW)
        result = db.sql("SELECT fmtcost(3841.0)").fetchone()[0]
        assert result == "$3841"


# ===========================================================================
# Query shape tests (summary rows)
# ===========================================================================

class TestQueries:
    def test_project_query_has_summary_rows(self):
        from clanker_analytics.main import QUERIES
        db = _make_db(*ALL_ROWS)
        result = db.sql(QUERIES["project"].format(limit=50)).fetchall()
        # First row should be project=*, tool=*
        assert result[0][0] == "*"
        assert result[0][1] == "*"

    def test_project_query_per_tool_summaries(self):
        from clanker_analytics.main import QUERIES
        db = _make_db(*ALL_ROWS)
        result = db.sql(QUERIES["project"].format(limit=50)).fetchall()
        star_rows = [(r[0], r[1]) for r in result if r[0] == "*"]
        tools_in_summary = [r[1] for r in star_rows if r[1] != "*"]
        assert "Claude Code" in tools_in_summary
        assert "Codex" in tools_in_summary
        assert "Gemini" in tools_in_summary

    def test_date_query(self):
        from clanker_analytics.main import QUERIES
        db = _make_db(*ALL_ROWS)
        result = db.sql(QUERIES["date"].format(limit=50)).fetchall()
        assert result[0][0] == "*"  # summary row first

    def test_model_query(self):
        from clanker_analytics.main import QUERIES
        db = _make_db(*ALL_ROWS)
        result = db.sql(QUERIES["model"].format(limit=50)).fetchall()
        assert result[0][0] == "*"

    def test_session_query(self):
        from clanker_analytics.main import QUERIES
        db = _make_db(*ALL_ROWS)
        result = db.sql(QUERIES["session"].format(limit=50)).fetchall()
        assert result[0][1] == "*"  # project=*

    def test_execution_query(self):
        from clanker_analytics.main import QUERIES
        db = _make_db(*ALL_ROWS)
        db.execute("UPDATE tokens SET execution_type = 'subagent' WHERE session = 's2'")

        result = db.sql(QUERIES["execution"].format(limit=50)).fetchall()

        assert result[0][:2] == ("*", "*")
        assert ("subagent", "Codex") in [row[:2] for row in result]

    def test_aggregation_labels_mixed_counts_as_estimated(self):
        agy_row = ("Agy", "project", "agy-session", "2026-03-15", "",
                   100, 20, 0, 0, 120)
        db = _make_db(SONNET_ROW, agy_row)
        db.execute("""UPDATE tokens
                      SET token_count_type = 'estimated_processed', retained_tokens = 30
                      WHERE tool = 'Agy'""")

        relation = db.sql(main_mod.QUERIES["project"].format(limit=50))
        rows = [dict(zip(relation.columns, row)) for row in relation.fetchall()]

        overall = next(row for row in rows if row["project"] == "*" and row["tool"] == "*")
        agy = next(row for row in rows if row["project"] == "*" and row["tool"] == "Agy")
        claude = next(row for row in rows if row["project"] == "*" and row["tool"] == "Claude Code")
        assert overall["count_basis"] == "processed estimate"
        assert agy["count_basis"] == "processed estimate"
        assert claude["count_basis"] == "exact"
        assert agy["retained_text"].strip() == "30"


# ===========================================================================
# Plan detection tests
# ===========================================================================

class TestPlanDetection:
    def test_claude_max_20x(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"plan":"max_20x"}')
            plans = detect_plans()
        assert plans["Claude Code"] == ("max_20x", 200)

    def test_claude_pro(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"plan":"pro"}')
            plans = detect_plans()
        assert plans["Claude Code"] == ("pro", 20)

    def test_claude_max_5x(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"plan":"max_5x"}')
            plans = detect_plans()
        assert plans["Claude Code"] == ("max_5x", 100)

    def test_codex_pro_is_200(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"plan":"pro"}')
            plans = detect_plans()
        assert plans["Codex"] == ("pro", 200)

    def test_codex_plus(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"plan":"plus"}')
            plans = detect_plans()
        assert plans["Codex"] == ("plus", 20)

    def test_gemini_pro_tier_normalized(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"account_quota":{"user_tier":"g1-pro-tier"}}')
            plans = detect_plans()
        assert plans["Gemini"] == ("pro", 20)

    def test_gemini_ultra_tier_normalized(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"account_quota":{"user_tier":"g1-ultra-tier"}}')
            plans = detect_plans()
        assert plans["Gemini"] == ("ultra", 250)

    def test_default_claude_prefix_stripped(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout='{"plan":"default_claude_max_5x"}')
            plans = detect_plans()
        assert plans["Claude Code"] == ("max_5x", 100)

    def test_tool_not_installed(self):
        with mock.patch("subprocess.run", side_effect=FileNotFoundError):
            assert detect_plans() == {}

    def test_tool_returns_error(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=1, stdout="")
            assert detect_plans() == {}

    def test_tool_timeout(self):
        with mock.patch("subprocess.run", side_effect=subprocess.TimeoutExpired("x", 10)):
            assert detect_plans() == {}

    def test_invalid_json(self):
        with mock.patch("subprocess.run") as m:
            m.return_value = mock.Mock(returncode=0, stdout="not json")
            assert detect_plans() == {}

    def test_subscription_cost_sum(self):
        outputs = {
            "ccusage": '{"plan":"max_20x"}',
            "codex_cli_usage": '{"plan":"pro"}',
            "gemini_cli_usage": '{"account_quota":{"user_tier":"g1-pro-tier"}}',
        }
        def fake_run(cmd_list, **kw):
            # cmd_list is [sys.executable, "-m", "module_name", "json"]
            module = cmd_list[2] if len(cmd_list) > 2 else ""
            return mock.Mock(returncode=0, stdout=outputs.get(module, "{}"))
        with mock.patch("subprocess.run", side_effect=fake_run):
            plans = detect_plans()
        assert sum(c for _, c in plans.values()) == 420


# ===========================================================================
# Chart generation tests
# ===========================================================================

class TestChartGeneration:
    def test_generate_creates_png(self, tmp_path):
        db = _make_db(*ALL_ROWS)
        import clanker_analytics.share as share_mod
        orig = share_mod.OUTPUT
        share_mod.OUTPUT = tmp_path / "test.png"
        try:
            path = share_mod.generate(db, "7d", {})
            assert path.exists()
            with open(path, "rb") as f:
                assert f.read(4) == b"\x89PNG"
        finally:
            share_mod.OUTPUT = orig

    def test_generate_with_plans(self, tmp_path):
        db = _make_db(*ALL_ROWS)
        import clanker_analytics.share as share_mod
        orig = share_mod.OUTPUT
        share_mod.OUTPUT = tmp_path / "test.png"
        try:
            plans = {"Claude Code": ("max_20x", 200), "Codex": ("pro", 200)}
            path = share_mod.generate(db, "7d", plans)
            assert path.exists()
        finally:
            share_mod.OUTPUT = orig

    def test_generate_empty_data_returns_none(self, tmp_path):
        db = _make_db()  # empty
        import clanker_analytics.share as share_mod
        orig = share_mod.OUTPUT
        share_mod.OUTPUT = tmp_path / "test.png"
        try:
            path = share_mod.generate(db, "24h", {})
            assert path is None
        finally:
            share_mod.OUTPUT = orig

    def test_bar_chart_for_single_date(self, tmp_path):
        """<=3 dates -> bar chart."""
        rows = [r for r in ALL_ROWS if r[3] == "2026-03-15"]
        db = _make_db(*rows)
        import clanker_analytics.share as share_mod
        orig = share_mod.OUTPUT
        share_mod.OUTPUT = tmp_path / "test.png"
        try:
            path = share_mod.generate(db, "24h", {})
            assert path.exists()
        finally:
            share_mod.OUTPUT = orig

    def test_area_chart_for_many_dates(self, tmp_path):
        """>3 dates -> area chart."""
        db = _make_db(*ALL_ROWS)
        for i in range(10, 15):
            db.execute(f"""
                INSERT INTO tokens (
                    tool, project, session, date, model, input_tokens, output_tokens,
                    cache_write_tokens, cache_read_tokens, total_tokens
                ) VALUES ('Claude Code','p','s','2026-03-{i}','sonnet',100,50,10,500,660)
            """)
        import clanker_analytics.share as share_mod
        orig = share_mod.OUTPUT
        share_mod.OUTPUT = tmp_path / "test.png"
        try:
            path = share_mod.generate(db, "7d", {})
            assert path.exists()
        finally:
            share_mod.OUTPUT = orig


# ===========================================================================
# Empty / edge case tests
# ===========================================================================

class TestEdgeCases:
    def test_empty_table_returns_no_data(self):
        db = _make_db()
        count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
        assert count == 0

    def test_since_filter_removes_all_data(self):
        db = _make_db(SONNET_ROW)  # date 2026-03-15
        db.execute("DELETE FROM tokens WHERE date < '2026-04-01'")
        count = db.sql("SELECT count(*) FROM tokens").fetchone()[0]
        assert count == 0

    def test_single_tool_only(self):
        db = _make_db(SONNET_ROW)
        tools = [r[0] for r in db.sql("SELECT DISTINCT tool FROM tokens").fetchall()]
        assert tools == ["Claude Code"]

    def test_zero_cache_tokens(self):
        row = ("Claude Code", "p", "s", "2026-03-15", "sonnet", 1000, 200, 0, 0, 1200)
        db = _make_db(row)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        assert cost > 0

    def test_all_cache_tokens(self):
        row = ("Claude Code", "p", "s", "2026-03-15", "sonnet", 0, 0, 0, 10000, 10000)
        db = _make_db(row)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        expected = (10000 * 0.30) / 1e6
        assert abs(cost - expected) < 0.0001


# ===========================================================================
# Windows path tests
# ===========================================================================

class TestWindowsPaths:
    def test_home_backslash_replaced(self):
        home = "C:\\Users\\Test".replace("\\", "/")
        assert home == "C:/Users/Test"

    def test_cache_file_posix(self):
        p = Path("C:/Users/Test/.cache/clanker-analytics/tokens.parquet")
        assert "\\" not in p.as_posix()


# ===========================================================================
# Debug / profiling tests
# ===========================================================================

class TestDebugHooks:
    def test_source_scan_discovers_only_canonical_agy_transcript(self, tmp_path, monkeypatch):
        brain = tmp_path / "antigravity-cli" / "brain"
        logs = brain / "session" / ".system_generated" / "logs"
        chunks = logs / "chunks" / "transcript_full"
        chunks.mkdir(parents=True)
        canonical = logs / "transcript_full.jsonl"
        canonical.write_text("{}\n")
        (logs / "transcript.jsonl").write_text("{}\n")
        (chunks / "00000000.jsonl").write_text("{}\n")
        presence = tmp_path / "antigravity-cli" / "presence"
        presence.mkdir()
        (presence / "session.lock").write_text("")

        monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            ("Agy", brain, "*/.system_generated/logs/transcript_full.jsonl"),
        ])
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        files, dir_count, _ = main_mod.scan_source_files()

        assert list(files) == [canonical.as_posix()]
        assert files[canonical.as_posix()].tool == "Agy"
        assert dir_count == 1

    def test_sources_mtime_includes_gemini_json(self, tmp_path, monkeypatch):
        claude_dir = tmp_path / "claude"
        claude_dir.mkdir()
        (claude_dir / "session.jsonl").write_text("{}")
        gemini_dir = tmp_path / "gemini"
        (gemini_dir / "chats").mkdir(parents=True)
        (gemini_dir / "chats" / "chat.json").write_text("{}")

        monkeypatch.setattr(
            main_mod,
            "SOURCE_TREES",
            [
                ("Claude Code", claude_dir, "*.jsonl"),
                ("Gemini", gemini_dir, "chats/*.json"),
            ],
        )
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        newest, file_count, dir_count, scan_seconds = main_mod.sources_mtime()

        assert newest > 0
        assert file_count == 2
        assert dir_count == 2
        assert scan_seconds >= 0

    def test_incremental_cache_updates_only_changed_files(self, tmp_path, monkeypatch):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        file_a = src_dir / "a.jsonl"
        file_b = src_dir / "b.jsonl"
        file_a.write_text(json.dumps({
            "project": "alpha", "session": "s1", "date": "2026-03-15", "model": "m",
            "input_tokens": 1, "output_tokens": 2, "cache_write_tokens": 0,
            "cache_read_tokens": 0, "total_tokens": 3,
        }) + "\n")
        file_b.write_text(json.dumps({
            "project": "beta", "session": "s2", "date": "2026-03-15", "model": "m",
            "input_tokens": 4, "output_tokens": 5, "cache_write_tokens": 0,
            "cache_read_tokens": 0, "total_tokens": 9,
        }) + "\n")

        def build_sql(source_expr):
            return f"""
                SELECT
                    'TestTool' as tool,
                    project,
                    session,
                    date,
                    model,
                    input_tokens::INT as input_tokens,
                    output_tokens::INT as output_tokens,
                    cache_write_tokens::INT as cache_write_tokens,
                    cache_read_tokens::INT as cache_read_tokens,
                    total_tokens::BIGINT as total_tokens,
                    'interactive' as execution_type,
                    project as project_path,
                    'exact' as token_count_type,
                    1 as turn_count,
                    NULL::BIGINT as retained_tokens,
                    replace(filename, '\\\\', '/') as source_file
                FROM read_json({source_expr},
                    format='newline_delimited', filename=true, union_by_name=true)
            """

        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [("TestTool", src_dir, "*.jsonl")])
        monkeypatch.setattr(main_mod, "SOURCES", {"test": ("TestTool", build_sql)})
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        db = duckdb.connect()
        register_macros(db)
        main_mod.load_tokens(db, refresh=False)
        rows = db.sql("SELECT project, total_tokens FROM tokens ORDER BY project").fetchall()
        assert rows == [("alpha", 3), ("beta", 9)]

        file_b.write_text(json.dumps({
            "project": "beta", "session": "s2", "date": "2026-03-15", "model": "m",
            "input_tokens": 10, "output_tokens": 20, "cache_write_tokens": 0,
            "cache_read_tokens": 0, "total_tokens": 30,
        }) + "\n")
        stat = file_b.stat()
        os.utime(file_b, ns=(stat.st_atime_ns + 1_000_000, stat.st_mtime_ns + 1_000_000))

        db2 = duckdb.connect()
        register_macros(db2)
        timer = main_mod.DebugTimer(True)
        main_mod.load_tokens(db2, refresh=False, timing=timer)
        rows = db2.sql("SELECT project, total_tokens FROM tokens ORDER BY project").fetchall()
        assert rows == [("alpha", 3), ("beta", 30)]
        assert any(sample.label == "append changed files" for sample in timer.samples)
        assert not any(sample.label.startswith("probe ") for sample in timer.samples)

    def test_incremental_cache_removes_deleted_files(self, tmp_path, monkeypatch):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        file_a = src_dir / "a.jsonl"
        file_b = src_dir / "b.jsonl"
        file_a.write_text(json.dumps({
            "project": "alpha", "session": "s1", "date": "2026-03-15", "model": "m",
            "input_tokens": 1, "output_tokens": 2, "cache_write_tokens": 0,
            "cache_read_tokens": 0, "total_tokens": 3,
        }) + "\n")
        file_b.write_text(json.dumps({
            "project": "beta", "session": "s2", "date": "2026-03-15", "model": "m",
            "input_tokens": 4, "output_tokens": 5, "cache_write_tokens": 0,
            "cache_read_tokens": 0, "total_tokens": 9,
        }) + "\n")

        def build_sql(source_expr):
            return f"""
                SELECT
                    'TestTool' as tool,
                    project,
                    session,
                    date,
                    model,
                    input_tokens::INT as input_tokens,
                    output_tokens::INT as output_tokens,
                    cache_write_tokens::INT as cache_write_tokens,
                    cache_read_tokens::INT as cache_read_tokens,
                    total_tokens::BIGINT as total_tokens,
                    'interactive' as execution_type,
                    project as project_path,
                    'exact' as token_count_type,
                    1 as turn_count,
                    NULL::BIGINT as retained_tokens,
                    replace(filename, '\\\\', '/') as source_file
                FROM read_json({source_expr},
                    format='newline_delimited', filename=true, union_by_name=true)
            """

        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [("TestTool", src_dir, "*.jsonl")])
        monkeypatch.setattr(main_mod, "SOURCES", {"test": ("TestTool", build_sql)})
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        db = duckdb.connect()
        register_macros(db)
        main_mod.load_tokens(db, refresh=False)
        rows = db.sql("SELECT project, total_tokens FROM tokens ORDER BY project").fetchall()
        assert rows == [("alpha", 3), ("beta", 9)]

        file_b.unlink()

        db2 = duckdb.connect()
        register_macros(db2)
        timer = main_mod.DebugTimer(True)
        main_mod.load_tokens(db2, refresh=False, timing=timer)
        rows = db2.sql("SELECT project, total_tokens FROM tokens ORDER BY project").fetchall()
        assert rows == [("alpha", 3)]
        assert any(sample.label == "drop changed rows" for sample in timer.samples)
        assert not any(sample.label == "append changed files" for sample in timer.samples)

    def test_agy_resume_replaces_cached_session_without_double_counting(
            self, tmp_path, monkeypatch):
        brain = tmp_path / "brain"
        transcript = brain / "session" / ".system_generated" / "logs" / "transcript_full.jsonl"
        transcript.parent.mkdir(parents=True)

        def event(step, source, event_type, content):
            return {
                "step_index": step,
                "source": source,
                "type": event_type,
                "status": "DONE",
                "created_at": f"2026-08-14T12:00:0{step}Z",
                "content": content,
            }

        original = [
            event(0, "USER_EXPLICIT", "USER_INPUT", "12345678"),
            event(1, "MODEL", "PLANNER_RESPONSE", "abcdefgh"),
        ]
        transcript.write_text("".join(json.dumps(row) + "\n" for row in original))

        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            ("Agy", brain, "*/.system_generated/logs/transcript_full.jsonl"),
        ])
        monkeypatch.setattr(main_mod, "SOURCES", {"agy": ("Agy", main_mod._agy_sql)})
        monkeypatch.setattr(main_mod, "_agy_metadata_paths", lambda: [])
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        db = duckdb.connect()
        register_macros(db)
        main_mod.load_tokens(db, refresh=False)
        assert db.sql("SELECT sum(total_tokens) FROM tokens").fetchone()[0] == 4

        resumed = original + [
            event(2, "SYSTEM", "CONVERSATION_HISTORY", "x" * 400),
            event(3, "USER_EXPLICIT", "USER_INPUT", "87654321"),
            event(4, "MODEL", "PLANNER_RESPONSE", "hgfedcba"),
        ]
        transcript.write_text("".join(json.dumps(row) + "\n" for row in resumed))
        stat = transcript.stat()
        os.utime(transcript, ns=(stat.st_atime_ns + 1_000_000, stat.st_mtime_ns + 1_000_000))

        db2 = duckdb.connect()
        register_macros(db2)
        timer = main_mod.DebugTimer(True)
        main_mod.load_tokens(db2, refresh=False, timing=timer)

        assert db2.sql("SELECT sum(total_tokens) FROM tokens").fetchone()[0] == 12
        assert any(sample.label == "drop changed rows" for sample in timer.samples)
        assert any(sample.label == "append changed files" for sample in timer.samples)

    def test_agy_metadata_change_reattributes_cached_session(self, tmp_path, monkeypatch):
        session = "11111111-2222-3333-4444-555555555555"
        brain = tmp_path / "brain"
        transcript = brain / session / ".system_generated" / "logs" / "transcript_full.jsonl"
        transcript.parent.mkdir(parents=True)
        transcript.write_text(json.dumps({
            "step_index": 1,
            "source": "MODEL",
            "type": "PLANNER_RESPONSE",
            "status": "DONE",
            "created_at": "2026-08-14T12:00:00Z",
            "content": "abcdefgh",
        }) + "\n")

        metadata_root = tmp_path / "agy-cache"
        metadata_root.mkdir()
        metadata = metadata_root / "conversation_metadata.json"

        def write_metadata(workspace):
            metadata.write_text(json.dumps({"conversations": {"display-id": {"summary": {
                "ID": session,
                "WorkspaceURIs": [f"file://{workspace}"],
            }}}}))

        write_metadata("/code/one")
        cache_dir = tmp_path / "clanker-cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            ("Agy", brain, "*/.system_generated/logs/transcript_full.jsonl"),
            (main_mod.AGY_METADATA_TOOL, metadata_root, "conversation_metadata.json"),
        ])
        monkeypatch.setattr(main_mod, "SOURCES", {"agy": ("Agy", main_mod._agy_sql)})
        monkeypatch.setattr(main_mod, "_agy_metadata_paths", lambda: [metadata.as_posix()])
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        db = duckdb.connect()
        register_macros(db)
        main_mod.load_tokens(db, refresh=False)
        assert db.sql("SELECT project_path FROM tokens").fetchone()[0] == "/code/one"

        write_metadata("/code/two")
        stat = metadata.stat()
        os.utime(metadata, ns=(stat.st_atime_ns + 1_000_000, stat.st_mtime_ns + 1_000_000))

        db2 = duckdb.connect()
        register_macros(db2)
        timer = main_mod.DebugTimer(True)
        main_mod.load_tokens(db2, refresh=False, timing=timer)

        assert db2.sql("SELECT project_path FROM tokens").fetchone()[0] == "/code/two"
        assert any(sample.label == "append changed files" for sample in timer.samples)

    def test_debug_timing_flag_prints_summary(self, capsys):
        def fake_run(args, timer):
            timer.note("cache hit")
            timer.record("load tokens", 0.01, "cached")
            return 0

        with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
            result = main_mod.main(["--debug-timing", "--sql", "select 1"])

        captured = capsys.readouterr()
        assert result == 0
        assert "[debug] timing summary" in captured.err
        assert "cache hit" in captured.err
        assert "load tokens" in captured.err

    def test_profile_flag_prints_summary(self, capsys):
        def fake_run(args, timer):
            return 0

        with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
            result = main_mod.main(["--profile", "--sql", "select 1"])

        captured = capsys.readouterr()
        assert result == 0
        assert "[profile] top functions by cumulative time" in captured.err
        assert "ncalls" in captured.err


# ===========================================================================
# CLI integration tests
# ===========================================================================

class TestCLI:
    def test_version_flag(self):
        result = subprocess.run(
            [sys.executable, "-m", "clanker_analytics.main", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "0." in result.stdout  # version number present
