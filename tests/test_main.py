"""Tests for clanker-analytics."""

import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from unittest import mock

import duckdb

import clanker_analytics.main as main_mod
from clanker_analytics.main import COST_PER_ROW, fmt, register_macros, detect_plans
from clanker_analytics.pricing import TOOL_PROVIDERS, price_tokens
import model_prices

from support import Case, approx, raises


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
        turn_count INT DEFAULT 1, retained_tokens BIGINT,
        source_kind VARCHAR DEFAULT 'native', cost_usd DOUBLE, source_file VARCHAR,
        timestamp VARCHAR
    )""")
    for r in rows:
        db.execute("""
            INSERT INTO tokens (
                tool, project, session, date, model, input_tokens, output_tokens,
                cache_write_tokens, cache_read_tokens, total_tokens, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (*r, *([None] * (11 - len(r)))))
    price_tokens(db, all_rows=True)
    return db


SONNET_ROW = ("Claude Code", "myproj", "s1", "2026-03-15", "claude-sonnet-4-20250514",
              1000, 200, 500, 8000, 9700)
OPUS_ROW = ("Claude Code", "myproj", "s1", "2026-03-16", "claude-opus-4-20250514",
            2000, 500, 1000, 15000, 18500)
CODEX_ROW = ("Codex", "codexproj", "s2", "2026-03-15", "",
             500, 100, 0, 4000, 4600)
GEMINI_ROW = ("Gemini", "gemproj", "s3", "2026-03-15", "gemini-2.5-pro",
              500, 50, 0, 200, 750)
HAIKU_ROW = ("Claude Code", "other", "s4", "2026-03-14", "claude-haiku-4-5-20251001",
             300, 50, 100, 2000, 2450)

ALL_ROWS = [SONNET_ROW, OPUS_ROW, CODEX_ROW, GEMINI_ROW, HAIKU_ROW]


def _write_aop_result(
    path: Path,
    *,
    provider: str = "codex",
    run_id: str = "run-1",
    session_id: str = "session-1",
    task: str = "task-1",
    model: str = "model-1",
    input_tokens: int = 100,
    cached_input_tokens: int = 30,
    cache_write_input_tokens: int = 0,
    cache_write_1h_input_tokens: int = 0,
    output_tokens: int = 10,
    reasoning_output_tokens: int = 5,
    inference_provider: str | None = None,
    cost_usd: float | None = 0.25,
    usage_schema: str = "aop-token-usage-v3",
    accounting_status: str = "complete",
    state_root: Path | None = None,
) -> None:
    """Write a result, by default in the run directory layout, whose .aop is its state root."""
    if state_root is None:
        state_root = path.parents[2]
    counts = (
        input_tokens,
        cached_input_tokens,
        cache_write_input_tokens,
        cache_write_1h_input_tokens,
        output_tokens,
        reasoning_output_tokens,
    )
    if min(counts) < 0:
        raise ValueError("AOP fixture token counts cannot be negative")
    if cached_input_tokens + cache_write_input_tokens > input_tokens:
        raise ValueError("AOP fixture cached input cannot exceed input")
    if cache_write_1h_input_tokens > cache_write_input_tokens:
        raise ValueError("AOP fixture 1h cache write cannot exceed cache write")
    if reasoning_output_tokens > output_tokens:
        raise ValueError("AOP fixture reasoning output cannot exceed output")
    if usage_schema != "aop-token-usage-v3":
        raise ValueError("unsupported AOP fixture usage schema")
    if accounting_status not in {"complete", "partial"}:
        raise ValueError("usage-bearing fixture requires complete or partial accounting")
    path.parent.mkdir(parents=True, exist_ok=True)
    calculated_cost = (
        {"amount_usd": cost_usd, "currency": "USD"}
        if cost_usd is not None else None
    )
    result = {
        "run_id": run_id,
        "provider": provider,
        "mode": "agent",
        "task": task,
        "model": model,
        "inference_provider": inference_provider,
        "session_id": session_id,
        "started_at": "2026-08-14T12:00:00+00:00",
        "finished_at": "2026-08-14T12:01:00+00:00",
        "usage_schema": usage_schema,
        "usage": {
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_input_tokens,
            "cache_write_input_tokens": cache_write_input_tokens,
            "cache_write_1h_input_tokens": cache_write_1h_input_tokens,
            "output_tokens": output_tokens,
            "reasoning_output_tokens": reasoning_output_tokens,
        },
        "calculated_cost": calculated_cost,
        "state_root": state_root.as_posix(),
    }
    result["accounting_status"] = accounting_status
    path.write_text(json.dumps(result))


# ===========================================================================
# Source loader tests
# ===========================================================================

class TestSourceLoaders(Case):
    def test_timestamps_become_utc_text_whatever_the_session_time_zone(self):
        db = duckdb.connect()
        db.execute("SET TimeZone = 'America/Toronto'")
        values = [
            ("2026-10-06 12:00:12.88", "2026-10-06T12:00:12.880Z"),  # zone-less logs are UTC
            ("2026-10-06T12:00:12.88Z", "2026-10-06T12:00:12.880Z"),
            ("2026-08-30T03:32:30.636575+00:00", "2026-08-30T03:32:30.636Z"),
            ("2026-08-30T03:32:30-04:00", "2026-08-30T07:32:30.000Z"),
        ]
        for value, expected in values:
            with self.subTest(value=value):
                sql = main_mod._utc_timestamp(main_mod._sql_literal(value))
                assert db.sql(f"SELECT {sql}").fetchone()[0] == expected
        # DuckDB may detect a log's timestamps as zone-less TIMESTAMP values.
        sql = main_mod._utc_timestamp("TIMESTAMP '2026-10-06 12:00:12.88'")
        assert db.sql(f"SELECT {sql}").fetchone()[0] == "2026-10-06T12:00:12.880Z"

    def test_codex_uses_own_session_metadata_once(self):
        tmp_path = self.tmp_path
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
                "timestamp": "2026-08-04T22:17:13.980Z",
                "type": "turn_context",
                "payload": {"model": "gpt-5.6-sol"},
            },
            {
                "timestamp": "2026-08-04T22:17:13.990Z",
                "type": "event_msg",
                "payload": {
                    "type": "thread_settings_applied",
                    "thread_settings": {
                        "model": "gpt-5.6-sol",
                        "service_tier": "default",
                    },
                },
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
        # Keyed by the thread id, as AOP records it for a run of this session.
        assert result[0][2] == session_id
        assert result[0][4] == "gpt-5.6-sol"
        assert result[0][5] == 50
        assert result[0][8] == 20
        assert result[0][9] == 100
        assert result[0][10] == "unknown"
        assert result[0][11] == "/code/child"
        assert result[0][18] == "2026-08-04T22:17:14.000Z"
        assert result[0][19] == "default"

    def test_codex_rows_before_the_first_model_event_take_the_sessions_model(self):
        tmp_path = self.tmp_path
        session_id = "019fd3f1-91fd-7902-8893-028d79084e33"
        path = tmp_path / f"rollout-2026-08-05T18-00-52-{session_id}.jsonl"
        usage = {"total_tokens": 100, "input_tokens": 90, "output_tokens": 10, "cached_input_tokens": 0}
        token_count = {"type": "token_count", "info": {"total_token_usage": usage, "last_token_usage": usage}}
        rows = [
            {"timestamp": "2026-08-05T22:00:52.288Z", "type": "session_meta",
             "payload": {"id": session_id, "cwd": "/code/stocks"}},
            {"timestamp": "2026-08-05T22:00:52.289Z", "type": "event_msg", "payload": token_count},
            {"timestamp": "2026-08-05T22:00:53.601Z", "type": "turn_context",
             "payload": {"model": "gpt-5.6-sol"}},
        ]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))

        db = duckdb.connect()
        result = db.sql(main_mod._codex_sql(main_mod._sql_literal(path.as_posix()))).fetchall()

        assert [row[4] for row in result] == ["gpt-5.6-sol"]

    def test_codex_execution_type(self):
        tmp_path = self.tmp_path
        for metadata, expected in [
            ({"thread_source": "user", "originator": "codex-tui", "source": "cli"},
             "interactive"),
            ({"originator": "codex_exec", "source": "exec"}, "exec"),
            ({"thread_source": "subagent", "source": {"subagent": {}}}, "subagent"),
            ({
                "cwd": "/code/project/.aop/worktrees/research-a",
                "originator": "codex_exec",
                "source": "exec",
            }, "subagent"),
        ]:
            with self.subTest(metadata=metadata, expected=expected):
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

    def test_codex_excludes_fork_replay_and_keeps_weekly_quota(self):
        tmp_path = self.tmp_path
        session_id = "019ffa59-948d-72f0-bc73-2e1b6b4880cd"
        path = tmp_path / f"rollout-{session_id}.jsonl"
        rows = [{
            "timestamp": "2026-08-13T05:00:02.000Z",
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "cwd": "/code/fork",
                "parent_thread_id": "parent",
            },
        }, {
            "timestamp": "2026-08-13T05:00:02.001Z",
            "type": "turn_context",
            "payload": {"model": "gpt-5.6-sol"},
        }]

        def token_event(timestamp, cumulative, used_pct):
            return {
                "timestamp": timestamp,
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {"total_tokens": cumulative},
                        "last_token_usage": {
                            "total_tokens": 100,
                            "input_tokens": 80,
                            "output_tokens": 10,
                            "cached_input_tokens": 20,
                        },
                    },
                    "rate_limits": {
                        "limit_id": "codex",
                        "plan_type": "pro",
                        "primary": {
                            "used_percent": used_pct,
                            "window_minutes": 10080,
                            "resets_at": 1787200000,
                        },
                    },
                },
            }

        rows.extend(
            token_event(f"2026-08-13T05:00:02.{100 + index:03d}Z", 100 * (index + 1), 40)
            for index in range(10)
        )
        rows.append(token_event("2026-08-13T05:00:12.000Z", 1100, 41))
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))

        result = duckdb.sql(
            main_mod._codex_sql(main_mod._sql_literal(path.as_posix()))
        ).fetchall()

        assert len(result) == 1
        assert result[0][18] == "2026-08-13T05:00:12.000Z"
        assert result[0][20:] == (41.0, 1787200000, 10080, "codex", "pro")

    def test_claude_includes_subagents_but_not_compaction_duplicates(self):
        tmp_path = self.tmp_path
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

        assert sorted(row[11] for row in rows) == ["interactive", "subagent"]

    def test_claude_reads_one_hour_cache_writes(self):
        tmp_path = self.tmp_path
        def assistant(uuid, usage):
            return {"type": "assistant", "uuid": uuid, "sessionId": "s", "cwd": "/code/demo",
                    "isSidechain": False,
                    "timestamp": "2026-10-03T12:00:00Z",
                    "message": {"model": "claude-opus-5-5", "usage": usage}}

        path = tmp_path / "s.jsonl"
        path.write_text("\n".join(json.dumps(line) for line in (
            assistant("split", {"input_tokens": 1, "output_tokens": 2, "cache_read_input_tokens": 0,
                                "cache_creation_input_tokens": 500,
                                "cache_creation": {"ephemeral_5m_input_tokens": 200,
                                                   "ephemeral_1h_input_tokens": 300}}),
            assistant("older", {"input_tokens": 1, "output_tokens": 2, "cache_read_input_tokens": 0,
                                "cache_creation_input_tokens": 400}),
        )) + "\n")

        source = main_mod._sql_file_list([path.as_posix()])
        rows = duckdb.sql(f"SELECT cache_write_tokens, cache_write_1h_tokens FROM "
                          f"({main_mod._claude_sql(source)}) ORDER BY 1").fetchall()

        assert rows == [(400, 0), (500, 300)]

    def test_agy_uses_exact_retained_usage_and_attributes_project(self):
        tmp_path = self.tmp_path
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

    @staticmethod
    def _agy_session_model(tmp_path, blobs):
        session = "11111111-2222-3333-4444-555555555555"
        root = tmp_path / "antigravity-cli"
        logs = root / "brain" / session / ".system_generated" / "logs"
        logs.mkdir(parents=True)
        transcript = logs / "transcript_full.jsonl"
        transcript.write_text(json.dumps({
            "step_index": 1, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE",
            "created_at": "2026-08-14T12:00:00Z", "content": "response without a model field",
        }) + "\n")
        (root / "conversations").mkdir()
        database = sqlite3.connect(root / "conversations" / f"{session}.db")
        database.execute("CREATE TABLE gen_metadata (idx integer, data blob, size integer)")
        database.executemany("INSERT INTO gen_metadata VALUES (?, ?, ?)",
                             [(index, blob, len(blob)) for index, blob in enumerate(blobs)])
        database.commit()
        database.close()
        metadata = tmp_path / "conversation_metadata.json"
        metadata.write_text(json.dumps({"conversations": {"id": {"summary": {"ID": session}}}}))

        db = duckdb.connect()
        main_mod._load_agy_models(db, [transcript.as_posix()])
        row = db.sql(main_mod._agy_sql(
            main_mod._sql_literal(transcript.as_posix()),
            main_mod._sql_literal(metadata.as_posix()),
            models_table=main_mod.AGY_MODELS_TABLE,
        )).fetchone()
        return row[4]

    # Generation metadata protobuf, as recorded by Antigravity: field 19 after the request
    # hash is the serving model and a trailing field 21 names the selected model.
    SERVED = b'"\x10465011cf2a4ad107\x9a\x01\x0egemini-default\xa2\x01#\n\nmodel_enum'
    SELECTED = b"\xaa\x01\x17Gemini 3.5 Flash (High)"

    def test_agy_model_is_the_selected_model_recorded_by_each_call(self):
        tmp_path = self.tmp_path
        blobs = [self.SERVED + self.SELECTED, self.SERVED + self.SELECTED, self.SERVED]

        assert self._agy_session_model(tmp_path, blobs) == "gemini-3.5-flash-high"

    def test_agy_model_falls_back_to_the_noted_model_selection(self):
        tmp_path = self.tmp_path
        note = (b"The user changed setting `Model Selection` from None to "
                b"Gemini 3.7 Flash (High). No need to acknowledge this.")
        served = b'"\x10ed9d840ced35d80d\x9a\x01\x16gemini-3.7-flash-exp-a\xa2\x01'

        assert self._agy_session_model(tmp_path, [note + served, served]) == "gemini-3.7-flash-high"

    def test_agy_model_falls_back_to_the_serving_name(self):
        tmp_path = self.tmp_path
        served = b'"\x10ed9d840ced35d80d\x9a\x01\x10gemini-3.7-flash\xa2\x01#\n\nmodel_enum'
        # The same tag inside message content, without the request hash, does not count.
        mention = b"content listing \x9a\x01\x0egpt-transcribe as a text mention"

        assert self._agy_session_model(tmp_path, [served, mention, mention]) == "gemini-3.7-flash"

    @staticmethod
    def _protobuf(*fields):
        """A protobuf message of (field, value) pairs: ints as varints, else bytes."""
        def varint(value):
            out = b""
            while True:
                byte, value = value & 0x7F, value >> 7
                out += bytes([byte | (0x80 if value else 0)])
                if not value:
                    return out
        out = b""
        for field, value in fields:
            if isinstance(value, int):
                out += varint(field << 3) + varint(value)
            else:
                value = value.encode() if isinstance(value, str) else value
                out += varint(field << 3 | 2) + varint(len(value)) + value
        return out

    def test_agy_counts_each_model_call_from_its_recorded_usage(self):
        pb = self._protobuf
        session = "11111111-2222-3333-4444-555555555555"
        root = self.tmp_path / "antigravity-cli"
        transcript = root / "brain" / session / ".system_generated" / "logs" / "transcript_full.jsonl"
        transcript.parent.mkdir(parents=True)
        # Four steps; the transcript alone would charge each response every earlier step.
        transcript.write_text("".join(json.dumps({
            "step_index": index, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE",
            "created_at": f"2026-10-20T12:00:0{index}Z", "content": "x" * 400_000,
        }) + "\n" for index in range(4)))

        def call(step, uncached, output, cached):
            # As Antigravity records a call: usage in field 4 with uncached input in 2,
            # output in 3 and cache reads in 5, and last_step_index among field 20's pairs.
            return pb((1, pb((3, 1418),
                             (4, pb((1, 1418), (2, uncached), (3, output), (5, cached), (6, 24))),
                             (19, "gemini-4-argon"),
                             (20, pb((1, "root_cascade_id"), (2, session))),
                             (20, pb((1, "last_step_index"), (2, str(step)))))),
                      (4, "a55925b3"))

        (root / "conversations").mkdir()
        database = sqlite3.connect(root / "conversations" / f"{session}.db")
        database.execute("CREATE TABLE gen_metadata (idx integer, data blob, size integer)")
        blobs = [call(1, 7586, 788, 16305), call(3, 4735, 728, 32638), b"no usage"]
        database.executemany("INSERT INTO gen_metadata VALUES (?, ?, ?)",
                             [(index, blob, len(blob)) for index, blob in enumerate(blobs)])
        database.commit()
        database.close()
        metadata = self.tmp_path / "conversation_metadata.json"
        metadata.write_text(json.dumps({"conversations": {"id": {"summary": {
            "ID": session, "WorkspaceURIs": ["file:///code/concord"]}}}}))

        db = duckdb.connect()
        main_mod._load_agy_models(db, [transcript.as_posix()])
        relation = db.sql(main_mod._agy_sql(
            main_mod._sql_literal(transcript.as_posix()),
            main_mod._sql_literal(metadata.as_posix()),
            models_table=main_mod.AGY_MODELS_TABLE, usage_table=main_mod.AGY_USAGE_TABLE))
        columns = [item[0] for item in relation.description]
        rows = sorted(tuple(dict(zip(columns, row))[key] for key in (
            "timestamp", "input_tokens", "cache_read_tokens", "output_tokens", "total_tokens",
            "token_count_type", "project")) for row in relation.fetchall())

        assert rows == [
            ("2026-10-20T12:00:01.000Z", 7586, 16305, 788, 24679, "exact", "concord"),
            ("2026-10-20T12:00:03.000Z", 4735, 32638, 728, 38101, "exact", "concord"),
        ]

    def test_agy_estimates_cumulative_turns_and_classifies_tool_results(self):
        tmp_path = self.tmp_path
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

        results = duckdb.sql(
            main_mod._agy_sql(main_mod._sql_literal(transcript.as_posix()), "")
        ).fetchall()

        assert len(results) == 2
        assert tuple(sum(row[index] for row in results) for index in range(5, 10)) == (
            8, 4, 0, 0, 12,
        )
        assert {row[12] for row in results} == {"estimated_processed"}
        assert sum(row[13] for row in results) == 2
        assert sum(row[14] or 0 for row in results) == 8
        assert sorted(row[18] for row in results) == [
            "2026-08-14T12:00:02.000Z",
            "2026-08-14T12:00:04.000Z",
        ]

    def test_agy_metadata_filters_internal_trajectories(self):
        tmp_path = self.tmp_path
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

    def test_gemini_jsonl_chat_counts_the_last_version_of_each_message(self):
        chat = self.tmp_path / "tmp" / "my-project" / "chats" / "session-2026-06-12T12-50-1a.jsonl"
        chat.parent.mkdir(parents=True)

        def reply(total, model="gemini-3-flash-preview", message_id="m1"):
            return {"id": message_id, "timestamp": "2026-06-12T12:50:46.864Z", "type": "gemini",
                    "content": "", "model": model,
                    "tokens": {"input": total - 10, "output": 10, "cached": 5, "thoughts": 0,
                               "tool": 0, "total": total}}

        # As Gemini CLI 0.66 writes them: a session line, messages, $set updates, and a
        # rewritten message appended again with the same id.
        lines = [
            {"sessionId": "session-1", "projectHash": "c91a", "startTime": "2026-06-12T12:50:40Z",
             "lastUpdated": "2026-06-12T12:50:40Z", "kind": "main"},
            {"id": "u1", "timestamp": "2026-06-12T12:50:40.769Z", "type": "user",
             "content": [{"text": "hi"}]},
            {"$set": {"lastUpdated": "2026-06-12T12:50:40.769Z"}},
            reply(100),
            reply(150),
            reply(40, message_id="m2"),
            {"$set": {"lastUpdated": "2026-06-12T12:51:00Z"}},
        ]
        chat.write_text("".join(json.dumps(line) + "\n" for line in lines))

        relation = duckdb.sql(main_mod._gemini_jsonl_sql(main_mod._sql_literal(chat.as_posix())))
        columns = [item[0] for item in relation.description]
        rows = sorted((dict(zip(columns, row)) for row in relation.fetchall()),
                      key=lambda row: row["total_tokens"])

        assert [(row["session"], row["total_tokens"], row["input_tokens"],
                 row["cache_read_tokens"]) for row in rows] == [
            ("session-1", 40, 30, 5), ("session-1", 150, 140, 5)]
        assert {(row["project"], row["date"], row["model"], row["timestamp"]) for row in rows} == {
            ("my-project", "2026-06-12", "gemini-3-flash-preview", "2026-06-12T12:50:46.864Z")}

    def test_gemini_chats_in_both_formats_are_scanned(self):
        tmp = self.tmp_path / ".gemini" / "tmp" / "p" / "chats"
        tmp.mkdir(parents=True)
        (tmp / "old.json").write_text("{}")
        (tmp / "new.jsonl").write_text("{}\n")
        self.monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            tree[:1] + (self.tmp_path / ".gemini" / "tmp",) + tree[2:]
            for tree in main_mod.SOURCE_TREES if tree[0] == "Gemini"])
        self.monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        files, _, _ = main_mod.scan_source_files()

        assert sorted(Path(path).name for path in files) == ["new.jsonl", "old.json"]

    def test_gemini_home_follows_gemini_cli_home(self):
        self.monkeypatch.setenv("GEMINI_CLI_HOME", str(self.tmp_path))
        assert main_mod._gemini_home() == self.tmp_path
        self.monkeypatch.delenv("GEMINI_CLI_HOME")
        assert main_mod._gemini_home() == Path(main_mod.HOME)

    def test_muse_counts_each_model_call_once(self):
        session = self.tmp_path / "muse" / "sessions" / "2026" / "10" / "25" / "01a1739f-session"
        subagent = session / "subagent" / "01a1739f-child" / "session.jsonl"
        subagent.parent.mkdir(parents=True)

        def record(payload_type, payload, at=1792941172328640):
            return json.dumps({"schema_version": 1, "stream": {"kind": "session", "id": "x"},
                               "recorded_at": at, "payload_type": payload_type,
                               "payload": payload}) + "\n"

        def call(model, input_tokens, cached, output, kind="model_completed", **usage):
            return record("runtime.session", {"kind": "run", "event": {
                "kind": kind, "model": model,
                "usage": {"input_tokens": input_tokens, "cached_tokens": cached,
                          "cache_read_tokens": cached, "cache_write_tokens": 0,
                          "output_tokens": output, "reasoning_tokens": 1, **usage}}})

        # As Muse Code 1.4.12 writes them. The session log attributes every call to its
        # owner: the main agent's and subagents' calls, which have model_completed events,
        # and the in-process skill reminder's, which do not. A review's usage is in the
        # session log and again in approval-review/.
        def attributed(owner_type, owner_id, input_tokens, cached, output, reported=True):
            return record("runtime.session", {"kind": "run", "event": {
                "kind": "goal_usage_attribution", "record": {
                    "usage_family": "provider",
                    "owner": {"owner_type": owner_type, "owner_id": owner_id},
                    "quantity": {"input_tokens": input_tokens, "cached_tokens": cached,
                                 "output_tokens": output, "reported": reported}}}})

        attribution = attributed("main_root", "main-root", 34317, 0, 151)
        (session / "session.jsonl").write_text(
            record("runtime.session.metadata", {"kind": "metadata",
                   "record": {"workspace_root": "/code/My-Project"}})
            + attribution + call("muse-wonder-1.0", 34317, 0, 151)
            + attributed("reminder_child", "01a1739f-reminder", 16541, 16369, 598)
            + attributed("reminder_child", "01a1739f-child", 5083, 0, 814)
            + attributed("reminder_child", "01a1739f-unreported", 0, 0, 0, reported=False)
            + call("muse-wonder-1.0", 36017, 35828, 494)
            + record("runtime.session", {"kind": "run", "event": {
                "kind": "automated_review_completed",
                "model": {"model_id": "muse-spark-1.3", "provider_id": "meta"},
                "usage": {"input_tokens": 17740, "cached_input_tokens": 0,
                          "output_tokens": 526, "total_tokens": 18266}}}))
        subagent.write_text(attribution + call("muse-spark-1.3", 5083, 0, 814))
        paths = [(session / "session.jsonl").as_posix(), subagent.as_posix()]
        db = duckdb.connect()

        for read in (paths, paths[1:]):
            with self.subTest(read=len(read)):
                main_mod._path_table(db, "muse_logs", read)
                main_mod._path_table(db, "muse_subagents", main_mod._muse_subagents(read))
                relation = db.sql(main_mod._muse_sql(main_mod._sql_file_list(
                    [*read, *main_mod._muse_parent_logs(read)])))
                columns = [item[0] for item in relation.description]
                rows = sorted(tuple(dict(zip(columns, row))[key] for key in (
                    "model", "execution_type", "input_tokens", "cache_read_tokens",
                    "output_tokens", "total_tokens", "project", "session"))
                    for row in relation.fetchall())
                expected = [
                    # The skill reminder; the subagent's attribution is its own log's call.
                    ("muse-spark-1.3", "subagent", 172, 16369, 598, 17139, "my-project", "01a1739f-session"),
                    ("muse-spark-1.3", "subagent", 5083, 0, 814, 5897, "my-project", "01a1739f-session"),
                    ("muse-spark-1.3", "subagent", 17740, 0, 526, 18266, "my-project", "01a1739f-session"),
                    ("muse-wonder-1.0", "interactive", 189, 35828, 494, 36511, "my-project", "01a1739f-session"),
                    ("muse-wonder-1.0", "interactive", 34317, 0, 151, 34468, "my-project", "01a1739f-session"),
                ]
                # A subagent log read alone still takes the session's project.
                assert rows == (expected if len(read) == 2 else expected[1:2])

    def test_gemini_timestamp_schema_shape_is_cast_before_slicing(self):
        tmp_path = self.tmp_path
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


class TestAOPSourceLoader(Case):
    def test_discovers_the_current_results_in_the_archive(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        archive = tmp_path / "results"
        first = archive / "a.json"
        second = archive / "b.json"
        for path in (first, second):
            _write_aop_result(path, run_id=path.stem, state_root=tmp_path / "one" / ".aop")
        # Originals that a migration replaced, and the run directories, are not read.
        _write_aop_result(archive / "originals" / "a.pre-v3.json", run_id="a",
                          state_root=tmp_path / "one" / ".aop")
        _write_aop_result(tmp_path / "one" / ".aop" / "runs" / "a" / "result.json", run_id="a")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [(main_mod.AOP_RESULT_TOOL, archive, "")])
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        files, dir_count, _ = main_mod.scan_source_files()

        assert set(files) == {first.as_posix(), second.as_posix()}
        assert dir_count == 1

    def test_takes_the_project_from_the_state_root(self):
        archive = self.tmp_path / "results" / "run-1.json"
        _write_aop_result(archive, state_root=Path("/code/Sycophant/.aop"))

        relation = duckdb.sql(main_mod._aop_sql(main_mod._sql_literal(archive.as_posix())))
        row = dict(zip([item[0] for item in relation.description], relation.fetchone()))

        assert row["project"] == "sycophant"
        assert row["project_path"] == "/code/Sycophant/.aop/worktrees/task-1"

    def test_archived_copy_replaces_cached_rows_of_its_deleted_run_directory(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        state_root = tmp_path / "bench" / ".aop"
        run_dir = state_root / "runs" / "run-1" / "result.json"
        archive = tmp_path / "aop" / "results"
        _write_aop_result(run_dir)
        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCES", {"aop": (main_mod.AOP_RESULT_TOOL, main_mod._aop_sql)})
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        def load(tree, refresh=False):
            monkeypatch.setattr(main_mod, "SOURCE_TREES", [(main_mod.AOP_RESULT_TOOL, tree, "")])
            db = duckdb.connect()
            register_macros(db)
            main_mod.load_tokens(db, refresh=refresh)
            return db.sql("SELECT count(*), sum(total_tokens), min(project) FROM tokens").fetchone()

        # A cache that read the run directory, as before the archive existed.
        def scan_run_directory(_root):
            return {run_dir.as_posix(): run_dir.stat()}

        scan_archive = main_mod._aop_results
        monkeypatch.setattr(main_mod, "_aop_results", scan_run_directory)
        assert load(archive) == (1, 110, "bench")
        monkeypatch.setattr(main_mod, "_aop_results", scan_archive)

        shutil.rmtree(tmp_path / "bench")
        _write_aop_result(archive / "run-1.json", state_root=state_root)
        _write_aop_result(archive / "run-2.json", run_id="run-2", state_root=state_root)
        assert load(archive) == (2, 220, "bench")
        assert load(archive, refresh=True) == (2, 220, "bench")

    def test_uses_provider_independent_token_semantics(self):
        tmp_path = self.tmp_path
        for provider, inference_provider, tool, expected in [
            ("agy", None, "Agy", (70, 10, 30, 110)),
            ("claude", None, "Claude Code", (70, 10, 30, 110)),
            ("codex", None, "Codex", (70, 10, 30, 110)),
            ("cursor", None, "Cursor", (70, 10, 30, 110)),
            ("devin", None, "Devin", (70, 10, 30, 110)),
            ("opencode", None, "OpenCode", (70, 10, 30, 110)),
            ("dsh", None, "DeepSeek Harness", (70, 10, 30, 110)),
            ("grok", None, "Grok", (70, 10, 30, 110)),
            ("hermes", None, "Hermes", (70, 10, 30, 110)),
            ("zcode", None, "Zcode", (70, 10, 30, 110)),
            ("dsh", "anthropic", "DeepSeek Harness", (70, 10, 30, 110)),
        ]:
            with self.subTest(provider=provider, inference_provider=inference_provider, tool=tool, expected=expected):
                result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
                _write_aop_result(
                    result_path,
                    provider=provider,
                    inference_provider=inference_provider,
                )

                relation = duckdb.sql(main_mod._aop_sql(
                    main_mod._sql_literal(result_path.as_posix())
                ))
                columns = [item[0] for item in relation.description]
                row = dict(zip(columns, relation.fetchone()))

                assert row["tool"] == tool
                assert (
                    row["input_tokens"], row["output_tokens"],
                    row["cache_read_tokens"], row["total_tokens"],
                ) == expected
                assert row["project"] == "project"
                assert row["project_path"] == result_path.parents[3].as_posix() + "/.aop/worktrees/task-1"
                assert row["execution_type"] == "subagent"
                assert row["token_count_type"] == "exact"
                assert row["source_kind"] == "aop"
                assert row["cost_usd"] == 0.25

    def test_rejects_results_that_are_not_v3(self):
        tmp_path = self.tmp_path
        result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
        _write_aop_result(result_path)
        value = json.loads(result_path.read_text())
        for schema in ("aop-token-usage-v2", "aop-token-usage-v4"):
            value["usage_schema"] = schema
            result_path.write_text(json.dumps(value))

            with raises(duckdb.Error, match="run aop migrate-results"):
                duckdb.sql(main_mod._aop_sql(
                    main_mod._sql_literal(result_path.as_posix())
                )).fetchall()

    def _load_aop(self, paths):
        db = duckdb.connect()
        register_macros(db)
        db.execute("CREATE TABLE tokens AS " + "".join(main_mod._build_source_sql(
            db, {main_mod.AOP_RESULT_TOOL: [path.as_posix() for path in paths]})))
        return db

    def test_accepts_v3_aop_usage(self):
        result_path = self.tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
        _write_aop_result(
            result_path,
            provider="claude",
            model="claude-sonnet-4-6",
            input_tokens=1_000,
            cached_input_tokens=200,
            cache_write_input_tokens=500,
            cache_write_1h_input_tokens=300,
            output_tokens=100,
            reasoning_output_tokens=0,
            cost_usd=None,
        )

        db = self._load_aop([result_path])
        price_tokens(db, all_rows=True)

        assert db.sql("""
            SELECT input_tokens, cache_read_tokens, cache_write_tokens,
                   cache_write_1h_tokens, output_tokens, total_tokens
            FROM tokens
        """).fetchall() == [
            (300, 200, 500, 300, 100, 1_100),
        ]
        cost, write_cost, write_1h_cost = db.sql("""
            SELECT api_cost_usd, api_cache_write_usd, api_cache_write_1h_usd
            FROM tokens
        """).fetchone()
        breakdown = model_prices.rates(
            "claude-sonnet-4-6",
            provider="anthropic",
            at="2026-08-14T12:00:00Z",
        ).breakdown(
            input_tokens=300,
            cache_read_tokens=200,
            cache_write_tokens=200,
            cache_write_1h_tokens=300,
            output_tokens=100,
        )
        assert cost == approx(breakdown.total)
        assert write_cost == approx(breakdown.cache_write)
        assert write_1h_cost == approx(breakdown.cache_write_1h)

    def test_accepts_usage_bearing_partial_accounting(self):
        tmp_path = self.tmp_path
        result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
        _write_aop_result(result_path, accounting_status="partial")

        row = duckdb.sql(main_mod._aop_sql(
            main_mod._sql_literal(result_path.as_posix())
        )).fetchone()

        assert (row[5], row[6], row[8], row[9]) == (70, 10, 30, 110)

    def test_ignores_accounting_without_usage(self):
        tmp_path = self.tmp_path
        for accounting_status in ["partial", "unavailable"]:
            with self.subTest(accounting_status=accounting_status):
                result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
                _write_aop_result(result_path)
                value = json.loads(result_path.read_text())
                value["accounting_status"] = accounting_status
                value["usage"] = None
                value["calculated_cost"] = None
                if accounting_status == "partial":
                    value["provider_reported_cost"] = {
                        "amount_usd": 0.25,
                        "currency": "USD",
                        "source": "provider",
                    }
                result_path.write_text(json.dumps(value))

                rows = duckdb.sql(main_mod._aop_sql(
                    main_mod._sql_literal(result_path.as_posix())
                )).fetchall()

                assert rows == []

    def test_rejects_invalid_accounting_status(self):
        tmp_path = self.tmp_path
        for accounting_status in [None, "future"]:
            with self.subTest(accounting_status=accounting_status):
                result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
                _write_aop_result(result_path)
                value = json.loads(result_path.read_text())
                if accounting_status is None:
                    value.pop("accounting_status")
                else:
                    value["accounting_status"] = accounting_status
                result_path.write_text(json.dumps(value))

                with raises(duckdb.Error, match="unsupported AOP accounting status"):
                    duckdb.sql(main_mod._aop_sql(
                        main_mod._sql_literal(result_path.as_posix())
                    )).fetchall()

    def test_aop_fixture_rejects_invalid_contract(self):
        tmp_path = self.tmp_path
        for overrides, message in [
            ({"input_tokens": -1}, "cannot be negative"),
            ({"cached_input_tokens": 101}, "cached input cannot exceed input"),
            ({"cached_input_tokens": 60, "cache_write_input_tokens": 50}, "cached input cannot exceed input"),
            ({"cache_write_input_tokens": 40, "cache_write_1h_input_tokens": 50}, "1h cache write cannot exceed cache write"),
            ({"reasoning_output_tokens": 11}, "reasoning output cannot exceed output"),
            ({"accounting_status": "unavailable"}, "requires complete or partial"),
        ]:
            with self.subTest(overrides=overrides, message=message):
                result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"

                with raises(ValueError, match=message):
                    _write_aop_result(result_path, **overrides)

    def test_resume_runs_aggregate_without_native_double_counting(self):
        tmp_path = self.tmp_path
        first = tmp_path / "project" / ".aop" / "runs" / "one" / "result.json"
        second = tmp_path / "project" / ".aop" / "runs" / "two" / "result.json"
        _write_aop_result(first, run_id="run-1", session_id="resumed", cost_usd=0.25)
        _write_aop_result(
            second,
            run_id="run-2",
            session_id="resumed",
            input_tokens=200,
            cached_input_tokens=50,
            output_tokens=20,
            reasoning_output_tokens=0,
            cost_usd=0.50,
        )
        source_expr = main_mod._sql_file_list([first.as_posix(), second.as_posix()])
        db = duckdb.connect()
        register_macros(db)
        db.execute("CREATE TABLE tokens AS " + main_mod._aop_sql(source_expr))
        db.execute("""
            INSERT INTO tokens (
                tool, project, session, date, model, input_tokens, output_tokens,
                cache_write_tokens, cache_read_tokens, total_tokens, execution_type,
                project_path, token_count_type, turn_count, retained_tokens,
                source_kind, cost_usd, source_file
            ) VALUES
                ('Codex', 'project', 'resumed', '2026-08-14', '', 999, 1,
                 0, 0, 1000, 'subagent', '/native', 'exact', 1, NULL,
                 'native', NULL, '/native/one.jsonl'),
                ('Codex', 'project', 'resumed', '2026-08-14', '', 999, 1,
                 0, 0, 1000, 'subagent', '/native', 'exact', 1, NULL,
                 'native', NULL, '/native/two.jsonl')
        """)

        main_mod._deduplicate_aop(db, main_mod.DebugTimer(False))
        price_tokens(db, all_rows=True)

        assert db.sql("SELECT count(*) FROM tokens").fetchone()[0] == 2
        assert db.sql("SELECT sum(total_tokens) FROM tokens").fetchone()[0] == 330
        assert db.sql("SELECT sum(turn_count) FROM tokens").fetchone()[0] == 2
        assert db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0] == 0.75


    def test_aop_run_without_a_model_or_native_log_assumes_the_days_model(self):
        tmp_path = self.tmp_path
        result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
        _write_aop_result(result_path, session_id="probe", model=None, cost_usd=None)
        source_expr = main_mod._sql_file_list([result_path.as_posix()])
        db = duckdb.connect()
        register_macros(db)
        db.execute("CREATE TABLE tokens AS " + main_mod._aop_sql(source_expr))
        date = db.sql("SELECT date FROM tokens").fetchone()[0]
        db.execute("""
            INSERT INTO tokens (tool, session, date, model, total_tokens, source_kind, source_file)
            VALUES ('Codex', 'busy', ?, 'gpt-5.6-sol', 900, 'native', '/native/one.jsonl'),
                   ('Codex', 'quiet', ?, 'gpt-5.5', 100, 'native', '/native/two.jsonl')
        """, [date, date])

        main_mod._deduplicate_aop(db, main_mod.DebugTimer(False))
        price_tokens(db, all_rows=True)

        assert db.sql("SELECT model, pricing, api_cost_usd > 0 FROM tokens WHERE session = 'probe'").fetchall() == [
            ("gpt-5.6-sol", "assumed model", True),
        ]

    def test_aop_run_without_a_model_takes_the_model_its_session_ran(self):
        tmp_path = self.tmp_path
        result_path = tmp_path / "project" / ".aop" / "runs" / "run" / "result.json"
        _write_aop_result(result_path, session_id="resumed", model=None, cost_usd=None)
        source_expr = main_mod._sql_file_list([result_path.as_posix()])
        db = duckdb.connect()
        register_macros(db)
        db.execute("CREATE TABLE tokens AS " + main_mod._aop_sql(source_expr))
        db.execute("""
            INSERT INTO tokens (tool, session, model, total_tokens, source_kind, source_file)
            VALUES ('Codex', 'resumed', 'gpt-5.6-sol', 900, 'native', '/native/one.jsonl'),
                   ('Codex', 'resumed', 'gpt-5.5', 100, 'native', '/native/one.jsonl'),
                   ('Codex', 'other', 'gpt-5.5', 5000, 'native', '/native/two.jsonl')
        """)

        main_mod._deduplicate_aop(db, main_mod.DebugTimer(False))

        assert db.sql("SELECT session, model, source_kind FROM tokens ORDER BY session").fetchall() == [
            ("other", "gpt-5.5", "native"), ("resumed", "gpt-5.6-sol", "aop"),
        ]

def _write_zcode_db(path: Path, requests: list[tuple]) -> sqlite3.Connection:
    """Create a minimal Zcode database; each request is
    (session, model, started_at_ms, input, output, cache_write, cache_read)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript("""
        CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL);
        CREATE TABLE model_usage (
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL, model_id TEXT NOT NULL,
            status TEXT NOT NULL, started_at INTEGER NOT NULL, completed_at INTEGER,
            input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0,
            cache_read_input_tokens INTEGER NOT NULL DEFAULT 0,
            error_type TEXT, error_message TEXT
        );
        INSERT INTO session VALUES ('main', NULL, '/code/Proj'), ('child', 'main', '/code/Proj');
    """)
    add_zcode_requests(connection, requests)
    return connection


def add_zcode_requests(connection: sqlite3.Connection, requests: list[tuple]) -> None:
    offset = connection.execute("SELECT count(*) FROM model_usage").fetchone()[0]
    connection.executemany(
        "INSERT INTO model_usage (id, session_id, model_id, status, started_at, input_tokens,"
        " output_tokens, cache_creation_input_tokens, cache_read_input_tokens)"
        " VALUES (?, ?, ?, 'completed', ?, ?, ?, ?, ?)",
        [(f"req-{offset + index}", *request) for index, request in enumerate(requests)],
    )
    connection.commit()


class TestZcodeSourceLoader(Case):
    def _configure(self, tmp_path, monkeypatch):
        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            (main_mod.ZCODE_TOOL, tmp_path / ".zcode" / "cli" / "db", "db.sqlite"),
        ])
        monkeypatch.setattr(main_mod, "SOURCES", {
            "zcode": (main_mod.ZCODE_TOOL, main_mod._zcode_sql),
        })
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])
        return tmp_path / ".zcode" / "cli" / "db" / "db.sqlite"

    def _load(self):
        db = duckdb.connect()
        register_macros(db)
        main_mod.load_tokens(db, refresh=False)
        return db

    def test_normalizes_requests_with_inclusive_input_counts(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        path = self._configure(tmp_path, monkeypatch)
        # 2026-09-27T14:11:47.654Z
        connection = _write_zcode_db(path, [
            ("main", "GLM-5.3", 1790518307654, 1000, 50, 0, 900),
            ("child", "GLM-5.3-Flash", 1790518307654, 100, 10, 0, 0),
            ("main", "GLM-5.3", 1790518307654, 0, 0, 0, 0),
        ])

        db = self._load()
        connection.close()
        rows = db.sql("""
            SELECT tool, project, session, date, model, input_tokens, output_tokens,
                   cache_read_tokens, total_tokens, execution_type, timestamp
            FROM tokens ORDER BY model
        """).fetchall()

        assert rows == [
            ("Zcode", "proj", "main", "2026-09-27", "zai/glm-5.3", 100, 50, 900, 1050,
             "interactive", "2026-09-27T14:11:47.654Z"),
            ("Zcode", "proj", "child", "2026-09-27", "zai/glm-5.3-flash", 100, 10, 0, 110,
             "subagent", "2026-09-27T14:11:47.654Z"),
        ]
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        assert cost == approx(
            (100 * 1.40 + 900 * 0.26 + 50 * 4.40 + 100 * 0.15 + 10 * 0.50) / 1e6
        )

    def test_requests_are_timed_at_completion(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        path = self._configure(tmp_path, monkeypatch)
        connection = _write_zcode_db(path, [("main", "GLM-5.3", 1790522685219, 10, 1, 0, 0)])
        connection.execute("UPDATE model_usage SET completed_at = 1790522692694")
        connection.commit()

        db = self._load()
        connection.close()

        assert db.sql("SELECT timestamp FROM tokens").fetchone()[0] == "2026-09-27T15:24:52.694Z"

    def test_usage_limit_errors_become_full_quota_observations(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        path = self._configure(tmp_path, monkeypatch)
        connection = _write_zcode_db(path, [("main", "GLM-5.3", 1790518307654, 10, 1, 0, 0)])
        connection.executemany(
            "INSERT INTO model_usage (id, session_id, model_id, status, started_at,"
            " error_type, error_message) VALUES (?, 'main', 'GLM-5.3', 'error', ?, ?, ?)",
            [
                ("limit", 1790522463000, "rate_limited",
                 "[1308][Usage limit reached for 5 hour. Your limit will reset at "
                 "2026-09-28 03:11:57][20260927232104af14a2546f604536]"),
                ("throttle", 1790522464000, "rate_limited", "Too many requests"),
                ("cancel", 1790522465000, "cancelled", "Model request was cancelled."),
            ],
        )
        connection.commit()

        db = self._load()
        connection.close()
        rows = db.sql("""
            SELECT total_tokens, turn_count, quota_used_pct, quota_resets_at,
                   quota_window_minutes, quota_limit_id
            FROM tokens ORDER BY timestamp
        """).fetchall()

        # 2026-09-28 03:11:57 China Standard Time is 2026-09-27T19:11:57Z.
        assert rows == [
            (11, 1, None, None, None, None),
            (0, 0, 100.0, 1790536317, 300, "5h"),
        ]

    def test_cache_refreshes_when_only_the_write_ahead_log_changes(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        path = self._configure(tmp_path, monkeypatch)
        connection = _write_zcode_db(path, [("main", "GLM-5.3", 1790518307654, 10, 1, 0, 0)])
        assert self._load().sql("SELECT count(*) FROM tokens").fetchone()[0] == 1

        add_zcode_requests(connection, [("main", "GLM-5.3", 1790518307655, 10, 1, 0, 0)])
        assert Path(f"{path}-wal").exists()

        assert self._load().sql("SELECT count(*) FROM tokens").fetchone()[0] == 2
        connection.close()

# ===========================================================================
# fmt() tests
# ===========================================================================

class TestOneHourCachePricing(Case):
    def test_one_hour_cache_writes_cost_more_than_five_minute_ones(self):
        db = duckdb.connect()
        db.execute(f"CREATE TABLE tokens ({main_mod.TOKEN_SCHEMA})")
        # Two Claude Opus 5.5 requests that each write 1M cache tokens: one to the
        # five-minute cache ($5/M), one to the one-hour cache (2x the $4/M input).
        db.execute("""
            INSERT INTO tokens (tool, project, session, date, model, input_tokens,
                output_tokens, cache_write_tokens, cache_read_tokens, cache_write_1h_tokens,
                total_tokens, execution_type, source_kind, timestamp)
            VALUES
                ('Claude Code', 'p', 's', '2026-10-01', 'claude-opus-5-5', 0, 0, 1000000, 0, 0,
                 1000000, 'subagent', 'native', '2026-10-01T12:00:00Z'),
                ('Claude Code', 'p', 's', '2026-10-01', 'claude-opus-5-5', 0, 0, 1000000, 0,
                 1000000, 1000000, 'interactive', 'native', '2026-10-01T12:00:00Z')
        """)

        price_tokens(db, all_rows=True)

        costs = db.sql("SELECT execution_type, api_cost_usd FROM tokens ORDER BY 1").fetchall()
        assert costs == [("interactive", approx(8.0)), ("subagent", approx(5.0))]


class TestFmt(Case):
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

class TestShareFormatters(Case):
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

class TestTokenCounting(Case):
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

class TestCostCalculation(Case):
    @staticmethod
    def _cost(*rows):
        return _make_db(*rows).sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]

    def test_rows_are_priced_by_model_prices_at_their_date(self):
        for row in [SONNET_ROW, OPUS_ROW, HAIKU_ROW, GEMINI_ROW]:
            with self.subTest(row=row):
                tool, _, _, date, model, uncached, output, written, read, _ = row
                expected = model_prices.cost(
                    model, provider=TOOL_PROVIDERS[tool], at=date, input_tokens=uncached,
                    output_tokens=output, cache_write_tokens=written, cache_read_tokens=read,
                )

                assert expected is not None
                assert self._cost(row) == approx(expected)

    def test_opus_5_5_uses_its_own_rates(self):
        row = ("Claude Code", "p", "s", "2026-09-25", "claude-opus-5-5",
               1000, 100, 200, 10000, 11300)

        assert self._cost(row) == approx(
            (1000 * 4 + 100 * 20 + 200 * 5 + 10000 * 0.20) / 1e6
        )

    def test_price_changes_apply_from_their_date(self):
        before = ("Codex", "p", "s", "2026-08-20", "gpt-5.6-sol", 1000, 100, 0, 0, 1100)
        after = ("Codex", "p", "s", "2026-09-01", "gpt-5.6-sol", 1000, 100, 0, 0, 1100)

        assert self._cost(before) == approx((1000 * 5 + 100 * 30) / 1e6)
        assert self._cost(after) == approx((1000 * 4 + 100 * 20) / 1e6)

    def test_current_prices_reprice_every_row(self):
        db = _make_db(("Codex", "p", "s", "2026-08-20", "gpt-5.6-sol", 1000, 100, 0, 0, 1100))

        current = db.sql("SELECT sum(current_api_cost_usd) FROM tokens").fetchone()[0]
        assert current == approx((1000 * 4 + 100 * 20) / 1e6)

    def test_deepseek_peak_hours_use_the_request_time(self):
        peak = ("DeepSeek Harness", "p", "s", "2026-09-30", "deepseek-flash",
                1000, 0, 0, 0, 1000, "2026-09-30T02:00:00Z")
        off_peak = peak[:10] + ("2026-09-30T12:00:00Z",)

        assert self._cost(peak) == approx(1000 * 0.30 / 1e6)
        assert self._cost(off_peak) == approx(1000 * 0.15 / 1e6)

    def test_long_context_tier_applies_per_request(self):
        for context_tokens in [272000, 272001]:
            with self.subTest(context_tokens=context_tokens):
                uncached, written = 1000, 500
                cached = context_tokens - uncached - written
                row = ("Codex", "proj", "s", "2026-09-20", "gpt-6-astra",
                       uncached, 200, written, cached, context_tokens + 200)
                rates = (20, 25, 2, 75) if context_tokens > 272000 else (10, 12.5, 1, 50)
                expected = 2 * (uncached * rates[0] + written * rates[1] + cached * rates[2]
                                + 200 * rates[3]) / 1e6

                # Two standard requests remain standard even when their aggregate exceeds the limit.
                assert self._cost(row, row) == approx(expected)

    def test_unknown_models_stay_unpriced(self):
        db = _make_db(CODEX_ROW)

        assert db.sql(f"SELECT sum({COST_PER_ROW}), any_value(pricing) FROM tokens").fetchone() == (
            0, "unpriced"
        )

    def test_aop_runs_are_priced_at_their_recorded_cost(self):
        # A Muse run's usage covers calls on two models; AOP priced each call at its own.
        db = _make_db()
        db.execute("""
            INSERT INTO tokens (tool, date, timestamp, model, input_tokens, output_tokens,
                                cache_write_tokens, cache_read_tokens, total_tokens,
                                source_kind, cost_usd)
            VALUES ('Muse', '2026-10-25', '2026-10-25T12:00:00Z', 'muse-wonder-1.0',
                    49747, 300, 0, 0, 50047, 'aop', 0.14268),
                   ('Muse', '2026-10-25', '2026-10-25T12:00:00Z', 'muse-wonder-1.0',
                    49747, 300, 0, 0, 50047, 'aop', NULL)
        """)
        price_tokens(db, all_rows=False)

        recorded, repriced = db.sql("""
            SELECT api_cost_usd, current_api_cost_usd, api_input_usd + api_output_usd, pricing
            FROM tokens ORDER BY cost_usd NULLS LAST
        """).fetchall()
        assert recorded[0] == approx(0.14268) and recorded[3] == "aop"
        # Today's price and the parts follow model-prices, scaled to the recorded cost.
        assert recorded[1] == approx(0.14268) and recorded[2] == approx(0.14268)
        assert repriced[0] > recorded[0] and repriced[3] == "model-prices"

    def test_aop_runs_of_unknown_models_keep_their_recorded_cost(self):
        db = _make_db()
        db.execute("""
            INSERT INTO tokens (tool, date, model, input_tokens, output_tokens,
                                cache_write_tokens, cache_read_tokens, total_tokens,
                                source_kind, cost_usd)
            VALUES ('Devin', '2026-09-01', 'no-such-model', 10, 1, 0, 0, 11, 'aop', 0.5)
        """)
        price_tokens(db, all_rows=False)

        assert db.sql("SELECT api_cost_usd, current_api_cost_usd, pricing FROM tokens").fetchone() == (
            0.5, 0.5, "aop"
        )

    def test_models_without_a_per_token_price_are_estimated_and_labelled(self):
        db = _make_db()
        db.execute("""
            INSERT INTO tokens (tool, date, model, input_tokens, output_tokens,
                                cache_write_tokens, cache_read_tokens, total_tokens, source_kind)
            VALUES ('Muse', '2026-10-26', 'muse-wonder-full-1.0', 1000000, 0, 0, 0, 1000000, 'native'),
                   ('Muse', '2026-10-26', 'muse-wonder-1.0', 1000000, 0, 0, 0, 1000000, 'native')
        """)
        price_tokens(db, all_rows=False)

        rows = db.sql("SELECT model, api_cost_usd, pricing, estimated_from FROM tokens ORDER BY model").fetchall()
        # Muse Wonder Full is sold only in Muse Code subscriptions; model-prices prices it at
        # Muse Wonder's rates, $4 per million input tokens.
        assert rows == [("muse-wonder-1.0", approx(4.0), "model-prices", None),
                        ("muse-wonder-full-1.0", approx(4.0), "estimated", "meta/muse-wonder-1.0")]

    def test_null_cost_on_empty(self):
        assert self._cost() is None


# ===========================================================================
# DuckDB macro tests
# ===========================================================================

class TestMacros(Case):
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

class TestQueries(Case):
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

class TestPlanDetection(Case):
    @staticmethod
    def _run(documents, returncode=0):
        """Answer `python -m clanker_usage PROVIDER json` with documents[PROVIDER]."""
        def run(cmd, **kwargs):
            provider = cmd[3]
            document = {"schema": "usage-v1", "provider": provider, "account": "a",
                        "status": "live", "error": None, "windows": [], "details": {},
                        **documents.get(provider, {"plan": None, "status": "unavailable"})}
            return subprocess.CompletedProcess(cmd, returncode, json.dumps(document), "")
        return mock.patch("subprocess.run", side_effect=run)

    def test_plans_are_priced_by_tool(self):
        for provider, plan, tool, expected in [
            ("claude", "max_20x", "Claude Code", ("max_20x", 200)),
            ("claude", "pro", "Claude Code", ("pro", 20)),
            ("claude", "max_5x", "Claude Code", ("max_5x", 100)),
            ("claude", "default_claude_max_5x", "Claude Code", ("max_5x", 100)),
            ("codex", "pro", "Codex", ("pro", 200)),
            ("codex", "plus", "Codex", ("plus", 20)),
            ("gemini", "g1-pro-tier", "Gemini", ("pro", 19.99)),
            # Google AI Ultra has two tiers that the plan name cannot tell apart.
            ("gemini", "g1-ultra-tier", "Gemini", ("ultra", 0)),
            # As clanker-usage names Google plans, with the tier id: $100 Ultra is ultra_100.
            ("gemini", "Google AI Ultra (g1-ultra-lite-tier)", "Gemini", ("ultra_100", 99.99)),
            ("agy", "Google AI Ultra (g1-ultra-lite-tier)", "Gemini", ("ultra_100", 99.99)),
            ("agy", "Google AI Ultra", "Gemini", ("Google AI Ultra", 0)),
            ("zcode", "lite", "Zcode", ("lite", 18)),
        ]:
            with self.subTest(provider=provider, plan=plan):
                with self._run({provider: {"plan": plan}}):
                    assert detect_plans()[tool] == expected

    def test_only_a_fresh_reading_names_a_plan(self):
        for status, plan, expected in [("live", "lite", ("lite", 18)), ("cached", "lite", ("lite", 18)),
                                       ("stale", "lite", None), ("live", None, None)]:
            with self.subTest(status=status, plan=plan):
                with self._run({"zcode": {"plan": plan, "status": status}}):
                    assert detect_plans().get("Zcode") == expected

    def test_unavailable_usage_names_no_plan(self):
        with self._run({}, returncode=1):
            assert detect_plans() == {}

    def test_clanker_usage_not_installed(self):
        with mock.patch("subprocess.run", side_effect=FileNotFoundError):
            assert detect_plans() == {}

    def test_tool_timeout(self):
        with mock.patch("subprocess.run", side_effect=subprocess.TimeoutExpired("x", 10)):
            assert detect_plans() == {}

    def test_output_without_a_usage_document(self):
        with mock.patch("subprocess.run", return_value=subprocess.CompletedProcess(
                [], 1, "not json", "No module named clanker_usage")):
            assert detect_plans() == {}

    def test_subscription_cost_sum(self):
        with self._run({"claude": {"plan": "max_20x"}, "codex": {"plan": "pro"},
                        "gemini": {"plan": "g1-pro-tier"}}):
            plans = detect_plans()
        assert sum(c for _, c in plans.values()) == approx(419.99)


# ===========================================================================
# Chart generation tests
# ===========================================================================

class TestChartGeneration(Case):
    def test_generate_saves_to_a_chosen_path(self):
        import clanker_analytics.share as share_mod
        self.monkeypatch.setattr(share_mod, "OUTPUT", self.tmp_path / "default.png")
        chosen = self.tmp_path / "charts" / "month.png"

        path = share_mod.generate(_make_db(*ALL_ROWS), "7d", {}, output=chosen)

        assert path == chosen and chosen.read_bytes()[:4] == b"\x89PNG"
        assert not (self.tmp_path / "default.png").exists()

    def test_generate_draws_weekly_bars_for_long_ranges(self):
        import clanker_analytics.share as share_mod
        self.monkeypatch.setattr(share_mod, "OUTPUT", self.tmp_path / "default.png")
        rows = [("Claude Code", "alpha", "s1", day, "claude-opus-4-6", 10, 20, 0, 0, 30)
                for day in ("2026-01-05", "2026-01-06", "2026-02-11", "2026-04-20")]
        bars = []
        original = share_mod._plot_periods
        self.monkeypatch.setattr(share_mod, "_plot_periods",
                                 lambda ax, periods, step, series: bars.append(
                                     (periods, step)) or original(ax, periods, step, series))

        path = share_mod.generate(_make_db(*rows), "2026-01-01", {})

        periods, step = bars[0]
        assert path.exists() and step == 7
        assert periods[0].isoformat() == "2026-01-05" and periods[-1].isoformat() == "2026-04-20"
        assert all(day.weekday() == 0 for day in periods)

    def test_generate_creates_png(self):
        tmp_path = self.tmp_path
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

    def test_generate_with_plans(self):
        tmp_path = self.tmp_path
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

    def test_generate_empty_data_returns_none(self):
        tmp_path = self.tmp_path
        db = _make_db()  # empty
        import clanker_analytics.share as share_mod
        orig = share_mod.OUTPUT
        share_mod.OUTPUT = tmp_path / "test.png"
        try:
            path = share_mod.generate(db, "24h", {})
            assert path is None
        finally:
            share_mod.OUTPUT = orig

    def test_bar_chart_for_single_date(self):
        tmp_path = self.tmp_path
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

    def test_area_chart_for_many_dates(self):
        tmp_path = self.tmp_path
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

class TestEdgeCases(Case):
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
        row = ("Claude Code", "p", "s", "2026-03-15", "claude-sonnet-4-6", 1000, 200, 0, 0, 1200)
        db = _make_db(row)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        assert cost > 0

    def test_all_cache_tokens(self):
        row = ("Claude Code", "p", "s", "2026-03-15", "claude-sonnet-4-6", 0, 0, 0, 10000, 10000)
        db = _make_db(row)
        cost = db.sql(f"SELECT sum({COST_PER_ROW}) FROM tokens").fetchone()[0]
        expected = (10000 * 0.30) / 1e6
        assert abs(cost - expected) < 0.0001


# ===========================================================================
# Windows path tests
# ===========================================================================

class TestWindowsPaths(Case):
    def test_home_backslash_replaced(self):
        home = "C:\\Users\\Test".replace("\\", "/")
        assert home == "C:/Users/Test"

    def test_cache_file_posix(self):
        p = Path("C:/Users/Test/.cache/clanker-analytics/tokens.parquet")
        assert "\\" not in p.as_posix()


# ===========================================================================
# Debug / profiling tests
# ===========================================================================

def _codex_line(timestamp, kind, payload):
    return json.dumps({"timestamp": timestamp, "type": kind, "payload": payload}) + "\n"


def _codex_tokens(timestamp, total, last=None):
    info = {"total_token_usage": {"total_tokens": total}}
    if last is not None:
        info["last_token_usage"] = {"total_tokens": last, "input_tokens": last - 10,
                                    "output_tokens": 10, "cached_input_tokens": 0}
    return _codex_line(timestamp, "event_msg", {"type": "token_count", "info": info})


def _claude_line(timestamp, output):
    return json.dumps({
        "type": "assistant", "timestamp": timestamp, "sessionId": "s1", "cwd": "/code/alpha",
        "message": {"model": "claude-opus-4-6", "usage": {
            "input_tokens": 5, "output_tokens": output, "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 100}},
    }) + "\n"


class TestClaudeHomes(Case):
    def test_claude_homes_come_from_the_environment_and_clanker_usage(self):
        home = self.tmp_path / "home"
        config = self.tmp_path / "config" / "clanker-usage"
        config.mkdir(parents=True)
        for name in (".claude", ".claude-b", ".claude-c", "elsewhere"):
            (home / name).mkdir(parents=True)
        (config / "config.json").write_text(json.dumps({"claude": {"config_dirs": [
            str(home / ".claude-b"), str(home / ".claude-c"), str(home / ".claude")]}}))
        self.monkeypatch.setattr(main_mod, "HOME", home.as_posix())
        self.monkeypatch.setenv("XDG_CONFIG_HOME", str(self.tmp_path / "config"))
        self.monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home / "elsewhere"))

        # ~/.claude is scanned already; each other home once.
        assert main_mod._claude_config_dirs() == [
            home / "elsewhere", home / ".claude-b", home / ".claude-c"]

        self.monkeypatch.delenv("CLAUDE_CONFIG_DIR")
        (config / "config.json").write_text("{")
        assert main_mod._claude_config_dirs() == []


class TestAppendOnlyLogs(Case):
    def setUp(self):
        super().setUp()
        self.claude = self.tmp_path / "claude" / "-code-alpha" / "s1.jsonl"
        self.codex = (self.tmp_path / "codex" / "2026" / "10" / "08"
                      / "rollout-2026-10-08T10-00-00-019fd3f1-91fd-7902-8893-028d79084e33.jsonl")
        for path in (self.claude, self.codex):
            path.parent.mkdir(parents=True)
        self.monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            ("Claude Code", self.tmp_path / "claude", "*.jsonl"),
            ("Codex", self.tmp_path / "codex", "*.jsonl"),
        ])
        self.monkeypatch.setattr(main_mod, "_WSL_HOMES", [])
        self.use_cache("cache")

    def use_cache(self, name):
        cache_dir = self.tmp_path / name
        self.monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        self.monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        self.monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")

    def load(self, refresh=False):
        db = duckdb.connect()
        register_macros(db)
        timer = main_mod.DebugTimer(True)
        with mock.patch("sys.stderr"):
            main_mod.load_tokens(db, refresh=refresh, timing=timer)
        rows = sorted(db.sql("SELECT * EXCLUDE (api_cost_usd, current_api_cost_usd, pricing) "
                             "FROM tokens").fetchall(), key=repr)
        return rows, timer

    def rebuilt_rows(self):
        self.use_cache("rebuilt")
        rows, _ = self.load(refresh=True)
        self.use_cache("cache")
        return rows

    def append(self, path, text):
        with open(path, "a") as log:
            log.write(text)

    def test_codex_log_moved_to_archived_sessions_counts_once(self):
        archived = self.tmp_path / "archived" / self.codex.name
        archived.parent.mkdir()
        self.monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            ("Codex", self.tmp_path / "codex", "*.jsonl"),
            ("Codex", archived.parent, "*.jsonl"),
        ])
        self.codex.write_text(
            _codex_line("2026-10-08T10:00:00Z", "session_meta",
                        {"id": "019fd3f1-91fd-7902-8893-028d79084e33", "cwd": "/code/beta"})
            + _codex_line("2026-10-08T10:00:01Z", "turn_context", {"model": "gpt-5.5"})
            + _codex_tokens("2026-10-08T10:00:20Z", 100, 100)
            + _codex_tokens("2026-10-08T10:01:20Z", 250, 150))
        rows, _ = self.load()
        assert len(rows) == 2

        self.codex.rename(archived)
        moved, _ = self.load()
        assert len(moved) == 2
        assert {row[-1] for row in moved} == {row[-1] for row in rows}
        assert len(self.load(refresh=True)[0]) == 2
        # Once archived logs are deleted too, their rows are kept.
        archived.unlink()
        assert len(self.load()[0]) == 2
        assert len(self.load(refresh=True)[0]) == 2

    def test_new_lines_parse_as_the_whole_log_does(self):
        self.claude.write_text(_claude_line("2026-10-08T10:00:01Z", 10)
                               + _claude_line("2026-10-08T10:00:02Z", 20))
        self.codex.write_text(
            _codex_line("2026-10-08T10:00:00Z", "session_meta",
                        {"id": "019fd3f1-91fd-7902-8893-028d79084e33", "cwd": "/code/beta"})
            + _codex_line("2026-10-08T10:00:01Z", "turn_context", {"model": "gpt-5.5"})
            + _codex_line("2026-10-08T10:00:01Z", "event_msg", {
                "type": "thread_settings_applied",
                "thread_settings": {"model": "gpt-5.5", "service_tier": "priority"}})
            + _codex_tokens("2026-10-08T10:00:20Z", 100, 100))
        rows, _ = self.load()
        assert len(rows) == 3

        steps = [
            # A partial last line waits for its newline; a repeated total adds nothing.
            (_claude_line("2026-10-08T10:01:00Z", 30) + _claude_line("2026-10-08T10:01:01Z", 40)[:50],
             _codex_tokens("2026-10-08T10:01:00Z", 100, 100)
             + _codex_tokens("2026-10-08T10:01:10Z", 250, 150)
             + _codex_line("2026-10-08T10:01:20Z", "turn_context", {"model": "gpt-5.6"})
             + _codex_tokens("2026-10-08T10:01:30Z", 400, 150)),
            # Totals without a last-turn count take the difference from the previous total.
            (_claude_line("2026-10-08T10:01:01Z", 40)[50:] + _claude_line("2026-10-08T10:02:00Z", 50),
             _codex_tokens("2026-10-08T10:02:00Z", 500)
             + _codex_tokens("2026-10-08T10:02:10Z", 500)),
        ]
        for step, (claude_text, codex_text) in enumerate(steps):
            with self.subTest(step=step):
                self.append(self.claude, claude_text)
                self.append(self.codex, codex_text)
                rows, timer = self.load()
                assert rows == self.rebuilt_rows()
                labels = {sample.label for sample in timer.samples}
                assert "copy new log lines" in labels
                # The rebuild recorded only the Claude log's position, so the Codex log
                # is read whole once; after that both logs are read from their positions.
                assert ("drop changed rows" in labels) == (step == 0)
        assert len(rows) == 9
        positions = main_mod._load_cache_meta()[3]
        assert positions[self.claude.as_posix()].offset == self.claude.stat().st_size
        assert len(positions[self.codex.as_posix()].context) == 4

        # A rewritten log is read again from its start.
        self.claude.write_text(_claude_line("2026-10-08T11:00:00Z", 99) * 3)
        rows, _ = self.load()
        assert rows == self.rebuilt_rows()
        assert [row[6] for row in rows if row[0] == "Claude Code"] == [99, 99, 99]

    def test_codex_log_inside_a_forks_replay_window_is_read_whole(self):
        meta = _codex_line("2026-10-08T10:00:00Z", "session_meta",
                           {"id": "019fd3f1-91fd-7902-8893-028d79084e33", "cwd": "/code/beta",
                            "forked_from_id": "parent"})
        model = _codex_line("2026-10-08T10:00:00Z", "turn_context", {"model": "gpt-5.5"})
        replay = "".join(_codex_tokens(f"2026-10-08T10:00:01.{i:03d}Z", 100 * (i + 1), 100)
                         for i in range(12))
        self.codex.write_text(meta + model + replay)
        mirror = self.tmp_path / "mirror.jsonl"
        position, whole = main_mod._stage_log(self.codex.as_posix(), "Codex", None, mirror)
        assert whole

        self.append(self.codex, _codex_tokens("2026-10-08T10:05:00Z", 1300, 100))
        _, whole = main_mod._stage_log(self.codex.as_posix(), "Codex", position, mirror)
        assert whole

    def test_vanished_log_keeps_its_rows_after_a_tail_read(self):
        self.claude.write_text(_claude_line("2026-10-08T10:00:01Z", 10))
        self.load()
        self.append(self.claude, _claude_line("2026-10-08T10:00:02Z", 20))
        rows, _ = self.load()
        assert len(rows) == 2
        self.claude.unlink()
        rows, _ = self.load()
        assert len(rows) == 2


class TestDebugHooks(Case):
    def test_source_scan_discovers_only_canonical_agy_transcript(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
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

    def test_source_scan_skips_aop_run_logs_inside_a_tool_tree(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        projects = tmp_path / "projects"
        transcript = projects / "-code-debate" / "session.jsonl"
        run_log = projects / "-code-debate" / "memory" / ".aop" / "runs" / "run" / "events.jsonl"
        run_log.parent.mkdir(parents=True)
        transcript.write_text("{}\n")
        run_log.write_text("{}\n")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [("Claude Code", projects, "*.jsonl")])
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        files, _, _ = main_mod.scan_source_files()

        assert list(files) == [transcript.as_posix()]

    def test_source_scan_reads_claude_subagent_transcripts_but_not_workflow_journals(self):
        projects = self.tmp_path / "projects"
        workflow = projects / "-code-x" / "session" / "subagents" / "workflows" / "wf_1"
        workflow.mkdir(parents=True)
        transcripts = [projects / "-code-x" / "session.jsonl",
                       projects / "-code-x" / "session" / "subagents" / "agent-a1.jsonl",
                       workflow / "agent-b2.jsonl"]
        for path in [*transcripts, workflow / "journal.jsonl"]:
            path.write_text("{}\n")
        self.monkeypatch.setattr(main_mod, "SOURCE_TREES", [("Claude Code", projects, "*.jsonl")])
        self.monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        files, _, _ = main_mod.scan_source_files()

        assert set(files) == {path.as_posix() for path in transcripts}

    def test_sources_mtime_includes_gemini_json(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
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

    def test_incremental_cache_updates_only_changed_files(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
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
                    'native' as source_kind,
                    NULL::DOUBLE as cost_usd,
                    replace(filename, '\\\\', '/') as source_file,
                    NULL::VARCHAR as timestamp
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

    def test_cache_keeps_rows_of_deleted_files_through_updates_and_rebuilds(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        file_a = src_dir / "a.jsonl"
        file_b = src_dir / "b.jsonl"
        file_a.write_text(json.dumps({
            "project": "alpha", "session": "s1", "date": "2026-03-15", "model": "m",
            "input_tokens": 1, "output_tokens": 2, "cache_write_tokens": 0,
            "cache_read_tokens": 0, "total_tokens": 3,
        }) + "\n")
        file_b_text = json.dumps({
            "project": "beta", "session": "s2", "date": "2026-03-15", "model": "m",
            "input_tokens": 4, "output_tokens": 5, "cache_write_tokens": 0,
            "cache_read_tokens": 0, "total_tokens": 9,
        }) + "\n"
        file_b.write_text(file_b_text)

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
                    'native' as source_kind,
                    NULL::DOUBLE as cost_usd,
                    replace(filename, '\\\\', '/') as source_file,
                    NULL::VARCHAR as timestamp
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

        def load(refresh=False):
            db = duckdb.connect()
            register_macros(db)
            main_mod.load_tokens(db, refresh=refresh)
            return db.sql("SELECT project, total_tokens FROM tokens ORDER BY project").fetchall()

        expected = [("alpha", 3), ("beta", 9)]
        assert load() == expected
        # A missing file is reported once, at the first cache write after it disappears.
        line = file_a.read_text()
        outputs = []
        for copies in (2, 1):
            file_a.write_text(line * copies)
            with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
                assert load() == [("alpha", 3)] * copies + [("beta", 9)]
            outputs.append(stderr.getvalue())
        assert "1 changed, 1 files no longer on disk, their rows kept" in outputs[0]
        assert "no longer on disk" not in outputs[1]
        assert load(refresh=True) == expected
        monkeypatch.setattr(main_mod, "CACHE_SCHEMA_VERSION", main_mod.CACHE_SCHEMA_VERSION + 1)
        assert load() == expected

        # A deleted file that reappears replaces its kept rows instead of adding to them.
        file_b.write_text(file_b_text)
        assert load() == expected
        assert load(refresh=True) == expected

    def test_files_deleted_after_the_scan_are_skipped(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        src_dir = tmp_path / "src"
        src_dir.mkdir()

        def write(name, project, total):
            (src_dir / name).write_text(json.dumps({
                "project": project, "session": "s", "date": "2026-03-15", "model": "m",
                "total_tokens": total,
            }) + "\n")

        def build_sql(source_expr):
            return f"""
                SELECT 'TestTool' AS tool, project, session, date, model,
                    0 AS input_tokens, 0 AS output_tokens, 0 AS cache_write_tokens,
                    0 AS cache_read_tokens, total_tokens::BIGINT AS total_tokens,
                    'interactive' AS execution_type, project AS project_path,
                    'exact' AS token_count_type, 1 AS turn_count,
                    NULL::BIGINT AS retained_tokens, 'native' AS source_kind,
                    NULL::DOUBLE AS cost_usd, replace(filename, '\\', '/') AS source_file,
                    NULL::VARCHAR AS timestamp
                FROM read_json({source_expr}, format='newline_delimited', filename=true)
            """

        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [("TestTool", src_dir, "*.jsonl")])
        monkeypatch.setattr(main_mod, "SOURCES", {"test": ("TestTool", build_sql)})
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])
        scan = main_mod.scan_source_files
        deleted_after_scan: list[str] = []

        def scan_then_delete():
            result = scan()
            for name in deleted_after_scan:
                (src_dir / name).unlink()
            return result

        monkeypatch.setattr(main_mod, "scan_source_files", scan_then_delete)

        def load():
            db = duckdb.connect()
            register_macros(db)
            main_mod.load_tokens(db, refresh=False)
            return db.sql("SELECT project, total_tokens FROM tokens ORDER BY project").fetchall()

        write("a.jsonl", "alpha", 3)
        write("b.jsonl", "beta", 9)
        deleted_after_scan[:] = ["b.jsonl"]
        assert load() == [("alpha", 3)]

        # A new file and a changed cached one both vanish before an update reads them;
        # the cached one keeps its rows.
        deleted_after_scan.clear()
        write("b.jsonl", "beta", 9)
        assert load() == [("alpha", 3), ("beta", 9)]
        write("b.jsonl", "beta", 10)
        os.utime(src_dir / "b.jsonl", ns=(1, 1))
        write("c.jsonl", "gamma", 5)
        deleted_after_scan[:] = ["b.jsonl", "c.jsonl"]
        assert load() == [("alpha", 3), ("beta", 9)]

    def test_agy_resume_replaces_cached_session_without_double_counting(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
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

    def test_agy_metadata_change_reattributes_cached_session(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
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

    def test_claude_projects_are_named_after_the_launch_directory(self):
        db = duckdb.connect()
        db.execute(f"CREATE TABLE tokens ({main_mod.TOKEN_SCHEMA})")
        folder = "/home/u/.claude/projects/-code-llama-cpp"
        db.execute("""
            INSERT INTO tokens (tool, project, project_path, source_kind, source_file) VALUES
                ('Claude Code', 'llama.cpp', '/code/llama.cpp', 'native', ?),
                ('Claude Code', 'cat', '/code/llama.cpp/build/exp/cat', 'native', ?),
                ('Claude Code', 'codex', '/code/codex', 'native', ?),
                ('Claude Code', 'sub', '/code/llama.cpp/sub', 'native', ?),
                ('Claude Code', 'other', '/elsewhere/other', 'native', '/home/u/.claude/projects/-x/b.jsonl'),
                ('Codex', 'cat', '/code/llama.cpp/build/exp/cat', 'native', '/home/u/.codex/c.jsonl')
        """, [f"{folder}/a.jsonl", f"{folder}/a.jsonl", f"{folder}/a.jsonl",
              f"{folder}/a/subagents/agent-1.jsonl"])

        main_mod._name_claude_projects(db, main_mod.DebugTimer(False))

        assert db.sql("SELECT tool, project FROM tokens").fetchall() == [
            ("Claude Code", "llama.cpp"), ("Claude Code", "llama.cpp"), ("Claude Code", "llama.cpp"),
            ("Claude Code", "llama.cpp"), ("Claude Code", "other"), ("Codex", "cat"),
        ]

    def test_aop_run_copied_under_two_roots_counts_once(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        archives = [tmp_path / root / "aop" / "results" for root in ("code-memory", "claude-memory")]
        copies = [archive / "run-1.json" for archive in archives]
        for path in copies:
            _write_aop_result(path, state_root=tmp_path / "project" / ".aop")
        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCES", {"aop": (main_mod.AOP_RESULT_TOOL, main_mod._aop_sql)})
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        def load(listed, refresh=False):
            monkeypatch.setattr(main_mod, "SOURCE_TREES", [
                (main_mod.AOP_RESULT_TOOL, path.parent, "") for path in listed])
            db = duckdb.connect()
            register_macros(db)
            main_mod.load_tokens(db, refresh=refresh)
            return db.sql("SELECT count(*), sum(total_tokens) FROM tokens").fetchone()

        # The first copy is read, then discovery lists only the second, then both.
        assert load(copies[:1]) == (1, 110)
        assert load(copies[1:]) == (1, 110)
        assert load(copies[1:], refresh=True) == (1, 110)
        assert load(copies) == (1, 110)
        assert load(copies, refresh=True) == (1, 110)

    def test_aop_results_are_read_in_batches(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        project = tmp_path / "project"
        archive = tmp_path / "results"
        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [(main_mod.AOP_RESULT_TOOL, archive, "")])
        monkeypatch.setattr(main_mod, "SOURCES", {"aop": (main_mod.AOP_RESULT_TOOL, main_mod._aop_sql)})
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])
        monkeypatch.setattr(main_mod, "AOP_BATCH", 2)

        def load(runs, refresh=False):
            for run in range(runs):
                _write_aop_result(archive / f"run-{run}.json", run_id=f"run-{run}",
                                  session_id=f"session-{run}", state_root=project / ".aop")
            db = duckdb.connect()
            register_macros(db)
            main_mod.load_tokens(db, refresh=refresh)
            return db.sql("SELECT count(*), sum(total_tokens) FROM tokens").fetchone()

        assert load(5) == (5, 550)
        # Five more arrive in one update, then everything is rebuilt.
        assert load(10) == (10, 1100)
        assert load(10, refresh=True) == (10, 1100)
        assert [len(batch.get(main_mod.AOP_RESULT_TOOL, [])) for batch in main_mod._source_batches(
            main_mod.scan_source_files()[0])] == [0, 2, 2, 2, 2, 2]

    def test_codex_logs_are_read_in_batches_by_size(self):
        self.monkeypatch.setattr(main_mod, "CODEX_BATCH_BYTES", 100)
        files = {f"/logs/{name}.jsonl": main_mod.SourceSnapshot("Codex", 0, size)
                 for name, size in (("a", 60), ("b", 30), ("c", 30), ("d", 500), ("e", 10))}
        files["/logs/claude.jsonl"] = main_mod.SourceSnapshot("Claude Code", 0, 500)

        batches = main_mod._source_batches(files)

        # A log larger than the limit gets a batch of its own.
        assert [batch["Codex"] for batch in batches[1:]] == [
            ["/logs/a.jsonl", "/logs/b.jsonl"], ["/logs/c.jsonl"], ["/logs/d.jsonl"],
            ["/logs/e.jsonl"]]
        assert batches[0]["Claude Code"] == ["/logs/claude.jsonl"]
        assert "Codex" not in batches[0]

    def test_aop_cache_appends_new_resume_result(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        project = tmp_path / "project"
        archive = tmp_path / "results"
        first = archive / "run-1.json"
        _write_aop_result(first, run_id="run-1", session_id="resumed", state_root=project / ".aop")

        cache_dir = tmp_path / "cache"
        monkeypatch.setattr(main_mod, "CACHE_DIR", cache_dir)
        monkeypatch.setattr(main_mod, "CACHE_FILE", cache_dir / "tokens.parquet")
        monkeypatch.setattr(main_mod, "CACHE_META_FILE", cache_dir / "tokens-meta.json")
        monkeypatch.setattr(main_mod, "SOURCE_TREES", [
            (main_mod.AOP_RESULT_TOOL, archive, ""),
        ])
        monkeypatch.setattr(main_mod, "SOURCES", {
            "aop": (main_mod.AOP_RESULT_TOOL, main_mod._aop_sql),
        })
        monkeypatch.setattr(main_mod, "_WSL_HOMES", [])

        db = duckdb.connect()
        register_macros(db)
        main_mod.load_tokens(db, refresh=False)
        assert db.sql("SELECT count(*), sum(total_tokens) FROM tokens").fetchone() == (1, 110)

        second = archive / "run-2.json"
        _write_aop_result(
            second,
            state_root=project / ".aop",
            run_id="run-2",
            session_id="resumed",
            input_tokens=200,
            cached_input_tokens=50,
            output_tokens=20,
            reasoning_output_tokens=0,
        )

        db2 = duckdb.connect()
        register_macros(db2)
        timer = main_mod.DebugTimer(True)
        main_mod.load_tokens(db2, refresh=False, timing=timer)

        assert db2.sql("SELECT count(*), sum(total_tokens) FROM tokens").fetchone() == (2, 330)
        assert db2.sql("SELECT count(DISTINCT session) FROM tokens").fetchone()[0] == 1

        # A result rewritten in an older format keeps its cached usage until AOP migrates it.
        value = json.loads(second.read_text())
        value["usage_schema"] = "aop-token-usage-v2"
        value["usage"]["output_tokens"] = 999
        second.write_text(json.dumps(value))
        for refresh in (False, True):
            db3 = duckdb.connect()
            register_macros(db3)
            main_mod.load_tokens(db3, refresh=refresh)
            assert db3.sql("SELECT count(*), sum(total_tokens) FROM tokens").fetchone() == (2, 330)
        assert any(sample.label == "append changed files" for sample in timer.samples)
        metadata = json.loads((cache_dir / "tokens-meta.json").read_text())
        assert set(metadata["files"]) == {first.as_posix(), second.as_posix()}

    def test_debug_timing_flag_prints_summary(self):
        capsys = self.capsys
        def fake_run(args, timer):
            timer.note("cache hit")
            timer.record("load tokens", 0.01, "cached")
            return 0

        with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
            result = main_mod.main(["sql", "select 1", "--debug-timing"])

        captured = capsys.readouterr()
        assert result == 0
        assert "[debug] timing summary" in captured.err
        assert "cache hit" in captured.err
        assert "load tokens" in captured.err

    def test_profile_flag_prints_summary(self):
        capsys = self.capsys
        def fake_run(args, timer):
            return 0

        with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
            result = main_mod.main(["sql", "select 1", "--profile"])

        captured = capsys.readouterr()
        assert result == 0
        assert "[profile] top functions by cumulative time" in captured.err
        assert "ncalls" in captured.err

    def test_pace_command_accepts_positional_periods(self):
        captured = {}

        def fake_run(args, timer):
            captured["args"] = args
            return 0

        with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
            result = main_mod.main(["pace", "5m", "10m", "15m"])

        assert result == 0
        assert captured["args"].command == "pace"
        assert captured["args"].periods == [300, 600, 900]

    def test_commands_take_only_their_own_options(self):
        capsys = self.capsys
        for argv in (["table", "--vs", "3d"], ["pace", "--since", "7d"], ["record-quota", "--json"],
                     ["chart", "--by", "date"]):
            with self.subTest(argv=argv):
                with raises(SystemExit):
                    main_mod.main(argv)
        capsys.readouterr()

    def test_chart_commands_take_an_output_path(self):
        capsys = self.capsys
        captured = {}

        def fake_run(args, timer):
            captured["args"] = args
            return 0

        for argv in (["chart", "-o", "~/month.png"], ["share", "--output", "~/month.png"],
                     ["regime", "-o", "~/month.png"]):
            with self.subTest(argv=argv):
                with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
                    assert main_mod.main(argv) == 0
                assert captured["args"].output == Path.home() / "month.png"
        with raises(SystemExit):
            main_mod.main(["table", "-o", "x.png"])
        capsys.readouterr()

    def test_bare_invocation_runs_the_table(self):
        captured = {}

        def fake_run(args, timer):
            captured["args"] = args
            return 0

        for argv, by in (([], "project"), (["--since", "30d", "--by", "date"], "date")):
            with self.subTest(argv=argv):
                with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
                    assert main_mod.main(argv) == 0
                assert (captured["args"].command, captured["args"].by) == ("table", by)

    def test_option_style_invocations_still_work_with_a_warning(self):
        capsys = self.capsys
        captured = {}

        def fake_run(args, timer):
            captured["args"] = args
            return 0

        for argv, command, check in (
            (["--record-quota"], "record-quota", {}),
            (["--sql", "select 1"], "sql", {"query": "select 1"}),
            (["--pace", "--lookback", "3h", "--quota-json"], "pace", {"periods": [10800], "json": True}),
            (["--table", "--by", "date"], "table", {"by": "date"}),
        ):
            with self.subTest(argv=argv):
                with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
                    assert main_mod.main(argv) == 0
                assert captured["args"].command == command
                for name, value in check.items():
                    assert getattr(captured["args"], name) == value
                assert "deprecated" in capsys.readouterr().err

    def test_pace_command_accepts_model_mix(self):
        captured = {}

        def fake_run(args, timer):
            captured["args"] = args
            return 0

        with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
            result = main_mod.main(["pace", "--model-mix"])

        assert result == 0
        assert captured["args"].command == "pace"
        assert captured["args"].model_mix is True


# ===========================================================================
# CLI integration tests
# ===========================================================================

class TestCLI(Case):
    def test_console_entrypoint(self):
        executable = Path(sys.executable).with_name("clanker-analytics")
        if sys.platform == "win32":
            executable = executable.with_suffix(".exe")
        result = subprocess.run(
            [executable, "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "0." in result.stdout

    def test_package_module_entrypoint(self):
        result = subprocess.run(
            [sys.executable, "-m", "clanker_analytics", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "0." in result.stdout

    def test_version_flag(self):
        result = subprocess.run(
            [sys.executable, "-m", "clanker_analytics.main", "--version"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "0." in result.stdout  # version number present


class TestPlanSettings(Case):
    def test_plan_rejects_invalid_values(self):
        for value in ["zcode", "nope=18", "agy=18", "zcode=0", "codex=pro_999"]:
            with self.subTest(value=value):
                with raises(SystemExit):
                    main_mod.main(["value", "--plan", value])

    def test_plan_accepts_plan_ids_and_prices(self):
        captured = {}

        def fake_run(args, timer):
            captured["args"] = args
            return 0

        with mock.patch("clanker_analytics.main._run", side_effect=fake_run):
            assert main_mod.main(["value", "--plan", "Claude=100", "--plan", "codex=pro_500"]) == 0
        assert captured["args"].plan == [
            ("Claude Code", ("custom", 100.0)), ("Codex", ("pro_500", 500)),
        ]

    def test_config_file_names_plans(self):
        tmp_path = self.tmp_path
        from clanker_analytics.pricing import configured_plans
        config = tmp_path / "config.toml"
        config.write_text('[plans]\ncodex = "pro_100"\ngemini = 199.99\n')

        assert configured_plans(config) == {
            "Codex": ("pro_100", 100), "Gemini": ("custom", 199.99),
        }
        config.write_text('[plans]\ngemini = "ultra"\n')
        with raises(ValueError, match="ultra_100, ultra_200"):
            configured_plans(config)

    def test_command_line_overrides_config_and_detection(self):
        tmp_path = self.tmp_path
        monkeypatch = self.monkeypatch
        from clanker_analytics.pricing import configured_plans
        config = tmp_path / "config.toml"
        config.write_text('[plans]\ncodex = "pro_100"\nclaude = "max_5x"\n')
        monkeypatch.setattr(main_mod, "configured_plans", lambda: configured_plans(config))
        monkeypatch.setattr(main_mod, "detect_plans",
                            lambda: {"Codex": ("pro", 200), "Claude Code": ("max_20x", 200)})
        args = main_mod.argparse.Namespace(plan=[("Codex", ("pro_500", 500))])

        assert main_mod.plans_for(args) == {
            "Codex": ("pro_500", 500), "Claude Code": ("max_5x", 100),
        }
