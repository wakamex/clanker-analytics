"""Record provider quota snapshots and compare them with local token usage."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
from typing import Any, Iterable

from clanker_analytics.pricing import normalize_plan, plan_price


DEFAULT_LOOKBACKS = (3600, 6 * 3600, 24 * 3600)
STATE_DIR = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
DEFAULT_DB_PATH = STATE_DIR / "clanker-analytics" / "quota.sqlite3"

PROVIDER_TO_TOOL = {
    "anthropic": "Claude Code",
    "openai": "Codex",
    "antigravity": "Agy",
    "zai": "Zcode",
}

PROVIDER_SHORT_NAMES = {
    "anthropic": "claude",
    "openai": "codex",
    "antigravity": "agy",
    "zai": "zcode",
}

# Snapshots are recorded every five minutes; a longer gap than this between two of an
# account's snapshots means it was logged out, or nothing was recording.
LOGGED_IN_GAP_SECONDS = 900


@dataclass(frozen=True)
class QuotaSample:
    provider: str
    account_key: str
    bucket_id: str
    bucket_label: str
    scope_type: str
    scope_id: str
    window_kind: str
    window_seconds: int | None
    effective_at: str
    collected_at: str
    used_pct: float
    resets_at: str | None
    source: str
    source_sample_id: str
    status: str = "ok"
    plan: str | None = None


def parse_duration(value: str) -> int:
    """Parse a compact positive duration such as 3h or 7d."""
    match = re.fullmatch(r"([1-9][0-9]*)([mhdw])", value)
    if not match:
        raise ValueError("duration must be a positive integer followed by m, h, d, or w")
    amount = int(match.group(1))
    multiplier = {"m": 60, "h": 3600, "d": 86400, "w": 604800}[match.group(2)]
    return amount * multiplier


def format_duration(seconds: int) -> str:
    for suffix, unit in (("w", 604800), ("d", 86400), ("h", 3600), ("m", 60)):
        if seconds % unit == 0:
            return f"{seconds // unit}{suffix}"
    return f"{seconds}s"


def _parse_time(value: str | int | float | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return datetime.fromtimestamp(int(text), timezone.utc)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _time_text(value: str | int | float | datetime | None, fallback: datetime | None = None) -> str:
    parsed = value if isinstance(value, datetime) else _parse_time(value)
    parsed = parsed or fallback or datetime.now(timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _rounded_time(value: str | int | float | None, seconds: int) -> str | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    timestamp = round(parsed.timestamp() / seconds) * seconds
    return _time_text(datetime.fromtimestamp(timestamp, timezone.utc))


def _sample_id(provider: str, account_key: str, bucket_id: str, scope_id: str,
               effective_at: str, used_pct: float, resets_at: str | None) -> str:
    raw = json.dumps(
        [provider, account_key, bucket_id, scope_id, effective_at, used_pct, resets_at],
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _make_sample(
    *,
    provider: str,
    account_key: str,
    bucket_id: str,
    bucket_label: str,
    scope_type: str,
    scope_id: str,
    window_kind: str,
    window_seconds: int | None,
    effective_at: str,
    collected_at: str,
    used_pct: float,
    resets_at: str | int | float | None,
    source: str,
    plan: str | None = None,
) -> QuotaSample:
    reset_text = _time_text(resets_at) if _parse_time(resets_at) else None
    return QuotaSample(
        provider=provider,
        account_key=account_key,
        bucket_id=bucket_id,
        bucket_label=bucket_label,
        scope_type=scope_type,
        scope_id=scope_id,
        window_kind=window_kind,
        window_seconds=window_seconds,
        effective_at=effective_at,
        collected_at=collected_at,
        used_pct=max(0.0, min(100.0, float(used_pct))),
        resets_at=reset_text,
        source=source,
        source_sample_id=_sample_id(
            provider, account_key, bucket_id, scope_id, effective_at, float(used_pct), reset_text
        ),
        plan=plan,
    )


def _claude_window(bucket_id: str) -> tuple[str, int | None, str]:
    """The length and model scope of a window ccusage before 0.4.0 named session, 7d or
    7d_<model>."""
    if bucket_id == "session":
        return bucket_id, 5 * 3600, ""
    if bucket_id == "7d":
        return bucket_id, 7 * 86400, ""
    if bucket_id.startswith("7d_"):
        return bucket_id, 7 * 86400, bucket_id[3:].replace("_", " ").title()
    return bucket_id, None, ""


USAGE_SCHEMA = "usage-v1"
WINDOW_NAMES = {5 * 3600: "5-hour", 7 * 86400: "weekly"}


def window_bucket(scope: str | None, seconds: int | None, name: str) -> tuple[str, str]:
    """The bucket id and label of a quota window: its length, such as 5h or 1w, or its
    name when it has no fixed length, prefixed by the models it covers when it covers
    only some."""
    length = format_duration(seconds) if seconds else name
    name = WINDOW_NAMES.get(seconds, name) if seconds else name
    return (f"{scope}:{length}", f"{scope} {name}") if scope else (length, name)


def normalize_usage_v1(provider: str, data: dict[str, Any],
                       collected_at: datetime) -> list[QuotaSample]:
    """Samples from a usage-v1 document, the format every clanker-usage provider prints.

    A window is named by its length, such as 5h or 1w, prefixed by the models it covers
    when it covers only some, and by its name when it has no fixed length.
    """
    effective_at = _time_text(data.get("retrieved_at"), collected_at)
    plan = data.get("plan")
    samples = []
    for window in data.get("windows") or []:
        seconds = window.get("seconds")
        scope = window.get("scope")
        bucket_id, bucket_label = window_bucket(scope, seconds, str(window["name"]))
        samples.append(_make_sample(
            provider=provider,
            account_key=str(data.get("account") or "default"),
            bucket_id=bucket_id,
            bucket_label=bucket_label,
            scope_type="model" if scope else "account",
            scope_id=scope or "",
            window_kind="fixed" if seconds else "unknown",
            window_seconds=seconds,
            effective_at=effective_at,
            collected_at=_time_text(collected_at),
            used_pct=window["used_pct"],
            # Claude reports one reset a few milliseconds apart from reading to reading.
            resets_at=_rounded_time(window.get("resets_at"), 1),
            source=str(data.get("status") or "unknown"),
            plan=normalize_plan(plan) if plan else None,
        ))
    return samples


# The clanker-usage provider that reports each recorded provider's quota.
QUOTA_PROVIDERS = {
    "anthropic": "claude",
    "openai": "codex",
    "antigravity": "agy",
    "zai": "zcode",
}


def claude_login() -> tuple[str | None, str | None]:
    """The organization and email of the Claude login in use, from Claude Code's
    .claude.json.

    The organization keys Claude snapshots recorded before clanker-usage named the
    account, and the email labels the account. Claude Code keeps .claude.json in CLAUDE_CONFIG_DIR when that is set, else in home.
    """
    override = os.environ.get("CLAUDE_CONFIG_DIR")
    path = Path(override) / ".claude.json" if override else Path.home() / ".claude.json"
    try:
        account = json.loads(path.read_text()).get("oauthAccount") or {}
    except (OSError, ValueError):
        return None, None
    return account.get("organizationUuid") or None, account.get("emailAddress") or None


def usage_document(name: str, timeout: int = 30) -> dict[str, Any]:
    """The usage-v1 document clanker-usage prints for a provider, such as claude. It is
    printed even when the provider's usage is unavailable, which exits with status 1."""
    result = subprocess.run([sys.executable, "-m", "clanker_usage", name, "json"],
                            capture_output=True, text=True, timeout=timeout)
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        data = None
    if not isinstance(data, dict) or data.get("schema") != USAGE_SCHEMA:
        detail = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "no usage-v1 document"
        raise RuntimeError(f"{name}: {detail}")
    return data


def collect_provider(provider: str, timeout: int = 30) -> list[QuotaSample]:
    collected_at = datetime.now(timezone.utc)
    data = usage_document(QUOTA_PROVIDERS[provider], timeout)
    # A stale document still carries the last windows the provider returned.
    if data.get("status") == "unavailable":
        raise RuntimeError(f"{provider}: {data.get('error') or 'usage unavailable'}")
    return normalize_usage_v1(provider, data, collected_at)


ANTIGRAVITY_RENAMES = (
    ("gemini-5h", "Gemini", "5h", "5-hour"),
    ("gemini-weekly", "Gemini", "1w", "weekly"),
    ("3p-5h", "Claude and GPT", "5h", "5-hour"),
    ("3p-weekly", "Claude and GPT", "1w", "weekly"),
)


def connect(path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("""
        CREATE TABLE IF NOT EXISTS quota_samples (
            id INTEGER PRIMARY KEY,
            provider TEXT NOT NULL,
            account_key TEXT NOT NULL,
            bucket_id TEXT NOT NULL,
            bucket_label TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            window_kind TEXT NOT NULL,
            window_seconds INTEGER,
            effective_at TEXT NOT NULL,
            collected_at TEXT NOT NULL,
            used_pct REAL NOT NULL,
            resets_at TEXT,
            source TEXT NOT NULL,
            source_sample_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            plan TEXT
        )
    """)
    if "plan" not in {row[1] for row in connection.execute("PRAGMA table_info(quota_samples)")}:
        connection.execute("ALTER TABLE quota_samples ADD COLUMN plan TEXT")
    connection.execute("""
        CREATE INDEX IF NOT EXISTS quota_samples_lookup
        ON quota_samples(provider, account_key, bucket_id, scope_type, scope_id, effective_at)
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS quota_accounts (
            provider TEXT NOT NULL,
            account_key TEXT NOT NULL,
            label TEXT NOT NULL,
            PRIMARY KEY (provider, account_key)
        )
    """)
    if connection.execute("PRAGMA user_version").fetchone()[0] < 2:
        # Antigravity's usage-v1 windows are named by scope and length; earlier
        # snapshots take the same names so each window's history stays one series.
        for old, scope, length, name in ANTIGRAVITY_RENAMES:
            connection.execute("""
                UPDATE quota_samples
                SET bucket_id = ?, bucket_label = ?, scope_type = 'model', scope_id = ?
                WHERE provider = 'antigravity' AND bucket_id = ?
            """, (f"{scope}:{length}", f"{scope} {name}", scope, old))
    if connection.execute("PRAGMA user_version").fetchone()[0] < 3:
        # Codex's usage-v1 windows are named by length and scope, where the stored ids
        # named their slot, such as primary, whose length depends on the plan.
        for old, scope, seconds in connection.execute("""
            SELECT DISTINCT bucket_id, scope_id, window_seconds FROM quota_samples
            WHERE provider = 'openai' AND window_seconds IS NOT NULL
        """).fetchall():
            bucket_id, label = window_bucket(scope or None, seconds, old)
            connection.execute("""
                UPDATE quota_samples SET bucket_id = ?, bucket_label = ?
                WHERE provider = 'openai' AND bucket_id = ? AND scope_id = ?
                  AND window_seconds = ?
            """, (bucket_id, label, old, scope, seconds))
    if connection.execute("PRAGMA user_version").fetchone()[0] < 4:
        # Claude's usage-v1 windows are 5h, 1w and <model>:1w, where earlier snapshots
        # were session, 7d and 7d_<model>.
        for (old,) in connection.execute("""
            SELECT DISTINCT bucket_id FROM quota_samples WHERE provider = 'anthropic'
        """).fetchall():
            _, seconds, scope = _claude_window(old)
            if seconds:
                bucket_id, label = window_bucket(scope or None, seconds, old)
                connection.execute("""
                    UPDATE quota_samples
                    SET bucket_id = ?, bucket_label = ?, scope_type = ?, scope_id = ?
                    WHERE provider = 'anthropic' AND bucket_id = ?
                """, (bucket_id, label, "model" if scope else "account", scope, old))
    connection.execute("PRAGMA user_version = 4")
    return connection


def insert_samples(connection: sqlite3.Connection, samples: Iterable[QuotaSample]) -> int:
    before = connection.total_changes
    connection.executemany("""
        INSERT OR IGNORE INTO quota_samples (
            provider, account_key, bucket_id, bucket_label, scope_type, scope_id,
            window_kind, window_seconds, effective_at, collected_at, used_pct,
            resets_at, source, source_sample_id, status, plan
        ) VALUES (
            :provider, :account_key, :bucket_id, :bucket_label, :scope_type, :scope_id,
            :window_kind, :window_seconds, :effective_at, :collected_at, :used_pct,
            :resets_at, :source, :source_sample_id, :status, :plan
        )
    """, [asdict(sample) for sample in samples])
    connection.commit()
    return connection.total_changes - before


def record_quota(path: Path = DEFAULT_DB_PATH, providers: Iterable[str] | None = None) -> tuple[int, list[str]]:
    selected = list(providers or QUOTA_PROVIDERS)
    samples = []
    errors = []
    for provider in selected:
        try:
            samples.extend(collect_provider(provider))
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as error:
            errors.append(str(error))
    with connect(path) as connection:
        inserted = insert_samples(connection, samples)
        adopt_unnamed_history(connection, samples)
        organization, email = claude_login()
        accounts = {sample.account_key for sample in samples if sample.provider == "anthropic"}
        if "anthropic" in selected and len(accounts) == 1:
            account = accounts.pop()
            # Snapshots before ccusage 0.4.0 were stored under the login's organization;
            # it now names the account itself, so the same login's history moves to it.
            if organization and organization != account:
                connection.execute("""
                    UPDATE quota_samples SET account_key = ?
                    WHERE provider = 'anthropic' AND account_key = ?
                """, (account, organization))
                connection.execute("""
                    DELETE FROM quota_accounts WHERE provider = 'anthropic' AND account_key = ?
                """, (organization,))
            if email:
                connection.execute("INSERT OR REPLACE INTO quota_accounts VALUES ('anthropic', ?, ?)",
                                   (account, email))
    return inserted, errors


def adopt_unnamed_history(connection: sqlite3.Connection, samples: Iterable[QuotaSample]) -> int:
    """Give a provider's snapshots recorded without an account to the account it now names.

    Usage packages that did not report an account were stored as "default". When a
    provider names one account and has never recorded another, its earlier snapshots are
    that account's, so its windows continue as one series. Returns the snapshots relabeled.
    """
    named: dict[str, set[str]] = {}
    for sample in samples:
        named.setdefault(sample.provider, set()).add(sample.account_key)
    changed = 0
    for provider, accounts in named.items():
        if len(accounts) != 1 or "default" in accounts:
            continue
        stored = {row[0] for row in connection.execute(
            "SELECT DISTINCT account_key FROM quota_samples WHERE provider = ?", (provider,))}
        if stored == {"default", *accounts}:
            changed += connection.execute("""
                UPDATE quota_samples SET account_key = ?
                WHERE provider = ? AND account_key = 'default'
            """, (next(iter(accounts)), provider)).rowcount
    connection.commit()
    return changed


def _sample_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def latest_samples(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute("""
        SELECT * FROM quota_samples q
        WHERE id = (
            SELECT id FROM quota_samples candidate
            WHERE candidate.provider = q.provider
              AND candidate.account_key = q.account_key
              AND candidate.bucket_id = q.bucket_id
              AND candidate.scope_type = q.scope_type
              AND candidate.scope_id = q.scope_id
            ORDER BY candidate.effective_at DESC, candidate.id DESC
            LIMIT 1
        )
        ORDER BY provider, bucket_id, scope_id
    """).fetchall()
    return [_sample_dict(row) for row in rows]


def historical_sample(connection: sqlite3.Connection, current: dict[str, Any],
                      target: datetime) -> dict[str, Any] | None:
    segment_reset = (
        current["resets_at"]
        if current["window_kind"] == "fixed" or current["provider"] == "openai"
        else None
    )
    row = connection.execute("""
        SELECT * FROM quota_samples
        WHERE provider = ? AND account_key = ? AND bucket_id = ?
          AND scope_type = ? AND scope_id = ? AND effective_at <= ?
          AND (
            ? IS NULL
            OR abs((julianday(resets_at) - julianday(?)) * 86400.0) <= 60.0
          )
          AND id != ?
        ORDER BY effective_at DESC, id DESC
        LIMIT 1
    """, (
        current["provider"], current["account_key"], current["bucket_id"],
        current["scope_type"], current["scope_id"], _time_text(target),
        segment_reset, segment_reset, current["id"],
    )).fetchone()
    return _sample_dict(row) if row else None


def oldest_segment_sample(connection: sqlite3.Connection,
                          current: dict[str, Any]) -> dict[str, Any] | None:
    segment_reset = (
        current["resets_at"]
        if current["window_kind"] == "fixed" or current["provider"] == "openai"
        else None
    )
    row = connection.execute("""
        SELECT * FROM quota_samples
        WHERE provider = ? AND account_key = ? AND bucket_id = ?
          AND scope_type = ? AND scope_id = ?
          AND (
            ? IS NULL
            OR abs((julianday(resets_at) - julianday(?)) * 86400.0) <= 60.0
          )
          AND id != ?
        ORDER BY effective_at ASC, id ASC
        LIMIT 1
    """, (
        current["provider"], current["account_key"], current["bucket_id"],
        current["scope_type"], current["scope_id"], segment_reset, segment_reset,
        current["id"],
    )).fetchone()
    return _sample_dict(row) if row else None


def account_labels(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
    """A short name for each account whose login was recorded: its email's local part."""
    return {(provider, account): label.split("@")[0] for provider, account, label
            in connection.execute("SELECT provider, account_key, label FROM quota_accounts")}


def window_pace(connection: sqlite3.Connection, current: dict[str, Any]) -> dict[str, Any] | None:
    """How fast an account used its current fixed quota window while it was logged in.

    Logged-in time is the stretches the account's own snapshots cover, so time spent on
    another login of the same provider does not dilute the rate. The rate is compared
    with using the whole window evenly.
    """
    if current["window_kind"] != "fixed" or not current["window_seconds"] or not current["resets_at"]:
        return None
    samples = connection.execute("""
        SELECT effective_at, used_pct FROM quota_samples
        WHERE provider = ? AND account_key = ? AND bucket_id = ?
          AND scope_type = ? AND scope_id = ?
          AND abs((julianday(resets_at) - julianday(?)) * 86400.0) <= 60.0
        ORDER BY effective_at, id
    """, (current["provider"], current["account_key"], current["bucket_id"],
          current["scope_type"], current["scope_id"], current["resets_at"])).fetchall()
    logged_in_seconds = 0.0
    gained = 0.0
    for (earlier_at, earlier_pct), (later_at, later_pct) in zip(samples, samples[1:]):
        gap = (_parse_time(later_at) - _parse_time(earlier_at)).total_seconds()
        if gap <= LOGGED_IN_GAP_SECONDS:
            logged_in_seconds += gap
            gained += later_pct - earlier_pct
    if not logged_in_seconds:
        return None
    rate = gained / (logged_in_seconds / 3600)
    even_rate = 100.0 / (current["window_seconds"] / 3600)
    return {
        "logged_in_hours": logged_in_seconds / 3600,
        "quota_gained_pct": gained,
        "quota_pp_per_hour": rate,
        "even_pp_per_hour": even_rate,
        "pace_ratio": rate / even_rate,
    }


def _model_patterns(bucket: dict[str, Any]) -> list[str]:
    if bucket["scope_type"] == "account" or not bucket["scope_id"]:
        return []
    scope = bucket["scope_id"].lower().removesuffix(" models")
    if scope == "gemini":
        return ["%gemini%"]
    if scope == "claude and gpt":
        return ["%claude%", "%gpt%"]
    normalized = re.sub(r"[^a-z0-9]+", "-", scope).strip("-")
    return [f"%{normalized}%"]


def _token_window(db: Any, tool: str, start: datetime, end: datetime,
                  cost_expression: str, model_patterns: list[str] | None = None) -> dict[str, Any]:
    patterns = model_patterns or []
    model_clause = ""
    parameters: list[Any] = [tool, start, end]
    if patterns:
        model_clause = " AND (" + " OR ".join("lower(model) LIKE ?" for _ in patterns) + ")"
        parameters.extend(patterns)
    rows = db.execute(f"""
        SELECT
            coalesce(nullif(model, ''), 'unknown') as model,
            coalesce(nullif(rate_mode, ''), 'default') as rate_mode,
            sum(total_tokens)::BIGINT as tokens,
            sum({cost_expression})::DOUBLE as api_cost_usd
        FROM tokens
        WHERE tool = ?
          AND try_cast(timestamp AS TIMESTAMPTZ) > ?
          AND try_cast(timestamp AS TIMESTAMPTZ) <= ?
          {model_clause}
        GROUP BY model, rate_mode
        ORDER BY api_cost_usd DESC NULLS LAST, tokens DESC
    """, parameters).fetchall()
    columns = [item[0] for item in db.description]
    mixes = [dict(zip(columns, row)) for row in rows]
    return {
        "total_tokens": sum(item["tokens"] or 0 for item in mixes),
        "api_cost_usd": sum(item["api_cost_usd"] or 0.0 for item in mixes),
        "model_mix": mixes,
        "pricing_status": (
            "standard" if all(item["rate_mode"] == "default" for item in mixes)
            else "unsupported_rate_mode"
        ),
    }


def build_report(connection: sqlite3.Connection, token_db: Any, lookbacks: Iterable[int],
                 cost_expression: str, include_inactive: bool = False,
                 pricing_basis: str = "") -> dict[str, Any]:
    assign_claude_accounts(connection)
    requested = sorted(set(lookbacks))
    buckets = []
    logged_out_buckets = []
    hidden_bucket_count = 0
    collecting_bucket_count = 0
    current_samples = latest_samples(connection)
    labels = account_labels(connection)
    # Only the login in use is paced: another account of the same provider with newer
    # snapshots means this one was switched away from, and local usage is no longer its own.
    newest = {}
    for sample in current_samples:
        newest[sample["provider"]] = max(newest.get(sample["provider"], ""), sample["effective_at"])
    for current in current_samples:
        now = _parse_time(current["effective_at"])
        if now is None:
            continue
        if current["effective_at"] < newest[current["provider"]] and any(
                other["provider"] == current["provider"]
                and other["account_key"] != current["account_key"]
                and other["effective_at"] > current["effective_at"]
                for other in current_samples):
            hidden_bucket_count += 1
            reset = _parse_time(current["resets_at"])
            pace = window_pace(connection, current)
            # Its window is still running, so the quota it has left is still usable.
            if (pace and reset and reset > _parse_time(newest[current["provider"]])
                    and (include_inactive or pace["quota_gained_pct"] > 0)):
                logged_out_buckets.append({
                    **{key: current[key] for key in (
                        "provider", "account_key", "bucket_id", "bucket_label", "scope_type",
                        "scope_id", "window_kind", "window_seconds", "used_pct", "resets_at")},
                    "account_label": labels.get((current["provider"], current["account_key"])),
                    "logged_out_since": current["effective_at"],
                    "window_pace": pace,
                })
            continue
        tool = PROVIDER_TO_TOOL.get(current["provider"])
        model_patterns = _model_patterns(current)
        reset = _parse_time(current["resets_at"])
        remaining_hours = (
            (reset - now).total_seconds() / 3600
            if reset and reset > now else None
        )
        forecast_basis = None
        if remaining_hours is not None:
            if current["window_kind"] == "fixed":
                forecast_basis = "fixed_window"
            elif current["provider"] == "openai":
                forecast_basis = "reported_openai_reset"
        target_rate = (
            (100.0 - current["used_pct"]) / remaining_hours
            if forecast_basis and remaining_hours and current["used_pct"] < 100
            else None
        )
        window_start = (
            reset - timedelta(seconds=current["window_seconds"])
            if reset and current["window_seconds"]
            else None
        )
        period_tokens = (
            _token_window(token_db, tool, window_start, now, cost_expression, model_patterns)
            if tool and window_start and window_start < now
            else None
        )
        api_usd_per_pp = None
        if (period_tokens and period_tokens["pricing_status"] == "standard"
                and current["used_pct"] > 0 and period_tokens["api_cost_usd"] > 0):
            api_usd_per_pp = period_tokens["api_cost_usd"] / current["used_pct"]

        intervals = []
        oldest = oldest_segment_sample(connection, current)
        oldest_time = _parse_time(oldest["effective_at"]) if oldest else None
        for seconds in requested:
            past = historical_sample(connection, current, now - timedelta(seconds=seconds))
            if not past:
                available_seconds = max(
                    0, int((now - oldest_time).total_seconds())
                ) if oldest_time else 0
                intervals.append({
                    "requested_seconds": seconds,
                    "available_seconds": available_seconds,
                    "lookback_fullness_pct": min(
                        100.0, available_seconds / seconds * 100
                    ),
                    "quality": "collecting_history",
                })
                continue
            past_time = _parse_time(past["effective_at"])
            if past_time is None or past_time >= now:
                continue
            elapsed_hours = (now - past_time).total_seconds() / 3600
            delta = current["used_pct"] - past["used_pct"]
            local = (
                _token_window(token_db, tool, past_time, now, cost_expression, model_patterns)
                if tool else None
            )
            actual_seconds = int((now - past_time).total_seconds())
            coverage = actual_seconds / seconds
            quality = "full" if 1.0 <= coverage <= 1.25 else "partial"
            token_pp_rate = None
            if (local and local["pricing_status"] == "standard"
                    and api_usd_per_pp and api_usd_per_pp > 0):
                token_pp_rate = local["api_cost_usd"] / elapsed_hours / api_usd_per_pp
            if delta > 0:
                quota_signal = "rising"
            elif delta < 0:
                quota_signal = "recovering_or_reset"
            elif local and local["total_tokens"] > 0:
                quota_signal = "flat_or_quantized"
            else:
                quota_signal = "flat"
            quota_rate = delta / elapsed_hours
            if abs(delta) > 1e-9:
                observed_rate = quota_rate
                observed_source = "quota_delta"
            elif token_pp_rate is not None:
                observed_rate = token_pp_rate
                observed_source = "token_derived"
            else:
                observed_rate = quota_rate
                observed_source = "quota_delta"
            pace_ratio = None
            projected_used_pct = None
            estimated_exhaustion_at = None
            hours_before_reset = None
            required_reduction = None
            headroom = None
            guidance = None
            if target_rate is not None and observed_rate >= 0:
                pace_ratio = observed_rate / target_rate if target_rate > 0 else None
                projected_used_pct = current["used_pct"] + observed_rate * remaining_hours
                if observed_rate > 0:
                    hours_to_exhaustion = (100.0 - current["used_pct"]) / observed_rate
                    if hours_to_exhaustion <= remaining_hours:
                        exhaustion = now + timedelta(hours=hours_to_exhaustion)
                        estimated_exhaustion_at = _time_text(exhaustion)
                        hours_before_reset = remaining_hours - hours_to_exhaustion
                    if observed_rate > target_rate:
                        required_reduction = (1.0 - target_rate / observed_rate) * 100
                    elif observed_rate < target_rate:
                        headroom = (target_rate / observed_rate - 1.0) * 100
                if observed_rate == 0:
                    guidance = "idle"
                elif pace_ratio is not None and pace_ratio > 1.10:
                    guidance = f"slow {required_reduction:.0f}%"
                elif pace_ratio is not None and pace_ratio < 0.90:
                    guidance = f"headroom {headroom:.0f}%"
                else:
                    guidance = "on pace"
            intervals.append({
                "requested_seconds": seconds,
                "elapsed_seconds": actual_seconds,
                "lookback_fullness_pct": min(100.0, seconds / actual_seconds * 100),
                "from": past["effective_at"],
                "to": current["effective_at"],
                "quota_delta_pct": delta,
                "quota_delta_pp_per_hour": quota_rate,
                "token_derived_pp_per_hour": token_pp_rate,
                "pace_gap_pp_per_hour": (
                    token_pp_rate - delta / elapsed_hours if token_pp_rate is not None else None
                ),
                "quota_signal": quota_signal,
                "observed_pp_per_hour": observed_rate,
                "observed_pace_source": observed_source,
                "target_pp_per_hour": target_rate,
                "pace_ratio": pace_ratio,
                "uncapped_projected_used_pct": projected_used_pct,
                "estimated_exhaustion_at": estimated_exhaustion_at,
                "hours_before_reset": hours_before_reset,
                "exhausts_before_reset": hours_before_reset is not None,
                "required_net_reduction_pct": required_reduction,
                "headroom_pct": headroom,
                "guidance": guidance,
                "local_api_usd_per_hour": (
                    local["api_cost_usd"] / elapsed_hours if local else None
                ),
                "local_total_tokens_per_hour": (
                    local["total_tokens"] / elapsed_hours if local else None
                ),
                "model_mix": local["model_mix"] if local else [],
                "quality": quality,
            })

        bucket = {
            "provider": current["provider"],
            "account_key": current["account_key"],
            "account_label": labels.get((current["provider"], current["account_key"])),
            "bucket_id": current["bucket_id"],
            "bucket_label": current["bucket_label"],
            "scope_type": current["scope_type"],
            "scope_id": current["scope_id"],
            "window_kind": current["window_kind"],
            "window_seconds": current["window_seconds"],
            "window_pace": window_pace(connection, current),
            "effective_at": current["effective_at"],
            "used_pct": current["used_pct"],
            "resets_at": current["resets_at"],
            "remaining_hours": remaining_hours,
            "target_percentage_points_per_hour": target_rate,
            "forecast_basis": forecast_basis,
            "token_calibration": {
                "status": "experimental",
                "basis": "local_api_equivalent_usd",
                "pricing_basis": pricing_basis,
                "method": "local_window_cost_divided_by_current_provider_used_pct",
                "api_usd_per_quota_percentage_point": api_usd_per_pp,
                "window_start": _time_text(window_start) if window_start else None,
                "local_period_usage": period_tokens,
                "coverage": "local_only",
            },
            "lookbacks": intervals,
        }
        completed = [
            interval for interval in intervals
            if interval["quality"] == "full"
        ]
        active = (
            any(
                abs(interval["quota_delta_pct"]) > 1e-9
                or (interval["local_total_tokens_per_hour"] or 0) > 0
                for interval in completed
            )
            if completed else False
        )
        if include_inactive or active:
            buckets.append(bucket)
        else:
            hidden_bucket_count += 1
            if not completed:
                collecting_bucket_count += 1
    return {
        "schema_version": 1,
        "generated_at": _time_text(datetime.now(timezone.utc)),
        "bucket_filter": "all" if include_inactive else "active",
        "sample_bucket_count": len(current_samples),
        "hidden_bucket_count": hidden_bucket_count,
        "collecting_bucket_count": collecting_bucket_count,
        "buckets": buckets,
        "logged_out_buckets": logged_out_buckets,
    }


# Codex logs carry their own quota history, so recorder samples are needed only here.
HISTORY_RECORDER_PROVIDERS = ("anthropic", "antigravity", "zai")

QUOTA_OBSERVATION_COLUMNS = """
    tool VARCHAR,
    limit_id VARCHAR,
    window_minutes INTEGER,
    resets_at BIGINT,
    observed_at TIMESTAMPTZ,
    used_pct DOUBLE,
    model_patterns VARCHAR[],
    account VARCHAR,
    plan VARCHAR
"""


def _recorder_observations(connection: sqlite3.Connection) -> list[tuple]:
    placeholders = ", ".join("?" for _ in HISTORY_RECORDER_PROVIDERS)
    rows = connection.execute(f"""
        SELECT * FROM quota_samples
        WHERE provider IN ({placeholders})
          AND window_kind = 'fixed'
          AND used_pct > 0
          AND resets_at IS NOT NULL
          AND window_seconds IS NOT NULL
    """, HISTORY_RECORDER_PROVIDERS).fetchall()
    observations = []
    for row in rows:
        sample = _sample_dict(row)
        reset = _parse_time(sample["resets_at"])
        observed = _parse_time(sample["effective_at"])
        if reset is None or observed is None:
            continue
        limit_id = sample["bucket_id"]
        if sample["account_key"] != "default":
            limit_id = f"{sample['account_key']}:{limit_id}"
        observations.append((
            PROVIDER_TO_TOOL[sample["provider"]],
            limit_id,
            sample["window_seconds"] // 60,
            int(reset.timestamp()),
            observed,
            sample["used_pct"],
            _model_patterns(sample),
            sample["account_key"],
            sample["plan"],
        ))
    return observations


def _weekly_anchor(resets_at: str | None) -> int | None:
    """A weekly reset's position within the week, in five-minute steps, which stays the
    same for one account from week to week."""
    reset = _parse_time(resets_at)
    return None if reset is None else round(reset.timestamp() % 604800 / 300) % 2016


def assign_claude_accounts(connection: sqlite3.Connection) -> int:
    """Label older Claude snapshots recorded before snapshots named their login.

    A snapshot without an account gets the account of labeled snapshots whose weekly quota
    resets at the same time of week, since each account keeps its own reset time. Snapshots
    matching no labeled account stay "default". Returns the number of snapshots relabeled.
    """
    labeled: dict[int, str] = {}
    for account, resets_at in connection.execute("""
        SELECT account_key, resets_at FROM quota_samples
        WHERE provider = 'anthropic' AND bucket_id = '1w' AND account_key != 'default'
    """).fetchall():
        anchor = _weekly_anchor(resets_at)
        if anchor is not None:
            labeled[anchor] = account
    if not labeled:
        return 0
    changed = 0
    for effective_at, resets_at in connection.execute("""
        SELECT DISTINCT effective_at, resets_at FROM quota_samples
        WHERE provider = 'anthropic' AND bucket_id = '1w' AND account_key = 'default'
    """).fetchall():
        account = labeled.get(_weekly_anchor(resets_at))
        if account:
            changed += connection.execute("""
                UPDATE quota_samples SET account_key = ?
                WHERE provider = 'anthropic' AND account_key = 'default' AND effective_at = ?
            """, (account, effective_at)).rowcount
    connection.commit()
    return changed


def build_quota_history(token_db: Any, cost_expression: str, pricing_basis: str,
                        quota_connection: sqlite3.Connection | None = None) -> dict[str, Any]:
    """Audit every fixed quota period observed in local logs or recorded snapshots.

    Codex session logs and Zcode usage-limit errors embed quota observations in the token
    cache. Claude, Antigravity, and Zcode snapshots come from the quota recorder database.
    """
    token_db.execute(
        f"CREATE OR REPLACE TEMP TABLE quota_observations ({QUOTA_OBSERVATION_COLUMNS})"
    )
    token_db.execute("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS quota_plan VARCHAR")
    token_db.execute("""
        INSERT INTO quota_observations
        SELECT
            tool,
            coalesce(nullif(quota_limit_id, ''), lower(tool)),
            quota_window_minutes,
            quota_resets_at,
            try_cast(timestamp AS TIMESTAMPTZ),
            quota_used_pct,
            []::VARCHAR[],
            'default',
            nullif(quota_plan, '')
        FROM tokens
        WHERE source_kind = 'native'
          AND quota_used_pct IS NOT NULL
          AND quota_used_pct > 0
          AND quota_resets_at IS NOT NULL
          AND quota_window_minutes IS NOT NULL
    """)
    if quota_connection is not None:
        assign_claude_accounts(quota_connection)
        recorded = _recorder_observations(quota_connection)
        if recorded:
            token_db.executemany(
                "INSERT INTO quota_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", recorded
            )
    rows = token_db.execute(f"""
        WITH observations AS (
            SELECT
                tool as quota_tool,
                round(resets_at / 300.0)::BIGINT * 300 as canonical_resets_at,
                arg_max(resets_at, observed_at) as quota_resets_at,
                window_minutes as quota_window_minutes,
                limit_id as quota_limit_id,
                arg_max(used_pct, observed_at) as last_used_pct,
                max(used_pct) as max_used_pct,
                min(observed_at) as first_observed_at,
                max(observed_at) as observed_at,
                count(*)::BIGINT as observation_count,
                any_value(model_patterns) as model_patterns,
                account,
                -- The plan at the reading the period's used percentage comes from.
                arg_max(plan, observed_at) FILTER (WHERE plan IS NOT NULL) as plan
            FROM quota_observations
            WHERE observed_at IS NOT NULL
            GROUP BY quota_tool, canonical_resets_at, window_minutes, limit_id, account
        ), active AS (
            -- A period's usage starts no earlier than the last snapshot of another account
            -- of the same tool before it, which marks a switch between logins.
            SELECT o.*, (
                SELECT max(q.observed_at) FROM quota_observations q
                WHERE q.tool = o.quota_tool AND q.account != o.account
                  AND q.observed_at < o.first_observed_at
            ) as other_account_seen
            FROM observations o
        )
        SELECT
            o.quota_tool,
            o.quota_resets_at,
            o.quota_window_minutes,
            o.quota_limit_id,
            o.last_used_pct,
            o.max_used_pct,
            cast(o.first_observed_at AS VARCHAR) as first_observed_at,
            cast(o.observed_at AS VARCHAR) as observed_at,
            o.observation_count,
            o.account,
            o.plan,
            coalesce(nullif(t.model, ''), 'unknown') as model,
            coalesce(nullif(t.rate_mode, ''), 'default') as rate_mode,
            coalesce(t.source_kind, 'native') as source_kind,
            count(*) FILTER (WHERE t.pricing = 'unpriced' AND t.total_tokens > 0)
                as unpriced_rows,
            count(*) FILTER (WHERE t.pricing = 'aop') as aop_priced_rows,
            coalesce(sum(t.input_tokens), 0)::BIGINT as input_tokens,
            coalesce(sum(t.output_tokens), 0)::BIGINT as output_tokens,
            coalesce(sum(t.cache_write_tokens), 0)::BIGINT as cache_write_tokens,
            coalesce(sum(t.cache_read_tokens), 0)::BIGINT as cache_read_tokens,
            coalesce(sum(t.total_tokens), 0)::BIGINT as total_tokens,
            coalesce(sum({cost_expression}), 0)::DOUBLE as api_cost_usd
        FROM active o
        LEFT JOIN tokens t
          ON t.tool = o.quota_tool
         AND try_cast(t.timestamp AS TIMESTAMPTZ)
             >= to_timestamp(o.quota_resets_at)
                - o.quota_window_minutes * INTERVAL '1 minute'
         AND try_cast(t.timestamp AS TIMESTAMPTZ)
             > coalesce(o.other_account_seen, '-infinity'::TIMESTAMPTZ)
         AND try_cast(t.timestamp AS TIMESTAMPTZ) <= o.observed_at
         AND (len(o.model_patterns) = 0
              OR len(list_filter(o.model_patterns, pattern -> lower(t.model) LIKE pattern)) > 0)
        GROUP BY
            o.quota_tool, o.quota_resets_at, o.quota_window_minutes, o.quota_limit_id,
            o.last_used_pct, o.max_used_pct, o.first_observed_at,
            o.observed_at, o.observation_count, o.account, o.plan,
            coalesce(nullif(t.model, ''), 'unknown'),
            coalesce(nullif(t.rate_mode, ''), 'default'),
            coalesce(t.source_kind, 'native')
        ORDER BY o.quota_resets_at, o.quota_tool, api_cost_usd DESC, total_tokens DESC
    """).fetchall()
    columns = [item[0] for item in token_db.description]

    grouped: dict[tuple[str, int, int, str], dict[str, Any]] = {}
    for raw_row in rows:
        row = dict(zip(columns, raw_row))
        key = (
            row["quota_tool"],
            row["quota_resets_at"],
            row["quota_window_minutes"],
            row["quota_limit_id"],
        )
        period = grouped.setdefault(key, {
            "tool": row["quota_tool"],
            "window_start": _time_text(
                datetime.fromtimestamp(row["quota_resets_at"], timezone.utc)
                - timedelta(minutes=row["quota_window_minutes"])
            ),
            "resets_at": _time_text(row["quota_resets_at"]),
            "observed_at": _time_text(row["observed_at"]),
            "first_observed_at": _time_text(row["first_observed_at"]),
            "window_minutes": row["quota_window_minutes"],
            "limit_id": row["quota_limit_id"],
            "account": row["account"],
            "plan": row["plan"],
            "used_pct": float(row["last_used_pct"]),
            "max_used_pct": float(row["max_used_pct"]),
            "observation_count": row["observation_count"],
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_write_tokens": 0,
            "cache_read_tokens": 0,
            "total_tokens": 0,
            "api_equivalent_cost_usd": 0.0,
            "model_mix": [],
        })
        for field in (
            "input_tokens", "output_tokens", "cache_write_tokens",
            "cache_read_tokens", "total_tokens",
        ):
            period[field] += row[field]
        period["api_equivalent_cost_usd"] += row["api_cost_usd"]
        if row["total_tokens"] or row["model"] != "unknown":
            if row["unpriced_rows"]:
                pricing = "unpriced"
            elif row["aop_priced_rows"]:
                pricing = "aop_recorded"
            else:
                pricing = "model-prices"
            period["model_mix"].append({
                "model": row["model"],
                "rate_mode": row["rate_mode"],
                "source_kind": row["source_kind"],
                "total_tokens": row["total_tokens"],
                "api_equivalent_cost_usd": row["api_cost_usd"],
                "pricing": pricing,
            })

    periods = sorted(grouped.values(), key=lambda item: (item["resets_at"], item["tool"]))
    previous_by_limit: dict[tuple[str, str, int], float] = {}
    for period in periods:
        used_pct = period["used_pct"]
        cost = period["api_equivalent_cost_usd"]
        period["api_equivalent_usd_per_quota_percentage_point"] = (
            cost / used_pct if used_pct > 0 else None
        )
        estimate = cost * 100 / used_pct if used_pct > 0 else None
        period["full_quota_api_equivalent_cost_estimate_usd"] = estimate
        denominator = (
            period["input_tokens"] + period["cache_write_tokens"]
            + period["cache_read_tokens"]
        )
        period["cached_input_pct"] = (
            period["cache_read_tokens"] / denominator * 100 if denominator else None
        )

        pricing_flags = []
        if any(item["pricing"] == "unpriced" for item in period["model_mix"]):
            pricing_flags.append("unpriced_models")
        if any(item["pricing"] == "aop_recorded" for item in period["model_mix"]):
            pricing_flags.append("aop_recorded_pricing")
        if any(item["rate_mode"] != "default" for item in period["model_mix"]):
            pricing_flags.append("unsupported_rate_mode")
        period["pricing_flags"] = pricing_flags

        limit_key = (period["tool"], period["limit_id"], period["window_minutes"])
        previous = previous_by_limit.get(limit_key)
        period["change_from_previous_estimate_pct"] = (
            (estimate / previous - 1) * 100
            if estimate is not None and previous not in (None, 0) else None
        )
        if estimate is not None:
            previous_by_limit[limit_key] = estimate

    return {
        "schema_version": 4,
        "report_type": "quota_history",
        "generated_at": _time_text(datetime.now(timezone.utc)),
        "pricing_basis": pricing_basis,
        "usage_source": "local_native_sessions_and_aop_runs",
        "periods": periods,
    }


def _compact_model_mix(mix: list[dict[str, Any]]) -> str:
    costs: dict[str, float] = {}
    for item in mix:
        costs[item["model"]] = costs.get(item["model"], 0.0) + item["api_equivalent_cost_usd"]
    total = sum(costs.values())
    parts = []
    for model, cost in sorted(costs.items(), key=lambda entry: -entry[1])[:2]:
        share = cost / total * 100 if total else 0
        parts.append(f"{model} {share:.0f}%")
    return ", ".join(parts) or "-"


def print_quota_history(report: dict[str, Any]) -> None:
    print(f"pricing: {report['pricing_basis']}")
    rows = []
    for period in report["periods"]:
        estimate = period["full_quota_api_equivalent_cost_estimate_usd"]
        change = period["change_from_previous_estimate_pct"]
        cache = period["cached_input_pct"]
        rows.append([
            period["resets_at"][:16].replace("T", " "),
            period["tool"],
            period["limit_id"],
            format_duration(period["window_minutes"] * 60),
            f"{period['used_pct']:.1f}%",
            f"${period['api_equivalent_cost_usd']:.2f}",
            f"${estimate:.2f}" if estimate is not None else "-",
            f"{change:+.1f}%" if change is not None else "-",
            f"{cache:.1f}%" if cache is not None else "-",
            _compact_model_mix(period["model_mix"]),
            ",".join(period["pricing_flags"]) or "-",
        ])
    headers = [
        "reset (UTC)", "tool", "limit", "window", "used", "api cost", "implied full",
        "change", "cache", "model mix", "pricing flags",
    ]
    for line in _format_table(headers, rows, {4, 5, 6, 7, 8}):
        print(line)


WEEKS_PER_MONTH = 365.25 / 12 / 7
# Below this much usage, extrapolating one week to a full quota is too noisy to use.
VALUE_MIN_USED_PCT = 15

# Antigravity quota comes with the Google AI plan that gemini-cli-usage reports.
PLAN_TOOLS = {"Agy": "Gemini"}


def _period_plan(tool: str, period: dict[str, Any],
                 current: tuple[str | None, float | None]) -> tuple[str | None, float | None]:
    """The plan a quota period ran on: the one its snapshots reported, priced as of the
    period, or the tool's current plan when they reported none. A current plan that is a
    tier of the reported one, such as pro_200 for Codex's "pro", is the more exact name."""
    observed = period.get("plan")
    name = current[0]
    if not observed or (name and (name == observed or name.startswith(f"{observed}_"))):
        return current
    return observed, plan_price(tool, observed, at=period["resets_at"])


def build_value_report(history: dict[str, Any], plans: dict[str, tuple[str, float]],
                       forced: Iterable[str] = ()) -> dict[str, Any]:
    """Compare each tool's latest weekly full-quota API value with the price of the plan
    that week ran on, for each account the tool was used with. Tools in `forced` use the
    plan in `plans` whatever the week's snapshots reported."""
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for period in history["periods"]:
        if (period["window_minutes"] != 7 * 24 * 60 or period["total_tokens"] <= 0
                or period["used_pct"] <= VALUE_MIN_USED_PCT):
            continue
        key = (period["tool"], period.get("account") or "default")
        current = latest.get(key)
        rank = (period["resets_at"], period["used_pct"])
        if current is None or rank > current["rank"]:
            latest[key] = {"rank": rank, "period": period}

    tools = []
    for (tool, account), entry in sorted(latest.items()):
        period = entry["period"]
        weekly = period["full_quota_api_equivalent_cost_estimate_usd"]
        monthly = weekly * WEEKS_PER_MONTH
        plan_tool = PLAN_TOOLS.get(tool, tool)
        plan, price = plans.get(plan_tool, (None, None))
        if plan_tool not in forced:
            plan, price = _period_plan(plan_tool, period, (plan, price))
        price = price or None
        multiple = monthly / price if price and monthly else None
        api_usd_per_million = (
            period["api_equivalent_cost_usd"] / period["total_tokens"] * 1e6
        )
        tools.append({
            "tool": tool,
            "account": account,
            "plan": plan,
            "plan_usd_per_month": price,
            "limit_id": period["limit_id"],
            "resets_at": period["resets_at"],
            "used_pct": period["used_pct"],
            "full_week_api_equivalent_usd": weekly,
            "monthly_api_equivalent_usd": monthly,
            "api_equivalent_multiple": multiple,
            "api_usd_per_million_tokens": api_usd_per_million,
            "effective_usd_per_million_tokens": (
                api_usd_per_million / multiple if multiple else None
            ),
            "pricing_flags": period["pricing_flags"],
        })
    return {
        "schema_version": 1,
        "report_type": "subscription_value",
        "generated_at": _time_text(datetime.now(timezone.utc)),
        "pricing_basis": history["pricing_basis"],
        "weeks_per_month": WEEKS_PER_MONTH,
        "tools": tools,
    }


def print_value_report(report: dict[str, Any]) -> None:
    def money(value: float | None, digits: int = 0) -> str:
        return f"${value:,.{digits}f}" if value is not None else "-"

    rows = []
    for item in report["tools"]:
        multiple = item["api_equivalent_multiple"]
        account = item.get("account") or "default"
        rows.append([
            item["tool"],
            account if account == "default" else account[:8],
            item["plan"] or "-",
            money(item["plan_usd_per_month"]),
            item["resets_at"][:10],
            f"{item['used_pct']:.0f}%",
            money(item["full_week_api_equivalent_usd"]),
            money(item["monthly_api_equivalent_usd"]),
            f"{multiple:.0f}x" if multiple else "-",
            money(item["api_usd_per_million_tokens"], 3),
            money(item["effective_usd_per_million_tokens"], 4),
            ",".join(item["pricing_flags"]) or "-",
        ])
    headers = [
        "tool", "account", "plan", "price/mo", "week ending", "used", "full week", "month",
        "multiple", "api $/1M", "effective $/1M", "pricing flags",
    ]
    for line in _format_table(headers, rows, {3, 5, 6, 7, 8, 9, 10}):
        print(line)
    print(
        "\nFull week: API-equivalent value of 100% of the latest weekly quota period "
        f"more than {VALUE_MIN_USED_PCT}% used. "
        f"Month: full week x {report['weeks_per_month']:.3f}. $/1M uses that week's own "
        "token mix, counting input, cache, and output tokens. Each account of a tool is a "
        "separate subscription. The plan is the one that week's quota snapshots reported, or "
        "else the one detected or configured for the tool."
    )


def _format_mix(mix: list[dict[str, Any]]) -> str:
    total = sum(item["api_cost_usd"] or 0 for item in mix)
    if not mix:
        return "-"
    parts = []
    for item in mix[:3]:
        share = (item["api_cost_usd"] or 0) / total * 100 if total else 0
        mode = f"/{item['rate_mode']}" if item["rate_mode"] != "default" else ""
        parts.append(f"{item['model']}{mode} {share:.0f}%")
    return ", ".join(parts)


def _format_table(headers: list[str], rows: list[list[str]],
                  right_aligned: set[int] | None = None) -> list[str]:
    align_right = right_aligned or set()
    widths = [
        max([len(headers[index]), *(len(row[index]) for row in rows)])
        for index in range(len(headers))
    ]

    def render(row: list[str]) -> str:
        cells = []
        for index, value in enumerate(row):
            cells.append(value.rjust(widths[index]) if index in align_right
                         else value.ljust(widths[index]))
        return "  ".join(cells).rstrip()

    return [render(headers), *(render(row) for row in rows)]


def _pace_bucket_label(bucket: dict[str, Any]) -> str:
    provider = bucket["provider"]
    seconds = bucket["window_seconds"]
    scope = bucket["scope_id"]
    if provider == "openai":
        window_label = {
            5 * 3600: "Session (5h)",
            24 * 3600: "Daily",
            7 * 86400: "Week (7d)",
            30 * 86400: "Month (30d)",
        }.get(seconds, bucket["bucket_label"].title())
        if scope:
            frequency = {
                5 * 3600: "5h",
                24 * 3600: "daily",
                7 * 86400: "weekly",
                30 * 86400: "monthly",
            }.get(seconds, window_label)
            return f"{scope} ({frequency})"
        return window_label
    if provider == "anthropic":
        if bucket["bucket_id"] == "5h":
            return "Session"
        if bucket["bucket_id"] == "1w":
            return "Week (all models)"
        if seconds == 7 * 86400 and scope:
            return f"Week ({scope})"
    if provider == "zai":
        return {"5h": "Session (5h)", "1w": "Week"}.get(
            bucket["bucket_id"], bucket["bucket_label"]
        )
    if provider == "antigravity" and scope:
        scope = re.sub(r"\s+models$", "", scope, flags=re.IGNORECASE)
        window_label = {5 * 3600: "5h", 7 * 86400: "weekly"}.get(seconds)
        if window_label:
            return f"{scope} ({window_label})"
    label = bucket["bucket_label"]
    if scope and scope.lower() not in label.lower():
        return f"{label} ({scope})"
    return label


def print_report(report: dict[str, Any], show_model_mix: bool = False) -> None:
    rows = []
    targets = []
    calibrations = []
    projection_unavailable: dict[str, list[str]] = {}
    for bucket in report["buckets"]:
        provider = PROVIDER_SHORT_NAMES.get(bucket["provider"], bucket["provider"])
        bucket_label = _pace_bucket_label(bucket)
        identity = f"{provider} {bucket_label}"
        calibration = bucket["token_calibration"]["api_usd_per_quota_percentage_point"]
        if calibration is not None:
            calibrations.append(f"{identity} ${calibration:.3f}")
        target = bucket["target_percentage_points_per_hour"]
        if target is not None:
            targets.append(f"{identity} {target:.3f} quota pp/h")
        has_full_lookback = False
        for interval in bucket["lookbacks"]:
            if interval["quality"] != "full":
                continue
            has_full_lookback = True
            period_label = format_duration(interval["requested_seconds"])
            quota_rate = interval["quota_delta_pp_per_hour"]
            token_rate = interval["token_derived_pp_per_hour"]
            cost_rate = interval["local_api_usd_per_hour"]
            projected = interval["uncapped_projected_used_pct"]
            row = [
                provider,
                bucket_label,
                f"{bucket['used_pct']:.1f}%",
                period_label,
                f"{quota_rate:.3f}",
                f"{token_rate:.3f}" if token_rate is not None else "-",
                f"{projected:.1f}%" if projected is not None else "-",
                interval["guidance"] or "-",
                f"${cost_rate:.3f}" if cost_rate is not None else "-",
            ]
            if show_model_mix:
                row.append(_format_mix(interval["model_mix"]))
            rows.append(row)
        if has_full_lookback and bucket["forecast_basis"] is None:
            if bucket["remaining_hours"] is None:
                reason = "no future reset is available"
            elif bucket["window_kind"] == "rolling":
                reason = "rolling-window expiration is not separately observable"
            elif bucket["window_kind"] == "unknown":
                reason = "window semantics are unknown"
            else:
                reason = "forecasting is unavailable"
            projection_unavailable.setdefault(reason, []).append(identity)
    if targets:
        print("target pace: " + "; ".join(targets))
    if calibrations:
        print("experimental calibration ($/quota pp): " + "; ".join(calibrations))
    headers = [
        "provider", "bucket", "used", "period", "quota pp/h", "token pp/h",
        "projected", "guidance", "local $/h",
    ]
    if show_model_mix:
        headers.append("model mix")
    if rows:
        for line in _format_table(headers, rows, {2, 4, 5, 6, 8}):
            print(line)
    else:
        print("No complete lookbacks yet.")
    window_rows = []
    for bucket in [*report["buckets"], *report.get("logged_out_buckets", [])]:
        pace = bucket.get("window_pace")
        # Shorter windows are already covered by the lookback periods above.
        if not pace or bucket["window_seconds"] < 86400:
            continue
        logged_out = _parse_time(bucket.get("logged_out_since"))
        window_rows.append([
            PROVIDER_SHORT_NAMES.get(bucket["provider"], bucket["provider"]),
            bucket.get("account_label") or bucket["account_key"][:8],
            _pace_bucket_label(bucket),
            f"{bucket['used_pct']:.1f}%",
            f"{pace['logged_in_hours']:.1f}",
            f"{pace['quota_pp_per_hour']:.3f}",
            f"{pace['pace_ratio']:.2f}x",
            f"logged out since {logged_out:%Y-%m-%d %H:%MZ}" if logged_out else "logged in",
        ])
    if window_rows:
        print("\nThis window by account, over logged-in time only (1.00x uses the whole window "
              "evenly):")
        for line in _format_table(
                ["provider", "account", "bucket", "used", "logged-in h", "quota pp/h",
                 "vs even", "status"], window_rows, {3, 4, 5, 6}):
            print(line)
    for reason, identities in projection_unavailable.items():
        print(
            f"\nProjection unavailable for {', '.join(identities)}: {reason}; "
            "quota pp/h is a net trend only."
        )
    if report.get("hidden_bucket_count"):
        collecting = report.get("collecting_bucket_count", 0)
        inactive = report["hidden_bucket_count"] - collecting
        reasons = []
        if inactive:
            reasons.append(
                f"{inactive} inactive quota bucket{'s' if inactive != 1 else ''}"
            )
        if collecting:
            reasons.append(
                f"{collecting} without complete lookbacks"
            )
        print(f"\nSkipped {' and '.join(reasons)}. Use --all-buckets to show them.")
