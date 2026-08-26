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


DEFAULT_LOOKBACKS = (3600, 6 * 3600, 24 * 3600)
STATE_DIR = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
DEFAULT_DB_PATH = STATE_DIR / "clanker-analytics" / "quota.sqlite3"

PROVIDER_TO_TOOL = {
    "anthropic": "Claude Code",
    "openai": "Codex",
    "antigravity": "Agy",
}

PROVIDER_SHORT_NAMES = {
    "anthropic": "claude",
    "openai": "codex",
    "antigravity": "agy",
}


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
    )


def normalize_anthropic(data: dict[str, Any], collected_at: datetime) -> list[QuotaSample]:
    effective_at = _time_text(data.get("updated_at"), collected_at)
    account_key = str(data.get("account_key") or "default")
    source = str(data.get("source") or "unknown")
    plan = str(data.get("plan") or "unknown").lower()
    if "enterprise" in plan:
        window_kind = "rolling"
    elif plan in {"ai", "free", "pro", "max", "max_5x", "max_20x"}:
        window_kind = "fixed"
    else:
        window_kind = "unknown"
    samples = []
    for bucket_id, value in data.items():
        if not isinstance(value, dict) or "pct" not in value:
            continue
        canonical_id = "session" if bucket_id == "5h" else bucket_id
        if canonical_id == "session":
            window_seconds = 5 * 3600
        elif canonical_id.startswith("7d"):
            window_seconds = 7 * 86400
        else:
            window_seconds = None
        samples.append(_make_sample(
            provider="anthropic",
            account_key=account_key,
            bucket_id=canonical_id,
            bucket_label=canonical_id.replace("_", " "),
            scope_type="account" if canonical_id in {"session", "7d"} else "model",
            scope_id="" if canonical_id in {"session", "7d"} else canonical_id.removeprefix("7d_"),
            window_kind=window_kind,
            window_seconds=window_seconds,
            effective_at=effective_at,
            collected_at=_time_text(collected_at),
            used_pct=value["pct"],
            resets_at=_rounded_time(value.get("resets_at"), 60),
            source=source,
        ))
    return samples


def normalize_openai(data: dict[str, Any], collected_at: datetime) -> list[QuotaSample]:
    effective_at = _time_text(data.get("updated_at"), collected_at)
    account_key = str(data.get("account_key") or "default")
    source = str(data.get("source") or "unknown")
    samples = []

    def add(bucket_id: str, label: str, value: Any, scope_type: str, scope_id: str) -> None:
        if not isinstance(value, dict) or "pct" not in value:
            return
        samples.append(_make_sample(
            provider="openai",
            account_key=account_key,
            bucket_id=bucket_id,
            bucket_label=label,
            scope_type=scope_type,
            scope_id=scope_id,
            window_kind="fixed",
            window_seconds=int(value["window_secs"]) if value.get("window_secs") else None,
            effective_at=effective_at,
            collected_at=_time_text(collected_at),
            used_pct=value["pct"],
            resets_at=value.get("resets_at"),
            source=source,
        ))

    if "primary" in data or "secondary" in data:
        add("primary", "primary", data.get("primary"), "account", "")
        add("secondary", "secondary", data.get("secondary"), "account", "")
    else:
        add("7d", "7d", data.get("7d"), "account", "")

    for group in data.get("additional") or []:
        if not isinstance(group, dict):
            continue
        name = str(group.get("name") or "additional")
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        add(f"additional:{slug}:primary", f"{name} primary", group.get("primary"), "model", name)
        add(f"additional:{slug}:secondary", f"{name} secondary", group.get("secondary"), "model", name)
    return samples


def normalize_antigravity(data: dict[str, Any], collected_at: datetime) -> list[QuotaSample]:
    effective_at = _time_text(data.get("updated_at"), collected_at)
    account_key = str(data.get("account_key") or "default")
    source = str(data.get("source") or "unknown")
    samples = []
    groups = (data.get("quota_summary") or {}).get("groups") or []
    for group in groups:
        group_name = str(group.get("display_name") or "models")
        for value in group.get("buckets") or []:
            if "remaining_pct" not in value:
                continue
            window = str(value.get("window") or "")
            window_seconds = {
                "5h": 5 * 3600,
                "weekly": 7 * 86400,
            }.get(window)
            bucket_id = str(value.get("bucket_id") or f"{group_name}:{window}")
            samples.append(_make_sample(
                provider="antigravity",
                account_key=account_key,
                bucket_id=bucket_id,
                bucket_label=f"{group_name} {window}".strip(),
                scope_type="model_group",
                scope_id=group_name,
                window_kind="fixed",
                window_seconds=window_seconds,
                effective_at=effective_at,
                collected_at=_time_text(collected_at),
                used_pct=100.0 - float(value["remaining_pct"]),
                resets_at=value.get("reset_time"),
                source=source,
            ))
    return samples


NORMALIZERS = {
    "anthropic": normalize_anthropic,
    "openai": normalize_openai,
    "antigravity": normalize_antigravity,
}


def _collector_command(provider: str) -> list[str] | None:
    if provider == "anthropic":
        return [sys.executable, "-m", "ccusage", "json"]
    if provider == "openai":
        return [sys.executable, "-m", "codex_cli_usage", "json"]
    if provider == "antigravity":
        return [sys.executable, "-m", "agy_usage", "json"]
    return None


def collect_provider(provider: str, timeout: int = 30) -> list[QuotaSample]:
    command = _collector_command(provider)
    if not command:
        raise RuntimeError(f"no quota collector found for {provider}")
    collected_at = datetime.now(timezone.utc)
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        detail = result.stderr.strip().splitlines()[-1] if result.stderr.strip() else "collector failed"
        raise RuntimeError(f"{provider}: {detail}")
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{provider}: collector returned invalid JSON") from error
    return NORMALIZERS[provider](data, collected_at)


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
            status TEXT NOT NULL
        )
    """)
    connection.execute("""
        CREATE INDEX IF NOT EXISTS quota_samples_lookup
        ON quota_samples(provider, account_key, bucket_id, scope_type, scope_id, effective_at)
    """)
    connection.execute("PRAGMA user_version = 1")
    return connection


def insert_samples(connection: sqlite3.Connection, samples: Iterable[QuotaSample]) -> int:
    before = connection.total_changes
    connection.executemany("""
        INSERT OR IGNORE INTO quota_samples (
            provider, account_key, bucket_id, bucket_label, scope_type, scope_id,
            window_kind, window_seconds, effective_at, collected_at, used_pct,
            resets_at, source, source_sample_id, status
        ) VALUES (
            :provider, :account_key, :bucket_id, :bucket_label, :scope_type, :scope_id,
            :window_kind, :window_seconds, :effective_at, :collected_at, :used_pct,
            :resets_at, :source, :source_sample_id, :status
        )
    """, [asdict(sample) for sample in samples])
    connection.commit()
    return connection.total_changes - before


def record_quota(path: Path = DEFAULT_DB_PATH, providers: Iterable[str] | None = None) -> tuple[int, list[str]]:
    selected = list(providers or NORMALIZERS)
    samples = []
    errors = []
    for provider in selected:
        try:
            samples.extend(collect_provider(provider))
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as error:
            errors.append(str(error))
    with connect(path) as connection:
        inserted = insert_samples(connection, samples)
    return inserted, errors


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


def _model_patterns(bucket: dict[str, Any]) -> list[str]:
    if bucket["scope_type"] == "account" or not bucket["scope_id"]:
        return []
    scope = bucket["scope_id"].lower()
    if scope == "gemini models":
        return ["%gemini%"]
    if scope == "claude and gpt models":
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
                 cost_expression: str, include_inactive: bool = False) -> dict[str, Any]:
    requested = sorted(set(lookbacks))
    buckets = []
    hidden_bucket_count = 0
    collecting_bucket_count = 0
    current_samples = latest_samples(connection)
    for current in current_samples:
        now = _parse_time(current["effective_at"])
        if now is None:
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
            "bucket_id": current["bucket_id"],
            "bucket_label": current["bucket_label"],
            "scope_type": current["scope_type"],
            "scope_id": current["scope_id"],
            "window_kind": current["window_kind"],
            "window_seconds": current["window_seconds"],
            "effective_at": current["effective_at"],
            "used_pct": current["used_pct"],
            "resets_at": current["resets_at"],
            "remaining_hours": remaining_hours,
            "target_percentage_points_per_hour": target_rate,
            "forecast_basis": forecast_basis,
            "token_calibration": {
                "status": "experimental",
                "basis": "local_api_equivalent_usd",
                "pricing_basis": "clanker-analytics-api-rates-2026-08-20",
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
    }


def _known_codex_price(model: str) -> bool:
    normalized = model.lower()
    return any(name in normalized for name in (
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
    ))


def build_quota_history(token_db: Any, cost_expression: str,
                        pricing_basis: str) -> dict[str, Any]:
    """Audit all weekly Codex quota periods retained in the token cache."""
    rows = token_db.execute(f"""
        WITH observations AS (
            SELECT
                round(quota_resets_at / 300.0)::BIGINT * 300
                    as canonical_resets_at,
                arg_max(quota_resets_at, try_cast(timestamp AS TIMESTAMPTZ))
                    as quota_resets_at,
                quota_window_minutes,
                coalesce(nullif(quota_limit_id, ''), 'codex') as quota_limit_id,
                arg_max(quota_used_pct, try_cast(timestamp AS TIMESTAMPTZ))
                    as last_used_pct,
                max(quota_used_pct) as max_used_pct,
                min(try_cast(timestamp AS TIMESTAMPTZ)) as first_observed_at,
                max(try_cast(timestamp AS TIMESTAMPTZ)) as observed_at,
                count(*)::BIGINT as observation_count
            FROM tokens
            WHERE tool = 'Codex'
              AND source_kind = 'native'
              AND quota_used_pct IS NOT NULL
              AND quota_used_pct > 0
              AND quota_resets_at IS NOT NULL
              AND quota_window_minutes = 10080
            GROUP BY canonical_resets_at, quota_window_minutes,
                     coalesce(nullif(quota_limit_id, ''), 'codex')
        )
        SELECT
            o.quota_resets_at,
            o.quota_window_minutes,
            o.quota_limit_id,
            o.last_used_pct,
            o.max_used_pct,
            cast(o.first_observed_at AS VARCHAR) as first_observed_at,
            cast(o.observed_at AS VARCHAR) as observed_at,
            o.observation_count,
            coalesce(nullif(t.model, ''), 'unknown') as model,
            coalesce(nullif(t.rate_mode, ''), 'default') as rate_mode,
            coalesce(sum(t.input_tokens), 0)::BIGINT as input_tokens,
            coalesce(sum(t.output_tokens), 0)::BIGINT as output_tokens,
            coalesce(sum(t.cache_write_tokens), 0)::BIGINT as cache_write_tokens,
            coalesce(sum(t.cache_read_tokens), 0)::BIGINT as cache_read_tokens,
            coalesce(sum(t.total_tokens), 0)::BIGINT as total_tokens,
            coalesce(sum({cost_expression}), 0)::DOUBLE as api_cost_usd
        FROM observations o
        LEFT JOIN tokens t
          ON t.tool = 'Codex'
         AND t.source_kind = 'native'
         AND try_cast(t.timestamp AS TIMESTAMPTZ)
             >= to_timestamp(o.quota_resets_at)
                - o.quota_window_minutes * INTERVAL '1 minute'
         AND try_cast(t.timestamp AS TIMESTAMPTZ) <= o.observed_at
        GROUP BY
            o.quota_resets_at, o.quota_window_minutes, o.quota_limit_id,
            o.last_used_pct, o.max_used_pct, o.first_observed_at,
            o.observed_at, o.observation_count,
            coalesce(nullif(t.model, ''), 'unknown'),
            coalesce(nullif(t.rate_mode, ''), 'default')
        ORDER BY o.quota_resets_at, api_cost_usd DESC, total_tokens DESC
    """).fetchall()
    columns = [item[0] for item in token_db.description]

    grouped: dict[tuple[int, int, str], dict[str, Any]] = {}
    for raw_row in rows:
        row = dict(zip(columns, raw_row))
        key = (
            row["quota_resets_at"],
            row["quota_window_minutes"],
            row["quota_limit_id"],
        )
        period = grouped.setdefault(key, {
            "window_start": _time_text(
                datetime.fromtimestamp(row["quota_resets_at"], timezone.utc)
                - timedelta(minutes=row["quota_window_minutes"])
            ),
            "resets_at": _time_text(row["quota_resets_at"]),
            "observed_at": _time_text(row["observed_at"]),
            "first_observed_at": _time_text(row["first_observed_at"]),
            "window_minutes": row["quota_window_minutes"],
            "limit_id": row["quota_limit_id"],
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
        period["model_mix"].append({
            "model": row["model"],
            "rate_mode": row["rate_mode"],
            "total_tokens": row["total_tokens"],
            "api_equivalent_cost_usd": row["api_cost_usd"],
            "pricing": (
                "rate_card" if _known_codex_price(row["model"])
                else "fallback"
            ),
        })

    periods = sorted(grouped.values(), key=lambda item: item["resets_at"])
    previous_by_limit: dict[str, float] = {}
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

        quality = []
        if used_pct < period["max_used_pct"]:
            quality.append("inconsistent")
        if used_pct < 25:
            quality.append("early")
        if period["observation_count"] < 2:
            quality.append("sparse")
        if any(item["pricing"] == "fallback" for item in period["model_mix"]):
            quality.append("fallback_pricing")
        if any(item["rate_mode"] != "default" for item in period["model_mix"]):
            quality.append("unsupported_rate_mode")
        period["quality"] = quality or ["high_utilization"]

        previous = previous_by_limit.get(period["limit_id"])
        period["change_from_previous_estimate_pct"] = (
            (estimate / previous - 1) * 100
            if estimate is not None and previous not in (None, 0) else None
        )
        if estimate is not None:
            previous_by_limit[period["limit_id"]] = estimate

    return {
        "schema_version": 1,
        "report_type": "codex_quota_history",
        "generated_at": _time_text(datetime.now(timezone.utc)),
        "pricing_basis": pricing_basis,
        "pricing_method": "constant_rate_card_applied_to_all_periods",
        "usage_source": "local_codex_native_session_files",
        "periods": periods,
    }


def _compact_model_mix(mix: list[dict[str, Any]]) -> str:
    total = sum(item["api_equivalent_cost_usd"] for item in mix)
    parts = []
    for item in mix[:2]:
        share = item["api_equivalent_cost_usd"] / total * 100 if total else 0
        parts.append(f"{item['model']} {share:.0f}%")
    return ", ".join(parts) or "-"


def print_quota_history(report: dict[str, Any]) -> None:
    print(f"pricing basis: {report['pricing_basis']} (constant across all periods)")
    print("reset (UTC)        used    api cost  implied full   change  cache  model mix  quality")
    for period in report["periods"]:
        estimate = period["full_quota_api_equivalent_cost_estimate_usd"]
        change = period["change_from_previous_estimate_pct"]
        cache = period["cached_input_pct"]
        estimate_text = f"${estimate:>11.2f}" if estimate is not None else f"{'-':>12}"
        change_text = f"{change:>+6.1f}%" if change is not None else f"{'-':>7}"
        cache_text = f"{cache:>5.1f}%" if cache is not None else f"{'-':>6}"
        reset_text = period["resets_at"][:16].replace("T", " ")
        print(
            f"{reset_text}  {period['used_pct']:>5.1f}%  "
            f"${period['api_equivalent_cost_usd']:>8.2f}  "
            f"{estimate_text}  {change_text}  {cache_text}  "
            f"{_compact_model_mix(period['model_mix'])}  "
            f"{','.join(period['quality'])}"
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
        if bucket["bucket_id"] == "session":
            return "Session"
        if bucket["bucket_id"] == "7d":
            return "Week (all models)"
        if bucket["bucket_id"].startswith("7d_"):
            return f"Week ({bucket['bucket_id'][3:].replace('_', ' ').title()})"
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
