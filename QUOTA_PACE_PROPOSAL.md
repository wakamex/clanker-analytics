# Quota pace and token usage rate proposal

Status: revised design proposal with the discovery recorder and comparison slice implemented

Date: 2026-08-20

## Summary

`clanker-analytics` should forecast bounded provider quota buckets from retained provider-utilization snapshots.

For fixed reset windows, it should estimate:

- Whether the bucket will be exhausted before reset.
- When exhaustion will occur at the observed net pace.
- How much the account-wide net quota pace must fall to avoid exhaustion.

For rolling windows, it should report recent net utilization trends and recovery state. It should not translate a rolling net trend into a workload adjustment or a user-facing exhaustion forecast unless gross consumption and scheduled expiration are separately observable.

Provider utilization remains authoritative because it includes activity outside local logs. Local metered usage is optional explanatory data showing which models, tools, projects, and execution types contributed to locally observed consumption. It must never substitute for the provider forecast.

The dogfood implementation also exposes an explicitly experimental token-derived percentage-point rate. It divides locally observed API-equivalent usage in the current provider window by current provider utilization, then applies that local calibration to each requested lookback. This is useful for testing whether local token accounting tracks quota movement across cache rates and model mixes. It is not used for forecasts or recommendations because missing local activity and undocumented provider weighting can invalidate the calibration.

The first implementation should be a one-shot snapshot recorder, a fixed-window terminal report, and a versioned JSON contract containing all lookbacks and source-quality states. Website and statusline consumers should format this output without reimplementing forecasting logic.

## Current system and problem

The existing website and `/code/scripts/usage-burn` display a value calculated as:

```text
time remaining percentage / quota remaining percentage
```

For example, if 40 percent of the week remains and 10 percent of the quota remains, the value is `4x`.

This measures quota pressure. It does not measure recent consumption, project utilization to the reset, estimate an exhaustion time, or say how the current account-wide pace must change. Two accounts with the same utilization and reset time receive the same value even if one stopped consuming quota yesterday and the other is consuming it rapidly now.

The website has only the latest provider snapshot because `publish_usage.sh` replaces `usage.json` on each update. Without retained snapshots, it cannot calculate a recent rate.

`clanker-analytics` already provides the eventual explanatory layer:

- It reads Claude Code, Codex, Gemini CLI, Antigravity, and AOP session records.
- It normalizes input, output, cache write, and cache read tokens.
- It estimates API-equivalent cost using model-specific rates.
- It retains project, session, tool, and execution attribution.
- It incrementally caches normalized rows in DuckDB and Parquet.

None of that token work is required to start quota pacing. The immediate missing input is retained provider-utilization history.

## Design decisions

The proposal makes these decisions explicit:

- Provider utilization snapshots are the forecasting authority.
- Fixed, rolling, and unknown windows have different semantics.
- Only confirmed fixed windows receive pace-change recommendations.
- Recommendations describe account-wide net quota pace, not necessarily personal activity.
- Local weighted token usage is optional context and is not on the MVP critical path.
- One-shot fresh collection comes before daemon management.
- All pace calculations live in `clanker-analytics` and are serialized in versioned JSON.
- Purchased credit balances without a bounded reset cycle are outside this quota-percentage model.

## Goals

- Show whether a confirmed fixed quota is on track to reach 100 percent before reset.
- Show recent net pace at several lookbacks.
- Estimate exhaustion time and how early it occurs relative to reset.
- Give a mathematically correct required reduction in account-wide net pace.
- Never calculate a fixed-window rate across a reset.
- Report rolling-window net trends without overstating their predictive meaning.
- Preserve source freshness, account identity, scope, and window semantics.
- Produce reusable JSON for terminal, website, and statusline views.
- Clearly label stale, sparse, quantized, inconsistent, and insufficient data.

## Non-goals

- Predict future task mix with machine learning.
- Infer an undocumented absolute quota size from a subscription price.
- Treat local logs as a complete billing ledger.
- Recommend workload changes for rolling or unknown windows.
- Model purchased credit balances that have no shared percentage and reset cycle.
- Replace provider usage monitors or duplicate their authentication flows.
- Add notifications, automated throttling, or a long-running daemon in the first version.
- Make local token pricing or attribution a prerequisite for quota pacing.

## Window semantics

Every normalized quota bucket has:

```text
window_kind: fixed | rolling | unknown
```

This value must come from adapter evidence or documented provider semantics. It must not be guessed from a familiar duration or the presence of a reset timestamp.

### Fixed windows

A fixed window has a bounded cycle and a reset at which accumulated utilization is cleared or replaced by a new cycle. Utilization should normally be monotonic within a cycle.

For fixed windows, the report may calculate target pace, projected utilization, estimated exhaustion time, and required net pace reduction.

A material utilization decrease before reset is not normal fixed-window recovery. It should be treated as a possible correction, reset, account change, or malformed sample and should invalidate the affected interval until it is classified.

### Rolling windows

A rolling-window utilization delta is net new consumption minus usage that expired during the same interval. A lower net pace does not necessarily imply a proportional reduction in current activity.

For rolling windows, report only values supported by the observed snapshots:

```text
current utilization: 72%
6h net trend: +0.7 percentage points/hour
status: rising
```

A negative trend should be labeled `recovering`. A near-zero trend should be labeled `steady` only when the provider's resolution and sampling history can distinguish it from quantization.

Do not show a workload-change recommendation. Do not show a user-facing exhaustion time based only on linear net-trend extrapolation because the future expiration schedule can change that trend abruptly.

### Unknown windows

Unknown windows should receive the same conservative treatment as rolling windows. They may show current utilization and recent net deltas, but no reset forecast or workload recommendation.

## Fixed-window product model

Each fixed quota bucket should have one headline and a lookback table.

Example output with synthetic values:

```text
Codex - weekly - 72% used - resets in 31h

lookback   net quota pace   exhausts          account-wide adjustment
1h         1.42x             9h before reset  pace must fall 30%
6h         1.21x             5h before reset  pace must fall 17%
24h        0.88x            after reset       headroom 14%

Target: at most 0.90 quota percentage points/hour
Headline: At the 6h net pace, quota would be exhausted about 5h before reset.
```

When a bucket is clearly personal and fixed, a compact UI may shorten `account-wide quota pace must fall about 17%` to `reduce current pace about 17%`. The JSON contract should retain the precise account-wide field name.

For windows of at least one day, the default lookbacks should be 1 hour, 6 hours, and 24 hours. For shorter fixed windows, they should be 15 minutes, 1 hour, and 3 hours.

Users may replace those defaults with arbitrary compact lookbacks by repeating `--lookback`:

```text
clanker-analytics pace 3h 7h
```

The initial duration grammar should stay deliberately small: a positive integer followed by `m`, `h`, `d`, or `w`. Values are normalized to seconds, deduplicated, and sorted. Natural-language durations, aliases, decimals, and adaptive lookback selection are not required. Custom lookbacks use the same sample-quality and reset-segmentation rules as defaults.

The initial weekly headline should use the 6-hour lookback when that interval has full-quality history. The other lookbacks remain visible so users can see acceleration or deceleration. Backtesting must confirm that 6 hours is a useful default before the website promotes it as the headline.

## Fixed-window calculations

For the current sample:

- `u_now` is provider-reported percentage used.
- `t_now` is the time when that provider measurement was effective.
- `t_reset` is the provider-reported reset time.
- `u_past` and `t_past` come from a valid earlier sample in the same fixed cycle.

The sustainable net rate from now until reset is:

```text
target_rate = (100 - u_now) / hours(t_reset - t_now)
```

The observed account-wide net quota rate is:

```text
observed_rate = (u_now - u_past) / hours(t_now - t_past)
```

The pace ratio is:

```text
pace_ratio = observed_rate / target_rate
```

The uncapped linear projection is:

```text
uncapped_projected_used_pct =
    u_now + observed_rate * hours(t_reset - t_now)
```

This field is explicitly uncapped because a value such as 112 percent describes trajectory, not a realizable provider utilization value.

When `observed_rate > 0`, exhaustion timing is:

```text
hours_to_exhaustion = (100 - u_now) / observed_rate
estimated_exhaustion_at = t_now + hours_to_exhaustion
hours_before_reset = hours(t_reset - estimated_exhaustion_at)
exhausts_before_reset = estimated_exhaustion_at < t_reset
```

When `pace_ratio > 1`, the required account-wide net pace reduction is:

```text
required_net_reduction = 1 - target_rate / observed_rate
```

When `0 < pace_ratio < 1`, headroom is:

```text
headroom = target_rate / observed_rate - 1
```

A pace ratio of `1.25x` therefore requires a 20 percent reduction, not 25 percent.

No headroom recommendation should be produced when the observed rate is zero, negative, stale, or indistinguishable from zero at the provider's apparent resolution. This prevents infinite or misleading recommendations from quantized percentage data.

## Fixed-cycle segmentation

A fixed-window calculation must never cross a reset.

Samples belong to the same fixed cycle only when all of these remain compatible:

- Provider and opaque account identity.
- Bucket identity and scope.
- Window kind and duration.
- Reset timestamp, allowing a small timestamp-normalization tolerance.

A new segment begins after the previous reset passes or when the reset advances to a new cycle. A material utilization decrease inside an otherwise unchanged fixed cycle marks the surrounding interval inconsistent. The implementation should not silently reinterpret that decrease as recovery.

Rolling and unknown windows are not segmented on utilization decreases or moving reset timestamps. Their lookback deltas are grouped by account, bucket, and scope, with data-quality checks applied to each interval.

## Deterministic lookback selection

For requested lookback `L` and current effective time `t_now`:

1. Set `target_time = t_now - L`.
2. In the current compatible segment, choose the latest sample whose `effective_at <= target_time`.
3. Calculate using the actual elapsed duration between that sample and the current sample.
4. Record requested duration, actual duration, and both timestamps in JSON.

The selected historical sample should be close to the target time. A sample is full quality when its distance from the target is no more than two recent collection intervals. The recent collection interval is the median positive interval between samples in the compatible segment. When too little history exists to estimate cadence, the configured recorder cadence is used. The initial configured cadence is five minutes.

If no full-quality sample exists, the report may calculate an available-history interval using the oldest compatible sample, but it must:

- Label the row with its actual duration rather than the requested lookback.
- Mark it `partial`.
- Exclude it from headline selection and pace recommendations.

This prevents a ten-minute interval from appearing under a 24-hour label without imposing an arbitrary percentage-coverage threshold.

Intervals use these initial quality states:

- `full`: requested lookback is covered with an acceptably close historical sample.
- `partial`: only a shorter explicitly labeled available interval exists.
- `quantized`: the observed delta is too small to distinguish from provider resolution.
- `stale`: the current provider measurement is too old for a current recommendation.
- `inconsistent`: samples violate fixed-cycle invariants.
- `insufficient`: no meaningful calculation is available.

The terminal view may collapse these into plain language. JSON should retain the machine-readable state.

## Why provider utilization is authoritative

Local token records are not account-wide. They can omit:

- Usage from another computer.
- Usage from web, desktop, or mobile product surfaces.
- Team or organization activity charged to the same pool.
- Background work whose local records were removed or never retained.
- Provider-side retries, hidden operations, or metering adjustments.

Anthropic documents that Claude usage can be shared across Claude product surfaces and that some organization usage uses rolling windows. OpenAI documents shared agentic allowances and model-specific token credit rates. Provider utilization snapshots naturally include activity outside the local log corpus, so their deltas are the safest available basis for account-wide pacing.

References:

- [Models, usage, and limits in Claude Code](https://support.claude.com/en/articles/14552983-models-usage-and-limits-in-claude-code)
- [How Claude usage and length limits work](https://support.claude.com/en/articles/11647753-how-do-usage-and-length-limits-work)
- [Using Codex with a ChatGPT plan](https://help.openai.com/en/articles/11369540-using-codex-with-your-chatgpt-plan)
- [Codex rate card](https://help.openai.com/en/articles/20001106)

## Quota sample schema

The durable snapshot schema should remain small:

```text
provider
account_key
bucket_id
bucket_label
scope_type
scope_id
window_kind
window_seconds
effective_at
collected_at
used_pct
resets_at
source
status
```

Field meanings:

- `account_key` is an opaque local identity that prevents two accounts or workspaces from sharing a history segment. It must not contain an email address, account ID, or credential. A provider adapter may supply a stable one-way fingerprint when available.
- `scope_type` distinguishes account-wide, model, feature, and other bounded scopes.
- `scope_id` identifies the model or feature for non-account-wide scopes.
- `window_kind` is fixed, rolling, or unknown and comes from adapter evidence.
- `effective_at` is when the provider measurement became effective and is the timestamp used for rate calculations.
- `collected_at` is when `clanker-analytics` ingested the measurement and is used for collection diagnostics.
- `used_pct` is always normalized to percentage used, even when a source reports percentage remaining.
- `status` records live, cached, stale, or unavailable source state.

Provider resolution should initially be inferred from retained observations and reported as derived quality metadata. A stored `used_pct_resolution` field can be added later if adapters begin exposing reliable resolution directly.

Purchased or promotional credit balances without a bounded percentage and reset cycle are not stored as quota buckets. A future absolute-balance feature should use a separate schema.

## Storage

The snapshot store should use SQLite in the platform-appropriate application state directory. On Linux, the default should be:

```text
~/.local/state/clanker-analytics/quota.sqlite3
```

SQLite is appropriate here because it is included with Python and directly provides:

- Idempotent inserts for one-shot recording and historical backfill.
- A uniqueness constraint across account, bucket, scope, and effective time.
- Ordered range queries for lookbacks.
- Transactional recovery if recording is interrupted.
- Straightforward future schema migration.

This remains one small table and one writer command. The design does not require a database service, background compactor, concurrent writer pool, or separate migration framework.

A representative uniqueness key is:

```text
provider, account_key, bucket_id, scope_type, scope_id, effective_at
```

If a provider corrects a measurement at the same effective time, the recorder may update that row transactionally and preserve the newer `collected_at` and source status.

Retention pruning is not required for the first version. Five-minute samples remain modest. A bounded retention policy can be added if real database growth warrants it.

## One-shot collection

The first collection interface should be a one-shot command:

```text
clanker-analytics --record-quota
```

It should ask each installed provider usage package for a fresh normalized measurement through that package's supported JSON or refresh interface. Provider packages continue owning authentication, token refresh, endpoint behavior, and secret-safe errors. `clanker-analytics` only adapts their normalized output into the snapshot schema.

Reading an unchanged cache every five minutes is not fresh collection. A cache-only mode may be useful for offline inspection, but it must preserve the provider's effective timestamp and must not pretend the collector read time is a new measurement.

Schedulers can invoke the one-shot command. On Linux, installation may offer a systemd user timer. Other platforms can use their native schedulers. Overlap should be prevented by a short recorder lock or SQLite transaction, and repeated samples should be idempotent.

`--quota-daemon` is deferred. A daemon should only be added if native schedulers prove inadequate.

Collection should begin before token-schema and pricing work so Claude and Antigravity history accumulates immediately.

## Historical backfill

Codex session logs retain provider rate-limit snapshots inside `token_count` events. Those records can backfill historical Codex quota samples and avoid a warm-up period.

The local corpus inspected during this proposal contained rate-limit data in 1,021 Codex session files, including hundreds of thousands of recent observations. The backfill must deduplicate identical account-wide snapshots emitted into concurrent session files and must not assign an account identity it cannot support.

Claude and Antigravity do not currently retain an equivalent complete history in their normalized caches. Their recent pace becomes available after scheduled recording accumulates enough samples. System journal output may help a one-off local investigation, but it is not a portable product data source.

The implemented audit can use this backfill directly:

```text
clanker-analytics --quota-history
clanker-analytics --quota-history --quota-json
```

It groups embedded 7-day observations by advertised reset, rounding reset timestamps to a
five-minute anchor because concurrent Codex sessions can differ by a few seconds. It selects the
chronologically last nonzero observation in each period and calculates accumulated local native
Codex tokens and API-equivalent cost through that point. Forked sessions require special handling:
Codex can replay thousands of inherited `token_count` events into a new JSONL file at startup. Those
startup bursts must be excluded or each fork counts inherited usage again.

Timestamp extraction must preserve the original RFC 3339 value. Letting the JSON reader infer a
timezone-less timestamp can shift Codex activity by the host UTC offset when the report later casts
it to `TIMESTAMPTZ`, moving usage into or out of a quota period.

The report includes `full_quota_api_equivalent_cost_estimate_usd`, calculated as accumulated local
API-equivalent cost divided by the last observed utilization fraction. It also exposes model mix,
cache share, observation count, pricing quality, and change from the previous estimate. Estimates
below 25 percent are labeled `early`; a last observation below the period maximum is labeled
`inconsistent`.

Historical comparison uses one named, immutable API-equivalent rate card for every period. Using
the price card that happened to be current during each week would mix API price changes into the
quota-capacity signal. Unknown models and non-default rate modes remain visible as quality warnings.
This still cannot prove a provider quota change: local logs can be incomplete, internal quota
weights are undisclosed, and Codex logs do not expose a stable account key for partitioning users
who switched accounts.

The token Parquet cache retains the extracted quota fields. A schema upgrade requires one full
backfill, then normal runs update only changed source files and aggregate all periods in one DuckDB
query. A separate historical quota database is unnecessary for this Codex-only audit.

## Versioned JSON contract

The JSON must contain all lookbacks needed by consumers. A headline-only object cannot reproduce the terminal table without recalculating it.

Example with synthetic values:

```json
{
  "schema_version": 1,
  "generated_at": "2026-08-20T18:00:00Z",
  "buckets": [
    {
      "provider": "openai",
      "bucket_id": "weekly_all",
      "scope_type": "account",
      "window_kind": "fixed",
      "used_pct": 72.0,
      "effective_at": "2026-08-20T18:00:00Z",
      "resets_at": "2026-08-22T01:00:00Z",
      "target_percentage_points_per_hour": 0.903,
      "headline_lookback_seconds": 21600,
      "lookbacks": [
        {
          "requested_seconds": 21600,
          "elapsed_seconds": 21480,
          "from": "2026-08-20T12:02:00Z",
          "to": "2026-08-20T18:00:00Z",
          "net_percentage_points_per_hour": 1.093,
          "pace_ratio": 1.21,
          "uncapped_projected_used_pct": 105.9,
          "estimated_exhaustion_at": "2026-08-21T19:37:00Z",
          "hours_before_reset": 5.38,
          "exhausts_before_reset": true,
          "required_net_reduction_pct": 17.4,
          "headroom_pct": null,
          "quality": "full",
          "local_metered_usage": null
        }
      ],
      "source": {
        "status": "live",
        "collected_at": "2026-08-20T18:00:02Z"
      }
    }
  ]
}
```

Important contract rules:

- `required_net_reduction_pct` and `headroom_pct` are separate nullable fields.
- A rolling or unknown bucket sets target, projection, exhaustion, reduction, and headroom fields to null.
- Unavailable values are null, not zero or sentinel numbers.
- `account_key` remains local and is omitted from publishable JSON. A local diagnostic JSON mode may include it when explicitly requested.
- Consumers display supplied values and quality states without repeating forecast formulas.

## Optional local metered usage

Local token metering is an explanatory enhancement after quota pacing works.

Raw tokens are misleading across token types and models. Cached input, uncached input, and output have different rates, and model rates differ. When available, each lookback may include:

```json
{
  "unit": "codex_credit",
  "rate_per_hour": 6.9,
  "pricing_basis": "openai-codex-credit-v3",
  "coverage": "partial",
  "status": "estimated"
}
```

Codex should use official Codex token credits when the account uses the token-based rate card. API-billed tools should use recorded cost when available. Claude subscription usage may show API-equivalent dollars as a proxy, but not as an exact quota unit. AOP should use retained API-equivalent cost when present rather than repricing an unknown routed provider.

As of April 2026, most Codex plans use model-specific credits for input, cached input, and output tokens. Fast mode can use different rates, and a small set of enterprise accounts can still use a legacy rate card. The current `COST_PER_ROW` expression is therefore not suitable as a Codex quota meter because it hardcodes one older GPT-5 API price and the current Codex loader leaves `model` empty.

Before local Codex credits are exposed, token events should be associated with active model and rate mode. Rate-card identity should include the metering profile and effective date and should refer to an immutable checked-in mapping. Unknown models, modes, or account profiles should remain unpriced.

This work improves explanation and correlation research. It does not block authoritative quota pace.

## CLI surface

The exact option names may follow the existing flat CLI. The initial behavior should be equivalent to:

```text
clanker-analytics --record-quota
clanker-analytics pace
clanker-analytics pace 3h 7h
clanker-analytics pace --quota-json
```

`--record-quota` performs one fresh collection transaction. `pace` reads retained snapshots without requiring a network call. Positional periods replace the window-aware defaults. Users or schedulers can run recording and reporting in sequence when a fresh report is wanted. The older `--pace` and repeated `--lookback` options remain compatibility aliases.

## Website integration

The website should eventually stop calculating pacing in `meters.js` and `status.html`. It should render the versioned JSON produced by `clanker-analytics`.

The compact homepage meter can show:

```text
codex   72%   pace 1.2x   exhausts 5h early
```

The detailed status page can show the fixed-window headline and lookbacks. Rolling buckets should show net trend and recovery state without an adjustment recommendation.

`Burn` should be removed or renamed `Pressure` if the old time-to-budget ratio is retained for comparison.

Only derived quota summaries should be published. Account fingerprints, project names, session identifiers, token events, source paths, and raw quota history remain local.

## Edge cases

The implementation should define these outcomes explicitly:

- Zero utilization and zero recent pace: show `unused`, with no headroom recommendation.
- Fixed quota already at 100 percent: show `exhausted`.
- Missing or expired fixed reset time: do not forecast.
- Stale current measurement: show age and omit a current recommendation.
- No qualifying historical sample: show `collecting history`.
- Provider quantization hides the observed delta: mark `quantized` and omit headroom.
- Negative rolling trend: show `recovering` without an exhaustion forecast.
- Negative fixed-window delta: mark the interval inconsistent unless a reset or correction is identified.
- Partial history: label the actual interval and exclude it from the headline.
- Concurrent Codex sessions emit identical snapshots: insert one normalized row.
- Account identity changes: never join the new snapshot to the previous account's segment.
- Unknown window semantics: show trend only.
- Unknown model or rate mode: omit optional local metering.
- Local usage disagrees with provider utilization: keep provider pace authoritative and label local coverage partial.

## Validation plan

Validation has two separate questions.

### Forecast validation

For completed fixed reset periods, backtest forecasts made at retained observation times and report:

- Absolute error in projected end utilization for each lookback.
- False-positive exhaustion forecasts.
- False-negative exhaustion forecasts.
- Error in estimated exhaustion time when exhaustion actually occurs.
- Results under sparse and quantized sampling.
- Results by provider, bucket, and window kind.
- Stability and error of the proposed headline lookback.

Comparing a collected snapshot with the provider display validates ingestion. It does not validate whether a six-hour linear forecast is useful.

Rolling and unknown windows should be evaluated as trend reports, not against fixed-window exhaustion metrics.

### Local metering correlation

Codex history contains both token events and quota snapshots, enabling a later counterfactual:

1. Reconstruct token deltas, active models, and observable rate modes.
2. Reconstruct authoritative utilization changes without crossing fixed resets.
3. Compare raw tokens, the existing API-cost estimate, and official Codex credits as predictors.
4. Segment by model, cache ratio, rate mode, and apparent external activity.
5. Require stability across multiple periods before describing any local unit as quota-proportional.

This is discovery evidence. It is not production forecast validation. If no local unit remains proportional, local metering stays descriptive while provider pace remains useful.

## Test strategy

Unit tests should cover:

- Fixed target rate, pace ratio, projection, exhaustion, and adjustment formulas.
- The difference between a 25 percent excess rate and a 20 percent required reduction.
- Separate reduction and headroom fields.
- Fixed-cycle reset segmentation.
- Rolling decreases that do not create false reset segments.
- Negative fixed deltas becoming inconsistent.
- Deterministic sample selection and actual elapsed duration.
- Full, partial, quantized, stale, inconsistent, and insufficient quality states.
- Percentage-used and percentage-remaining normalization.
- Duplicate and out-of-order snapshots.
- Account separation.
- Stale, malformed, missing, non-finite, zero, and 100 percent values.
- JSON null behavior and omission of account fingerprints from publishable output.
- SQLite uniqueness and interrupted transaction recovery.

Integration tests should use synthetic provider and session fixtures. Live account values must not enter committed fixtures, logs, screenshots, or public benchmark output.

## Rollout

### Phase 1: record snapshots immediately

- Add the minimal SQLite schema and one-shot recorder.
- Implement adapters for current Claude, Codex, and Antigravity usage output.
- Schedule local recording every five minutes with a systemd user timer.
- Begin accumulating history before other analytics work.

### Phase 2: fixed-window pace

- Add evidence-based window-kind classification to adapters.
- Implement fixed segmentation, deterministic lookbacks, exhaustion time, and versioned JSON.
- Show rolling and unknown buckets as net trends only.
- Backfill Codex snapshots where account and bucket identity can be supported.

### Phase 3: local metering

- Preserve full timestamps in normalized token rows.
- Associate Codex token events with active model and rate mode.
- Add immutable, effective-dated metering mappings.
- Populate optional local usage rates without changing forecast authority.

### Phase 4: shadow backtesting

- Score forecast error and exhaustion classification over completed fixed periods.
- Evaluate whether the 6-hour weekly headline is stable and useful.
- Run local metering correlation as a separate discovery analysis.
- Change only the layer identified by the evidence.

### Phase 5: website presentation

- Publish only derived, privacy-safe pace JSON.
- Replace browser-side burn calculations with supplied fixed forecasts and rolling trends.
- Keep terminal and website output on the same calculation contract.

Rolling-window workload recommendations remain deferred until gross consumption and expiration are separately observable.

## Alternatives considered

### Average fixed-cycle pace from one snapshot

When fixed `window_seconds` is trustworthy, average pace since the inferred cycle start requires no history. It reacts slowly to recent behavior and should be a clearly labeled fallback, not the main signal.

### Local weighted tokens only

This reacts immediately and provides attribution, but it misses other clients and shared product surfaces. It cannot be the account-wide authority.

### Browser-local history

Local storage only accumulates while a particular browser visits the page and makes the browser responsible for analytics semantics. It is not a reusable source.

### JSONL snapshot storage

Append-only JSONL is simple with one enforced writer, deterministic locking, tail repair, and read-time deduplication. Once one-shot recording and historical backfill both require idempotency and ordered lookbacks, SQLite provides the smaller complete implementation.

### History in every usage package

Each provider monitor could append its own history, but that distributes storage, migration, and deduplication logic across several repositories. A central recorder is smaller while `clanker-analytics` is the only historical consumer.

### Long-running collector daemon

A daemon duplicates scheduling and lifecycle management already provided by systemd timers and native platform schedulers. It should be added only if those schedulers prove inadequate.

### Inferred dollar value for a full quota

Dividing local API-equivalent cost by current utilization assumes complete local coverage and exact price proportionality. It is excluded from forecasts and recommendations. The dogfood report may expose the same calculation as an experimental, `local_only` comparison so collected quota deltas can falsify the token-weighting hypothesis across lookbacks and model mixes.

## Acceptance criteria

The initial feature is ready for normal local use when:

- A one-shot command records fresh provider snapshots idempotently.
- Account, bucket, and scope changes cannot merge unrelated history.
- Confirmed fixed windows show full-quality lookbacks, exhaustion timing, and required account-wide net pace reduction.
- Rolling and unknown windows show net trends without workload recommendations.
- Every fixed-window calculation stays inside one reset segment.
- Requested and actual lookback durations are explicit.
- Repeated compact `--lookback` arguments such as `3h` and `7h` replace the defaults deterministically.
- Stale, partial, quantized, inconsistent, and insufficient data degrade clearly.
- JSON contains every lookback and all derived fields needed by consumers.
- Publishable JSON excludes opaque account identity and raw local activity.
- Local token pricing is absent or clearly optional and cannot alter forecast authority.
- Forecast backtests are available before the website promotes a default headline.

## Recommended first step

Implement the one-shot SQLite recorder and schedule it immediately. This starts accumulating Claude and Antigravity evidence while fixed-window classification and pace calculations are built. Then ship fixed-window pacing without waiting for token-schema, model-attribution, or rate-card work.
