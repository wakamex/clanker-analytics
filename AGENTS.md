# Usage history

- Never delete the user's usage history. Tools prune their own logs and AOP runs get removed, so the token cache at `~/.cache/clanker-analytics/tokens.parquet` can hold the only remaining record of past usage.
- Before anything that rebuilds or rewrites the whole cache, such as a `CACHE_SCHEMA_VERSION` bump, `--refresh`, or removing cache files, copy `tokens.parquet` and `tokens-meta.json` to `~/.local/share/clanker-analytics/backups/` with the date in each file name, and confirm the copies exist. Never delete or overwrite those backups.
- Never delete, truncate, or move source logs such as `~/.claude/projects`, `~/.codex/sessions`, `~/.codex/archived_sessions` and `.aop/runs`, or the quota database at `~/.local/state/clanker-analytics/quota.sqlite3`. Never shorten a tool's log retention; Claude Code's `cleanupPeriodDays` is set to 36500.
- Code changes must not drop rows whose source file no longer exists. Updates and rebuilds keep the cached rows of every source file a scan does not find, through `_carry_over_vanished_rows` and the file list in `tokens-meta.json`; keep a test that a deleted file's rows survive an update, `--refresh`, and a schema bump.
- AOP results are discovered relative to the working directory, so a run outside `/code` finds none. Their rows are kept, but new AOP runs appear only after a run from inside `/code`.
