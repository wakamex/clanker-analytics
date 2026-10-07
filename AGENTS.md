# Usage history

- Never delete the user's usage history. Tools prune their own logs and AOP runs get removed, so the token cache at `~/.cache/clanker-analytics/tokens.parquet` can hold the only remaining record of past usage.
- Before anything that rebuilds or rewrites the whole cache, such as a `CACHE_SCHEMA_VERSION` bump, `--refresh`, or removing cache files, copy `tokens.parquet` and `tokens-meta.json` to `~/.local/share/clanker-analytics/backups/` with the date in each file name, and confirm the copies exist. Never delete or overwrite those backups.
- Never delete, truncate, or move source logs such as `~/.claude/projects`, `~/.codex/sessions`, `~/.codex/archived_sessions` and `.aop/runs`, or the quota database at `~/.local/state/clanker-analytics/quota.sqlite3`. Never shorten a tool's log retention; Claude Code's `cleanupPeriodDays` is set to 36500.
- Code changes must not drop rows whose source file no longer exists. The cache currently deletes the rows of a source file that disappears, and a rebuild reads only the files still on disk, so the backups are the only protection until the cache keeps rows for vanished sources.
