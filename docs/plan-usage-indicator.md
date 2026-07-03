# Plan Usage Indicator

## Research

CodexBar reads Codex credentials from `~/.codex/auth.json`, using `tokens.access_token` and the optional `tokens.account_id`. On this VPS, `https://chatgpt.com/backend-api/wham/usage` returned Codex Pro usage with a five-hour `rate_limit.primary_window`, a weekly `rate_limit.secondary_window`, and one `additional_rate_limits` entry for `GPT-5.3-Codex-Spark`. The Spark entry is intentionally ignored for T3.

CodexBar also contains support for `https://chatgpt.com/api/codex/usage` when the configured ChatGPT base URL is not the web `backend-api` base. On this host that endpoint variant returned 403, so T3 uses the verified `backend-api/wham/usage` path.

Claude usage comes from `https://api.anthropic.com/api/oauth/usage` with the Claude Code OAuth access token from `~/.claude/.credentials.json`, `anthropic-beta: oauth-2025-04-20`, and a Claude Code user agent. This VPS has a Max plan credential (`subscriptionType=max`, `rateLimitTier=default_claude_max_20x`). The source of truth is the dynamic `limits[]` array, not the legacy top-level fields such as `seven_day_opus` or `seven_day_sonnet`. Live data reported `session`, `weekly_all`, and `weekly_scoped` for `scope.model.display_name=Fable`, with `percent`, `resets_at`, `is_active`, and `severity`. T3 renders every valid `limits[]` entry generically, including scoped model labels, so the Fable-only weekly bar disappears naturally when Anthropic stops reporting that limit.

CodexBar has a `wham/rate-limit-reset-credits` endpoint for reset-credit availability, but not a clearly safe rate-limit-reset mutation endpoint. T3 does not implement a reset button in this PR. Follow-up: ADA-14.

## Design

Usage is server-owned per backend and per selected provider instance. The web client asks the active thread's environment backend for `/api/plan-usage?providerInstanceId=...`; that backend resolves the selected provider instance from `ServerSettings.providerInstances`, reads only that instance's local credentials, and returns normalized usage windows. This is required for desktop clients that can have local threads using local logins and remote threads routed to a VPS backend with different logins.

Codex instances read `auth.json` from `shadowHomePath` when configured, otherwise `homePath`, otherwise `CODEX_HOME`/`~/.codex`. Claude instances read `.claude/.credentials.json` under their configured `homePath`, otherwise the backend process `HOME`. Unsupported or disabled provider instances return an empty provider list so the UI hides the indicator instead of showing stale data from another account.

The endpoint degrades to an empty provider list when credentials are missing or the upstream endpoint fails. Upstream requests have a 10 second timeout so one provider cannot hang the composer. Non-empty results are cached server-side for 60 seconds per resolved credential scope, with a small bounded cache, and HTTP-cached privately for 30 seconds.

The payload contains providers and dynamic windows. Codex currently emits `Codex 5h` and `Codex weekly`. Claude emits one window per `limits[]` entry, labeled from `kind` plus `scope.model.display_name` when present, and carries Anthropic `severity` for coloring. The composer indicator displays the single highest `usedPercent` across all reported windows, and the hover popover shows every reported bar with reset timing.

## OAuth Persistence And Handoff

Credential reuse is implemented now: each backend reads the Codex and Claude credential files already present on that host, scoped by provider instance as described above.

Codex credentials are refreshed on a 401 from the usage endpoint using `https://auth.openai.com/oauth/token` with the same OAuth client id and refresh-token grant observed in CodexBar. T3 preserves the existing `auth.json` shape, rereads the file before writing so a concurrent login/logout or refresh is not clobbered, writes refreshed tokens back with private file permissions, and retries the usage request once.

Claude credentials are reread whenever the server refreshes its provider snapshot. If Anthropic returns 401, T3 rereads the Claude credentials file and retries once, piggybacking on Claude Code's own refresh/persistence rather than implementing a separate refresh-token flow.

For enterprise/work contexts, the intended handoff is an explicit local-to-remote credential enrollment flow: the local desktop reads provider OAuth credentials after user confirmation, sends them over an authenticated T3 environment connection to the target backend, and the backend persists them in a provider-scoped secret store with private file permissions or OS keychain support. The backend should expose credential health without ever returning tokens to the client. DPoP-authenticated remote environments need this to live behind the generated environment HTTP client or a shared request-signing helper rather than ad hoc `fetch`.
