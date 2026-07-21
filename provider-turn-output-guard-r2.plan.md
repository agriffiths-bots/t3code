# Provider turn output guard round-2 plan

## Intent

Keep PR #214's empty-success safeguard without letting event ordering or stale
lifecycle events turn real provider work into an empty-response failure. Repair
the recorded process-death double so every watchdog-facing operation observes
the same absent session. The fix remains local to `ProviderService`'s guard and
the ingestion test double.

## Blast radius and adapter audit

`ProviderService` applies the guard after canonicalization and before publishing
events for every adapter instance. The production adapters currently routed
through it are Codex, Claude Agent, Grok, Cursor, and OpenCode; the driver kind is
open, so a future registry adapter inherits the same ordering rules.

- Codex emits assistant/reasoning deltas, proposed plans, plan updates, diffs,
  completed assistant/reasoning/plan items, and structured tool/file/MCP/web
  items before its normal terminal event.
- Claude Agent emits assistant/reasoning content, proposed plans, plan updates,
  completed tool/task/hook work, summaries, and persisted-file evidence before
  completion.
- Grok emits ACP assistant content/items, plan updates, and completed structured
  tool work before completion.
- Cursor emits the ACP and proposed-plan shapes above, but can legitimately emit
  assistant deltas after `turn.completed`; terminal arrival is therefore not an
  output boundary for this adapter ordering.
- OpenCode emits assistant deltas/items before completion and is covered by the
  generic pre-completion output classifier even though PR #214's requested
  provider matrix names the other four adapters.

The tracker is isolated by adapter identity, provider instance, thread, and turn
id. No stale or duplicate lifecycle event may clear output belonging to the
live turn in that bucket.

## Edge-case and red-first matrix

| Shape / ordering                                                           | Required result                                                                       | Red-first proof on `fc169c521e`                                                                                                           |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Successful completion with no meaningful event                             | Rewrite to the explicit empty-response failure where completion is an output boundary | Keep the existing negative ProviderService test green throughout                                                                          |
| Assistant text before completion                                           | Preserve successful completion                                                        | Existing assistant-output coverage                                                                                                        |
| Reasoning-only output                                                      | Preserve successful completion                                                        | Existing Codex reasoning-only matrix case                                                                                                 |
| Proposed-plan delta/final or plan update only                              | Preserve successful completion                                                        | Existing Codex/Claude/Cursor/Grok plan matrix cases                                                                                       |
| Structured diff, tool, task, hook, file, or summary only                   | Preserve successful completion                                                        | Existing structured-output matrix cases across the adapters                                                                               |
| Successful completion followed by a Cursor assistant delta                 | Never publish the completion as empty-response failure                                | Add a ProviderService regression that currently observes `failed` before the late delta                                                   |
| Output, then stale `turn.started` for another turn, then active completion | Preserve the active turn's tracked output and successful completion                   | Extend the stale-start test with meaningful output; current session-wide clear makes it red                                               |
| Output, then duplicate `turn.started` for the active turn, then completion | Preserve the active turn's tracked output and successful completion                   | Add an idempotency regression; current session-wide clear makes it red                                                                    |
| Failed/cancelled completion or aborted turn                                | Preserve provider terminal state and clear only its own tracker                       | Retain existing terminal-state coverage                                                                                                   |
| Session exit, adapter replacement, distinct instance/thread/turn           | Never leak meaningful-output state across ownership boundaries                        | Retain existing isolation/cleanup coverage                                                                                                |
| Recorded process death with no terminal event                              | Watchdog snapshot and stop path both observe absence and settle the turn              | Assert the death wrapper handles `requireSessionAbsent` without delegating to the stale live service; current double delegates and is red |

For each new regression, first commit/run the test against the unchanged guard or
double and record the failing assertion before implementing the corresponding
fix.

## Smallest-change argument

No protocol, schema, adapter capability, projection, watchdog, or ingestion
production change is required. Encode the known post-completion ordering as a
small guard-local provider policy, leave those successful completions untouched,
and stop clearing meaningful-output state on `turn.started` (exact terminal,
abort, and session-exit events already own cleanup). In the death test only,
make the wrapper's absent-session stop operation invoke the watchdog callbacks
directly instead of delegating to a service whose underlying adapter still owns
the synthetic session. This changes only the guard, its focused tests, and the
existing death double.

## Verification and delivery

Run focused `ProviderService.test.ts` and the recorded-death
`ProviderRuntimeIngestion.test.ts` case, plus targeted formatting/lint/type checks
available for the server package. Commit each logical unit through the factory
pre-commit gate, run the final Codex gpt-5.5 autoreview/factory gate, push only
`wizzo/provider-turn-e2e`, and leave PR #214's state and threads untouched.
