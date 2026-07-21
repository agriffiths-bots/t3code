# PR #214 tests-only split

## Intent

Remove the provider output-preservation guard from PR #214 so `ProviderService.ts` is byte-identical to `origin/main`, while retaining the recorded Codex E2E harness and every desired guard regression as executable coverage or an explicit `TODO(ADA-192)` skip. Empirically classify tests against main production behavior without weakening their assertions.

## Blast radius

- Restore `apps/server/src/provider/Layers/ProviderService.ts` exactly from `origin/main`; this preserves main semantics for event correlation, session persistence/restart, adapter replacement, Cursor ordering, MCP cleanup, and runtime fanout.
- Retain and exercise `apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.test.ts`, `apps/server/src/provider/Layers/ProviderService.test.ts`, and `apps/server/src/provider/Layers/fixtures/replay-codex-app-server-turn.mjs`.
- Remove `provider-turn-output-guard*.plan.md` and this temporary split plan from the final branch.
- No contracts, persistence schemas, projections, clients, migrations, runtime services, or production consumers change.

## Edge-case matrix and red-first proof

| Case                                                                             | Named proof against restored main                                                                                          | Expected classification                                                               |
| -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Recorded success emits ordered assistant deltas before completion                | `replays a live Codex PONG stream through projection and releases the child lease`                                         | Keep if green; this is the core E2E value.                                            |
| Successful completion has no meaningful output                                   | `fails a recorded Codex turn explicitly when the assistant response is empty` and ProviderService empty/control-only cases | Expected guard-dependent; preserve assertion and skip with `TODO(ADA-192)` if red.    |
| Process dies between output and a terminal event                                 | `lets the watchdog fail a recorded Codex turn after process death loses its terminal event`                                | Keep if main watchdog behavior is independently green.                                |
| Duplicate/replayed starts or completions                                         | ProviderService duplicate/stale lifecycle cases                                                                            | Keep only when green on main; otherwise skip unchanged for ADA-192.                   |
| Output and completion arrive in either order, including Cursor grace drain       | ProviderService Cursor late-delta/session-exit cases                                                                       | Keep only when green on main; otherwise skip unchanged for ADA-192.                   |
| Restart seeds an already-active turn; crash occurs between output and completion | ProviderService recovered-active-turn/restart cases                                                                        | Keep only when green on main; otherwise skip unchanged for ADA-192.                   |
| Same thread/turn is reused across adapter generations or instances               | ProviderService generation/instance ownership cases                                                                        | Keep only when green on main; otherwise skip unchanged for ADA-192.                   |
| Same-timestamp/runtime event ties and version skew after the main sync           | Run both affected suites after exact production restore                                                                    | No production compatibility delta; failures are classified, never assertion-weakened. |
| Concurrent session removal/adapter replacement drains pending events             | ProviderService removal/replacement cases                                                                                  | Keep only when green on main; otherwise skip unchanged for ADA-192.                   |

## Smallest change

An exact path restore is safer than manually unwinding four guard rounds. After that restore, only failing guard-dependent tests receive a one-line skip plus the required ADA-192 marker; passing test and fixture coverage stays unchanged.
