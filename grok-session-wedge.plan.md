# Grok session wedge plan

## Intent

Keep each ACP adapter's notification consumer alive for the lifetime of its provider session, not merely for the lifetime of the caller that starts the session. Prove the production-shaped Grok failure first, then make the smallest ownership change and verify that a real Grok turn emits a first delta, settles, and no longer holds shutdown open.

## Proof and root cause

- Live thread `304c7858-7086-4032-bdbb-0106357f91f8` has no assistant message. Its persisted runtime row remained `running` with an active turn and `lastRuntimeEvent = turn.started`; watchdog errors only settled earlier attempts.
- Provider logs show successful ACP initialization/session creation and successful `session/prompt` dispatch. Grok continued sending ACP frames and permission requests, so neither startup nor the provider process was dead.
- An isolated current-main replay produced the same owned-but-unsettled state for turn `37fc880e-460e-4a42-ad74-82a0df471337`: `turn.started`, prompt-complete fallback, no content event or terminal turn.
- `GrokAdapter.ts:848-947` starts the ACP event-stream consumer with `Effect.forkChild`. Effect's contract at `.repos/effect-smol/packages/effect/src/Effect.ts:8426-8468` says that fiber is terminated when its parent terminates.
- `ProviderService.ts:464-487` awaits `adapter.startSession(...)` in a startup operation that then returns. Consequently, the consumer dies after startup; subsequent ACP updates and the `EventStreamBarrier` enqueued by `GrokAdapter.ts:1190` have no consumer. `sendTurn` has already emitted `turn.started`, then waits forever at the barrier and never emits a delta or terminal event.
- `CursorAdapter.ts:1144` repeats the same session-consumer ownership defect. It has a completion race that avoids the identical endless barrier wait, but its notifications are still scoped to the wrong caller. The class-wide fix must cover both ACP adapters.
- Stopping the isolated wedged replay took 5.17 seconds, exactly exhausting the harness grace period before group SIGKILL. This is treated as the same lifetime/unfinished-turn slice unless the fixed replay still exhausts teardown.

## Blast radius

- Direct: Grok and Cursor ACP notification delivery, drain barriers, turn settlement, and session stop.
- State: `provider_session_runtime` active operation ownership and projected `turn.started`/delta/completion events.
- Concurrency: a wedged Grok turn must not impede another provider thread (the existing ProviderCommandReactor bulkhead regression remains part of the focused gate).
- Lifecycle: normal start, start-caller exit, turn completion, explicit stop, server shutdown, and ACP child cleanup.
- Out of scope: quarantine flags, deployment/restart of the live server, protocol redesign, prompt timeouts, and unrelated provider behavior.

## Test matrix

| Edge                                      | Named proof                                                                                                        | Expected result                                                                                         |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| Start caller exits before first prompt    | `keeps the Grok notification consumer alive after the start caller exits`                                          | RED on current main by timing out after `turn.started`; GREEN with `content.delta` and `turn.completed` |
| Wedged provider alongside healthy work    | Existing `keeps turn starts flowing when one provider sendTurn never resolves` in `ProviderCommandReactor.test.ts` | Healthy thread keeps streaming/settling while the recorded Grok-shaped operation remains owned          |
| Prompt completion precedes queued updates | The new Grok test uses the ACP mock's prompt-complete/update sequence and awaits terminal delivery                 | Barrier drains only after preceding content is consumed                                                 |
| Session stop with a live consumer         | Existing Grok/Cursor ACP child cleanup tests plus isolated shutdown timing                                         | Consumer is interrupted by the session scope; child exits without the five-second SIGKILL fallback      |
| Real provider path                        | One isolated supervised `grok/grok-4.5` PONG after the fix                                                         | First delta observed, turnCount > 0, terminal completion observed                                       |

## Smallest change

Replace the two session-notification `forkChild` calls with `forkIn(sessionScope)`. Both adapters already create, retain, and close that scope in their session lifecycle, so no new timers, queues, state, or teardown mechanism is needed.
