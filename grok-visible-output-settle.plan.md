# Grok visible-output settlement plan

## Intent and invariant

Fix the Grok ACP adapter so visible-output bookkeeping remains attached to a
shared turn until that turn reaches its terminal settlement. A prompt-level
steer/supersession event may decrement or discard one prompt, but it must not
clear the turn-level `turnsWithVisibleOutput` entry while another prompt still
owns the same active turn. A turn that surfaced assistant content, a plan, a
tool call, or a user interaction must therefore never be classified as the
adapter's synthetic empty-output failure solely because of steer ordering.

This is a correctness fix only. Grok's quarantine/enabled state, provider
protocols, contracts, projections, and client behavior remain unchanged.

## Blast radius and code-path inventory

Primary owner boundary:

- `apps/server/src/provider/Layers/GrokAdapter.ts`
  - Producers of the turn flag: distinct `PlanUpdated`, Grok user-input and
    approval callbacks, `ToolCallUpdated`, and non-blank `ContentDelta`.
  - Readers: non-live-context completion payload construction, the late-output
    grace predicate, and terminal `settleTurnId` completion payload
    construction.
  - Delete sites to audit:
    1. non-live-context prompt settlement (current line 386);
    2. `settleAllPrompts` with no live fallback turn (current line 405);
    3. terminal settlement with `emitTurnCompletion: false` (current line 453);
    4. normal failed/completed/cancelled terminal settlement (current line
       479).
  - Steer/settle callers: preparation interruption/failure, stale ACP-session
    failure, normal prompt return, ensuring/failure cleanup, and
    `interruptTurn`'s settle-all path.
- `apps/server/src/provider/Layers/GrokAdapter.test.ts`: add the focused
  regression that drives two prompts sharing one turn, observes visible
  assistant output before the superseded prompt settles, and asserts the final
  completion is `completed`, not the synthetic empty-output failure.
- `apps/server/scripts/acp-mock-agent.ts`: change only if deterministic prompt
  ordering cannot be expressed with its existing first/second delay and output
  controls. Any addition will be test-only and narrowly scoped to the ordering.

Class sweep, no expected production edits:

- `CursorAdapter.ts` has its own prompt counter and visible-output set. Its
  non-terminal steer branches retain the flag; deletes occur only on emitted
  cancellation, final prompt completion, or cancellation of a resumed active
  turn. It does not call Grok's settlement helper.
- `ClaudeAdapter.ts` and `OpenCodeAdapter.ts` reuse an active turn for steering
  but own separate state machines and do not read or write
  `turnsWithVisibleOutput`.
- `CodexAdapter.ts` does not share the Grok/Cursor prompt-counter settlement
  machinery. No cross-adapter utility or contract is involved.

Expected production blast radius is one local Grok settlement guard/delete
placement. If the proof requires a protocol change or cross-layer refactor,
stop as blocked instead of expanding the diff.

## Edge-case and proof matrix

| Ordering                                                                                                 | Required result                                                                               | Proof                                                                                   |
| -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| First prompt emits visible output; steer joins the same turn; superseded prompt settles before the steer | No early flag deletion; one final `completed` payload on the shared turn                      | New red-first integration regression                                                    |
| Steer settles before the older prompt                                                                    | Earlier settlement leaves the turn running and preserves the flag; last prompt completes once | Exercise if the same deterministic harness can cover it without broadening              |
| Steer preparation/finalizer fails while an older prompt still owns the turn                              | Prompt count decrements only; existing turn flag survives                                     | Audit existing early-return path; add a regression only if the touched guard changes it |
| Non-live/stale settlement                                                                                | Delete only if that exact turn is terminal and cannot still be the live shared turn           | Audit delete site 1 and cover if changed                                                |
| `settleAllPrompts` target differs from the live fallback                                                 | Never delete the fallback turn's flag before fallback completion                              | Audit delete site 2 and cover the changed ordering                                      |
| Terminal settlement with completion suppressed                                                           | Flag may be deleted after the turn is made ready                                              | Audit delete site 3                                                                     |
| Normal completed/failed/cancelled terminal settlement                                                    | Snapshot `hasVisibleOutput`, emit at most one terminal payload, then delete                   | Existing tests plus new shared-turn regression; audit delete site 4                     |
| Late visible update during grace                                                                         | Drain before snapshot; preserve current interrupt precedence                                  | Existing focused regressions                                                            |

## Red-first and implementation sequence

1. Add the smallest deterministic shared-turn test using the real Grok adapter
   and mock ACP process. Assert both sends return the same `turnId`, visible
   output precedes terminal settlement, exactly one terminal event is emitted,
   and its payload state is `completed`.
2. Run only that named test against current code and retain the failing output
   as proof. The expected red is `failed` with the Grok empty-output message.
3. Move or guard only the premature `turnsWithVisibleOutput.delete(...)` that
   the red ordering reaches. Do not alter prompt IDs, protocols, session state,
   quarantine, or shared runtime machinery.
4. Re-run the named regression, the adjacent Grok late-output/interrupt/steer
   settlement tests, targeted formatting/lint/type checks for the touched
   server scope, and the full focused `GrokAdapter.test.ts` file if practical.
5. Commit the red regression separately from the minimal fix when the factory
   gate permits the intentionally red proof commit; otherwise preserve the red
   command/output in the audit memo and land test plus fix as the next small
   gated commit. Every commit uses the normal pre-commit gate.

## Smallest-change argument

`turnsWithVisibleOutput` is already the correct turn-scoped data structure and
the terminal path already snapshots it before deletion. The defect is lifetime
ordering, not missing state or protocol data. Constraining deletion to a proven
terminal turn fixes the invariant at its owner boundary and avoids new maps,
IDs, cross-layer APIs, or adapter-wide refactors.
