# WS217 Claude E2E rebase plan

Intent: Mechanically reconcile PR #217 with current `origin/main` and its advanced stacked base while retaining the recorded Claude Agent end-to-end coverage. Keep current production semantics unchanged; stale test expectations will assert the exact production watchdog message rather than altering or weakening production behavior.

Blast radius: `ProviderRuntimeIngestion.test.ts` and its recorded Claude fixture are the intended PR surface; the stacked base's provider-output guard tests/implementation, session reaper, child coordinator, runtime-event projection/persistence readers, replay handling, and adapter lifecycle are merge neighbors that must remain semantically identical to their current base/main versions.

Edge-case matrix (existing tests are the red-first guards for this mechanical merge):

| Edge                   | Ordering / condition                                                             | Named guard                                                                                    |
| ---------------------- | -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Normal ordering        | Claude delta(s) precede the terminal result                                      | `replays a real Claude Agent stream through the adapter and persistent turn pipeline`          |
| Reverse/empty ordering | Terminal success arrives without assistant delta or message                      | `fails a recorded Claude Agent turn explicitly when the assistant response is empty`           |
| Crash between steps    | First visible delta is followed by provider death before terminal events         | `lets the watchdog fail a recorded Claude Agent turn after stream death loses terminal events` |
| Duplicate/replay       | Persisted/runtime events are replayed without double projection                  | existing `ProviderRuntimeIngestion` replay/idempotency coverage in the touched suite           |
| Same-timestamp tie     | Event identity/order, not timestamp, determines projection                       | existing `ProviderRuntimeIngestion` ordering coverage in the touched suite                     |
| Version skew           | Claude 2.1.207 fixture is decoded by the current adapter/contracts               | the recorded-stream success guard above                                                        |
| Concurrency            | Parent late-output/steer tests and Claude turn tests coexist in one merged suite | current stacked-base late-output tests plus the three Claude guards above                      |

Smallest change: merge the two current ancestry lines, resolve the single test-file overlap by retaining both test sets, remove the stale reaper production normalization from this test-only child, and make only exact test expectation/import adjustments required by current production constants or literals.
