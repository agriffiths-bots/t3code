# WS217 post-ADA-192 forward-merge plan

Intent: Rebase PR #217 onto `origin/main` at or beyond `ad89d61ec` and retain its recorded Claude Agent turn E2E coverage against the landed ADA-192 output-preservation contract. Keep this PR test-only: conflicts may align test harnesses and assertions, but production files must remain byte-identical to `origin/main`.

Blast radius: the intended delta is `ProviderRuntimeIngestion.test.ts`, its recorded Claude JSONL fixture, and planning documentation. Merge neighbors are `ProviderService` output tracking, `ProviderSessionReaper` ownership/cleanup, provider failure messages, runtime-event projection/persistence, adapter lifetime, and the already-landed Codex recorded-turn harness; their production behavior must come solely from `origin/main`.

| Edge                   | Ordering / condition                                                                     | Red-first guard retained or run                                                                      |
| ---------------------- | ---------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Normal ordering        | Claude deltas precede successful terminal completion                                     | `replays a real Claude Agent stream through the adapter and persistent turn pipeline`                |
| Reverse / empty        | Successful terminal event arrives with no meaningful assistant output                    | `fails a recorded Claude Agent turn explicitly when the assistant response is empty`                 |
| Crash between steps    | Visible delta is followed by stream death with terminal events hidden                    | `lets the watchdog fail a recorded Claude Agent turn after stream death loses terminal events`       |
| Duplicate / replay     | A repeated start or terminal lifecycle cannot erase accepted output                      | `does not let duplicate turn.started events clear active turn output` plus the ProviderService suite |
| Same-time / late order | Ownership, not timestamp proximity, fences stale lifecycle events                        | `does not rewrite a stale completion after ownership advances` plus the ProviderService suite        |
| Restart / version skew | Landed output tracking survives service rebuild while Claude 2.1.207 JSONL still decodes | `preserves a recovered active turn completion after service restart` and the Claude success E2E      |
| Concurrency            | Provider completion and watchdog observation settle one terminal state                   | both Claude success and stream-death E2Es, with the full focused ingestion suite                     |

Smallest change: replay the existing test commits onto current `origin/main`, take current-main production code for every conflict, and adapt only the Claude test harness/assertions required by the landed interfaces and constants. If an active Claude guard exposes a production defect or an ambiguous contract, stop under the decision tripwire rather than expanding this PR.
