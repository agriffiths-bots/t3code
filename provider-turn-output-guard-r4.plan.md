# Provider turn output guard round-4 plan

## Intent

Converge PR #214's empty-response guard by requiring positive ownership and
positive absence before a successful completion can be rewritten or its output
tracking cleared. Preserve the six mapped legitimate-output paths without
changing provider contracts, projections, persistence schemas, or watchdog
production behavior.

## Blast radius and adapter/output audit

- `apps/server/src/provider/Layers/ProviderService.ts` owns canonical event
  output tracking, terminal ownership checks, Cursor's late-delta grace,
  persisted active-turn recovery, and publication to ingestion. Its consumers
  are the orchestration ingestion/projections, session directory restart path,
  metrics/logger fan-out, and every registered adapter instance.
- Codex legitimate non-text output includes proposed-plan delta/completion,
  plan/diff updates, reasoning/command/file deltas, completed
  assistant/reasoning/plan items with detail, structured tool items, realtime
  audio, and tool/task/file summary lifecycle events.
- Claude Agent includes proposed plans, task plans, assistant/reasoning content,
  completed tool/task/hook work, summaries, denied tools, and persisted files.
- Grok includes ACP assistant content/items, plan updates, and completed ACP
  tool items. Cursor includes the same ACP shapes plus `cursor/create_plan`, and
  uniquely may deliver assistant content after `turn.completed`.
- `apps/server/src/provider/Layers/ProviderService.test.ts` is the focused owner
  for guard ordering, ownership, adapter-shape, duplicate/replay, and restart
  regressions. `apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.test.ts`
  owns the real recorded-Codex watchdog/process-death proof; its double must
  expose absence consistently through stream, live snapshot, and stop paths.

## Edge-case and red-first matrix

| Named regression                                                                            | Ordering / failure represented                                        | Required result and revert mutation                                                                               |
| ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `preserves successful Codex proposed plan-only turns`                                       | proposed plan, then successful completion                             | completion remains successful; removing proposed-plan classification makes it red                                 |
| `holds Cursor completion decision through a late assistant delta`                           | completion, then delta within grace                                   | ordered successful completion plus delta; removing grace/buffering makes it red                                   |
| `preserves a recovered active turn completion after service restart`                        | output persisted before crash/rebuild, recovery, terminal-only resume | completion remains successful; removing recovered-turn seeding makes it red                                       |
| `does not rewrite a stale completion after ownership advances`                              | accepted new active turn, then old duplicate completion               | stale event is published unchanged and cannot clear owned output; removing terminal ownership gating makes it red |
| `does not let stale turn.started events replace the persisted active turn`                  | active output, stale start, active completion                         | active completion remains successful; clearing before accepted-start ownership makes it red                       |
| `lets the watchdog fail a recorded Codex turn after process death loses its terminal event` | process exit hidden from ingestion, absent snapshot, watchdog stop    | watchdog owns and fails the turn; delegating snapshot/stop to the original live session makes it red              |

Additional matrix coverage: duplicate same-turn starts preserve tracked output;
stale/duplicate completions and aborts never clear a foreign active bucket;
session exit remains the only whole-session clear; same-timestamp events remain
source-ordered rather than timestamp-ordered; adapter generations and provider
instances cannot share trackers; recovery marks a pre-existing active turn as
inconclusive even across runtime/version skew; concurrent Cursor ingress is
buffered through the fixed grace boundary; and genuinely output-free owned
completions still fail after all known delivery paths are closed.

## Smallest-change argument

Reuse the existing persisted-active-turn ownership read for every terminal
event, pass that fact into the guard, and seed the existing per-turn tracker
when ProviderService adopts/resumes a pre-existing active turn. This is a local
guard/recovery change plus focused assertions and the corrected death double;
no new state model, protocol, schema, projection, adapter API, or watchdog
machinery is required. If those local changes prove insufficient, stop and
recommend splitting the empty-response hardening from PR #214 as directed.

## Verification and delivery

Run the six named focused regressions red-first against their reverted guard or
test-double mutation, then green together with the existing empty-response and
adapter-shape matrix. Run targeted server formatting/lint/type checks, commit
through the factory gate, run Codex gpt-5.5 autoreview, push only
`wizzo/provider-turn-e2e`, reply to all mapped finding threads without resolving
them, write the dispatch memo and stabilization log, and leave PR state alone.
