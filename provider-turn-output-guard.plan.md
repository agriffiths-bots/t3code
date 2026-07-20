# Provider turn output guard review-fix plan

## Intent

Preserve the empty-success safeguard added by PR #214 while preventing it from
rewriting a successful turn that produced legitimate non-text work. Repair the
recorded Codex process-death test double so the watchdog sees the same vanished
session through both its event stream and live ownership snapshot.

## Blast radius and adapter audit

The guard is in `ProviderService`, after every adapter event is canonicalized,
so its behavior applies to every registered provider instance and not only
Codex.

- Codex can finish after `turn.proposed.delta` / `turn.proposed.completed`,
  `turn.plan.updated`, `turn.diff.updated`, reasoning, command, or file-change
  `content.delta`, and structured assistant, reasoning, plan, or tool lifecycle
  items (`command_execution`, `file_change`, MCP, dynamic, collab, web search,
  image view). It can also emit request, user-input, and tool-progress events
  during real work.
- Claude Agent can finish after assistant or reasoning content, structured
  assistant/tool lifecycle items and tool results, `turn.proposed.completed`,
  `turn.plan.updated`, task lifecycle, hook lifecycle, tool progress/summary,
  requests/user input, or persisted-file evidence.
- Grok can finish after ACP assistant content/items, `turn.plan.updated`, or ACP
  tool lifecycle items; approval and user-input events are also turn-scoped.
- Cursor can finish after ACP assistant content/items, `turn.plan.updated`,
  `turn.proposed.completed` from `cursor/create_plan`, or ACP tool lifecycle
  items; approval and user-input events are also turn-scoped.

The guard must continue to ignore control/telemetry-only events such as
`turn.started`, thread/session state, token usage, account/auth/config notices,
runtime warnings/errors, and `turn.completed` itself. Otherwise ordinary usage
or lifecycle bookkeeping would make a genuinely empty success look non-empty.

## Edge-case and red-first matrix

Each regression assertion will emit exactly one meaningful-output shape before
a successful `turn.completed`; on the current code the completion must be
rewritten to `failed`, proving the test is red before the fix.

| Provider path | Only meaningful output emitted | Class proved                               |
| ------------- | ------------------------------ | ------------------------------------------ |
| Codex         | `turn.proposed.completed`      | final proposed-plan output (P1 exact case) |
| Codex         | reasoning `content.delta`      | reasoning-only output                      |
| Codex         | `turn.diff.updated`            | structured diff output                     |
| Codex         | tool `item.completed`          | tool call/result without prose             |
| Claude Agent  | `turn.proposed.completed`      | provider-specific proposed plan            |
| Claude Agent  | `task.completed`               | structured/background task work            |
| Claude Agent  | `hook.completed`               | hook-only work                             |
| Grok          | `turn.plan.updated`            | ACP plan-only work                         |
| Grok          | tool `item.completed`          | ACP tool-only work                         |
| Cursor        | `turn.proposed.completed`      | Cursor extension proposed plan             |
| Cursor        | tool `item.completed`          | ACP tool-only work                         |

Keep the existing negative test that a successful completion with no
meaningful event is converted to the explicit empty-response failure. Keep the
existing generation/session isolation tests to prove output state cannot leak
between turns or adapters.

For the watchdog finding, first run the recorded process-death test on the
current double and capture its timeout. Then wrap the live service consistently:
filter the disappeared session from `listSessions`, and route
`stopFailedSession` through the same wrapper so its `requireSessionAbsent`
ownership check observes absence. The test should then settle the running turn
without exposing `session.exited` to ingestion.

## Smallest-change argument

No schema, adapter, projection, or watchdog production change is needed. Add
one explicit canonical-event classifier beside the existing guard and reuse the
guard's current per-adapter/per-instance/per-thread/per-turn tracking. Change
only the recorded death test's service wrapper for P2. Tests stay in the two
files already changed by PR #214, plus this required plan document.

## Verification

1. Run the new ProviderService cases against the pre-fix guard and retain the
   expected failures as red-first proof.
2. Run the recorded Codex death scenario against the pre-fix double and retain
   its timeout as the P2 reproduction.
3. After the fixes, run the affected ProviderService and
   ProviderRuntimeIngestion Vitest files green, followed by focused formatting,
   lint, and type checks available for the changed server scope.
4. Commit every logical unit through the factory pre-commit gate and push the
   existing `wizzo/provider-turn-e2e` branch only.
