import { ProjectId, ScheduledTaskEntry, ThreadId, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import {
  ScheduleBusyPolicy,
  ScheduledTaskId,
} from "../../../persistence/Services/ScheduledTasks.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ThreadStartToolError, ThreadStartToolInput, ThreadStartMode } from "../thread/tools.ts";

const dependencies = [McpInvocationContext.McpInvocationContext];

// Logical wait budget bounds (seconds) surfaced to `t3_wait_subagent`. The
// server clamps the requested timeout into this range; the per-invocation HTTP
// hold itself is bounded separately by WAIT_SLICE_SECONDS in the coordinator.
export const WAIT_TIMEOUT_DEFAULT_SECONDS = 600;
export const WAIT_TIMEOUT_MIN_SECONDS = 1;
export const WAIT_TIMEOUT_MAX_SECONDS = 3_900;

// Cumulative blocking budget cap for a single `t3_wait_subagent` (across
// resumeToken re-calls), regardless of the caller's timeoutSeconds (R-A). Once
// this elapses with children still running, the wait auto-promotes them to
// wake-on-completion and returns so the model stops polling.
export const WAIT_AUTO_PROMOTE_SECONDS = 90;

export const SpawnSubagentInput = Schema.Struct({
  ...ThreadStartToolInput.fields,
  detached: Schema.optional(Schema.Boolean),
  waitTimeoutSeconds: Schema.optional(Schema.Int),
});
export type SpawnSubagentInput = typeof SpawnSubagentInput.Type;

export const SpawnSubagentOutput = Schema.Struct({
  childThreadId: ThreadId,
  projectId: ProjectId,
  mode: ThreadStartMode,
  branch: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  parentThreadId: ThreadId,
  warning: Schema.optional(Schema.String),
  // Present only for a foreground (detached=false) spawn that ran to terminal
  // or exhausted its wait budget.
  status: Schema.optional(Schema.String),
  finalAssistantText: Schema.optional(Schema.NullOr(Schema.String)),
});
export type SpawnSubagentOutput = typeof SpawnSubagentOutput.Type;

export const SteerSubagentInput = Schema.Struct({
  childThreadId: ThreadId,
  message: Schema.String,
});
export type SteerSubagentInput = typeof SteerSubagentInput.Type;

export const SteerSubagentApplied = Schema.Literals([
  "now",
  "queued-midturn",
  "deferred-until-idle",
]);
export type SteerSubagentApplied = typeof SteerSubagentApplied.Type;

export const SteerSubagentOutput = Schema.Struct({
  childThreadId: ThreadId,
  accepted: Schema.Boolean,
  // How the steer was applied given the child's provider + turn state (R-C):
  // "now" (idle, dispatched), "queued-midturn" (a known driver —
  // claudeAgent/codex/cursor/grok/opencode, all support mid-turn steer — folded
  // into the running turn), or "deferred-until-idle" (an unknown/future driver
  // with unverified mid-turn semantics, persisted and dispatched once idle).
  applied: SteerSubagentApplied,
});
export type SteerSubagentOutput = typeof SteerSubagentOutput.Type;

export const CheckSubagentInput = Schema.Struct({
  childThreadId: ThreadId,
});
export type CheckSubagentInput = typeof CheckSubagentInput.Type;

export const CheckSubagentOutput = Schema.Struct({
  threadId: ThreadId,
  status: Schema.String,
  turnCount: Schema.Int,
  latestAssistantText: Schema.NullOr(Schema.String),
});
export type CheckSubagentOutput = typeof CheckSubagentOutput.Type;

export const WaitSubagentMode = Schema.Literals(["all", "any"]);
export type WaitSubagentMode = typeof WaitSubagentMode.Type;

export const WaitSubagentInput = Schema.Struct({
  childThreadIds: Schema.Array(ThreadId).check(Schema.isMinLength(1)),
  timeoutSeconds: Schema.optional(Schema.Int),
  mode: Schema.optional(WaitSubagentMode),
  resumeToken: Schema.optional(Schema.String),
});
export type WaitSubagentInput = typeof WaitSubagentInput.Type;

export const WaitSubagentResult = Schema.Struct({
  childThreadId: ThreadId,
  status: Schema.String,
  turnCount: Schema.Int,
  finalAssistantText: Schema.NullOr(Schema.String),
  error: Schema.NullOr(Schema.String),
  // Present on a still-running child once the wait auto-promoted (R-A): the
  // child will now notify the parent on completion, so the model should stop
  // waiting and do other work.
  note: Schema.optional(Schema.String),
});

export const WaitSubagentOutput = Schema.Struct({
  results: Schema.Array(WaitSubagentResult),
  settledCount: Schema.Int,
  timedOutCount: Schema.Int,
  pending: Schema.Boolean,
  resumeToken: Schema.String,
  // True when the ~90s auto-promote budget elapsed with one+ children still
  // running (R-A): those children were promoted to wake-on-completion and the
  // model should stop calling wait.
  promoted: Schema.optional(Schema.Boolean),
});
export type WaitSubagentOutput = typeof WaitSubagentOutput.Type;

export const ListSubagentsInput = Schema.Struct({
  parentThreadId: Schema.optional(ThreadId),
});
export type ListSubagentsInput = typeof ListSubagentsInput.Type;

export const ListSubagentEntry = Schema.Struct({
  childThreadId: ThreadId,
  parentThreadId: ThreadId,
  detached: Schema.Boolean,
  depth: Schema.Int,
  spawnedAtMs: Schema.Number,
  settled: Schema.Boolean,
  status: Schema.String,
  turnCount: Schema.Int,
});

export const ListSubagentsOutput = Schema.Struct({
  parentThreadId: ThreadId,
  children: Schema.Array(ListSubagentEntry),
});
export type ListSubagentsOutput = typeof ListSubagentsOutput.Type;

export const ScheduleCreateInput = Schema.Struct({
  threadId: Schema.optional(ThreadId),
  prompt: Schema.String,
  intervalSeconds: Schema.optional(Schema.Int),
  cronExpr: Schema.optional(Schema.String),
  timezone: Schema.optional(Schema.String),
  busyPolicy: Schema.optional(ScheduleBusyPolicy),
  // Optional plain model name (e.g. "claude-opus-4-8" or "gpt-5.4"); the
  // provider/harness is inferred from the live model lists, so the caller never
  // guesses a harness/instance id. Omit to inherit the thread's current model.
  model: Schema.optional(TrimmedNonEmptyString),
});
export type ScheduleCreateInput = typeof ScheduleCreateInput.Type;

// Canonical schema lifted into `@t3tools/contracts` (`ScheduledTaskEntry`) so
// the MCP toolkit and the web client subscription share one wire shape.
// Re-exported under the existing `ScheduleEntry` name to keep tool wiring
// (ScheduleListOutput, etc.) unchanged.
export const ScheduleEntry = ScheduledTaskEntry;
export type ScheduleEntry = typeof ScheduleEntry.Type;

export const ScheduleListInput = Schema.Struct({
  threadId: Schema.optional(ThreadId),
});
export type ScheduleListInput = typeof ScheduleListInput.Type;

export const ScheduleListOutput = Schema.Struct({
  tasks: Schema.Array(ScheduleEntry),
});
export type ScheduleListOutput = typeof ScheduleListOutput.Type;

export const ScheduleUpdateInput = Schema.Struct({
  taskId: ScheduledTaskId,
  enabled: Schema.optional(Schema.Boolean),
  busyPolicy: Schema.optional(ScheduleBusyPolicy),
  intervalSeconds: Schema.optional(Schema.Int),
  cronExpr: Schema.optional(Schema.String),
  // Re-route the schedule to a new plain model name (provider/harness inferred).
  // Omit to leave the current model unchanged.
  model: Schema.optional(TrimmedNonEmptyString),
});
export type ScheduleUpdateInput = typeof ScheduleUpdateInput.Type;

export const ScheduleDeleteInput = Schema.Struct({
  taskId: ScheduledTaskId,
});
export type ScheduleDeleteInput = typeof ScheduleDeleteInput.Type;

export const ScheduleDeleteOutput = Schema.Struct({
  taskId: ScheduledTaskId,
  deleted: Schema.Boolean,
});
export type ScheduleDeleteOutput = typeof ScheduleDeleteOutput.Type;

export const SpawnSubagentTool = Tool.make("t3_spawn_subagent", {
  description:
    "Delegate a unit of work to an autonomous sub-agent thread. Use this freely to fan out background or parallel work — research, refactors, exploring an approach — without blocking yourself. Defaults to detached=true: the sub-agent runs independently and wakes you with its result when it finishes, so prefer spawning detached and continuing your own work. Set detached=false only when you must have the result before proceeding; then optionally pass waitTimeoutSeconds for the foreground wait budget. Defaults to a new Git worktree off the repository default branch. To pick the sub-agent's model, pass `model` as a plain model name (e.g. 'claude-opus-4-8' or 'gpt-5.4'); the provider/harness is inferred automatically, so you never guess a harness/instance id. This is the delegation primitive — for human-requested thread creation use t3_thread_start instead.",
  parameters: SpawnSubagentInput,
  success: SpawnSubagentOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Spawn T3 Code sub-agent")
  .annotate(Tool.OpenWorld, true)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false);

export const SteerSubagentTool = Tool.make("t3_steer_subagent", {
  description:
    "Send an additional instruction to a sub-agent you spawned. This is provider-safe: the system picks the right mechanism automatically — an idle sub-agent gets the message now, a Claude sub-agent mid-turn safely queues it, and a Codex or Cursor sub-agent mid-turn auto-defers it until the sub-agent goes idle (no mid-turn injection is ever sent to those providers). You do not need to know the provider or check whether the sub-agent is busy; just steer. Only the parent that spawned the sub-agent may steer it.",
  parameters: SteerSubagentInput,
  success: SteerSubagentOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Steer T3 Code sub-agent")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false);

export const CheckSubagentTool = Tool.make("t3_check_subagent", {
  description:
    "Read the current status of a sub-agent without waiting. Returns its status, turn count, and latest assistant text. Use this for a quick non-blocking poll; use t3_wait_subagent when you actually need to block until it finishes.",
  parameters: CheckSubagentInput,
  success: CheckSubagentOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Check T3 Code sub-agent")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Idempotent, true);

export const WaitSubagentTool = Tool.make("t3_wait_subagent", {
  description:
    'Wait for one or more sub-agents to finish. This returns quickly with one result row per requested child; a child that has not finished yet has status "pending". While pending is true and you still want to wait, re-call this tool with the returned resumeToken (and the same childThreadIds) to keep waiting — never assume a single call blocks until completion. This waits at most ~90 seconds in total (across resumeToken re-calls); once that elapses with children still running, it returns promoted=true and those children have status "running" — STOP calling wait and go do other work, you will receive a new message automatically when each one finishes. mode "all" (default) waits for every child; "any" returns as soon as one settles. timeoutSeconds (default 600, clamped to [1,3900]) is the requested logical budget; children still unfinished once it is exhausted are returned with status "timeout".',
  parameters: WaitSubagentInput,
  success: WaitSubagentOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Wait for T3 Code sub-agents")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Idempotent, false);

export const ListSubagentsTool = Tool.make("t3_list_subagents", {
  description:
    "List the sub-agents spawned by a parent thread (defaults to the calling thread), merging in-memory registration metadata (spawn time, detached, depth) with each child's current status and turn count.",
  parameters: ListSubagentsInput,
  success: ListSubagentsOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "List T3 Code sub-agents")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Idempotent, true);

export const ScheduleCreateTool = Tool.make("t3_schedule_create", {
  description:
    "Schedule a recurring prompt to be sent to a thread (defaults to the calling thread). Provide exactly one of intervalSeconds (fixed interval) or cronExpr (a cron expression, validated on create); optionally a timezone (IANA name, default UTC) and busyPolicy (\"skip\" default, or \"queue_once\"). The same thread is reused on every trigger. To pin the model each run uses, pass `model` as a plain model name (e.g. 'claude-opus-4-8' or 'gpt-5.4'); the provider/harness is inferred automatically, so you never guess a harness/instance id. Omit `model` to inherit the thread's current model on each run.",
  parameters: ScheduleCreateInput,
  success: ScheduleEntry,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Create scheduled task")
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false);

export const ScheduleListTool = Tool.make("t3_schedule_list", {
  description:
    "List scheduled tasks. Pass threadId to scope to one thread, otherwise lists all scheduled tasks.",
  parameters: ScheduleListInput,
  success: ScheduleListOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "List scheduled tasks")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Idempotent, true);

export const ScheduleUpdateTool = Tool.make("t3_schedule_update", {
  description:
    "Update a scheduled task: enable/disable it, change its busyPolicy, change its interval or cron expression (cron is re-validated), or re-route it to a new model by passing `model` as a plain model name (provider/harness inferred). Only the supplied fields are changed.",
  parameters: ScheduleUpdateInput,
  success: ScheduleEntry,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Update scheduled task")
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false);

export const ScheduleDeleteTool = Tool.make("t3_schedule_delete", {
  description: "Delete a scheduled task by id.",
  parameters: ScheduleDeleteInput,
  success: ScheduleDeleteOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Delete scheduled task")
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true);

export const SubagentToolkit = Toolkit.make(
  SpawnSubagentTool,
  SteerSubagentTool,
  CheckSubagentTool,
  WaitSubagentTool,
  ListSubagentsTool,
  ScheduleCreateTool,
  ScheduleListTool,
  ScheduleUpdateTool,
  ScheduleDeleteTool,
);
