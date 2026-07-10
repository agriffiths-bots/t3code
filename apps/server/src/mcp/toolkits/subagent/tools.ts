import {
  EnvironmentId,
  ProjectId,
  ScheduledTaskEntry,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import { ScheduledTaskId } from "../../../persistence/Services/ScheduledTasks.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import {
  ThreadStartInternalInput,
  ThreadStartPublicInput,
  ThreadStartToolError,
  ThreadStartMode,
} from "../thread/tools.ts";

const dependencies = [McpInvocationContext.McpInvocationContext];

export const SpawnSubagentInput = ThreadStartPublicInput;
export type SpawnSubagentInput = typeof SpawnSubagentInput.Type;

export const SpawnSubagentInternalInput = Schema.Struct({
  ...ThreadStartInternalInput.fields,
  target: Schema.optional(TrimmedNonEmptyString),
  remoteParentThreadId: Schema.optional(ThreadId),
  remoteParentEnvironmentId: Schema.optional(EnvironmentId),
  detached: Schema.optional(Schema.Boolean),
  waitTimeoutSeconds: Schema.optional(Schema.Int),
});
export type SpawnSubagentInternalInput = typeof SpawnSubagentInternalInput.Type;

export const SpawnSubagentOutput = Schema.Struct({
  childThreadId: ThreadId,
  projectId: ProjectId,
  mode: ThreadStartMode,
  branch: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  parentThreadId: ThreadId,
  warning: Schema.optional(Schema.String),
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

export const SubagentDetailOutput = Schema.Struct({
  threadId: ThreadId,
  status: Schema.String,
  turnCount: Schema.Int,
  latestAssistantText: Schema.NullOr(Schema.String),
});
export type SubagentDetailOutput = typeof SubagentDetailOutput.Type;

export const LegacyCheckSubagentInput = Schema.Struct({ childThreadId: ThreadId });
export type LegacyCheckSubagentInput = typeof LegacyCheckSubagentInput.Type;

export const SubagentsInput = Schema.Struct({
  childThreadId: Schema.optional(ThreadId),
});
export type SubagentsInput = typeof SubagentsInput.Type;

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

export const SubagentsOutput = Schema.Struct({
  parentThreadId: Schema.optional(ThreadId),
  children: Schema.optional(Schema.Array(ListSubagentEntry)),
  threadId: Schema.optional(ThreadId),
  status: Schema.optional(Schema.String),
  turnCount: Schema.optional(Schema.Int),
  latestAssistantText: Schema.optional(Schema.NullOr(Schema.String)),
});
export type SubagentsOutput = typeof SubagentsOutput.Type;

export const ScheduleCreateInput = Schema.Struct({
  threadId: Schema.optional(ThreadId),
  prompt: Schema.String,
  intervalSeconds: Schema.optional(Schema.Int),
  cronExpr: Schema.optional(Schema.String),
  timezone: Schema.optional(Schema.String),
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
  intervalSeconds: Schema.optional(Schema.Int),
  cronExpr: Schema.optional(Schema.String),
  // Re-route the schedule to a new plain model name (provider/harness inferred),
  // or pass null to un-pin and inherit the thread's current model again. Omit to
  // leave the current model unchanged.
  model: Schema.optional(Schema.NullOr(TrimmedNonEmptyString)),
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
    "Delegate a unit of work to an autonomous sub-agent thread. The model and title are required. The child inherits the parent runtime and interaction modes, runs independently, and wakes the parent with its result on completion. Defaults to a new Git worktree from project configuration. Pass `directory` to target another local project and `branch` to name the new worktree branch. `reasoningEffort` defaults to `xhigh` for Codex models that advertise reasoning effort and overrides that default when supplied. For human-requested thread creation use t3_thread_start instead.",
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

export const SubagentsTool = Tool.make("t3_subagents", {
  description:
    "List sub-agents spawned by the calling thread with their current statuses. Pass `childThreadId` to inspect one owned child in detail, including its latest assistant text. Omitting `childThreadId` defaults to listing all children of the calling thread.",
  parameters: SubagentsInput,
  success: SubagentsOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Inspect T3 Code sub-agents")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Idempotent, true);

export const ScheduleCreateTool = Tool.make("t3_schedule_create", {
  description:
    "Schedule a recurring prompt to be sent to a thread (defaults to the calling thread). Provide exactly one of intervalSeconds (fixed interval) or cronExpr (a cron expression, validated on create); timezone defaults to UTC and the busy policy always defaults to skip. The same thread is reused on every trigger. To pin the model each run uses, pass `model` as a plain model name (e.g. 'claude-opus-4-8' or 'gpt-5.4'); the provider/harness is inferred automatically, so you never guess a harness/instance id. Pin a model on the thread's own provider (like the interactive model picker) — pinning a different provider than the thread's active session errors at run time, so prefer a dedicated thread for a cross-provider schedule. Omit `model` to inherit the thread's current model on each run.",
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
    "Update a scheduled task: enable/disable it, change its interval or cron expression (cron is re-validated), re-route it to a new model by passing `model` as a plain model name (provider/harness inferred), or pass `model: null` to un-pin the model so runs inherit the thread's current model again. The persisted busy policy is preserved. Only the supplied fields are changed.",
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
  SubagentsTool,
  ScheduleCreateTool,
  ScheduleListTool,
  ScheduleUpdateTool,
  ScheduleDeleteTool,
);
