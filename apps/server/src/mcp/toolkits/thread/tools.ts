import {
  ModelSelection,
  ProjectId,
  ProviderInteractionMode,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  RuntimeMode,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";

export const ThreadStartMode = Schema.Literals([
  "new_worktree",
  "existing_worktree",
  "current_checkout",
]);
export type ThreadStartMode = typeof ThreadStartMode.Type;

const ThreadStartBaseBranchSource = Schema.Literals(["default", "source"]);

export const ThreadStartToolInput = Schema.Struct({
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
  title: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(255))),
  mode: Schema.optional(ThreadStartMode),
  worktreePath: Schema.optional(TrimmedNonEmptyString),
  branch: Schema.optional(TrimmedNonEmptyString),
  baseBranch: Schema.optional(TrimmedNonEmptyString),
  baseBranchSource: Schema.optional(ThreadStartBaseBranchSource),
  runSetupScript: Schema.optional(Schema.Boolean),
  model: Schema.optional(TrimmedNonEmptyString),
  modelSelection: Schema.optional(ModelSelection),
  runtimeMode: Schema.optional(RuntimeMode),
  interactionMode: Schema.optional(ProviderInteractionMode),
});
export type ThreadStartToolInput = typeof ThreadStartToolInput.Type;

export const ThreadStartToolOutput = Schema.Struct({
  threadId: ThreadId,
  projectId: ProjectId,
  mode: ThreadStartMode,
  branch: Schema.NullOr(TrimmedNonEmptyString),
  worktreePath: Schema.NullOr(TrimmedNonEmptyString),
  warning: Schema.optional(Schema.String),
});
export type ThreadStartToolOutput = typeof ThreadStartToolOutput.Type;

export class ThreadStartToolError extends Schema.TaggedErrorClass<ThreadStartToolError>()(
  "ThreadStartToolError",
  {
    message: Schema.String,
  },
) {}

const dependencies = [McpInvocationContext.McpInvocationContext];

export const ThreadStartTool = Tool.make("t3_thread_start", {
  description:
    "Start a new T3 Code thread with the supplied initial prompt, only when the user explicitly asks to start/spawn/create another thread or agent. Do not use for autonomous delegation or background parallel work. Defaults to creating a new Git worktree from the repository default branch. Use current_checkout only when the user explicitly asks for the same checkout. To choose the model, pass `model` as a plain model name (e.g. 'claude-opus-4-8' or 'gpt-5.4') — the provider/harness is inferred automatically, so you never need to know or pass a harness/instance id. This tool launches the child turn and returns metadata without waiting for completion.",
  parameters: ThreadStartToolInput,
  success: ThreadStartToolOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Start T3 Code thread")
  .annotate(Tool.OpenWorld, true)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false);

export const ThreadToolkit = Toolkit.make(ThreadStartTool);
