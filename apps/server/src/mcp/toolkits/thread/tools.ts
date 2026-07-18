import {
  ModelSelection,
  NonNegativeInt,
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

export const ThreadStartPublicInput = Schema.Struct({
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
  model: TrimmedNonEmptyString,
  title: TrimmedNonEmptyString.check(Schema.isMaxLength(255)),
  directory: Schema.optionalKey(TrimmedNonEmptyString),
  branch: Schema.optionalKey(TrimmedNonEmptyString),
  reasoningEffort: Schema.optionalKey(TrimmedNonEmptyString),
});
export type ThreadStartPublicInput = typeof ThreadStartPublicInput.Type;

/**
 * Full server-side thread-start request. The MCP tool deliberately exposes a
 * much smaller input; orchestration callers still use these controls after the
 * public request has been normalized.
 */
export const ThreadStartInternalInput = Schema.Struct({
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
  title: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(255))),
  mode: Schema.optional(ThreadStartMode),
  // Absolute path overriding the spawn base directory: VCS detection, worktree
  // creation, and current-checkout runs all resolve against it instead of the
  // source thread's cwd/project root. Mutually exclusive with worktreePath.
  directory: Schema.optional(TrimmedNonEmptyString),
  worktreePath: Schema.optional(TrimmedNonEmptyString),
  branch: Schema.optional(TrimmedNonEmptyString),
  baseBranch: Schema.optional(TrimmedNonEmptyString),
  baseBranchSource: Schema.optional(ThreadStartBaseBranchSource),
  runSetupScript: Schema.optional(Schema.Boolean),
  model: Schema.optional(TrimmedNonEmptyString),
  modelSelection: Schema.optional(ModelSelection),
  runtimeMode: Schema.optional(RuntimeMode),
  interactionMode: Schema.optional(ProviderInteractionMode),
  reasoningEffort: Schema.optional(TrimmedNonEmptyString),
});
export type ThreadStartInternalInput = typeof ThreadStartInternalInput.Type;

// Backward-compatible internal name used by orchestration callers. The MCP
// tool itself is wired to ThreadStartPublicInput below.
export const ThreadStartToolInput = ThreadStartInternalInput;
export type ThreadStartToolInput = ThreadStartInternalInput;

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

export const ThreadArchiveToolInput = Schema.Struct({
  threadId: ThreadId,
});
export type ThreadArchiveToolInput = typeof ThreadArchiveToolInput.Type;

export const ThreadArchiveToolOutput = Schema.Struct({
  threadId: ThreadId,
  sequence: NonNegativeInt,
});
export type ThreadArchiveToolOutput = typeof ThreadArchiveToolOutput.Type;

export const ThreadStartTool = Tool.make("t3_thread_start", {
  description:
    "Start a new T3 Code thread with the supplied initial prompt, only when the user explicitly asks to start/spawn/create another thread or agent. Do not use for autonomous delegation or background parallel work. The model and title are required. Defaults to creating a new Git worktree from the repository default branch when the project directory is a Git repository; non-Git projects start in the current directory with warning metadata. The child inherits the source thread's runtime and interaction modes, and configured setup runs for a new project worktree. Pass `directory` (absolute path) to base the thread somewhere else entirely. `reasoningEffort` defaults to `xhigh` for Codex models that advertise reasoning effort and overrides that default when supplied. This tool launches the child turn and returns metadata without waiting for completion.",
  parameters: ThreadStartPublicInput,
  success: ThreadStartToolOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Start T3 Code thread")
  .annotate(Tool.OpenWorld, true)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false);

export const ThreadArchiveTool = Tool.make("t3_archive_thread", {
  description:
    "Archive a stuck non-terminal sub-agent descendant through T3 Code's existing thread archive lifecycle. This owner-only administration tool is available only from a private root thread and only for descendants owned by that root. Use it only when the user explicitly asks to settle/archive a ghost child; it rejects factory, peer, child, cross-root, terminal, missing, and already-archived targets.",
  parameters: ThreadArchiveToolInput,
  success: ThreadArchiveToolOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Archive stuck T3 Code child")
  .annotate(Tool.OpenWorld, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false);

export const ThreadToolkit = Toolkit.make(ThreadStartTool, ThreadArchiveTool);
