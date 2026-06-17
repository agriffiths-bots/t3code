import {
  CommandId,
  MessageId,
  ThreadId,
  type ModelSelection,
  type OrchestrationProjectShell,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as BootstrapTurnStartDispatcher from "../../../orchestration/Services/BootstrapTurnStartDispatcher.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { GitWorkflowService } from "../../../git/GitWorkflowService.ts";
import {
  ThreadStartToolError,
  type ThreadStartMode,
  type ThreadStartToolInput,
  type ThreadStartToolOutput,
  ThreadToolkit,
} from "./tools.ts";

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const isThreadStartToolError = Schema.is(ThreadStartToolError);

const fail = (message: string) => new ThreadStartToolError({ message });

const truncateTitle = (value: string): string => {
  const trimmed = value.trim().replace(/\s+/g, " ");
  if (trimmed.length === 0) return "New thread";
  return trimmed.length <= 80 ? trimmed : `${trimmed.slice(0, 77)}...`;
};

const resolveOption = <A>(
  option: Option.Option<A>,
  message: string,
): Effect.Effect<A, ThreadStartToolError> =>
  Option.match(option, {
    onNone: () => Effect.fail(fail(message)),
    onSome: Effect.succeed,
  });

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

export type ActiveThreadStartRuntime = (
  input: ThreadStartToolInput,
  invocation: McpInvocationContext.McpInvocationScope,
) => Effect.Effect<ThreadStartToolOutput, ThreadStartToolError>;

let activeThreadStartRuntime: ActiveThreadStartRuntime | null = null;

/** Reach the live thread-start runtime from sub-agent tool handlers (mirrors `dispatchActive`). */
export const activeThreadStartRuntimeOf = (): ActiveThreadStartRuntime | null =>
  activeThreadStartRuntime;

const makeActiveThreadStartRuntime = Effect.fn("ThreadToolkit.makeActiveRuntime")(function* () {
  const crypto = yield* Crypto.Crypto;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const gitWorkflow = yield* GitWorkflowService;
  const uuid = () => crypto.randomUUIDv4.pipe(Effect.orDie);

  const makeIds = Effect.fn("ThreadToolkit.makeIds")(function* () {
    return {
      commandId: CommandId.make(yield* uuid()),
      messageId: MessageId.make(yield* uuid()),
      threadId: ThreadId.make(yield* uuid()),
    };
  });

  const makeTemporaryBranchName = Effect.fn("ThreadToolkit.makeTemporaryBranchName")(function* () {
    const bytes = yield* crypto.randomBytes(4).pipe(Effect.orDie);
    return buildTemporaryWorktreeBranchName((byteLength) =>
      byteLength === 4 ? bytesToHex(bytes) : "",
    );
  });

  const resolveCurrentBranch = Effect.fn("ThreadToolkit.resolveCurrentBranch")(function* (
    cwd: string,
  ) {
    return yield* gitWorkflow.status({ cwd }).pipe(
      Effect.map((status) => status.refName),
      Effect.orElseSucceed(() => null),
    );
  });

  const resolveDefaultBranch = Effect.fn("ThreadToolkit.resolveDefaultBranch")(function* (
    cwd: string,
  ) {
    return yield* gitWorkflow.listRefs({ cwd, limit: 200 }).pipe(
      Effect.map(
        (result) => result.refs.find((ref) => ref.isDefault && !ref.isRemote)?.name ?? null,
      ),
      Effect.orElseSucceed(() => null),
    );
  });

  const resolveNewWorktreeBaseBranch = Effect.fn("ThreadToolkit.resolveNewWorktreeBaseBranch")(
    function* (
      input: ThreadStartToolInput,
      project: OrchestrationProjectShell,
      sourceThread: OrchestrationThreadShell,
    ) {
      if (input.baseBranch) return input.baseBranch;
      if (input.baseBranchSource === "source" && sourceThread.branch) return sourceThread.branch;

      const defaultBranch = yield* resolveDefaultBranch(project.workspaceRoot);
      if (defaultBranch) return defaultBranch;
      if (sourceThread.branch) return sourceThread.branch;

      const currentBranch = yield* resolveCurrentBranch(project.workspaceRoot);
      if (currentBranch) return currentBranch;

      return yield* fail("Could not resolve a base branch for the new worktree.");
    },
  );

  const resolveInitialBranch = Effect.fn("ThreadToolkit.resolveInitialBranch")(function* (
    mode: ThreadStartMode,
    input: ThreadStartToolInput,
    project: OrchestrationProjectShell,
    sourceThread: OrchestrationThreadShell,
  ) {
    if (input.branch) return input.branch;
    if (mode === "new_worktree") return yield* makeTemporaryBranchName();
    if (mode === "existing_worktree") {
      if (!input.worktreePath) {
        return yield* fail("existing_worktree mode requires worktreePath.");
      }
      return yield* resolveCurrentBranch(input.worktreePath);
    }
    return sourceThread.branch ?? (yield* resolveCurrentBranch(project.workspaceRoot));
  });

  const loadSourceContext = Effect.fn("ThreadToolkit.loadSourceContext")(function* (
    invocation: McpInvocationContext.McpInvocationScope,
  ) {
    const sourceThread = yield* projectionSnapshotQuery
      .getThreadShellById(invocation.threadId)
      .pipe(
        Effect.flatMap((thread) =>
          resolveOption(thread, `Source thread ${invocation.threadId} was not found.`),
        ),
        Effect.mapError((error) =>
          isThreadStartToolError(error)
            ? error
            : fail(error instanceof Error ? error.message : "Failed to load source thread."),
        ),
      );
    const project = yield* projectionSnapshotQuery.getProjectShellById(sourceThread.projectId).pipe(
      Effect.flatMap((project) =>
        resolveOption(project, `Project ${sourceThread.projectId} was not found.`),
      ),
      Effect.mapError((error) =>
        isThreadStartToolError(error)
          ? error
          : fail(error instanceof Error ? error.message : "Failed to load source project."),
      ),
    );

    return { sourceThread, project };
  });

  return Effect.fn("ThreadToolkit.startThread")(function* (
    input: ThreadStartToolInput,
    invocation: McpInvocationContext.McpInvocationScope,
  ) {
    const { sourceThread, project } = yield* loadSourceContext(invocation);
    const mode = input.mode ?? "new_worktree";
    const ids = yield* makeIds();
    const createdAt = yield* nowIso;
    const branch = (yield* resolveInitialBranch(mode, input, project, sourceThread)) ?? null;
    const worktreePath: string | null =
      mode === "existing_worktree" ? (input.worktreePath ?? null) : null;
    const title = input.title ?? truncateTitle(input.prompt);
    const modelSelection = resolveModelSelection(input, sourceThread);
    const runtimeMode = input.runtimeMode ?? sourceThread.runtimeMode;
    const interactionMode = input.interactionMode ?? sourceThread.interactionMode;
    const prepareWorktree =
      mode === "new_worktree"
        ? {
            projectCwd: project.workspaceRoot,
            baseBranch: yield* resolveNewWorktreeBaseBranch(input, project, sourceThread),
            branch: branch ?? undefined,
          }
        : undefined;

    if (mode === "existing_worktree" && !worktreePath) {
      return yield* fail("existing_worktree mode requires worktreePath.");
    }

    yield* BootstrapTurnStartDispatcher.dispatchActive({
      type: "thread.turn.start",
      commandId: ids.commandId,
      threadId: ids.threadId,
      message: {
        messageId: ids.messageId,
        role: "user",
        text: input.prompt,
        attachments: [],
      },
      modelSelection,
      titleSeed: title,
      runtimeMode,
      interactionMode,
      bootstrap: {
        createThread: {
          projectId: project.id,
          title,
          modelSelection,
          runtimeMode,
          interactionMode,
          branch,
          worktreePath,
          createdAt,
        },
        ...(prepareWorktree
          ? {
              prepareWorktree,
              runSetupScript: input.runSetupScript ?? true,
            }
          : {}),
      },
      createdAt,
    }).pipe(
      Effect.mapError((error) =>
        fail(error instanceof Error ? error.message : "Failed to start child thread."),
      ),
    );

    return {
      threadId: ids.threadId,
      projectId: project.id,
      mode,
      branch,
      worktreePath,
      ...(mode === "current_checkout"
        ? {
            warning:
              "Child thread was started on the current checkout and may conflict with concurrent writes.",
          }
        : {}),
    };
  });
});

export const ThreadStartRuntimeLive = Layer.effectDiscard(
  Effect.acquireRelease(
    makeActiveThreadStartRuntime().pipe(
      Effect.tap((runtime) =>
        Effect.sync(() => {
          activeThreadStartRuntime = runtime;
        }),
      ),
    ),
    (runtime) =>
      Effect.sync(() => {
        if (activeThreadStartRuntime === runtime) activeThreadStartRuntime = null;
      }),
  ),
);

const resolveModelSelection = (
  input: ThreadStartToolInput,
  sourceThread: OrchestrationThreadShell,
): ModelSelection => input.modelSelection ?? sourceThread.modelSelection;

const startThread = Effect.fn("ThreadToolkit.startThread")(function* (input: ThreadStartToolInput) {
  const invocation = yield* McpInvocationContext.requireMcpCapability("thread-management").pipe(
    Effect.mapError((error) => fail(error.message)),
  );
  const runtime = activeThreadStartRuntime;
  if (!runtime) return yield* fail("Thread start runtime is not available.");
  return yield* runtime(input, invocation);
});

const handlers = {
  t3_thread_start: startThread,
} satisfies Parameters<typeof ThreadToolkit.toLayer>[0];

export const ThreadToolkitHandlersLive = ThreadToolkit.toLayer(handlers);
