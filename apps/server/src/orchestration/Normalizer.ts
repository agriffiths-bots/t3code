import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Cause from "effect/Cause";
import {
  type ChatAttachment,
  type ClientOrchestrationCommand,
  type IsoDateTime,
  type OrchestrationCommand,
  OrchestrationDispatchCommandError,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
} from "@t3tools/contracts";

import { createAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import { ServerConfig } from "../config.ts";
import { parseBase64DataUrl } from "../imageMime.ts";
import { expandHomePath } from "../pathExpansion.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import {
  authorizeOrchestrationCommandReceiptReplay,
  authorizeOrchestrationCommandMutation,
  type OrchestrationCommandDispatchAuthority,
} from "./commandAudienceGuard.ts";

const isPersistedChatAttachment = (attachment: unknown): attachment is ChatAttachment =>
  typeof attachment === "object" &&
  attachment !== null &&
  "type" in attachment &&
  attachment.type === "image" &&
  "id" in attachment &&
  typeof attachment.id === "string";

interface AttachmentWritePlan {
  readonly attachment: ChatAttachment;
  readonly attachmentPath: string;
  readonly bytes: Buffer;
}

export const cleanupPersistedCommandAttachments = (command: OrchestrationCommand) =>
  Effect.gen(function* () {
    if (command.type !== "thread.turn.start") {
      return;
    }

    const fileSystem = yield* FileSystem.FileSystem;
    const serverConfig = yield* ServerConfig;
    yield* Effect.forEach(
      command.message.attachments,
      (attachment) => {
        if (!isPersistedChatAttachment(attachment)) {
          return Effect.void;
        }
        const attachmentPath = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!attachmentPath) {
          return Effect.void;
        }
        return fileSystem.remove(attachmentPath).pipe(
          Effect.catchCause((cause) =>
            Effect.logDebug("failed to clean up rejected command attachment", {
              attachmentId: attachment.id,
              cause: Cause.pretty(cause),
            }),
          ),
        );
      },
      { concurrency: 1, discard: true },
    );
  });

export const canonicalizeClientCommandTimestamps = (
  command: ClientOrchestrationCommand,
  receivedAt: IsoDateTime,
): ClientOrchestrationCommand => {
  const canonicalCommand =
    "createdAt" in command
      ? {
          ...command,
          createdAt: receivedAt,
        }
      : command;

  if (canonicalCommand.type !== "thread.turn.start" || !canonicalCommand.bootstrap?.createThread) {
    return canonicalCommand;
  }

  return {
    ...canonicalCommand,
    bootstrap: {
      ...canonicalCommand.bootstrap,
      createThread: {
        ...canonicalCommand.bootstrap.createThread,
        createdAt: receivedAt,
      },
    },
  };
};

// `BootstrapTurnStartDispatcher` recognises a replayed bootstrap turn by
// fingerprinting the request against the thread it already created, and that
// fingerprint includes `createdAt`. Canonicalizing the bootstrap timestamp onto
// the server receipt time would give every retry a fresh value, so the replay
// would never match and the dispatcher would create a duplicate thread. Keep
// the client's bootstrap timestamp as the stable identity for that check; the
// turn-level `createdAt` is still canonicalized.
const preserveBootstrapCreateThreadTimestamp = (
  canonicalCommand: ClientOrchestrationCommand,
  originalCommand: ClientOrchestrationCommand,
): ClientOrchestrationCommand => {
  if (
    canonicalCommand.type !== "thread.turn.start" ||
    !canonicalCommand.bootstrap?.createThread ||
    originalCommand.type !== "thread.turn.start" ||
    !originalCommand.bootstrap?.createThread
  ) {
    return canonicalCommand;
  }

  return {
    ...canonicalCommand,
    bootstrap: {
      ...canonicalCommand.bootstrap,
      createThread: {
        ...canonicalCommand.bootstrap.createThread,
        createdAt: originalCommand.bootstrap.createThread.createdAt,
      },
    },
  };
};

export const normalizeDispatchCommand = (command: ClientOrchestrationCommand) =>
  Effect.gen(function* () {
    const receivedAt = DateTime.formatIso(yield* DateTime.now);
    const canonicalCommand = preserveBootstrapCreateThreadTimestamp(
      canonicalizeClientCommandTimestamps(command, receivedAt),
      command,
    );
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const serverConfig = yield* ServerConfig;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;

    const normalizeProjectWorkspaceRoot = (workspaceRoot: string) =>
      workspacePaths.normalizeWorkspaceRoot(workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationDispatchCommandError({
              message: cause.message,
            }),
        ),
      );

    const normalizeProjectWorkspaceRootForCreate = (
      workspaceRoot: string,
      createIfMissing: boolean | undefined,
    ) =>
      workspacePaths
        .normalizeWorkspaceRoot(workspaceRoot, {
          createIfMissing: createIfMissing === true,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new OrchestrationDispatchCommandError({
                message: cause.message,
              }),
          ),
        );

    if (canonicalCommand.type === "project.create") {
      return {
        ...canonicalCommand,
        workspaceRoot: yield* normalizeProjectWorkspaceRootForCreate(
          canonicalCommand.workspaceRoot,
          canonicalCommand.createWorkspaceRootIfMissing,
        ),
        createWorkspaceRootIfMissing: canonicalCommand.createWorkspaceRootIfMissing === true,
      } satisfies OrchestrationCommand;
    }

    if (
      canonicalCommand.type === "project.meta.update" &&
      canonicalCommand.workspaceRoot !== undefined
    ) {
      return {
        ...canonicalCommand,
        workspaceRoot: yield* normalizeProjectWorkspaceRoot(canonicalCommand.workspaceRoot),
      } satisfies OrchestrationCommand;
    }

    if (canonicalCommand.type !== "thread.turn.start") {
      return canonicalCommand as OrchestrationCommand;
    }

    const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
    const existingReceipt = yield* commandReceiptRepository.getByCommandId({
      commandId: canonicalCommand.commandId,
    });
    if (Option.isSome(existingReceipt)) {
      return canonicalCommand as unknown as OrchestrationCommand;
    }

    const attachmentWritePlans = yield* Effect.forEach(
      canonicalCommand.message.attachments,
      (attachment) =>
        Effect.gen(function* () {
          const parsed = parseBase64DataUrl(attachment.dataUrl);
          if (!parsed || !parsed.mimeType.startsWith("image/")) {
            return yield* new OrchestrationDispatchCommandError({
              message: `Invalid image attachment payload for '${attachment.name}'.`,
            });
          }

          const bytes = Buffer.from(parsed.base64, "base64");
          if (bytes.byteLength === 0 || bytes.byteLength > PROVIDER_SEND_TURN_MAX_IMAGE_BYTES) {
            return yield* new OrchestrationDispatchCommandError({
              message: `Image attachment '${attachment.name}' is empty or too large.`,
            });
          }

          const attachmentId = createAttachmentId(canonicalCommand.threadId);
          if (!attachmentId) {
            return yield* new OrchestrationDispatchCommandError({
              message: "Failed to create a safe attachment id.",
            });
          }

          const persistedAttachment = {
            type: "image" as const,
            id: attachmentId,
            name: attachment.name,
            mimeType: parsed.mimeType.toLowerCase(),
            sizeBytes: bytes.byteLength,
          };

          const attachmentPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment: persistedAttachment,
          });
          if (!attachmentPath) {
            return yield* new OrchestrationDispatchCommandError({
              message: `Failed to resolve persisted path for '${attachment.name}'.`,
            });
          }

          return {
            attachment: persistedAttachment,
            attachmentPath,
            bytes,
          } satisfies AttachmentWritePlan;
        }),
      { concurrency: 1 },
    );

    const commandWithPersistedAttachments = {
      ...canonicalCommand,
      message: {
        ...canonicalCommand.message,
        attachments: attachmentWritePlans.map(({ attachment }) => attachment),
      },
    } satisfies OrchestrationCommand;

    yield* Effect.forEach(
      attachmentWritePlans,
      ({ attachment, attachmentPath, bytes }) =>
        Effect.gen(function* () {
          yield* fileSystem.makeDirectory(path.dirname(attachmentPath), { recursive: true }).pipe(
            Effect.mapError(
              () =>
                new OrchestrationDispatchCommandError({
                  message: `Failed to create attachment directory for '${attachment.name}'.`,
                }),
            ),
          );
          yield* fileSystem.writeFile(attachmentPath, bytes).pipe(
            Effect.mapError(
              () =>
                new OrchestrationDispatchCommandError({
                  message: `Failed to persist attachment '${attachment.name}'.`,
                }),
            ),
          );
        }),
      { concurrency: 1, discard: true },
    ).pipe(
      Effect.catch((error) =>
        cleanupPersistedCommandAttachments(commandWithPersistedAttachments).pipe(
          Effect.andThen(Effect.fail(error)),
        ),
      ),
    );

    return commandWithPersistedAttachments;
  });

const normalizeDispatchCommandForAuthorization = (
  command: ClientOrchestrationCommand,
  path: Path.Path,
): OrchestrationCommand => {
  if (command.type === "project.create") {
    return {
      ...command,
      workspaceRoot: path.resolve(expandHomePath(command.workspaceRoot.trim())),
    };
  }
  if (command.type === "project.meta.update" && command.workspaceRoot !== undefined) {
    return {
      ...command,
      workspaceRoot: path.resolve(expandHomePath(command.workspaceRoot.trim())),
    };
  }
  if (command.type === "thread.turn.start") {
    return {
      ...command,
      message: {
        ...command.message,
        // Attachment payloads do not participate in audience authorization.
        // Keep the preflight side-effect free; persistence happens only after approval.
        attachments: [],
      },
    };
  }
  return command;
};

export const normalizeAuthorizedDispatchCommand = (
  command: ClientOrchestrationCommand,
  authority: OrchestrationCommandDispatchAuthority,
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const readModel = yield* projectionSnapshotQuery.getCommandReadModel();
    const existingReceipt = yield* commandReceiptRepository.getByCommandId({
      commandId: command.commandId,
    });
    if (Option.isSome(existingReceipt)) {
      const replayCommand = command as unknown as OrchestrationCommand;
      yield* authorizeOrchestrationCommandReceiptReplay({
        command: replayCommand,
        readModel,
        authority,
        receipt: existingReceipt.value,
      });
      return replayCommand;
    }
    yield* authorizeOrchestrationCommandMutation({
      command: normalizeDispatchCommandForAuthorization(command, path),
      readModel,
      authority,
      fileSystem,
      path,
    });
    return yield* normalizeDispatchCommand(command);
  });
