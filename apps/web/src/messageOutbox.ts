import {
  ClientOrchestrationCommand,
  CommandId,
  EnvironmentId,
  MessageId,
  type OrchestrationMessage,
  ThreadId,
} from "@t3tools/contracts";
import type { StartThreadTurnInput } from "@t3tools/client-runtime/operations";
import * as Schema from "effect/Schema";

import type { ChatAttachment, ChatMessage } from "./types";

export const MESSAGE_OUTBOX_STORAGE_KEY = "t3code:message-outbox:v1";

export type MessageOutboxStatus = "pending" | "sending" | "failed";

export interface MessageOutboxSubmission {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly commandId: CommandId;
  readonly input: StartThreadTurnInput;
  readonly optimisticAttachments: ReadonlyArray<ChatAttachment>;
  readonly durable: boolean;
  readonly retryable?: boolean;
  readonly status: MessageOutboxStatus;
  readonly attempts: number;
  readonly error: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly sequence?: number;
}

export interface MessageOutboxDocument {
  readonly version: 1;
  readonly submissions: ReadonlyArray<MessageOutboxSubmission>;
}

export interface MessageOutboxReconcileResult {
  readonly document: MessageOutboxDocument;
  readonly deliveredMessageIds: ReadonlySet<MessageId>;
  readonly changed: boolean;
}

type StorageLike = Pick<Storage, "getItem" | "setItem" | "removeItem">;
type MessageOutboxListener = () => void;

const decodeClientOrchestrationCommand = Schema.decodeUnknownOption(ClientOrchestrationCommand);
let sessionMessageOutbox: MessageOutboxDocument | null = null;
const messageOutboxListeners = new Set<MessageOutboxListener>();

export function emptyMessageOutbox(): MessageOutboxDocument {
  return { version: 1, submissions: [] };
}

export function clientTurnCommandId(messageId: MessageId): CommandId {
  return CommandId.make(`client:turn:${messageId}`);
}

function browserStorage(): StorageLike | undefined {
  if (typeof window === "undefined") {
    return undefined;
  }
  try {
    return window.localStorage;
  } catch {
    return undefined;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function normalizeStatus(value: unknown): MessageOutboxStatus {
  return value === "failed" ? "failed" : "pending";
}

function normalizeStartThreadTurnInput(value: unknown): StartThreadTurnInput | null {
  if (!isRecord(value)) return null;
  const decoded = decodeClientOrchestrationCommand({
    ...value,
    type: "thread.turn.start",
  });
  if (decoded._tag === "None" || decoded.value.type !== "thread.turn.start") {
    return null;
  }
  const { type: _type, ...input } = decoded.value;
  return input as StartThreadTurnInput;
}

function normalizeSubmission(
  value: unknown,
  fallbackSequence: number,
): MessageOutboxSubmission | null {
  if (!isRecord(value)) return null;
  const environmentId = stringValue(value.environmentId);
  const threadId = stringValue(value.threadId);
  const messageId = stringValue(value.messageId);
  const commandId = stringValue(value.commandId);
  const createdAt = stringValue(value.createdAt);
  const updatedAt = stringValue(value.updatedAt) ?? createdAt;
  const input = normalizeStartThreadTurnInput(value.input);
  if (
    environmentId === null ||
    threadId === null ||
    messageId === null ||
    commandId === null ||
    createdAt === null ||
    updatedAt === null ||
    input === null
  ) {
    return null;
  }
  if (
    input.threadId !== threadId ||
    input.commandId !== commandId ||
    input.message.messageId !== messageId
  ) {
    return null;
  }

  const attempts =
    typeof value.attempts === "number" && Number.isFinite(value.attempts)
      ? Math.max(0, Math.trunc(value.attempts))
      : 0;
  const sequence =
    typeof value.sequence === "number" && Number.isFinite(value.sequence)
      ? Math.max(0, Math.trunc(value.sequence))
      : fallbackSequence;
  const optimisticAttachments = Array.isArray(value.optimisticAttachments)
    ? (value.optimisticAttachments.filter(isRecord) as unknown as ChatAttachment[])
    : [];

  const submission: MessageOutboxSubmission = {
    environmentId: EnvironmentId.make(environmentId),
    threadId: ThreadId.make(threadId),
    messageId: MessageId.make(messageId),
    commandId: CommandId.make(commandId),
    input,
    optimisticAttachments,
    durable: true,
    retryable: typeof value.retryable === "boolean" ? value.retryable : true,
    status: normalizeStatus(value.status),
    attempts,
    error: typeof value.error === "string" && value.error.length > 0 ? value.error : null,
    createdAt,
    updatedAt,
    sequence,
  };
  return {
    ...submission,
    durable:
      typeof value.durable === "boolean"
        ? value.durable
        : messageOutboxSubmissionBaseIsDurable(submission),
  };
}

export function normalizeMessageOutbox(raw: unknown): MessageOutboxDocument {
  if (!isRecord(raw) || !Array.isArray(raw.submissions)) {
    return emptyMessageOutbox();
  }
  const deduped = new Map<string, MessageOutboxSubmission>();
  raw.submissions.forEach((value, index) => {
    const submission = normalizeSubmission(value, index);
    if (submission) {
      deduped.set(submission.messageId, submission);
    }
  });
  return {
    version: 1,
    submissions: [...deduped.values()].toSorted(compareOutboxSubmissions),
  };
}

export function readMessageOutbox(storage: StorageLike | undefined = browserStorage()) {
  if (!storage) return emptyMessageOutbox();
  try {
    const raw = storage.getItem(MESSAGE_OUTBOX_STORAGE_KEY);
    return raw
      ? durableMessageOutboxDocument(normalizeMessageOutbox(JSON.parse(raw)))
      : emptyMessageOutbox();
  } catch {
    return emptyMessageOutbox();
  }
}

export function getMessageOutboxSnapshot(): MessageOutboxDocument {
  sessionMessageOutbox ??= readMessageOutbox();
  return sessionMessageOutbox;
}

export function subscribeMessageOutbox(listener: MessageOutboxListener): () => void {
  messageOutboxListeners.add(listener);
  return () => {
    messageOutboxListeners.delete(listener);
  };
}

function emitMessageOutboxChange() {
  for (const listener of messageOutboxListeners) {
    listener();
  }
}

export function updateSessionMessageOutbox(
  update: (document: MessageOutboxDocument) => MessageOutboxDocument,
  storage: StorageLike | undefined = browserStorage(),
): MessageOutboxDocument {
  sessionMessageOutbox ??= readMessageOutbox(storage);
  const current = sessionMessageOutbox;
  const next = update(current);
  if (next === current) {
    return current;
  }
  sessionMessageOutbox = next;
  writeMessageOutbox(next, storage);
  emitMessageOutboxChange();
  return next;
}

export function resetMessageOutboxSessionForTests(
  document: MessageOutboxDocument | null = null,
): void {
  sessionMessageOutbox = document;
  emitMessageOutboxChange();
}

function durableMessageOutboxDocument(document: MessageOutboxDocument): MessageOutboxDocument {
  const submissions = document.submissions.filter(messageOutboxSubmissionIsDurable);
  return submissions.length === document.submissions.length
    ? document
    : { ...document, submissions };
}

export function messageOutboxSubmissionIsDurable(submission: MessageOutboxSubmission): boolean {
  return submission.durable && messageOutboxSubmissionBaseIsDurable(submission);
}

function messageOutboxSubmissionBaseIsDurable(submission: MessageOutboxSubmission): boolean {
  if (messageOutboxSubmissionHasBootstrap(submission)) {
    return false;
  }
  const message = (submission.input as { readonly message?: { readonly attachments?: unknown } })
    .message;
  return Array.isArray(message?.attachments) && message.attachments.length === 0;
}

export function messageOutboxSubmissionHasBootstrap(submission: MessageOutboxSubmission): boolean {
  return submission.input.bootstrap !== undefined;
}

export function messageOutboxSubmissionHasAttachments(
  submission: MessageOutboxSubmission,
): boolean {
  return submission.input.message.attachments.length > 0;
}

export function messageOutboxSubmissionHasNonIdempotentDispatchPayload(
  submission: MessageOutboxSubmission,
): boolean {
  return (
    messageOutboxSubmissionHasBootstrap(submission) ||
    messageOutboxSubmissionHasAttachments(submission)
  );
}

export function messageOutboxSubmissionRequiresServerShell(
  submission: MessageOutboxSubmission,
): boolean {
  return !messageOutboxSubmissionHasBootstrap(submission);
}

function persistableOutboxSubmission(submission: MessageOutboxSubmission): MessageOutboxSubmission {
  return submission.status === "sending" ? { ...submission, status: "pending" } : submission;
}

export function writeMessageOutbox(
  document: MessageOutboxDocument,
  storage: StorageLike | undefined = browserStorage(),
) {
  if (!storage) return;
  try {
    const durableSubmissions = document.submissions
      .filter(messageOutboxSubmissionIsDurable)
      .map(persistableOutboxSubmission);
    if (durableSubmissions.length === 0) {
      storage.removeItem(MESSAGE_OUTBOX_STORAGE_KEY);
      return;
    }
    storage.setItem(
      MESSAGE_OUTBOX_STORAGE_KEY,
      JSON.stringify({ ...document, submissions: durableSubmissions }),
    );
  } catch {
    // localStorage can be full or unavailable; the in-memory outbox still protects this session.
  }
}

export function upsertOutboxSubmission(
  document: MessageOutboxDocument,
  submission: MessageOutboxSubmission,
): MessageOutboxDocument {
  const byMessageId = new Map(document.submissions.map((entry) => [entry.messageId, entry]));
  const existing = byMessageId.get(submission.messageId);
  byMessageId.set(submission.messageId, {
    ...submission,
    sequence: existing?.sequence ?? submission.sequence ?? nextOutboxSequence(document),
  });
  return {
    version: 1,
    submissions: [...byMessageId.values()].toSorted(compareOutboxSubmissions),
  };
}

function nextOutboxSequence(document: MessageOutboxDocument): number {
  return document.submissions.reduce(
    (next, submission, index) =>
      Math.max(
        next,
        typeof submission.sequence === "number" && Number.isFinite(submission.sequence)
          ? submission.sequence + 1
          : index + 1,
      ),
    0,
  );
}

export function updateOutboxSubmission(
  document: MessageOutboxDocument,
  messageId: MessageId,
  update: (submission: MessageOutboxSubmission) => MessageOutboxSubmission,
): MessageOutboxDocument {
  let changed = false;
  const submissions = document.submissions.map((submission) => {
    if (submission.messageId !== messageId) return submission;
    changed = true;
    return update(submission);
  });
  return changed ? { version: 1, submissions } : document;
}

export function markOutboxSubmissionsFailedNonretryable(
  document: MessageOutboxDocument,
  submissionsToFail: ReadonlyArray<MessageOutboxSubmission>,
  error: string,
  updatedAt: string,
): MessageOutboxDocument {
  return submissionsToFail.reduce(
    (nextDocument, submissionToFail) =>
      updateOutboxSubmission(nextDocument, submissionToFail.messageId, (submission) => ({
        ...submission,
        status: "failed",
        retryable: false,
        error,
        updatedAt,
      })),
    document,
  );
}

export function removeOutboxSubmission(
  document: MessageOutboxDocument,
  messageId: MessageId,
): MessageOutboxDocument {
  const submissions = document.submissions.filter((entry) => entry.messageId !== messageId);
  return submissions.length === document.submissions.length
    ? document
    : { version: 1, submissions };
}

export function outboxSubmissionsForThread(
  document: MessageOutboxDocument,
  environmentId: EnvironmentId,
  threadId: ThreadId,
): ReadonlyArray<MessageOutboxSubmission> {
  return document.submissions
    .filter((entry) => entry.environmentId === environmentId && entry.threadId === threadId)
    .toSorted(compareOutboxSubmissions);
}

export function compareOutboxSubmissions(
  left: MessageOutboxSubmission,
  right: MessageOutboxSubmission,
): number {
  const createdAtOrder = left.createdAt.localeCompare(right.createdAt);
  if (createdAtOrder !== 0) return createdAtOrder;
  const sequenceOrder = (left.sequence ?? 0) - (right.sequence ?? 0);
  return sequenceOrder !== 0 ? sequenceOrder : left.messageId.localeCompare(right.messageId);
}

export function messageOutboxHasSubmissionForThread(
  document: MessageOutboxDocument,
  environmentId: EnvironmentId,
  threadId: ThreadId,
): boolean {
  return document.submissions.some(
    (entry) =>
      entry.environmentId === environmentId &&
      entry.threadId === threadId &&
      !messageOutboxSubmissionIsTerminal(entry),
  );
}

export function messageOutboxHasBootstrapSubmissionForThread(
  document: MessageOutboxDocument,
  environmentId: EnvironmentId,
  threadId: ThreadId,
): boolean {
  return outboxSubmissionsForThread(document, environmentId, threadId).some(
    (submission) =>
      !messageOutboxSubmissionIsTerminal(submission) &&
      messageOutboxSubmissionHasBootstrap(submission),
  );
}

export function messageOutboxHasSessionOnlySubmissionForThread(
  document: MessageOutboxDocument,
  environmentId: EnvironmentId,
  threadId: ThreadId,
): boolean {
  return outboxSubmissionsForThread(document, environmentId, threadId).some(
    (submission) =>
      !messageOutboxSubmissionIsTerminal(submission) &&
      !messageOutboxSubmissionIsDurable(submission),
  );
}

export function messageOutboxSubmissionIsFirstForThread(
  document: MessageOutboxDocument,
  submission: MessageOutboxSubmission,
): boolean {
  return (
    outboxSubmissionsForThread(document, submission.environmentId, submission.threadId).find(
      (entry) => !messageOutboxSubmissionIsTerminal(entry),
    )?.messageId === submission.messageId
  );
}

export function messageOutboxSubmissionIsLaterInSameThread(
  submission: MessageOutboxSubmission,
  earlier: MessageOutboxSubmission,
): boolean {
  return (
    submission.environmentId === earlier.environmentId &&
    submission.threadId === earlier.threadId &&
    submission.messageId !== earlier.messageId &&
    compareOutboxSubmissions(submission, earlier) > 0
  );
}

export function messageOutboxSubmissionIsTerminal(submission: MessageOutboxSubmission): boolean {
  return submission.status === "failed" && submission.retryable === false;
}

export function reconcileOutboxWithServerMessages(
  document: MessageOutboxDocument,
  serverMessages: ReadonlyArray<Pick<OrchestrationMessage, "id">>,
): MessageOutboxReconcileResult {
  const serverMessageIds = new Set<MessageId>(serverMessages.map((message) => message.id));
  const deliveredMessageIds = new Set<MessageId>();
  const submissions = document.submissions.filter((entry) => {
    if (!serverMessageIds.has(entry.messageId)) {
      return true;
    }
    deliveredMessageIds.add(entry.messageId);
    return false;
  });
  const changed = submissions.length !== document.submissions.length;
  return {
    document: changed ? { version: 1, submissions } : document,
    deliveredMessageIds,
    changed,
  };
}

export function outboxSubmissionToChatMessage(submission: MessageOutboxSubmission): ChatMessage {
  return {
    id: submission.messageId,
    role: "user",
    text: submission.input.message.text,
    ...(submission.optimisticAttachments.length > 0
      ? { attachments: submission.optimisticAttachments }
      : {}),
    turnId: null,
    streaming: false,
    createdAt: submission.input.createdAt ?? submission.createdAt,
    updatedAt: submission.updatedAt,
    deliveryStatus: submission.status === "pending" ? "queued" : submission.status,
    deliveryError: submission.error,
    deliveryRetryable: submission.retryable !== false,
  };
}

export function mergePendingAndOutboxChatMessages(
  pendingMessages: ReadonlyArray<ChatMessage>,
  outboxSubmissions: ReadonlyArray<MessageOutboxSubmission>,
): ChatMessage[] {
  const pendingByMessageId = new Map(pendingMessages.map((message) => [message.id, message]));
  const outboxMessageIds = new Set(outboxSubmissions.map((submission) => submission.messageId));
  const outboxEntries = outboxSubmissions.map((submission, index) => ({
    kind: "outbox" as const,
    message:
      pendingByMessageId.get(submission.messageId) ?? outboxSubmissionToChatMessage(submission),
    submission,
    index,
  }));
  const localPendingEntries = pendingMessages
    .filter((message) => !outboxMessageIds.has(message.id))
    .map((message, index) => ({
      kind: "pending" as const,
      message,
      index,
    }));

  return [...outboxEntries, ...localPendingEntries]
    .toSorted((left, right) => {
      if (left.kind === "outbox" && right.kind === "outbox") {
        return compareOutboxSubmissions(left.submission, right.submission);
      }
      const leftCreatedAt =
        left.kind === "outbox" ? left.submission.createdAt : (left.message.createdAt ?? "");
      const rightCreatedAt =
        right.kind === "outbox" ? right.submission.createdAt : (right.message.createdAt ?? "");
      const createdAtOrder = leftCreatedAt.localeCompare(rightCreatedAt);
      if (createdAtOrder !== 0) return createdAtOrder;
      if (left.kind === right.kind) {
        return left.index - right.index;
      }
      return left.kind === "outbox" ? -1 : 1;
    })
    .map((entry) => entry.message);
}
