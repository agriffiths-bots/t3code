import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  MESSAGE_OUTBOX_STORAGE_KEY,
  clientTurnCommandId,
  emptyMessageOutbox,
  getMessageOutboxSnapshot,
  messageOutboxHasBootstrapSubmissionForThread,
  messageOutboxHasSubmissionForThread,
  messageOutboxHasSessionOnlySubmissionForThread,
  messageOutboxSubmissionIsFirstForThread,
  messageOutboxSubmissionIsLaterInSameThread,
  markOutboxSubmissionsFailedNonretryable,
  normalizeMessageOutbox,
  messageOutboxSubmissionHasNonIdempotentDispatchPayload,
  messageOutboxSubmissionHasBootstrap,
  messageOutboxSubmissionIsDurable,
  messageOutboxSubmissionRequiresServerShell,
  mergePendingAndOutboxChatMessages,
  outboxSubmissionsForThread,
  outboxSubmissionToChatMessage,
  readMessageOutbox,
  reconcileOutboxWithServerMessages,
  removeOutboxSubmission,
  resetMessageOutboxSessionForTests,
  subscribeMessageOutbox,
  updateOutboxSubmission,
  updateSessionMessageOutbox,
  upsertOutboxSubmission,
  writeMessageOutbox,
  type MessageOutboxSubmission,
} from "./messageOutbox";

function memoryStorage(initial: Record<string, string> = {}): Storage {
  const data = new Map(Object.entries(initial));
  return {
    get length() {
      return data.size;
    },
    clear: () => data.clear(),
    getItem: (key) => data.get(key) ?? null,
    key: (index) => [...data.keys()][index] ?? null,
    removeItem: (key) => {
      data.delete(key);
    },
    setItem: (key, value) => {
      data.set(key, value);
    },
  };
}

function submission(input: {
  messageId: string;
  commandId?: string;
  status?: MessageOutboxSubmission["status"];
  attempts?: number;
  durable?: boolean;
  retryable?: boolean;
  attachments?: MessageOutboxSubmission["input"]["message"]["attachments"];
  bootstrap?: MessageOutboxSubmission["input"]["bootstrap"];
  optimisticAttachments?: MessageOutboxSubmission["optimisticAttachments"];
  sequence?: number;
  createdAt?: string;
  updatedAt?: string;
}): MessageOutboxSubmission {
  const messageId = MessageId.make(input.messageId);
  const commandId = CommandId.make(input.commandId ?? `client:turn:${messageId}`);
  const attachments = input.attachments ?? [];
  const createdAt = input.createdAt ?? "2026-07-03T12:00:00.000Z";
  const updatedAt = input.updatedAt ?? createdAt;
  return {
    environmentId: EnvironmentId.make("env-1"),
    threadId: ThreadId.make("thread-1"),
    messageId,
    commandId,
    durable: input.durable ?? true,
    retryable: input.retryable ?? true,
    status: input.status ?? "pending",
    attempts: input.attempts ?? 0,
    error: null,
    createdAt,
    updatedAt,
    ...(input.sequence !== undefined ? { sequence: input.sequence } : {}),
    optimisticAttachments: input.optimisticAttachments ?? [],
    input: {
      commandId,
      threadId: ThreadId.make("thread-1"),
      message: {
        messageId,
        role: "user",
        text: "hello",
        attachments,
      },
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.5",
        options: [],
      },
      titleSeed: "hello",
      runtimeMode: "full-access",
      interactionMode: "default",
      ...(input.bootstrap !== undefined ? { bootstrap: input.bootstrap } : {}),
      createdAt,
    },
  };
}

afterEach(() => {
  resetMessageOutboxSessionForTests();
  vi.unstubAllGlobals();
});

describe("messageOutbox", () => {
  it("uses a stable client command id derived from the message id", () => {
    const messageId = MessageId.make("message-1");

    expect(clientTurnCommandId(messageId)).toBe(clientTurnCommandId(messageId));
    expect(clientTurnCommandId(messageId)).toBe("client:turn:message-1");
  });

  it("round-trips valid submissions through localStorage and tolerates malformed data", () => {
    const storage = memoryStorage({
      [MESSAGE_OUTBOX_STORAGE_KEY]: "{not-json",
    });

    expect(readMessageOutbox(storage)).toEqual(emptyMessageOutbox());

    const document = upsertOutboxSubmission(emptyMessageOutbox(), submission({ messageId: "m-1" }));
    writeMessageOutbox(document, storage);

    expect(readMessageOutbox(storage).submissions).toHaveLength(1);
    expect(readMessageOutbox(storage).submissions[0]?.messageId).toBe("m-1");
  });

  it("falls back to an in-memory outbox when browser localStorage is unavailable", () => {
    const throwingWindow = {};
    Object.defineProperty(throwingWindow, "localStorage", {
      get() {
        throw new Error("storage blocked");
      },
    });
    vi.stubGlobal("window", throwingWindow);

    expect(readMessageOutbox()).toEqual(emptyMessageOutbox());
    expect(() =>
      writeMessageOutbox(
        upsertOutboxSubmission(emptyMessageOutbox(), submission({ messageId: "m-1" })),
      ),
    ).not.toThrow();
  });

  it("persists only durable text submissions to localStorage", () => {
    const storage = memoryStorage();
    const textSubmission = submission({ messageId: "m-text" });
    const bootstrapSubmission = submission({
      messageId: "m-bootstrap",
      bootstrap: {
        prepareWorktree: {
          projectCwd: "/repo",
          baseBranch: "main",
          branch: "feature/bootstrap",
        },
      },
    });
    const imageSubmission = submission({
      messageId: "m-image",
      attachments: [
        {
          type: "image",
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 4,
          dataUrl: "data:image/png;base64,AAAA",
        },
      ],
      optimisticAttachments: [
        {
          type: "image",
          id: "image-1",
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 4,
          previewUrl: "blob:preview",
        },
      ],
    });
    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(
        upsertOutboxSubmission(emptyMessageOutbox(), textSubmission),
        bootstrapSubmission,
      ),
      imageSubmission,
    );

    writeMessageOutbox(document, storage);

    const persisted = readMessageOutbox(storage);
    expect(messageOutboxSubmissionIsDurable(textSubmission)).toBe(true);
    expect(messageOutboxSubmissionHasBootstrap(bootstrapSubmission)).toBe(true);
    expect(messageOutboxSubmissionIsDurable(bootstrapSubmission)).toBe(false);
    expect(messageOutboxSubmissionIsDurable(imageSubmission)).toBe(false);
    expect(persisted.submissions.map((entry) => entry.messageId)).toEqual(["m-text"]);
  });

  it("persists in-flight durable submissions as pending after reload", () => {
    const storage = memoryStorage();
    const document = upsertOutboxSubmission(
      emptyMessageOutbox(),
      submission({ messageId: "m-sending", status: "sending", attempts: 1 }),
    );

    writeMessageOutbox(document, storage);

    const persistedRaw = JSON.parse(storage.getItem(MESSAGE_OUTBOX_STORAGE_KEY) ?? "{}");
    expect(persistedRaw.submissions[0]?.status).toBe("pending");
    expect(readMessageOutbox(storage).submissions[0]?.status).toBe("pending");
  });

  it("does not persist text submissions that depend on session-only queued setup", () => {
    const storage = memoryStorage();
    const document = upsertOutboxSubmission(
      emptyMessageOutbox(),
      submission({ messageId: "m-dependent-text", durable: false }),
    );

    writeMessageOutbox(document, storage);

    expect(storage.getItem(MESSAGE_OUTBOX_STORAGE_KEY)).toBeNull();
    expect(readMessageOutbox(storage).submissions).toHaveLength(0);
  });

  it("keeps session-only submissions in memory without persisting them", () => {
    const storage = memoryStorage();
    const listener = vi.fn();
    const unsubscribe = subscribeMessageOutbox(listener);
    const sessionOnly = submission({
      messageId: "m-session-only",
      durable: false,
      attachments: [
        {
          type: "image",
          name: "queued.png",
          mimeType: "image/png",
          sizeBytes: 12,
          dataUrl: "data:image/png;base64,abc",
        },
      ],
    });

    updateSessionMessageOutbox(
      (document) => upsertOutboxSubmission(document, sessionOnly),
      storage,
    );

    expect(listener).toHaveBeenCalledTimes(1);
    expect(getMessageOutboxSnapshot().submissions.map((entry) => entry.messageId)).toEqual([
      "m-session-only",
    ]);
    expect(storage.getItem(MESSAGE_OUTBOX_STORAGE_KEY)).toBeNull();

    unsubscribe();
  });

  it("hydrates session updates from the same injected storage it writes", () => {
    const storage = memoryStorage();
    const existingDurable = submission({ messageId: "m-existing" });
    writeMessageOutbox(upsertOutboxSubmission(emptyMessageOutbox(), existingDurable), storage);

    updateSessionMessageOutbox(
      (document) => upsertOutboxSubmission(document, submission({ messageId: "m-next" })),
      storage,
    );

    expect(getMessageOutboxSnapshot().submissions.map((entry) => entry.messageId)).toEqual([
      "m-existing",
      "m-next",
    ]);
    expect(readMessageOutbox(storage).submissions.map((entry) => entry.messageId)).toEqual([
      "m-existing",
      "m-next",
    ]);
  });

  it("drops legacy persisted non-durable submissions on reload", () => {
    const storage = memoryStorage({
      [MESSAGE_OUTBOX_STORAGE_KEY]: JSON.stringify({
        version: 1,
        submissions: [
          submission({ messageId: "m-text" }),
          submission({
            messageId: "m-bootstrap",
            bootstrap: {
              prepareWorktree: {
                projectCwd: "/repo",
                baseBranch: "main",
                branch: "feature/bootstrap",
              },
            },
          }),
          submission({
            messageId: "m-image",
            attachments: [
              {
                type: "image",
                name: "image.png",
                mimeType: "image/png",
                sizeBytes: 4,
                dataUrl: "data:image/png;base64,AAAA",
              },
            ],
          }),
        ],
      }),
    });

    expect(readMessageOutbox(storage).submissions.map((entry) => entry.messageId)).toEqual([
      "m-text",
    ]);
  });

  it("normalizes legacy persisted sending submissions to pending", () => {
    const normalized = normalizeMessageOutbox({
      version: 1,
      submissions: [submission({ messageId: "m-sending", status: "sending" })],
    });

    expect(normalized.submissions[0]?.status).toBe("pending");
  });

  it("drops persisted submissions with invalid or mismatched turn input", () => {
    const malformed = submission({ messageId: "m-malformed" });
    const mismatched = submission({ messageId: "m-mismatched" });

    const normalized = normalizeMessageOutbox({
      version: 1,
      submissions: [
        {
          ...malformed,
          input: {
            ...malformed.input,
            message: {
              messageId: malformed.messageId,
              role: "user",
              attachments: [],
            },
          },
        },
        {
          ...mismatched,
          input: {
            ...mismatched.input,
            message: {
              ...mismatched.input.message,
              messageId: MessageId.make("m-other"),
            },
          },
        },
      ],
    });

    expect(normalized.submissions).toHaveLength(0);
  });

  it("dedupes by message id and preserves the latest submission state", () => {
    const first = submission({ messageId: "m-1", status: "pending" });
    const second = { ...first, status: "failed" as const, error: "offline" };

    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), first),
      second,
    );

    expect(document.submissions).toHaveLength(1);
    expect(document.submissions[0]?.status).toBe("failed");
    expect(document.submissions[0]?.error).toBe("offline");
  });

  it("detects existing same-thread submissions and queued bootstrap work", () => {
    const plain = submission({ messageId: "m-1" });
    const bootstrap = submission({
      messageId: "m-bootstrap",
      bootstrap: {
        prepareWorktree: {
          projectCwd: "/repo",
          baseBranch: "main",
          branch: "feature/bootstrap",
        },
      },
    });
    const otherThread = {
      ...submission({ messageId: "m-other" }),
      threadId: ThreadId.make("thread-2"),
    };
    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(upsertOutboxSubmission(emptyMessageOutbox(), plain), otherThread),
      bootstrap,
    );

    expect(
      messageOutboxHasSubmissionForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-1"),
      ),
    ).toBe(true);
    expect(
      messageOutboxHasBootstrapSubmissionForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-1"),
      ),
    ).toBe(true);
    expect(
      messageOutboxHasBootstrapSubmissionForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-2"),
      ),
    ).toBe(false);
    expect(
      messageOutboxHasSessionOnlySubmissionForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-1"),
      ),
    ).toBe(true);
  });

  it("classifies queue drain and attempted-dispatch idempotency policy", () => {
    const text = submission({ messageId: "m-text" });
    const bootstrap = submission({
      messageId: "m-bootstrap",
      bootstrap: {
        prepareWorktree: {
          projectCwd: "/repo",
          baseBranch: "main",
          branch: "feature/bootstrap",
        },
      },
    });
    const image = submission({
      messageId: "m-image",
      attachments: [
        {
          type: "image",
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 4,
          dataUrl: "data:image/png;base64,AAAA",
        },
      ],
    });

    expect(messageOutboxSubmissionRequiresServerShell(text)).toBe(true);
    expect(messageOutboxSubmissionRequiresServerShell(image)).toBe(true);
    expect(messageOutboxSubmissionRequiresServerShell(bootstrap)).toBe(false);
    expect(messageOutboxSubmissionHasNonIdempotentDispatchPayload(text)).toBe(false);
    expect(messageOutboxSubmissionHasNonIdempotentDispatchPayload(image)).toBe(true);
    expect(messageOutboxSubmissionHasNonIdempotentDispatchPayload(bootstrap)).toBe(true);
  });

  it("identifies only the oldest unresolved submission as first for a thread", () => {
    const first = submission({ messageId: "m-1" });
    const second = {
      ...submission({ messageId: "m-2" }),
      createdAt: "2026-07-03T12:00:01.000Z",
      updatedAt: "2026-07-03T12:00:01.000Z",
    };
    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), second),
      first,
    );

    expect(messageOutboxSubmissionIsFirstForThread(document, first)).toBe(true);
    expect(messageOutboxSubmissionIsFirstForThread(document, second)).toBe(false);
  });

  it("preserves insertion order for same-millisecond submissions", () => {
    const first = submission({ messageId: "z-later-lexical" });
    const second = submission({ messageId: "a-earlier-lexical" });
    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), first),
      second,
    );

    expect(
      outboxSubmissionsForThread(document, first.environmentId, first.threadId).map(
        (entry) => entry.messageId,
      ),
    ).toEqual([first.messageId, second.messageId]);
    expect(document.submissions.map((entry) => entry.sequence)).toEqual([0, 1]);
  });

  it("uses insertion order when classifying later same-millisecond submissions", () => {
    const first = submission({ messageId: "z-later-lexical" });
    const second = submission({ messageId: "a-earlier-lexical" });
    const [queuedFirst, queuedSecond] = upsertOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), first),
      second,
    ).submissions;

    expect(messageOutboxSubmissionIsLaterInSameThread(queuedSecond!, queuedFirst!)).toBe(true);
    expect(messageOutboxSubmissionIsLaterInSameThread(queuedFirst!, queuedSecond!)).toBe(false);
  });

  it("uses persisted order as the sequence fallback for legacy outbox rows", () => {
    const first = submission({ messageId: "z-later-lexical" });
    const second = submission({ messageId: "a-earlier-lexical" });
    const normalized = normalizeMessageOutbox({
      version: 1,
      submissions: [
        { ...first, sequence: undefined },
        { ...second, sequence: undefined },
      ],
    });

    expect(normalized.submissions.map((entry) => entry.messageId)).toEqual([
      first.messageId,
      second.messageId,
    ]);
    expect(normalized.submissions.map((entry) => entry.sequence)).toEqual([0, 1]);
  });

  it("does not let terminal nonretryable failures block later submissions", () => {
    const terminal = submission({
      messageId: "m-terminal",
      status: "failed",
      retryable: false,
    });
    const next = {
      ...submission({ messageId: "m-next" }),
      createdAt: "2026-07-03T12:00:01.000Z",
      updatedAt: "2026-07-03T12:00:01.000Z",
    };
    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), terminal),
      next,
    );

    expect(messageOutboxSubmissionIsFirstForThread(document, next)).toBe(true);
  });

  it("ignores terminal nonretryable failures when classifying active queued work", () => {
    const terminalBootstrap = submission({
      messageId: "m-terminal-bootstrap",
      status: "failed",
      retryable: false,
      durable: false,
      bootstrap: {
        createThread: {
          projectId: ProjectId.make("project-1"),
          title: "Draft",
          modelSelection: {
            instanceId: ProviderInstanceId.make("codex_default"),
            model: "gpt-5.5",
            options: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: "2026-07-03T12:00:00.000Z",
        },
      },
    });
    const document = upsertOutboxSubmission(emptyMessageOutbox(), terminalBootstrap);

    expect(
      outboxSubmissionsForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-1"),
      ).map((entry) => entry.messageId),
    ).toEqual(["m-terminal-bootstrap"]);
    expect(
      messageOutboxHasSubmissionForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-1"),
      ),
    ).toBe(false);
    expect(
      messageOutboxHasBootstrapSubmissionForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-1"),
      ),
    ).toBe(false);
    expect(
      messageOutboxHasSessionOnlySubmissionForThread(
        document,
        EnvironmentId.make("env-1"),
        ThreadId.make("thread-1"),
      ),
    ).toBe(false);
  });

  it("reconciles delivered messages by client message id", () => {
    const pending = submission({ messageId: "m-1" });
    const other = submission({ messageId: "m-2" });
    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), pending),
      other,
    );

    const result = reconcileOutboxWithServerMessages(document, [
      {
        id: MessageId.make("m-1"),
      },
    ]);

    expect(result.changed).toBe(true);
    expect(result.deliveredMessageIds.has(MessageId.make("m-1"))).toBe(true);
    expect(result.document.submissions.map((entry) => entry.messageId)).toEqual(["m-2"]);
  });

  it("retains the same command id across retry state transitions", () => {
    const original = submission({ messageId: "m-1", commandId: "client:turn:m-1" });
    const sending = updateOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), original),
      original.messageId,
      (entry) => ({
        ...entry,
        status: "sending",
        attempts: entry.attempts + 1,
        updatedAt: "2026-07-03T12:00:01.000Z",
      }),
    );

    expect(sending.submissions[0]?.commandId).toBe("client:turn:m-1");
    expect(sending.submissions[0]?.attempts).toBe(1);
  });

  it("projects queued outbox submissions as pending user messages", () => {
    const message = outboxSubmissionToChatMessage(
      submission({ messageId: "m-queued", status: "pending" }),
    );

    expect(message).toMatchObject({
      id: "m-queued",
      role: "user",
      text: "hello",
      deliveryStatus: "queued",
    });
  });

  it("projects nonretryable outbox submissions into chat messages", () => {
    const message = outboxSubmissionToChatMessage(
      submission({
        messageId: "m-nonretryable",
        status: "failed",
        retryable: false,
      }),
    );

    expect(message).toMatchObject({
      id: "m-nonretryable",
      deliveryStatus: "failed",
      deliveryRetryable: false,
    });
  });

  it("merges pending and restored outbox messages in submission order", () => {
    const restoredFirst = submission({
      messageId: "m-first",
      createdAt: "2026-07-03T12:00:00.000Z",
      sequence: 1,
    });
    const newSecond = submission({
      messageId: "m-second",
      createdAt: "2026-07-03T12:00:01.000Z",
      sequence: 2,
    });
    const pendingSecond = {
      ...outboxSubmissionToChatMessage(newSecond),
      deliveryStatus: "queued" as const,
    };

    expect(mergePendingAndOutboxChatMessages([pendingSecond], [restoredFirst, newSecond])).toEqual([
      outboxSubmissionToChatMessage(restoredFirst),
      pendingSecond,
    ]);
  });

  it("uses outbox sequence for queued messages created in the same millisecond", () => {
    const laterPendingSubmission = submission({
      messageId: "m-second",
      createdAt: "2026-07-03T12:00:00.000Z",
      sequence: 2,
    });
    const earlierRestoredSubmission = submission({
      messageId: "m-first",
      createdAt: "2026-07-03T12:00:00.000Z",
      sequence: 1,
    });
    const pendingSecond = outboxSubmissionToChatMessage(laterPendingSubmission);

    expect(
      mergePendingAndOutboxChatMessages(
        [pendingSecond],
        [laterPendingSubmission, earlierRestoredSubmission],
      ).map((message) => message.id),
    ).toEqual(["m-first", "m-second"]);
  });

  it("preserves dependent failures in the outbox as terminal nonretryable rows", () => {
    const first = submission({ messageId: "m-1" });
    const dependent = submission({ messageId: "m-2" });
    const document = upsertOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), first),
      dependent,
    );

    const failed = markOutboxSubmissionsFailedNonretryable(
      document,
      [dependent],
      "Earlier queued work is unresolved.",
      "2026-07-03T12:00:02.000Z",
    );

    expect(failed.submissions).toHaveLength(2);
    expect(failed.submissions[1]).toMatchObject({
      messageId: dependent.messageId,
      status: "failed",
      retryable: false,
      error: "Earlier queued work is unresolved.",
      updatedAt: "2026-07-03T12:00:02.000Z",
    });
    expect(messageOutboxHasSubmissionForThread(failed, first.environmentId, first.threadId)).toBe(
      true,
    );
    expect(outboxSubmissionToChatMessage(failed.submissions[1]!)).toMatchObject({
      id: dependent.messageId,
      deliveryStatus: "failed",
      deliveryRetryable: false,
    });
  });

  it("removes acknowledged submissions", () => {
    const original = submission({ messageId: "m-1" });
    const document = removeOutboxSubmission(
      upsertOutboxSubmission(emptyMessageOutbox(), original),
      original.messageId,
    );

    expect(document.submissions).toHaveLength(0);
  });

  it("normalizes duplicate persisted entries defensively", () => {
    const normalized = normalizeMessageOutbox({
      version: 1,
      submissions: [
        submission({ messageId: "m-1", status: "pending" }),
        submission({ messageId: "m-1", status: "failed" }),
        { bad: "row" },
      ],
    });

    expect(normalized.submissions).toHaveLength(1);
    expect(normalized.submissions[0]?.status).toBe("failed");
  });
});
