import { CommandId, EventId, ThreadId, type OrchestrationEvent } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { isReplayEventSupported, isThreadDetailEvent } from "./ws.ts";

const threadId = ThreadId.make("thread-settlement-stream");
const occurredAt = "2026-07-22T00:00:00.000Z";

function settlementEvent(type: "thread.settled" | "thread.unsettled"): OrchestrationEvent {
  const base = {
    sequence: 1,
    eventId: EventId.make(`event-${type}`),
    aggregateKind: "thread" as const,
    aggregateId: threadId,
    occurredAt,
    commandId: CommandId.make(`command-${type}`),
    causationEventId: null,
    correlationId: CommandId.make(`command-${type}`),
    metadata: {},
  };
  return type === "thread.settled"
    ? {
        ...base,
        type,
        payload: { threadId, settledAt: occurredAt, updatedAt: occurredAt },
      }
    : {
        ...base,
        type,
        payload: { threadId, reason: "user", updatedAt: occurredAt },
      };
}

describe("isThreadDetailEvent settlement lifecycle", () => {
  it("requires explicit negotiation before delivering new settlement variants", () => {
    expect(isThreadDetailEvent(settlementEvent("thread.settled"))).toBe(false);
    expect(isThreadDetailEvent(settlementEvent("thread.unsettled"))).toBe(false);
    expect(isThreadDetailEvent(settlementEvent("thread.settled"), true)).toBe(true);
    expect(isThreadDetailEvent(settlementEvent("thread.unsettled"), true)).toBe(true);
  });
});

describe("isReplayEventSupported settlement lifecycle", () => {
  it("requires explicit negotiation before replaying new settlement variants", () => {
    expect(isReplayEventSupported(settlementEvent("thread.settled"))).toBe(false);
    expect(isReplayEventSupported(settlementEvent("thread.unsettled"))).toBe(false);
    expect(isReplayEventSupported(settlementEvent("thread.settled"), true)).toBe(true);
    expect(isReplayEventSupported(settlementEvent("thread.unsettled"), true)).toBe(true);
  });
});
