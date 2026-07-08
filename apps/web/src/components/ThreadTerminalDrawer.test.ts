import { describe, expect, it } from "vite-plus/test";

import {
  passiveTerminalStatusEffect,
  resolveTerminalSelectionActionPosition,
  shouldHandleTerminalSelectionMouseUp,
  terminalSelectionActionDelayForClickCount,
} from "./ThreadTerminalDrawer";
import {
  TERMINAL_RESET_SEQUENCE,
  TerminalWriteQueue,
  terminalBufferDelta,
  terminalBufferNeedsFullResync,
} from "./terminalWriteQueue";

class FakeTerminalWriteTarget {
  readonly writes: string[] = [];
  readonly callbacks: Array<() => void> = [];
  error: unknown;

  write(data: string, callback?: () => void): void {
    if (this.error !== undefined) {
      throw this.error;
    }
    this.writes.push(data);
    if (callback) {
      this.callbacks.push(callback);
    }
  }

  drainNext(): void {
    const callback = this.callbacks.shift();
    if (callback) {
      callback();
    }
  }
}

describe("resolveTerminalSelectionActionPosition", () => {
  it("treats passive terminal status changes as local UI messages only", () => {
    expect(passiveTerminalStatusEffect("closed")).toEqual({
      message: "Terminal closed",
      closeRemoteSession: false,
    });
    expect(passiveTerminalStatusEffect("exited")).toEqual({
      message: "Process exited",
      closeRemoteSession: false,
    });
    expect(passiveTerminalStatusEffect("running")).toEqual({
      message: null,
      closeRemoteSession: false,
    });
  });

  it("prefers the selection rect over the last pointer position", () => {
    expect(
      resolveTerminalSelectionActionPosition({
        bounds: { left: 100, top: 50, width: 500, height: 220 },
        selectionRect: { right: 260, bottom: 140 },
        pointer: { x: 520, y: 200 },
        viewport: { width: 1024, height: 768 },
      }),
    ).toEqual({
      x: 260,
      y: 144,
    });
  });

  it("falls back to the pointer position when no selection rect is available", () => {
    expect(
      resolveTerminalSelectionActionPosition({
        bounds: { left: 100, top: 50, width: 500, height: 220 },
        selectionRect: null,
        pointer: { x: 180, y: 130 },
        viewport: { width: 1024, height: 768 },
      }),
    ).toEqual({
      x: 180,
      y: 130,
    });
  });

  it("clamps the pointer fallback into the terminal drawer bounds", () => {
    expect(
      resolveTerminalSelectionActionPosition({
        bounds: { left: 100, top: 50, width: 500, height: 220 },
        selectionRect: null,
        pointer: { x: 720, y: 340 },
        viewport: { width: 1024, height: 768 },
      }),
    ).toEqual({
      x: 600,
      y: 270,
    });

    expect(
      resolveTerminalSelectionActionPosition({
        bounds: { left: 100, top: 50, width: 500, height: 220 },
        selectionRect: null,
        pointer: { x: 40, y: 20 },
        viewport: { width: 1024, height: 768 },
      }),
    ).toEqual({
      x: 100,
      y: 50,
    });
  });

  it("delays multi-click selection actions so triple-click selection can complete", () => {
    expect(terminalSelectionActionDelayForClickCount(1)).toBe(0);
    expect(terminalSelectionActionDelayForClickCount(2)).toBe(260);
    expect(terminalSelectionActionDelayForClickCount(3)).toBe(260);
  });

  it("only handles mouseup when the selection gesture started in the terminal", () => {
    expect(shouldHandleTerminalSelectionMouseUp(true, 0)).toBe(true);
    expect(shouldHandleTerminalSelectionMouseUp(false, 0)).toBe(false);
    expect(shouldHandleTerminalSelectionMouseUp(true, 1)).toBe(false);
  });
});

describe("terminalBufferNeedsFullResync", () => {
  it("uses deltas for small append-only updates", () => {
    expect(
      terminalBufferNeedsFullResync({
        currentBuffer: "abcdef",
        previousBuffer: "abc",
        force: false,
        largeDeltaThreshold: 10,
      }),
    ).toBe(false);
    expect(
      terminalBufferDelta({
        currentBuffer: "abcdef",
        previousBuffer: "abc",
      }),
    ).toBe("def");
  });

  it("resyncs on forced, non-append, truncated, or large deltas", () => {
    expect(
      terminalBufferNeedsFullResync({
        currentBuffer: "abcdef",
        previousBuffer: "abc",
        force: true,
      }),
    ).toBe(true);
    expect(
      terminalBufferNeedsFullResync({
        currentBuffer: "zabcdef",
        previousBuffer: "abc",
        force: false,
      }),
    ).toBe(true);
    expect(
      terminalBufferNeedsFullResync({
        currentBuffer: "ab",
        previousBuffer: "abc",
        force: false,
      }),
    ).toBe(true);
    expect(
      terminalBufferNeedsFullResync({
        currentBuffer: "abc123456",
        previousBuffer: "abc",
        force: false,
        largeDeltaThreshold: 5,
      }),
    ).toBe(true);
  });
});

describe("TerminalWriteQueue", () => {
  it("chunks writes and waits for xterm callbacks before sending the next chunk", () => {
    const terminal = new FakeTerminalWriteTarget();
    const queue = new TerminalWriteQueue(terminal, {
      chunkSize: 4,
      scheduleDrain: (drain) => drain(),
    });

    queue.enqueue("abcdefghij");

    expect(terminal.writes).toEqual(["abcd"]);
    terminal.drainNext();
    expect(terminal.writes).toEqual(["abcd", "efgh"]);
    terminal.drainNext();
    expect(terminal.writes).toEqual(["abcd", "efgh", "ij"]);
  });

  it("clears queued deltas when a full buffer resync is requested", () => {
    const terminal = new FakeTerminalWriteTarget();
    const queue = new TerminalWriteQueue(terminal, {
      chunkSize: 20,
      scheduleDrain: (drain) => drain(),
    });

    queue.enqueue("delta-1");
    queue.enqueue("delta-2");
    queue.writeTerminalBuffer("full-state");
    terminal.drainNext();

    expect(terminal.writes).toEqual(["delta-1", TERMINAL_RESET_SEQUENCE]);
    terminal.drainNext();
    expect(terminal.writes).toEqual(["delta-1", TERMINAL_RESET_SEQUENCE, "full-state"]);
  });

  it("turns write failures into resync requests instead of throwing", () => {
    const terminal = new FakeTerminalWriteTarget();
    const failures: unknown[] = [];
    const error = new Error("write data discarded, use flow control to avoid losing data");
    terminal.error = error;
    const queue = new TerminalWriteQueue(terminal, {
      chunkSize: 20,
      onWriteFailure: (failure) => failures.push(failure),
      scheduleDrain: (drain) => drain(),
    });

    expect(() => queue.enqueue("heavy-output")).not.toThrow();
    expect(failures).toEqual([error]);

    terminal.error = undefined;
    queue.writeTerminalBuffer("replayed");
    expect(terminal.writes).toEqual([TERMINAL_RESET_SEQUENCE]);
    terminal.drainNext();
    expect(terminal.writes).toEqual([TERMINAL_RESET_SEQUENCE, "replayed"]);
  });
});
