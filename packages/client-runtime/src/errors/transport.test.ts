import { describe, expect, it } from "vite-plus/test";

import {
  INTERRUPTED_TURN_ERROR_MESSAGE,
  isClaudeInterruptedTurnDiagnosticMessage,
  isTransportConnectionErrorMessage,
  sanitizeThreadErrorMessage,
} from "./transport.ts";

describe("isTransportConnectionErrorMessage", () => {
  it("returns true for SocketCloseError", () => {
    expect(isTransportConnectionErrorMessage("SocketCloseError: connection reset")).toBe(true);
  });

  it("returns true for SocketOpenError", () => {
    expect(isTransportConnectionErrorMessage("SocketOpenError: ECONNREFUSED")).toBe(true);
  });

  it("returns true for React Native disconnected socket errors", () => {
    expect(
      isTransportConnectionErrorMessage(
        "The operation couldn't be completed. Socket is not connected",
      ),
    ).toBe(true);
  });

  it("recognizes connection errors emitted by the Effect RPC session", () => {
    expect(isTransportConnectionErrorMessage("Test environment disconnected.")).toBe(true);
    expect(
      isTransportConnectionErrorMessage(
        "Test environment could not establish a WebSocket connection.",
      ),
    ).toBe(true);
    expect(isTransportConnectionErrorMessage("Test environment is not connected.")).toBe(true);
    expect(isTransportConnectionErrorMessage("ClientProtocolError: socket closed")).toBe(true);
  });

  it("returns true for the T3 server WebSocket message", () => {
    expect(isTransportConnectionErrorMessage("Unable to connect to the T3 server WebSocket.")).toBe(
      true,
    );
  });

  it("returns true for ping timeout", () => {
    expect(isTransportConnectionErrorMessage("ping timeout")).toBe(true);
  });

  it("returns false for business logic errors", () => {
    expect(isTransportConnectionErrorMessage("Thread not found")).toBe(false);
    expect(isTransportConnectionErrorMessage("Invalid model selection")).toBe(false);
  });

  it("returns false for null, undefined, and empty strings", () => {
    expect(isTransportConnectionErrorMessage(null)).toBe(false);
    expect(isTransportConnectionErrorMessage(undefined)).toBe(false);
    expect(isTransportConnectionErrorMessage("")).toBe(false);
    expect(isTransportConnectionErrorMessage("   ")).toBe(false);
  });
});

describe("isClaudeInterruptedTurnDiagnosticMessage", () => {
  it("recognizes Claude SDK abort diagnostics", () => {
    expect(
      isClaudeInterruptedTurnDiagnosticMessage(
        "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use",
      ),
    ).toBe(true);
    expect(
      isClaudeInterruptedTurnDiagnosticMessage(
        "provider result terminal_reason=aborted_streaming stop_reason=tool_use",
      ),
    ).toBe(true);
  });

  it("does not match unrelated provider errors", () => {
    expect(isClaudeInterruptedTurnDiagnosticMessage("Claude API key missing")).toBe(false);
  });
});

describe("sanitizeThreadErrorMessage", () => {
  it("strips transport errors", () => {
    expect(sanitizeThreadErrorMessage("SocketCloseError: oops")).toBeNull();
  });

  it("suppresses Claude non-error steer diagnostics to avoid duplicate retry advice", () => {
    expect(
      sanitizeThreadErrorMessage(
        "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use",
        { turnState: "interrupted" },
      ),
    ).toBeNull();
    expect(
      sanitizeThreadErrorMessage("[ede_diagnostic] turn aborted (steer) stop_reason=tool_use", {
        turnState: "interrupted",
      }),
    ).toBeNull();
  });

  it("preserves steer-shaped diagnostics for failed Claude turns", () => {
    const errors = [
      "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use",
      [
        "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use",
        "Claude execution failed after the steer boundary.",
      ].join("\n"),
      "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use Claude execution failed",
    ];
    for (const error of errors) {
      expect(sanitizeThreadErrorMessage(error, { turnState: "error" })).toBe(error);
    }
  });

  it("does not suppress a steer diagnostic with a companion error", () => {
    const error = [
      "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use",
      "Claude execution failed after the steer boundary.",
    ].join("\n");
    expect(sanitizeThreadErrorMessage(error, { turnState: "interrupted" })).toBe(
      INTERRUPTED_TURN_ERROR_MESSAGE,
    );
    expect(
      sanitizeThreadErrorMessage(
        "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use Claude execution failed",
        { turnState: "interrupted" },
      ),
    ).toBe(INTERRUPTED_TURN_ERROR_MESSAGE);
  });

  it("does not suppress near-miss Claude diagnostics", () => {
    expect(
      sanitizeThreadErrorMessage(
        "[ede_diagnostic] result_type=assistant last_content_type=text stop_reason=tool_use",
      ),
    ).toBe(INTERRUPTED_TURN_ERROR_MESSAGE);
    expect(sanitizeThreadErrorMessage("[ede_diagnostic] result_type=user status=failed")).toBe(
      "[ede_diagnostic] result_type=user status=failed",
    );
  });

  it("retains friendly copy for explicit Claude request aborts", () => {
    expect(sanitizeThreadErrorMessage("Error: Request was aborted.")).toBe(
      INTERRUPTED_TURN_ERROR_MESSAGE,
    );
  });

  it("preserves non-transport errors", () => {
    expect(sanitizeThreadErrorMessage("Thread not found")).toBe("Thread not found");
    expect(sanitizeThreadErrorMessage("Select a base branch before sending.")).toBe(
      "Select a base branch before sending.",
    );
  });

  it("returns null for null/undefined", () => {
    expect(sanitizeThreadErrorMessage(null)).toBeNull();
    expect(sanitizeThreadErrorMessage(undefined)).toBeNull();
  });
});
