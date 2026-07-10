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

  it("suppresses Claude user-steer abort diagnostics to avoid duplicate retry advice", () => {
    expect(
      sanitizeThreadErrorMessage(
        "[ede_diagnostic] result_type=user is_error=false last_content_type=n/a stop_reason=tool_use",
      ),
    ).toBeNull();
    expect(
      sanitizeThreadErrorMessage(
        "[ede_diagnostic] result_type=user stop_reason=tool_use is_error=false",
      ),
    ).toBeNull();
    expect(
      sanitizeThreadErrorMessage("[ede_diagnostic] result_type=user terminal_reason=aborted_tools"),
    ).toBeNull();
    expect(sanitizeThreadErrorMessage("[ede_diagnostic] result_type=user turn aborted")).toBeNull();
  });

  it("does not suppress Claude diagnostics without the user-steer result type", () => {
    expect(
      sanitizeThreadErrorMessage(
        "[ede_diagnostic] result_type=assistant last_content_type=text stop_reason=tool_use",
      ),
    ).toBe(INTERRUPTED_TURN_ERROR_MESSAGE);
  });

  it("does not suppress user diagnostics without a known steer-abort marker", () => {
    const diagnostics = [
      "[ede_diagnostic] result_type=user status=failed",
      "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=null",
    ];
    for (const diagnostic of diagnostics) {
      expect(sanitizeThreadErrorMessage(diagnostic)).toBe(diagnostic);
    }
  });

  it("does not suppress an ambiguous user tool-use diagnostic without a clean result marker", () => {
    expect(
      sanitizeThreadErrorMessage(
        "[ede_diagnostic] result_type=user last_content_type=n/a stop_reason=tool_use",
      ),
    ).toBe(INTERRUPTED_TURN_ERROR_MESSAGE);
  });

  it("retains the friendly copy for other known Claude interruption messages", () => {
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
