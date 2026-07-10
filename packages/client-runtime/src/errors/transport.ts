const TRANSPORT_ERROR_PATTERNS = [
  /\bSocketCloseError\b/i,
  /\bSocketOpenError\b/i,
  /\bSocket is not connected\b/i,
  /Unable to connect to the T3 server WebSocket\./i,
  /\bis not connected\.$/i,
  /\bdisconnected\.$/i,
  /\bcould not establish a WebSocket connection\.$/i,
  /\bClientProtocolError\b/i,
  /\bRpcClientError\b/i,
  /\bping timeout\b/i,
] as const;

const CLAUDE_INTERRUPTED_TURN_PATTERNS = [
  /\[ede_diagnostic\][^\n]*(?:stop_reason=tool_use|terminal_reason=aborted_streaming)/i,
  /\bterminal_reason=aborted_streaming\b/i,
  /^Error:\s*Request was aborted\.?$/i,
] as const;

export const INTERRUPTED_TURN_ERROR_MESSAGE =
  "The turn was interrupted. Send your message again to retry.";

/**
 * Check whether an error message originates from a transport-level connection
 * failure (socket close, socket open, ping timeout, etc.) rather than a
 * business-logic error.
 */
export function isTransportConnectionErrorMessage(message: string | null | undefined): boolean {
  if (typeof message !== "string") {
    return false;
  }

  const normalizedMessage = message.trim();
  if (normalizedMessage.length === 0) {
    return false;
  }

  return TRANSPORT_ERROR_PATTERNS.some((pattern) => pattern.test(normalizedMessage));
}

export function isClaudeInterruptedTurnDiagnosticMessage(
  message: string | null | undefined,
): boolean {
  if (typeof message !== "string") {
    return false;
  }

  const normalizedMessage = message.trim();
  if (normalizedMessage.length === 0) {
    return false;
  }

  return CLAUDE_INTERRUPTED_TURN_PATTERNS.some((pattern) => pattern.test(normalizedMessage));
}

function isClaudeUserSteerAbortDiagnosticMessage(message: string): boolean {
  return message.split(/\r?\n/u).some((line) => {
    if (!/\[ede_diagnostic\]/i.test(line) || !/\bresult_type=user\b/i.test(line)) {
      return false;
    }

    const hasKnownAbortReason =
      /\bterminal_reason=(?:aborted_streaming|aborted_tools)\b/i.test(line) ||
      /\bturn aborted\b/i.test(line);
    const hasCleanToolUseResult =
      /\bis_error=false\b/i.test(line) && /\bstop_reason=tool_use\b/i.test(line);

    return hasKnownAbortReason || hasCleanToolUseResult;
  });
}

/**
 * Strip transport connection errors from user-facing error messages.
 * Returns `null` for transport errors so the UI can distinguish between
 * real errors and transient connection issues.
 */
export function sanitizeThreadErrorMessage(message: string | null | undefined): string | null {
  if (typeof message !== "string") {
    return null;
  }

  const normalizedMessage = message.trim();
  if (normalizedMessage.length === 0 || isTransportConnectionErrorMessage(normalizedMessage)) {
    return null;
  }

  if (isClaudeUserSteerAbortDiagnosticMessage(normalizedMessage)) {
    return null;
  }

  if (isClaudeInterruptedTurnDiagnosticMessage(normalizedMessage)) {
    return INTERRUPTED_TURN_ERROR_MESSAGE;
  }

  return normalizedMessage;
}
