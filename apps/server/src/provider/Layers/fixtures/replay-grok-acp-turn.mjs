#!/usr/bin/env node

// Replays the redacted ACP frames captured from a live Grok 0.2.93 PONG turn
// on 2026-07-21. The standard session/prompt RPC deliberately remains pending:
// live Grok settles through _x.ai/session/prompt_complete after streaming deltas.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const fixturePath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "recorded-grok-0.2.93-pong.jsonl",
);
const capturedMessages = NodeFS.readFileSync(fixturePath, "utf8")
  .trimEnd()
  .split("\n")
  .map((line) => JSON.parse(line));
const requestOrder = ["initialize", "authenticate", "session/new", "session/prompt"];
let nextRequestIndex = 0;

function writeMessage(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

process.stdin.setEncoding("utf8");
let remainder = "";
process.stdin.on("data", (chunk) => {
  remainder += chunk;
  const lines = remainder.split("\n");
  remainder = lines.pop() ?? "";

  for (const line of lines) {
    if (line.trim().length === 0) continue;
    const request = JSON.parse(line);
    if (request.method === "initialized") continue;
    if (request.id === undefined || typeof request.method !== "string") continue;

    const expectedMethod = requestOrder[nextRequestIndex];
    if (request.method !== expectedMethod) {
      writeMessage({
        jsonrpc: "2.0",
        id: request.id,
        error: { code: -32601, message: `Unexpected request ${request.method}` },
      });
      continue;
    }

    if (request.method === "session/prompt") {
      for (const notification of capturedMessages.slice(3)) {
        writeMessage(notification);
      }
    } else {
      const response = capturedMessages[nextRequestIndex];
      if (response === undefined || response.id === undefined) {
        throw new Error(`Recorded response for ${request.method} is missing`);
      }
      writeMessage({ ...response, id: request.id });
    }
    nextRequestIndex += 1;
  }
});
