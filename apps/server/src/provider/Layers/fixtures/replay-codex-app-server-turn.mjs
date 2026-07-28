#!/usr/bin/env node

// Replays the raw stdout captured from a live codex-cli 0.144.4 PONG turn on
// 2026-07-16. Responses are released only after the matching client request;
// the notifications between responses preserve their original byte order.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const fixturePath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "codex-app-server-0.144.4-pong.jsonl",
);
const capturedLines = NodeFS.readFileSync(fixturePath, "utf8").trimEnd().split("\n");
const capturedMessages = capturedLines.map((line) => JSON.parse(line));
const requestOrder = ["initialize", "thread/start", "turn/start"];
const scenario = process.env.T3_PROVIDER_E2E_SCENARIO ?? "success";
let nextRequestIndex = 0;

function replayResponseAndFollowingNotifications(requestId) {
  const responseIndex = capturedMessages.findIndex((message) => message.id === requestId);
  if (responseIndex < 0) {
    throw new Error(`Recorded response ${requestId} is missing`);
  }
  const nextResponseIndex = capturedMessages.findIndex(
    (message, index) => index > responseIndex && message.id !== undefined,
  );
  const endIndex = nextResponseIndex < 0 ? capturedLines.length : nextResponseIndex;
  for (let index = responseIndex; index < endIndex; index += 1) {
    const message = capturedMessages[index];
    if (scenario === "empty" && message?.method === "item/agentMessage/delta") {
      continue;
    }
    if (
      scenario === "empty" &&
      message?.method === "item/completed" &&
      message.params?.item?.type === "agentMessage"
    ) {
      process.stdout.write(
        `${JSON.stringify({ ...message, params: { ...message.params, item: { ...message.params.item, text: "" } } })}\n`,
      );
      continue;
    }
    process.stdout.write(`${capturedLines[index]}\n`);
    if (scenario === "death" && message?.method === "item/agentMessage/delta") {
      process.stdout.write("", () => process.exit(17));
      return;
    }
  }
}

let remainder = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  remainder += chunk;
  const lines = remainder.split("\n");
  remainder = lines.pop() ?? "";

  for (const line of lines) {
    if (line.trim().length === 0) continue;
    const message = JSON.parse(line);
    if (message.method === "initialized") continue;
    if (message.id === undefined || typeof message.method !== "string") continue;

    const expectedMethod = requestOrder[nextRequestIndex];
    if (message.method !== expectedMethod) {
      process.stdout.write(
        `${JSON.stringify({ id: message.id, error: { code: -32601, message: `Unexpected request ${message.method}` } })}\n`,
      );
      continue;
    }

    replayResponseAndFollowingNotifications(message.id);
    nextRequestIndex += 1;
  }
});
