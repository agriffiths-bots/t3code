// End-to-end proof for the post-pair blocked-connection fix.
// Boots against an ephemeral T3 server (source instance.env first), seeds a
// project + thread over the dispatch API, then drives a FRESH browser through
// the real /pair flow and asserts — WITHOUT any reload — that the app opens
// its websocket and renders the seeded project/thread.
//
// Usage (see .claude/skills/t3-test-server/scripts/t3-up.sh):
//   exports="$(.claude/skills/t3-test-server/scripts/t3-up.sh --name pair-unblock)" && eval "$exports"
//   node e2e/pair-unblock-proof.mjs
//   .claude/skills/t3-test-server/scripts/t3-down.sh pair-unblock
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const require = NodeModule.createRequire(import.meta.url);
const { chromium } = require(NodePath.join(repoRoot, "apps/desktop/node_modules/playwright-core"));
const { mintPairingToken } = await import(NodePath.join(repoRoot, "e2e/ui.mjs"));

const ORIGIN = process.env.T3_ORIGIN;
const TOKEN = process.env.T3_TOKEN;
if (!ORIGIN || !TOKEN) throw new Error("source instance.env first");

async function dispatch(command) {
  const res = await fetch(`${ORIGIN}/api/orchestration/dispatch`, {
    method: "POST",
    headers: { authorization: `Bearer ${TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify(command),
  });
  if (!res.ok)
    throw new Error(`dispatch ${command.type} failed: HTTP ${res.status} ${await res.text()}`);
  return res.json().catch(() => null);
}

// Seed one project + one thread so the paired UI has something to render.
const projectId = NodeCrypto.randomUUID();
const threadId = NodeCrypto.randomUUID();
const nowIso = new Date().toISOString();
// WorkspacePaths rejects a missing workspace root (Normalizer only auto-creates
// it when createWorkspaceRootIfMissing is set), so ensure it exists first —
// otherwise project.create fails before the pairing flow is exercised.
const workspaceRoot = "/tmp/t3-eph-proj";
NodeFS.mkdirSync(workspaceRoot, { recursive: true });
await dispatch({
  type: "project.create",
  commandId: NodeCrypto.randomUUID(),
  projectId,
  title: "pair-unblock-proof",
  workspaceRoot,
  defaultModelSelection: { instanceId: "claudeAgent", model: "claude-sonnet-4-6" },
  createdAt: nowIso,
});
await dispatch({
  type: "thread.create",
  commandId: NodeCrypto.randomUUID(),
  threadId,
  projectId,
  title: "pair-unblock-proof-thread",
  modelSelection: { instanceId: "claudeAgent", model: "claude-sonnet-4-6" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  createdAt: nowIso,
});
console.log(`[seed] project ${projectId} thread ${threadId}`);

const pairingCode = mintPairingToken({ label: "pair-unblock-proof" });

const browser = await chromium.launch({ headless: true, channel: "chrome" });
const page = await browser.newPage();
const wsEvents = [];
page.on("websocket", (ws) => {
  const entry = { url: ws.url().split("?")[0], frames: 0 };
  wsEvents.push(entry);
  ws.on("framereceived", () => {
    entry.frames += 1;
  });
});
let reloads = 0;
page.on("framenavigated", (frame) => {
  if (frame === page.mainFrame()) reloads += 1; // counts SPA-external navigations
});

// Load the app UNPAIRED so the primary supervisor's first connect attempt
// fails auth and parks (the bug's precondition), then pair through the form.
await page.goto(`${ORIGIN}/`, { waitUntil: "domcontentloaded" });
await page.waitForURL((u) => u.pathname.startsWith("/pair"), { timeout: 20000 });
await page.waitForTimeout(7000); // let the poll register the primary + park the supervisor
const wsBeforePair = wsEvents.length;

await page.locator("#pairing-token").fill(pairingCode);
await page.locator("button[type=submit]").first().click();
await page.waitForURL((u) => !u.pathname.startsWith("/pair"), { timeout: 20000 });

// NO reload from here on. The fix must kick the parked supervisor itself.
await page.waitForTimeout(10000);
const rendered = await page.evaluate(() => document.body.innerText);
const threadVisible = rendered.includes("pair-unblock-proof-thread");
const projectVisible = rendered.includes("pair-unblock-proof");
const wsAfterPair = wsEvents.slice(wsBeforePair);
await page.screenshot({ path: "/tmp/t3-pair-unblock-proof.png", fullPage: true });
await browser.close();

const wsOk = wsAfterPair.some((ws) => ws.url.endsWith("/ws") && ws.frames > 0);
console.log(
  JSON.stringify(
    {
      wsBeforePair,
      wsAfterPair,
      projectVisible,
      threadVisible,
    },
    null,
    1,
  ),
);
if (!wsOk || !threadVisible || !projectVisible) {
  console.log("PROOF FAILED — post-pair state did not render without a reload");
  process.exit(1);
}
console.log(
  "PROOF PASSED — pair unblocked the connection: WS live + seeded project/thread rendered without reload",
);
