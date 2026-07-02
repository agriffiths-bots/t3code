// ui.mjs — Playwright helper for driving the T3 web UI against an ephemeral
// server (see .claude/skills/t3-test-server/scripts/t3-up.sh). Uses
// playwright-core from apps/desktop + the system Chrome, so no browser
// download is needed.
//
//   . ~/.cache/t3-ephemeral/instances/<name>/instance.env   # or eval t3-up.sh
//   node --input-type=module -e '
//     import { openT3 } from "./e2e/ui.mjs";
//     const { browser, page } = await openT3();
//     // drive the app: page.click / page.getByRole / page.screenshot ...
//     await browser.close();
//   '
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const require = NodeModule.createRequire(import.meta.url);
const { chromium } = require(NodePath.join(repoRoot, "apps/desktop/node_modules/playwright-core"));

/** Mint a one-time pairing token for the instance (server-side CLI). */
export function mintPairingToken({
  home = process.env.T3_HOME ?? process.env.T3CODE_HOME,
  entry = process.env.T3_ENTRY ?? "apps/server/dist/bin.mjs",
  ttl = "10m",
  label = "e2e",
} = {}) {
  if (!home) throw new Error("mintPairingToken: T3_HOME not set (source instance.env first)");
  const out = NodeChildProcess.execFileSync(
    "node",
    [
      NodePath.join(repoRoot, entry),
      "auth",
      "pairing",
      "create",
      "--json",
      "--ttl",
      ttl,
      "--label",
      label,
    ],
    { env: { ...process.env, T3CODE_HOME: home }, encoding: "utf8" },
  );
  const parsed = JSON.parse(out);
  if (!parsed.credential) throw new Error(`pairing create returned no credential: ${out}`);
  return parsed.credential;
}

/**
 * Launch headless Chrome, pair with the ephemeral T3 server, and land on the
 * authenticated UI. Returns { browser, page }; caller must browser.close().
 */
export async function openT3({
  origin = process.env.T3_ORIGIN,
  home = process.env.T3_HOME ?? process.env.T3CODE_HOME,
  entry = process.env.T3_ENTRY ?? "apps/server/dist/bin.mjs",
  headless = true,
  timeoutMs = 30_000,
} = {}) {
  if (!origin) throw new Error("openT3: T3_ORIGIN not set (source instance.env first)");
  const token = mintPairingToken({ home, entry });
  const browser = await chromium.launch({
    headless,
    channel: process.env.T3_UI_CHROME_CHANNEL ?? "chrome",
  });
  try {
    const page = await browser.newPage();
    await page.goto(`${origin}/pair#token=${token}`, { waitUntil: "domcontentloaded" });
    // Pairing exchanges the one-time token for a t3_session cookie and
    // redirects into the app; treat leaving /pair as authenticated.
    await page.waitForURL((u) => !u.pathname.startsWith("/pair"), { timeout: timeoutMs });
    return { browser, page };
  } catch (error) {
    // Don't orphan the headless Chrome when pairing/navigation fails.
    await browser.close().catch(() => {});
    throw error;
  }
}
