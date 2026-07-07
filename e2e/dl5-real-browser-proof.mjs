#!/usr/bin/env node
// Real-browser proof for the Cloudflare front-door deployments.
//
// This intentionally drives deployed origins in Chrome. It first obtains the
// Cloudflare front-door cookie through the Access service token, then pairs the
// browser through the normal T3 one-time pairing flow so the app can fetch
// orchestration data and open /ws like Adam's browser would after login.

import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const require = NodeModule.createRequire(import.meta.url);
const { chromium } = require(NodePath.join(repoRoot, "apps/desktop/node_modules/playwright-core"));

const DEFAULT_CF_ENV_PATH = "/home/adam/.openclaw/secrets/cloudflare-access-playwright-ci.env";
const DEFAULT_ARTIFACT_DIR = NodePath.join(
  process.env.HOME ?? "/tmp",
  ".cache",
  "t3code",
  "dl5-real-browser-proof",
);
const DEFAULT_T3_BASE_DIR = "/home/adam/.t3-vps";
const PRIVATE_DIRECTORY_MODE = 0o700;
const PRIVATE_FILE_MODE = 0o600;

const TARGETS = [
  {
    label: "dl5",
    url: process.env.DL5_PROOF_PUBLIC_URL ?? "https://dl5-5uq.pages.dev",
    expectedCloudflareCookieName: "oc_session",
  },
  {
    label: "oc-control",
    url: process.env.DL5_PROOF_CONTROL_URL ?? "https://oc.agriffiths.dev",
    expectedCloudflareCookieName: "CF_Authorization",
  },
];

function arg(name, fallback = null) {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 && index + 1 < process.argv.length ? process.argv[index + 1] : fallback;
}

const artifactDir = arg("artifact-dir", process.env.DL5_PROOF_ARTIFACT_DIR ?? DEFAULT_ARTIFACT_DIR);
const cfEnvPath = arg("cf-env", process.env.CF_ACCESS_ENV_PATH ?? DEFAULT_CF_ENV_PATH);
const t3BaseDir = arg(
  "base-dir",
  process.env.T3CODE_HOME ?? process.env.T3_HOME ?? DEFAULT_T3_BASE_DIR,
);
const timeoutMs = Number(arg("timeout-ms", process.env.DL5_PROOF_TIMEOUT_MS ?? "45000"));

function loadDotenvFile(path) {
  const values = {};
  const text = NodeFS.readFileSync(path, "utf8");
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(trimmed);
    if (!match) continue;
    let value = match[2].trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    values[match[1]] = value;
  }
  return values;
}

function accessHeaders(env) {
  const clientId =
    process.env.CF_ACCESS_CLIENT_ID ??
    process.env.WORKER_E2E_CLIENT_ID ??
    env.CF_ACCESS_CLIENT_ID ??
    env.WORKER_E2E_CLIENT_ID;
  const clientSecret =
    process.env.CF_ACCESS_CLIENT_SECRET ??
    process.env.WORKER_E2E_CLIENT_SECRET ??
    env.CF_ACCESS_CLIENT_SECRET ??
    env.WORKER_E2E_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new Error(
      `CF Access service token not configured; set CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET or provide --cf-env ${cfEnvPath}`,
    );
  }
  return {
    "CF-Access-Client-Id": clientId,
    "CF-Access-Client-Secret": clientSecret,
  };
}

function parseSetCookie(headerValue) {
  const [nameValue, ...rawAttrs] = headerValue.split(";").map((part) => part.trim());
  const equalsIndex = nameValue.indexOf("=");
  if (equalsIndex < 1) return null;
  const cookie = {
    name: nameValue.slice(0, equalsIndex),
    value: nameValue.slice(equalsIndex + 1),
    attrs: {},
  };
  for (const rawAttr of rawAttrs) {
    if (!rawAttr) continue;
    const attrEqualsIndex = rawAttr.indexOf("=");
    if (attrEqualsIndex === -1) {
      cookie.attrs[rawAttr.toLowerCase()] = true;
      continue;
    }
    cookie.attrs[rawAttr.slice(0, attrEqualsIndex).trim().toLowerCase()] = rawAttr
      .slice(attrEqualsIndex + 1)
      .trim();
  }
  return cookie;
}

function setCookieHeaders(response) {
  if (typeof response.headers.getSetCookie === "function") {
    return response.headers.getSetCookie();
  }
  const singleHeader = response.headers.get("set-cookie");
  return singleHeader ? [singleHeader] : [];
}

function normalizeSameSite(value) {
  if (!value) return undefined;
  const lower = String(value).toLowerCase();
  if (lower === "none") return "None";
  if (lower === "strict") return "Strict";
  return "Lax";
}

function browserCookieFromSetCookie(cookie, origin) {
  const url = new URL(origin);
  const browserCookie = {
    name: cookie.name,
    value: cookie.value,
    domain: cookie.attrs.domain ?? url.hostname,
    path: cookie.attrs.path ?? "/",
    httpOnly: Boolean(cookie.attrs.httponly),
    secure: Boolean(cookie.attrs.secure) || url.protocol === "https:",
  };
  const sameSite = normalizeSameSite(cookie.attrs.samesite);
  if (sameSite) browserCookie.sameSite = sameSite;
  return browserCookie;
}

function cookieMetadata(cookie) {
  const attrs = cookie.attrs ?? {};
  return {
    name: cookie.name,
    domain: attrs.domain ?? null,
    path: attrs.path ?? cookie.path ?? null,
    secure: Boolean(attrs.secure ?? cookie.secure),
    httpOnly: Boolean(attrs.httponly ?? cookie.httpOnly),
    sameSite: attrs.samesite ?? cookie.sameSite ?? null,
    maxAge: attrs["max-age"] ?? null,
    expires: attrs.expires ?? null,
  };
}

function sanitizeUrl(value) {
  try {
    const url = new URL(value);
    for (const key of url.searchParams.keys()) {
      if (/token|ticket|secret|credential|code/i.test(key)) {
        url.searchParams.set(key, "<redacted>");
      }
    }
    if (url.hash) {
      url.hash = "#<redacted>";
    }
    return url.toString();
  } catch {
    return String(value).replace(
      /(token|ticket|secret|credential|code)=([^&\s]+)/gi,
      "$1=<redacted>",
    );
  }
}

function sanitizeText(value) {
  return String(value)
    .replace(/#token=[^\s"'`<>]+/gi, "#token=<redacted>")
    .replace(/#%3Credacted%3E/gi, "#<redacted>")
    .replace(/(token|ticket|secret|credential|code)=([^&\s"'`<>]+)/gi, "$1=<redacted>");
}

function errorMetadata(error) {
  if (!(error instanceof Error)) return sanitizeText(error);
  return {
    message: sanitizeText(error.message),
    stack: error.stack ? sanitizeText(error.stack) : undefined,
  };
}

function sanitizeHeaders(headers) {
  const redacted = {};
  for (const [key, value] of Object.entries(headers ?? {})) {
    if (/cookie|authorization|token|secret/i.test(key)) {
      redacted[key] = "<redacted>";
    } else {
      redacted[key] = value;
    }
  }
  return redacted;
}

function textSnippet(value, maxLength = 400) {
  if (typeof value !== "string") return "";
  const compact = value.replace(/\s+/g, " ").trim();
  return compact.length > maxLength ? `${compact.slice(0, maxLength)}...` : compact;
}

function interestingFrame(payload) {
  return /snapshot|project|thread|shell|subscribe|orchestration/i.test(payload);
}

function summarizeFrame(payload) {
  return {
    length: payload.length,
    interesting: interestingFrame(payload),
    snippet: textSnippet(payload, 500),
  };
}

function mkdirp(path) {
  NodeFS.mkdirSync(path, { recursive: true, mode: PRIVATE_DIRECTORY_MODE });
  NodeFS.chmodSync(path, PRIVATE_DIRECTORY_MODE);
}

function writeJson(path, value) {
  NodeFS.writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, {
    mode: PRIVATE_FILE_MODE,
  });
  NodeFS.chmodSync(path, PRIVATE_FILE_MODE);
}

async function writeScreenshot(page, path, options = {}) {
  await page.screenshot({ path, ...options });
  NodeFS.chmodSync(path, PRIVATE_FILE_MODE);
}

async function obtainCloudflareCookie(target, headers) {
  const response = await fetch(target.url, {
    redirect: "manual",
    headers,
  });
  const cookies = setCookieHeaders(response).map(parseSetCookie).filter(Boolean);
  const selected = cookies.find((cookie) => cookie.name === target.expectedCloudflareCookieName);
  if (!selected) {
    throw new Error(
      `${target.label}: expected ${target.expectedCloudflareCookieName} from ${target.url}, got HTTP ${response.status} cookies=[${cookies.map((cookie) => cookie.name).join(", ")}]`,
    );
  }
  return {
    status: response.status,
    cookie: selected,
    cookieMetadata: cookieMetadata(selected),
  };
}

function mintPairingToken(target) {
  const env = { ...process.env, T3CODE_HOME: t3BaseDir };
  delete env.VITE_DEV_SERVER_URL;
  const out = NodeChildProcess.execFileSync(
    "node",
    [
      NodePath.join(repoRoot, "apps/server/src/bin.ts"),
      "auth",
      "pairing",
      "create",
      "--base-dir",
      t3BaseDir,
      "--json",
      "--ttl",
      "10m",
      "--label",
      `dl5-real-browser-proof-${target.label}-${Date.now()}`,
    ],
    {
      cwd: repoRoot,
      env,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const parsed = JSON.parse(out);
  if (!parsed.credential) {
    throw new Error(`${target.label}: pairing create returned no credential`);
  }
  return parsed.credential;
}

async function waitForProjectsInDom(page) {
  await page.waitForFunction(
    () => {
      const text = document.body?.innerText ?? "";
      const projectRows = document.querySelectorAll('[data-testid="sidebar-project-row"]');
      return (
        text.includes("PROJECTS") &&
        !text.includes("No projects yet") &&
        projectRows.length > 0 &&
        Array.from(projectRows).some((row) => (row.textContent ?? "").trim().length > 0)
      );
    },
    undefined,
    { timeout: timeoutMs },
  );
}

function isWebSocketUrl(value) {
  try {
    return new URL(value).pathname === "/ws";
  } catch {
    return false;
  }
}

async function waitForWebSocket(wsEvents) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ws101 = wsEvents.handshakes.find(
      (handshake) => isWebSocketUrl(handshake.url) && handshake.status === 101,
    );
    const inbound = wsEvents.framesReceived.find((frame) => isWebSocketUrl(frame.url));
    if (ws101 && inbound) return;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(
    `timed out waiting for /ws 101 and inbound frame (handshakes=${wsEvents.handshakes.length}, inbound=${wsEvents.framesReceived.length})`,
  );
}

async function runTarget(target, headers) {
  const targetArtifactPrefix = `${target.label}-${new URL(target.url).hostname}`;
  const events = {
    responses: [],
    requestFailures: [],
    console: [],
    pageErrors: [],
    handshakes: [],
    framesReceived: [],
    framesSent: [],
  };
  let browser = null;
  let context = null;
  let page = null;
  const wsUrlByRequestId = new Map();

  try {
    const cloudflare = await obtainCloudflareCookie(target, headers);
    const pairingToken = mintPairingToken(target);

    browser = await chromium.launch({
      headless: true,
      channel: process.env.T3_UI_CHROME_CHANNEL ?? "chrome",
    });
    context = await browser.newContext({
      ignoreHTTPSErrors: false,
    });
    await context.addCookies([browserCookieFromSetCookie(cloudflare.cookie, target.url)]);
    page = await context.newPage();
    const cdp = await context.newCDPSession(page);
    await cdp.send("Network.enable");

    cdp.on("Network.webSocketCreated", (event) => {
      wsUrlByRequestId.set(event.requestId, event.url);
    });
    cdp.on("Network.webSocketWillSendHandshakeRequest", (event) => {
      const url = event.request.url ?? wsUrlByRequestId.get(event.requestId) ?? "";
      wsUrlByRequestId.set(event.requestId, url);
      events.handshakes.push({
        phase: "request",
        url: sanitizeUrl(url),
        headers: sanitizeHeaders(event.request.headers),
      });
    });
    cdp.on("Network.webSocketHandshakeResponseReceived", (event) => {
      const url = event.response.url ?? wsUrlByRequestId.get(event.requestId) ?? "";
      wsUrlByRequestId.set(event.requestId, url);
      events.handshakes.push({
        phase: "response",
        url: sanitizeUrl(url),
        status: event.response.status,
        headers: sanitizeHeaders(event.response.headers),
      });
    });
    cdp.on("Network.webSocketFrameReceived", (event) => {
      const payload = String(event.response.payloadData ?? "");
      events.framesReceived.push({
        url: sanitizeUrl(wsUrlByRequestId.get(event.requestId) ?? ""),
        ...summarizeFrame(payload),
      });
    });
    cdp.on("Network.webSocketFrameSent", (event) => {
      const payload = String(event.response.payloadData ?? "");
      events.framesSent.push({
        url: sanitizeUrl(wsUrlByRequestId.get(event.requestId) ?? ""),
        ...summarizeFrame(payload),
      });
    });

    page.on("console", (message) => {
      if (message.type() === "error" || message.type() === "warning") {
        events.console.push({
          type: message.type(),
          text: textSnippet(message.text(), 1000),
          location: message.location(),
        });
      }
    });
    page.on("pageerror", (error) => {
      events.pageErrors.push({ message: error.message, stack: error.stack });
    });
    page.on("requestfailed", (request) => {
      events.requestFailures.push({
        url: sanitizeUrl(request.url()),
        method: request.method(),
        failure: request.failure()?.errorText ?? null,
      });
    });
    page.on("response", (response) => {
      const url = sanitizeUrl(response.url());
      let pathname = "";
      try {
        pathname = new URL(response.url()).pathname;
      } catch {
        pathname = "";
      }
      if (pathname.startsWith("/api/") || pathname === "/ws" || response.status() >= 400) {
        events.responses.push({
          url,
          status: response.status(),
          requestMethod: response.request().method(),
        });
      }
    });

    await page.goto(`${target.url}/pair#token=${encodeURIComponent(pairingToken)}`, {
      waitUntil: "commit",
      timeout: timeoutMs,
    });
    await page.waitForURL((url) => !url.pathname.startsWith("/pair"), { timeout: timeoutMs });

    await waitForWebSocket(events);
    await waitForProjectsInDom(page);

    const screenshotPath = NodePath.join(
      artifactDir,
      `${targetArtifactPrefix}-pass-${Date.now()}.png`,
    );
    await writeScreenshot(page, screenshotPath, { fullPage: true });
    const cookies = await context.cookies(target.url);
    const domEvidence = await page.evaluate(() => ({
      bodyText: document.body?.innerText ?? "",
      projectRows: Array.from(document.querySelectorAll('[data-testid="sidebar-project-row"]'))
        .map((row) => (row.textContent ?? "").replace(/\s+/g, " ").trim())
        .filter(Boolean),
      threadRows: Array.from(document.querySelectorAll('[data-testid="thread-row"]'))
        .map((row) => (row.textContent ?? "").replace(/\s+/g, " ").trim())
        .filter(Boolean),
    }));
    const result = {
      label: target.label,
      url: target.url,
      cloudflareCookie: cloudflare.cookieMetadata,
      t3SessionCookies: cookies
        .filter((cookie) => cookie.name.startsWith("t3_session"))
        .map((cookie) => cookieMetadata(cookie)),
      projectCount: domEvidence.projectRows.length,
      projectNames: domEvidence.projectRows.slice(0, 10),
      threadCount: domEvidence.threadRows.length,
      threadNames: domEvidence.threadRows.slice(0, 10),
      wsHandshake: events.handshakes.find(
        (handshake) =>
          handshake.phase === "response" &&
          handshake.status === 101 &&
          isWebSocketUrl(handshake.url),
      ),
      wsFrameCounts: {
        received: events.framesReceived.length,
        sent: events.framesSent.length,
        interestingReceived: events.framesReceived.filter((frame) => frame.interesting).length,
      },
      screenshotPath,
      bodySnippet: textSnippet(domEvidence.bodyText, 700),
      consoleIssues: events.console,
      requestFailures: events.requestFailures,
      relevantResponses: events.responses,
    };
    writeJson(NodePath.join(artifactDir, `${targetArtifactPrefix}-pass.json`), result);
    return result;
  } catch (error) {
    const failurePath = NodePath.join(artifactDir, `${targetArtifactPrefix}-failure-${Date.now()}`);
    const state = {
      label: target.label,
      url: target.url,
      error: errorMetadata(error),
      events,
    };
    if (page) {
      state.page = await page
        .evaluate(() => ({
          href: window.location.href,
          title: document.title,
          bodyText: document.body?.innerText?.slice(0, 4000) ?? "",
        }))
        .then((pageState) => ({ ...pageState, href: sanitizeUrl(pageState.href) }))
        .catch((evaluateError) => ({ evaluateError: String(evaluateError) }));
      await writeScreenshot(page, `${failurePath}.png`, { fullPage: true }).catch(() => {});
    }
    if (context) {
      state.cookies = await context
        .cookies(target.url)
        .then((cookies) => cookies.map(cookieMetadata))
        .catch((cookieError) => ({ cookieError: String(cookieError) }));
    }
    writeJson(`${failurePath}.json`, state);
    throw new Error(`${target.label} failed; diagnostics: ${failurePath}.json`, {
      cause: error,
    });
  } finally {
    await browser?.close().catch(() => {});
  }
}

async function main() {
  mkdirp(artifactDir);
  const env = NodeFS.existsSync(cfEnvPath) ? loadDotenvFile(cfEnvPath) : {};
  const headers = accessHeaders(env);
  const results = [];
  for (const target of TARGETS) {
    console.log(`[proof] running ${target.label} ${target.url}`);
    const result = await runTarget(target, headers);
    results.push(result);
    console.log(
      `[proof] ${target.label} ok: projects=${result.projectCount} threads=${result.threadCount} wsReceived=${result.wsFrameCounts.received} screenshot=${result.screenshotPath}`,
    );
  }
  const summaryPath = NodePath.join(artifactDir, `summary-${Date.now()}.json`);
  const summary = {
    status: "PASS",
    generatedAt: new Date().toISOString(),
    artifactDir,
    results,
  };
  writeJson(summaryPath, summary);
  console.log(JSON.stringify({ ...summary, summaryPath }, null, 2));
}

main().catch((error) => {
  console.error(
    sanitizeText(error instanceof Error ? error.stack || error.message : String(error)),
  );
  process.exitCode = 1;
});
