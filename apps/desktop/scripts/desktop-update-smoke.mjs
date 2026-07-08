#!/usr/bin/env node
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeURL from "node:url";

import { _electron as electron } from "playwright-core";

const DEFAULT_TIMEOUT_MS = 240_000;
const DESCRIPTOR_PATH = "/.well-known/t3/environment";
const READY_MARKER_FILE = "main-window-ready.json";
const MAX_CAPTURED_OUTPUT_BYTES = 256 * 1024;
const BACKEND_LOOPBACK_HOST = "127.0.0.1";
const UPDATE_SERVER_HOST = "127.0.0.1";
// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone CI script.
const hostPlatform = process.platform;
const repoRoot = NodePath.resolve(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "../../..",
);

function usage() {
  return [
    "usage: node apps/desktop/scripts/desktop-update-smoke.mjs --command <installed-exe> --update-root <dir> --expected-to-version <version> [--expected-from-version <version>]",
    "",
    "Launches an installed packaged desktop app, serves a local update feed,",
    "drives check/download/install through the preload bridge, and waits for",
    "the updated app to relaunch and signal the expected version.",
  ].join("\n");
}

function parseArgs(argv) {
  const options = {
    command: undefined,
    updateRoot: undefined,
    expectedFromVersion: undefined,
    expectedToVersion: undefined,
    updateServerPort: undefined,
    timeoutMs: DEFAULT_TIMEOUT_MS,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const value = argv[index + 1];
    switch (arg) {
      case "--command":
        if (!value) throw new Error("--command requires a value");
        options.command = value;
        index += 1;
        break;
      case "--update-root":
        if (!value) throw new Error("--update-root requires a value");
        options.updateRoot = value;
        index += 1;
        break;
      case "--expected-from-version":
        if (!value) throw new Error("--expected-from-version requires a value");
        options.expectedFromVersion = value;
        index += 1;
        break;
      case "--expected-to-version":
        if (!value) throw new Error("--expected-to-version requires a value");
        options.expectedToVersion = value;
        index += 1;
        break;
      case "--update-server-port":
        if (!value) throw new Error("--update-server-port requires a value");
        options.updateServerPort = Number.parseInt(value, 10);
        if (!Number.isInteger(options.updateServerPort) || options.updateServerPort <= 0) {
          throw new Error("--update-server-port must be a positive integer");
        }
        index += 1;
        break;
      case "--timeout-ms":
        if (!value) throw new Error("--timeout-ms requires a value");
        options.timeoutMs = Number.parseInt(value, 10);
        if (!Number.isInteger(options.timeoutMs) || options.timeoutMs <= 0) {
          throw new Error("--timeout-ms must be a positive integer");
        }
        index += 1;
        break;
      case "--help":
      case "-h":
        console.log(usage());
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (!options.command) throw new Error("Pass --command");
  if (!options.updateRoot) throw new Error("Pass --update-root");
  if (!options.expectedToVersion) throw new Error("Pass --expected-to-version");

  return {
    ...options,
    command: NodePath.resolve(options.command),
    updateRoot: NodePath.resolve(options.updateRoot),
  };
}

function appendOutput(current, streamName, chunk) {
  const next = `${current}${streamName}: ${chunk.toString()}`;
  return next.length > MAX_CAPTURED_OUTPUT_BYTES
    ? next.slice(next.length - MAX_CAPTURED_OUTPUT_BYTES)
    : next;
}

function reserveLoopbackPort(hostname = BACKEND_LOOPBACK_HOST) {
  return new Promise((resolve, reject) => {
    const server = NodeNet.createServer();
    server.once("error", reject);
    server.listen(0, hostname, () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("Could not reserve a loopback TCP port")));
        return;
      }
      const port = address.port;
      server.close((error) => {
        if (error) reject(error);
        else resolve(port);
      });
    });
  });
}

function fetchText(hostname, port, path, timeoutMs) {
  return new Promise((resolve, reject) => {
    const request = NodeHttp.get(
      {
        hostname,
        port,
        path,
        timeout: timeoutMs,
      },
      (response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          body += chunk;
        });
        response.on("end", () => {
          if ((response.statusCode ?? 0) < 200 || (response.statusCode ?? 0) >= 300) {
            reject(new Error(`${path} returned HTTP ${response.statusCode ?? "unknown"}`));
            return;
          }
          resolve(body);
        });
      },
    );
    request.once("timeout", () => {
      request.destroy(new Error(`${path} request timed out after ${timeoutMs}ms`));
    });
    request.once("error", reject);
  });
}

async function waitForUpdateServer(hostname, port, deadline) {
  let lastError;
  while (Date.now() < deadline) {
    for (const channelFile of ["/latest.yml", "/nightly.yml"]) {
      try {
        const text = await fetchText(hostname, port, channelFile, 1_000);
        return { channelFile, text };
      } catch (error) {
        lastError = error;
      }
    }
    await NodeTimersPromises.setTimeout(500);
  }

  throw new Error(
    `Timed out waiting for mock update server on ${hostname}:${port}: ${
      lastError instanceof Error ? lastError.message : String(lastError)
    }`,
  );
}

function startMockUpdateServer(updateRoot, port) {
  const child = NodeChildProcess.spawn(
    process.execPath,
    [NodePath.join(repoRoot, "scripts/mock-update-server.ts")],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        T3CODE_DESKTOP_MOCK_UPDATE_SERVER_PORT: String(port),
        T3CODE_DESKTOP_MOCK_UPDATE_SERVER_ROOT: updateRoot,
      },
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    },
  );

  let output = "";
  child.stdout.on("data", (chunk) => {
    output = appendOutput(output, "update-server stdout", chunk);
  });
  child.stderr.on("data", (chunk) => {
    output = appendOutput(output, "update-server stderr", chunk);
  });

  return {
    child,
    output: () => output,
    stop: async () => {
      if (child.exitCode !== null || child.signalCode !== null) {
        return;
      }
      child.kill("SIGTERM");
      await NodeTimersPromises.setTimeout(1_000);
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
      }
    },
  };
}

async function readReadyMarker(filePath) {
  let body;
  try {
    body = await NodeFSP.readFile(filePath, "utf8");
  } catch (error) {
    if (error && typeof error === "object" && error.code === "ENOENT") {
      return undefined;
    }
    throw error;
  }

  try {
    const parsed = JSON.parse(body);
    if (!parsed || typeof parsed !== "object" || parsed.status !== "main-window-ready") {
      return undefined;
    }
    if (typeof parsed.windowId !== "number") {
      return undefined;
    }
    if (typeof parsed.url !== "string" || parsed.url.length === 0) {
      return undefined;
    }
    const url = new URL(parsed.url);
    if (url.protocol !== "t3code:" && url.protocol !== "t3code-dev:") {
      return undefined;
    }
    return parsed;
  } catch {
    return undefined;
  }
}

async function waitForReadyMarker(filePath, expectedVersion, deadline) {
  let lastMarker;
  while (Date.now() < deadline) {
    const marker = await readReadyMarker(filePath);
    if (marker) {
      lastMarker = marker;
      if (expectedVersion === undefined || marker.appVersion === expectedVersion) {
        return marker;
      }
    }
    await NodeTimersPromises.setTimeout(500);
  }

  throw new Error(
    `Timed out waiting for ${READY_MARKER_FILE}${
      expectedVersion ? ` with appVersion ${expectedVersion}` : ""
    }. Last marker: ${lastMarker ? JSON.stringify(lastMarker) : "none"}`,
  );
}

async function waitForDescriptor(hostname, port, deadline) {
  let lastError;
  while (Date.now() < deadline) {
    try {
      const text = await fetchText(hostname, port, DESCRIPTOR_PATH, 1_000);
      const parsed = JSON.parse(text);
      if (typeof parsed.environmentId === "string" && parsed.environmentId.length > 0) {
        return parsed;
      }
    } catch (error) {
      lastError = error;
    }
    await NodeTimersPromises.setTimeout(500);
  }

  throw new Error(
    `Timed out waiting for desktop backend descriptor on ${hostname}:${port}: ${
      lastError instanceof Error ? lastError.message : String(lastError)
    }`,
  );
}

async function pageHasDesktopBridge(page) {
  try {
    return await page.evaluate(() => Boolean(window.desktopBridge?.getUpdateState));
  } catch {
    return false;
  }
}

async function waitForDesktopBridgePage(electronApp, deadline) {
  const seenPages = new WeakSet();
  while (Date.now() < deadline) {
    for (const page of electronApp.windows()) {
      if (!seenPages.has(page)) {
        seenPages.add(page);
        page.on("console", (message) => {
          console.log(`[desktop-update-smoke renderer] ${message.type()}: ${message.text()}`);
        });
      }
      if (await pageHasDesktopBridge(page)) {
        return page;
      }
    }

    try {
      const page = await electronApp.waitForEvent("window", { timeout: 1_000 });
      page.on("console", (message) => {
        console.log(`[desktop-update-smoke renderer] ${message.type()}: ${message.text()}`);
      });
      if (await pageHasDesktopBridge(page)) {
        return page;
      }
    } catch {
      // Keep polling existing windows until the overall deadline.
    }
  }

  throw new Error("Timed out waiting for a renderer window with desktopBridge.");
}

async function waitForUpdateState(page, predicate, description, deadline) {
  let lastState;
  while (Date.now() < deadline) {
    lastState = await page.evaluate(() => window.desktopBridge.getUpdateState());
    if (predicate(lastState)) {
      return lastState;
    }
    await NodeTimersPromises.setTimeout(500);
  }

  throw new Error(
    `Timed out waiting for update state ${description}. Last state: ${JSON.stringify(lastState)}`,
  );
}

function makeDesktopEnv(input) {
  const env = {
    ...process.env,
    APPDATA: input.appData,
    ELECTRON_ENABLE_LOGGING: "1",
    NO_AT_BRIDGE: "1",
    T3CODE_HOME: input.t3Home,
    T3CODE_DESKTOP_MOCK_UPDATES: "1",
    T3CODE_DESKTOP_MOCK_UPDATE_SERVER_PORT: String(input.updateServerPort),
    T3CODE_DESKTOP_SMOKE_READY_FILE: input.readyMarkerPath,
    T3CODE_DESKTOP_VERIFY_RUNTIME_DEPENDENCIES: "1",
    T3CODE_NO_BROWSER: "1",
    T3CODE_PORT: String(input.backendPort),
    XDG_CONFIG_HOME: input.appData,
  };
  delete env.ELECTRON_RUN_AS_NODE;
  delete env.VITE_DEV_SERVER_URL;
  delete env.T3CODE_DISABLE_AUTO_UPDATE;
  return env;
}

async function launchInstalledApp(executablePath, env, timeoutMs) {
  const app = await electron.launch({
    executablePath,
    cwd: NodePath.dirname(executablePath),
    args: hostPlatform === "linux" ? ["--no-sandbox", "--disable-gpu"] : [],
    env,
    timeout: Math.min(timeoutMs, 90_000),
  });

  app.on("console", (message) => {
    console.log(`[desktop-update-smoke main] ${message.type()}: ${message.text()}`);
  });

  return app;
}

function resolveRealPath(path) {
  try {
    return NodeFS.realpathSync.native(path);
  } catch {
    try {
      return NodeFS.realpathSync(path);
    } catch {
      return NodePath.resolve(path);
    }
  }
}

function escapeRegexLiteral(value) {
  return value.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
}

export function buildPosixExecutablePattern(executablePath) {
  return `^${escapeRegexLiteral(resolveRealPath(executablePath))}([[:space:]]|$)`;
}

export function listLinuxSmokeProcessIds(input) {
  const procRoot = input.procRoot ?? "/proc";
  const processIds = [];
  let entries;
  try {
    entries = NodeFS.readdirSync(procRoot, { withFileTypes: true });
  } catch {
    return processIds;
  }

  const requiredHomeEntry = `T3CODE_HOME=${input.t3Home}\0`;
  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) {
      continue;
    }
    const pid = Number.parseInt(entry.name, 10);
    if (!Number.isInteger(pid) || pid === process.pid) {
      continue;
    }
    let environ;
    try {
      environ = NodeFS.readFileSync(NodePath.join(procRoot, entry.name, "environ"), "utf8");
    } catch {
      continue;
    }
    if (
      environ.includes(requiredHomeEntry) &&
      environ.includes("T3CODE_DESKTOP_MOCK_UPDATES=1\0")
    ) {
      processIds.push(pid);
    }
  }
  return processIds;
}

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function sleepSync(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function terminateProcessIds(processIds) {
  for (const pid of processIds) {
    try {
      process.kill(pid, "SIGTERM");
    } catch {
      // Ignore processes that already exited.
    }
  }
  if (processIds.length > 0) {
    sleepSync(1_000);
  }
  for (const pid of processIds) {
    if (!processExists(pid)) {
      continue;
    }
    try {
      process.kill(pid, "SIGKILL");
    } catch {
      // Ignore processes that exited between the liveness probe and SIGKILL.
    }
  }
}

function terminatePosixInstalledExecutable(executablePath, options) {
  if (hostPlatform === "linux" && options.t3Home) {
    terminateProcessIds(listLinuxSmokeProcessIds({ t3Home: options.t3Home }));
  }

  const pattern = buildPosixExecutablePattern(executablePath);
  NodeChildProcess.spawnSync("pkill", ["-TERM", "-f", pattern], {
    stdio: "ignore",
  });
  sleepSync(1_000);
  NodeChildProcess.spawnSync("pkill", ["-KILL", "-f", pattern], {
    stdio: "ignore",
  });
}

function terminateInstalledExecutable(executablePath, options = {}) {
  if (hostPlatform === "win32") {
    const script = [
      "$ErrorActionPreference = 'SilentlyContinue'",
      "$target = [System.IO.Path]::GetFullPath($env:T3CODE_UPDATED_APP_EXE)",
      "Get-CimInstance Win32_Process |",
      "Where-Object { $_.ExecutablePath -and ([System.IO.Path]::GetFullPath($_.ExecutablePath) -eq $target) } |",
      "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }",
    ].join("\n");
    NodeChildProcess.spawnSync("powershell.exe", ["-NoProfile", "-Command", script], {
      env: {
        ...process.env,
        T3CODE_UPDATED_APP_EXE: executablePath,
      },
      stdio: "ignore",
      windowsHide: true,
    });
    return;
  }

  if (hostPlatform === "darwin" || hostPlatform === "linux") {
    terminatePosixInstalledExecutable(executablePath, options);
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (!NodeFS.existsSync(options.command)) {
    throw new Error(`Installed executable does not exist: ${options.command}`);
  }
  if (!NodeFS.existsSync(options.updateRoot)) {
    throw new Error(`Update root does not exist: ${options.updateRoot}`);
  }

  const updateServerPort =
    options.updateServerPort ?? (await reserveLoopbackPort(UPDATE_SERVER_HOST));
  const backendPort = await reserveLoopbackPort(BACKEND_LOOPBACK_HOST);
  const tempRoot = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.tmpdir(), "t3-desktop-update-smoke-"),
  );
  const t3Home = NodePath.join(tempRoot, "t3-home");
  const appData = NodePath.join(tempRoot, "app-data");
  const readyMarkerPath = NodePath.join(tempRoot, READY_MARKER_FILE);
  await NodeFSP.mkdir(t3Home, { recursive: true });
  await NodeFSP.mkdir(appData, { recursive: true });

  const deadline = Date.now() + options.timeoutMs;
  const updateServer = startMockUpdateServer(options.updateRoot, updateServerPort);
  let electronApp;

  try {
    const updateFeed = await waitForUpdateServer(UPDATE_SERVER_HOST, updateServerPort, deadline);
    if (!updateFeed.text.includes(`version: ${options.expectedToVersion}`)) {
      throw new Error(
        `${updateFeed.channelFile} did not advertise expected version ${options.expectedToVersion}.`,
      );
    }
    console.log(
      `[desktop-update-smoke] Mock update server ready on port ${updateServerPort} (${updateFeed.channelFile})`,
    );

    const env = makeDesktopEnv({
      appData,
      backendPort,
      readyMarkerPath,
      t3Home,
      updateServerPort,
    });

    console.log(`[desktop-update-smoke] Launching installed app ${options.command}`);
    electronApp = await launchInstalledApp(options.command, env, options.timeoutMs);
    const page = await waitForDesktopBridgePage(electronApp, deadline);
    const appVersion = await electronApp.evaluate(({ app }) => app.getVersion());
    if (options.expectedFromVersion && appVersion !== options.expectedFromVersion) {
      throw new Error(
        `Installed app version was ${appVersion}, expected ${options.expectedFromVersion}.`,
      );
    }

    const descriptor = await waitForDescriptor(BACKEND_LOOPBACK_HOST, backendPort, deadline);
    const readyMarker = await waitForReadyMarker(
      readyMarkerPath,
      options.expectedFromVersion,
      deadline,
    );
    console.log(
      `[desktop-update-smoke] Installed app ready: version=${appVersion} environmentId=${descriptor.environmentId} windowId=${readyMarker.windowId}`,
    );

    const initialState = await page.evaluate(() => window.desktopBridge.getUpdateState());
    if (!initialState.enabled) {
      throw new Error(`Desktop updates were disabled: ${initialState.message ?? "no message"}`);
    }

    await page.evaluate(() => window.desktopBridge.checkForUpdate());
    const availableState = await waitForUpdateState(
      page,
      (state) =>
        (state.status === "available" || state.status === "downloaded") &&
        (state.availableVersion === options.expectedToVersion ||
          state.downloadedVersion === options.expectedToVersion),
      `available version ${options.expectedToVersion}`,
      deadline,
    );
    console.log(
      `[desktop-update-smoke] Update available: status=${availableState.status} version=${
        availableState.availableVersion ?? availableState.downloadedVersion
      }`,
    );

    const downloadResult = await page.evaluate(() => window.desktopBridge.downloadUpdate());
    if (!downloadResult.accepted || !downloadResult.completed) {
      throw new Error(`Update download was not completed: ${JSON.stringify(downloadResult)}`);
    }
    const downloadedState = await waitForUpdateState(
      page,
      (state) =>
        state.status === "downloaded" && state.downloadedVersion === options.expectedToVersion,
      `downloaded version ${options.expectedToVersion}`,
      deadline,
    );
    console.log(
      `[desktop-update-smoke] Update downloaded: version=${downloadedState.downloadedVersion}`,
    );

    await NodeFSP.rm(readyMarkerPath, { force: true });
    const closePromise = electronApp.waitForEvent("close", {
      timeout: Math.max(1, deadline - Date.now()),
    });
    const installPromise = page
      .evaluate(() => window.desktopBridge.installUpdate())
      .catch((error) => {
        if (
          /Target.*closed|browser has been closed|Execution context was destroyed/iu.test(
            String(error),
          )
        ) {
          return { accepted: true, completed: false };
        }
        throw error;
      });
    const installResult = await installPromise;
    if (!installResult.accepted) {
      throw new Error(`Update install was not accepted: ${JSON.stringify(installResult)}`);
    }
    await closePromise;
    electronApp = undefined;
    console.log("[desktop-update-smoke] Installed app exited for update installation.");

    const updatedMarker = await waitForReadyMarker(
      readyMarkerPath,
      options.expectedToVersion,
      deadline,
    );
    console.log(
      `[desktop-update-smoke] Updated app relaunched: version=${updatedMarker.appVersion} windowId=${updatedMarker.windowId}`,
    );
  } catch (error) {
    const serverOutput = updateServer.output().trim();
    throw new Error(
      `${error instanceof Error ? error.message : String(error)}\nSmoke temp root: ${tempRoot}${
        serverOutput ? `\nMock update server output:\n${serverOutput}` : ""
      }`,
      { cause: error },
    );
  } finally {
    if (electronApp) {
      await electronApp.close().catch(() => undefined);
    }
    terminateInstalledExecutable(options.command, { t3Home });
    await updateServer.stop();
  }
}

if (process.argv[1] && import.meta.url === NodeURL.pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack || error.message : String(error));
    process.exit(1);
  });
}
