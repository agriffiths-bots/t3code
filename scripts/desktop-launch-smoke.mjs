#!/usr/bin/env node
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTimersPromises from "node:timers/promises";

const DEFAULT_TIMEOUT_MS = 90_000;
const DESCRIPTOR_PATH = "/.well-known/t3/environment";
const MAX_CAPTURED_OUTPUT_BYTES = 256 * 1024;
const FATAL_OUTPUT_PATTERNS = [
  /ERR_MODULE_NOT_FOUND/i,
  /\bMODULE_NOT_FOUND\b/i,
  /Cannot find module/i,
];
// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone CI script.
const hostPlatform = process.platform;

function usage() {
  return [
    "usage: node scripts/desktop-launch-smoke.mjs (--artifact <path-or-glob> | --command <path>) [--timeout-ms <ms>]",
    "",
    "Launches a packaged desktop app with an isolated T3CODE_HOME, waits for",
    `${DESCRIPTOR_PATH} on a forced loopback port, and fails on module-resolution crashes.`,
  ].join("\n");
}

function parseArgs(argv) {
  const options = {
    artifact: undefined,
    command: undefined,
    timeoutMs: DEFAULT_TIMEOUT_MS,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const value = argv[index + 1];
    switch (arg) {
      case "--artifact":
        if (!value) throw new Error("--artifact requires a value");
        options.artifact = value;
        index += 1;
        break;
      case "--command":
        if (!value) throw new Error("--command requires a value");
        options.command = value;
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

  if (Boolean(options.artifact) === Boolean(options.command)) {
    throw new Error("Pass exactly one of --artifact or --command");
  }

  return options;
}

function escapeRegex(value) {
  return value.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
}

function globBasenameToRegex(value) {
  return new RegExp(
    `^${Array.from(value)
      .map((character) => {
        if (character === "*") return ".*";
        if (character === "?") return ".";
        return escapeRegex(character);
      })
      .join("")}$`,
  );
}

async function resolvePattern(pattern) {
  if (!/[*?]/.test(pattern)) {
    return [NodePath.resolve(pattern)];
  }

  const normalized = pattern.replaceAll("\\", "/");
  const wildcardIndex = normalized.search(/[*?]/);
  const slashIndex = normalized.lastIndexOf("/", wildcardIndex);
  const directoryPattern = slashIndex >= 0 ? normalized.slice(0, slashIndex) : ".";
  const basenamePattern = slashIndex >= 0 ? normalized.slice(slashIndex + 1) : normalized;
  if (/[*?]/.test(directoryPattern)) {
    throw new Error(`Only filename globs are supported: ${pattern}`);
  }

  const directory = NodePath.resolve(directoryPattern);
  const matcher = globBasenameToRegex(basenamePattern);
  const entries = await NodeFSP.readdir(directory);
  return entries
    .filter((entry) => matcher.test(entry))
    .map((entry) => NodePath.join(directory, entry))
    .sort();
}

async function resolveExecutable(options) {
  if (options.command) {
    return NodePath.resolve(options.command);
  }

  const matches = await resolvePattern(options.artifact);
  if (matches.length !== 1) {
    throw new Error(
      `Expected exactly one artifact for ${options.artifact}; found ${matches.length}: ${matches.join(", ")}`,
    );
  }
  return matches[0];
}

function reserveLoopbackPort() {
  return new Promise((resolve, reject) => {
    const server = NodeNet.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
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

function fetchDescriptor(port, timeoutMs) {
  return new Promise((resolve, reject) => {
    const request = NodeHttp.get(
      {
        hostname: "127.0.0.1",
        port,
        path: DESCRIPTOR_PATH,
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
            reject(new Error(`descriptor returned HTTP ${response.statusCode ?? "unknown"}`));
            return;
          }
          try {
            const parsed = JSON.parse(body);
            if (typeof parsed.environmentId !== "string" || parsed.environmentId.length === 0) {
              reject(new Error("descriptor response did not include environmentId"));
              return;
            }
            resolve(parsed);
          } catch (error) {
            reject(error);
          }
        });
      },
    );
    request.once("timeout", () => {
      request.destroy(new Error(`descriptor request timed out after ${timeoutMs}ms`));
    });
    request.once("error", reject);
  });
}

function appendOutput(current, streamName, chunk) {
  const next = `${current}${streamName}: ${chunk.toString()}`;
  return next.length > MAX_CAPTURED_OUTPUT_BYTES
    ? next.slice(next.length - MAX_CAPTURED_OUTPUT_BYTES)
    : next;
}

function outputHasFatalPattern(output) {
  return FATAL_OUTPUT_PATTERNS.find((pattern) => pattern.test(output));
}

function signalProcessTree(child, signal) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }

  if (hostPlatform === "win32") {
    NodeChildProcess.spawnSync("taskkill", ["/pid", String(child.pid), "/t", "/f"], {
      stdio: "ignore",
      windowsHide: true,
    });
    return;
  }

  try {
    process.kill(-child.pid, signal);
  } catch {
    try {
      child.kill(signal);
    } catch {
      // Already gone.
    }
  }
}

async function terminateProcessTree(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  signalProcessTree(child, "SIGTERM");
  await NodeTimersPromises.setTimeout(1_500);
  signalProcessTree(child, "SIGKILL");
}

async function readOptionalText(filePath) {
  try {
    const text = await NodeFSP.readFile(filePath, "utf8");
    return text.length > MAX_CAPTURED_OUTPUT_BYTES
      ? text.slice(text.length - MAX_CAPTURED_OUTPUT_BYTES)
      : text;
  } catch {
    return "";
  }
}

async function collectDiagnostics(tempRoot, output) {
  const t3Home = NodePath.join(tempRoot, "t3-home");
  const logDir = NodePath.join(t3Home, "userdata", "logs");
  const serverChildLog = await readOptionalText(NodePath.join(logDir, "server-child.log"));
  return [
    output.trim() ? `\nProcess output:\n${output.trim()}` : "",
    serverChildLog.trim() ? `\nserver-child.log:\n${serverChildLog.trim()}` : "",
    `\nSmoke temp root: ${tempRoot}`,
  ].join("");
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const executablePath = await resolveExecutable(options);
  if (!NodeFS.existsSync(executablePath)) {
    throw new Error(`Executable does not exist: ${executablePath}`);
  }

  if (hostPlatform !== "win32") {
    NodeFS.chmodSync(executablePath, 0o755);
  }

  const tempRoot = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-desktop-smoke-"));
  const t3Home = NodePath.join(tempRoot, "t3-home");
  const appData = NodePath.join(tempRoot, "app-data");
  await NodeFSP.mkdir(t3Home, { recursive: true });
  await NodeFSP.mkdir(appData, { recursive: true });

  const port = await reserveLoopbackPort();
  const serverChildLogPath = NodePath.join(t3Home, "userdata", "logs", "server-child.log");
  const launchArgs = hostPlatform === "linux" ? ["--no-sandbox", "--disable-gpu"] : [];
  const childEnv = {
    ...process.env,
    APPDATA: appData,
    APPIMAGE_EXTRACT_AND_RUN: "1",
    ELECTRON_ENABLE_LOGGING: "1",
    NO_AT_BRIDGE: "1",
    T3CODE_DISABLE_AUTO_UPDATE: "1",
    T3CODE_HOME: t3Home,
    T3CODE_NO_BROWSER: "1",
    T3CODE_PORT: String(port),
    XDG_CONFIG_HOME: appData,
  };
  delete childEnv.ELECTRON_RUN_AS_NODE;
  delete childEnv.VITE_DEV_SERVER_URL;

  console.log(`[desktop-launch-smoke] Launching ${executablePath}`);
  console.log(`[desktop-launch-smoke] Waiting for ${DESCRIPTOR_PATH} on 127.0.0.1:${port}`);

  const child = NodeChildProcess.spawn(executablePath, launchArgs, {
    cwd: NodePath.dirname(executablePath),
    detached: true,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
  });

  let output = "";
  let exit = null;
  child.stdout.on("data", (chunk) => {
    output = appendOutput(output, "stdout", chunk);
  });
  child.stderr.on("data", (chunk) => {
    output = appendOutput(output, "stderr", chunk);
  });
  child.once("exit", (code, signal) => {
    exit = { code, signal };
  });

  const deadline = Date.now() + options.timeoutMs;
  try {
    while (Date.now() < deadline) {
      const serverChildLog = await readOptionalText(serverChildLogPath);
      const fatalPattern = outputHasFatalPattern(`${output}\n${serverChildLog}`);
      if (fatalPattern) {
        throw new Error(`Desktop launch emitted fatal pattern ${fatalPattern}`);
      }
      if (exit) {
        throw new Error(
          `Desktop process exited before readiness (code=${exit.code ?? "null"}, signal=${exit.signal ?? "null"})`,
        );
      }

      try {
        const descriptor = await fetchDescriptor(port, 1_000);
        console.log(
          `[desktop-launch-smoke] Ready: environmentId=${descriptor.environmentId} label=${descriptor.label ?? ""}`,
        );
        return;
      } catch {
        await NodeTimersPromises.setTimeout(500);
      }
    }

    throw new Error(`Timed out after ${options.timeoutMs}ms waiting for desktop backend readiness`);
  } catch (error) {
    const diagnostics = await collectDiagnostics(tempRoot, output);
    throw new Error(`${error instanceof Error ? error.message : String(error)}${diagnostics}`, {
      cause: error,
    });
  } finally {
    await terminateProcessTree(child);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack || error.message : String(error));
  process.exit(1);
});
