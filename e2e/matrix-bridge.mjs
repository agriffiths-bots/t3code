#!/usr/bin/env node
// Manual/release-gate Matrix bridge E2E. Owns every temp path, loopback port,
// captured PID, credential, assertion, teardown, and redaction. Never points at
// live T3 state or a real Matrix deployment.
//
//   node e2e/matrix-bridge.mjs            full bridge release gate
//   node e2e/matrix-bridge.mjs --smoke    harness self-check, no bridge
//
// The release gate needs a server that advertises
// environment.capabilities.matrixBridge. The self-check needs nothing but this
// repo: it proves the disposable homeserver, the two registered accounts, the
// encrypted CLI client, and the ephemeral T3 boot all work, so a release-gate
// failure can be read as a bridge failure rather than a harness failure.

import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

const REPO_ROOT = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const CLI_PATH = NodePath.join(REPO_ROOT, "e2e", "matrix-cli.mjs");
const FIXTURE_PATH = NodePath.join(REPO_ROOT, "e2e", "fixtures", "conduwuit-matrix-bridge.toml");
const SERVER_ENTRY = NodePath.join(REPO_ROOT, "apps", "server", "src", "bin.ts");
const HEARTBEAT_PATH = process.env.MATRIX_E2E_HEARTBEAT ?? "/tmp/t3-matrix-e2e.progress";
const FAILURE_LOG_PATH = process.env.MATRIX_E2E_FAILURE_LOG ?? "/tmp/t3-matrix-e2e-failure.log";
const CONDUWUIT_VERSION = "v0.5.0-rc4";
const CONDUWUIT_ASSETS = {
  x86_64: {
    name: "static-x86_64-linux-musl",
    sha256: "bf8cf47d42a86907d473aba7fc041d04779b88d308ad88fe395dbeb133ce1c53",
  },
  arm64: {
    name: "static-aarch64-linux-musl",
    sha256: "91c19569564334f15876ac1af7aaa38c4c62a9d247cf2b79062958b8e26bd927",
  },
};
// Upstream conduwuit, archived after the 0.5 line and since renamed from
// girlbossceo/conduwuit. The pin that matters is the checksum below, not the
// host: an asset that does not match is never executed.
const CONDUWUIT_RELEASE = "https://github.com/x86pup/conduwuit/releases/download";
const SDK_PACKAGES = ["matrix-bot-sdk@0.8.0", "@matrix-org/matrix-sdk-crypto-nodejs@0.4.0"];
// Downloaded fixtures are expensive and immutable, so they are cached for the
// user rather than re-fetched per run. Everything else lives in the temp root.
const USER_CACHE = NodePath.join(NodeOS.homedir(), ".cache", "t3-matrix-e2e");
const T3_PORT_MIN = 13_910;
const T3_PORT_MAX = 13_940;
const MATRIX_PORT_MIN = 18_700;
const MATRIX_PORT_MAX = 18_900;
// Every write this harness makes lands in its temp root or the fixture cache.
// The port and paths below are the ones a bug would plausibly reach anyway:
// the default T3 port and the developer's live homes.
const FORBIDDEN_PORT = 3773;
const FORBIDDEN_PATHS = [".t3", ".t3-vps"].map((entry) => NodePath.join(NodeOS.homedir(), entry));
const MEGOLM = "m.megolm.v1.aes-sha2";
const PAIRING_PROMPT =
  "T3 bridge is locked. Reply with a pairing code from T3 Settings > Connections.";
const PAIRING_REJECT = "Pairing code rejected. It is invalid, expired, revoked, or already used.";
const PAIRING_OK = "Pairing complete. T3 bridging is active when a thread is selected.";
const CAPABILITY_MISSING =
  "This T3 server does not advertise environment.capabilities.matrixBridge, so it cannot create the encrypted room, run pairing, or bridge a thread. Run the gate against a build whose Matrix bridge reactor and pairing gate are present, or run `node e2e/matrix-bridge.mjs --smoke` to check the harness itself.";
// No harness request may hang: a stalled response has to abort inside the
// deadline its caller advertised, or teardown never runs.
const HTTP_TIMEOUT_MS = 15_000;
const MIN_HTTP_TIMEOUT_MS = 1_000;
const DOWNLOAD_TIMEOUT_MS = 300_000;
const LOCAL_SEND_MS = 5_000;
const INBOUND_MS = 5_000;
const TURN_MS = 180_000;
const RECOVERY_MS = 90_000;
const RUN_ID = NodeCrypto.randomUUID();

const secrets = new Set();
const pids = [];
let tempRoot = null;
let failed = false;
let teardownPromise = null;

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Write a fixed well-known path without following a symlink into someone
 * else's file. The heartbeat and failure log are the only paths this harness
 * writes outside its own temp root.
 */
function writeOwnedFile(filePath, contents) {
  try {
    const stat = NodeFS.lstatSync(filePath);
    if (stat.isSymbolicLink()) throw new Error(`refusing to follow symlink ${filePath}`);
    if (!stat.isFile()) throw new Error(`refusing to write non-file ${filePath}`);
    if (stat.uid !== NodeOS.userInfo().uid)
      throw new Error(`refusing to write unowned ${filePath}`);
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  const fd = NodeFS.openSync(
    filePath,
    NodeFS.constants.O_WRONLY |
      NodeFS.constants.O_CREAT |
      NodeFS.constants.O_TRUNC |
      NodeFS.constants.O_NOFOLLOW,
    0o600,
  );
  try {
    NodeFS.writeFileSync(fd, contents);
  } finally {
    NodeFS.closeSync(fd);
  }
}

/** Progress only. It must never be the reason a run, or a teardown, dies. */
function heartbeat(step, detail = "") {
  try {
    writeOwnedFile(HEARTBEAT_PATH, `${nowIso()} ${step}${detail ? ` ${detail}` : ""}\n`);
  } catch (error) {
    console.error(`heartbeat write failed: ${error instanceof Error ? error.message : error}`);
  }
}

function rememberSecret(value) {
  if (value) secrets.add(String(value));
}

/**
 * `t3 serve` prints a startup pairing token, a pairing URL, and a terminal QR
 * code that encodes the same URL, so whole glyph lines are dropped rather than
 * scrubbed. Callers must pass whole lines: half a line cannot be recognised.
 */
function redact(text) {
  let output = String(text);
  for (const secret of secrets) {
    if (secret) output = output.split(secret).join("[redacted]");
  }
  return output
    .replace(/^[ \t]*[█▀▄▐▌■]+.*$/gm, "[qr code omitted]")
    .replace(/Bearer [A-Za-z0-9._~+/-]+/g, "Bearer [redacted]")
    .replace(/Token: \S+/g, "Token: [redacted]")
    .replace(/token=[A-Za-z0-9._~+/-]+/g, "token=[redacted]")
    .replace(/"access_token"\s*:\s*"[^"]+"/g, '"access_token":"[redacted]"')
    .replace(/"credential"\s*:\s*"[^"]+"/g, '"credential":"[redacted]"');
}

function log(message) {
  console.log(redact(message));
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

/**
 * Follow symlinks on both sides: a lexical compare is trivial to walk around.
 * A path that does not exist yet still has to be resolved, so this walks up to
 * the deepest ancestor that does exist, resolves that, and re-appends the rest.
 */
function realPathOrSelf(target) {
  const absolute = NodePath.resolve(target);
  const trailing = [];
  let cursor = absolute;
  for (;;) {
    try {
      return NodePath.join(NodeFS.realpathSync(cursor), ...trailing);
    } catch {
      const parent = NodePath.dirname(cursor);
      if (parent === cursor) return absolute;
      trailing.unshift(NodePath.basename(cursor));
      cursor = parent;
    }
  }
}

function assertSafePath(target) {
  const resolved = realPathOrSelf(target);
  for (const entry of FORBIDDEN_PATHS) {
    const forbidden = realPathOrSelf(entry);
    if (resolved === forbidden || resolved.startsWith(`${forbidden}${NodePath.sep}`)) {
      throw new Error(`refusing to touch ${forbidden}`);
    }
  }
  return resolved;
}

function assertSafePort(port) {
  if (port === FORBIDDEN_PORT) throw new Error(`refusing to bind the default T3 port ${port}`);
  return port;
}

function portIsFree(port) {
  return new Promise((resolve) => {
    const server = NodeNet.createServer();
    server.unref();
    server.on("error", () => resolve(false));
    server.listen(port, "127.0.0.1", () => server.close(() => resolve(true)));
  });
}

async function randomFreePort(min, max) {
  for (let attempt = 0; attempt < 40; attempt += 1) {
    const port = assertSafePort(min + Math.floor(Math.random() * (max - min + 1)));
    if (await portIsFree(port)) return port;
  }
  throw new Error(`no free loopback port in ${min}-${max}`);
}

async function firstFreePort(min, max) {
  for (let port = min; port <= max; port += 1) {
    if (await portIsFree(assertSafePort(port))) return port;
  }
  throw new Error(`no free loopback port in ${min}-${max}`);
}

async function waitHttpOk(url, timeoutMs = 20_000) {
  const started = Date.now();
  let lastError = "not attempted";
  while (Date.now() - started < timeoutMs) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(1_500) });
      if (response.ok) return;
      lastError = `HTTP ${response.status}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(100);
  }
  throw new Error(`timed out waiting for ${url}: ${lastError}`);
}

/**
 * Short-lived helpers (`npm install`, the native downloader, the token CLI) get
 * the same treatment as the long-lived servers: their own process group, a
 * tracked record, and the shared terminate-then-kill sweep. A signal arriving
 * mid-install would otherwise leave npm and its downloader descendants
 * rewriting the shared SDK cache, or the token CLI reading a T3 home that
 * teardown is deleting.
 */
function execFile(file, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = NodeChildProcess.execFile(
      file,
      args,
      { timeout: 180_000, detached: true, ...options },
      (error, out, err) => {
        if (error) {
          error.stdout = out;
          error.stderr = err;
          reject(error);
          return;
        }
        resolve({ stdout: out, stderr: err });
      },
    );
    if (child.pid != null) trackPid(child.pid, `helper ${NodePath.basename(file)}`, null, child);
  });
}

function sha256File(filePath) {
  return NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(filePath)).digest("hex");
}

function cpuArch() {
  const machine = NodeChildProcess.execFileSync("uname", ["-m"], { encoding: "utf8" }).trim();
  if (machine === "x86_64" || machine === "amd64") return "x86_64";
  if (machine === "aarch64" || machine === "arm64") return "arm64";
  throw new Error(`unsupported architecture ${machine}`);
}

async function download(url, destination) {
  const response = await fetch(url, { signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS) });
  if (!response.ok) throw new Error(`download ${url} failed: HTTP ${response.status}`);
  NodeFS.writeFileSync(destination, Buffer.from(await response.arrayBuffer()), { mode: 0o644 });
}

/**
 * Fetch the pinned homeserver into the user cache when absent, verify the
 * checksum, then run a copy from the temp root. An unpinned `latest` binary is
 * never executed.
 */
async function ensureConduwuit() {
  const asset = CONDUWUIT_ASSETS[cpuArch()];
  const cacheDir = NodePath.join(USER_CACHE, "conduwuit", CONDUWUIT_VERSION);
  NodeFS.mkdirSync(cacheDir, { recursive: true, mode: 0o700 });
  const cached = NodePath.join(cacheDir, asset.name);
  if (!NodeFS.existsSync(cached) || sha256File(cached) !== asset.sha256) {
    heartbeat("download-conduwuit", asset.name);
    await download(`${CONDUWUIT_RELEASE}/${CONDUWUIT_VERSION}/${asset.name}`, cached);
  }
  const digest = sha256File(cached);
  assert(digest === asset.sha256, `conduwuit checksum mismatch: ${digest}`);
  const local = NodePath.join(tempRoot, "bin", "conduwuit");
  NodeFS.mkdirSync(NodePath.dirname(local), { recursive: true, mode: 0o700 });
  NodeFS.copyFileSync(cached, local);
  NodeFS.chmodSync(local, 0o755);
  return local;
}

/**
 * The Matrix SDK and its native crypto binding are installed into the user
 * cache, not the workspace: the server does not depend on them yet, and the
 * pin must not drift with the repo lockfile.
 */
async function ensureSdk() {
  const cache = NodePath.join(USER_CACHE, "npm");
  const marker = NodePath.join(cache, ".pinned");
  const pin = `${SDK_PACKAGES.join("\n")}\n`;
  NodeFS.mkdirSync(cache, { recursive: true, mode: 0o700 });
  const installed =
    NodeFS.existsSync(marker) &&
    NodeFS.readFileSync(marker, "utf8") === pin &&
    NodeFS.existsSync(NodePath.join(cache, "node_modules", "matrix-bot-sdk"));
  if (!installed) {
    heartbeat("install-sdk");
    NodeFS.rmSync(marker, { force: true });
    NodeFS.writeFileSync(
      NodePath.join(cache, "package.json"),
      `${JSON.stringify({ name: "t3-matrix-e2e-sdk", private: true }, null, 2)}\n`,
    );
    // --ignore-scripts: nothing in this tree's transitive dependencies gets to
    // run code at install time. The two pinned packages are exact, but their
    // dependency graph is resolved fresh, so the install must not be a code
    // execution path. The one script this harness does need is invoked
    // explicitly below.
    await execFile(
      "npm",
      ["install", "--omit=dev", "--no-fund", "--no-audit", "--ignore-scripts", ...SDK_PACKAGES],
      { cwd: cache },
    );
    for (const spec of SDK_PACKAGES) {
      const at = spec.lastIndexOf("@");
      const [name, wanted] = [spec.slice(0, at), spec.slice(at + 1)];
      const manifest = NodePath.join(cache, "node_modules", ...name.split("/"), "package.json");
      const found = JSON.parse(NodeFS.readFileSync(manifest, "utf8")).version;
      assert(found === wanted, `${name} resolved to ${found}, expected the pinned ${wanted}`);
    }
    const cryptoDir = NodePath.join(
      cache,
      "node_modules",
      "@matrix-org",
      "matrix-sdk-crypto-nodejs",
    );
    if (NodeFS.existsSync(NodePath.join(cryptoDir, "download-lib.js"))) {
      await execFile(process.execPath, ["download-lib.js"], { cwd: cryptoDir });
    }
    NodeFS.writeFileSync(marker, pin);
  }
  process.env.MATRIX_E2E_SDK_DIR = cache;
  return cache;
}

function envHas(pid, needle) {
  try {
    return NodeFS.readFileSync(`/proc/${pid}/environ`, "utf8").split("\0").includes(needle);
  } catch {
    return false;
  }
}

function cwdIs(pid, expected) {
  try {
    return NodeFS.readlinkSync(`/proc/${pid}/cwd`) === expected;
  } catch {
    return false;
  }
}

function isAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function retire(record) {
  const index = pids.indexOf(record);
  if (index >= 0) pids.splice(index, 1);
}

/**
 * A record outlives its leader only for as long as the leader's process group
 * still has members. That is the exact window worth covering: a detached
 * leader that dies early can orphan its group (a T3 server's provider
 * subprocesses, say), and while the group is populated its id cannot be
 * recycled onto anything else. Once it drains, the record is retired, because
 * signalling that id later would be signalling a stranger.
 */
function retireWhenGroupDrains(record) {
  const timer = setInterval(() => {
    if (groupIsPopulated(record.pid)) return;
    clearInterval(timer);
    retire(record);
  }, 100);
  timer.unref();
}

function trackPid(pid, kind, prove, child) {
  const record = { pid, kind, prove };
  pids.push(record);
  child.on("exit", () => {
    if (!groupIsPopulated(pid)) {
      retire(record);
      return;
    }
    retireWhenGroupDrains(record);
  });
  return record;
}

function signalTree(pid, signal) {
  // Children are spawned detached, so the process group id equals the captured
  // pid and one signal reaches the whole tree. The group is only ever signalled
  // while the leader is alive, which is what keeps the id from being recycled.
  try {
    process.kill(-pid, signal);
    return;
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
  try {
    process.kill(pid, signal);
  } catch {
    // already gone
  }
}

/** Is any process still in this group? */
function groupIsPopulated(pid) {
  try {
    process.kill(-pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Stop one captured process tree. While the leader is alive its identity is
 * re-proved from /proc first, because this harness must never kill a process
 * it did not start. Once the leader has exited there is nothing left to prove
 * identity against, so the group is swept only if it is still populated: the
 * leader's own exit is what created the orphans worth sweeping.
 */
async function stopPid(record) {
  if (!record?.pid) return;
  const leaderAlive = isAlive(record.pid);
  if (leaderAlive && record.prove && !record.prove(record.pid)) {
    throw new Error(`refusing to kill ${record.kind} pid ${record.pid}: identity check failed`);
  }
  if (!leaderAlive && !groupIsPopulated(record.pid)) {
    retire(record);
    return;
  }
  signalTree(record.pid, "SIGTERM");
  if (await waitForTree(record, 5_000)) {
    retire(record);
    return;
  }
  signalTree(record.pid, "SIGKILL");
  if (await waitForTree(record, 2_000)) {
    retire(record);
    return;
  }
  throw new Error(`${record.kind} pid ${record.pid} survived SIGKILL`);
}

function treeIsGone(record) {
  return !isAlive(record.pid) && !groupIsPopulated(record.pid);
}

async function waitForTree(record, timeoutMs) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (treeIsGone(record)) return true;
    await sleep(50);
  }
  return treeIsGone(record);
}

/**
 * Redaction is line-based, so child output is buffered until a newline. A
 * half-written QR row cannot be recognised and must never reach the log.
 */
function lineRedactor(onLine) {
  let buffer = "";
  return {
    push(chunk) {
      buffer += chunk.toString();
      let index = buffer.indexOf("\n");
      while (index >= 0) {
        onLine(buffer.slice(0, index));
        buffer = buffer.slice(index + 1);
        index = buffer.indexOf("\n");
      }
    },
    flush() {
      if (buffer) {
        onLine(buffer);
        buffer = "";
      }
    },
  };
}

function spawnCaptured(command, args, options, kind, prove) {
  const child = NodeChildProcess.spawn(command, args, {
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
    ...options,
  });
  if (child.pid == null) throw new Error(`failed to spawn ${kind}`);
  const record = trackPid(child.pid, kind, prove, child);
  const stream = NodeFS.createWriteStream(NodePath.join(tempRoot, `${kind}.log`), { flags: "a" });
  const onLine = (line) => {
    const printedToken = line.match(/Token: (\S+)/);
    if (printedToken) rememberSecret(printedToken[1]);
    stream.write(`${redact(line)}\n`);
  };
  // One buffer per pipe. Sharing a buffer would splice a partial stdout line
  // onto a stderr line and produce a synthetic line the QR filter cannot
  // recognise, which is the leak this whole mechanism exists to prevent.
  const redactors = [
    { source: child.stdout, redactor: lineRedactor(onLine) },
    { source: child.stderr, redactor: lineRedactor(onLine) },
  ];
  for (const { source, redactor } of redactors) source.on("data", (chunk) => redactor.push(chunk));
  // `close` fires once both pipes are drained; `exit` can arrive before them,
  // and finalizing there loses trailing output or writes after end().
  child.on("close", () => {
    for (const { redactor } of redactors) redactor.flush();
    stream.end();
  });
  return { child, record };
}

async function startConduwuit(binary, configPath, port, dataDir) {
  const spawned = spawnCaptured(
    binary,
    ["-c", configPath],
    { cwd: dataDir, env: { ...process.env, MATRIX_E2E_RUN: RUN_ID } },
    "conduwuit",
    (pid) => cwdIs(pid, dataDir) && envHas(pid, `MATRIX_E2E_RUN=${RUN_ID}`),
  );
  await waitHttpOk(`http://127.0.0.1:${port}/_matrix/client/versions`);
  return spawned;
}

/** One `matrix-cli.mjs` child, spoken to over its JSON-lines protocol. */
class MatrixCli {
  constructor(role) {
    this.role = role;
    this.seq = 0;
    this.pending = new Map();
    this.child = NodeChildProcess.spawn(process.execPath, [CLI_PATH], {
      stdio: ["pipe", "pipe", "pipe"],
      cwd: REPO_ROOT,
      detached: true,
      env: {
        ...process.env,
        MATRIX_E2E_SDK_DIR: process.env.MATRIX_E2E_SDK_DIR,
        MATRIX_E2E_RUN: RUN_ID,
        MATRIX_E2E_ROLE: role,
      },
    });
    const kind = `matrix-cli-${role}`;
    if (this.child.pid == null) throw new Error(`failed to spawn ${kind}`);
    this.record = trackPid(
      this.child.pid,
      kind,
      (pid) => envHas(pid, `MATRIX_E2E_ROLE=${role}`) && envHas(pid, `MATRIX_E2E_RUN=${RUN_ID}`),
      this.child,
    );
    const errLog = NodeFS.createWriteStream(NodePath.join(tempRoot, `${kind}.stderr.log`), {
      flags: "a",
    });
    const redactor = lineRedactor((line) => errLog.write(`${redact(line)}\n`));
    this.child.stderr.on("data", (chunk) => redactor.push(chunk));
    this.child.on("exit", () => {
      for (const [, waiter] of this.pending) waiter.reject(new Error(`${kind} exited`));
      this.pending.clear();
    });
    // Only `close` guarantees stderr has drained; flushing on `exit` drops the
    // trailing diagnostics that explain why the client died.
    this.child.on("close", () => {
      redactor.flush();
      errLog.end();
    });
    NodeReadline.createInterface({ input: this.child.stdout }).on("line", (line) => {
      if (!line.trim()) return;
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        log(`[${kind}] non-json: ${line}`);
        return;
      }
      if (message.accessToken) rememberSecret(message.accessToken);
      const waiter = message.id == null ? undefined : this.pending.get(message.id);
      if (!waiter) return;
      this.pending.delete(message.id);
      if (message.ok) waiter.resolve(message);
      else waiter.reject(new Error(`${this.role} ${message.op ?? "?"}: ${message.error}`));
    });
  }

  request(payload, timeoutMs = 20_000) {
    const id = ++this.seq;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${this.role} ${payload.op} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      this.child.stdin.write(`${JSON.stringify({ id, ...payload })}\n`);
    });
  }

  async close() {
    try {
      await this.request({ op: "stop" }, 5_000);
    } catch {
      // The teardown sweep owns the process if it will not stop politely.
    }
    await stopPid(this.record).catch(() => {});
  }
}

/** Minimal client for the server's typed WebSocket RPC protocol. */
class WsRpc {
  constructor(ws) {
    this.ws = ws;
    this.nextId = 1;
    this.pending = new Map();
    this.streams = new Map();
    this.ws.addEventListener("message", (event) => {
      const parsed = JSON.parse(
        typeof event.data === "string" ? event.data : new TextDecoder().decode(event.data),
      );
      for (const message of Array.isArray(parsed) ? parsed : [parsed]) this.#dispatch(message);
    });
  }

  #dispatch(message) {
    if (message._tag === "Ping") {
      this.ws.send(JSON.stringify({ _tag: "Pong" }));
      return;
    }
    if (message._tag === "Chunk") {
      const stream = this.streams.get(String(message.requestId));
      if (stream) for (const value of message.values) stream.onChunk(value);
      this.ws.send(JSON.stringify({ _tag: "Ack", requestId: message.requestId }));
      return;
    }
    if (message._tag !== "Exit") return;
    const id = String(message.requestId);
    const success = message.exit._tag === "Success";
    const stream = this.streams.get(id);
    if (stream) {
      this.streams.delete(id);
      if (success) stream.resolve();
      else stream.reject(this.#error(message.exit));
    }
    const pending = this.pending.get(id);
    if (!pending) return;
    this.pending.delete(id);
    if (success) pending.resolve(message.exit.value);
    else pending.reject(this.#error(message.exit));
  }

  #error(exit) {
    const cause = exit.cause?.[0];
    const error = new Error(cause?.error?.message ?? cause?.defect ?? "rpc failed");
    error.rpcError = cause?.error;
    return error;
  }

  call(tag, payload = {}, timeoutMs = 20_000) {
    const id = String(this.nextId++);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`rpc ${tag} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      this.ws.send(JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] }));
    });
  }

  subscribe(tag, payload, onChunk) {
    const id = String(this.nextId++);
    let resolve;
    let reject;
    const ended = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    this.streams.set(id, { onChunk, resolve, reject });
    this.ws.send(JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] }));
    return {
      interrupt: () => this.ws.send(JSON.stringify({ _tag: "Interrupt", requestId: id })),
      ended,
    };
  }

  close() {
    this.ws.close();
  }
}

async function connectRpc(origin, token) {
  const ticketResponse = await fetch(`${origin}/api/auth/websocket-ticket`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
  });
  assert(ticketResponse.ok, `websocket-ticket HTTP ${ticketResponse.status}`);
  const { ticket } = await ticketResponse.json();
  rememberSecret(ticket);
  const ws = new WebSocket(
    `${origin.replace(/^http/, "ws")}/ws?wsTicket=${encodeURIComponent(ticket)}`,
  );
  // A server that accepts the socket and then stalls the upgrade emits neither
  // open nor error, so the handshake needs its own deadline like every other
  // request here.
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.close();
      reject(new Error(`websocket handshake timed out after ${HTTP_TIMEOUT_MS}ms`));
    }, HTTP_TIMEOUT_MS);
    ws.addEventListener(
      "open",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
    ws.addEventListener(
      "error",
      () => {
        clearTimeout(timer);
        ws.close();
        reject(new Error("websocket failed to open"));
      },
      { once: true },
    );
  });
  return new WsRpc(ws);
}

/**
 * Every request carries an abort deadline. A server that accepts a connection
 * and then stalls would otherwise outlive the polling budget its caller was
 * promised, and the run could never reach failure reporting or teardown.
 */
async function t3Fetch(origin, token, pathname, options = {}) {
  const { timeoutMs = HTTP_TIMEOUT_MS, ...init } = options;
  const response = await fetch(`${origin}${pathname}`, {
    ...init,
    signal: AbortSignal.timeout(timeoutMs),
    headers: {
      authorization: `Bearer ${token}`,
      ...(init.body ? { "content-type": "application/json" } : {}),
      ...init.headers,
    },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${init.method ?? "GET"} ${pathname} HTTP ${response.status} ${text}`);
  }
  return text ? JSON.parse(text) : null;
}

function dispatch(t3, command) {
  return t3Fetch(t3.origin, t3.token, "/api/orchestration/dispatch", {
    method: "POST",
    body: JSON.stringify(command),
  });
}

function threadSnapshot(t3, threadId, timeoutMs) {
  return t3Fetch(t3.origin, t3.token, `/api/orchestration/threads/${threadId}`, { timeoutMs });
}

async function waitThread(t3, threadId, predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  let last = null;
  while (Date.now() < deadline) {
    // Spend at most what is left of the caller's budget on one request, with a
    // floor so the last poll before the deadline still has time to answer.
    const remaining = Math.max(MIN_HTTP_TIMEOUT_MS, deadline - Date.now());
    last = await threadSnapshot(t3, threadId, Math.min(remaining, HTTP_TIMEOUT_MS));
    if (predicate(last.thread)) return last.thread;
    await sleep(100);
  }
  throw new Error(
    `${label} timed out after ${timeoutMs}ms (last ${last?.thread?.latestTurn?.state})`,
  );
}

const TERMINAL_TURN_STATES = ["completed", "error", "interrupted"];

/**
 * A thread's turn position, taken before dispatching work.
 *
 * Every completion wait in this file has to be bound to one of these. T3 keeps
 * the previous turn as `latestTurn` until the new turn is identified, and it
 * projects the inbound user message before that happens, so a predicate that
 * only names a state matches the OLD turn and hands back a final that was
 * already delivered. The whole gate rests on negatives, and a stale final
 * makes those negatives vacuous.
 */
function turnMarker(thread) {
  return { turnId: thread.latestTurn?.turnId ?? null, turns: thread.turns.length };
}

function isNewTurn(thread, marker) {
  return (
    thread.latestTurn?.turnId != null &&
    thread.latestTurn.turnId !== marker.turnId &&
    thread.turns.length === marker.turns + 1
  );
}

function waitNewTurnSettled(t3, threadId, marker, label, states = TERMINAL_TURN_STATES) {
  return waitThread(
    t3,
    threadId,
    (thread) => isNewTurn(thread, marker) && states.includes(thread.latestTurn.state),
    TURN_MS,
    label,
  );
}

async function waitStatus(getStatus, predicate, timeoutMs, label) {
  const started = Date.now();
  let last = getStatus();
  while (Date.now() - started < timeoutMs) {
    last = getStatus();
    if (last && predicate(last)) return last;
    await sleep(100);
  }
  throw new Error(`${label} timed out after ${timeoutMs}ms (last ${JSON.stringify(last)})`);
}

/**
 * The projected final for a turn. A turn can carry several non-streaming
 * assistant messages (mid-turn segments), and only the last one is the final
 * the bridge is allowed to post.
 */
function lastAssistant(thread, turnId) {
  return (
    thread.messages.findLast(
      (message) =>
        message.role === "assistant" &&
        message.streaming === false &&
        (turnId == null || message.turnId === turnId) &&
        message.text.length > 0,
    ) ?? null
  );
}

/**
 * T3 emits the inbound user message with `turnId: null` and never rebinds it
 * to the turn it starts (`decider.ts`, the `thread.turn.start` branch), so
 * user messages are matched by text and turns are identified separately by
 * id. Scoping a user-message lookup by turn id here would silently match
 * nothing.
 */
function userMessages(thread) {
  return thread.messages.filter((message) => message.role === "user");
}

/**
 * `availability` says whether the driver exists at all; `status` is the health
 * of the installed one. A provider can be installed, authenticated, and still
 * report `error`, so both have to hold before the preference is applied,
 * otherwise a broken Codex is chosen over a healthy alternative.
 */
function pickProvider(config) {
  const ready = (config.providers ?? []).filter(
    (provider) =>
      provider.enabled &&
      provider.installed &&
      provider.availability !== "unavailable" &&
      provider.status === "ready" &&
      provider.auth?.status === "authenticated",
  );
  const preferred = ready.find((provider) => provider.driver === "codex") ?? ready[0];
  assert(preferred, "no ready, authenticated provider is available for the release-gate turns");
  const model = preferred.models.find((item) => item.isDefault)?.slug ?? preferred.models[0]?.slug;
  assert(model, `provider ${preferred.instanceId} has no models`);
  return { instanceId: preferred.instanceId, model };
}

async function startT3(t3HomeDir, workspace, port) {
  const env = { ...process.env };
  // Dev-only origins bake localhost into the bundle; tracing and tailnet
  // serving belong to the developer's real server, not this child.
  for (const name of [
    "VITE_DEV_SERVER_URL",
    "VITE_HTTP_URL",
    "VITE_WS_URL",
    "T3CODE_TAILSCALE_SERVE",
    "T3CODE_TAILSCALE_SERVE_PORT",
    "T3CODE_TRACE_FILE",
    "T3CODE_OTLP_TRACES_URL",
    "T3CODE_OTLP_METRICS_URL",
  ]) {
    delete env[name];
  }
  env.T3CODE_HOME = t3HomeDir;
  env.T3CODE_NO_BROWSER = "1";
  env.T3CODE_LOG_LEVEL = "Info";
  env.MATRIX_E2E_RUN = RUN_ID;
  const { child } = spawnCaptured(
    process.execPath,
    [SERVER_ENTRY, "serve", "--port", String(port), "--host", "127.0.0.1", workspace],
    { cwd: REPO_ROOT, env },
    "t3",
    (pid) => envHas(pid, `T3CODE_HOME=${t3HomeDir}`) && envHas(pid, `MATRIX_E2E_RUN=${RUN_ID}`),
  );
  const origin = `http://127.0.0.1:${port}`;
  const logPath = NodePath.join(tempRoot, "t3.log");
  let exited = null;
  child.on("exit", (code, signal) => {
    exited = { code, signal };
  });
  const deadline = Date.now() + 240_000;
  let listening = false;
  while (!listening && Date.now() < deadline) {
    if (exited) throw new Error(`ephemeral T3 exited during boot (${JSON.stringify(exited)})`);
    const logText = NodeFS.existsSync(logPath) ? NodeFS.readFileSync(logPath, "utf8") : "";
    if (logText.includes("T3 Code server is ready.")) break;
    try {
      await fetch(origin, { signal: AbortSignal.timeout(400) });
      listening = true;
    } catch {
      await sleep(200);
    }
  }
  const issued = await execFile(
    process.execPath,
    [SERVER_ENTRY, "auth", "session", "issue", "--token-only", "--label", "matrix-e2e"],
    { cwd: REPO_ROOT, env },
  );
  const token = issued.stdout.trim().split("\n").at(-1).trim();
  assert(token.length > 20, "failed to mint an administrative bearer for the ephemeral T3");
  rememberSecret(token);
  const t3 = { origin, token, env, child };
  for (let attempt = 0; attempt < 60; attempt += 1) {
    if (exited) throw new Error(`ephemeral T3 exited during boot (${JSON.stringify(exited)})`);
    const response = await fetch(`${origin}/api/orchestration/snapshot`, {
      headers: { authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
    });
    if (response.status === 200) return t3;
    await sleep(250);
  }
  throw new Error("ephemeral T3 never became command-ready");
}

function requireMatrixBridgeCapability(serverConfig) {
  if (serverConfig.environment?.capabilities?.matrixBridge !== true) {
    throw new Error(CAPABILITY_MISSING);
  }
}

async function createThread(t3, projectId, title, modelSelection) {
  const threadId = NodeCrypto.randomUUID();
  await dispatch(t3, {
    type: "thread.create",
    commandId: NodeCrypto.randomUUID(),
    threadId,
    projectId,
    title,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: nowIso(),
  });
  return threadId;
}

function startTurn(t3, threadId, text, modelSelection) {
  return dispatch(t3, {
    type: "thread.turn.start",
    commandId: NodeCrypto.randomUUID(),
    threadId,
    message: { messageId: NodeCrypto.randomUUID(), role: "user", text, attachments: [] },
    runtimeMode: "full-access",
    interactionMode: "default",
    modelSelection,
    createdAt: nowIso(),
  });
}

/**
 * Everything both modes need: a disposable homeserver, two registered Matrix
 * accounts, an ephemeral T3 on its own home, and an authenticated RPC session.
 * Neither Matrix client is started here, because who owns the bot device
 * differs between the two modes.
 */
async function bringUp(mode) {
  // Check the real temp parent before writing into it: a TMPDIR under, or
  // symlinked into, a live T3 home would otherwise be created first and only
  // rejected afterwards, leaving a directory behind at best.
  const tmpParent = assertSafePath(NodeOS.tmpdir());
  tempRoot = assertSafePath(NodeFS.mkdtempSync(NodePath.join(tmpParent, "t3-matrix-e2e-")));
  NodeFS.chmodSync(tempRoot, 0o700);
  const t3Home = NodePath.join(tempRoot, "t3-home");
  const workspace = NodePath.join(tempRoot, "workspace");
  const conduwuitDir = NodePath.join(tempRoot, "conduwuit");
  const dbPath = NodePath.join(conduwuitDir, "db");
  const tokenPath = NodePath.join(conduwuitDir, "registration-token");
  for (const dir of [t3Home, workspace, dbPath]) {
    NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
  }
  const matrixPort = await randomFreePort(MATRIX_PORT_MIN, MATRIX_PORT_MAX);
  const t3Port = await firstFreePort(T3_PORT_MIN, T3_PORT_MAX);

  // T3 comes up first so the release gate can refuse a server without the
  // capability before paying for a homeserver download and a Matrix bring-up
  // it is about to throw away.
  heartbeat("1", `ephemeral T3 :${t3Port}`);
  const t3 = await startT3(t3Home, workspace, t3Port);
  const rpc = await connectRpc(t3.origin, t3.token);
  const serverConfig = await rpc.call("server.getConfig", {});
  log(`ephemeral T3 ${serverConfig.environment.serverVersion} on ${t3.origin}`);
  if (mode === "release-gate") requireMatrixBridgeCapability(serverConfig);

  await ensureSdk();
  const conduwuitBin = await ensureConduwuit();

  const registrationToken = NodeCrypto.randomBytes(18).toString("base64url");
  const botPassword = NodeCrypto.randomBytes(18).toString("base64url");
  const ownerPassword = NodeCrypto.randomBytes(18).toString("base64url");
  for (const secret of [registrationToken, botPassword, ownerPassword]) rememberSecret(secret);
  NodeFS.writeFileSync(tokenPath, `${registrationToken}\n`, { mode: 0o600 });
  const configPath = NodePath.join(conduwuitDir, "conduwuit.toml");
  NodeFS.writeFileSync(
    configPath,
    NodeFS.readFileSync(FIXTURE_PATH, "utf8")
      .replaceAll('"__PORT__"', String(matrixPort))
      .replaceAll("__DATABASE_PATH__", dbPath)
      .replaceAll("__REGISTRATION_TOKEN_FILE__", tokenPath)
      .replaceAll("__WELL_KNOWN_SERVER__", `127.0.0.1:${matrixPort}`)
      .replaceAll("__WELL_KNOWN_CLIENT__", `http://127.0.0.1:${matrixPort}`),
  );
  const homeserver = `http://127.0.0.1:${matrixPort}`;

  heartbeat("2", `conduwuit :${matrixPort}`);
  const conduwuit = await startConduwuit(conduwuitBin, configPath, matrixPort, conduwuitDir);

  heartbeat("3", "register bot and owner accounts");
  const botCli = new MatrixCli("bot");
  await botCli.request({
    op: "configure",
    homeserver,
    store: NodePath.join(tempRoot, "bot-store"),
  });
  const bot = await botCli.request({
    op: "register",
    username: "t3bot",
    password: botPassword,
    registrationToken,
  });
  const ownerStore = NodePath.join(tempRoot, "owner-store");
  const ownerCli = new MatrixCli("owner");
  await ownerCli.request({ op: "configure", homeserver, store: ownerStore });
  const owner = await ownerCli.request({
    op: "register",
    username: "owner",
    password: ownerPassword,
    registrationToken,
  });

  return {
    conduwuit,
    conduwuitBin,
    configPath,
    conduwuitDir,
    matrixPort,
    homeserver,
    botCli,
    bot,
    ownerCli,
    owner,
    ownerStore,
    workspace,
    t3,
    rpc,
    serverConfig,
  };
}

/**
 * Harness self-check. No bridge is involved: the two registered accounts talk
 * to each other directly, which is exactly the machinery the release gate
 * borrows. If this is red, the release gate proves nothing.
 */
async function runSmoke(ctx) {
  heartbeat("smoke-1", "capability precondition");
  const advertised = ctx.serverConfig.environment?.capabilities?.matrixBridge === true;
  if (advertised) {
    log("server advertises the Matrix bridge capability; the release gate can run here");
  } else {
    let message = null;
    try {
      requireMatrixBridgeCapability(ctx.serverConfig);
    } catch (error) {
      message = error.message;
    }
    assert(message === CAPABILITY_MISSING, "the capability precondition did not report itself");
    log(`release gate would stop here: ${message}`);
  }

  heartbeat("smoke-2", "start both encrypted clients");
  await ctx.botCli.request({ op: "start" }, 60_000);
  await ctx.ownerCli.request({ op: "start" }, 60_000);

  heartbeat("smoke-3", "encrypted room");
  const created = await ctx.botCli.request(
    { op: "createRoom", invite: [ctx.owner.userId], name: "t3-matrix-e2e" },
    30_000,
  );
  const roomId = created.roomId;
  await ctx.ownerCli.request({ op: "waitInvite", roomId, timeoutMs: 30_000 }, 31_000);
  await ctx.ownerCli.request({ op: "join", roomId }, 30_000);
  const state = await ctx.ownerCli.request({ op: "roomState", roomId });
  assert(state.joinRules.join_rule === "invite", `join rule is ${state.joinRules.join_rule}`);
  assert(state.encryption.algorithm === MEGOLM, `encryption is ${state.encryption.algorithm}`);
  const expected = JSON.stringify([ctx.bot.userId, ctx.owner.userId].toSorted());
  const members = await ctx.ownerCli.request({ op: "members", roomId });
  assert(
    JSON.stringify(members.members) === expected,
    `unexpected membership ${JSON.stringify(members.members)}`,
  );

  heartbeat("smoke-4", "encrypted round trip");
  const outbound = `E2EE_BOT_TO_OWNER_${NodeCrypto.randomBytes(4).toString("hex")}`;
  await ctx.botCli.request({ op: "send", roomId, body: outbound }, 30_000);
  const received = await ctx.ownerCli.request(
    { op: "waitText", roomId, from: ctx.bot.userId, body: outbound, timeoutMs: 30_000 },
    31_000,
  );
  assert(received.event.encrypted === true, "bot message did not arrive encrypted");

  const inbound = `E2EE_OWNER_TO_BOT_${NodeCrypto.randomBytes(4).toString("hex")}`;
  await ctx.ownerCli.request({ op: "send", roomId, body: inbound }, 30_000);
  const echoed = await ctx.botCli.request(
    { op: "waitText", roomId, from: ctx.owner.userId, body: inbound, timeoutMs: 30_000 },
    31_000,
  );
  assert(echoed.event.encrypted === true, "owner message did not arrive encrypted");

  heartbeat("smoke-5", "close clients");
  await ctx.botCli.close();
  await ctx.ownerCli.close();
  ctx.rpc.close();
  log("matrix bridge harness self-check passed");
}

/**
 * The full scripted flow. Every step is a product requirement: pairing gate,
 * final-output-only outbound, inbound turns, mid-turn steering, owner moves,
 * outage recovery, and unbridge silence.
 */
async function runReleaseGate(ctx) {
  const { t3, rpc } = ctx;
  requireMatrixBridgeCapability(ctx.serverConfig);

  // The server owns the bot's Matrix device from here, so the harness must not
  // keep a second client on that account.
  await ctx.botCli.close();
  await ctx.ownerCli.request({ op: "start" }, 60_000);
  const botMxid = ctx.bot.userId;
  const ownerMxid = ctx.owner.userId;
  const ownerCli = ctx.ownerCli;

  let status = { state: "disabled", ownerThreadId: null };
  const statusSub = rpc.subscribe("matrixBridge.subscribeStatus", {}, (value) => {
    status = value;
  });
  // The Matrix client the harness talks through swaps when the homeserver is
  // restarted mid-run, so the witness below reads it indirectly.
  let activeCli = ownerCli;
  const setOwner = async (ownerThreadId, label) => {
    await rpc.call("matrixBridge.setOwner", { ownerThreadId });
    await waitStatus(
      () => status,
      (v) => v.ownerThreadId === ownerThreadId,
      10_000,
      label,
    );
  };

  /**
   * Force the bot inside T3 to observably catch up, and return the room events
   * that prove it. This exists because every negative assertion here needs a
   * clock, and the harness client's own /sync counter is the wrong one: it
   * says the harness polled, which is silent about where the independent bot
   * has reached, so a slow or reconnecting bot could act after the assertion
   * and turn the gate false-green.
   *
   * Matrix totally orders a room's timeline. Bridging a control thread and
   * getting a later message to land in T3 therefore proves the bot's inbound
   * consumer already passed everything sent before it, and getting that turn's
   * final back into the room proves its outbound worker drained past every
   * job queued before it. Both halves of silence get a real witness.
   */
  const bridgeWitness = async (controlThreadId, marker, restoreOwnerThreadId) => {
    await setOwner(controlThreadId, `${marker} witness owner`);
    const before = turnMarker((await threadSnapshot(t3, controlThreadId)).thread);
    const text = `${marker}_WITNESS_${NodeCrypto.randomBytes(4).toString("hex")}`;
    await activeCli.request({ op: "send", roomId, body: text });
    // The user message is projected before the turn it starts exists, and a
    // pending turn carries a null id, so "the message is here" and "a turn is
    // completed" can both be true while `latestTurn` is still the PREVIOUS
    // turn holding its old final. Waiting on that would hand the caller a
    // stale reply and prove nothing about the bridge draining. The witness
    // must name a new, identified turn that arrived with this message.
    const dispatched = await waitThread(
      t3,
      controlThreadId,
      (thread) =>
        isNewTurn(thread, before) && userMessages(thread).some((message) => message.text === text),
      INBOUND_MS,
      `${marker} witness message started a new turn`,
    );
    const witnessTurnId = dispatched.latestTurn.turnId;
    const settled = await waitThread(
      t3,
      controlThreadId,
      (thread) =>
        thread.latestTurn?.turnId === witnessTurnId &&
        thread.latestTurn.state === "completed" &&
        lastAssistant(thread, witnessTurnId),
      TURN_MS,
      `${marker} witness turn completed`,
    );
    const final = lastAssistant(settled, witnessTurnId).text.trim();
    await activeCli.request(
      { op: "waitText", roomId, from: botMxid, body: final, timeoutMs: LOCAL_SEND_MS },
      LOCAL_SEND_MS + 1_000,
    );
    await setOwner(restoreOwnerThreadId, `${marker} owner restored`);
    return { text, final };
  };
  const configView = await rpc.call("matrixBridge.configure", {
    homeserverUrl: ctx.homeserver,
    accessToken: ctx.bot.accessToken,
    allowedUserIds: [ownerMxid],
  });
  log(`configured homeserver=${configView.homeserverUrl} room=${configView.roomId}`);

  heartbeat("4", "join the bridge room");
  const invite = await ownerCli.request({ op: "waitInvite", timeoutMs: 20_000 }, 21_000);
  const roomId = invite.roomId;
  await ownerCli.request({ op: "join", roomId });
  const roomState = await ownerCli.request({ op: "roomState", roomId });
  assert(roomState.joinRules.join_rule === "invite", "join rule is not invite");
  assert(roomState.encryption.algorithm === MEGOLM, "room is not Megolm encrypted");
  const members = await ownerCli.request({ op: "members", roomId });
  assert(
    JSON.stringify(members.members) === JSON.stringify([botMxid, ownerMxid].toSorted()),
    `unexpected membership ${JSON.stringify(members.members)}`,
  );

  heartbeat("5", "reject an invalid pairing code");
  await waitStatus(
    () => status,
    (v) => v.state === "awaiting-pairing",
    15_000,
    "awaiting-pairing",
  );
  const prompt = await ownerCli.request(
    { op: "waitText", roomId, from: botMxid, body: PAIRING_PROMPT, timeoutMs: 10_000 },
    11_000,
  );
  assert(prompt.event.encrypted === true, "the pairing prompt was not encrypted on the wire");
  await ownerCli.request({ op: "send", roomId, body: "definitely-invalid" });
  await ownerCli.request(
    {
      op: "waitText",
      roomId,
      from: botMxid,
      body: PAIRING_REJECT,
      afterTs: prompt.event.ts,
      timeoutMs: 8_000,
    },
    9_000,
  );
  assert(status.state === "awaiting-pairing", `status after a rejected code: ${status.state}`);

  heartbeat("6", "accept a real pairing code");
  const clientsBefore = await t3Fetch(t3.origin, t3.token, "/api/auth/clients");
  const pairing = await t3Fetch(t3.origin, t3.token, "/api/auth/pairing-token", {
    method: "POST",
    body: JSON.stringify({
      audienceCeiling: "private",
      label: "Matrix bridge",
      scopes: ["orchestration:read"],
    }),
  });
  rememberSecret(pairing.credential);
  const afterReject = Date.now();
  await ownerCli.request({ op: "send", roomId, body: pairing.credential });
  await ownerCli.request(
    {
      op: "waitText",
      roomId,
      from: botMxid,
      body: PAIRING_OK,
      afterTs: afterReject,
      timeoutMs: 8_000,
    },
    9_000,
  );
  await waitStatus(
    () => status,
    (v) => v.state === "active",
    10_000,
    "active after pairing",
  );
  const pairingLinks = await t3Fetch(t3.origin, t3.token, "/api/auth/pairing-links");
  assert(!pairingLinks.some((link) => link.id === pairing.id), "the pairing code was not consumed");
  const replay = await fetch(`${t3.origin}/oauth/token`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      subject_token: pairing.credential,
      subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
      requested_token_type: "urn:t3:params:oauth:token-type:access_token",
      scope: "orchestration:read",
      audience_ceiling: "private",
    }),
  });
  assert(replay.status !== 200, "a consumed pairing code still exchanged for a session");
  const clientsAfter = await t3Fetch(t3.origin, t3.token, "/api/auth/clients");
  assert(clientsAfter.length === clientsBefore.length, "pairing created an access-token session");

  heartbeat("7", "threads and owner");
  const modelSelection = pickProvider(ctx.serverConfig);
  const projectId = NodeCrypto.randomUUID();
  await dispatch(t3, {
    type: "project.create",
    commandId: NodeCrypto.randomUUID(),
    projectId,
    title: "matrix-e2e",
    workspaceRoot: ctx.workspace,
    defaultModelSelection: modelSelection,
    createdAt: nowIso(),
  });
  const threadA = await createThread(t3, projectId, "thread-A", modelSelection);
  const threadB = await createThread(t3, projectId, "thread-B", modelSelection);
  await rpc.call("matrixBridge.setOwner", { ownerThreadId: threadA });
  await waitStatus(
    () => status,
    (v) => v.ownerThreadId === threadA,
    5_000,
    "owner is thread A",
  );

  heartbeat("8", "final output only");
  const botCount = async () =>
    (await ownerCli.request({ op: "messages", roomId, from: botMxid })).texts.length;
  const botBefore = await botCount();
  const streamMarker = turnMarker((await threadSnapshot(t3, threadA)).thread);
  await startTurn(
    t3,
    threadA,
    "Run the command `echo MID_TURN_MARKER` then reply with the exact word FINAL_STREAM_A and nothing else.",
    modelSelection,
  );
  const streamDeadline = Date.now() + TURN_MS;
  const terminalWithFinal = (thread) =>
    isNewTurn(thread, streamMarker) &&
    thread.latestTurn.state === "completed" &&
    lastAssistant(thread, thread.latestTurn.turnId);
  let terminalA = null;
  while (Date.now() < streamDeadline) {
    const { thread } = await threadSnapshot(t3, threadA);
    if (terminalWithFinal(thread)) {
      terminalA = thread;
      break;
    }
    if ((await botCount()) !== botBefore) {
      // The turn can go terminal in the gap between those two reads, so a new
      // bot message is only an early post if the turn is still not terminal
      // once re-read. Otherwise this is the normal path, one poll late.
      const { thread: recheck } = await threadSnapshot(t3, threadA);
      assert(terminalWithFinal(recheck), "the bot posted before the T3 turn was terminal");
      terminalA = recheck;
      break;
    }
    await sleep(100);
  }
  assert(terminalA, "the thread A streaming turn timed out");
  assert(
    terminalA.activities.some(
      (activity) => activity.turnId === terminalA.latestTurn.turnId && activity.tone === "tool",
    ),
    "the turn produced no tool activity, so final-only suppression was never exercised",
  );
  const finalA = lastAssistant(terminalA, terminalA.latestTurn.turnId).text.trim();
  const outboundA = await ownerCli.request(
    { op: "waitText", roomId, from: botMxid, body: finalA, timeoutMs: LOCAL_SEND_MS },
    LOCAL_SEND_MS + 1_000,
  );
  assert(outboundA.event.encrypted === true, "the assistant final was not encrypted on the wire");
  assert((await botCount()) === botBefore + 1, "expected exactly one bot message for the turn");

  heartbeat("9", "the bot's own event starts no turn");
  // The bot's final is a room event it must ignore, and the witness for that
  // must not touch thread A. Sending the next owner message here would let a
  // wrongly created echo turn absorb it as a steer, leaving exactly the turn
  // count a correct bridge produces. Bridging thread B instead makes the bot
  // dispatch somewhere else, which by room ordering proves it already consumed
  // its own event, and thread A has to be untouched at that point.
  const echoMarker = turnMarker(terminalA);
  await bridgeWitness(threadB, "ECHO", threadA);
  const afterEcho = (await threadSnapshot(t3, threadA)).thread;
  assert(
    afterEcho.turns.length === echoMarker.turns &&
      afterEcho.latestTurn?.turnId === echoMarker.turnId,
    "the bot's own Matrix event created a turn on the owner thread",
  );

  heartbeat("10", "inbound message on an idle thread");
  const inboundIdle = `INBOUND_IDLE_${NodeCrypto.randomBytes(4).toString("hex")}`;
  const idleMarker = turnMarker(afterEcho);
  await ownerCli.request({ op: "send", roomId, body: inboundIdle });
  const idleDispatched = await waitThread(
    t3,
    threadA,
    (thread) =>
      isNewTurn(thread, idleMarker) && userMessages(thread).some((m) => m.text === inboundIdle),
    INBOUND_MS,
    "inbound idle dispatch",
  );
  const idleTurnId = idleDispatched.latestTurn.turnId;
  const idleTerminal = await waitThread(
    t3,
    threadA,
    (thread) =>
      thread.latestTurn?.turnId === idleTurnId &&
      thread.latestTurn.state === "completed" &&
      lastAssistant(thread, idleTurnId),
    TURN_MS,
    "inbound idle final",
  );
  const idleFinal = lastAssistant(idleTerminal, idleTurnId).text.trim();
  await ownerCli.request(
    { op: "waitText", roomId, from: botMxid, body: idleFinal, timeoutMs: LOCAL_SEND_MS },
    LOCAL_SEND_MS + 1_000,
  );

  heartbeat("11", "inbound message steers a running turn");
  const steerMarker = turnMarker(idleTerminal);
  await ownerCli.request({
    op: "send",
    roomId,
    body: "Run the command `sleep 20` then reply with the exact word SLOW_UNSTEERED and nothing else.",
  });
  const running = await waitThread(
    t3,
    threadA,
    (thread) => isNewTurn(thread, steerMarker) && thread.latestTurn.state === "running",
    INBOUND_MS,
    "slow inbound turn running",
  );
  const runningTurnId = running.latestTurn.turnId;
  const userMessagesBeforeSteer = userMessages(running).length;
  await ownerCli.request({
    op: "send",
    roomId,
    body: "Stop waiting. Reply with the exact word STEERED_FINAL and nothing else.",
  });
  // Both messages land on the thread while the turn count stays put: that is
  // what makes this a steer rather than a second turn.
  const steered = await waitThread(
    t3,
    threadA,
    (thread) =>
      thread.turns.length === steerMarker.turns + 1 &&
      userMessages(thread).length === userMessagesBeforeSteer + 1 &&
      thread.latestTurn?.turnId === runningTurnId &&
      thread.latestTurn?.state === "completed",
    TURN_MS,
    "steered turn complete",
  );
  assert(steered.latestTurn.turnId === runningTurnId, "the steer created a second turn");
  const steeredFinal = lastAssistant(steered, runningTurnId).text.trim();
  assert(
    steeredFinal.includes("STEERED_FINAL") && !steeredFinal.includes("SLOW_UNSTEERED"),
    "the steer did not change the assistant result",
  );
  await ownerCli.request(
    { op: "waitText", roomId, from: botMxid, body: steeredFinal, timeoutMs: LOCAL_SEND_MS },
    LOCAL_SEND_MS + 1_000,
  );

  heartbeat("12", "owner move drops the old final");
  const moveMarker = turnMarker(steered);
  await startTurn(
    t3,
    threadA,
    "Run the command `sleep 15` then reply with the exact word OWNER_A_FINAL and nothing else.",
    modelSelection,
  );
  const moveRunning = await waitThread(
    t3,
    threadA,
    (thread) => isNewTurn(thread, moveMarker) && thread.latestTurn.state === "running",
    20_000,
    "owner-move turn running",
  );
  const movedTurnId = moveRunning.latestTurn.turnId;
  await rpc.call("matrixBridge.setOwner", { ownerThreadId: threadB });
  // Completed with a real final, not merely settled: an errored or interrupted
  // turn produces nothing to suppress, which would make the drop assertion
  // below true for the wrong reason.
  const finishedA = await waitThread(
    t3,
    threadA,
    (thread) =>
      thread.latestTurn?.turnId === movedTurnId &&
      thread.latestTurn.state === "completed" &&
      lastAssistant(thread, movedTurnId),
    TURN_MS,
    "owner-move thread A finished",
  );
  const droppedText = lastAssistant(finishedA, movedTurnId).text.trim();
  assert(
    droppedText.length > 0,
    "the moved-away turn produced no final, so the drop was never exercised",
  );
  // The new owner's turn is the witness for the dropped one. Its final is
  // queued after thread A's, so once it has arrived the outbound worker has
  // drained past the abandoned job; a fixed sleep would instead declare the
  // drop while a delayed or retrying send was still in flight.
  const ownerBMarker = turnMarker((await threadSnapshot(t3, threadB)).thread);
  await startTurn(
    t3,
    threadB,
    "Reply with the exact word OWNER_B_FINAL and nothing else.",
    modelSelection,
  );
  const doneB = await waitThread(
    t3,
    threadB,
    (thread) =>
      isNewTurn(thread, ownerBMarker) &&
      thread.latestTurn.state === "completed" &&
      lastAssistant(thread, thread.latestTurn.turnId),
    TURN_MS,
    "new owner final",
  );
  const finalB = lastAssistant(doneB, doneB.latestTurn.turnId).text.trim();
  await ownerCli.request(
    { op: "waitText", roomId, from: botMxid, body: finalB, timeoutMs: LOCAL_SEND_MS },
    LOCAL_SEND_MS + 1_000,
  );
  const afterDrop = await ownerCli.request({ op: "messages", roomId, from: botMxid });
  assert(
    !afterDrop.texts.some((item) => item.body === droppedText),
    "the old owner's final was posted after the move",
  );

  heartbeat("13", "homeserver outage and recovery");
  await stopPid(ctx.conduwuit.record);
  const queuedMarker = turnMarker(doneB);
  await startTurn(
    t3,
    threadB,
    "Reply with the exact word QUEUED_FINAL and nothing else.",
    modelSelection,
  );
  const queuedThread = await waitThread(
    t3,
    threadB,
    (thread) =>
      isNewTurn(thread, queuedMarker) &&
      thread.latestTurn.state === "completed" &&
      lastAssistant(thread, thread.latestTurn.turnId),
    TURN_MS,
    "queued turn terminal",
  );
  const queuedFinal = lastAssistant(queuedThread, queuedThread.latestTurn.turnId).text.trim();
  // Observational, not an assertion: the design only owes `degraded` on retry
  // expiry, unexpected membership, queue overflow, or adapter failure, and a
  // short outage inside the retry window need trigger none of them. What is
  // owed is that the lifecycle does not get stuck, which the post-recovery
  // check below enforces.
  const degradedDeadline = Date.now() + 20_000;
  while (Date.now() < degradedDeadline && status.state !== "degraded") await sleep(100);
  log(`outage status=${status.state} reason=${status.reason ?? "none"}`);
  await ownerCli.close();
  ctx.conduwuit = await startConduwuit(
    ctx.conduwuitBin,
    ctx.configPath,
    ctx.matrixPort,
    ctx.conduwuitDir,
  );
  const recoveredCli = new MatrixCli("owner-2");
  await recoveredCli.request(
    {
      op: "start",
      homeserver: ctx.homeserver,
      store: ctx.ownerStore,
      accessToken: ctx.owner.accessToken,
    },
    60_000,
  );
  await recoveredCli.request(
    { op: "waitText", roomId, from: botMxid, body: queuedFinal, timeoutMs: RECOVERY_MS },
    RECOVERY_MS + 1_000,
  );
  const recovered = await recoveredCli.request({ op: "messages", roomId, from: botMxid });
  assert(
    recovered.texts.filter((item) => item.body === queuedFinal).length === 1,
    "the queued final was not delivered exactly once",
  );
  // Delivering the backlog is not enough: a client reading the status stream
  // must see the bridge come back, not sit on a stale outage lifecycle.
  await waitStatus(
    () => status,
    (v) => v.state === "active" && v.ownerThreadId === threadB,
    30_000,
    "status returns to active after recovery",
  );

  activeCli = recoveredCli;

  /**
   * Inbound half of silence, run while `silentThreadId` is idle. Idleness is
   * load-bearing: starting work on the thread first would let a wrongly-live
   * inbound path have its dispatch rejected as busy, which leaves exactly the
   * same evidence as the intended drop.
   */
  const assertInboundSilent = async (silentThreadId, marker, label) => {
    const probe = `${marker}_INBOUND_${NodeCrypto.randomBytes(4).toString("hex")}`;
    const before = (await threadSnapshot(t3, silentThreadId)).thread;
    await activeCli.request({ op: "send", roomId, body: probe });
    await bridgeWitness(threadA, `${marker}_IN`, null);
    const after = (await threadSnapshot(t3, silentThreadId)).thread;
    assert(
      !userMessages(after).some((message) => message.text === probe),
      `a Matrix message reached the ${label} thread`,
    );
    assert(
      after.turns.length === before.turns.length,
      `a Matrix message started a T3 turn on the ${label} thread`,
    );
    // The probe preceded the witness bridging, so a bot that deferred it
    // across the ownership change would land it on the control thread instead
    // of dropping it. That is a failure here rather than an invisible pass.
    const control = (await threadSnapshot(t3, threadA)).thread;
    assert(
      !userMessages(control).some((message) => message.text === probe),
      `a ${label} Matrix message was dispatched once ownership was restored`,
    );
  };

  /**
   * Outbound half of silence. A live thread has to produce a real final for
   * the suppression to mean anything, so a rejected dispatch there is a gate
   * failure; only an archived thread may refuse the turn outright.
   */
  const assertOutboundSilent = async (silentThreadId, marker, label, prompt, { requireFinal }) => {
    const before = (await threadSnapshot(t3, silentThreadId)).thread;
    const probeMarker = turnMarker(before);
    const botBefore = (await activeCli.request({ op: "messages", roomId, from: botMxid })).texts
      .length;
    const dispatched = await startTurn(t3, silentThreadId, prompt, modelSelection).then(
      () => true,
      (error) => {
        if (requireFinal) throw error;
        return false;
      },
    );
    let final = "";
    if (dispatched) {
      const settled = await waitNewTurnSettled(
        t3,
        silentThreadId,
        probeMarker,
        `${label} probe turn terminal`,
      );
      final = lastAssistant(settled, settled.latestTurn.turnId)?.text.trim() ?? "";
      if (requireFinal) {
        assert(
          settled.latestTurn.state === "completed",
          `the ${label} probe turn ended as ${settled.latestTurn.state}, so outbound suppression was never exercised`,
        );
        assert(final.length > 0, `the ${label} probe turn produced no assistant final to suppress`);
      }
    }
    const witness = await bridgeWitness(threadA, `${marker}_OUT`, null);
    const texts = (await activeCli.request({ op: "messages", roomId, from: botMxid })).texts;
    assert(
      final.length === 0 || !texts.some((item) => item.body === final),
      `the ${label} thread still posted a Matrix message`,
    );
    // Exactly one new bot message since the probe: the witness final.
    assert(
      texts.length === botBefore + 1 && texts.at(-1).body === witness.final,
      `the bot posted something other than the witness while ${label}`,
    );
  };

  heartbeat("14", "unbridge is silent in both directions");
  await setOwner(null, "owner cleared");
  await assertInboundSilent(threadB, "UNBRIDGED", "unbridged");
  await assertOutboundSilent(
    threadB,
    "UNBRIDGED",
    "unbridged",
    "Reply with the exact word SILENCE_SHOULD_NOT_ARRIVE and nothing else.",
    { requireFinal: true },
  );

  // Ownership has to be observed as thread B before the archive, or the null
  // below is satisfied by the stale null the unbridge above already published
  // and the archive proves nothing.
  await setOwner(threadB, "owner is thread B again");
  await dispatch(t3, {
    type: "thread.archive",
    commandId: NodeCrypto.randomUUID(),
    threadId: threadB,
  });
  await waitStatus(
    () => status,
    (v) => v.ownerThreadId === null,
    10_000,
    "archive unset the owner",
  );
  await assertInboundSilent(threadB, "ARCHIVED", "archived");
  await assertOutboundSilent(
    threadB,
    "ARCHIVED",
    "archived",
    "Reply with the exact word ARCHIVED_SHOULD_NOT_BRIDGE and nothing else.",
    // An archived thread is allowed to refuse the turn outright.
    { requireFinal: false },
  );

  heartbeat("15", "close clients");
  statusSub.interrupt();
  // A subscription that never acknowledges the interrupt must not hold the run
  // open; the socket close below ends it either way.
  await Promise.race([statusSub.ended.catch(() => {}), sleep(5_000)]);
  rpc.close();
  await recoveredCli.close();
  log("matrix bridge release gate passed");
}

function parseMode(argv) {
  if (argv.length === 0) return "release-gate";
  if (argv.length === 1 && argv[0] === "--smoke") return "smoke";
  throw new Error(`usage: node e2e/matrix-bridge.mjs [--smoke] (got ${argv.join(" ")})`);
}

async function main() {
  const mode = parseMode(process.argv.slice(2));
  heartbeat("start", mode);
  assertSafePath(REPO_ROOT);
  assertSafePath(USER_CACHE);
  const ctx = await bringUp(mode);
  if (mode === "smoke") await runSmoke(ctx);
  else await runReleaseGate(ctx);
}

async function runTeardown() {
  heartbeat("teardown", failed ? "after failure" : "after success");
  for (const record of pids.toReversed()) {
    try {
      await stopPid(record);
    } catch (error) {
      console.error(redact(error instanceof Error ? error.message : String(error)));
    }
  }
  const survivors = pids.filter((record) => !treeIsGone(record));
  if (survivors.length > 0) {
    // A run that leaks a homeserver, a T3 server, or a Matrix client is not a
    // pass whatever the assertions said: the next run inherits its ports, and
    // the temp root deleted below is still that process's backing state.
    failed = true;
    const leaked = survivors.map((record) => `${record.kind}:${record.pid}`).join(", ");
    console.error(`teardown failed, these processes are still running: ${leaked}`);
  }
  if (!tempRoot) return;
  if (failed) {
    try {
      const collected = [];
      for (const name of NodeFS.readdirSync(tempRoot)) {
        if (!name.endsWith(".log")) continue;
        const target = NodePath.join(tempRoot, name);
        if (!NodeFS.statSync(target).isFile()) continue;
        collected.push(`--- ${name} ---\n${redact(NodeFS.readFileSync(target, "utf8"))}`);
      }
      writeOwnedFile(FAILURE_LOG_PATH, collected.join("\n"));
      log(`retained redacted logs at ${FAILURE_LOG_PATH}`);
    } catch (error) {
      console.error(redact(error instanceof Error ? error.message : String(error)));
    }
  }
  // The temp root holds Matrix access tokens, a crypto store, and a T3 home:
  // it goes on every exit path, failure included.
  NodeFS.rmSync(tempRoot, { recursive: true, force: true });
  tempRoot = null;
}

// Memoised, so a signal, a rejection, and the normal exit path all await the
// same sweep instead of racing process.exit against a half-killed tree.
function teardown() {
  teardownPromise ??= runTeardown().catch((error) => {
    // A sweep that threw may have left the temp root, and with it the Matrix
    // access tokens, the crypto store, and the T3 bearer, on disk. That is a
    // failed run no matter what the assertions concluded.
    failed = true;
    console.error(redact(error instanceof Error ? (error.stack ?? error.message) : String(error)));
  });
  return teardownPromise;
}

function report(error) {
  console.error(redact(error instanceof Error ? (error.stack ?? error.message) : String(error)));
}

for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(signal, () => {
    failed = true;
    teardown().finally(() => process.exit(signal === "SIGINT" ? 130 : 143));
  });
}
process.on("unhandledRejection", (error) => {
  failed = true;
  report(error);
  teardown().finally(() => process.exit(1));
});
process.on("uncaughtException", (error) => {
  failed = true;
  report(error);
  teardown().finally(() => process.exit(1));
});

main()
  .catch((error) => {
    failed = true;
    report(error);
  })
  .finally(async () => {
    await teardown();
    process.exit(failed ? 1 : 0);
  });
