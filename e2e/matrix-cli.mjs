#!/usr/bin/env node
// Tiny encrypted Matrix client for the disposable E2E harness.
// JSON-lines on stdin/stdout: one request object per line in, one reply object
// with the same `id` per line out. Access tokens are returned once on
// register/start and must be redacted by the orchestrator. SDK logs go to
// stderr only, so stdout stays a clean protocol stream.

import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

import { sdkCacheDir } from "./matrix-sdk-pin.mjs";

// StoreType.Sqlite from @matrix-org/matrix-sdk-crypto-nodejs. That enum is a
// TypeScript `const enum`, so it is erased at runtime and cannot be imported.
const CRYPTO_STORE_SQLITE = 0;
const MEGOLM = "m.megolm.v1.aes-sha2";

function loadSdk() {
  const here = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
  // Only the tree for the current pin. Earlier pins can still be sitting in the
  // cache, and silently loading one would defeat the pin.
  const roots = [
    process.env.MATRIX_E2E_SDK_DIR,
    sdkCacheDir(),
    NodePath.join(here, "..", "apps", "server"),
  ].filter(Boolean);
  const errors = [];
  for (const root of roots) {
    const pkg = NodePath.join(root, "node_modules", "matrix-bot-sdk", "package.json");
    if (!NodeFS.existsSync(pkg)) continue;
    try {
      return NodeModule.createRequire(pkg)("matrix-bot-sdk");
    } catch (error) {
      errors.push(`${pkg}: ${error instanceof Error ? error.message : error}`);
    }
  }
  throw new Error(
    `matrix-bot-sdk is not available. Run e2e/matrix-bridge.mjs, which installs it under ~/.cache/t3-matrix-e2e. ${errors.join("; ")}`,
  );
}

const sdk = loadSdk();
const {
  LogLevel,
  LogService,
  MatrixClient,
  RustSdkCryptoStorageProvider,
  SimpleFsStorageProvider,
} = sdk;

LogService.setLevel(LogLevel.WARN);
LogService.setLogger({
  trace() {},
  debug() {},
  info() {},
  warn(...args) {
    console.error(...args);
  },
  error(...args) {
    console.error(...args);
  },
});

function writeLine(payload, onFlush) {
  process.stdout.write(`${JSON.stringify(payload)}\n`, onFlush);
}

function fail(id, error) {
  writeLine({ id, ok: false, error: error instanceof Error ? error.message : String(error) });
}

/**
 * Drive the interactive-auth registration endpoint directly. The SDK's
 * MatrixAuth helper only knows the dummy flow, and the disposable homeserver
 * is token-gated so open registration cannot be abused mid-run.
 */
async function registerAccount(homeserver, username, password, registrationToken) {
  const url = `${homeserver.replace(/\/$/, "")}/_matrix/client/v3/register`;
  let session;
  let auth = registrationToken
    ? { type: "m.login.registration_token", token: registrationToken }
    : { type: "m.login.dummy" };
  for (let attempt = 0; attempt < 6; attempt += 1) {
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        username,
        password,
        initial_device_display_name: `t3-matrix-e2e-${username}`,
        auth: session ? { ...auth, session } : auth,
      }),
    });
    const json = await response.json().catch(() => ({}));
    if (response.ok && json.access_token) {
      return { userId: json.user_id, deviceId: json.device_id, accessToken: json.access_token };
    }
    if (response.status !== 401 || typeof json !== "object" || json === null) {
      throw new Error(`register failed: HTTP ${response.status} ${JSON.stringify(json)}`);
    }
    session = json.session;
    const completed = new Set(json.completed ?? []);
    const stages = new Set((json.flows ?? []).flatMap((flow) => flow.stages ?? []));
    if (registrationToken && stages.has("m.login.registration_token")) {
      auth = { type: "m.login.registration_token", token: registrationToken };
    } else if (stages.has("m.login.dummy")) {
      auth = { type: "m.login.dummy" };
    } else {
      throw new Error(`register UIA offered no usable flow: ${JSON.stringify(json.flows)}`);
    }
    // A token stage that already completed must not be replayed: the next
    // round has to advance the flow with the dummy stage instead.
    if (completed.has(auth.type) && stages.has("m.login.dummy")) {
      auth = { type: "m.login.dummy" };
    }
  }
  throw new Error("register exceeded interactive-auth attempts");
}

class Session {
  constructor() {
    this.homeserver = null;
    this.storeDir = null;
    this.client = null;
    this.userId = null;
    this.deviceId = null;
    this.accessToken = null;
    this.syncCycles = 0;
    this.texts = [];
    this.invites = [];
    this.waiters = [];
  }

  emit(event) {
    writeLine({ ok: true, op: "event", ...event });
    for (const waiter of this.waiters.slice()) {
      if (waiter.match(event)) {
        this.waiters = this.waiters.filter((item) => item !== waiter);
        waiter.resolve(event);
      }
    }
  }

  waitFor(match, timeoutMs) {
    const existing = [...this.texts, ...this.invites].find(match);
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const waiter = { match, resolve, reject };
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter((item) => item !== waiter);
        reject(new Error(`wait timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      waiter.resolve = (event) => {
        clearTimeout(timer);
        resolve(event);
      };
      this.waiters.push(waiter);
    });
  }

  requireClient() {
    if (!this.client) throw new Error("start the client first");
    return this.client;
  }

  async start() {
    if (!this.homeserver || !this.storeDir || !this.accessToken) {
      throw new Error("configure/register before start");
    }
    NodeFS.mkdirSync(this.storeDir, { recursive: true, mode: 0o700 });
    const storage = new SimpleFsStorageProvider(NodePath.join(this.storeDir, "sync.json"));
    const crypto = new RustSdkCryptoStorageProvider(
      NodePath.join(this.storeDir, "crypto"),
      CRYPTO_STORE_SQLITE,
    );
    const client = new MatrixClient(this.homeserver, this.accessToken, storage, crypto);
    client.syncingTimeout = 3_000;
    if (typeof client.processSync !== "function") {
      throw new Error(
        "matrix-bot-sdk no longer exposes processSync; the sync counter needs a port",
      );
    }
    // Counting completed /sync round trips is how the orchestrator proves a
    // negative ("nothing arrived") without sleeping on a wall clock.
    const processSync = client.processSync.bind(client);
    client.processSync = async (raw, emitFn) => {
      const result = await processSync(raw, emitFn);
      this.syncCycles += 1;
      return result;
    };
    client.on("room.invite", (roomId) => {
      const event = { kind: "invite", roomId };
      this.invites.push(event);
      this.emit(event);
    });
    const decryptedIds = new Set();
    client.on("room.decrypted_event", (_roomId, event) => {
      if (event?.event_id) decryptedIds.add(event.event_id);
    });
    client.on("room.message", (roomId, event) => {
      const body = event?.content?.body;
      if (typeof body !== "string" || event?.content?.msgtype !== "m.text") return;
      const text = {
        kind: "text",
        roomId,
        sender: event.sender,
        body,
        eventId: event.event_id,
        ts: event.origin_server_ts ?? Date.now(),
        // The SDK emits room.decrypted_event before it re-emits the plaintext
        // as room.message, so this flag proves the event arrived encrypted.
        encrypted: decryptedIds.has(event.event_id),
      };
      this.texts.push(text);
      this.emit(text);
    });
    client.on("room.failed_decryption", (roomId, event, error) => {
      this.emit({
        kind: "decrypt-error",
        roomId,
        eventId: event?.event_id,
        error: error instanceof Error ? error.message : String(error),
      });
    });
    this.client = client;
    await client.start();
    this.userId = await client.getUserId();
  }

  stop() {
    if (!this.client) return;
    this.client.stop();
    this.client = null;
  }
}

const session = new Session();

async function handle(command) {
  const { id, op } = command;
  switch (op) {
    case "configure": {
      session.homeserver = String(command.homeserver).replace(/\/$/, "");
      session.storeDir = String(command.store);
      writeLine({ id, ok: true, op });
      return;
    }
    case "register": {
      const homeserver = String(session.homeserver ?? command.homeserver).replace(/\/$/, "");
      const registered = await registerAccount(
        homeserver,
        command.username,
        command.password,
        command.registrationToken,
      );
      session.homeserver = homeserver;
      session.userId = registered.userId;
      session.deviceId = registered.deviceId;
      session.accessToken = registered.accessToken;
      if (command.store) session.storeDir = String(command.store);
      writeLine({ id, ok: true, op, ...registered });
      return;
    }
    case "start": {
      if (command.accessToken) session.accessToken = command.accessToken;
      if (command.homeserver) session.homeserver = String(command.homeserver).replace(/\/$/, "");
      if (command.store) session.storeDir = String(command.store);
      await session.start();
      writeLine({ id, ok: true, op, userId: session.userId, deviceId: session.deviceId });
      return;
    }
    case "whoami": {
      const whoami = await session.requireClient().getWhoAmI();
      writeLine({ id, ok: true, op, ...whoami });
      return;
    }
    case "createRoom": {
      // Same shape the bridge itself creates: private, invite-only, Megolm,
      // no alias, no directory listing.
      const roomId = await session.requireClient().createRoom({
        preset: "private_chat",
        visibility: "private",
        is_direct: true,
        invite: command.invite ?? [],
        ...(command.name ? { name: command.name } : {}),
        initial_state: [
          { type: "m.room.encryption", state_key: "", content: { algorithm: MEGOLM } },
        ],
      });
      writeLine({ id, ok: true, op, roomId });
      return;
    }
    case "waitInvite": {
      const event = await session.waitFor(
        (item) => item.kind === "invite" && (!command.roomId || item.roomId === command.roomId),
        command.timeoutMs ?? 15_000,
      );
      writeLine({ id, ok: true, op, roomId: event.roomId });
      return;
    }
    case "join": {
      const roomId = await session.requireClient().joinRoom(command.roomId);
      writeLine({ id, ok: true, op, roomId });
      return;
    }
    case "roomState": {
      const client = session.requireClient();
      const joinRules = await client.getRoomStateEvent(command.roomId, "m.room.join_rules", "");
      const encryption = await client.getRoomStateEvent(command.roomId, "m.room.encryption", "");
      writeLine({ id, ok: true, op, joinRules, encryption });
      return;
    }
    case "members": {
      const members = await session.requireClient().getJoinedRoomMembers(command.roomId);
      writeLine({ id, ok: true, op, members: [...members].toSorted() });
      return;
    }
    case "send": {
      const eventId = await session
        .requireClient()
        .sendMessage(command.roomId, { msgtype: "m.text", body: command.body });
      writeLine({ id, ok: true, op, eventId });
      return;
    }
    case "waitText": {
      const afterTs = command.afterTs ?? 0;
      const event = await session.waitFor((item) => {
        if (item.kind !== "text") return false;
        if (command.roomId && item.roomId !== command.roomId) return false;
        if (command.from && item.sender !== command.from) return false;
        if (command.body !== undefined && item.body !== command.body) return false;
        return (item.ts ?? 0) > afterTs;
      }, command.timeoutMs ?? 5_000);
      writeLine({ id, ok: true, op, event });
      return;
    }
    case "messages": {
      const texts = session.texts.filter(
        (item) =>
          (!command.roomId || item.roomId === command.roomId) &&
          (!command.from || item.sender === command.from),
      );
      writeLine({ id, ok: true, op, texts });
      return;
    }
    case "syncCycles": {
      writeLine({ id, ok: true, op, cycles: session.syncCycles });
      return;
    }
    case "waitSyncCycles": {
      const target = session.syncCycles + (command.count ?? 2);
      const timeoutMs = command.timeoutMs ?? 20_000;
      const started = Date.now();
      while (session.syncCycles < target) {
        if (Date.now() - started > timeoutMs) {
          throw new Error(`waitSyncCycles timed out at ${session.syncCycles}, wanted ${target}`);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      writeLine({ id, ok: true, op, cycles: session.syncCycles });
      return;
    }
    case "stop": {
      session.stop();
      // Exit only once the reply has actually left the pipe: stdout to a pipe
      // is asynchronous, so exiting first truncates the orchestrator's read.
      writeLine({ id, ok: true, op }, () => process.exit(0));
      return;
    }
    default:
      throw new Error(`unknown op ${op}`);
  }
}

const rl = NodeReadline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  if (!line.trim()) return;
  let command;
  try {
    command = JSON.parse(line);
  } catch (error) {
    fail(undefined, error);
    return;
  }
  handle(command).catch((error) => fail(command.id, error));
});
rl.on("close", () => {
  session.stop();
  process.exit(0);
});
