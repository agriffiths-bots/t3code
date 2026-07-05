// @effect-diagnostics nodeBuiltinImport:off - Tests exercise manifest file rewrites directly.
// @effect-diagnostics globalDate:off - Tests pin Date instances to deterministic ISO manifest values.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, assert, describe, it } from "@effect/vitest";

import {
  RESUME_MESSAGE,
  injectResume,
  optionsFromCliArgs,
  parseArgs,
  parseManifest,
} from "./inject-resume.ts";

const tempDirs: Array<string> = [];

afterEach(async () => {
  await Promise.all(
    tempDirs.splice(0).map((dir) => NodeFSP.rm(dir, { recursive: true, force: true })),
  );
});

async function writeManifest(threads: Array<Record<string, unknown>>): Promise<string> {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "inject-resume-test-"));
  tempDirs.push(dir);
  const path = NodePath.join(dir, "resume-manifest.json");
  await NodeFSP.writeFile(
    path,
    `${JSON.stringify({ version: 1, captured_at: "2026-07-03T12:00:00.000Z", threads }, null, 2)}\n`,
  );
  return path;
}

async function readManifest(path: string): Promise<any> {
  return JSON.parse(await NodeFSP.readFile(path, "utf8")) as any;
}

describe("inject-resume", () => {
  it("keeps parsed attachments dir in CLI options", () => {
    const args = parseArgs(
      [
        "--manifest",
        "/tmp/resume-manifest.json",
        "--origin",
        "http://127.0.0.1:3773",
        "--token",
        "test-token",
        "--attachments-dir",
        "/tmp/t3/attachments",
      ],
      {},
    );

    assert.deepEqual(optionsFromCliArgs(args), {
      manifestPath: "/tmp/resume-manifest.json",
      origin: "http://127.0.0.1:3773",
      token: "test-token",
      attachmentsDir: "/tmp/t3/attachments",
      dryRun: false,
    });
  });

  it("validates the manifest shape", () => {
    assert.throws(() =>
      parseManifest(JSON.stringify({ version: 2, captured_at: "x", threads: [] })),
    );
    assert.throws(() =>
      parseManifest(
        JSON.stringify({
          version: 1,
          captured_at: "x",
          threads: [{ thread_id: "thread-1", role: "paused", injected_at: null }],
        }),
      ),
    );
    assert.throws(() =>
      parseManifest(
        JSON.stringify({
          version: 1,
          captured_at: "x",
          threads: [
            {
              thread_id: "thread-1",
              role: "active",
              runtime_mode: "read-only",
              injected_at: null,
            },
          ],
        }),
      ),
    );
    assert.throws(() =>
      parseManifest(
        JSON.stringify({
          version: 1,
          captured_at: "x",
          threads: [
            {
              thread_id: "thread-1",
              role: "active",
              interaction_mode: "ask",
              injected_at: null,
            },
          ],
        }),
      ),
    );
    assert.throws(() =>
      parseManifest(
        JSON.stringify({
          version: 1,
          captured_at: "x",
          threads: [
            {
              thread_id: "thread-1",
              role: "active",
              pending_message: {
                message_id: "message-1",
                role: "user",
                text: "hello",
                attachments: {},
              },
              injected_at: null,
            },
          ],
        }),
      ),
    );
  });

  it("injects active null entries once and skips waiting/already-injected entries", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-ok", role: "active", status: "running", injected_at: null },
      { thread_id: "waiting", role: "waiting", status: "waiting", injected_at: null },
      {
        thread_id: "already",
        role: "active",
        status: "running",
        injected_at: "2026-07-03T12:30:00.000Z",
      },
    ]);
    const requests: Array<any> = [];
    const options = {
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        requests.push(body);
        return new Response("{}", { status: 200 });
      },
    };

    const result = await injectResume(options);
    assert.deepEqual(result, { injected: 1, skipped: 2, failed: 0, failures: [] });
    assert.equal(requests[0]?.type, "thread.interaction-mode.set");
    assert.equal(requests[0]?.threadId, "active-ok");
    assert.equal(requests[0]?.interactionMode, "default");
    assert.equal(requests[1]?.type, "thread.turn.start");
    assert.equal(requests[1]?.threadId, "active-ok");
    assert.equal(requests[1]?.message.role, "user");
    assert.equal(requests[1]?.message.text, RESUME_MESSAGE);
    assert.deepEqual(requests[1]?.message.attachments, []);
    assert.equal(requests[1]?.runtimeMode, "full-access");
    assert.equal(requests[1]?.interactionMode, "default");

    const manifest = await readManifest(manifestPath);
    assert.equal(manifest.threads[0].injected_at, "2026-07-03T13:00:00.000Z");
    assert.equal(manifest.threads[1].injected_at, null);
    assert.equal(manifest.threads[2].injected_at, "2026-07-03T12:30:00.000Z");
  });

  it("preserves persisted runtime and interaction modes for resume turns", async () => {
    const manifestPath = await writeManifest([
      {
        thread_id: "active-approval",
        role: "active",
        status: "running",
        runtime_mode: "approval-required",
        interaction_mode: "plan",
        injected_at: null,
      },
    ]);
    const attachmentsDir = NodePath.join(NodePath.dirname(manifestPath), "attachments");
    await NodeFSP.mkdir(attachmentsDir);
    await NodeFSP.writeFile(NodePath.join(attachmentsDir, "pending-image.png"), "png-bytes");
    const requests: Array<any> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      attachmentsDir,
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        requests.push(JSON.parse(String(init?.body)));
        return new Response("{}", { status: 200 });
      },
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.equal(requests[0]?.type, "thread.interaction-mode.set");
    assert.equal(requests[0]?.interactionMode, "plan");
    assert.equal(requests[1]?.type, "thread.turn.start");
    assert.equal(requests[1]?.runtimeMode, "approval-required");
    assert.equal(requests[1]?.interactionMode, "plan");
  });

  it("dispatches original pending messages instead of the generic resume prompt", async () => {
    const pendingAttachments = [
      {
        type: "image",
        id: "pending-image",
        name: "pending.png",
        mimeType: "image/png",
        sizeBytes: 9,
      },
    ];
    const manifestPath = await writeManifest([
      {
        thread_id: "pending-start",
        role: "active",
        status: "ready",
        runtime_mode: "approval-required",
        interaction_mode: "plan",
        pending_message: {
          message_id: "message-pending-start",
          role: "system",
          text: "Original user request",
          attachments: pendingAttachments,
          model_selection: { provider: "codex", model: "gpt-5.4" },
          title_seed: "Investigate capture",
          source_proposed_plan: {
            threadId: "source-plan-thread",
            planId: "plan-1",
          },
        },
        injected_at: null,
      },
    ]);
    const attachmentsDir = NodePath.join(NodePath.dirname(manifestPath), "attachments");
    await NodeFSP.mkdir(attachmentsDir);
    await NodeFSP.writeFile(NodePath.join(attachmentsDir, "pending-image.png"), "png-bytes");
    const requests: Array<any> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      attachmentsDir,
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        requests.push(JSON.parse(String(init?.body)));
        return new Response("{}", { status: 200 });
      },
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.equal(requests[0]?.type, "thread.interaction-mode.set");
    assert.equal(requests[0]?.interactionMode, "plan");
    assert.equal(requests[1]?.type, "thread.turn.start");
    assert.equal(requests[1]?.message.messageId, "message-pending-start");
    assert.equal(requests[1]?.message.role, "system");
    assert.equal(requests[1]?.message.text, "Original user request");
    assert.notEqual(requests[1]?.message.text, RESUME_MESSAGE);
    assert.deepEqual(requests[1]?.message.attachments, [
      {
        type: "image",
        name: "pending.png",
        mimeType: "image/png",
        sizeBytes: 9,
        dataUrl: "data:image/png;base64,cG5nLWJ5dGVz",
      },
    ]);
    assert.deepEqual(requests[1]?.modelSelection, { provider: "codex", model: "gpt-5.4" });
    assert.equal(requests[1]?.titleSeed, "Investigate capture");
    assert.deepEqual(requests[1]?.sourceProposedPlan, {
      threadId: "source-plan-thread",
      planId: "plan-1",
    });
    assert.equal(requests[1]?.runtimeMode, "approval-required");
    assert.equal(requests[1]?.interactionMode, "plan");
  });

  it("resumes interrupted active turns before replaying queued pending messages", async () => {
    const manifestPath = await writeManifest([
      {
        thread_id: "active-with-queued",
        role: "active",
        status: "running",
        active_turn_id: "turn-active",
        runtime_mode: "approval-required",
        interaction_mode: "default",
        pending_message: {
          message_id: "message-queued",
          role: "user",
          text: "Queued prompt",
          attachments: [],
          runtime_mode: "full-access",
          interaction_mode: "plan",
          model_selection: { provider: "codex", model: "gpt-5.4" },
          title_seed: "Queued title",
          source_proposed_plan: {
            threadId: "source-plan-thread",
            planId: "plan-queued",
          },
        },
        injected_at: null,
      },
    ]);
    const requests: Array<any> = [];
    const events: Array<string> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        if (init?.method === "GET") {
          events.push("snapshot");
          const resumeRequest = requests.find((request) => request.type === "thread.turn.start");
          return new Response(
            JSON.stringify({
              threads: [
                {
                  id: "active-with-queued",
                  messages: [{ id: resumeRequest?.message.messageId, turnId: "turn-resume" }],
                },
              ],
            }),
            { status: 200 },
          );
        }
        const request = JSON.parse(String(init?.body));
        requests.push(request);
        events.push(request.type);
        return new Response("{}", { status: 200 });
      },
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.deepEqual(events, [
      "snapshot",
      "thread.interaction-mode.set",
      "thread.turn.start",
      "snapshot",
      "thread.turn.start",
    ]);
    assert.deepEqual(
      requests.map((request) => request.type),
      ["thread.interaction-mode.set", "thread.turn.start", "thread.turn.start"],
    );
    assert.equal(requests[0]?.interactionMode, "default");
    assert.equal(requests[1]?.message.role, "user");
    assert.equal(requests[1]?.message.text, RESUME_MESSAGE);
    assert.notEqual(requests[1]?.message.messageId, "message-queued");
    assert.equal(requests[1]?.modelSelection, undefined);
    assert.equal(requests[1]?.titleSeed, undefined);
    assert.equal(requests[1]?.sourceProposedPlan, undefined);
    assert.equal(requests[1]?.runtimeMode, "approval-required");
    assert.equal(requests[1]?.interactionMode, "default");
    assert.equal(requests[2]?.message.messageId, "message-queued");
    assert.equal(requests[2]?.message.text, "Queued prompt");
    assert.equal(requests[2]?.runtimeMode, "full-access");
    assert.equal(requests[2]?.interactionMode, "plan");
    assert.deepEqual(requests[2]?.modelSelection, { provider: "codex", model: "gpt-5.4" });
    assert.equal(requests[2]?.titleSeed, "Queued title");
    assert.deepEqual(requests[2]?.sourceProposedPlan, {
      threadId: "source-plan-thread",
      planId: "plan-queued",
    });
    assert.notEqual(requests[1]?.commandId, requests[2]?.commandId);
  });

  it("preflights snapshot read access before dispatching queued resumes", async () => {
    const manifestPath = await writeManifest([
      {
        thread_id: "active-with-queued-read-denied",
        role: "active",
        status: "running",
        active_turn_id: "turn-active",
        pending_message: {
          message_id: "message-queued",
          role: "user",
          text: "Queued prompt",
          attachments: [],
        },
        injected_at: null,
      },
    ]);
    const requests: Array<any> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        if (init?.method === "GET") return new Response("forbidden", { status: 403 });
        requests.push(JSON.parse(String(init?.body)));
        return new Response("{}", { status: 200 });
      },
    });

    assert.equal(result.injected, 0);
    assert.equal(result.failed, 1);
    assert.equal(result.failures[0]?.threadId, "active-with-queued-read-denied");
    assert.match(
      result.failures[0]?.error ?? "",
      /snapshot read access before dispatch.*HTTP 403/u,
    );
    assert.deepEqual(requests, []);
  });

  it("retries transient snapshot preflight failures before queued resumes", async () => {
    const manifestPath = await writeManifest([
      {
        thread_id: "active-with-queued-transient-read",
        role: "active",
        status: "running",
        active_turn_id: "turn-active",
        pending_message: {
          message_id: "message-queued",
          role: "user",
          text: "Queued prompt",
          attachments: [],
        },
        injected_at: null,
      },
    ]);
    const requests: Array<any> = [];
    const delays: Array<number> = [];
    let snapshotAttempts = 0;

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        if (init?.method === "GET") {
          snapshotAttempts += 1;
          if (snapshotAttempts === 1) return new Response("warming", { status: 503 });
          const resumeRequest = requests.find((request) => request.type === "thread.turn.start");
          return new Response(
            JSON.stringify({
              threads: [
                {
                  id: "active-with-queued-transient-read",
                  messages: [{ id: resumeRequest?.message.messageId, turnId: "turn-resume" }],
                },
              ],
            }),
            { status: 200 },
          );
        }
        const request = JSON.parse(String(init?.body));
        requests.push(request);
        return new Response("{}", { status: 200 });
      },
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 0.5,
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.equal(snapshotAttempts, 3);
    assert.deepEqual(delays, [4_000]);
    assert.deepEqual(
      requests.map((request) => request.type),
      ["thread.interaction-mode.set", "thread.turn.start", "thread.turn.start"],
    );
  });

  it("bounds queued replay waits by the resume-start timeout", async () => {
    const manifestPath = await writeManifest([
      {
        thread_id: "active-with-hung-snapshot",
        role: "active",
        status: "running",
        active_turn_id: "turn-active",
        runtime_mode: "approval-required",
        interaction_mode: "default",
        pending_message: {
          message_id: "message-queued",
          role: "user",
          text: "Queued prompt",
          attachments: [],
        },
        injected_at: null,
      },
    ]);
    const requests: Array<any> = [];
    let snapshotAttempts = 0;

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      dispatchAttemptTimeoutMs: 50,
      resumeStartTimeoutMs: 1,
      resumeStartPollMs: 1,
      sleep: async () => undefined,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        if (init?.method === "GET") {
          snapshotAttempts += 1;
          if (snapshotAttempts === 1) {
            return new Response(JSON.stringify({ threads: [] }), { status: 200 });
          }
          return await new Promise<Response>((_resolve, reject) => {
            const abortError = () => {
              const error = new Error("aborted");
              error.name = "AbortError";
              return error;
            };
            if (init.signal?.aborted) {
              reject(abortError());
              return;
            }
            init.signal?.addEventListener("abort", () => reject(abortError()), { once: true });
          });
        }
        const request = JSON.parse(String(init?.body));
        requests.push(request);
        return new Response("{}", { status: 200 });
      },
    });

    assert.equal(result.injected, 0);
    assert.equal(result.failed, 1);
    assert.match(result.failures[0]?.error ?? "", /queued replay timeout/u);
    assert.equal(snapshotAttempts, 2);
    assert.deepEqual(
      requests.map((request) => request.type),
      ["thread.interaction-mode.set", "thread.turn.start"],
    );
  });

  it("does not let queued attachment failures block interrupted active resumes", async () => {
    const manifestPath = await writeManifest([
      {
        thread_id: "active-with-bad-queued-attachment",
        role: "active",
        status: "running",
        active_turn_id: "turn-active",
        runtime_mode: "approval-required",
        interaction_mode: "default",
        pending_message: {
          message_id: "message-queued",
          role: "user",
          text: "Queued prompt",
          attachments: [
            {
              type: "image",
              id: "queued-image",
              name: "queued.png",
              mimeType: "image/png",
              sizeBytes: 9,
            },
          ],
          runtime_mode: "full-access",
          interaction_mode: "plan",
        },
        injected_at: null,
      },
    ]);
    const requests: Array<any> = [];
    const events: Array<string> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        if (init?.method === "GET") {
          events.push("snapshot");
          const resumeRequest = requests.find((request) => request.type === "thread.turn.start");
          return new Response(
            JSON.stringify({
              threads: [
                {
                  id: "active-with-bad-queued-attachment",
                  messages: [{ id: resumeRequest?.message.messageId, turnId: "turn-resume" }],
                },
              ],
            }),
            { status: 200 },
          );
        }
        const request = JSON.parse(String(init?.body));
        requests.push(request);
        events.push(request.type);
        return new Response("{}", { status: 200 });
      },
    });

    assert.equal(result.injected, 0);
    assert.equal(result.failed, 1);
    assert.equal(result.failures[0]?.threadId, "active-with-bad-queued-attachment");
    assert.match(result.failures[0]?.error ?? "", /pending attachment replay requires/u);
    assert.deepEqual(
      requests.map((request) => request.type),
      ["thread.interaction-mode.set", "thread.turn.start"],
    );
    assert.deepEqual(events, [
      "snapshot",
      "thread.interaction-mode.set",
      "thread.turn.start",
      "snapshot",
    ]);
    assert.equal(requests[1]?.message.text, RESUME_MESSAGE);
    assert.equal(requests[1]?.runtimeMode, "approval-required");
    assert.equal(requests[1]?.interactionMode, "default");

    const manifest = await readManifest(manifestPath);
    assert.equal(manifest.threads[0].injected_at, null);
  });

  it("defaults old pending message manifests without roles to user messages", async () => {
    const manifestPath = await writeManifest([
      {
        thread_id: "old-pending-start",
        role: "active",
        status: "ready",
        pending_message: {
          message_id: "message-old-pending-start",
          text: "Old pending request",
          attachments: [],
        },
        injected_at: null,
      },
    ]);
    const requests: Array<any> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        requests.push(JSON.parse(String(init?.body)));
        return new Response("{}", { status: 200 });
      },
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    const turnStart = requests.find((request) => request.type === "thread.turn.start");
    assert.equal(turnStart?.message.messageId, "message-old-pending-start");
    assert.equal(turnStart?.message.role, "user");
    assert.equal(turnStart?.message.text, "Old pending request");
  });

  it("retries transient dispatch failures and records one injection after success", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-retry", role: "active", status: "running", injected_at: null },
    ]);
    const requests: Array<any> = [];
    const delays: Array<number> = [];
    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        requests.push(body);
        if (body.type === "thread.turn.start" && requests.length === 2) {
          return new Response("warming", { status: 503 });
        }
        if (body.type === "thread.turn.start" && requests.length === 3) {
          return new Response("slow", { status: 429 });
        }
        return new Response("{}", { status: 200 });
      },
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 0.5,
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.deepEqual(
      requests.map((request) => request.type),
      [
        "thread.interaction-mode.set",
        "thread.turn.start",
        "thread.turn.start",
        "thread.turn.start",
      ],
    );
    assert.deepEqual(delays, [4_000, 8_000]);
    const turnRequests = requests.filter((request) => request.type === "thread.turn.start");
    assert.equal(new Set(turnRequests.map((request) => request.commandId)).size, 1);
    assert.equal(new Set(turnRequests.map((request) => request.message.messageId)).size, 1);

    const manifest = await readManifest(manifestPath);
    assert.equal(manifest.threads[0].injected_at, "2026-07-03T13:00:00.000Z");
  });

  it("honors Retry-After for transient rate limits", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-rate-limit", role: "active", status: "running", injected_at: null },
    ]);
    const delays: Array<number> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        return body.type === "thread.turn.start" && delays.length === 0
          ? new Response("slow", { status: 429, headers: { "retry-after": "9" } })
          : new Response("{}", { status: 200 });
      },
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 0.5,
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.deepEqual(delays, [9_000]);
  });

  it("caps Retry-After delays to the retry sleep budget", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-rate-limit", role: "active", status: "running", injected_at: null },
    ]);
    const delays: Array<number> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        return body.type === "thread.turn.start" && delays.length === 0
          ? new Response("slow", { status: 429, headers: { "retry-after": "3600" } })
          : new Response("{}", { status: 200 });
      },
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 0.5,
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.deepEqual(delays, [60_000]);
  });

  it("times out hung dispatch attempts and retries the command", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-hangs", role: "active", status: "running", injected_at: null },
    ]);
    const requests: Array<any> = [];
    const delays: Array<number> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        requests.push(body);
        if (body.type !== "thread.turn.start" || requests.length > 2) {
          return new Response("{}", { status: 200 });
        }

        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => {
            reject(new DOMException("aborted", "AbortError"));
          });
        });
      },
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 0.5,
      dispatchAttemptTimeoutMs: 1,
    });

    assert.deepEqual(result, { injected: 1, skipped: 0, failed: 0, failures: [] });
    assert.deepEqual(
      requests.map((request) => request.type),
      ["thread.interaction-mode.set", "thread.turn.start", "thread.turn.start"],
    );
    assert.deepEqual(delays, [4_000]);
  });

  it("does not retry hard 400 dispatch failures", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-bad", role: "active", status: "running", injected_at: null },
    ]);
    let requests = 0;

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        requests += 1;
        const body = JSON.parse(String(init?.body));
        return body.type === "thread.interaction-mode.set"
          ? new Response("{}", { status: 200 })
          : new Response("bad request", { status: 400 });
      },
      sleep: async () => {
        throw new Error("sleep should not be called");
      },
    });

    assert.equal(requests, 2);
    assert.equal(result.injected, 0);
    assert.equal(result.failed, 1);
    assert.equal(result.failures[0]?.threadId, "active-bad");
    assert.match(result.failures[0]?.error ?? "", /^HTTP 400 bad request$/u);
    const manifest = await readManifest(manifestPath);
    assert.equal(manifest.threads[0].injected_at, null);
  });

  it("exhausts transient retries per thread while continuing to process other threads", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-exhausts", role: "active", status: "running", injected_at: null },
      { thread_id: "active-ok", role: "active", status: "running", injected_at: null },
    ]);
    const requests: Array<string> = [];
    const delays: Array<number> = [];

    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        requests.push(`${body.threadId}:${body.type}`);
        if (body.threadId === "active-exhausts" && body.type === "thread.turn.start") {
          return new Response("warming", { status: 503 });
        }
        return new Response("{}", { status: 200 });
      },
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 0.5,
    });

    assert.equal(result.injected, 1);
    assert.equal(result.failed, 1);
    assert.equal(result.failures[0]?.threadId, "active-exhausts");
    assert.deepEqual(requests, [
      "active-exhausts:thread.interaction-mode.set",
      "active-exhausts:thread.turn.start",
      "active-exhausts:thread.turn.start",
      "active-exhausts:thread.turn.start",
      "active-exhausts:thread.turn.start",
      "active-exhausts:thread.turn.start",
      "active-ok:thread.interaction-mode.set",
      "active-ok:thread.turn.start",
    ]);
    assert.deepEqual(delays, [4_000, 8_000, 16_000, 32_000]);
    const manifest = await readManifest(manifestPath);
    assert.equal(manifest.threads[0].injected_at, null);
    assert.equal(manifest.threads[1].injected_at, "2026-07-03T13:00:00.000Z");
  });

  it("dry-run counts active null entries without HTTP or manifest writes", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active", role: "active", status: "running", injected_at: null },
      { thread_id: "waiting", role: "waiting", status: "waiting", injected_at: null },
    ]);
    const before = await NodeFSP.readFile(manifestPath, "utf8");
    const result = await injectResume({
      manifestPath,
      origin: "http://127.0.0.1:1",
      dryRun: true,
      fetchImpl: async () => {
        throw new Error("fetch should not be called");
      },
    });

    assert.deepEqual(result, { injected: 1, skipped: 1, failed: 0, failures: [] });
    assert.equal(await NodeFSP.readFile(manifestPath, "utf8"), before);
  });
});
