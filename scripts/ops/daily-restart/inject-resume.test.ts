// @effect-diagnostics nodeBuiltinImport:off - Tests exercise manifest file rewrites directly.
// @effect-diagnostics globalDate:off - Tests pin Date instances to deterministic ISO manifest values.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { afterEach, assert, describe, it } from "@effect/vitest";

import { RESUME_MESSAGE, injectResume, parseManifest } from "./inject-resume.ts";

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
  });

  it("injects active null entries once, skips waiting/already-injected entries, and retries failures", async () => {
    const manifestPath = await writeManifest([
      { thread_id: "active-ok", role: "active", status: "running", injected_at: null },
      { thread_id: "waiting", role: "waiting", status: "waiting", injected_at: null },
      {
        thread_id: "already",
        role: "active",
        status: "running",
        injected_at: "2026-07-03T12:30:00.000Z",
      },
      { thread_id: "active-fail", role: "active", status: "running", injected_at: null },
    ]);
    const requests: Array<any> = [];
    let failActiveFail = true;
    const options = {
      manifestPath,
      origin: "http://127.0.0.1:1",
      token: "test-token",
      dryRun: false,
      now: () => new Date("2026-07-03T13:00:00.000Z"),
      fetchImpl: async (_url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        requests.push(body);
        if (body.threadId === "active-fail" && failActiveFail) {
          return new Response("nope", { status: 500 });
        }
        return new Response("{}", { status: 200 });
      },
    };

    const first = await injectResume(options);
    assert.equal(first.injected, 1);
    assert.equal(first.skipped, 2);
    assert.equal(first.failed, 1);
    assert.equal(first.failures[0]?.threadId, "active-fail");
    assert.equal(requests[0]?.type, "thread.turn.start");
    assert.equal(requests[0]?.threadId, "active-ok");
    assert.equal(requests[0]?.message.role, "user");
    assert.equal(requests[0]?.message.text, RESUME_MESSAGE);
    assert.deepEqual(requests[0]?.message.attachments, []);

    let manifest = await readManifest(manifestPath);
    assert.equal(manifest.threads[0].injected_at, "2026-07-03T13:00:00.000Z");
    assert.equal(manifest.threads[1].injected_at, null);
    assert.equal(manifest.threads[2].injected_at, "2026-07-03T12:30:00.000Z");
    assert.equal(manifest.threads[3].injected_at, null);

    failActiveFail = false;
    const second = await injectResume(options);
    assert.deepEqual(second, { injected: 1, skipped: 3, failed: 0, failures: [] });
    assert.deepEqual(
      requests.map((request) => request.threadId),
      ["active-ok", "active-fail", "active-fail"],
    );
    assert.equal(requests[1]?.commandId, requests[2]?.commandId);
    assert.equal(requests[1]?.message.messageId, requests[2]?.message.messageId);
    manifest = await readManifest(manifestPath);
    assert.equal(manifest.threads[3].injected_at, "2026-07-03T13:00:00.000Z");
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
