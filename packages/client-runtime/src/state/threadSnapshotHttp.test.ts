import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { PrimaryConnectionTarget, type PreparedConnection } from "../connection/model.ts";
import { remoteHttpClientLayer } from "../rpc/http.ts";
import { ThreadRevisionLoader, threadRevisionLoaderLayer } from "./threadSnapshotHttp.ts";

const THREAD_ID = ThreadId.make("thread-404");
const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-404"),
  label: "Revision loader test",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});
const PREPARED: PreparedConnection = {
  environmentId: TARGET.environmentId,
  label: TARGET.label,
  httpBaseUrl: TARGET.httpBaseUrl,
  socketUrl: TARGET.wsBaseUrl,
  httpAuthorization: null,
  target: TARGET,
};

const loadRevisionWith = (fetchFn: typeof fetch) =>
  Effect.gen(function* () {
    const loader = yield* ThreadRevisionLoader;
    return yield* loader.load(PREPARED, THREAD_ID);
  }).pipe(
    Effect.provide(threadRevisionLoaderLayer.pipe(Layer.provide(remoteHttpClientLayer(fetchFn)))),
  );

describe("thread revision HTTP loader", () => {
  it.effect("classifies a typed thread_not_found 404 as gone", () =>
    Effect.gen(function* () {
      const result = yield* loadRevisionWith(() =>
        Promise.resolve(
          Response.json(
            {
              _tag: "EnvironmentResourceNotFoundError",
              code: "not_found",
              reason: "thread_not_found",
              traceId: "trace-revision-not-found",
            },
            { status: 404 },
          ),
        ),
      );

      expect(result).toEqual({ kind: "gone" });
    }),
  );

  it.effect("classifies a transient revision endpoint failure as unavailable", () =>
    Effect.gen(function* () {
      const result = yield* loadRevisionWith(() =>
        Promise.resolve(
          Response.json(
            {
              _tag: "EnvironmentInternalError",
              code: "internal_error",
              reason: "orchestration_thread_revision_failed",
              traceId: "trace-revision-projection-lag",
            },
            { status: 500 },
          ),
        ),
      );

      expect(result).toEqual({ kind: "unavailable" });
    }),
  );

  it.effect("classifies a transient fetch failure as unavailable", () =>
    Effect.gen(function* () {
      const result = yield* loadRevisionWith(() =>
        Promise.reject(new Error("temporary network failure")),
      );

      expect(result).toEqual({ kind: "unavailable" });
    }),
  );
});
