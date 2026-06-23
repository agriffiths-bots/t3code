import { AuthOrchestrationOperateScope } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import {
  HttpClient,
  HttpClientResponse,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";

import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import { ttsSpeakHandler } from "./http.ts";

const SECRET_KEY = "sk-super-secret-elevenlabs-key";

const authLayer = Layer.succeed(
  EnvironmentAuth.EnvironmentAuth,
  EnvironmentAuth.EnvironmentAuth.of({
    authenticateHttpRequest: () =>
      Effect.succeed({
        sessionId: "session_test" as never,
        subject: "test-subject",
        method: "browser-session-cookie" as const,
        scopes: [AuthOrchestrationOperateScope],
      }),
  } as never),
);

const requestLayer = (body: unknown) =>
  Layer.succeed(
    HttpServerRequest.HttpServerRequest,
    HttpServerRequest.fromWeb(
      new Request("http://localhost/api/tts/speak", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
    ),
  );

const mockHttpClientLayer = (respond: () => Response) =>
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) => Effect.succeed(HttpClientResponse.fromWeb(request, respond()))),
  );

function runHandler(input: { readonly body: unknown; readonly respond?: () => Response }) {
  return Effect.gen(function* () {
    const logs: string[] = [];
    const captureLogger = Logger.make((options) => {
      logs.push(JSON.stringify({ message: options.message }));
    });

    const httpClientLayer = mockHttpClientLayer(
      input.respond ?? (() => new Response(new Uint8Array(), { status: 200 })),
    );

    const response = yield* ttsSpeakHandler.pipe(
      Effect.provide(
        Layer.mergeAll(
          authLayer,
          requestLayer(input.body),
          httpClientLayer,
          Logger.layer([captureLogger]),
        ),
      ),
    );

    const web = HttpServerResponse.toWeb(response);
    const bodyBytes = new Uint8Array(yield* Effect.promise(() => web.arrayBuffer()));
    const bodyText = new TextDecoder().decode(bodyBytes);
    return {
      status: web.status,
      contentType: web.headers.get("content-type"),
      bodyText,
      bodyBytes,
      logText: logs.join("\n"),
    };
  });
}

describe("ttsSpeakHandler", () => {
  beforeEach(() => {
    process.env.ELEVENLABS_API_KEY = SECRET_KEY;
    process.env.ELEVENLABS_VOICE_ID = "voice-default";
  });

  afterEach(() => {
    delete process.env.ELEVENLABS_API_KEY;
    delete process.env.ELEVENLABS_VOICE_ID;
  });

  it.effect("streams a 200 audio/mpeg response through unchanged", () =>
    Effect.gen(function* () {
      const audioBytes = new Uint8Array([1, 2, 3, 4, 5]);
      const result = yield* runHandler({
        body: { text: "hello world" },
        respond: () =>
          new Response(audioBytes, {
            status: 200,
            headers: { "content-type": "audio/mpeg" },
          }),
      });

      expect(result.status).toBe(200);
      expect(result.contentType).toContain("audio/mpeg");
      expect(Array.from(result.bodyBytes)).toEqual([1, 2, 3, 4, 5]);
    }),
  );

  it.effect("maps an upstream 401 to a 502 without leaking the api key", () =>
    Effect.gen(function* () {
      const result = yield* runHandler({
        body: { text: "hello world" },
        respond: () => new Response("unauthorized", { status: 401 }),
      });

      expect(result.status).toBe(502);
      expect(result.bodyText).not.toContain(SECRET_KEY);
      expect(result.logText).not.toContain(SECRET_KEY);
    }),
  );

  it.effect("returns 503 when the api key is unset", () =>
    Effect.gen(function* () {
      delete process.env.ELEVENLABS_API_KEY;
      const result = yield* runHandler({ body: { text: "hello world" } });

      expect(result.status).toBe(503);
      expect(result.bodyText).not.toContain(SECRET_KEY);
    }),
  );

  it.effect("returns 400 when the text is empty", () =>
    Effect.gen(function* () {
      const result = yield* runHandler({ body: { text: "   " } });

      expect(result.status).toBe(400);
    }),
  );
});
