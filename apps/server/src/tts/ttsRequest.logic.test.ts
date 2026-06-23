import { describe, expect, it } from "vite-plus/test";

import {
  buildElevenLabsRequest,
  ELEVENLABS_TTS_MODEL_ID,
  MAX_TTS_TEXT_CHARS,
  resolveVoiceId,
  validateTtsText,
} from "./ttsRequest.logic.ts";

describe("validateTtsText", () => {
  it("rejects empty input", () => {
    expect(validateTtsText("")).toEqual({ ok: false, reason: "empty" });
  });

  it("rejects whitespace-only input", () => {
    expect(validateTtsText("   \n\t  ")).toEqual({ ok: false, reason: "empty" });
  });

  it("trims and accepts normal input", () => {
    expect(validateTtsText("  hello world  ")).toEqual({ ok: true, text: "hello world" });
  });

  it("accepts text exactly at the limit", () => {
    const text = "a".repeat(MAX_TTS_TEXT_CHARS);
    expect(validateTtsText(text)).toEqual({ ok: true, text });
  });

  it("rejects text one char over the limit", () => {
    const text = "a".repeat(MAX_TTS_TEXT_CHARS + 1);
    expect(validateTtsText(text)).toEqual({ ok: false, reason: "too_long" });
  });
});

describe("resolveVoiceId", () => {
  it("prefers the requested voice id", () => {
    expect(resolveVoiceId("requested", "envDefault")).toBe("requested");
  });

  it("falls back to the env default when requested is blank", () => {
    expect(resolveVoiceId("   ", "envDefault")).toBe("envDefault");
  });

  it("falls back to the env default when requested is undefined", () => {
    expect(resolveVoiceId(undefined, "envDefault")).toBe("envDefault");
  });

  it("returns null when both are blank", () => {
    expect(resolveVoiceId("  ", "  ")).toBeNull();
  });

  it("returns null when both are undefined", () => {
    expect(resolveVoiceId(undefined, undefined)).toBeNull();
  });
});

describe("buildElevenLabsRequest", () => {
  it("URL-encodes the voice id and appends /stream", () => {
    const request = buildElevenLabsRequest({
      text: "hi",
      voiceId: "voice id/with weird?chars",
      apiKey: "secret",
    });
    expect(request.url).toBe(
      `https://api.elevenlabs.io/v1/text-to-speech/${encodeURIComponent(
        "voice id/with weird?chars",
      )}/stream`,
    );
    expect(request.url.endsWith("/stream")).toBe(true);
  });

  it("sets the xi-api-key, content-type, and accept headers", () => {
    const request = buildElevenLabsRequest({ text: "hi", voiceId: "v", apiKey: "secret" });
    expect(request.headers["xi-api-key"]).toBe("secret");
    expect(request.headers["content-type"]).toBe("application/json");
    expect(request.headers.accept).toBe("audio/mpeg");
  });

  it("builds the body with the turbo model id", () => {
    const request = buildElevenLabsRequest({ text: "hello", voiceId: "v", apiKey: "secret" });
    expect(request.body).toEqual({ text: "hello", model_id: ELEVENLABS_TTS_MODEL_ID });
    expect(request.body.model_id).toBe("eleven_turbo_v2_5");
  });
});
