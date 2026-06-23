export const MAX_TTS_TEXT_CHARS = 5000;
export const ELEVENLABS_TTS_MODEL_ID = "eleven_turbo_v2_5";
export const ELEVENLABS_TTS_BASE_URL = "https://api.elevenlabs.io/v1/text-to-speech";

export type TtsTextValidation =
  | { readonly ok: true; readonly text: string }
  | { readonly ok: false; readonly reason: "empty" | "too_long" };

export function validateTtsText(raw: string): TtsTextValidation {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return { ok: false, reason: "empty" };
  }
  if (trimmed.length > MAX_TTS_TEXT_CHARS) {
    return { ok: false, reason: "too_long" };
  }
  return { ok: true, text: trimmed };
}

export function resolveVoiceId(
  requested: string | undefined,
  envDefault: string | undefined,
): string | null {
  const requestedTrimmed = requested?.trim();
  if (requestedTrimmed) {
    return requestedTrimmed;
  }
  const envTrimmed = envDefault?.trim();
  if (envTrimmed) {
    return envTrimmed;
  }
  return null;
}

export interface ElevenLabsRequest {
  readonly url: string;
  readonly headers: Record<string, string>;
  readonly body: { readonly text: string; readonly model_id: string };
}

export function buildElevenLabsRequest(input: {
  readonly text: string;
  readonly voiceId: string;
  readonly apiKey: string;
}): ElevenLabsRequest {
  return {
    url: `${ELEVENLABS_TTS_BASE_URL}/${encodeURIComponent(input.voiceId)}/stream`,
    headers: {
      "xi-api-key": input.apiKey,
      "content-type": "application/json",
      accept: "audio/mpeg",
    },
    body: {
      text: input.text,
      model_id: ELEVENLABS_TTS_MODEL_ID,
    },
  };
}
