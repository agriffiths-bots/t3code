import { useEffect, useReducer, useSyncExternalStore } from "react";

import { readDesktopPrimaryBearerToken } from "../../environments/primary/desktopAuth";
import { resolvePrimaryEnvironmentHttpUrl } from "../../environments/primary/target";
import {
  dictationReducer,
  IDLE_DICTATION_STATE,
  viewForMessage,
  type DictationEvent,
  type DictationView,
} from "./MessageDictate.logic";

const TTS_SPEAK_PATH = "/api/tts/speak";
const AUDIO_MIME = "audio/mpeg";

type Dispatch = (event: DictationEvent) => void;

interface ActiveDictation {
  messageId: string;
  audio: HTMLAudioElement;
  mediaSource: MediaSource;
  controller: AbortController;
  objectUrl: string;
}

let active: ActiveDictation | null = null;

// A tiny external store so every button re-renders when the single active
// message changes — viewForMessage already returns idle for non-active ids.
let activeMessageId: string | null = null;
const activeMessageListeners = new Set<() => void>();

function setActiveMessageId(next: string | null): void {
  if (activeMessageId === next) {
    return;
  }
  activeMessageId = next;
  for (const listener of activeMessageListeners) {
    listener();
  }
}

function subscribeActiveMessageId(listener: () => void): () => void {
  activeMessageListeners.add(listener);
  return () => {
    activeMessageListeners.delete(listener);
  };
}

function getActiveMessageId(): string | null {
  return activeMessageId;
}

function isSameOriginCookieMode(): boolean {
  if (
    typeof window === "undefined" ||
    window.desktopBridge !== undefined ||
    window.nativeApi !== undefined ||
    !window.location.origin.startsWith("http")
  ) {
    return false;
  }
  return new URL(resolvePrimaryEnvironmentHttpUrl("/")).origin === window.location.origin;
}

export async function buildDictationRequestInit(
  body: string,
  signal: AbortSignal,
): Promise<RequestInit> {
  const headers: Record<string, string> = { "content-type": "application/json" };

  if (isSameOriginCookieMode()) {
    return { method: "POST", headers, body, credentials: "include", signal };
  }

  const bearerToken = await readDesktopPrimaryBearerToken();
  if (bearerToken) {
    headers.authorization = `Bearer ${bearerToken}`;
  }
  return { method: "POST", headers, body, credentials: "omit", signal };
}

function teardownActive(): void {
  if (!active) {
    return;
  }
  const current = active;
  active = null;
  try {
    current.controller.abort();
  } catch {
    // ignore abort failures
  }
  try {
    current.audio.pause();
    current.audio.removeAttribute("src");
    current.audio.load();
  } catch {
    // ignore teardown failures
  }
  try {
    URL.revokeObjectURL(current.objectUrl);
  } catch {
    // ignore revoke failures
  }
  setActiveMessageId(null);
}

export function resetDictation(): void {
  teardownActive();
}

function dictationSupported(): boolean {
  return (
    typeof window !== "undefined" &&
    typeof MediaSource !== "undefined" &&
    typeof MediaSource.isTypeSupported === "function" &&
    MediaSource.isTypeSupported(AUDIO_MIME)
  );
}

function waitForUpdateEnd(sourceBuffer: SourceBuffer): Promise<void> {
  if (!sourceBuffer.updating) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    sourceBuffer.addEventListener("updateend", () => resolve(), { once: true });
  });
}

async function pumpStream(input: {
  messageId: string;
  response: Response;
  mediaSource: MediaSource;
  sourceBuffer: SourceBuffer;
  audio: HTMLAudioElement;
  signal: AbortSignal;
  dispatch: Dispatch;
}): Promise<void> {
  const { response, mediaSource, sourceBuffer, audio, signal, dispatch, messageId } = input;
  if (!response.body) {
    throw new Error("Dictation response had no body.");
  }
  const reader = response.body.getReader();
  let started = false;

  while (!signal.aborted) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    if (!value || value.length === 0) {
      continue;
    }
    await waitForUpdateEnd(sourceBuffer);
    if (signal.aborted) {
      return;
    }
    sourceBuffer.appendBuffer(value);
    await waitForUpdateEnd(sourceBuffer);
    if (!started) {
      started = true;
      dispatch({ type: "STREAM_READY", messageId });
      void audio.play().catch(() => {
        /* autoplay rejection surfaces via the audio error handler */
      });
    }
  }

  if (!signal.aborted && mediaSource.readyState === "open") {
    mediaSource.endOfStream();
  }
}

function startDictation(messageId: string, text: string, dispatch: Dispatch): void {
  teardownActive();

  if (!dictationSupported()) {
    dispatch({ type: "FAIL", messageId, errorMessage: "Voice unavailable" });
    return;
  }

  const controller = new AbortController();
  const audio = new Audio();
  const mediaSource = new MediaSource();
  const objectUrl = URL.createObjectURL(mediaSource);
  audio.src = objectUrl;

  active = { messageId, audio, mediaSource, controller, objectUrl };
  setActiveMessageId(messageId);
  dispatch({ type: "START", messageId });

  const fail = (errorMessage: string) => {
    if (active?.messageId === messageId) {
      teardownActive();
    }
    dispatch({ type: "FAIL", messageId, errorMessage });
  };

  audio.addEventListener("ended", () => {
    if (active?.messageId === messageId) {
      teardownActive();
    }
    dispatch({ type: "ENDED" });
  });
  audio.addEventListener("error", () => {
    fail("Dictation failed");
  });

  mediaSource.addEventListener(
    "sourceopen",
    () => {
      void (async () => {
        try {
          const sourceBuffer = mediaSource.addSourceBuffer(AUDIO_MIME);
          const requestInit = await buildDictationRequestInit(
            JSON.stringify({ text }),
            controller.signal,
          );
          const response = await fetch(
            resolvePrimaryEnvironmentHttpUrl(TTS_SPEAK_PATH),
            requestInit,
          );
          if (!response.ok) {
            fail(response.status === 503 ? "Voice unavailable" : "Dictation failed");
            return;
          }
          await pumpStream({
            messageId,
            response,
            mediaSource,
            sourceBuffer,
            audio,
            signal: controller.signal,
            dispatch,
          });
        } catch (error) {
          if (controller.signal.aborted) {
            return;
          }
          fail(error instanceof Error && error.message ? error.message : "Dictation failed");
        }
      })();
    },
    { once: true },
  );
}

function pauseDictation(dispatch: Dispatch): void {
  if (!active) {
    return;
  }
  const { messageId, audio } = active;
  audio.pause();
  dispatch({ type: "PAUSE", messageId });
}

function resumeDictation(dispatch: Dispatch): void {
  if (!active) {
    return;
  }
  const { messageId, audio } = active;
  void audio.play().catch(() => {
    /* ignore — error handler will surface playback failures */
  });
  dispatch({ type: "RESUME", messageId });
}

export interface DictationController extends DictationView {
  readonly onClick: () => void;
}

export function useDictationController(messageId: string, text: string): DictationController {
  const [state, dispatch] = useReducer(dictationReducer, IDLE_DICTATION_STATE);

  // Re-render this button when the single active message changes so a button
  // whose message is no longer active falls back to its idle view.
  const currentActiveMessageId = useSyncExternalStore(
    subscribeActiveMessageId,
    getActiveMessageId,
    getActiveMessageId,
  );

  // Reconcile a stale reducer against the singleton. When another message takes
  // over playback, the singleton tears down our audio but cannot dispatch into
  // this reducer, so a previously-active button can be left holding a stale
  // loading/playing/paused state. Reset it so subsequent clicks behave as idle.
  useEffect(() => {
    if (
      state.messageId === messageId &&
      currentActiveMessageId !== messageId &&
      state.status !== "idle" &&
      state.status !== "error"
    ) {
      dispatch({ type: "RESET" });
    }
  }, [currentActiveMessageId, messageId, state.messageId, state.status]);

  useEffect(() => {
    return () => {
      if (active?.messageId === messageId) {
        teardownActive();
      }
    };
  }, [messageId]);

  const view = viewForMessage(state, messageId, currentActiveMessageId);

  const onClick = () => {
    switch (view.mode) {
      case "idle":
      case "error":
        startDictation(messageId, text, dispatch);
        break;
      case "playing":
        pauseDictation(dispatch);
        break;
      case "paused":
        resumeDictation(dispatch);
        break;
      case "loading":
        break;
    }
  };

  return { ...view, onClick };
}
