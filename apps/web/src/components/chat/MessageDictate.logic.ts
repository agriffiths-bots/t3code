export type DictationStatus = "idle" | "loading" | "playing" | "paused" | "error";

export interface DictationState {
  readonly status: DictationStatus;
  readonly messageId: string | null;
  readonly errorMessage?: string;
}

export type DictationEvent =
  | { readonly type: "START"; readonly messageId: string }
  | { readonly type: "STREAM_READY"; readonly messageId: string }
  | { readonly type: "PLAY"; readonly messageId: string }
  | { readonly type: "PAUSE"; readonly messageId: string }
  | { readonly type: "RESUME"; readonly messageId: string }
  | { readonly type: "ENDED" }
  | { readonly type: "FAIL"; readonly messageId: string; readonly errorMessage: string }
  | { readonly type: "RESET" };

export const IDLE_DICTATION_STATE: DictationState = { status: "idle", messageId: null };

export function dictationReducer(state: DictationState, event: DictationEvent): DictationState {
  switch (event.type) {
    case "START":
      return { status: "loading", messageId: event.messageId };

    case "STREAM_READY":
      if (state.status !== "loading" || state.messageId !== event.messageId) {
        return state;
      }
      return { status: "playing", messageId: event.messageId };

    case "PLAY":
      if (state.messageId !== event.messageId) {
        return state;
      }
      return { status: "playing", messageId: event.messageId };

    case "PAUSE":
      if (state.status !== "playing" || state.messageId !== event.messageId) {
        return state;
      }
      return { status: "paused", messageId: event.messageId };

    case "RESUME":
      if (state.status !== "paused" || state.messageId !== event.messageId) {
        return state;
      }
      return { status: "playing", messageId: event.messageId };

    case "ENDED":
      return IDLE_DICTATION_STATE;

    case "FAIL":
      return { status: "error", messageId: event.messageId, errorMessage: event.errorMessage };

    case "RESET":
      return IDLE_DICTATION_STATE;
  }
}

export function resolveAssistantMessageDictateState({
  text,
  showButton,
  streaming,
}: {
  text: string | null;
  showButton: boolean;
  streaming: boolean;
}) {
  const hasText = text !== null && text.trim().length > 0;
  return {
    text: hasText ? text : null,
    visible: showButton && hasText && !streaming,
  };
}

export type DictationViewMode = "idle" | "loading" | "playing" | "paused" | "error";

export interface DictationView {
  readonly mode: DictationViewMode;
  readonly label: string;
  readonly disabled: boolean;
  readonly spinning: boolean;
}

const IDLE_VIEW: DictationView = {
  mode: "idle",
  label: "Read aloud",
  disabled: false,
  spinning: false,
};

export function viewForMessage(
  state: DictationState,
  messageId: string,
  activeMessageId: string | null = state.messageId,
): DictationView {
  if (state.messageId !== messageId) {
    return IDLE_VIEW;
  }

  // error is a terminal, local-only state (the singleton has already been torn
  // down) so it must render regardless of the active-message guard below.
  if (state.status === "error") {
    return {
      mode: "error",
      label: state.errorMessage ?? "Dictation failed",
      disabled: false,
      spinning: false,
    };
  }

  // Single-active-playback guard: the loading/playing/paused views depend on a
  // live singleton. If a different message became active, the singleton tore
  // down our audio but could not dispatch into our reducer, so our local state
  // may be stale. Force idle when we are no longer the active message.
  if (activeMessageId !== messageId) {
    return IDLE_VIEW;
  }

  switch (state.status) {
    case "idle":
      return IDLE_VIEW;
    case "loading":
      return { mode: "loading", label: "Loading dictation", disabled: true, spinning: true };
    case "playing":
      return { mode: "playing", label: "Pause dictation", disabled: false, spinning: false };
    case "paused":
      return { mode: "paused", label: "Resume dictation", disabled: false, spinning: false };
  }
}
