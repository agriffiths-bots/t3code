import { describe, expect, it } from "vite-plus/test";

import {
  dictationReducer,
  IDLE_DICTATION_STATE,
  resolveAssistantMessageDictateState,
  viewForMessage,
  type DictationState,
} from "./MessageDictate.logic";

const M = "msg-1";
const OTHER = "msg-2";

describe("dictationReducer", () => {
  it("START moves to loading and claims the message", () => {
    expect(dictationReducer(IDLE_DICTATION_STATE, { type: "START", messageId: M })).toEqual({
      status: "loading",
      messageId: M,
    });
  });

  it("STREAM_READY moves loading -> playing for the same message", () => {
    const loading: DictationState = { status: "loading", messageId: M };
    expect(dictationReducer(loading, { type: "STREAM_READY", messageId: M })).toEqual({
      status: "playing",
      messageId: M,
    });
  });

  it("STREAM_READY is ignored for a mismatched messageId", () => {
    const loading: DictationState = { status: "loading", messageId: M };
    expect(dictationReducer(loading, { type: "STREAM_READY", messageId: OTHER })).toBe(loading);
  });

  it("STREAM_READY is ignored when not loading", () => {
    const playing: DictationState = { status: "playing", messageId: M };
    expect(dictationReducer(playing, { type: "STREAM_READY", messageId: M })).toBe(playing);
  });

  it("PAUSE moves playing -> paused for the same message", () => {
    const playing: DictationState = { status: "playing", messageId: M };
    expect(dictationReducer(playing, { type: "PAUSE", messageId: M })).toEqual({
      status: "paused",
      messageId: M,
    });
  });

  it("PAUSE is ignored for a mismatched messageId", () => {
    const playing: DictationState = { status: "playing", messageId: M };
    expect(dictationReducer(playing, { type: "PAUSE", messageId: OTHER })).toBe(playing);
  });

  it("PAUSE is ignored when not playing", () => {
    const paused: DictationState = { status: "paused", messageId: M };
    expect(dictationReducer(paused, { type: "PAUSE", messageId: M })).toBe(paused);
  });

  it("RESUME moves paused -> playing for the same message", () => {
    const paused: DictationState = { status: "paused", messageId: M };
    expect(dictationReducer(paused, { type: "RESUME", messageId: M })).toEqual({
      status: "playing",
      messageId: M,
    });
  });

  it("RESUME is ignored for a mismatched messageId", () => {
    const paused: DictationState = { status: "paused", messageId: M };
    expect(dictationReducer(paused, { type: "RESUME", messageId: OTHER })).toBe(paused);
  });

  it("RESUME is ignored when not paused", () => {
    const playing: DictationState = { status: "playing", messageId: M };
    expect(dictationReducer(playing, { type: "RESUME", messageId: M })).toBe(playing);
  });

  it("ENDED resets to idle/null", () => {
    const playing: DictationState = { status: "playing", messageId: M };
    expect(dictationReducer(playing, { type: "ENDED" })).toEqual(IDLE_DICTATION_STATE);
  });

  it("RESET resets to idle/null", () => {
    const paused: DictationState = { status: "paused", messageId: M };
    expect(dictationReducer(paused, { type: "RESET" })).toEqual(IDLE_DICTATION_STATE);
  });

  it("FAIL moves to error with the message and errorMessage", () => {
    expect(
      dictationReducer(
        { status: "loading", messageId: M },
        { type: "FAIL", messageId: M, errorMessage: "Voice unavailable" },
      ),
    ).toEqual({ status: "error", messageId: M, errorMessage: "Voice unavailable" });
  });
});

describe("resolveAssistantMessageDictateState", () => {
  it("is hidden while streaming", () => {
    const state = resolveAssistantMessageDictateState({
      text: "some text",
      showButton: true,
      streaming: true,
    });
    expect(state.visible).toBe(false);
  });

  it("is hidden when text is empty", () => {
    const state = resolveAssistantMessageDictateState({
      text: "",
      showButton: true,
      streaming: false,
    });
    expect(state.visible).toBe(false);
    expect(state.text).toBeNull();
  });

  it("is hidden when text is whitespace-only", () => {
    const state = resolveAssistantMessageDictateState({
      text: "   \n ",
      showButton: true,
      streaming: false,
    });
    expect(state.visible).toBe(false);
    expect(state.text).toBeNull();
  });

  it("is hidden when showButton is false", () => {
    const state = resolveAssistantMessageDictateState({
      text: "some text",
      showButton: false,
      streaming: false,
    });
    expect(state.visible).toBe(false);
  });

  it("is visible with non-empty text, showButton, and not streaming", () => {
    const state = resolveAssistantMessageDictateState({
      text: "some text",
      showButton: true,
      streaming: false,
    });
    expect(state.visible).toBe(true);
    expect(state.text).toBe("some text");
  });

  it("is hidden when text is null", () => {
    const state = resolveAssistantMessageDictateState({
      text: null,
      showButton: true,
      streaming: false,
    });
    expect(state.visible).toBe(false);
  });
});

describe("viewForMessage", () => {
  it("renders idle for a different active message even while another plays", () => {
    const playingOther: DictationState = { status: "playing", messageId: OTHER };
    const view = viewForMessage(playingOther, M);
    expect(view.mode).toBe("idle");
    expect(view.disabled).toBe(false);
    expect(view.spinning).toBe(false);
  });

  it("renders loading for the active message", () => {
    const view = viewForMessage({ status: "loading", messageId: M }, M);
    expect(view.mode).toBe("loading");
    expect(view.disabled).toBe(true);
    expect(view.spinning).toBe(true);
  });

  it("renders playing with a pause label for the active message", () => {
    const view = viewForMessage({ status: "playing", messageId: M }, M);
    expect(view.mode).toBe("playing");
    expect(view.label).toBe("Pause dictation");
    expect(view.disabled).toBe(false);
  });

  it("renders paused with a resume label for the active message", () => {
    const view = viewForMessage({ status: "paused", messageId: M }, M);
    expect(view.mode).toBe("paused");
    expect(view.label).toBe("Resume dictation");
  });

  it("renders error with the errorMessage as the label", () => {
    const view = viewForMessage(
      { status: "error", messageId: M, errorMessage: "Voice unavailable" },
      M,
    );
    expect(view.mode).toBe("error");
    expect(view.label).toBe("Voice unavailable");
    expect(view.disabled).toBe(false);
  });

  it("renders idle for the active message when status is idle", () => {
    const view = viewForMessage({ status: "idle", messageId: M }, M);
    expect(view.mode).toBe("idle");
  });
});
