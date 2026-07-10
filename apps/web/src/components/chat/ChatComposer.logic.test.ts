import { describe, expect, it } from "vite-plus/test";

import { shouldSubmitComposerOnEnter } from "./ChatComposer.logic";

describe("shouldSubmitComposerOnEnter", () => {
  it("submits desktop plain Enter", () => {
    expect(shouldSubmitComposerOnEnter({ hasCoarsePointer: false, shiftKey: false })).toBe(true);
  });

  it("keeps desktop Shift+Enter as a newline", () => {
    expect(shouldSubmitComposerOnEnter({ hasCoarsePointer: false, shiftKey: true })).toBe(false);
  });

  it("keeps coarse-pointer plain Enter as a newline regardless of viewport or origin", () => {
    expect(shouldSubmitComposerOnEnter({ hasCoarsePointer: true, shiftKey: false })).toBe(false);
  });
});
