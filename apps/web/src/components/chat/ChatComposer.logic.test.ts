import { describe, expect, it } from "vite-plus/test";

import { shouldSubmitComposerOnEnter } from "./ChatComposer.logic";

describe("shouldSubmitComposerOnEnter", () => {
  it("submits desktop plain Enter", () => {
    expect(shouldSubmitComposerOnEnter({ isMobileViewport: false, shiftKey: false })).toBe(true);
  });

  it("keeps desktop Shift+Enter as a newline", () => {
    expect(shouldSubmitComposerOnEnter({ isMobileViewport: false, shiftKey: true })).toBe(false);
  });

  it("keeps mobile plain Enter as a newline", () => {
    expect(shouldSubmitComposerOnEnter({ isMobileViewport: true, shiftKey: false })).toBe(false);
  });
});
