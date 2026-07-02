import { TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { shouldShowThreadComposerStopAction } from "./threadComposerLogic";

describe("shouldShowThreadComposerStopAction", () => {
  it("keeps send available for parked waiting sessions", () => {
    expect(
      shouldShowThreadComposerStopAction({
        status: "waiting",
        activeTurnId: null,
      }),
    ).toBe(false);
  });

  it("shows stop while a waiting session still has an active turn", () => {
    expect(
      shouldShowThreadComposerStopAction({
        status: "waiting",
        activeTurnId: TurnId.make("turn-1"),
      }),
    ).toBe(true);
  });
});
