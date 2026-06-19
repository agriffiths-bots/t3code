import * as Schema from "effect/Schema";
import { beforeEach, describe, expect, it } from "vite-plus/test";

import { removeLocalStorageItem, setLocalStorageItem } from "./hooks/useLocalStorage";
import {
  dismissScheduleBanner,
  isScheduleBannerDismissed,
  SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY,
} from "./scheduleBannerDismissal";

describe("scheduleBannerDismissal", () => {
  beforeEach(() => {
    removeLocalStorageItem(SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY);
  });

  it("is not dismissed before and dismissed after", () => {
    const key = "env|T1:2026-06-19T14:30:00.000Z";
    expect(isScheduleBannerDismissed(key)).toBe(false);
    dismissScheduleBanner(key);
    expect(isScheduleBannerDismissed(key)).toBe(true);
  });

  it("does not let one thread's dismissal suppress another thread", () => {
    dismissScheduleBanner("env|T1:2026-06-19T14:30:00.000Z");
    expect(isScheduleBannerDismissed("env|T2:2026-06-19T14:30:00.000Z")).toBe(false);
  });

  it("re-shows when nextRunAt advances (new key)", () => {
    dismissScheduleBanner("env|T1:2026-06-19T14:30:00.000Z");
    // Reactor advanced next_run_at -> the dismiss key changes -> not dismissed.
    expect(isScheduleBannerDismissed("env|T1:2026-06-19T15:00:00.000Z")).toBe(false);
  });

  it("tolerates malformed localStorage (treated as not dismissed)", () => {
    // Write a shape the dismissals schema cannot decode; reads must not throw.
    setLocalStorageItem(SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY, "not the right shape", Schema.String);
    expect(isScheduleBannerDismissed("env|T1:2026-06-19T14:30:00.000Z")).toBe(false);
  });
});
