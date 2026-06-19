import { useCallback, useMemo } from "react";
import * as Schema from "effect/Schema";

import { getLocalStorageItem, setLocalStorageItem, useLocalStorage } from "./hooks/useLocalStorage";

export const SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY = "t3code:schedule-banner-dismissals:v1";

const ScheduleBannerDismissalsSchema = Schema.Struct({
  keys: Schema.Array(Schema.String),
});

type ScheduleBannerDismissals = typeof ScheduleBannerDismissalsSchema.Type;

function readScheduleBannerDismissals(): ScheduleBannerDismissals {
  try {
    return (
      getLocalStorageItem(
        SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY,
        ScheduleBannerDismissalsSchema,
      ) ?? {
        keys: [],
      }
    );
  } catch {
    // Malformed localStorage is tolerated: an unreadable document means the
    // banner has not been dismissed for the current run (design.md PERSISTENCE).
    return { keys: [] };
  }
}

function writeScheduleBannerDismissals(document: ScheduleBannerDismissals): void {
  try {
    setLocalStorageItem(
      SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY,
      document,
      ScheduleBannerDismissalsSchema,
    );
  } catch {
    // Dismissal state is best-effort UI state; a storage failure should not block the banner.
  }
}

export function isScheduleBannerDismissed(dismissalKey: string | null | undefined): boolean {
  if (!dismissalKey) {
    return false;
  }
  return readScheduleBannerDismissals().keys.includes(dismissalKey);
}

export function dismissScheduleBanner(dismissalKey: string | null | undefined): void {
  const trimmedKey = dismissalKey?.trim();
  if (!trimmedKey) {
    return;
  }
  const document = readScheduleBannerDismissals();
  if (document.keys.includes(trimmedKey)) {
    return;
  }
  writeScheduleBannerDismissals({
    keys: [...document.keys, trimmedKey],
  });
}

export function useDismissedScheduleBannerKeys() {
  const [dismissals, setDismissals] = useLocalStorage(
    SCHEDULE_BANNER_DISMISSALS_STORAGE_KEY,
    { keys: [] },
    ScheduleBannerDismissalsSchema,
  );
  const dismissedKeys = dismissals.keys;

  const dismissedKeySet = useMemo(() => new Set(dismissedKeys), [dismissedKeys]);

  const dismissBannerKey = useCallback(
    (key: string) => {
      const trimmedKey = key.trim();
      if (trimmedKey.length === 0 || dismissedKeySet.has(trimmedKey)) {
        return;
      }

      setDismissals({
        keys: [...dismissedKeys, trimmedKey],
      });
    },
    [dismissedKeySet, dismissedKeys, setDismissals],
  );

  return {
    dismissedBannerKeys: dismissedKeySet,
    dismissBannerKey,
  };
}
