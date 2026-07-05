import { useEffect } from "react";

const SERVICE_WORKER_URL = "/t3-service-worker.js";
const SERVICE_WORKER_SCOPE = "/";

type WakeLockSentinelLike = {
  readonly released: boolean;
  readonly release: () => Promise<void>;
  readonly addEventListener: (type: "release", listener: () => void) => void;
};

type NavigatorWithWakeLock = Navigator & {
  readonly wakeLock?: {
    readonly request: (type: "screen") => Promise<WakeLockSentinelLike>;
  };
};

function isProbablyLocalhost(): boolean {
  return (
    window.location.hostname === "localhost" ||
    window.location.hostname === "127.0.0.1" ||
    window.location.hostname === "::1"
  );
}

function canRegisterServiceWorker(): boolean {
  return "serviceWorker" in navigator && (window.isSecureContext || isProbablyLocalhost());
}

async function registerServiceWorker(): Promise<ServiceWorkerRegistration> {
  await navigator.serviceWorker.register(SERVICE_WORKER_URL, {
    scope: SERVICE_WORKER_SCOPE,
  });
  return await navigator.serviceWorker.ready;
}

function shouldRunPwaRuntime(): boolean {
  return (
    typeof window !== "undefined" &&
    (Boolean(window.desktopBridge) || window.location.protocol !== "t3code:")
  );
}

export function PwaRuntime() {
  if (!shouldRunPwaRuntime()) {
    return null;
  }
  return <PwaRuntimeEffects />;
}

function PwaRuntimeEffects() {
  useEffect(() => {
    if (!canRegisterServiceWorker()) return;
    void registerServiceWorker().catch(() => undefined);
  }, []);

  useEffect(() => {
    const navigatorWithWakeLock = navigator as NavigatorWithWakeLock;
    if (!navigatorWithWakeLock.wakeLock) return;

    let cancelled = false;
    let sentinel: WakeLockSentinelLike | null = null;

    const release = () => {
      const current = sentinel;
      sentinel = null;
      if (current && !current.released) {
        void current.release().catch(() => undefined);
      }
    };

    const acquire = async () => {
      if (cancelled || document.visibilityState !== "visible" || sentinel) return;
      try {
        sentinel = await navigatorWithWakeLock.wakeLock?.request("screen");
        sentinel?.addEventListener("release", () => {
          sentinel = null;
        });
      } catch {
        sentinel = null;
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        void acquire();
      } else {
        release();
      }
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    void acquire();
    return () => {
      cancelled = true;
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      release();
    };
  }, []);

  return null;
}
