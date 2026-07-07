import { useAtomValue } from "@effect/atom-react";
import type {
  EnvironmentId,
  ServerDeviceNotification,
  ServerNotificationStreamEvent,
  ServerWebPushSubscription,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { useCallback, useEffect, useRef } from "react";

import { isElectron } from "../env";
import { useEnvironmentHttpBaseUrl, usePrimaryEnvironmentId } from "../state/environments";
import { serverEnvironment } from "../state/server";
import { useAtomCommand } from "../state/use-atom-command";

const SERVICE_WORKER_URL = "/sw.js";
const SERVICE_WORKER_SCOPE = "/";
const DEVICE_ID_STORAGE_KEY = "t3code:pwa-device-id";
const REGISTRATION_REFRESH_MS = 4 * 60 * 1_000;

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

function canUseWebPush(): boolean {
  return canRegisterServiceWorker() && "PushManager" in window && "Notification" in window;
}

async function registerServiceWorker(): Promise<ServiceWorkerRegistration> {
  await navigator.serviceWorker.register(SERVICE_WORKER_URL, {
    scope: SERVICE_WORKER_SCOPE,
  });
  return await navigator.serviceWorker.ready;
}

function resolveDeviceId(): string {
  const existing = window.localStorage.getItem(DEVICE_ID_STORAGE_KEY);
  if (existing) return existing;

  const generated = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  window.localStorage.setItem(DEVICE_ID_STORAGE_KEY, generated);
  return generated;
}

function urlBase64ToArrayBuffer(value: string): ArrayBuffer {
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  const base64 = `${value}${padding}`.replace(/-/g, "+").replace(/_/g, "/");
  const raw = window.atob(base64);
  const output = new Uint8Array(new ArrayBuffer(raw.length));
  for (let index = 0; index < raw.length; index++) {
    output[index] = raw.charCodeAt(index);
  }
  return output.buffer;
}

function toServerSubscription(subscription: PushSubscription): ServerWebPushSubscription | null {
  const json = subscription.toJSON();
  if (!json.endpoint || !json.keys?.p256dh || !json.keys.auth) {
    return null;
  }
  return {
    endpoint: json.endpoint,
    expirationTime: json.expirationTime ?? null,
    keys: {
      p256dh: json.keys.p256dh,
      auth: json.keys.auth,
    },
  };
}

function applicationServerKeyMatches(
  subscription: PushSubscription,
  applicationServerKey: ArrayBuffer,
): boolean {
  const existingKey = subscription.options.applicationServerKey;
  if (!existingKey) return false;

  const existing = new Uint8Array(existingKey);
  const next = new Uint8Array(applicationServerKey);
  if (existing.byteLength !== next.byteLength) return false;

  for (let index = 0; index < existing.byteLength; index++) {
    if (existing[index] !== next[index]) return false;
  }
  return true;
}

function resolveNotificationAckUrl(httpBaseUrl: string | null): string | undefined {
  try {
    return new URL("/api/notifications/ack", httpBaseUrl ?? window.location.origin).toString();
  } catch {
    return undefined;
  }
}

function shouldRunPwaRuntime(): boolean {
  return (
    typeof window !== "undefined" &&
    (Boolean(window.desktopBridge) || window.location.protocol !== "t3code:")
  );
}

function deviceLabel(): string {
  if (window.desktopBridge) {
    return "Desktop app";
  }
  const userAgent = navigator.userAgent;
  if (/Android/i.test(userAgent)) return "Android Chrome";
  return "Web app";
}

type DeepLinkHistoryMode = "browser" | "hash";

export function resolveDeepLinkTarget(
  deepLink: string | undefined,
  historyMode: DeepLinkHistoryMode,
): string | null {
  if (!deepLink || !deepLink.startsWith("/") || deepLink.startsWith("//")) {
    return null;
  }
  return historyMode === "hash" ? `#${deepLink}` : deepLink;
}

function openDeepLink(deepLink: string | undefined): void {
  window.focus();
  const target = resolveDeepLinkTarget(deepLink, isElectron ? "hash" : "browser");
  if (target) {
    window.location.assign(target);
  }
}

export function PwaRuntime() {
  const environmentId = usePrimaryEnvironmentId();
  return <PwaRuntimeView enabled={shouldRunPwaRuntime()} environmentId={environmentId ?? null} />;
}

export function PwaRuntimeView({
  enabled,
  environmentId,
}: {
  readonly enabled: boolean;
  readonly environmentId: EnvironmentId | null;
}) {
  if (!enabled) {
    return null;
  }

  return (
    <>
      <PwaServiceWorkerRegistration />
      {environmentId ? <PwaRuntimeForEnvironment environmentId={environmentId} /> : null}
    </>
  );
}

export function PwaServiceWorkerRegistration() {
  useEffect(() => {
    if (!canRegisterServiceWorker()) return;
    void registerServiceWorker().catch(() => undefined);
  }, []);

  return null;
}

function PwaRuntimeForEnvironment({ environmentId }: { readonly environmentId: EnvironmentId }) {
  const httpBaseUrl = useEnvironmentHttpBaseUrl(environmentId);
  const notificationConfigResult = useAtomValue(
    serverEnvironment.notificationConfig({ environmentId, input: {} }),
  );
  const notificationConfig = Option.getOrNull(AsyncResult.value(notificationConfigResult));
  const notificationEventResult = useAtomValue(
    serverEnvironment.notificationEvents({ environmentId, input: {} }),
  );
  const notificationEvent = Option.getOrNull(AsyncResult.value(notificationEventResult));
  const registerDevice = useAtomCommand(serverEnvironment.registerNotificationDevice, {
    reportFailure: false,
  });
  const ackNotification = useAtomCommand(serverEnvironment.ackNotification, {
    reportFailure: false,
  });
  const activeNotifications = useRef(new Map<string, Notification>());
  const suppressedCloseAcks = useRef(new Set<string>());
  const lastEventRef = useRef<ServerNotificationStreamEvent | null>(null);

  const acknowledge = useCallback(
    (notificationId: string, action: "opened" | "dismissed" | "closed") => {
      void ackNotification({
        environmentId,
        input: {
          notificationId,
          action,
        },
      });
    },
    [ackNotification, environmentId],
  );

  const showDesktopNotification = useCallback(
    (notification: ServerDeviceNotification) => {
      if (
        !window.desktopBridge ||
        !("Notification" in window) ||
        Notification.permission !== "granted"
      ) {
        return;
      }
      const existing = activeNotifications.current.get(notification.notificationId);
      if (existing) {
        existing.close();
      }

      const rendered = new Notification(notification.title, {
        body: notification.body ?? "",
        tag: `t3:${notification.notificationId}`,
        data: notification,
        icon: "/pwa-icon-192.png",
        requireInteraction: notification.requireInteraction,
      });
      activeNotifications.current.set(notification.notificationId, rendered);
      rendered.addEventListener("click", () => {
        suppressedCloseAcks.current.add(notification.notificationId);
        acknowledge(notification.notificationId, "opened");
        rendered.close();
        openDeepLink(notification.deepLink);
      });
      rendered.addEventListener("close", () => {
        activeNotifications.current.delete(notification.notificationId);
        if (suppressedCloseAcks.current.delete(notification.notificationId)) {
          return;
        }
        acknowledge(notification.notificationId, "closed");
      });
    },
    [acknowledge],
  );

  useEffect(() => {
    if (!notificationEvent || notificationEvent === lastEventRef.current) {
      return;
    }
    lastEventRef.current = notificationEvent;
    if (notificationEvent.type === "show") {
      showDesktopNotification(notificationEvent.notification);
      return;
    }

    suppressedCloseAcks.current.add(notificationEvent.notificationId);
    activeNotifications.current.get(notificationEvent.notificationId)?.close();
    activeNotifications.current.delete(notificationEvent.notificationId);
  }, [notificationEvent, showDesktopNotification]);

  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      if (event.data?.type !== "t3-notification-open") return;
      openDeepLink(typeof event.data.deepLink === "string" ? event.data.deepLink : undefined);
    };
    navigator.serviceWorker?.addEventListener("message", handleMessage);
    return () => {
      navigator.serviceWorker?.removeEventListener("message", handleMessage);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    let refreshTimer: ReturnType<typeof setInterval> | null = null;
    let removePermissionGestureListener: (() => void) | null = null;
    let pendingPermissionRequest: Promise<NotificationPermission> | null = null;
    let resolvePendingPermission: ((permission: NotificationPermission) => void) | null = null;

    const clearPermissionGestureListener = () => {
      removePermissionGestureListener?.();
      removePermissionGestureListener = null;
    };

    async function requestPermissionAfterGesture(): Promise<NotificationPermission> {
      if (!("Notification" in window)) return "denied";
      if (Notification.permission !== "default") return Notification.permission;
      if (pendingPermissionRequest) return await pendingPermissionRequest;

      pendingPermissionRequest = new Promise<NotificationPermission>((resolve) => {
        resolvePendingPermission = resolve;
        const request = () => {
          clearPermissionGestureListener();
          void Notification.requestPermission().then(
            (permission) => {
              pendingPermissionRequest = null;
              resolvePendingPermission = null;
              resolve(permission);
            },
            () => {
              pendingPermissionRequest = null;
              resolvePendingPermission = null;
              resolve(Notification.permission);
            },
          );
        };
        window.addEventListener("pointerdown", request, { once: true, passive: true });
        window.addEventListener("keydown", request, { once: true });
        removePermissionGestureListener = () => {
          window.removeEventListener("pointerdown", request);
          window.removeEventListener("keydown", request);
        };
      });
      return await pendingPermissionRequest;
    }

    async function registerDesktop(): Promise<void> {
      if (!("Notification" in window)) return;
      const permission = await requestPermissionAfterGesture();
      if (cancelled || permission !== "granted") return;

      await registerDevice({
        environmentId,
        input: {
          deviceId: resolveDeviceId(),
          deviceKind: "desktop",
          deviceLabel: deviceLabel(),
          userAgent: navigator.userAgent,
        },
      });
    }

    async function registerWebPush(vapidPublicKey: string): Promise<void> {
      if (!canUseWebPush()) return;
      const registration = await registerServiceWorker();

      const permission = await requestPermissionAfterGesture();
      if (cancelled || permission !== "granted") return;

      const applicationServerKey = urlBase64ToArrayBuffer(vapidPublicKey);
      let existing = await registration.pushManager.getSubscription();
      if (existing && !applicationServerKeyMatches(existing, applicationServerKey)) {
        await existing.unsubscribe().catch(() => undefined);
        existing = null;
      }
      const subscription =
        existing ??
        (await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey,
        }));
      const serverSubscription = toServerSubscription(subscription);
      if (!serverSubscription) return;
      const ackUrl = resolveNotificationAckUrl(httpBaseUrl);

      await registerDevice({
        environmentId,
        input: {
          deviceId: resolveDeviceId(),
          deviceKind: "web-push",
          deviceLabel: deviceLabel(),
          userAgent: navigator.userAgent,
          ...(ackUrl === undefined ? {} : { ackUrl }),
          subscription: serverSubscription,
        },
      });
    }

    const refreshRegistration = () => {
      if (window.desktopBridge) {
        void registerDesktop();
        return;
      }
      if (notificationConfig) {
        void registerWebPush(notificationConfig.vapidPublicKey);
      }
    };

    refreshRegistration();
    refreshTimer = setInterval(refreshRegistration, REGISTRATION_REFRESH_MS);
    return () => {
      cancelled = true;
      if (refreshTimer) clearInterval(refreshTimer);
      clearPermissionGestureListener();
      resolvePendingPermission?.("denied");
      pendingPermissionRequest = null;
      resolvePendingPermission = null;
    };
  }, [environmentId, httpBaseUrl, notificationConfig, registerDevice]);

  useEffect(() => {
    const navigatorWithWakeLock = navigator as NavigatorWithWakeLock;
    if (!navigatorWithWakeLock.wakeLock) return;

    let cancelled = false;
    let requestInFlight = false;
    let sentinel: WakeLockSentinelLike | null = null;

    const release = () => {
      const current = sentinel;
      sentinel = null;
      if (current && !current.released) {
        void current.release().catch(() => undefined);
      }
    };

    const acquire = async () => {
      if (cancelled || document.visibilityState !== "visible" || sentinel || requestInFlight)
        return;
      requestInFlight = true;
      try {
        const nextSentinel = await navigatorWithWakeLock.wakeLock?.request("screen");
        if (!nextSentinel) return;
        if (cancelled || document.visibilityState !== "visible" || sentinel) {
          void nextSentinel.release().catch(() => undefined);
          return;
        }

        sentinel = nextSentinel;
        nextSentinel.addEventListener("release", () => {
          if (sentinel === nextSentinel) {
            sentinel = null;
          }
        });
      } catch {
        sentinel = null;
      } finally {
        requestInFlight = false;
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
