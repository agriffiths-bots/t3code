import type { DesktopNotificationActionEvent } from "@t3tools/contracts";

const MAX_PENDING_NOTIFICATION_ACTIONS = 32;

export function createDesktopNotificationActionBuffer() {
  const listeners = new Set<(event: DesktopNotificationActionEvent) => void>();
  const pending: DesktopNotificationActionEvent[] = [];

  const dispatch = (event: DesktopNotificationActionEvent) => {
    if (listeners.size === 0) {
      pending.push(event);
      while (pending.length > MAX_PENDING_NOTIFICATION_ACTIONS) {
        pending.shift();
      }
      return;
    }

    for (const listener of listeners) {
      listener(event);
    }
  };

  const subscribe = (listener: (event: DesktopNotificationActionEvent) => void) => {
    listeners.add(listener);
    for (const event of pending.splice(0)) {
      listener(event);
    }
    return () => {
      listeners.delete(listener);
    };
  };

  return {
    dispatch,
    subscribe,
    hasListeners: () => listeners.size > 0,
  };
}
