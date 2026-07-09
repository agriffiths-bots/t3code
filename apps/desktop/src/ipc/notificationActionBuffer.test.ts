import { assert, describe, it } from "@effect/vitest";
import type { DesktopNotificationActionEvent } from "@t3tools/contracts";

import {
  createDesktopNotificationActionBuffer,
  createDesktopNotificationActionDrainScheduler,
} from "./notificationActionBuffer.ts";

function action(index: number): DesktopNotificationActionEvent {
  return {
    notificationId: `notification-${index}`,
    action: "opened",
    deepLink: `/thread/${index}`,
  };
}

describe("createDesktopNotificationActionBuffer", () => {
  it("delivers actions immediately after a listener is registered", () => {
    const buffer = createDesktopNotificationActionBuffer();
    const received: DesktopNotificationActionEvent[] = [];

    assert.equal(buffer.hasListeners(), false);
    buffer.subscribe((event) => received.push(event));
    assert.equal(buffer.hasListeners(), true);
    buffer.dispatch(action(1));

    assert.deepEqual(received, [action(1)]);
  });

  it("replays actions received before the listener registers", () => {
    const buffer = createDesktopNotificationActionBuffer();
    const received: DesktopNotificationActionEvent[] = [];

    buffer.dispatch(action(1));
    buffer.dispatch(action(2));
    buffer.subscribe((event) => received.push(event));

    assert.deepEqual(received, [action(1), action(2)]);
  });

  it("bounds pending actions while no listener is registered", () => {
    const buffer = createDesktopNotificationActionBuffer();
    const received: DesktopNotificationActionEvent[] = [];

    for (let index = 0; index < 40; index++) {
      buffer.dispatch(action(index));
    }
    buffer.subscribe((event) => received.push(event));

    assert.equal(received.length, 32);
    assert.equal(received[0]?.notificationId, "notification-8");
    assert.equal(received.at(-1)?.notificationId, "notification-39");
  });
});

describe("createDesktopNotificationActionDrainScheduler", () => {
  it("coalesces concurrent drain requests into a serialized rerun", async () => {
    const drainResolutions: Array<() => void> = [];
    let drainCalls = 0;
    const scheduler = createDesktopNotificationActionDrainScheduler({
      hasListeners: () => true,
      drain: () =>
        new Promise<void>((resolve) => {
          drainCalls += 1;
          drainResolutions.push(resolve);
        }),
    });

    scheduler.request();
    scheduler.request();

    assert.equal(drainCalls, 1);
    drainResolutions[0]?.();
    await Promise.resolve();

    assert.equal(drainCalls, 2);
    drainResolutions[1]?.();
    await Promise.resolve();

    assert.equal(drainCalls, 2);
  });
});
