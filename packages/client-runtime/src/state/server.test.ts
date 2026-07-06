import { type ServerConfig, type ServerLifecycleWelcomePayload } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Option from "effect/Option";

import {
  applyServerConfigProjection,
  projectServerConfig,
  projectServerWelcome,
} from "./server.ts";

const CONFIG = {
  availableEditors: [],
  issues: [],
  keybindings: {},
  keybindingsConfigPath: null,
  observability: null,
  providers: [],
  settings: {},
} as unknown as ServerConfig;

describe("server state projection", () => {
  it("applies every config category to the projected snapshot", () => {
    const snapshot = applyServerConfigProjection(Option.none(), {
      version: 1,
      type: "snapshot",
      config: CONFIG,
    });
    const settings = { ...CONFIG.settings };
    const projected = applyServerConfigProjection(snapshot, {
      version: 1,
      type: "settingsUpdated",
      payload: { settings },
    });

    const result = Option.getOrThrow(projected);
    expect(result.config.settings).toBe(settings);
    expect(result.latestEvent.type).toBe("settingsUpdated");
  });

  it("keeps websocket heartbeat frames out of the projected server config", () => {
    const snapshot = applyServerConfigProjection(Option.none(), {
      version: 1,
      type: "snapshot",
      config: CONFIG,
    });

    expect(
      applyServerConfigProjection(snapshot, {
        version: 1,
        type: "heartbeat",
      }),
    ).toBe(snapshot);
  });

  it("emits no downstream projection for websocket heartbeat frames", () => {
    const [snapshotState] = projectServerConfig(Option.none(), {
      version: 1,
      type: "snapshot",
      config: CONFIG,
    });

    const [nextState, emitted] = projectServerConfig(snapshotState, {
      version: 1,
      type: "heartbeat",
    });

    // State is preserved but nothing is pushed to subscribers, so a quiet
    // session does not re-emit the unchanged config every keepalive tick.
    expect(nextState).toBe(snapshotState);
    expect(emitted).toEqual([]);
  });

  it("retains welcome when a ready event follows in the same stream chunk", () => {
    const welcome = {
      environment: {} as ServerLifecycleWelcomePayload["environment"],
      cwd: "/repo",
      projectName: "repo",
    } as ServerLifecycleWelcomePayload;
    const [afterWelcome] = projectServerWelcome(Option.none(), {
      type: "welcome",
      payload: welcome,
    });
    const [afterReady, emitted] = projectServerWelcome(afterWelcome, {
      type: "ready",
      payload: {},
    });

    expect(Option.getOrThrow(afterReady)).toBe(welcome);
    expect(emitted).toEqual([]);
  });
});
