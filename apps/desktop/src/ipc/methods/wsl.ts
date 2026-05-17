import {
  DesktopWslModeSchema,
  DesktopWslStateSchema,
  type DesktopWslState,
} from "@t3tools/contracts";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as DesktopBackendPool from "../../backend/DesktopBackendPool.ts";
import * as DesktopAppSettings from "../../settings/DesktopAppSettings.ts";
import * as DesktopWslEnvironment from "../../wsl/DesktopWslEnvironment.ts";
import * as IpcChannels from "../channels.ts";
import { makeIpcMethod } from "../DesktopIpc.ts";

// Cap how long we wait for the new backend to come up before rolling back to
// the previous mode. Generous enough to cover cold WSL boots (VM spin-up,
// initial wslhost forwarding handshake) and node-pty preparation on a fresh
// distro; tight enough that a truly stuck swap doesn't strand the user.
const SWAP_READINESS_TIMEOUT = Duration.minutes(2);

const SetWslBackendInput = Schema.Struct({
  mode: DesktopWslModeSchema,
  distro: Schema.NullOr(Schema.String),
});

class WslBackendSwapError extends Data.TaggedError("WslBackendSwapError")<{
  readonly message: string;
}> {}

const readWslState: Effect.Effect<
  DesktopWslState,
  never,
  DesktopAppSettings.DesktopAppSettings | DesktopWslEnvironment.DesktopWslEnvironment
> = Effect.gen(function* () {
  const appSettings = yield* DesktopAppSettings.DesktopAppSettings;
  const wslEnvironment = yield* DesktopWslEnvironment.DesktopWslEnvironment;
  const settings = yield* appSettings.get;
  const available = yield* wslEnvironment.isAvailable;
  // Only enumerate distros when WSL is actually available — listDistros on a
  // non-WSL host would spawn wsl.exe and hit the timeout for nothing.
  const distros = available ? yield* wslEnvironment.listDistros : [];
  return {
    mode: settings.wslBackendEnabled ? "wsl" : "local",
    distro: settings.wslDistro,
    available,
    distros,
  };
});

export const getWslState = makeIpcMethod({
  channel: IpcChannels.GET_WSL_STATE_CHANNEL,
  payload: Schema.Void,
  result: DesktopWslStateSchema,
  handler: Effect.fn("desktop.ipc.wsl.getState")(function* () {
    return yield* readWslState;
  }),
});

export const setWslBackend = makeIpcMethod({
  channel: IpcChannels.SET_WSL_BACKEND_CHANNEL,
  payload: SetWslBackendInput,
  result: DesktopWslStateSchema,
  handler: Effect.fn("desktop.ipc.wsl.setBackend")(function* (input) {
    const appSettings = yield* DesktopAppSettings.DesktopAppSettings;
    const pool = yield* DesktopBackendPool.DesktopBackendPool;
    const primaryBackend = yield* pool.primary;
    const wslEnvironment = yield* DesktopWslEnvironment.DesktopWslEnvironment;

    // Pre-warm the WSL VM before swapping so the new backend boot doesn't
    // race wsl.exe's first-spawn cold start against the HTTP readiness probe.
    if (input.mode === "wsl") {
      yield* wslEnvironment.preWarm(input.distro);
    }

    const previousSettings = yield* appSettings.get;
    const targetEnabled = input.mode === "wsl";
    const enabledChange = yield* appSettings.setWslBackendEnabled(targetEnabled);
    const distroChange = yield* appSettings.setWslDistro(input.distro);

    if (!enabledChange.changed && !distroChange.changed) {
      return yield* readWslState;
    }

    // In-process swap: stop the running backend, then start it again. The
    // backend instance re-resolves config on start, so the new toggle picks
    // up automatically.
    yield* primaryBackend.stop();
    yield* primaryBackend.start;

    // Bounded readiness wait — if the new backend doesn't come up in time
    // (bad distro, missing node-pty, preflight failure that scheduled
    // restarts forever) revert to the previous toggle so the user isn't stuck.
    const ready = yield* primaryBackend.waitForReady(SWAP_READINESS_TIMEOUT);
    if (!ready) {
      yield* appSettings.setWslBackendEnabled(previousSettings.wslBackendEnabled);
      yield* appSettings.setWslDistro(previousSettings.wslDistro);
      yield* primaryBackend.stop();
      yield* primaryBackend.start;
      const rolledBack = yield* primaryBackend.waitForReady(SWAP_READINESS_TIMEOUT);
      const failedTarget = targetEnabled ? "WSL backend" : "local backend";
      return yield* new WslBackendSwapError({
        message: rolledBack
          ? `The ${failedTarget} didn't come up. Rolled back to the previous mode — check that the chosen distro is healthy and try again.`
          : `The ${failedTarget} didn't come up, and the rollback also failed to start. The app is in a degraded state — restart T3 Code to recover.`,
      });
    }

    return yield* readWslState;
  }),
});
