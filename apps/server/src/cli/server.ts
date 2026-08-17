import * as Effect from "effect/Effect";
import { Command, GlobalFlag } from "effect/unstable/cli";

import * as Layer from "effect/Layer";
import { ServerConfig, type StartupPresentation } from "../config.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import { runServer } from "../server.ts";
import { type CliServerFlags, resolveServerConfig, sharedServerCommandFlags } from "./config.ts";

export const runServerCommand = (
  flags: CliServerFlags,
  options?: {
    readonly startupPresentation?: StartupPresentation;
    readonly forceAutoBootstrapProjectFromCwd?: boolean;
  },
) =>
  Effect.gen(function* () {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveServerConfig(flags, logLevel, options);
    const configLayer = Layer.succeed(ServerConfig, config);
    return yield* runServer.pipe(
      Effect.provide(
        Layer.mergeAll(configLayer, ServerSecretStore.layer.pipe(Layer.provide(configLayer))),
      ),
    );
  });

export const startCommand = Command.make("start", { ...sharedServerCommandFlags }).pipe(
  Command.withDescription("Run the T3 Code server."),
  Command.withHandler((flags) => runServerCommand(flags)),
);

export const serveCommand = Command.make("serve", { ...sharedServerCommandFlags }).pipe(
  Command.withDescription(
    "Run the T3 Code server without opening a browser and print headless pairing details.",
  ),
  Command.withHandler((flags) =>
    runServerCommand(flags, {
      startupPresentation: "headless",
      forceAutoBootstrapProjectFromCwd: false,
    }),
  ),
);
