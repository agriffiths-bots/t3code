import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

type ElectronUpdaterModule = typeof import("electron-updater");
type ElectronUpdaterModuleNamespace = Partial<ElectronUpdaterModule> & {
  readonly default?: Partial<ElectronUpdaterModule>;
};
type AutoUpdater = ElectronUpdaterModule["autoUpdater"];

export type ElectronUpdaterFeedUrl = Parameters<AutoUpdater["setFeedURL"]>[0];

let autoUpdaterInstance: AutoUpdater | undefined;

function formatUnknownError(cause: unknown): string {
  return cause instanceof Error ? (cause.stack ?? cause.message) : String(cause);
}

export function resolveAutoUpdaterModule(module: ElectronUpdaterModuleNamespace): AutoUpdater {
  const autoUpdater = module.autoUpdater ?? module.default?.autoUpdater;
  if (autoUpdater === undefined) {
    throw new Error("electron-updater module did not expose autoUpdater.");
  }
  return autoUpdater;
}

const loadAutoUpdater = Effect.promise(async () => {
  if (autoUpdaterInstance !== undefined) {
    return autoUpdaterInstance;
  }
  try {
    const module = await import("electron-updater");
    autoUpdaterInstance = resolveAutoUpdaterModule(module);
    return autoUpdaterInstance;
  } catch (cause) {
    process.stderr.write(
      `fatal startup error: failed to load packaged runtime dependency electron-updater\n${formatUnknownError(cause)}\n`,
    );
    throw cause;
  }
});

export class ElectronUpdaterCheckForUpdatesError extends Schema.TaggedErrorClass<ElectronUpdaterCheckForUpdatesError>()(
  "ElectronUpdaterCheckForUpdatesError",
  {
    channel: Schema.NullOr(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Electron updater failed to check for updates on channel ${this.channel ?? "default"}.`;
  }
}

export class ElectronUpdaterDownloadUpdateError extends Schema.TaggedErrorClass<ElectronUpdaterDownloadUpdateError>()(
  "ElectronUpdaterDownloadUpdateError",
  {
    channel: Schema.NullOr(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Electron updater failed to download the update on channel ${this.channel ?? "default"}.`;
  }
}

export class ElectronUpdaterQuitAndInstallError extends Schema.TaggedErrorClass<ElectronUpdaterQuitAndInstallError>()(
  "ElectronUpdaterQuitAndInstallError",
  {
    channel: Schema.NullOr(Schema.String),
    isSilent: Schema.Boolean,
    isForceRunAfter: Schema.Boolean,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Electron updater failed to quit and install the update on channel ${this.channel ?? "default"} (silent: ${this.isSilent}, force run after: ${this.isForceRunAfter}).`;
  }
}

export const ElectronUpdaterError = Schema.Union([
  ElectronUpdaterCheckForUpdatesError,
  ElectronUpdaterDownloadUpdateError,
  ElectronUpdaterQuitAndInstallError,
]);
export type ElectronUpdaterError = typeof ElectronUpdaterError.Type;
export const isElectronUpdaterError = Schema.is(ElectronUpdaterError);

export class ElectronUpdater extends Context.Service<
  ElectronUpdater,
  {
    readonly setFeedURL: (options: ElectronUpdaterFeedUrl) => Effect.Effect<void>;
    readonly setAutoDownload: (value: boolean) => Effect.Effect<void>;
    readonly setAutoInstallOnAppQuit: (value: boolean) => Effect.Effect<void>;
    readonly setChannel: (channel: string) => Effect.Effect<void>;
    readonly setAllowPrerelease: (value: boolean) => Effect.Effect<void>;
    readonly allowDowngrade: Effect.Effect<boolean>;
    readonly setAllowDowngrade: (value: boolean) => Effect.Effect<void>;
    readonly setDisableDifferentialDownload: (value: boolean) => Effect.Effect<void>;
    readonly verifyAvailable: Effect.Effect<void>;
    readonly checkForUpdates: Effect.Effect<void, ElectronUpdaterCheckForUpdatesError>;
    readonly downloadUpdate: Effect.Effect<void, ElectronUpdaterDownloadUpdateError>;
    readonly quitAndInstall: (options: {
      readonly isSilent: boolean;
      readonly isForceRunAfter: boolean;
    }) => Effect.Effect<void, ElectronUpdaterQuitAndInstallError>;
    readonly on: <Args extends ReadonlyArray<unknown>>(
      eventName: string,
      listener: (...args: Args) => void,
    ) => Effect.Effect<void, never, Scope.Scope>;
  }
>()("@t3tools/desktop/electron/ElectronUpdater") {}

export const make = ElectronUpdater.of({
  setFeedURL: (options) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) =>
        Effect.sync(() => {
          autoUpdater.setFeedURL(options);
        }),
      ),
    ),
  setAutoDownload: (value) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) =>
        Effect.sync(() => {
          autoUpdater.autoDownload = value;
        }),
      ),
    ),
  setAutoInstallOnAppQuit: (value) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) =>
        Effect.sync(() => {
          autoUpdater.autoInstallOnAppQuit = value;
        }),
      ),
    ),
  setChannel: (channel) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) =>
        Effect.sync(() => {
          autoUpdater.channel = channel;
        }),
      ),
    ),
  setAllowPrerelease: (value) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) =>
        Effect.sync(() => {
          autoUpdater.allowPrerelease = value;
        }),
      ),
    ),
  allowDowngrade: loadAutoUpdater.pipe(Effect.map((autoUpdater) => autoUpdater.allowDowngrade)),
  setAllowDowngrade: (value) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) =>
        Effect.sync(() => {
          autoUpdater.allowDowngrade = value;
        }),
      ),
    ),
  setDisableDifferentialDownload: (value) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) =>
        Effect.sync(() => {
          autoUpdater.disableDifferentialDownload = value;
        }),
      ),
    ),
  verifyAvailable: loadAutoUpdater.pipe(Effect.asVoid),
  checkForUpdates: loadAutoUpdater.pipe(
    Effect.flatMap((autoUpdater) => {
      const channel = autoUpdater.channel;
      return Effect.tryPromise({
        try: () => autoUpdater.checkForUpdates(),
        catch: (cause) => new ElectronUpdaterCheckForUpdatesError({ channel, cause }),
      }).pipe(Effect.asVoid);
    }),
  ),
  downloadUpdate: loadAutoUpdater.pipe(
    Effect.flatMap((autoUpdater) => {
      const channel = autoUpdater.channel;
      return Effect.tryPromise({
        try: () => autoUpdater.downloadUpdate(),
        catch: (cause) => new ElectronUpdaterDownloadUpdateError({ channel, cause }),
      }).pipe(Effect.asVoid);
    }),
  ),
  quitAndInstall: ({ isSilent, isForceRunAfter }) =>
    loadAutoUpdater.pipe(
      Effect.flatMap((autoUpdater) => {
        const channel = autoUpdater.channel;
        return Effect.try({
          try: () => autoUpdater.quitAndInstall(isSilent, isForceRunAfter),
          catch: (cause) =>
            new ElectronUpdaterQuitAndInstallError({
              channel,
              isSilent,
              isForceRunAfter,
              cause,
            }),
        });
      }),
    ),
  on: (eventName, listener) =>
    Effect.acquireRelease(
      loadAutoUpdater.pipe(
        Effect.map((autoUpdater) => {
          const eventTarget = autoUpdater as unknown as {
            on: (eventName: string, listener: (...args: Array<unknown>) => void) => void;
            removeListener: (
              eventName: string,
              listener: (...args: Array<unknown>) => void,
            ) => void;
          };
          const untypedListener = listener as unknown as (...args: Array<unknown>) => void;
          eventTarget.on(eventName, untypedListener);
          return { eventTarget, untypedListener };
        }),
      ),
      ({ eventTarget, untypedListener }) =>
        Effect.sync(() => {
          eventTarget.removeListener(eventName, untypedListener);
        }),
    ).pipe(Effect.asVoid),
});

export const layer = Layer.succeed(ElectronUpdater, make);
