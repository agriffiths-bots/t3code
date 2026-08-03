// Hosted-build safety helpers.
//
// The 2026-07-22 outage was caused by a hosted web bundle that inherited
// desktop/dev Vite endpoint variables (VITE_HTTP_URL=http://127.0.0.1:15773,
// VITE_WS_URL=ws://127.0.0.1:15773). Vite inlines VITE_*-prefixed values from
// the build environment into `import.meta.env.*`, so those loopback URLs were
// compiled into multiple emitted JS chunks and every browser tried to reach a
// desktop-only loopback backend.
//
// A hosted build must NEVER carry a configured backend endpoint: the web app
// already derives its backend from `window.location.origin` when none is
// configured (see apps/web/src/environments/primary/target.ts →
// resolveWindowOriginPrimaryTarget). These helpers let the Vite config detect
// hosted builds and fail loudly if any backend-pointing Vite variable is set.

type Environment = Readonly<Record<string, string | undefined>>;

/**
 * Vite variables that, when non-empty, pin the web client to a specific
 * backend (HTTP/WS) or dev server. A hosted build must have all of these
 * empty/unset so the client falls back to `window.location.origin`.
 */
export const HOSTED_BACKEND_ENV_KEYS = [
  "VITE_HTTP_URL",
  "VITE_WS_URL",
  "VITE_DEV_SERVER_URL",
] as const;

export type HostedBackendEnvKey = (typeof HOSTED_BACKEND_ENV_KEYS)[number];

function isTruthyFlag(value: string | undefined): boolean {
  const normalized = value?.trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

/**
 * True when this build is a hosted (server-served, origin-relative) web build.
 * Signalled explicitly by `T3CODE_HOSTED_BUILD=1` so desktop/dev builds — which
 * legitimately pin a loopback backend — are never affected.
 */
export function isHostedBuild(env: Environment): boolean {
  return isTruthyFlag(env.T3CODE_HOSTED_BUILD);
}

/**
 * Returns the backend-pointing Vite variables that are set to a non-empty
 * value in `env`. Empty strings and unset variables are fine (they mean
 * "derive from window.location.origin").
 */
export function findConfiguredBackendEnv(env: Environment): HostedBackendEnvKey[] {
  const offenders: HostedBackendEnvKey[] = [];
  for (const key of HOSTED_BACKEND_ENV_KEYS) {
    const value = env[key]?.trim();
    if (value) {
      offenders.push(key);
    }
  }
  return offenders;
}

export class HostedBuildEnvError extends Error {
  readonly offenders: ReadonlyArray<HostedBackendEnvKey>;
  constructor(offenders: ReadonlyArray<HostedBackendEnvKey>) {
    super(
      `Hosted web build refuses configured backend endpoint variables: ${offenders.join(", ")}. ` +
        `A hosted build must derive its backend from ` +
        `window.location.origin; unset ${HOSTED_BACKEND_ENV_KEYS.join(", ")} before building ` +
        `(the deploy pipeline scrubs these automatically).`,
    );
    this.name = "HostedBuildEnvError";
    this.offenders = offenders;
  }
}

/**
 * Throws {@link HostedBuildEnvError} if `env` pins any backend endpoint. Call
 * this before Vite inlines `import.meta.env.*` for a hosted build.
 */
export function assertHostedBuildEnvClean(env: Environment): void {
  const offenders = findConfiguredBackendEnv(env);
  if (offenders.length > 0) {
    throw new HostedBuildEnvError(offenders);
  }
}
