import { assert, it } from "@effect/vitest";

import {
  assertHostedBuildEnvClean,
  findConfiguredBackendEnv,
  HOSTED_BACKEND_ENV_KEYS,
  HostedBuildEnvError,
  isHostedBuild,
} from "./hosted-build.ts";

it("detects an explicit hosted-build flag or the fail-closed package entrypoint", () => {
  assert.isTrue(isHostedBuild({ T3CODE_HOSTED_BUILD: "1" }));
  assert.isTrue(isHostedBuild({ T3CODE_HOSTED_BUILD: "true" }));
  assert.isTrue(isHostedBuild({ T3CODE_HOSTED_BUILD: " YES " }));
  assert.isTrue(isHostedBuild({ npm_lifecycle_event: "build:hosted" }));
  assert.isFalse(isHostedBuild({ npm_lifecycle_event: "build" }));
  assert.isFalse(isHostedBuild({ T3CODE_HOSTED_BUILD: "0" }));
  assert.isFalse(isHostedBuild({ T3CODE_HOSTED_BUILD: "" }));
  assert.isFalse(isHostedBuild({}));
});

it("treats empty and unset backend variables as clean", () => {
  assert.deepEqual(findConfiguredBackendEnv({}), []);
  assert.deepEqual(findConfiguredBackendEnv({ VITE_HTTP_URL: "", VITE_WS_URL: "   " }), []);
  // Must not throw.
  assertHostedBuildEnvClean({ VITE_HTTP_URL: "", VITE_WS_URL: undefined });
});

it("flags every configured backend endpoint", () => {
  const offenders = findConfiguredBackendEnv({
    VITE_HTTP_URL: "http://127.0.0.1:15773",
    VITE_WS_URL: "ws://127.0.0.1:15773",
    VITE_DEV_SERVER_URL: "http://127.0.0.1:5733",
    UNRELATED: "keep",
  });
  assert.deepEqual(offenders.sort(), [...HOSTED_BACKEND_ENV_KEYS].sort());
});

it("rejects a backend endpoint inherited from the process environment", () => {
  // The 2026-07-22 vector was an INHERITED process env var (a desktop/dev worker
  // exported VITE_HTTP_URL=http://127.0.0.1:15773), NOT a repo env-file entry.
  // The vite config now asserts on process.env, so a process-env-shaped object
  // with the offender must throw rather than be silently scrubbed.
  const processEnvShaped: NodeJS.ProcessEnv = {
    PATH: "/usr/bin",
    HOME: "/home/adam",
    VITE_WS_URL: "ws://127.0.0.1:15773",
  };
  assert.throws(() => assertHostedBuildEnvClean(processEnvShaped), HostedBuildEnvError);
});

it("throws HostedBuildEnvError listing only offender names and redacting values", () => {
  const sensitiveEndpoint = "https://user:password@example.test/api?token=secret";
  try {
    assertHostedBuildEnvClean({ VITE_HTTP_URL: sensitiveEndpoint });
    assert.fail("expected assertHostedBuildEnvClean to throw");
  } catch (error) {
    assert.instanceOf(error, HostedBuildEnvError);
    const hostedError = error as HostedBuildEnvError;
    assert.include(hostedError.message, "VITE_HTTP_URL");
    assert.notInclude(hostedError.message, sensitiveEndpoint);
    assert.deepEqual(hostedError.offenders, ["VITE_HTTP_URL"]);
  }
});
