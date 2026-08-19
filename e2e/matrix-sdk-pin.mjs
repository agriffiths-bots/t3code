// The Matrix SDK pin, shared by the orchestrator that installs it and the CLI
// client that loads it. Both derive the cache directory from the same pin, so
// the client cannot resolve a tree left behind by an earlier pin: a stale
// `matrix-bot-sdk` or native crypto binding would defeat the pin and produce
// misleading E2E behaviour.

import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

export const SDK_PACKAGES = ["matrix-bot-sdk@0.8.0", "@matrix-org/matrix-sdk-crypto-nodejs@0.4.0"];

/** Downloaded fixtures are expensive and immutable, so they persist per user. */
export const USER_CACHE = NodePath.join(NodeOS.homedir(), ".cache", "t3-matrix-e2e");

export function sdkCacheDir() {
  const key = NodeCrypto.createHash("sha256")
    .update(SDK_PACKAGES.join("\n"))
    .digest("hex")
    .slice(0, 16);
  return NodePath.join(USER_CACHE, `npm-${key}`);
}
