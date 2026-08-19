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

/**
 * The key covers the host as well as the versions: the crypto package ships a
 * native binding, and its downloader picks a different artifact per platform,
 * architecture and C library. A cache shared or mounted across machines would
 * otherwise hand one host's binding to another and fail to load before either
 * mode could start. Linux x64 alone has separate GNU and musl artifacts.
 */
/** glibc reports a runtime version here; musl builds do not. */
function libcTag() {
  try {
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone e2e harness has no Effect runtime.
    return process.report.getReport().header.glibcVersionRuntime ? "glibc" : "musl";
  } catch {
    return "unknown-libc";
  }
}

export function sdkCacheDir() {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone e2e harness has no Effect runtime.
  const host = [process.platform, process.arch, libcTag()];
  const key = NodeCrypto.createHash("sha256")
    .update([...SDK_PACKAGES, ...host].join("\n"))
    .digest("hex")
    .slice(0, 16);
  return NodePath.join(USER_CACHE, `npm-${key}`);
}
