/**
 * The orchestration engine rejects a `project.delete` command whenever the project
 * still has any NON-DELETED thread — archived threads count as non-empty (see
 * apps/server/src/orchestration/decider.ts) — unless `force === true`. The sidebar's
 * visible-thread count excludes archived threads, so an archived-only project looks
 * empty in the UI and is deleted WITHOUT force, surfacing this invariant as a raw
 * error toast. Detect that specific failure so the UI can re-prompt with a force
 * confirmation (engine as the source of truth for emptiness).
 *
 * Robust to both shapes the failure can arrive in: the decoded tagged error
 * (`_tag === "OrchestrationCommandInvariantError"`, `commandType === "project.delete"`)
 * and a plain Error/string carrying the invariant message.
 *
 * NOTE: the dispatchCommand RPC re-wraps the engine invariant server-side as an
 * `OrchestrationDispatchCommandError` (a different `_tag`) whose `.message` carries
 * the full invariant text, so in practice the MESSAGE-pattern branch is the
 * load-bearing one and the tagged-error branch is defensive. Do NOT tighten this to
 * require the tag — it would stop matching the error the client actually receives.
 */
export function isProjectNotEmptyInvariant(error: unknown): boolean {
  if (error !== null && typeof error === "object") {
    const e = error as {
      readonly _tag?: unknown;
      readonly commandType?: unknown;
      readonly detail?: unknown;
      readonly message?: unknown;
    };
    if (e._tag === "OrchestrationCommandInvariantError" && e.commandType === "project.delete") {
      return true;
    }
    const text =
      typeof e.detail === "string" ? e.detail : typeof e.message === "string" ? e.message : "";
    if (PROJECT_NOT_EMPTY_PATTERN.test(text)) {
      return true;
    }
  }
  return PROJECT_NOT_EMPTY_PATTERN.test(String(error ?? ""));
}

const PROJECT_NOT_EMPTY_PATTERN = /is not empty and cannot be deleted/i;
