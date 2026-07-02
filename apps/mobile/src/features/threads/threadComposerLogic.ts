import type { OrchestrationSession } from "@t3tools/contracts";

type ComposerSessionState = Pick<OrchestrationSession, "status" | "activeTurnId">;

export function shouldShowThreadComposerStopAction(
  session: ComposerSessionState | null | undefined,
): boolean {
  return (
    session?.status === "running" ||
    session?.status === "starting" ||
    (session?.status === "waiting" && session.activeTurnId !== null)
  );
}
