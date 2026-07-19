import {
  AuthOrchestrationReadScope,
  AuthRelayReadScope,
  ORCHESTRATION_WS_METHODS,
  WS_METHODS,
  type AuthAudienceCeiling,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";

// Read delivery is audience-scoped at the projection/event boundaries. Command
// and capability scopes stay denied until their dedicated guards land. An
// allowlist keeps newly-added scopes denied by default.
const FACTORY_AUDIENCE_ALLOWED_SCOPES = new Set<AuthEnvironmentScope>([
  AuthOrchestrationReadScope,
  AuthRelayReadScope,
]);

const FACTORY_AUDIENCE_ALLOWED_ORCHESTRATION_READ_RPC_METHODS = new Set<string>([
  ORCHESTRATION_WS_METHODS.getTurnDiff,
  ORCHESTRATION_WS_METHODS.getFullThreadDiff,
  ORCHESTRATION_WS_METHODS.replayEvents,
  ORCHESTRATION_WS_METHODS.subscribeShell,
  ORCHESTRATION_WS_METHODS.getArchivedShellSnapshot,
  ORCHESTRATION_WS_METHODS.subscribeThread,
  ORCHESTRATION_WS_METHODS.subscribeScheduledTasks,
  WS_METHODS.subscribeNotificationEvents,
  WS_METHODS.serverAckNotification,
]);

export function canNarrowAudienceCeiling(
  source: AuthAudienceCeiling,
  requested: AuthAudienceCeiling,
): boolean {
  return source === "private" || requested === "factory";
}

export function isScopeAllowedForAudienceCeiling(
  scope: AuthEnvironmentScope,
  audienceCeiling: AuthAudienceCeiling,
): boolean {
  return audienceCeiling === "private" || FACTORY_AUDIENCE_ALLOWED_SCOPES.has(scope);
}

export function isAudienceScopedReadRpcMethod(method: string): boolean {
  return FACTORY_AUDIENCE_ALLOWED_ORCHESTRATION_READ_RPC_METHODS.has(method);
}

export function isRpcMethodAllowedForAudienceCeiling(
  method: string,
  scope: AuthEnvironmentScope,
  audienceCeiling: AuthAudienceCeiling,
): boolean {
  return (
    isScopeAllowedForAudienceCeiling(scope, audienceCeiling) &&
    (audienceCeiling === "private" ||
      scope !== AuthOrchestrationReadScope ||
      isAudienceScopedReadRpcMethod(method))
  );
}

export function restrictScopesForAudienceCeiling(
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  audienceCeiling: AuthAudienceCeiling,
): ReadonlyArray<AuthEnvironmentScope> {
  return audienceCeiling === "private"
    ? scopes
    : scopes.filter((scope) => isScopeAllowedForAudienceCeiling(scope, audienceCeiling));
}
