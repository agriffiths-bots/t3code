import {
  AuthRelayReadScope,
  type AuthAudienceCeiling,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";

// Until event/stream filtering lands, factory sessions may only inspect relay
// connectivity. An allowlist keeps newly-added scopes denied by default.
const FACTORY_AUDIENCE_ALLOWED_SCOPES = new Set<AuthEnvironmentScope>([AuthRelayReadScope]);

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

export function restrictScopesForAudienceCeiling(
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  audienceCeiling: AuthAudienceCeiling,
): ReadonlyArray<AuthEnvironmentScope> {
  return audienceCeiling === "private"
    ? scopes
    : scopes.filter((scope) => isScopeAllowedForAudienceCeiling(scope, audienceCeiling));
}
