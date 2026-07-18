import {
  EnvironmentAuthenticatedPrincipal,
  type AuthAudienceCeiling,
  type DataAudience,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

/**
 * Internal work runs without an authenticated principal and retains the
 * unrestricted/private view. Request handlers provide a principal so the same
 * repository boundary can narrow every project/thread read consistently.
 *
 * Projection queries and event/stream delivery consume this same ceiling so
 * every read surface applies one audience ordering rule.
 */
export const currentReadAudienceCeiling: Effect.Effect<AuthAudienceCeiling> = Effect.map(
  Effect.serviceOption(EnvironmentAuthenticatedPrincipal),
  Option.match({
    onNone: () => "private" as const,
    onSome: (principal) => principal.audienceCeiling,
  }),
);

export function canReadDataAudience(
  audienceCeiling: AuthAudienceCeiling,
  dataAudience: DataAudience,
): boolean {
  return audienceCeiling === "private" || dataAudience === "factory";
}
