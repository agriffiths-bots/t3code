import {
  type AuthSessionId,
  type EnvironmentId,
  PreviewAutomationUnavailableError,
  type ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const McpCapability = Schema.Literals([
  "preview",
  "thread-management",
  "notification",
  "subagent:spawn",
  "subagent:check",
  "subagent:wait",
  "subagent:list",
]);
export type McpCapability = typeof McpCapability.Type;

interface McpInvocationScopeBase {
  readonly environmentId: EnvironmentId;
  readonly capabilities: ReadonlySet<McpCapability>;
  readonly issuedAt: number;
  readonly expiresAt: number | null;
}

export interface ProviderMcpInvocationScope extends McpInvocationScopeBase {
  readonly credentialKind: "provider-session";
  readonly threadId: ThreadId;
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly expiresAt: number;
}

export interface PeerMcpInvocationScope extends McpInvocationScopeBase {
  readonly credentialKind: "peer";
  readonly peerTokenId: string;
  readonly sourceSessionId?: AuthSessionId;
  readonly sourceEnvironmentId?: EnvironmentId;
  readonly allowedParentThreadIds?: ReadonlySet<ThreadId>;
  readonly allowedChildThreadIds?: ReadonlySet<ThreadId>;
  readonly expiresAt: number | null;
}

export type McpInvocationScope = ProviderMcpInvocationScope | PeerMcpInvocationScope;

export class McpInvocationContext extends Context.Service<
  McpInvocationContext,
  McpInvocationScope
>()("t3/mcp/McpInvocationContext") {}

export class McpCapabilityUnavailableError extends Schema.TaggedErrorClass<McpCapabilityUnavailableError>()(
  "McpCapabilityUnavailableError",
  {
    capability: McpCapability,
    environmentId: Schema.String,
    credentialKind: Schema.Literals(["provider-session", "peer"]),
  },
) {
  override get message(): string {
    return `MCP credential does not grant the ${this.capability} capability.`;
  }
}

export class McpProviderSessionRequiredError extends Schema.TaggedErrorClass<McpProviderSessionRequiredError>()(
  "McpProviderSessionRequiredError",
  {
    capability: McpCapability,
    environmentId: Schema.String,
  },
) {
  override get message(): string {
    return `MCP credential for ${this.capability} must be scoped to a provider session.`;
  }
}

export const isProviderInvocationScope = (
  invocation: McpInvocationScope,
): invocation is ProviderMcpInvocationScope => invocation.credentialKind === "provider-session";

const isLegacyProviderCapability = (
  capability: McpCapability,
): capability is "preview" | "thread-management" | "notification" =>
  capability === "preview" || capability === "thread-management" || capability === "notification";

export const requireMcpCapability = Effect.fn("mcp.requireCapability")(function* (
  capability: McpCapability,
) {
  const invocation = yield* McpInvocationContext;
  if (!invocation.capabilities.has(capability)) {
    if (!isProviderInvocationScope(invocation) || !isLegacyProviderCapability(capability)) {
      return yield* new McpCapabilityUnavailableError({
        capability,
        environmentId: invocation.environmentId,
        credentialKind: invocation.credentialKind,
      });
    }
    return yield* new PreviewAutomationUnavailableError({
      capability,
      environmentId: invocation.environmentId,
      threadId: invocation.threadId,
      providerSessionId: invocation.providerSessionId,
      providerInstanceId: invocation.providerInstanceId,
    });
  }
  return invocation;
});

export const requireAnyMcpCapability = Effect.fn("mcp.requireAnyCapability")(function* (
  capabilities: ReadonlyArray<McpCapability>,
) {
  const invocation = yield* McpInvocationContext;
  if (capabilities.some((capability) => invocation.capabilities.has(capability))) {
    return invocation;
  }
  const capability = capabilities[0] ?? "thread-management";
  if (!isProviderInvocationScope(invocation) || !isLegacyProviderCapability(capability)) {
    return yield* new McpCapabilityUnavailableError({
      capability,
      environmentId: invocation.environmentId,
      credentialKind: invocation.credentialKind,
    });
  }
  return yield* new PreviewAutomationUnavailableError({
    capability,
    environmentId: invocation.environmentId,
    threadId: invocation.threadId,
    providerSessionId: invocation.providerSessionId,
    providerInstanceId: invocation.providerInstanceId,
  });
});

export const requireProviderMcpCapability = Effect.fn("mcp.requireProviderCapability")(function* (
  capability: McpCapability,
) {
  const invocation = yield* requireMcpCapability(capability);
  if (!isProviderInvocationScope(invocation)) {
    return yield* new McpProviderSessionRequiredError({
      capability,
      environmentId: invocation.environmentId,
    });
  }
  return invocation;
});
