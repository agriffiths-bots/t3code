import {
  EnvironmentId,
  ExecutionEnvironmentPlatformOs,
  IsoDateTime,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProviderState,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as ServerEnvironment from "../../../environment/ServerEnvironment.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import * as SubagentPeerRegistry from "../../../subagents/SubagentPeerRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [
  ProviderRegistry,
  ServerEnvironment.ServerEnvironment,
  SubagentPeerRegistry.SubagentPeerRegistry,
  HttpClient.HttpClient,
  McpInvocationContext.McpInvocationContext,
];

// This tool takes no arguments. `Schema.Struct({})` must NOT be used here: its
// JSON Schema emits `anyOf: [{type:"object"},{type:"array"}]` with no top-level
// `type`, and the OpenAI function-calling API rejects the ENTIRE request with
// HTTP 400 `invalid_function_parameters` when any attached tool's schema lacks
// top-level `type: "object"` — killing every Codex turn on its first call.
// `Schema.Record(String, Never)` emits `{type:"object",additionalProperties:false}`.
export const ListBackendsInput = Schema.Record(Schema.String, Schema.Never);
export type ListBackendsInput = typeof ListBackendsInput.Type;

export const BackendModel = Schema.Struct({
  slug: TrimmedNonEmptyString,
  name: TrimmedNonEmptyString,
});
export type BackendModel = typeof BackendModel.Type;

export const BackendProvider = Schema.Struct({
  instanceId: ProviderInstanceId,
  driver: ProviderDriverKind,
  label: TrimmedNonEmptyString,
  displayName: Schema.optional(TrimmedNonEmptyString),
  enabled: Schema.Boolean,
  installed: Schema.Boolean,
  status: ServerProviderState,
  availability: Schema.Literals(["available", "unavailable"]),
  available: Schema.Boolean,
  models: Schema.Array(BackendModel),
});
export type BackendProvider = typeof BackendProvider.Type;

export const BackendStatus = Schema.Literals(["online", "offline", "error"]);
export type BackendStatus = typeof BackendStatus.Type;

export const BackendSummary = Schema.Struct({
  alias: TrimmedNonEmptyString,
  environmentId: EnvironmentId,
  label: TrimmedNonEmptyString,
  os: ExecutionEnvironmentPlatformOs,
  status: BackendStatus,
  lastSeenAt: Schema.optional(IsoDateTime),
  error: Schema.optional(TrimmedNonEmptyString),
  providers: Schema.Array(BackendProvider),
});
export type BackendSummary = typeof BackendSummary.Type;

export const ListBackendsOutput = Schema.Struct({
  backends: Schema.Array(BackendSummary),
});
export type ListBackendsOutput = typeof ListBackendsOutput.Type;

export class ListBackendsToolError extends Schema.TaggedErrorClass<ListBackendsToolError>()(
  "ListBackendsToolError",
  {
    message: Schema.String,
  },
) {}

export const ListBackendsTool = Tool.make("t3_list_backends", {
  description:
    "List the local T3 Code backend and configured peer backends by alias, with OS, connection status, and available providers/models nested under each backend. Peer inventory failures remain visible as offline/error rows instead of disappearing.",
  parameters: ListBackendsInput,
  success: ListBackendsOutput,
  failure: ListBackendsToolError,
  dependencies,
})
  .annotate(Tool.Title, "List T3 Code backends")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

export const VisibilityToolkit = Toolkit.make(ListBackendsTool);
