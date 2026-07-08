import {
  ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProviderState,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [
  ProviderRegistry,
  ProjectionSnapshotQuery,
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

const BackendProjectFields = {
  id: ProjectId,
  title: TrimmedNonEmptyString,
  workspaceRoot: TrimmedNonEmptyString,
  defaultModelSelection: Schema.NullOr(ModelSelection),
};

export const BackendProject = Schema.Struct(BackendProjectFields);
export type BackendProject = typeof BackendProject.Type;

export const BackendModel = Schema.Struct({
  slug: TrimmedNonEmptyString,
  name: TrimmedNonEmptyString,
});
export type BackendModel = typeof BackendModel.Type;

export const BackendSummary = Schema.Struct({
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
  projects: Schema.Array(BackendProject),
});
export type BackendSummary = typeof BackendSummary.Type;

export const UnknownBackendProject = Schema.Struct({
  ...BackendProjectFields,
  requestedInstanceId: ProviderInstanceId,
});
export type UnknownBackendProject = typeof UnknownBackendProject.Type;

export const ListBackendsOutput = Schema.Struct({
  backends: Schema.Array(BackendSummary),
  unassignedProjects: Schema.Array(BackendProject),
  unknownBackendProjects: Schema.Array(UnknownBackendProject),
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
    "List configured T3 Code provider backends with their exact instance ids, human labels, models, and projects grouped by each project's default backend. Use this when you need the correct backend or model names before starting threads or subagents.",
  parameters: ListBackendsInput,
  success: ListBackendsOutput,
  failure: ListBackendsToolError,
  dependencies,
})
  .annotate(Tool.Title, "List T3 Code backends")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const VisibilityToolkit = Toolkit.make(ListBackendsTool);
