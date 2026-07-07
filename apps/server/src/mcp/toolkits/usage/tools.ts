import { PlanUsageSnapshotSchema, ProviderInstanceId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import { ServerSettingsService } from "../../../serverSettings.ts";

const dependencies = [ServerSettingsService];

export const GetUsageInput = Schema.Struct({
  providerInstanceId: Schema.optional(ProviderInstanceId),
});
export type GetUsageInput = typeof GetUsageInput.Type;

export const GetUsageOutput = PlanUsageSnapshotSchema;
export type GetUsageOutput = typeof GetUsageOutput.Type;

export class GetUsageToolError extends Schema.TaggedErrorClass<GetUsageToolError>()(
  "GetUsageToolError",
  {
    message: Schema.String,
  },
) {}

export const GetUsageTool = Tool.make("t3_get_usage", {
  description:
    "Return the current T3 Code plan-usage snapshot for Codex and Claude provider backends. Omitting providerInstanceId aggregates enabled Codex/Claude instances; passing providerInstanceId scopes the read to that configured backend. Codex usage is read from ChatGPT wham usage; Claude usage is read through the official Claude CLI usage path.",
  parameters: GetUsageInput,
  success: GetUsageOutput,
  failure: GetUsageToolError,
  dependencies,
})
  .annotate(Tool.Title, "Get T3 Code usage")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

export const UsageToolkit = Toolkit.make(GetUsageTool);
