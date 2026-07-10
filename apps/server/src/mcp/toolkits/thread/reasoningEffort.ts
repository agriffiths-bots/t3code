import { type ModelSelection } from "@t3tools/contracts";
import { type ProviderModelSource } from "@t3tools/shared/model";

export const DEFAULT_MCP_REASONING_EFFORT = "xhigh";

export type ReasoningEffortResolution =
  | { readonly selection: ModelSelection; readonly error?: never }
  | { readonly selection?: never; readonly error: string };

/** Apply the slim MCP surface's Codex effort default after model routing. */
export const applyMcpReasoningEffort = (
  selection: ModelSelection,
  sources: ReadonlyArray<ProviderModelSource>,
  requestedEffort: string | undefined,
): ReasoningEffortResolution => {
  const source = sources.find((candidate) => candidate.instanceId === selection.instanceId);
  const model = source?.models.find((candidate) => candidate.slug === selection.model);
  const descriptor = model?.optionDescriptors?.find(
    (candidate) => candidate.id === "reasoningEffort",
  );

  if (source?.driverKind !== "codex" || descriptor?.type !== "select") {
    return requestedEffort === undefined
      ? { selection }
      : {
          error: `Model "${selection.model}" does not advertise a reasoningEffort option; received "${requestedEffort}".`,
        };
  }

  const effort = requestedEffort ?? DEFAULT_MCP_REASONING_EFFORT;
  const validEfforts = descriptor.options.map((option) => option.id);
  if (!validEfforts.includes(effort)) {
    return {
      error: `Invalid reasoningEffort "${effort}" for model "${selection.model}". Valid values: ${validEfforts.join(", ")}.`,
    };
  }

  const options = (selection.options ?? []).filter((option) => option.id !== "reasoningEffort");
  options.push({ id: "reasoningEffort", value: effort });
  return { selection: { ...selection, options } };
};
