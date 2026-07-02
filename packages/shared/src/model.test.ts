import { describe, expect, it } from "vite-plus/test";
import {
  DEFAULT_MODEL,
  ProviderDriverKind,
  ProviderInstanceId,
  type ModelCapabilities,
} from "@t3tools/contracts";

import {
  applyClaudePromptEffortPrefix,
  buildProviderOptionSelectionsFromDescriptors,
  createModelCapabilities,
  createModelSelection,
  getModelSelectionBooleanOptionValue,
  getModelSelectionStringOptionValue,
  getProviderOptionDescriptors,
  getProviderOptionBooleanSelectionValue,
  getProviderOptionStringSelectionValue,
  isClaudeUltrathinkPrompt,
  normalizeModelSlug,
  pickModelSelectionFromInstances,
  type ProviderModelSource,
  resolveModelSlugForProvider,
  resolveSelectableModel,
  trimOrNull,
} from "./model.ts";

const codexCaps: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: [
        { id: "xhigh", label: "Extra High" },
        { id: "high", label: "High", isDefault: true },
      ],
      currentValue: "high",
    },
    {
      id: "fastMode",
      label: "Fast Mode",
      type: "boolean",
    },
  ],
});

const claudeCaps: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    {
      id: "effort",
      label: "Reasoning",
      type: "select",
      options: [
        { id: "medium", label: "Medium" },
        { id: "high", label: "High", isDefault: true },
        { id: "ultrathink", label: "Ultrathink" },
      ],
      currentValue: "high",
      promptInjectedValues: ["ultrathink"],
    },
    {
      id: "contextWindow",
      label: "Context Window",
      type: "select",
      options: [
        { id: "200k", label: "200k" },
        { id: "1m", label: "1M", isDefault: true },
      ],
      currentValue: "1m",
    },
  ],
});

describe("normalizeModelSlug", () => {
  it("maps known aliases to canonical slugs", () => {
    const claude = ProviderDriverKind.make("claudeAgent");
    expect(normalizeModelSlug("gpt-5-codex")).toBe("gpt-5.4");
    expect(normalizeModelSlug("5.3")).toBe("gpt-5.3-codex");
    expect(normalizeModelSlug("sonnet", claude)).toBe("claude-sonnet-5");
  });

  it("returns null for empty or missing values", () => {
    expect(normalizeModelSlug("")).toBeNull();
    expect(normalizeModelSlug("   ")).toBeNull();
    expect(normalizeModelSlug(null)).toBeNull();
    expect(normalizeModelSlug(undefined)).toBeNull();
  });
});

describe("resolveModelSlugForProvider", () => {
  it("returns defaults when the model is missing", () => {
    expect(resolveModelSlugForProvider(ProviderDriverKind.make("codex"), undefined)).toBe(
      DEFAULT_MODEL,
    );
    expect(resolveModelSlugForProvider(ProviderDriverKind.make("ollama"), undefined)).toBe(
      DEFAULT_MODEL,
    );
    expect(resolveModelSlugForProvider(ProviderDriverKind.make("grok"), undefined)).toBe(
      "grok-build",
    );
  });

  it("preserves normalized unknown models", () => {
    expect(
      resolveModelSlugForProvider(ProviderDriverKind.make("codex"), "custom/internal-model"),
    ).toBe("custom/internal-model");
  });
});

describe("resolveSelectableModel", () => {
  it("resolves exact slugs, labels, and aliases", () => {
    const options = [
      { slug: "gpt-5.3-codex", name: "GPT-5.3 Codex" },
      { slug: "claude-sonnet-5", name: "Claude Sonnet 5" },
    ];
    expect(resolveSelectableModel(ProviderDriverKind.make("codex"), "gpt-5.3-codex", options)).toBe(
      "gpt-5.3-codex",
    );
    expect(resolveSelectableModel(ProviderDriverKind.make("codex"), "gpt-5.3 codex", options)).toBe(
      "gpt-5.3-codex",
    );
    expect(resolveSelectableModel(ProviderDriverKind.make("claudeAgent"), "sonnet", options)).toBe(
      "claude-sonnet-5",
    );
  });
});

describe("misc helpers", () => {
  it("detects ultrathink prompts", () => {
    expect(isClaudeUltrathinkPrompt("Please ultrathink about this")).toBe(true);
    expect(isClaudeUltrathinkPrompt("Ultrathink:\nInvestigate")).toBe(true);
    expect(isClaudeUltrathinkPrompt("Investigate")).toBe(false);
  });

  it("prefixes ultrathink prompts once", () => {
    expect(applyClaudePromptEffortPrefix("Investigate", "ultrathink")).toBe(
      "Ultrathink:\nInvestigate",
    );
    expect(applyClaudePromptEffortPrefix("Ultrathink:\nInvestigate", "ultrathink")).toBe(
      "Ultrathink:\nInvestigate",
    );
  });

  it("trims strings to null", () => {
    expect(trimOrNull("  hi  ")).toBe("hi");
    expect(trimOrNull("   ")).toBeNull();
  });
});

describe("descriptor helpers", () => {
  it("applies selection values to capability descriptors", () => {
    expect(
      getProviderOptionDescriptors({
        caps: claudeCaps,
        selections: [
          { id: "effort", value: "medium" },
          { id: "contextWindow", value: "200k" },
        ],
      }),
    ).toEqual([
      {
        id: "effort",
        label: "Reasoning",
        type: "select",
        options: [
          { id: "medium", label: "Medium" },
          { id: "high", label: "High", isDefault: true },
          { id: "ultrathink", label: "Ultrathink" },
        ],
        currentValue: "medium",
        promptInjectedValues: ["ultrathink"],
      },
      {
        id: "contextWindow",
        label: "Context Window",
        type: "select",
        options: [
          { id: "200k", label: "200k" },
          { id: "1m", label: "1M", isDefault: true },
        ],
        currentValue: "200k",
      },
    ]);
  });

  it("builds wire-format option selections from descriptors", () => {
    const descriptors = getProviderOptionDescriptors({
      caps: codexCaps,
      selections: [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ],
    });

    expect(buildProviderOptionSelectionsFromDescriptors(descriptors)).toEqual([
      { id: "reasoningEffort", value: "high" },
      { id: "fastMode", value: true },
    ]);
  });

  it("stores option selection arrays in model selections", () => {
    expect(
      createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ]),
    ).toEqual({
      instanceId: "codex",
      model: "gpt-5.4",
      options: [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ],
    });
  });

  it("reads typed option selection values", () => {
    const selection = createModelSelection(ProviderInstanceId.make("codex"), "gpt-5.4", [
      { id: "reasoningEffort", value: "high" },
      { id: "fastMode", value: true },
    ]);

    expect(getProviderOptionStringSelectionValue(selection.options, "reasoningEffort")).toBe(
      "high",
    );
    expect(getProviderOptionStringSelectionValue(selection.options, "fastMode")).toBeUndefined();
    expect(getProviderOptionBooleanSelectionValue(selection.options, "fastMode")).toBe(true);
    expect(
      getProviderOptionBooleanSelectionValue(selection.options, "reasoningEffort"),
    ).toBeUndefined();
    expect(getModelSelectionStringOptionValue(selection, "reasoningEffort")).toBe("high");
    expect(getModelSelectionBooleanOptionValue(selection, "fastMode")).toBe(true);
  });
});

describe("pickModelSelectionFromInstances", () => {
  const source = (
    id: string,
    driver: string,
    slugs: ReadonlyArray<string>,
  ): ProviderModelSource => ({
    instanceId: ProviderInstanceId.make(id),
    driverKind: ProviderDriverKind.make(driver),
    models: slugs.map((slug) => ({ slug, defaultOptions: undefined })),
  });

  // Mirrors what the runtime registry reports (each provider's live models).
  const sources: ReadonlyArray<ProviderModelSource> = [
    source("codex", "codex", ["gpt-5.4", "gpt-5.3-codex", "gpt-5.4-mini"]),
    source("claudeAgent", "claudeAgent", [
      "claude-opus-4-8",
      "claude-opus-4-6",
      "claude-sonnet-4-6",
      "claude-haiku-4-5",
      // Present in the live provider list but NOT in the registry alias maps:
      "claude-fable-5",
    ]),
    // The Cursor aggregator also re-lists some Claude models.
    source("cursor", "cursor", ["composer-2", "claude-sonnet-4-6", "claude-opus-4-6"]),
    source("grok", "grok", ["grok-build"]),
  ];

  it("matches plain canonical models against the live provider lists", () => {
    expect(pickModelSelectionFromInstances("claude-opus-4-8", sources)).toEqual({
      instanceId: "claudeAgent",
      model: "claude-opus-4-8",
    });
    expect(pickModelSelectionFromInstances("gpt-5.4", sources)).toEqual({
      instanceId: "codex",
      model: "gpt-5.4",
    });
    expect(pickModelSelectionFromInstances("grok-build", sources)).toEqual({
      instanceId: "grok",
      model: "grok-build",
    });
  });

  it("routes a newly-added live model with no code change (e.g. Fable 5)", () => {
    // The critical case: claude-fable-5 is served by the provider but is not in
    // any alias/default map. Because we match the LIVE list, it still resolves.
    expect(pickModelSelectionFromInstances("claude-fable-5", sources)).toEqual({
      instanceId: "claudeAgent",
      model: "claude-fable-5",
    });
  });

  it("resolves registry aliases to the canonical live slug", () => {
    expect(pickModelSelectionFromInstances("opus", sources)).toEqual({
      instanceId: "claudeAgent",
      model: "claude-opus-4-8",
    });
    expect(pickModelSelectionFromInstances("gpt-5-codex", sources)).toEqual({
      instanceId: "codex",
      model: "gpt-5.4",
    });
  });

  it("prefers the native provider when several serve the same model", () => {
    // claude-sonnet-4-6 is served by both claudeAgent and the cursor aggregator.
    expect(pickModelSelectionFromInstances("claude-sonnet-4-6", sources)).toEqual({
      instanceId: "claudeAgent",
      model: "claude-sonnet-4-6",
    });
  });

  it("routes a provider-specific alias's canonical model to the native provider", () => {
    // "opus-4.6-thinking" is a Cursor-only alias -> claude-opus-4-6, which both
    // claudeAgent and cursor serve. The native provider must still win.
    expect(pickModelSelectionFromInstances("opus-4.6-thinking", sources)).toEqual({
      instanceId: "claudeAgent",
      model: "claude-opus-4-6",
    });
  });

  it("prefers the source instance when one provider has several instances", () => {
    const multi: ReadonlyArray<ProviderModelSource> = [
      source("codex_personal", "codex", ["gpt-5.4"]),
      source("codex_work", "codex", ["gpt-5.4"]),
    ];
    expect(
      pickModelSelectionFromInstances("gpt-5.4", multi, ProviderInstanceId.make("codex_work")),
    ).toEqual({ instanceId: "codex_work", model: "gpt-5.4" });
  });

  it("preserves the matched model's default options", () => {
    const withOptions: ReadonlyArray<ProviderModelSource> = [
      {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        driverKind: ProviderDriverKind.make("claudeAgent"),
        models: [{ slug: "claude-opus-4-8", defaultOptions: [{ id: "effort", value: "high" }] }],
      },
    ];
    expect(pickModelSelectionFromInstances("claude-opus-4-8", withOptions)).toEqual({
      instanceId: "claudeAgent",
      model: "claude-opus-4-8",
      options: [{ id: "effort", value: "high" }],
    });
  });

  it("returns null for unknown or empty models so callers can fall back", () => {
    expect(pickModelSelectionFromInstances("totally-unknown-model", sources)).toBeNull();
    expect(pickModelSelectionFromInstances("", sources)).toBeNull();
    expect(pickModelSelectionFromInstances("   ", sources)).toBeNull();
    expect(pickModelSelectionFromInstances(null, sources)).toBeNull();
    expect(pickModelSelectionFromInstances("gpt-5.4", [])).toBeNull();
  });
});
