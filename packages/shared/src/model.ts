import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  MODEL_SLUG_ALIASES_BY_PROVIDER,
  type ModelCapabilities,
  type ModelSelection,
  PROVIDER_INFERENCE_PRIORITY,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderOptionDescriptor,
  type ProviderOptionSelection,
} from "@t3tools/contracts";

const DEFAULT_PROVIDER_DRIVER_KIND = ProviderDriverKind.make("codex");
const CLAUDE_PROVIDER_DRIVER_KIND = ProviderDriverKind.make("claudeAgent");

export const CLAUDE_RETIRED_MODEL_SLUGS: ReadonlySet<string> = new Set(["claude-opus-4-8"]);

export const CLAUDE_RESERVED_MODEL_SLUGS: ReadonlySet<string> = new Set([
  "claude-fable-5",
  "claude-opus-5",
  "claude-opus-4-7",
  ...CLAUDE_RETIRED_MODEL_SLUGS,
]);

const RETIRED_SELECTABLE_MODEL_MIGRATIONS: Readonly<
  Record<string, Readonly<Record<string, ReadonlyArray<string>>>>
> = {
  claudeAgent: {
    opus: ["claude-opus-5", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4-5"],
    "claude-opus-4-8": ["claude-opus-5", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4-5"],
    "opus-4.8": ["claude-opus-5", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4-5"],
    "claude-opus-4.8": ["claude-opus-5", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4-5"],
  },
};

function resolveRetiredSelectableModelMigrations(
  provider: ProviderDriverKind,
  value: string,
): ReadonlyArray<string> {
  return RETIRED_SELECTABLE_MODEL_MIGRATIONS[provider]?.[value] ?? [];
}

export interface SelectableModelOption {
  slug: string;
  name: string;
}

export function createModelCapabilities(input: {
  optionDescriptors: ReadonlyArray<ProviderOptionDescriptor>;
}): ModelCapabilities {
  return {
    optionDescriptors: input.optionDescriptors.map(cloneDescriptor),
  };
}

export const CLAUDE_OPUS_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    {
      id: "effort",
      label: "Reasoning",
      type: "select",
      options: [
        { id: "low", label: "Low" },
        { id: "medium", label: "Medium" },
        { id: "high", label: "High", isDefault: true },
        { id: "xhigh", label: "Extra High" },
        { id: "max", label: "Max" },
        { id: "ultracode", label: "Ultracode" },
        { id: "ultrathink", label: "Ultrathink" },
      ],
      currentValue: "high",
      promptInjectedValues: ["ultrathink"],
    },
    {
      id: "fastMode",
      label: "Fast Mode",
      type: "boolean",
    },
  ],
});

function getRawSelectionValueById(
  selections: ReadonlyArray<ProviderOptionSelection> | null | undefined,
  id: string,
): string | boolean | undefined {
  const selection = selections?.find((candidate) => candidate.id === id);
  return selection?.value;
}

export function getProviderOptionSelectionValue(
  selections: ReadonlyArray<ProviderOptionSelection> | null | undefined,
  id: string,
): string | boolean | undefined {
  return getRawSelectionValueById(selections, id);
}

export function getProviderOptionStringSelectionValue(
  selections: ReadonlyArray<ProviderOptionSelection> | null | undefined,
  id: string,
): string | undefined {
  const value = getProviderOptionSelectionValue(selections, id);
  return typeof value === "string" ? value : undefined;
}

export function getProviderOptionBooleanSelectionValue(
  selections: ReadonlyArray<ProviderOptionSelection> | null | undefined,
  id: string,
): boolean | undefined {
  const value = getProviderOptionSelectionValue(selections, id);
  return typeof value === "boolean" ? value : undefined;
}

export function getModelSelectionOptionValue(
  modelSelection: ModelSelection | null | undefined,
  id: string,
): string | boolean | undefined {
  return getProviderOptionSelectionValue(modelSelection?.options, id);
}

export function getModelSelectionStringOptionValue(
  modelSelection: ModelSelection | null | undefined,
  id: string,
): string | undefined {
  return getProviderOptionStringSelectionValue(modelSelection?.options, id);
}

export function getModelSelectionBooleanOptionValue(
  modelSelection: ModelSelection | null | undefined,
  id: string,
): boolean | undefined {
  return getProviderOptionBooleanSelectionValue(modelSelection?.options, id);
}

function resolveDescriptorChoiceValue(
  descriptor: Extract<ProviderOptionDescriptor, { type: "select" }>,
  raw: string | null | undefined,
): string | undefined {
  const trimmed = trimOrNull(raw);
  if (!trimmed) {
    return descriptor.currentValue ?? descriptor.options.find((option) => option.isDefault)?.id;
  }
  if (descriptor.options.length === 0) {
    return trimmed;
  }
  if (
    descriptor.promptInjectedValues?.includes(trimmed) &&
    descriptor.options.some((option) => option.id === trimmed)
  ) {
    return descriptor.options.find((option) => option.isDefault)?.id;
  }
  if (descriptor.options.some((option) => option.id === trimmed)) {
    return trimmed;
  }
  return descriptor.currentValue ?? descriptor.options.find((option) => option.isDefault)?.id;
}

function cloneDescriptor(descriptor: ProviderOptionDescriptor): ProviderOptionDescriptor {
  return descriptor.type === "select"
    ? {
        ...descriptor,
        options: [...descriptor.options],
        ...(descriptor.promptInjectedValues
          ? { promptInjectedValues: [...descriptor.promptInjectedValues] }
          : {}),
      }
    : { ...descriptor };
}

function cloneSelection(selection: ProviderOptionSelection): ProviderOptionSelection {
  return { ...selection };
}

function withDescriptorCurrentValue(
  descriptor: ProviderOptionDescriptor,
  rawCurrentValue: string | boolean | undefined,
): ProviderOptionDescriptor {
  if (descriptor.type === "boolean") {
    if (typeof rawCurrentValue === "boolean") {
      return {
        ...descriptor,
        currentValue: rawCurrentValue,
      };
    }
    return descriptor;
  }
  const currentValue =
    typeof rawCurrentValue === "string"
      ? resolveDescriptorChoiceValue(descriptor, rawCurrentValue)
      : resolveDescriptorChoiceValue(descriptor, descriptor.currentValue);
  if (!currentValue) {
    const { currentValue: _unusedCurrentValue, ...rest } = descriptor;
    return rest;
  }
  return {
    ...descriptor,
    currentValue,
  };
}

export function getProviderOptionDescriptors(input: {
  caps: ModelCapabilities;
  selections?: ReadonlyArray<ProviderOptionSelection> | null | undefined;
}): ReadonlyArray<ProviderOptionDescriptor> {
  const { caps, selections } = input;
  const baseDescriptors = (caps.optionDescriptors ?? []).map(cloneDescriptor);

  return baseDescriptors.map((descriptor) => {
    let selectedValue = getRawSelectionValueById(selections, descriptor.id);
    // Codex used a boolean `fastMode` option before app-server exposed service tiers.
    // Preserve its string and boolean legacy states against the current descriptor ids.
    if (descriptor.id === "serviceTier") {
      if (typeof selectedValue === "string" && selectedValue.trim().toLowerCase() === "fast") {
        selectedValue = "priority";
      } else if (selectedValue === undefined) {
        const legacyFastMode = getRawSelectionValueById(selections, "fastMode");
        if (legacyFastMode === true) selectedValue = "priority";
        if (legacyFastMode === false) selectedValue = "default";
      }
    }
    return withDescriptorCurrentValue(descriptor, selectedValue ?? descriptor.currentValue);
  });
}

export function getProviderOptionCurrentValue(
  descriptor: ProviderOptionDescriptor | null | undefined,
): string | boolean | undefined {
  if (!descriptor) {
    return undefined;
  }
  if (descriptor.type === "boolean") {
    return descriptor.currentValue;
  }
  if (descriptor.currentValue) {
    return descriptor.currentValue;
  }
  return descriptor.options.find((option) => option.isDefault)?.id;
}

export function getProviderOptionCurrentLabel(
  descriptor: ProviderOptionDescriptor | null | undefined,
): string | undefined {
  if (!descriptor) {
    return undefined;
  }
  if (descriptor.type === "boolean") {
    return typeof descriptor.currentValue === "boolean"
      ? descriptor.currentValue
        ? "On"
        : "Off"
      : undefined;
  }
  const currentValue = getProviderOptionCurrentValue(descriptor);
  if (typeof currentValue !== "string") {
    return undefined;
  }
  return descriptor.options.find((option) => option.id === currentValue)?.label;
}

export function buildProviderOptionSelectionsFromDescriptors(
  descriptors: ReadonlyArray<ProviderOptionDescriptor> | null | undefined,
): Array<ProviderOptionSelection> | undefined {
  if (!descriptors || descriptors.length === 0) {
    return undefined;
  }

  const nextSelections: Array<ProviderOptionSelection> = [];

  for (const descriptor of descriptors) {
    const value = getProviderOptionCurrentValue(descriptor);
    if (typeof value === "string" || typeof value === "boolean") {
      nextSelections.push({ id: descriptor.id, value });
    }
  }

  return nextSelections.length > 0 ? nextSelections : undefined;
}

export function getModelSelectionOptionDescriptors(
  modelSelection: ModelSelection | null | undefined,
  caps?: ModelCapabilities | null | undefined,
): ReadonlyArray<ProviderOptionDescriptor> {
  if (!modelSelection) {
    return [];
  }
  if (!caps) {
    return [];
  }
  return getProviderOptionDescriptors({
    caps,
    selections: modelSelection.options,
  });
}

export function isClaudeUltrathinkPrompt(text: string | null | undefined): boolean {
  return typeof text === "string" && /\bultrathink\b/i.test(text);
}

export function normalizeModelSlug(
  model: string | null | undefined,
  provider: ProviderDriverKind = DEFAULT_PROVIDER_DRIVER_KIND,
): string | null {
  const trimmed = normalizeCustomModelSlug(model);
  if (!trimmed) {
    return null;
  }

  const aliases = MODEL_SLUG_ALIASES_BY_PROVIDER[provider] ?? {};
  const aliased = Object.prototype.hasOwnProperty.call(aliases, trimmed)
    ? aliases[trimmed]
    : undefined;
  return typeof aliased === "string" ? aliased : trimmed;
}

/** Custom model identifiers are provider-owned, so only trim them; never expand aliases. */
export function normalizeCustomModelSlug(model: string | null | undefined): string | null {
  if (typeof model !== "string") {
    return null;
  }

  return model.trim() || null;
}

export function resolveSelectableModel(
  provider: ProviderDriverKind,
  value: string | null | undefined,
  options: ReadonlyArray<SelectableModelOption>,
): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const direct = options.find((option) => option.slug === trimmed);
  if (direct) {
    return direct.slug;
  }

  const byName = options.find((option) => option.name.toLowerCase() === trimmed.toLowerCase());
  if (byName) {
    return byName.slug;
  }

  const candidates = [...resolveRetiredSelectableModelMigrations(provider, trimmed), trimmed];
  for (const candidate of candidates) {
    const normalized = normalizeModelSlug(candidate, provider);
    if (!normalized) {
      continue;
    }
    const resolved = options.find((option) => option.slug === normalized);
    if (resolved) {
      return resolved.slug;
    }
  }
  return null;
}

function resolveModelSlug(model: string | null | undefined, provider: ProviderDriverKind): string {
  const normalized = normalizeModelSlug(model, provider);
  if (!normalized) {
    return DEFAULT_MODEL_BY_PROVIDER[provider] ?? DEFAULT_MODEL;
  }
  return normalized;
}

export function resolveModelSlugForProvider(
  provider: ProviderDriverKind,
  model: string | null | undefined,
): string {
  return resolveModelSlug(model, provider);
}

/** A single model a provider instance serves, plus its default option selections. */
export interface ProviderModelEntry {
  readonly slug: string;
  readonly isCustom: boolean;
  readonly defaultOptions: ReadonlyArray<ProviderOptionSelection> | undefined;
  readonly optionDescriptors: ReadonlyArray<ProviderOptionDescriptor> | undefined;
}

/**
 * A live provider instance's model catalog, as seen by the runtime provider
 * registry: the instance's routing id, its driver kind, and the models it
 * currently serves (each with its default options). This is the same
 * upstream-maintained source the model picker uses, so it already includes
 * newly-added built-in models (e.g. `claude-fable-5`) and any custom models.
 * Callers should include ENABLED instances only.
 */
export interface ProviderModelSource {
  readonly instanceId: ProviderInstanceId;
  readonly driverKind: ProviderDriverKind;
  readonly models: ReadonlyArray<ProviderModelEntry>;
}

function providerPriorityIndex(driver: ProviderDriverKind): number {
  const index = PROVIDER_INFERENCE_PRIORITY.indexOf(driver);
  // Providers not yet ranked sort last, so a newly-introduced provider still
  // resolves (just at lowest priority) without a code change here.
  return index === -1 ? PROVIDER_INFERENCE_PRIORITY.length : index;
}

function makeModelSelection(
  instanceId: ProviderInstanceId,
  driverKind: ProviderDriverKind,
  slug: string,
  defaultOptions: ReadonlyArray<ProviderOptionSelection> | undefined,
  optionDescriptors: ReadonlyArray<ProviderOptionDescriptor> | undefined,
): ModelSelection {
  const options = applyThreadCreationDefaultEffort(
    driverKind,
    slug,
    defaultOptions,
    optionDescriptors,
  );
  // Preserve provider defaults and layer T3's plain-model thread-creation
  // effort defaults on top; omit options when there are none.
  return options && options.length > 0
    ? { instanceId, model: slug, options }
    : { instanceId, model: slug };
}

const THREAD_CREATION_DEFAULT_EFFORT_BY_MODEL: Record<string, string> = {
  "gpt-5.5": "xhigh",
  "claude-opus-5": "xhigh",
  "claude-opus-4-8": "xhigh",
  "claude-sonnet-5": "xhigh",
  "claude-fable-5": "high",
};

const THREAD_CREATION_EFFORT_OPTION_BY_DRIVER: Record<string, string> = {
  codex: "reasoningEffort",
  claudeAgent: "effort",
  cursor: "reasoning",
};

function applyThreadCreationDefaultEffort(
  driverKind: ProviderDriverKind,
  slug: string,
  defaultOptions: ReadonlyArray<ProviderOptionSelection> | undefined,
  optionDescriptors: ReadonlyArray<ProviderOptionDescriptor> | undefined,
): Array<ProviderOptionSelection> | undefined {
  const directedValue = THREAD_CREATION_DEFAULT_EFFORT_BY_MODEL[slug];
  const optionId = THREAD_CREATION_EFFORT_OPTION_BY_DRIVER[driverKind];
  if (
    !directedValue ||
    !optionId ||
    !isThreadCreationEffortAdvertised(optionDescriptors, optionId, directedValue)
  ) {
    return defaultOptions && defaultOptions.length > 0
      ? defaultOptions.map(cloneSelection)
      : undefined;
  }

  const next =
    defaultOptions && defaultOptions.length > 0 ? defaultOptions.map(cloneSelection) : [];
  const existingIndex = next.findIndex((selection) => selection.id === optionId);
  if (existingIndex < 0) {
    next.push({ id: optionId, value: directedValue });
    return next;
  }
  next[existingIndex] = { ...next[existingIndex]!, value: directedValue };
  return next;
}

function isThreadCreationEffortAdvertised(
  optionDescriptors: ReadonlyArray<ProviderOptionDescriptor> | undefined,
  optionId: string,
  value: string,
): boolean {
  const descriptor = optionDescriptors?.find((candidate) => candidate.id === optionId);
  return descriptor?.type === "select" && descriptor.options.some((option) => option.id === value);
}

/**
 * Resolve a plain model name to a `ModelSelection` against the LIVE provider
 * model lists, so a caller never has to know or guess a harness/instance id —
 * they pass e.g. `claude-opus-5`, `gpt-5.4`, or a future `fable-5` and the
 * official provider is found from the registry data itself (NO hardcoded
 * model-name patterns). Selection order: the native provider first
 * (`PROVIDER_INFERENCE_PRIORITY`, so Claude models resolve to `claudeAgent`, not
 * the Cursor aggregator), and within a provider the `preferInstanceId` (usually
 * the source thread's instance) wins so a multi-instance setup keeps continuity.
 * Alias inputs (e.g. `opus`) resolve through the registry alias maps and then
 * match the live lists; the matched model's default options are preserved.
 * Returns null when unresolved so callers can fall back to an inherited selection.
 */
export function pickModelSelectionFromInstances(
  model: string | null | undefined,
  sources: ReadonlyArray<ProviderModelSource>,
  preferInstanceId?: ProviderInstanceId,
): ModelSelection | null {
  const trimmed = typeof model === "string" ? model.trim() : "";
  if (!trimmed) return null;

  // Lower rank wins: native provider first, then the preferred (source) instance.
  const rank = (source: ProviderModelSource): number =>
    providerPriorityIndex(source.driverKind) * 2 +
    (preferInstanceId !== undefined && source.instanceId === preferInstanceId ? 0 : 1);

  // Best (highest-priority, source-preferred) instance that serves an exact slug.
  const resolveSlug = (
    slug: string,
    acceptEntry: (entry: ProviderModelEntry) => boolean = () => true,
    acceptSource: (source: ProviderModelSource) => boolean = () => true,
  ): ModelSelection | null => {
    const best = sources
      .filter(
        (source) =>
          acceptSource(source) &&
          source.models.some((entry) => entry.slug === slug && acceptEntry(entry)),
      )
      .sort((a, b) => rank(a) - rank(b))[0];
    if (best === undefined) return null;
    const entry = best.models.find(
      (candidate) => candidate.slug === slug && acceptEntry(candidate),
    );
    return makeModelSelection(
      best.instanceId,
      best.driverKind,
      slug,
      entry?.defaultOptions,
      entry?.optionDescriptors,
    );
  };

  const resolveBestCanonical = (
    canonicals: ReadonlySet<string>,
    acceptEntry?: (entry: ProviderModelEntry) => boolean,
    acceptSource?: (source: ProviderModelSource) => boolean,
  ): ModelSelection | null => {
    let best: ModelSelection | null = null;
    let bestRank = Number.POSITIVE_INFINITY;
    for (const canonical of canonicals) {
      const selection = resolveSlug(canonical, acceptEntry, acceptSource);
      if (selection === null) continue;
      const chosen = sources.find((source) => source.instanceId === selection.instanceId);
      const selectionRank = chosen ? rank(chosen) : Number.POSITIVE_INFINITY;
      if (selectionRank < bestRank) {
        best = selection;
        bestRank = selectionRank;
      }
    }
    return best;
  };

  // Retired built-ins migrate before aggregator exact matches. When the latest
  // replacement is version-gated, stay on the newest Opus model the native
  // Claude instance actually serves instead of falling to an unrelated model.
  const migrations = new Set<string>();
  for (const source of sources) {
    for (const migrated of resolveRetiredSelectableModelMigrations(source.driverKind, trimmed)) {
      migrations.add(migrated);
    }
  }

  // An exact legacy slug remains runnable when the native Claude provider
  // still reports it (for example, a persisted session or schedule created
  // before catalog retirement). Aggregator-only stale entries must not win:
  // those continue through the native replacement migration below.
  if (migrations.size > 0 && CLAUDE_RETIRED_MODEL_SLUGS.has(trimmed)) {
    const directNative = resolveSlug(
      trimmed,
      () => true,
      (source) => source.driverKind === CLAUDE_PROVIDER_DRIVER_KIND,
    );
    if (directNative !== null) return directNative;
  }

  // Preserve an exact provider-owned custom model before migrating a retired
  // built-in. This is gated to retired inputs so unrelated slug collisions
  // still use normal native-provider priority.
  if (migrations.size > 0) {
    const directCustom = resolveSlug(trimmed, (entry) => entry.isCustom);
    if (directCustom !== null) return directCustom;
  }

  const migrated = resolveBestCanonical(
    migrations,
    (entry) => !entry.isCustom,
    (source) => source.driverKind === CLAUDE_PROVIDER_DRIVER_KIND,
  );
  if (migrated !== null) return migrated;
  if (migrations.size > 0) return null;

  // 1. Direct match against the models each provider actually serves.
  const direct = resolveSlug(trimmed);
  if (direct !== null) return direct;

  // 2. Resolve the input through the registry alias maps to its canonical slug(s),
  // then match each canonical against ALL providers with the same native-priority
  // preference — so a provider-specific alias (e.g. the Cursor-only
  // "opus-4.6-thinking") still routes its canonical model to the official
  // provider rather than the aliasing one.
  const canonicals = new Set<string>();
  for (const source of sources) {
    const aliases = MODEL_SLUG_ALIASES_BY_PROVIDER[source.driverKind] ?? {};
    const canonical = Object.prototype.hasOwnProperty.call(aliases, trimmed)
      ? aliases[trimmed]
      : undefined;
    if (typeof canonical === "string") canonicals.add(canonical);
    for (const migrated of resolveRetiredSelectableModelMigrations(source.driverKind, trimmed)) {
      canonicals.add(migrated);
    }
  }
  return resolveBestCanonical(canonicals);
}

/** Trim a string, returning null for empty/missing values. */
export function trimOrNull<T extends string>(value: T | null | undefined): T | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim() as T;
  return trimmed || null;
}

function cloneSelections(
  selections: ReadonlyArray<ProviderOptionSelection>,
): Array<ProviderOptionSelection> {
  return selections.map(cloneSelection);
}

export function createModelSelection(
  instanceId: ProviderInstanceId,
  model: string,
  options?: ReadonlyArray<ProviderOptionSelection> | null,
): ModelSelection {
  const selections = options ? cloneSelections(options) : [];
  const base: ModelSelection = {
    instanceId,
    model,
  };
  return selections.length > 0 ? { ...base, options: selections } : base;
}

/**
 * Returns the effort value if it is a prompt-injected value according to
 * any select descriptor in the given capabilities, or null otherwise.
 *
 * Unlike a single `find`, this checks every descriptor so that the
 * correct descriptor's `promptInjectedValues` list is consulted even when
 * multiple select descriptors exist.
 */
export function resolvePromptInjectedEffort(
  caps: ModelCapabilities,
  rawEffort: string | null | undefined,
): string | null {
  const trimmed = trimOrNull(rawEffort);
  if (!trimmed) return null;
  const descriptors = getProviderOptionDescriptors({ caps });
  for (const descriptor of descriptors) {
    if (descriptor.type === "select" && descriptor.promptInjectedValues?.includes(trimmed)) {
      return trimmed;
    }
  }
  return null;
}

export function applyClaudePromptEffortPrefix(
  text: string,
  effort: string | null | undefined,
): string {
  const trimmed = text.trim();
  if (!trimmed) {
    return trimmed;
  }
  if (effort !== "ultrathink") {
    return trimmed;
  }
  if (trimmed.startsWith("Ultrathink:")) {
    return trimmed;
  }
  return `Ultrathink:\n${trimmed}`;
}
