import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type * as CodexClient from "effect-codex-app-server/client";
import type * as CodexSchema from "effect-codex-app-server/schema";
import { ProviderInstanceId } from "@t3tools/contracts";
import {
  buildProviderOptionSelectionsFromDescriptors,
  createModelSelection,
} from "@t3tools/shared/model";

import { getCodexServiceTierOptionValue } from "../../codexModelOptions.ts";
import {
  applyPreferredCodexDefaultModel,
  mapCodexModelCapabilities,
  requestAllCodexModels,
} from "./CodexProvider.ts";

const priorityDefaultModel = {
  additionalSpeedTiers: [],
  defaultReasoningEffort: "medium",
  description: "Test model",
  displayName: "GPT Test",
  hidden: false,
  id: "gpt-test",
  isDefault: true,
  model: "gpt-test",
  defaultServiceTier: "priority",
  serviceTiers: [
    {
      id: "priority",
      name: "Fast",
      description: "1.5x speed, increased usage",
    },
    {
      id: "flex",
      name: "Flex",
      description: "Lower-cost asynchronous routing.",
    },
  ],
  supportedReasoningEfforts: [],
} satisfies CodexSchema.V2ModelListResponse__Model;

function selectedServiceTier(
  capabilities: ReturnType<typeof mapCodexModelCapabilities>,
): string | undefined {
  const descriptor = capabilities.optionDescriptors?.find(
    (candidate) => candidate.id === "serviceTier",
  );
  return descriptor?.type === "select" ? descriptor.currentValue : undefined;
}

function dispatchedServiceTier(
  capabilities: ReturnType<typeof mapCodexModelCapabilities>,
): string | undefined {
  return getCodexServiceTierOptionValue(
    createModelSelection(
      ProviderInstanceId.make("codex"),
      priorityDefaultModel.model,
      buildProviderOptionSelectionsFromDescriptors(capabilities.optionDescriptors ?? []),
    ),
  );
}

it("maps current Codex model capability fields", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: [],
    defaultReasoningEffort: "super-high",
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    defaultServiceTier: "flex",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "Lower latency responses.",
      },
      {
        id: "flex",
        name: "Flex",
        description: "Lower-cost asynchronous routing.",
      },
    ],
    supportedReasoningEfforts: [
      {
        description: "Maximum reasoning",
        reasoningEffort: "super-high",
      },
    ],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "reasoningEffort",
      label: "Reasoning",
      type: "select",
      options: [{ id: "super-high", label: "super-high", isDefault: true }],
      currentValue: "super-high",
    },
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard" },
        {
          id: "priority",
          label: "Fast",
          description: "Lower latency responses.",
        },
        {
          id: "flex",
          label: "Flex",
          description: "Lower-cost asynchronous routing.",
          isDefault: true,
        },
      ],
      currentValue: "flex",
    },
  ]);
});

it("uses standard routing when the catalog has no default service tier", () => {
  const capabilities = mapCodexModelCapabilities({
    additionalSpeedTiers: ["fast"],
    defaultReasoningEffort: "medium",
    defaultServiceTier: null,
    description: "Test model",
    displayName: "GPT Test",
    hidden: false,
    id: "gpt-test",
    isDefault: true,
    model: "gpt-test",
    serviceTiers: [
      {
        id: "priority",
        name: "Fast",
        description: "1.5x speed, increased usage",
      },
    ],
    supportedReasoningEfforts: [],
  });

  assert.deepStrictEqual(capabilities.optionDescriptors, [
    {
      id: "serviceTier",
      label: "Service Tier",
      type: "select",
      options: [
        { id: "default", label: "Standard", isDefault: true },
        {
          id: "priority",
          label: "Fast",
          description: "1.5x speed, increased usage",
        },
      ],
      currentValue: "default",
    },
  ]);
  assert.equal(dispatchedServiceTier(capabilities), "default");
});

it("uses Standard when effective config carries the durable Fast-default opt-out", () => {
  const capabilities = mapCodexModelCapabilities(priorityDefaultModel, {
    service_tier: null,
    notice: { fast_default_opt_out: true },
  });

  assert.equal(selectedServiceTier(capabilities), "default");
  assert.equal(dispatchedServiceTier(capabilities), "default");
  const descriptor = capabilities.optionDescriptors?.find(
    (candidate) => candidate.id === "serviceTier",
  );
  assert.equal(descriptor?.type, "select");
  if (descriptor?.type === "select") {
    assert.equal(
      descriptor.options.some((option) => option.id === "priority"),
      true,
    );
  }
  assert.equal(
    selectedServiceTier(
      mapCodexModelCapabilities(priorityDefaultModel, {
        service_tier: "fast",
        notice: { fast_default_opt_out: true },
      }),
    ),
    "priority",
  );
});

it("uses Standard when effective config explicitly selects the default tier", () => {
  const capabilities = mapCodexModelCapabilities(priorityDefaultModel, {
    service_tier: "default",
  });

  assert.equal(selectedServiceTier(capabilities), "default");
  assert.equal(dispatchedServiceTier(capabilities), "default");
});

it("adopts the catalog-managed Fast default only when config has no explicit choice", () => {
  const capabilities = mapCodexModelCapabilities(priorityDefaultModel, {
    service_tier: null,
    notice: null,
  });

  assert.equal(selectedServiceTier(capabilities), "priority");
  assert.equal(dispatchedServiceTier(capabilities), "priority");
});

it("honors explicit Fast config in legacy and canonical forms", () => {
  for (const configured of ["fast", "priority"] as const) {
    const capabilities = mapCodexModelCapabilities(priorityDefaultModel, {
      service_tier: configured,
    });
    assert.equal(selectedServiceTier(capabilities), "priority");
    assert.equal(dispatchedServiceTier(capabilities), "priority");
  }
});

it("honors explicit Flex config ahead of the catalog-managed Fast default", () => {
  const capabilities = mapCodexModelCapabilities(priorityDefaultModel, {
    service_tier: "flex",
  });

  assert.equal(selectedServiceTier(capabilities), "flex");
  assert.equal(dispatchedServiceTier(capabilities), "flex");
});

it("uses explicit Standard when a configured tier is unsupported by the model", () => {
  const capabilities = mapCodexModelCapabilities(
    {
      ...priorityDefaultModel,
      serviceTiers: priorityDefaultModel.serviceTiers.filter((tier) => tier.id === "priority"),
    },
    { service_tier: "flex" },
  );

  assert.equal(selectedServiceTier(capabilities), "default");
  assert.equal(dispatchedServiceTier(capabilities), "default");
});

it.effect(
  "reads Windows and WSL catalogs through each provider client's own effective config",
  () =>
    Effect.gen(function* () {
      const makeClient = (
        config: CodexSchema.V2ConfigReadResponse__Config,
        expectedCwd: string,
      ): CodexClient.CodexAppServerClient["Service"] => {
        const client = {
          request: (method: string, params: unknown) => {
            if (method === "config/read") {
              assert.deepStrictEqual(params, { cwd: expectedCwd });
              return Effect.succeed({ config, origins: {} });
            }
            if (method === "model/list") {
              return Effect.succeed({ data: [priorityDefaultModel], nextCursor: null });
            }
            return Effect.die(new Error(`Unexpected request: ${method}`));
          },
        };
        return client as unknown as CodexClient.CodexAppServerClient["Service"];
      };

      const windowsModels = yield* requestAllCodexModels(
        makeClient(
          { service_tier: null, notice: { fast_default_opt_out: true } },
          "C:\\work\\project",
        ),
        "C:\\work\\project",
      );
      const wslModels = yield* requestAllCodexModels(
        makeClient({ service_tier: null, notice: null }, "/home/work/project"),
        "/home/work/project",
      );

      assert.equal(
        selectedServiceTier(windowsModels[0]?.capabilities ?? { optionDescriptors: [] }),
        "default",
      );
      assert.equal(
        selectedServiceTier(wslModels[0]?.capabilities ?? { optionDescriptors: [] }),
        "priority",
      );
    }),
);

it("marks the most preferred available model as default", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(
    models.map((model) => ({ slug: model.slug, isDefault: model.isDefault })),
    [
      { slug: "gpt-5.6-terra", isDefault: true },
      { slug: "gpt-5.4", isDefault: undefined },
    ],
  );
});

it("prefers sol over terra when both are available", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-terra", name: "GPT-5.6-Terra", isCustom: false, capabilities: null },
    { slug: "gpt-5.6-sol", name: "GPT-5.6-Sol", isCustom: false, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.6-sol");
});

it("keeps Codex's own default when no preferred model is available", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.5", name: "GPT-5.5", isCustom: false, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});

it("ignores custom models that shadow a preferred slug", () => {
  const models = applyPreferredCodexDefaultModel([
    { slug: "gpt-5.6-sol", name: "gpt-5.6-sol", isCustom: true, capabilities: null },
    { slug: "gpt-5.4", name: "GPT-5.4", isCustom: false, isDefault: true, capabilities: null },
  ]);

  assert.deepStrictEqual(models.find((model) => model.isDefault)?.slug, "gpt-5.4");
});
