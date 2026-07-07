import type { ProviderInstanceId, ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import {
  ListBackendsToolError,
  VisibilityToolkit,
  type BackendProject,
  type BackendSummary,
  type ListBackendsOutput,
  type UnknownBackendProject,
} from "./tools.ts";

const backendLabel = (provider: ServerProvider): string =>
  provider.displayName ?? provider.instanceId;

const backendAvailable = (provider: ServerProvider): boolean =>
  provider.availability !== "unavailable" &&
  provider.enabled &&
  provider.installed &&
  (provider.status === "ready" || provider.status === "warning");

const projectSummary = (project: {
  readonly id: BackendProject["id"];
  readonly title: BackendProject["title"];
  readonly workspaceRoot: BackendProject["workspaceRoot"];
  readonly defaultModelSelection: BackendProject["defaultModelSelection"];
}): BackendProject => ({
  id: project.id,
  title: project.title,
  workspaceRoot: project.workspaceRoot,
  defaultModelSelection: project.defaultModelSelection,
});

const toToolError = (error: unknown): ListBackendsToolError =>
  new ListBackendsToolError({
    message: error instanceof Error ? error.message : "Failed to list T3 Code backends.",
  });

const makeHandlers = Effect.fn("VisibilityToolkit.makeHandlers")(function* () {
  const providerRegistry = yield* ProviderRegistry;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;

  const listBackends = () =>
    Effect.gen(function* () {
      const [providers, shellSnapshot] = yield* Effect.all(
        [providerRegistry.getProviders, projectionSnapshotQuery.getShellSnapshot()],
        { concurrency: "unbounded" },
      );

      const projectsByInstance = new Map<ProviderInstanceId, BackendProject[]>();
      const unassignedProjects: BackendProject[] = [];
      const unknownBackendProjects: UnknownBackendProject[] = [];
      const providerIds = new Set(providers.map((provider) => provider.instanceId));

      for (const project of shellSnapshot.projects) {
        const summary = projectSummary(project);
        const instanceId = project.defaultModelSelection?.instanceId;
        if (!instanceId) {
          unassignedProjects.push(summary);
          continue;
        }

        if (!providerIds.has(instanceId)) {
          unknownBackendProjects.push({ ...summary, requestedInstanceId: instanceId });
          continue;
        }

        const bucket = projectsByInstance.get(instanceId) ?? [];
        bucket.push(summary);
        projectsByInstance.set(instanceId, bucket);
      }

      return {
        backends: providers.map(
          (provider): BackendSummary => ({
            instanceId: provider.instanceId,
            driver: provider.driver,
            label: backendLabel(provider),
            ...(provider.displayName !== undefined ? { displayName: provider.displayName } : {}),
            enabled: provider.enabled,
            installed: provider.installed,
            status: provider.status,
            availability: provider.availability ?? "available",
            available: backendAvailable(provider),
            models: provider.models.map((model) => ({ slug: model.slug, name: model.name })),
            projects: projectsByInstance.get(provider.instanceId) ?? [],
          }),
        ),
        unassignedProjects,
        unknownBackendProjects,
      } satisfies ListBackendsOutput;
    }).pipe(Effect.mapError(toToolError));

  return {
    t3_list_backends: listBackends,
  } satisfies Parameters<typeof VisibilityToolkit.toLayer>[0];
});

export const VisibilityToolkitHandlersLive = Layer.unwrap(
  makeHandlers().pipe(Effect.map((handlers) => VisibilityToolkit.toLayer(handlers))),
);
