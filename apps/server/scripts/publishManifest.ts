import { resolveCatalogDependencies } from "../../../scripts/lib/resolve-catalog.ts";

export interface PublishManifestSource {
  readonly name: string;
  readonly repository: {
    readonly type: string;
    readonly url: string;
    readonly directory: string;
  };
  readonly bin: Record<string, string>;
  readonly type: string;
  readonly engines: Record<string, string>;
  readonly files: ReadonlyArray<string>;
  readonly dependencies: Record<string, string>;
  readonly optionalDependencies?: Record<string, string> | undefined;
}

export interface PublishManifest {
  readonly name: string;
  readonly repository: PublishManifestSource["repository"];
  readonly bin: Record<string, string>;
  readonly type: string;
  readonly version: string;
  readonly engines: Record<string, string>;
  readonly files: ReadonlyArray<string>;
  readonly dependencies: Record<string, string>;
  readonly optionalDependencies?: Record<string, string>;
  readonly overrides: Record<string, string>;
}

/**
 * Builds the package manifest published to npm.
 *
 * Optional dependencies travel with it: the CLI bundle externalizes packages
 * such as the Matrix SDK and its native crypto binding, so dropping them here
 * would leave an npm-installed release unable to resolve them at runtime.
 */
export function createPublishManifest(input: {
  readonly source: PublishManifestSource;
  readonly version: string;
  readonly catalog: Record<string, string>;
  readonly overrides: Record<string, string>;
}): PublishManifest {
  const { source, version, catalog, overrides } = input;
  const optionalDependencies = resolveCatalogDependencies(
    source.optionalDependencies ?? {},
    catalog,
    "apps/server",
  );

  return {
    name: source.name,
    repository: source.repository,
    bin: source.bin,
    type: source.type,
    version,
    engines: source.engines,
    files: source.files,
    dependencies: resolveCatalogDependencies(source.dependencies, catalog, "apps/server"),
    ...(Object.keys(optionalDependencies).length > 0 ? { optionalDependencies } : {}),
    overrides: resolveCatalogDependencies(overrides, catalog, "apps/server"),
  };
}
