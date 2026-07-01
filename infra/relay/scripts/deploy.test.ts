import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  hasDeployChanges,
  missingRelayPublicConfigFields,
  publicConfigFromOutput,
  reconcileRootEnvPublicConfig,
  reconcileRootEnvRelayUrl,
  RelayDeployError,
  RelayDeployPublicConfigUnavailableError,
  serializeGithubOutput,
  serializeRelayClientTracingEnvironment,
} from "./deploy.ts";

describe("RelayDeployError", () => {
  it("reports the incomplete state source, stage, and missing fields", () => {
    const missingFields = missingRelayPublicConfigFields({
      url: "https://relay.example.test",
      mobileTracingUrl: "https://api.axiom.co/v1/traces",
    });
    const error = new RelayDeployError({
      source: "alchemy_state",
      stage: "production",
      missingFields,
    });

    expect(error).toMatchObject({
      source: "alchemy_state",
      stage: "production",
      missingFields: [
        "mobileTracingDataset",
        "mobileTracingToken",
        "clientTracingUrl",
        "clientTracingDataset",
        "clientTracingToken",
      ],
    });
    expect(error.message).toBe(
      "Relay deploy output from 'alchemy_state' for stage 'production' is missing required public config fields: mobileTracingDataset, mobileTracingToken, clientTracingUrl, clientTracingDataset, clientTracingToken",
    );
  });

  it("distinguishes deploy results that do not produce public config", () => {
    const error = new RelayDeployPublicConfigUnavailableError({
      result: "dry-run",
      stage: "production",
      outputPath: "/tmp/relay-client.env",
    });

    expect(error.message).toBe(
      "Relay deploy result 'dry-run' for stage 'production' did not produce public config required by GitHub environment output '/tmp/relay-client.env'.",
    );
  });
});

describe("hasDeployChanges", () => {
  it("detects resource, binding, and deletion changes", () => {
    expect(hasDeployChanges({ resources: {}, deletions: {} } as never)).toBe(false);
    expect(
      hasDeployChanges({
        resources: {
          api: { action: "create", bindings: [] },
        },
        deletions: {},
      } as never),
    ).toBe(true);
    expect(
      hasDeployChanges({
        resources: {
          api: { action: "noop", bindings: [{ action: "update" }] },
        },
        deletions: {},
      } as never),
    ).toBe(true);
    expect(
      hasDeployChanges({
        resources: {},
        deletions: {
          api: { action: "delete", bindings: [] },
        },
      } as never),
    ).toBe(true);
  });
});

describe("reconcileRootEnvRelayUrl", () => {
  it("adds the relay URL to an empty root env file", () => {
    expect(reconcileRootEnvRelayUrl("", "https://relay.example.test")).toBe(
      "T3CODE_RELAY_URL=https://relay.example.test\n",
    );
  });

  it("preserves unrelated root env entries while replacing a previous relay URL", () => {
    expect(
      reconcileRootEnvRelayUrl(
        "T3CODE_CLERK_PUBLISHABLE_KEY=pk_test_example\nT3CODE_RELAY_URL=https://old.example.test\n",
        "https://relay.example.test",
      ),
    ).toBe(
      "T3CODE_CLERK_PUBLISHABLE_KEY=pk_test_example\nT3CODE_RELAY_URL=https://relay.example.test\n",
    );
  });
});

describe("reconcileRootEnvPublicConfig", () => {
  const config = {
    relayUrl: "https://relay.example.test",
    mobileTracingUrl: "https://api.axiom.co/v1/traces",
    mobileTracingDataset: "t3-code-mobile-traces-dev",
    mobileTracingToken: "xaat-public-ingest",
    clientTracingUrl: "https://api.axiom.co/v1/traces",
    clientTracingDataset: "t3-code-relay-client-traces-dev",
    clientTracingToken: "xaat-relay-client-ingest",
  } as const;

  it("adds the complete local client config", () => {
    expect(reconcileRootEnvPublicConfig("", config)).toBe(
      [
        "T3CODE_RELAY_URL=https://relay.example.test",
        "T3CODE_MOBILE_OTLP_TRACES_URL=https://api.axiom.co/v1/traces",
        "T3CODE_MOBILE_OTLP_TRACES_DATASET=t3-code-mobile-traces-dev",
        "T3CODE_MOBILE_OTLP_TRACES_TOKEN=xaat-public-ingest",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_URL=https://api.axiom.co/v1/traces",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_DATASET=t3-code-relay-client-traces-dev",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_TOKEN=xaat-relay-client-ingest",
        "",
      ].join("\n"),
    );
  });

  it("replaces stale values while preserving unrelated entries", () => {
    expect(
      reconcileRootEnvPublicConfig(
        [
          "T3CODE_CLERK_PUBLISHABLE_KEY=pk_test_example",
          "T3CODE_RELAY_URL=https://old.example.test",
          "T3CODE_MOBILE_OTLP_TRACES_URL=https://old.example.test/v1/traces",
          "T3CODE_MOBILE_OTLP_TRACES_DATASET=old-dataset",
          "T3CODE_MOBILE_OTLP_TRACES_TOKEN=old-token",
          "T3CODE_RELAY_CLIENT_OTLP_TRACES_URL=https://old.example.test/v1/traces",
          "T3CODE_RELAY_CLIENT_OTLP_TRACES_DATASET=old-client-dataset",
          "T3CODE_RELAY_CLIENT_OTLP_TRACES_TOKEN=old-client-token",
          "",
        ].join("\n"),
        config,
      ),
    ).toBe(
      [
        "T3CODE_CLERK_PUBLISHABLE_KEY=pk_test_example",
        "T3CODE_RELAY_URL=https://relay.example.test",
        "T3CODE_MOBILE_OTLP_TRACES_URL=https://api.axiom.co/v1/traces",
        "T3CODE_MOBILE_OTLP_TRACES_DATASET=t3-code-mobile-traces-dev",
        "T3CODE_MOBILE_OTLP_TRACES_TOKEN=xaat-public-ingest",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_URL=https://api.axiom.co/v1/traces",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_DATASET=t3-code-relay-client-traces-dev",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_TOKEN=xaat-relay-client-ingest",
        "",
      ].join("\n"),
    );
  });
});

describe("serializeGithubOutput", () => {
  it("serializes relay deploy metadata for GitHub Actions outputs", () => {
    expect(
      serializeGithubOutput({
        changed: false,
        result: "noop",
        relay_url: "https://relay.example.test",
      }),
    ).toBe("changed=false\nresult=noop\nrelay_url=https://relay.example.test\n");
  });
});

describe("serializeRelayClientTracingEnvironment", () => {
  it("serializes tracing config for downstream GITHUB_ENV loading", () => {
    expect(
      serializeRelayClientTracingEnvironment({
        relayUrl: "https://relay.example.test",
        mobileTracingUrl: "https://api.axiom.co/v1/traces",
        mobileTracingDataset: "mobile",
        mobileTracingToken: "mobile-token",
        clientTracingUrl: "https://api.axiom.co/v1/traces",
        clientTracingDataset: "relay",
        clientTracingToken: "client-token",
      }),
    ).toBe(
      [
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_URL=https://api.axiom.co/v1/traces",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_DATASET=relay",
        "T3CODE_RELAY_CLIENT_OTLP_TRACES_TOKEN=client-token",
        "",
      ].join("\n"),
    );
  });
});

describe("artifact release workflows", () => {
  it.effect("publish app artifacts without relay deployment", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const stableWorkflowPath = yield* path.fromFileUrl(
        new URL("../../../.github/workflows/release.yml", import.meta.url),
      );
      const mainWorkflowPath = yield* path.fromFileUrl(
        new URL("../../../.github/workflows/main-artifacts-release.yml", import.meta.url),
      );
      const reusableWorkflowPath = yield* path.fromFileUrl(
        new URL("../../../.github/workflows/reusable-build-release-artifacts.yml", import.meta.url),
      );
      const ciWorkflowPath = yield* path.fromFileUrl(
        new URL("../../../.github/workflows/ci.yml", import.meta.url),
      );
      const stableWorkflow = yield* fileSystem.readFileString(stableWorkflowPath);
      const mainWorkflow = yield* fileSystem.readFileString(mainWorkflowPath);
      const reusableWorkflow = yield* fileSystem.readFileString(reusableWorkflowPath);
      const ciWorkflow = yield* fileSystem.readFileString(ciWorkflowPath);

      for (const workflow of [stableWorkflow, mainWorkflow]) {
        expect(workflow).toContain(
          "uses: ./.github/workflows/reusable-build-release-artifacts.yml",
        );
        expect(workflow).not.toContain("relay_public_config");
        expect(workflow).not.toContain("client_tracing_token:");
        expect(workflow).not.toContain("deploy-relay");
      }

      expect(stableWorkflow).toContain("name: Stable Artifact Release");
      expect(stableWorkflow).toContain("needs: [metadata, preflight, public_config]");
      expect(stableWorkflow).toContain("run: vp check");
      expect(stableWorkflow).toContain("run: vp run typecheck");
      expect(stableWorkflow).toContain("run: vp run test");
      expect(stableWorkflow).toContain('relay_url="https://$relay_domain"');
      expect(stableWorkflow).toContain("android_required: true");
      expect(stableWorkflow).toContain("android_profile: production-apk");
      expect(stableWorkflow).toContain("android_artifact_name: t3-code-android.apk");
      expect(stableWorkflow).toContain("android_mobile_version_policy: appVersion");
      expect(stableWorkflow).toContain(
        "android_app_version: ${{ needs.metadata.outputs.release_version }}",
      );
      expect(stableWorkflow).toContain(
        "clerk_publishable_key: ${{ needs.public_config.outputs.clerk_publishable_key }}",
      );
      expect(stableWorkflow).toContain("relay_url: ${{ needs.public_config.outputs.relay_url }}");
      expect(stableWorkflow).toContain("prerelease: false");
      expect(stableWorkflow).toContain("make_latest: true");
      expect(stableWorkflow).toContain("windows_signing: true");
      expect(mainWorkflow).toContain("name: Main Artifact Release");
      // Manual-only: landing on main must not auto-publish a release.
      expect(mainWorkflow).toContain("workflow_dispatch:");
      expect(mainWorkflow).not.toContain("push:");
      // ...but a manual dispatch may only build/publish from main, never an arbitrary ref.
      // All three jobs (metadata, public_config, publish_artifacts) must carry the guard,
      // so count occurrences rather than merely asserting presence.
      const mainRefGuard = "if: ${{ github.ref == 'refs/heads/main' }}";
      expect(mainWorkflow.split(mainRefGuard).length - 1).toBe(3);
      expect(mainWorkflow).toContain("needs: [metadata, public_config]");
      expect(mainWorkflow).toContain("android_required: false");
      expect(mainWorkflow).toContain("android_profile: preview");
      expect(mainWorkflow).toContain("android_artifact_name: t3-code-preview-android.apk");
      expect(mainWorkflow).toContain("android_mobile_version_policy: fingerprint");
      expect(mainWorkflow).toContain("android_public_config: false");
      expect(mainWorkflow).toContain("prerelease: ${{ inputs.prerelease }}");
      expect(mainWorkflow).toContain("windows_signing: true");
      expect(reusableWorkflow).toContain("android_mobile_version_policy:");
      expect(reusableWorkflow).toContain("android_app_version:");
      expect(reusableWorkflow).toContain("android_public_config:");
      expect(reusableWorkflow).toContain("MOBILE_APP_VERSION: ${{ inputs.android_app_version }}");
      expect(reusableWorkflow).toContain(
        "T3CODE_CLERK_PUBLISHABLE_KEY: ${{ inputs.android_public_config && inputs.clerk_publishable_key || '' }}",
      );
      expect(reusableWorkflow).toContain(
        "T3CODE_RELAY_URL: ${{ inputs.android_public_config && inputs.relay_url || '' }}",
      );
      expect(reusableWorkflow).toContain(
        "MOBILE_VERSION_POLICY: ${{ inputs.android_mobile_version_policy }}",
      );
      expect(reusableWorkflow).toContain("T3CODE_RELAY_URL: ${{ inputs.relay_url }}");
      // Cloud sign-in config must come only from the caller's validated
      // public_config outputs, never from a raw repo-vars fallback that would
      // bypass the all-or-nothing / HTTPS validation.
      expect(reusableWorkflow).not.toContain("|| vars.CLERK_PUBLISHABLE_KEY");
      expect(reusableWorkflow).not.toContain("|| vars.CLERK_JWT_TEMPLATE");
      expect(reusableWorkflow).not.toContain("|| vars.CLERK_CLI_OAUTH_CLIENT_ID");
      expect(reusableWorkflow).not.toContain("|| vars.T3CODE_RELAY_URL");
      expect(reusableWorkflow).toContain(
        'run: node scripts/update-release-package-versions.ts "${{ inputs.release_version }}"',
      );
      expect(reusableWorkflow).toContain("needs: android_preflight");
      expect(reusableWorkflow).toContain("needs: [android_preflight, build_wsl_node_pty]");
      expect(reusableWorkflow).toContain(
        "if: needs.android_preflight.result == 'success' && needs.build_wsl_node_pty.result == 'success'",
      );
      // Windows is the always-on artifact: publish when it succeeds and Android
      // either built or is not required, so a failing EAS build never blocks it.
      expect(reusableWorkflow).toContain("needs.android_apk.result == 'success' ||");
      expect(reusableWorkflow).toContain("!inputs.android_required");
      // An optional Android failure must not turn the whole release run red.
      expect(reusableWorkflow).toContain("continue-on-error: ${{ !inputs.android_required }}");
      expect(ciWorkflow).toContain("mobile_native_static_analysis:");
      expect(ciWorkflow).toContain("brew bundle install --file apps/mobile/Brewfile");
      expect(ciWorkflow).toContain("run: vp run lint:mobile");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("publicConfigFromOutput", () => {
  it("reads the complete public tracing config from persisted Alchemy output", () => {
    expect(
      publicConfigFromOutput({
        url: "https://relay.example.test",
        mobileTracingUrl: "https://api.axiom.co/v1/traces",
        mobileTracingDataset: "mobile",
        mobileTracingToken: "mobile-token",
        clientTracingUrl: "https://api.axiom.co/v1/traces",
        clientTracingDataset: "relay",
        clientTracingToken: "client-token",
      }),
    ).toEqual({
      relayUrl: "https://relay.example.test",
      mobileTracingUrl: "https://api.axiom.co/v1/traces",
      mobileTracingDataset: "mobile",
      mobileTracingToken: "mobile-token",
      clientTracingUrl: "https://api.axiom.co/v1/traces",
      clientTracingDataset: "relay",
      clientTracingToken: "client-token",
    });
  });

  it("rejects incomplete stack output", () => {
    expect(publicConfigFromOutput({ url: "https://relay.example.test" })).toBeNull();
  });
});
