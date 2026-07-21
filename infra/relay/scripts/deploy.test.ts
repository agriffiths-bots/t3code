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
      const verifiedNightlyWorkflowPath = yield* path.fromFileUrl(
        new URL("../../../.github/workflows/verified-nightly-promotion.yml", import.meta.url),
      );
      const ciWorkflowPath = yield* path.fromFileUrl(
        new URL("../../../.github/workflows/ci.yml", import.meta.url),
      );
      const stableWorkflow = yield* fileSystem.readFileString(stableWorkflowPath);
      const mainWorkflow = yield* fileSystem.readFileString(mainWorkflowPath);
      const reusableWorkflow = yield* fileSystem.readFileString(reusableWorkflowPath);
      const verifiedNightlyWorkflow = yield* fileSystem.readFileString(verifiedNightlyWorkflowPath);
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
      expect(stableWorkflow).toContain("Mobile app builds are deprecated");
      expect(stableWorkflow).not.toContain("android_required:");
      expect(stableWorkflow).not.toContain("android_profile:");
      expect(stableWorkflow).not.toContain("android_artifact_name:");
      expect(stableWorkflow).not.toContain("android_mobile_version_policy:");
      expect(stableWorkflow).not.toContain("android_app_version:");
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
      // Nightly build at 23:00 UTC (manual dispatch still supported).
      // Nightly cron is offset off minute zero to dodge GitHub's top-of-hour
      // scheduler congestion (minute-zero runs get delayed/dropped under load).
      expect(mainWorkflow).toContain('cron: "17 23 * * *"');
      // ...but a manual dispatch may only build/publish from main, never an arbitrary ref.
      // All three jobs (metadata, public_config, publish_artifacts) must carry the guard,
      // so count occurrences rather than merely asserting presence.
      const mainRefGuard = "if: ${{ github.ref == 'refs/heads/main' }}";
      expect(mainWorkflow.split(mainRefGuard).length - 1).toBe(3);
      expect(mainWorkflow).toContain("needs: [metadata, public_config]");
      expect(mainWorkflow).toContain("release_tag: ${{ steps.nightly.outputs.tag }}");
      expect(mainWorkflow).toContain("release_name: ${{ steps.nightly.outputs.name }}");
      expect(mainWorkflow).not.toContain("release_tag=main-");
      expect(mainWorkflow).not.toContain("T3 Code main");
      expect(mainWorkflow).not.toContain("Fail the prerelease unless the Android APK is built");
      expect(mainWorkflow).not.toContain("android_required:");
      expect(mainWorkflow).not.toContain("android_profile:");
      expect(mainWorkflow).not.toContain("android_artifact_name:");
      expect(mainWorkflow).not.toContain("android_mobile_version_policy:");
      expect(mainWorkflow).not.toContain("android_app_version:");
      expect(mainWorkflow).not.toContain("android_public_config:");
      expect(mainWorkflow).not.toContain("platform:");
      expect(mainWorkflow).not.toContain("inputs.prerelease");
      expect(mainWorkflow).toContain("prerelease: true");
      expect(mainWorkflow).toContain("windows_signing: true");
      expect(mainWorkflow).toContain("stable_baseline_tag:");
      expect(mainWorkflow).toContain("name: verified-nightly-source");
      expect(mainWorkflow).toContain("sourceRunId: $sourceRunId");
      // Nightly prereleases would otherwise accumulate one release/day forever;
      // a retention job prunes stale nightly prereleases after a successful publish.
      expect(mainWorkflow).toContain("prune_nightly:");
      expect(mainWorkflow).toContain("Keep only the 14 most recent nightly prereleases");
      expect(mainWorkflow).toContain(
        'test("^(?:nightly-)?v[0-9]+\\\\.[0-9]+\\\\.[0-9]+-nightly\\\\.[0-9]{8}\\\\.[0-9]+$")',
      );
      expect(mainWorkflow).toContain("needs: publish_artifacts");
      expect(mainWorkflow).toContain(
        "if: ${{ github.ref == 'refs/heads/main' && needs.publish_artifacts.result == 'success' }}",
      );
      expect(mainWorkflow).toContain(
        'gh api "repos/$GH_REPO/contents/client-verified-latest.json?ref=client-verified-latest"',
      );
      expect(mainWorkflow).toContain("Keeping verified client pointer release");
      expect(mainWorkflow).toContain('jq -r --arg keep "$pinned_release_tag"');
      expect(mainWorkflow).toContain("and .tagName != $keep");
      expect(reusableWorkflow).toContain("Mobile app builds are deprecated");
      expect(reusableWorkflow).not.toContain("android_mobile_version_policy:");
      expect(reusableWorkflow).not.toContain("android_app_version:");
      expect(reusableWorkflow).not.toContain("android_public_config:");
      expect(reusableWorkflow).not.toContain("android_required:");
      expect(reusableWorkflow).not.toContain("android_profile:");
      expect(reusableWorkflow).not.toContain("EXPO_TOKEN:");
      expect(reusableWorkflow).not.toContain("MOBILE_APP_VERSION:");
      expect(reusableWorkflow).not.toContain("MOBILE_VERSION_POLICY:");
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
      expect(reusableWorkflow).toContain("needs: build_wsl_node_pty");
      expect(reusableWorkflow).toContain(
        "needs: [server_e2e_gate, windows_x64, windows_launch_smoke]",
      );
      expect(reusableWorkflow).not.toContain("android_preflight");
      expect(reusableWorkflow).not.toContain("android_apk");
      expect(reusableWorkflow).not.toContain("inputs.platform");
      expect(reusableWorkflow).toContain("needs.windows_x64.result == 'success'");
      expect(reusableWorkflow).toContain("needs.windows_launch_smoke.result == 'success'");
      expect(reusableWorkflow).toContain("publish_release:");
      expect(reusableWorkflow).toContain("artifact_suffix:");
      expect(reusableWorkflow).toContain("if: inputs.publish_release");
      expect(reusableWorkflow).toContain("if: ${{ !inputs.publish_release }}");
      expect(reusableWorkflow).toContain("name: release-assets${{ inputs.artifact_suffix }}");

      expect(verifiedNightlyWorkflow).toContain("name: Verified Nightly Promotion");
      expect(verifiedNightlyWorkflow).toContain('workflows: ["Main Artifact Release"]');
      expect(verifiedNightlyWorkflow).toContain("Refuse rerun-to-green promotion");
      expect(verifiedNightlyWorkflow).toContain("Refuse rerun-to-green publication");
      expect(verifiedNightlyWorkflow).toContain("github.event.workflow_run.run_attempt != 1");
      expect(verifiedNightlyWorkflow).toContain("Provider E2E Gate");
      expect(verifiedNightlyWorkflow).toContain(
        "apps/server/src/orchestration/Layers/ProviderRuntimeIngestion.test.ts",
      );
      expect(verifiedNightlyWorkflow).toContain(
        "apps/server/src/provider/Layers/fixtures/recorded-claude-agent-*-turn.jsonl",
      );
      expect(verifiedNightlyWorkflow).toContain(
        "apps/server/src/provider/Layers/CursorAdapter.test.ts",
      );
      expect(verifiedNightlyWorkflow).toContain("Peer-spawn Characterization Gate");
      expect(verifiedNightlyWorkflow).toContain(
        "apps/server/src/mcp/toolkits/subagent/handlers.test.ts",
      );
      expect(verifiedNightlyWorkflow).toContain(
        "run-id: ${{ needs.source.outputs.source_run_id }}",
      );
      expect(verifiedNightlyWorkflow).toContain("name: windows-x64");
      expect(verifiedNightlyWorkflow).toContain("publish_release: false");
      expect(verifiedNightlyWorkflow).toContain("artifact_suffix: -stable-promotion");
      expect(verifiedNightlyWorkflow).toContain('--expected-latest "$STABLE_BASELINE_TAG"');
      expect(verifiedNightlyWorkflow).toContain('gh release create "$RELEASE_TAG"');
      expect(verifiedNightlyWorkflow).toContain(
        'gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs"',
      );
      expect(verifiedNightlyWorkflow).toContain('-f sha="$SOURCE_SHA"');
      expect(verifiedNightlyWorkflow).toContain("--verify-tag");
      expect(verifiedNightlyWorkflow).toContain("--latest");
      expect(ciWorkflow).not.toContain("mobile_native_static_analysis:");
      expect(ciWorkflow).not.toContain("brew bundle install --file apps/mobile/Brewfile");
      expect(ciWorkflow).not.toContain("run: vp run lint:mobile");
      expect(ciWorkflow).not.toContain("Prebuild mobile Android config");
      expect(ciWorkflow).not.toContain(
        "pnpm exec expo prebuild --clean --platform android --no-install",
      );
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
