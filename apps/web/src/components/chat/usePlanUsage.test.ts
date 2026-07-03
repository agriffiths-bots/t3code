import type { PreparedConnection } from "@t3tools/client-runtime/connection";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { describe, expect, it } from "vite-plus/test";

import {
  planUsageConnectionKey,
  planUsageDpopNeedsRefresh,
  planUsageRequestCredentials,
  planUsageRequestHeaders,
} from "./usePlanUsage";

type PlanUsageConnection = Parameters<typeof planUsageRequestHeaders>[0];

function preparedConnection(
  input: Pick<PreparedConnection, "httpAuthorization" | "socketHeaders">,
): PlanUsageConnection {
  return Option.some({
    environmentId: "env-test" as PreparedConnection["environmentId"],
    label: "Test",
    httpBaseUrl: "https://example.test",
    socketUrl: "wss://example.test/ws",
    target: {} as PreparedConnection["target"],
    ...input,
  });
}

describe("planUsageRequestHeaders", () => {
  it("uses browser credentials for cookie-backed primary sessions", async () => {
    const connection = preparedConnection({
      httpAuthorization: null,
    });

    await expect(
      planUsageRequestHeaders(connection, "https://example.test/api/plan-usage"),
    ).resolves.toEqual({});
    expect(planUsageRequestCredentials(connection)).toBe("include");
  });

  it("passes bearer tokens through as authorization headers", async () => {
    const connection = preparedConnection({
      httpAuthorization: { _tag: "Bearer", token: "bearer-token" },
      socketHeaders: {
        "cf-access-client-id": "client-id",
        "cf-access-client-secret": "client-secret",
      },
    });
    const headers = await planUsageRequestHeaders(
      connection,
      "https://example.test/api/plan-usage",
    );

    expect(headers).toEqual({
      "cf-access-client-id": "client-id",
      "cf-access-client-secret": "client-secret",
      authorization: "Bearer bearer-token",
    });
    expect(planUsageRequestCredentials(connection)).toBeUndefined();
  });

  it("creates a DPoP proof for the exact usage endpoint request", async () => {
    const connection = preparedConnection({
      httpAuthorization: { _tag: "Dpop", accessToken: "dpop-access-token" },
      socketHeaders: {
        "cf-access-jwt-assertion": "access-jwt",
        cookie: "CF_Authorization=access-jwt",
      },
    });
    const proofInputs: Array<{ method: string; url: string; accessToken: string }> = [];
    const headers = await planUsageRequestHeaders(
      connection,
      "https://example.test/api/plan-usage",
      async (input) => {
        proofInputs.push(input);
        return "signed-proof";
      },
    );

    expect(headers).toEqual({
      "cf-access-jwt-assertion": "access-jwt",
      authorization: "DPoP dpop-access-token",
      dpop: "signed-proof",
    });
    expect(proofInputs).toEqual([
      {
        method: "GET",
        url: "https://example.test/api/plan-usage",
        accessToken: "dpop-access-token",
      },
    ]);
    expect(planUsageRequestCredentials(connection)).toBeUndefined();
  });
});

describe("planUsageConnectionKey", () => {
  it("changes across environment backends without including token values", () => {
    const first = preparedConnection({
      httpAuthorization: { _tag: "Bearer", token: "first-secret" },
    });
    const firstValue = Option.getOrThrow(first);
    const second = Option.some({
      ...firstValue,
      environmentId: "env-other" as PreparedConnection["environmentId"],
      httpBaseUrl: "https://other.example.test",
      httpAuthorization: { _tag: "Bearer" as const, token: "second-secret" },
    });

    expect(planUsageConnectionKey(first)).toBe("env-test:https://example.test:Bearer:default");
    expect(planUsageConnectionKey(second)).toBe(
      "env-other:https://other.example.test:Bearer:default",
    );
  });

  it("changes across provider instances on the same backend", () => {
    const connection = preparedConnection({
      httpAuthorization: { _tag: "Bearer", token: "secret" },
    });

    expect(planUsageConnectionKey(connection, ProviderInstanceId.make("codex_work"))).toBe(
      "env-test:https://example.test:Bearer:codex_work",
    );
  });
});

describe("planUsageDpopNeedsRefresh", () => {
  it("refreshes DPoP connections before the token expires", () => {
    expect(
      planUsageDpopNeedsRefresh(
        preparedConnection({
          httpAuthorization: {
            _tag: "Dpop",
            accessToken: "dpop-access-token",
            expiresAtEpochMs: 160_000,
          },
        }),
        100_000,
      ),
    ).toBe(true);
  });

  it("keeps fresh or non-DPoP connections on the current prepared auth", () => {
    expect(
      planUsageDpopNeedsRefresh(
        preparedConnection({
          httpAuthorization: {
            _tag: "Dpop",
            accessToken: "dpop-access-token",
            expiresAtEpochMs: 161_000,
          },
        }),
        100_000,
      ),
    ).toBe(false);
    expect(
      planUsageDpopNeedsRefresh(
        preparedConnection({
          httpAuthorization: { _tag: "Bearer", token: "bearer-token" },
        }),
        100_000,
      ),
    ).toBe(false);
  });
});
