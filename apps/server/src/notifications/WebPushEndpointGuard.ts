import * as NodeDnsPromises from "node:dns/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off - web-push only accepts a Node HTTPS Agent for request lookup pinning.
import * as NodeHttps from "node:https";
import type * as NodeNet from "node:net";

import { ServerNotificationEndpointError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  getWebPushEndpointValidationError,
  isPublicWebPushIpAddress,
} from "./webPushEndpointPolicy.ts";

export interface WebPushEndpointAddress {
  readonly address: string;
  readonly family: 4 | 6;
}

export interface GuardedWebPushEndpoint {
  readonly agent: NodeHttps.Agent;
  readonly address: WebPushEndpointAddress;
  readonly hostname: string;
}

export class WebPushEndpointGuard extends Context.Service<
  WebPushEndpointGuard,
  {
    readonly prepare: (
      endpoint: string,
    ) => Effect.Effect<GuardedWebPushEndpoint, ServerNotificationEndpointError>;
  }
>()("t3/notifications/WebPushEndpointGuard") {}

function endpointHost(endpoint: string): string | null {
  try {
    const hostname = new URL(endpoint).hostname;
    const withoutBrackets =
      hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;
    return withoutBrackets.endsWith(".")
      ? withoutBrackets.slice(0, -1).toLowerCase()
      : withoutBrackets.toLowerCase();
  } catch {
    return null;
  }
}

function endpointError(endpoint: string, message: string): ServerNotificationEndpointError {
  const host = endpointHost(endpoint);
  return new ServerNotificationEndpointError({
    message,
    ...(host === null || host.length === 0 ? {} : { endpointHost: host }),
  });
}

function isIpLiteral(hostname: string): boolean {
  return hostname.includes(":") || /^\d{1,3}(?:\.\d{1,3}){3}$/.test(hostname);
}

function addressFamily(family: number): 4 | 6 | null {
  return family === 4 || family === 6 ? family : null;
}

const defaultResolve = (hostname: string) =>
  Effect.tryPromise({
    try: () => NodeDnsPromises.lookup(hostname, { all: true, verbatim: true }),
    catch: (cause) =>
      endpointError(
        `https://${hostname}/`,
        `Failed to resolve web push endpoint host: ${String(cause)}`,
      ),
  }).pipe(
    Effect.map((entries) =>
      entries.flatMap((entry): ReadonlyArray<WebPushEndpointAddress> => {
        const family = addressFamily(entry.family);
        return family === null ? [] : [{ address: entry.address, family }];
      }),
    ),
  );

function makePinnedAgent(hostname: string, address: WebPushEndpointAddress): NodeHttps.Agent {
  const lookup: NodeNet.LookupFunction = (requestedHostname, options, callback) => {
    if (requestedHostname.toLowerCase() !== hostname) {
      callback(
        Object.assign(new Error("Unexpected web push endpoint host"), {
          code: "ERR_T3_WEB_PUSH_HOST",
        }),
        "",
        4,
      );
      return;
    }
    if (options.all) {
      callback(null, [address]);
      return;
    }
    callback(null, address.address, address.family);
  };
  return new NodeHttps.Agent({ lookup });
}

function validateResolvedAddresses(
  endpoint: string,
  addresses: ReadonlyArray<WebPushEndpointAddress>,
): Effect.Effect<WebPushEndpointAddress, ServerNotificationEndpointError> {
  if (addresses.length === 0) {
    return Effect.fail(endpointError(endpoint, "Web push endpoint host did not resolve"));
  }
  const blocked = addresses.find((address) => !isPublicWebPushIpAddress(address.address));
  if (blocked !== undefined) {
    return Effect.fail(
      endpointError(endpoint, "Web push endpoint resolved to a non-public address"),
    );
  }
  return Effect.succeed(addresses[0] as WebPushEndpointAddress);
}

export const make = (resolve = defaultResolve) =>
  WebPushEndpointGuard.of({
    prepare: Effect.fn("WebPushEndpointGuard.prepare")(function* (endpoint) {
      const staticValidationError = getWebPushEndpointValidationError(endpoint);
      if (staticValidationError !== null) {
        return yield* endpointError(endpoint, staticValidationError);
      }
      const hostname = endpointHost(endpoint);
      if (hostname === null) {
        return yield* endpointError(
          endpoint,
          "Web push endpoint must be an HTTPS URL with a public host",
        );
      }
      const address = yield* isIpLiteral(hostname)
        ? validateResolvedAddresses(endpoint, [
            { address: hostname, family: hostname.includes(":") ? 6 : 4 },
          ])
        : resolve(hostname).pipe(
            Effect.flatMap((addresses) => validateResolvedAddresses(endpoint, addresses)),
          );
      return {
        agent: makePinnedAgent(hostname, address),
        address,
        hostname,
      };
    }),
  });

export const layer = Layer.succeed(WebPushEndpointGuard, make());
