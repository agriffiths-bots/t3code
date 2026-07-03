import {
  type AuthClientPresentationMetadata,
  type AuthEnvironmentScope,
  type DesktopSshEnvironmentBootstrap,
  type DesktopSshEnvironmentTarget,
  EnvironmentId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { ConnectionAttemptError } from "../connection/model.ts";

const HEADER_OPTIONS_CAPABLE_WEBSOCKET = Symbol.for(
  "@t3tools/client-runtime/WebSocket.headerOptionsCapable",
);

type HeaderOptionsCapableWebSocketConstructor = {
  readonly [HEADER_OPTIONS_CAPABLE_WEBSOCKET]?: true;
};

type WebSocketHeaderOptions = {
  readonly headers?: Readonly<Record<string, string>>;
};

type HeaderOptionsWebSocketConstructor = {
  new (
    url: string | URL,
    protocols?: string | string[],
    options?: WebSocketHeaderOptions,
  ): globalThis.WebSocket;
};

export function markWebSocketHeaderOptionsCapable<T extends object>(constructor: T): T {
  if (
    (constructor as HeaderOptionsCapableWebSocketConstructor)[HEADER_OPTIONS_CAPABLE_WEBSOCKET] !==
    true
  ) {
    Object.defineProperty(constructor, HEADER_OPTIONS_CAPABLE_WEBSOCKET, {
      configurable: false,
      enumerable: false,
      value: true,
    });
  }
  return constructor;
}

function isConstructor(value: unknown): boolean {
  if (typeof value !== "function") {
    return false;
  }
  try {
    Reflect.construct(String, [], value);
    return true;
  } catch {
    return false;
  }
}

export function makeHeaderOptionsCapableWebSocketConstructor(
  webSocketConstructor: HeaderOptionsWebSocketConstructor,
): (
  url: string | URL,
  protocols?: string | string[],
  options?: WebSocketHeaderOptions,
) => globalThis.WebSocket {
  function HeaderOptionsCapableWebSocket(
    url: string | URL,
    protocols?: string | string[],
    options?: WebSocketHeaderOptions,
  ) {
    return new webSocketConstructor(url, protocols, options);
  }
  return markWebSocketHeaderOptionsCapable(HeaderOptionsCapableWebSocket);
}

export function canPassWebSocketHeaderOptions(
  webSocketConstructor: unknown = globalThis.WebSocket,
) {
  if (!isConstructor(webSocketConstructor)) {
    return false;
  }
  if (
    (webSocketConstructor as HeaderOptionsCapableWebSocketConstructor)[
      HEADER_OPTIONS_CAPABLE_WEBSOCKET
    ] === true
  ) {
    return true;
  }
  const navigatorProduct = (globalThis.navigator as { readonly product?: string } | undefined)
    ?.product;
  if (navigatorProduct === "ReactNative") {
    return true;
  }
  return false;
}

export interface PreparedSshEnvironment {
  readonly bootstrap: DesktopSshEnvironmentBootstrap;
  readonly bearerToken: string;
}

export interface ProvisionedSshEnvironment extends PreparedSshEnvironment {
  readonly environmentId: EnvironmentId;
  readonly label: string;
}

export class CloudSession extends Context.Service<
  CloudSession,
  {
    readonly clerkToken: Effect.Effect<string, ConnectionAttemptError>;
  }
>()("@t3tools/client-runtime/platform/capabilities/CloudSession") {}

export class RelayDeviceIdentity extends Context.Service<
  RelayDeviceIdentity,
  {
    readonly deviceId: Effect.Effect<Option.Option<string>, ConnectionAttemptError>;
  }
>()("@t3tools/client-runtime/platform/capabilities/RelayDeviceIdentity") {}

export class ClientPresentation extends Context.Service<
  ClientPresentation,
  {
    readonly metadata: AuthClientPresentationMetadata;
    readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
  }
>()("@t3tools/client-runtime/platform/capabilities/ClientPresentation") {}

export class PrimaryEnvironmentAuth extends Context.Service<
  PrimaryEnvironmentAuth,
  {
    readonly bearerToken: Effect.Effect<Option.Option<string>, ConnectionAttemptError>;
  }
>()("@t3tools/client-runtime/platform/capabilities/PrimaryEnvironmentAuth") {}

export class SshEnvironmentGateway extends Context.Service<
  SshEnvironmentGateway,
  {
    readonly provision: (
      target: DesktopSshEnvironmentTarget,
    ) => Effect.Effect<ProvisionedSshEnvironment, ConnectionAttemptError>;
    readonly prepare: (input: {
      readonly connectionId: string;
      readonly expectedEnvironmentId: EnvironmentId;
      readonly target: DesktopSshEnvironmentTarget;
    }) => Effect.Effect<PreparedSshEnvironment, ConnectionAttemptError>;
    readonly disconnect: (
      target: DesktopSshEnvironmentTarget,
    ) => Effect.Effect<void, ConnectionAttemptError>;
  }
>()("@t3tools/client-runtime/platform/capabilities/SshEnvironmentGateway") {}
