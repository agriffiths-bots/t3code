import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Socket from "effect/unstable/socket/Socket";

import { makeHeaderOptionsCapableWebSocketConstructor } from "@t3tools/client-runtime/platform";
import { remoteHttpClientLayer } from "@t3tools/client-runtime/rpc";

import { cryptoLayer } from "../features/cloud/dpop";
import { managedRelayClientLayer } from "../features/cloud/managedRelayLayer";
import { resolveCloudPublicConfig } from "../features/cloud/publicConfig";
import { tracingLayer } from "../features/observability/tracing";
import * as Persistence from "../persistence/layer";

function configuredRelayUrl(): string {
  return resolveCloudPublicConfig().relay.url ?? "http://relay.invalid";
}

const httpClientLayer = remoteHttpClientLayer(fetch);
const webSocketConstructorLayer = Layer.sync(Socket.WebSocketConstructor, () =>
  makeHeaderOptionsCapableWebSocketConstructor(globalThis.WebSocket),
);

type RuntimeLayerSource =
  | ReturnType<typeof managedRelayClientLayer>
  | typeof webSocketConstructorLayer
  | typeof cryptoLayer
  | typeof httpClientLayer
  | typeof Persistence.layer
  | typeof tracingLayer;

const runtimeLayer = Layer.merge(
  managedRelayClientLayer(configuredRelayUrl()),
  webSocketConstructorLayer,
).pipe(
  Layer.provideMerge(cryptoLayer),
  Layer.provideMerge(httpClientLayer),
  Layer.provideMerge(tracingLayer.pipe(Layer.provide(httpClientLayer))),
  Layer.provideMerge(Persistence.layer),
);

export const runtime: ManagedRuntime.ManagedRuntime<
  Layer.Success<RuntimeLayerSource>,
  Layer.Error<RuntimeLayerSource>
> = ManagedRuntime.make(runtimeLayer);

export const runtimeContextLayer: Layer.Layer<
  Layer.Success<RuntimeLayerSource>,
  Layer.Error<RuntimeLayerSource>
> = Layer.effectContext(runtime.contextEffect);
