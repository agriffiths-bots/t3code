import type {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderRuntimeEvent,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";

export interface CapturedProviderRuntimeEventBinding {
  readonly threadId: ThreadId;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId | undefined;
  readonly runtimeMode: RuntimeMode | undefined;
  readonly cwd: string | undefined;
}

// ProviderService publishes the same immutable event object to every local
// subscriber. A WeakMap preserves the binding captured at canonical-stream
// ingress without changing the protocol envelope or retaining drained events.
const bindingByEvent = new WeakMap<ProviderRuntimeEvent, CapturedProviderRuntimeEventBinding>();

export function captureProviderRuntimeEventBinding(
  event: ProviderRuntimeEvent,
  binding: CapturedProviderRuntimeEventBinding | undefined,
): void {
  if (binding !== undefined) bindingByEvent.set(event, binding);
}

export function getCapturedProviderRuntimeEventBinding(
  event: ProviderRuntimeEvent,
): CapturedProviderRuntimeEventBinding | undefined {
  return bindingByEvent.get(event);
}
