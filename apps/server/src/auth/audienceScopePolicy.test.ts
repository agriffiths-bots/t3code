import { expect, it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthRelayReadScope,
  ORCHESTRATION_WS_METHODS,
  WS_METHODS,
} from "@t3tools/contracts";

import { isRpcMethodAllowedForAudienceCeiling } from "./audienceScopePolicy.ts";

it.each([
  [ORCHESTRATION_WS_METHODS.replayEvents, AuthOrchestrationReadScope, "factory", true],
  [ORCHESTRATION_WS_METHODS.subscribeScheduledTasks, AuthOrchestrationReadScope, "factory", true],
  [WS_METHODS.subscribeNotificationEvents, AuthOrchestrationReadScope, "factory", true],
  [WS_METHODS.serverAckNotification, AuthOrchestrationReadScope, "factory", true],
  [WS_METHODS.serverGetConfig, AuthOrchestrationReadScope, "factory", false],
  [WS_METHODS.projectsReadFile, AuthOrchestrationReadScope, "factory", false],
  [WS_METHODS.filesystemBrowse, AuthOrchestrationReadScope, "factory", false],
  [WS_METHODS.assetsCreateUrl, AuthOrchestrationReadScope, "factory", false],
  [WS_METHODS.previewList, AuthOrchestrationReadScope, "factory", false],
  ["future-unclassified-read", AuthOrchestrationReadScope, "factory", false],
  [WS_METHODS.serverGetConfig, AuthOrchestrationReadScope, "private", true],
  ["relay-read", AuthRelayReadScope, "factory", true],
] as const)("classifies RPC audience access for %s", (method, scope, audienceCeiling, expected) => {
  expect(isRpcMethodAllowedForAudienceCeiling(method, scope, audienceCeiling)).toBe(expected);
});
