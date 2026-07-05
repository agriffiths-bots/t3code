import {
  ServerNotifyInput,
  ServerNotifyResult,
  ThreadId,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ThreadStartToolError } from "../thread/tools.ts";

const dependencies = [McpInvocationContext.McpInvocationContext];

export const NotifyInput = ServerNotifyInput;
export type NotifyInput = typeof NotifyInput.Type;

export const NotifyOutput = Schema.Struct({
  ...ServerNotifyResult.fields,
  threadId: ThreadId,
  deepLink: TrimmedNonEmptyString,
});
export type NotifyOutput = typeof NotifyOutput.Type;

export const NotifyTool = Tool.make("t3_notify", {
  description:
    "Send a native notification to the user's registered T3 Code devices. Use this only for important state changes the user should notice outside the active chat, such as needing approval/input, a long-running task finishing, or a failure that needs attention. If deepLink is omitted, the notification opens this thread.",
  parameters: NotifyInput,
  success: NotifyOutput,
  failure: ThreadStartToolError,
  dependencies,
})
  .annotate(Tool.Title, "Notify T3 Code devices")
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

export const NotifyToolkit = Toolkit.make(NotifyTool);
