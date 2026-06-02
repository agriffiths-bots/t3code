import { DesktopCloudflaredStatusSchema } from "@t3tools/contracts";
import * as Cloudflared from "@t3tools/shared/cloudflared";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as IpcChannels from "../channels.ts";
import { makeIpcMethod } from "../DesktopIpc.ts";

export const getCloudflaredStatus = makeIpcMethod({
  channel: IpcChannels.GET_CLOUDFLARED_STATUS_CHANNEL,
  payload: Schema.Undefined,
  result: DesktopCloudflaredStatusSchema,
  handler: Effect.fn("desktop.ipc.cloudflared.getStatus")(function* () {
    const cloudflared = yield* Cloudflared.CloudflaredExecutable;
    return yield* cloudflared.resolve;
  }),
});

export const installCloudflared = makeIpcMethod({
  channel: IpcChannels.INSTALL_CLOUDFLARED_CHANNEL,
  payload: Schema.Undefined,
  result: DesktopCloudflaredStatusSchema,
  handler: Effect.fn("desktop.ipc.cloudflared.install")(function* () {
    const cloudflared = yield* Cloudflared.CloudflaredExecutable;
    return yield* cloudflared.install;
  }),
});
