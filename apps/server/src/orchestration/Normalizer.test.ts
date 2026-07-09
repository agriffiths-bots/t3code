import { CommandId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import { rejectReservedClientCommandId } from "./Normalizer.ts";

describe("normalizeDispatchCommand", () => {
  it.effect("rejects client-supplied command ids in the immediate steer namespace", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        rejectReservedClientCommandId(CommandId.make("server:subagent-steer-immediate:spoofed")),
      );

      expect(Exit.isFailure(exit)).toBe(true);
      expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "").toContain(
        "reserved server namespace",
      );
    }),
  );

  it.effect("allows existing live COS server command ids", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        rejectReservedClientCommandId(CommandId.make("server:cos-wake:thread-1")),
      );

      expect(Exit.isSuccess(exit)).toBe(true);
    }),
  );
});
