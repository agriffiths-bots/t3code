import { expect, it } from "@effect/vitest";
import { Tool } from "effect/unstable/ai";

import { NotifyToolkit } from "./toolkits/notify/tools.ts";
import { SubagentToolkit } from "./toolkits/subagent/tools.ts";
import { ThreadToolkit } from "./toolkits/thread/tools.ts";
import { UsageToolkit } from "./toolkits/usage/tools.ts";
import { VisibilityToolkit } from "./toolkits/visibility/tools.ts";

const allToolkits = [
  NotifyToolkit,
  SubagentToolkit,
  ThreadToolkit,
  UsageToolkit,
  VisibilityToolkit,
];

// The OpenAI function-calling API requires every advertised tool's parameters
// to be a JSON Schema with top-level `type: "object"`. A single tool that
// violates this (e.g. the `anyOf: [object, array]` that `Schema.Struct({})`
// emits for an empty input) makes the API reject the ENTIRE request with HTTP
// 400 `invalid_function_parameters`, killing every Codex turn that has the
// toolkit attached. Regression: t3_list_backends, 2026-07-08.
it("every MCP tool advertises a top-level object input schema", () => {
  const checked: string[] = [];
  for (const toolkit of allToolkits) {
    for (const tool of Object.values(toolkit.tools)) {
      const schema = Tool.getJsonSchema(tool) as { readonly type?: unknown };
      expect(schema.type, `tool ${tool.name} inputSchema.type`).toBe("object");
      checked.push(tool.name);
    }
  }
  // Guard the guard: if toolkit wiring changes shape, this test must not
  // silently pass on an empty list.
  expect(checked).toHaveLength(11);
  expect(checked).toContain("t3_list_backends");
});
