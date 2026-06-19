import { describe, expect, it } from "vite-plus/test";

import { isProjectNotEmptyInvariant } from "./sidebarProjectDelete";

describe("isProjectNotEmptyInvariant", () => {
  it("matches the decoded tagged invariant for project.delete", () => {
    expect(
      isProjectNotEmptyInvariant({
        _tag: "OrchestrationCommandInvariantError",
        commandType: "project.delete",
        detail: "Project 'b2bd5fdc' is not empty and cannot be deleted without force=true.",
      }),
    ).toBe(true);
  });

  it("matches a tagged invariant even if only the message phrase is present", () => {
    expect(
      isProjectNotEmptyInvariant({
        _tag: "OrchestrationCommandInvariantError",
        commandType: "project.delete",
      }),
    ).toBe(true);
  });

  it("matches via the Error message fallback", () => {
    expect(
      isProjectNotEmptyInvariant(
        new Error(
          "Orchestration command invariant failed (project.delete): Project 'b2bd5fdc' is not empty and cannot be deleted without force=true.",
        ),
      ),
    ).toBe(true);
  });

  it("matches via a detail-only object", () => {
    expect(
      isProjectNotEmptyInvariant({
        detail: "Project 'x' is not empty and cannot be deleted without force=true.",
      }),
    ).toBe(true);
  });

  it("matches via a raw string", () => {
    expect(
      isProjectNotEmptyInvariant("… is not empty and cannot be deleted without force=true."),
    ).toBe(true);
  });

  it("does not match unrelated errors", () => {
    expect(isProjectNotEmptyInvariant(new Error("network error"))).toBe(false);
    expect(isProjectNotEmptyInvariant(null)).toBe(false);
    expect(isProjectNotEmptyInvariant(undefined)).toBe(false);
    expect(isProjectNotEmptyInvariant("random failure")).toBe(false);
    expect(
      isProjectNotEmptyInvariant({
        _tag: "OrchestrationCommandInvariantError",
        commandType: "thread.delete",
        detail: "some other invariant",
      }),
    ).toBe(false);
  });
});
