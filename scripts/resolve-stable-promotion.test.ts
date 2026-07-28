import { assert, it } from "@effect/vitest";

import { parseStableTag, resolveStablePromotionPlan } from "./resolve-stable-promotion.ts";

it("selects the highest plain-semver tag and increments only its patch", () => {
  assert.deepStrictEqual(
    resolveStablePromotionPlan([
      "v0.0.31",
      "v0.0.99-nightly.20260721.1",
      "v0.1.0",
      "nightly-v9.0.0",
      "v0.0.32-rc.1",
    ]),
    {
      latestTag: "v0.1.0",
      nextTag: "v0.1.1",
      nextVersion: "0.1.1",
    },
  );
});

it("handles semver components beyond Number.MAX_SAFE_INTEGER", () => {
  assert.deepStrictEqual(resolveStablePromotionPlan(["v1.2.9007199254740993"]), {
    latestTag: "v1.2.9007199254740993",
    nextTag: "v1.2.9007199254740994",
    nextVersion: "1.2.9007199254740994",
  });
});

it("refuses to advance after a manual stable-tag race", () => {
  assert.throws(
    () => resolveStablePromotionPlan(["v0.0.31", "v0.0.32"], "v0.0.31"),
    /expected v0\.0\.31, found v0\.0\.32/,
  );
});

it("refuses malformed expected tags and repositories without a stable line", () => {
  assert.throws(
    () => resolveStablePromotionPlan(["v0.0.31"], "v0.0.31-nightly.1"),
    /is not plain semver/,
  );
  assert.throws(
    () => resolveStablePromotionPlan(["v0.0.31-nightly.20260721.1"]),
    /No plain-semver stable tag exists/,
  );
});

it("accepts canonical stable tags only", () => {
  assert.deepStrictEqual(parseStableTag("v1.2.3"), {
    major: 1n,
    minor: 2n,
    patch: 3n,
  });
  for (const tag of ["1.2.3", "v01.2.3", "v1.2", "v1.2.3+build", "v1.2.3-rc.1"]) {
    assert.isUndefined(parseStableTag(tag));
  }
});
