import { describe, expect, it } from "vite-plus/test";

import {
  compareSemverVersions,
  normalizeBuildVersion,
  normalizeSemverVersion,
  satisfiesSemverRange,
} from "./semver.ts";

describe("semver helpers", () => {
  it("matches supported range groups", () => {
    const range = "^22.16 || ^23.11 || >=24.10";

    expect(satisfiesSemverRange("22.16.0", range)).toBe(true);
    expect(satisfiesSemverRange("23.11.1", range)).toBe(true);
    expect(satisfiesSemverRange("24.10.0", range)).toBe(true);
    expect(satisfiesSemverRange("22.15.9", range)).toBe(false);
    expect(satisfiesSemverRange("23.10.9", range)).toBe(false);
    expect(satisfiesSemverRange("24.9.9", range)).toBe(false);
  });

  it("normalizes versions with a missing patch segment", () => {
    expect(normalizeSemverVersion("2.1")).toBe("2.1.0");
  });

  it("accepts stamped build versions and rejects malformed values", () => {
    expect(normalizeBuildVersion(" 0.0.29-nightly.20260710.30 ")).toBe(
      "0.0.29-nightly.20260710.30",
    );
    expect(normalizeBuildVersion("1.2.3+build.4")).toBe("1.2.3+build.4");
    expect(normalizeBuildVersion("1.2.3+01")).toBe("1.2.3+01");
    expect(normalizeBuildVersion("01.2.3")).toBeUndefined();
    expect(normalizeBuildVersion("1.02.3")).toBeUndefined();
    expect(normalizeBuildVersion("1.2.03")).toBeUndefined();
    expect(normalizeBuildVersion("1.2.3-01")).toBeUndefined();
    expect(normalizeBuildVersion("1.2.3-alpha.01")).toBeUndefined();
    expect(normalizeBuildVersion("not-semver")).toBeUndefined();
  });

  it("compares prerelease versions before stable versions", () => {
    expect(compareSemverVersions("2.1.111-beta.1", "2.1.111")).toBeLessThan(0);
  });

  it("falls back to lexical comparison for malformed numeric segments", () => {
    expect(compareSemverVersions("1.2.3abc", "1.2.10")).toBeGreaterThan(0);
  });

  it("supports comparison comparators", () => {
    expect(satisfiesSemverRange("24.9.0", ">=24.0 <24.10")).toBe(true);
    expect(satisfiesSemverRange("24.10.0", ">=24.0 <24.10")).toBe(false);
  });

  it("honors caret range upper bounds for zero-major versions", () => {
    expect(satisfiesSemverRange("0.2.3", "^0.2.3")).toBe(true);
    expect(satisfiesSemverRange("0.2.9", "^0.2.3")).toBe(true);
    expect(satisfiesSemverRange("0.3.0", "^0.2.3")).toBe(false);
    expect(satisfiesSemverRange("0.5.0", "^0.2.3")).toBe(false);
    expect(satisfiesSemverRange("0.0.3", "^0.0.3")).toBe(true);
    expect(satisfiesSemverRange("0.0.4", "^0.0.3")).toBe(false);
  });

  it("rejects invalid versions and unsupported range syntax", () => {
    expect(satisfiesSemverRange("not-a-version", ">=24.0")).toBe(false);
    expect(satisfiesSemverRange("24.10.0", "~24.10")).toBe(false);
  });

  it("keeps the range checker stringifiable and executable as plain JavaScript", () => {
    const source = satisfiesSemverRange.toString();
    const recreated = Function(`return (${source});`)() as typeof satisfiesSemverRange;

    expect(source).toContain("function satisfiesSemverRange");
    expect(source).not.toContain(": string");
    expect(source).not.toContain(": boolean");
    expect(recreated("24.10.0", ">=24.10")).toBe(true);
    expect(recreated("24.9.9", ">=24.10")).toBe(false);
  });
});
