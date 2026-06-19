import { describe, expect, it } from "vite-plus/test";

import { cronToProse, intervalToProse, scheduleCadenceLabel } from "./formatCadence";

describe("intervalToProse", () => {
  it("humanizes minute intervals", () => {
    expect(intervalToProse(1800)).toBe("Every 30 min");
  });

  it("humanizes hour intervals", () => {
    expect(intervalToProse(3600)).toBe("Every 1h");
  });

  it("renders a full day as hours, not days", () => {
    expect(intervalToProse(86400)).toBe("Every 24h");
  });
});

describe("cronToProse", () => {
  it("humanizes a weekday range", () => {
    expect(cronToProse("0 7 * * 1-5")).toBe("Mon–Fri 07:00");
  });

  it("humanizes a daily cron", () => {
    expect(cronToProse("0 2 * * *")).toBe("Daily 02:00");
  });

  it("returns the raw expression unchanged when it cannot be parsed", () => {
    expect(cronToProse("*/5 8-17 * * 1,3,5")).toBe("*/5 8-17 * * 1,3,5");
    expect(cronToProse("not a cron")).toBe("not a cron");
  });
});

describe("scheduleCadenceLabel", () => {
  it("prefers the interval humanizer", () => {
    expect(scheduleCadenceLabel({ intervalSeconds: 1800, cronExpr: null })).toBe("Every 30 min");
  });

  it("falls back to cron prose", () => {
    expect(scheduleCadenceLabel({ intervalSeconds: null, cronExpr: "0 2 * * *" })).toBe(
      "Daily 02:00",
    );
  });

  it("handles a schedule with neither field set", () => {
    expect(scheduleCadenceLabel({ intervalSeconds: null, cronExpr: null })).toBe("On a schedule");
  });
});
