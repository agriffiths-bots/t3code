import type { PlanUsageSnapshot, PlanUsageWindow } from "@t3tools/contracts";

export function flattenPlanUsageWindows(snapshot: PlanUsageSnapshot | null): PlanUsageWindow[] {
  if (!snapshot) return [];
  return snapshot.providers.flatMap((provider) => provider.windows);
}

export function selectMostConstrainedPlanUsageWindow(
  snapshot: PlanUsageSnapshot | null,
): PlanUsageWindow | null {
  const windows = flattenPlanUsageWindows(snapshot).filter(
    (window) => Number.isFinite(window.usedPercent) && window.usedPercent >= 0,
  );
  if (windows.length === 0) return null;
  return windows.reduce((selected, window) =>
    planUsagePriority(window) > planUsagePriority(selected) ? window : selected,
  );
}

export function formatPlanUsageReset(resetAt: string | null, now = Date.now()): string {
  if (!resetAt) return "Reset unavailable";
  const time = Date.parse(resetAt);
  if (!Number.isFinite(time)) return "Reset unavailable";
  const deltaMs = time - now;
  if (deltaMs <= 0) return "Reset due";
  const minutes = Math.ceil(deltaMs / 60_000);
  if (minutes < 60) return `Resets in ${minutes}m`;
  const hours = Math.ceil(minutes / 60);
  if (hours < 48) return `Resets in ${hours}h`;
  const days = Math.ceil(hours / 24);
  return `Resets in ${days}d`;
}

export function formatPlanUsageValue(window: PlanUsageWindow): string {
  const percent =
    window.usedPercent < 10
      ? window.usedPercent.toFixed(1)
      : Math.round(window.usedPercent).toString();
  if (window.used !== null && window.limit !== null) {
    const unit = window.unit ? ` ${window.unit}` : "";
    return `${percent}% · ${window.used}/${window.limit}${unit}`;
  }
  return `${percent}%`;
}

export function planUsageColor(window: PlanUsageWindow): string {
  switch (window.severity) {
    case "critical":
      return "var(--color-red-500)";
    case "warning":
      return "var(--color-amber-500)";
    case "normal":
    case "info":
    case null:
      return window.usedPercent > 90 ? "var(--color-red-500)" : "var(--color-blue-500)";
  }
}

function severityRank(window: PlanUsageWindow): number {
  switch (window.severity) {
    case "critical":
      return 3;
    case "warning":
      return 2;
    case "normal":
    case "info":
    case null:
      return 1;
  }
}

function planUsagePriority(window: PlanUsageWindow): number {
  return severityRank(window) * 1_000 + Math.max(0, Math.min(100, window.usedPercent));
}
