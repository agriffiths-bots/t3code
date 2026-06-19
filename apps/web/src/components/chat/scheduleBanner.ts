import type { ThreadScheduleSummary } from "../../state/schedules";
import { formatRelativeTimeUntilLabel } from "../../timestampFormat";

/**
 * The dismissible composer banner descriptor for the open thread's next
 * scheduled run (design.md SURFACE 3). Pure so the push/suppress/overdue/
 * multi-summarize decision is unit-testable without rendering ChatView.
 *
 * Returns `null` when the banner must be suppressed — no active thread, no
 * schedule, the schedule is disabled, or there is no upcoming run. Overdue
 * only swaps the icon + copy; the variant stays "info" (calm, no nag). The
 * `dismissKey` embeds `nextRunAt` so dismissing hides only the current run:
 * when the reactor advances `next_run_at` the key changes and the banner
 * re-surfaces automatically.
 */
export interface ScheduleBannerDescriptor {
  readonly id: string;
  readonly dismissKey: string;
  readonly overdue: boolean;
  readonly title: string;
  readonly description: string;
}

export function buildScheduleBanner(
  activeThreadKey: string | null,
  summary: ThreadScheduleSummary | null,
): ScheduleBannerDescriptor | null {
  if (
    activeThreadKey === null ||
    summary === null ||
    !summary.enabled ||
    summary.nextRunAt === null
  ) {
    return null;
  }
  const nextRunAt = summary.nextRunAt;
  const overdue = summary.overdue;
  const moreSuffix = summary.count > 1 ? ` · +${summary.count - 1} more` : "";
  const nextTime = new Date(nextRunAt);
  const absolute = Number.isNaN(nextTime.getTime())
    ? nextRunAt
    : nextTime.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  return {
    id: `scheduled:${activeThreadKey}`,
    dismissKey: `${activeThreadKey}:${nextRunAt}`,
    overdue,
    title: overdue
      ? "Scheduled task is overdue"
      : `Scheduled task runs ${formatRelativeTimeUntilLabel(nextRunAt)}`,
    description: overdue
      ? `Was due ${absolute} · will run when this thread is free${moreSuffix}`
      : `Next at ${absolute} · ${summary.cadenceLabel} · runs in this thread${moreSuffix}`,
  };
}
