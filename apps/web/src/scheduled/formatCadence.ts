import type { ScheduledTaskEntry } from "@t3tools/contracts";

/**
 * Humanize a schedule cadence into the VISIBLE Badge label. The raw cron
 * expression is never the label — it only appears in a Tooltip (design.md
 * SURFACE 1 + ACCESSIBILITY CONTRACT) — so these helpers always return prose
 * a screen reader can speak, falling back to the raw string only when a cron
 * is unparseable.
 */

export function intervalToProse(intervalSeconds: number): string {
  if (!Number.isFinite(intervalSeconds) || intervalSeconds <= 0) {
    return "On a schedule";
  }
  if (intervalSeconds % 3_600 === 0) {
    // Hours, not days: the test contract wants intervalToProse(86400)="Every 24h".
    return `Every ${intervalSeconds / 3_600}h`;
  }
  if (intervalSeconds % 60 === 0) {
    return `Every ${intervalSeconds / 60} min`;
  }
  return `Every ${intervalSeconds}s`;
}

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"] as const;

function parseDayOfWeek(field: string): ReadonlyArray<number> | null {
  if (field === "*") {
    return [0, 1, 2, 3, 4, 5, 6];
  }
  // Only the simple "a-b" range or single day are humanized; anything else
  // (lists, steps, names) falls through to the raw-cron fallback.
  const rangeMatch = /^(\d)-(\d)$/.exec(field);
  if (rangeMatch) {
    const start = Number(rangeMatch[1]);
    const end = Number(rangeMatch[2]);
    if (start > 6 || end > 6 || start > end) {
      return null;
    }
    const days: number[] = [];
    for (let day = start; day <= end; day += 1) {
      days.push(day);
    }
    return days;
  }
  if (/^\d$/.test(field)) {
    const day = Number(field);
    return day <= 6 ? [day] : null;
  }
  return null;
}

function formatDays(days: ReadonlyArray<number>): string | null {
  if (days.length === 7) {
    return null; // every day -> "Daily"
  }
  if (days.length === 1) {
    return DAY_NAMES[days[0]!]!;
  }
  // Contiguous range -> "Mon–Fri" (en dash); otherwise list -> "Mon, Wed, Fri".
  const isContiguous = days.every((day, index) => index === 0 || day === days[index - 1]! + 1);
  if (isContiguous) {
    return `${DAY_NAMES[days[0]!]!}–${DAY_NAMES[days[days.length - 1]!]!}`;
  }
  return days.map((day) => DAY_NAMES[day]!).join(", ");
}

/**
 * Humanize the small, common subset of 5-field cron the scheduler emits:
 * `M H * * DOW` with literal minute/hour. Anything outside that subset returns
 * the raw expression unchanged so the label is never misleading.
 */
export function cronToProse(cronExpr: string): string {
  const trimmed = cronExpr.trim();
  const fields = trimmed.split(/\s+/);
  if (fields.length !== 5) {
    return cronExpr;
  }
  const [minute, hour, dayOfMonth, month, dayOfWeek] = fields as [
    string,
    string,
    string,
    string,
    string,
  ];
  if (dayOfMonth !== "*" || month !== "*") {
    return cronExpr;
  }
  if (!/^\d{1,2}$/.test(minute) || !/^\d{1,2}$/.test(hour)) {
    return cronExpr;
  }
  const minuteNum = Number(minute);
  const hourNum = Number(hour);
  if (minuteNum > 59 || hourNum > 23) {
    return cronExpr;
  }
  const time = `${String(hourNum).padStart(2, "0")}:${String(minuteNum).padStart(2, "0")}`;
  const days = parseDayOfWeek(dayOfWeek);
  if (days === null) {
    return cronExpr;
  }
  const dayLabel = formatDays(days);
  return dayLabel === null ? `Daily ${time}` : `${dayLabel} ${time}`;
}

/**
 * The single entry point the Card/icon use: pick the right humanizer for the
 * schedule kind, falling back to a generic label when neither field is set.
 */
export function scheduleCadenceLabel(
  task: Pick<ScheduledTaskEntry, "intervalSeconds" | "cronExpr">,
): string {
  if (task.intervalSeconds !== null) {
    return intervalToProse(task.intervalSeconds);
  }
  if (task.cronExpr !== null && task.cronExpr.trim().length > 0) {
    return cronToProse(task.cronExpr);
  }
  return "On a schedule";
}

export function isCronSchedule(
  task: Pick<ScheduledTaskEntry, "intervalSeconds" | "cronExpr">,
): boolean {
  return task.intervalSeconds === null && task.cronExpr !== null;
}
