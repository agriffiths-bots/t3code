import {
  effectiveSettled,
  hasActiveThreadSession,
} from "@t3tools/client-runtime/state/thread-settled";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { EnvironmentId, ProjectId } from "@t3tools/contracts";

/**
 * Thread List v2 model, ported from the web sidebar v2
 * (apps/web/src/components/Sidebar.logic.ts + SidebarV2.tsx).
 *
 * Four visual states, three colors: color is reserved for "act now"
 * (approval), "in motion" (working), and "broken" (failed). Ready is the
 * unlabeled resting state.
 */
export type ThreadListV2Status = "approval" | "input" | "working" | "failed" | "ready";

export function resolveThreadListV2Status(
  thread: Pick<EnvironmentThreadShell, "hasPendingApprovals" | "hasPendingUserInput" | "session">,
): ThreadListV2Status {
  if (thread.hasPendingApprovals) {
    return "approval";
  }
  if (thread.hasPendingUserInput) {
    return "input";
  }
  if (hasActiveThreadSession(thread.session)) {
    return "working";
  }
  if (thread.session?.status === "error") {
    return "failed";
  }
  return "ready";
}

/** NaN-safe Date.parse for sort comparators: a malformed timestamp must not
    poison the whole ordering, so it sinks to the epoch instead. */
function parseTimestampMs(isoDate: string): number {
  const parsed = Date.parse(isoDate);
  return Number.isNaN(parsed) ? 0 : parsed;
}

/** First VALID timestamp wins: a present-yet-malformed string falls through
    to the next candidate rather than sinking the row to the epoch. */
function firstValidTimestampMs(...candidates: ReadonlyArray<string | null | undefined>): number {
  for (const candidate of candidates) {
    if (candidate == null) continue;
    const parsed = Date.parse(candidate);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return 0;
}

/**
 * v2 sort: static creation order, newest thread on top. Activity NEVER
 * reorders the list — a row holds its position from open until settled, so
 * the screen only moves at lifecycle transitions. Mirrors web's
 * sortThreadsForSidebarV2.
 */
export function sortThreadsForListV2<T extends { readonly id: string; readonly createdAt: string }>(
  threads: readonly T[],
): T[] {
  // .sort() on a copy, not .toSorted(): Hermes doesn't ship the ES2023
  // change-by-copy array methods.
  return [...threads].sort(
    (left, right) =>
      parseTimestampMs(right.createdAt) - parseTimestampMs(left.createdAt) ||
      left.id.localeCompare(right.id),
  );
}

export interface ThreadListV2Item {
  readonly thread: EnvironmentThreadShell;
  readonly variant: "card" | "slim";
  /** First settled row after the card block draws the SETTLED divider. */
  readonly showSettledDivider: boolean;
  readonly isLast: boolean;
}

export interface ThreadListV2Layout {
  readonly items: ThreadListV2Item[];
  /** Settled threads beyond the render limit (behind "Show more"). */
  readonly hiddenSettledCount: number;
}

interface ThreadListV2ScopeInput {
  readonly environmentId: EnvironmentId | null;
  readonly projectRef?: {
    readonly environmentId: EnvironmentId;
    readonly projectId: ProjectId;
  } | null;
  readonly searchQuery: string;
}

function threadMatchesListV2Scope(
  thread: EnvironmentThreadShell,
  input: ThreadListV2ScopeInput,
  query: string,
): boolean {
  if (input.environmentId !== null && thread.environmentId !== input.environmentId) return false;
  if (
    input.projectRef != null &&
    (thread.environmentId !== input.projectRef.environmentId ||
      thread.projectId !== input.projectRef.projectId)
  ) {
    return false;
  }
  return query.length === 0 || thread.title.toLocaleLowerCase().includes(query);
}

/**
 * Threads whose effective settlement can change when a PR closes or reopens.
 * This intentionally ignores the current PR state so a PR-derived settled
 * thread keeps its reporter and can return to the active partition on reopen.
 */
export function selectThreadListV2PrSettlementCandidates(
  input: ThreadListV2ScopeInput & {
    readonly threads: ReadonlyArray<EnvironmentThreadShell>;
    readonly settlementEnvironmentIds?: ReadonlySet<EnvironmentId>;
    readonly autoSettleAfterDays?: number | null;
    readonly now?: string;
  },
): EnvironmentThreadShell[] {
  const now = input.now ?? new Date().toISOString();
  const autoSettleAfterDays = input.autoSettleAfterDays ?? null;
  const query = input.searchQuery.trim().toLocaleLowerCase();
  return input.threads.filter((thread) => {
    if (!threadMatchesListV2Scope(thread, input, query)) return false;
    if (!(input.settlementEnvironmentIds?.has(thread.environmentId) ?? true)) return false;
    const activeWithoutPr = !effectiveSettled(thread, {
      now,
      autoSettleAfterDays,
      changeRequestState: null,
    });
    const settledWithClosedPr = effectiveSettled(thread, {
      now,
      autoSettleAfterDays,
      changeRequestState: "closed",
    });
    return activeWithoutPr && settledWithClosedPr;
  });
}

/**
 * Partitions visible threads into the active card block (creation order) and
 * the settled recency tail, matching the web v2 list. Mobile has no
 * client-settings sync yet, so inactivity settlement defaults off here just
 * as it does in the shared web/desktop settings.
 */
export function buildThreadListV2Items(input: {
  readonly threads: ReadonlyArray<EnvironmentThreadShell>;
  readonly environmentId: EnvironmentId | null;
  readonly projectRef?: {
    readonly environmentId: EnvironmentId;
    readonly projectId: ProjectId;
  } | null;
  readonly searchQuery: string;
  /** PR state reported before virtualized row rendering ("env:threadId" keys). */
  readonly changeRequestStateByKey?: ReadonlyMap<string, "open" | "closed" | "merged">;
  /** Environments whose server supports thread.settle/unsettle. Threads on
      other environments never classify as settled — the user could neither
      un-settle nor pin them. Absent = no gating (tests). */
  readonly settlementEnvironmentIds?: ReadonlySet<EnvironmentId>;
  readonly autoSettleAfterDays?: number | null;
  /** Max settled rows to render; the rest are counted, not built. */
  readonly settledLimit?: number;
  /** Injectable for tests; defaults to now. */
  readonly now?: string;
}): ThreadListV2Layout {
  const now = input.now ?? new Date().toISOString();
  const autoSettleAfterDays = input.autoSettleAfterDays ?? null;
  const query = input.searchQuery.trim().toLocaleLowerCase();

  const active: EnvironmentThreadShell[] = [];
  const settled: EnvironmentThreadShell[] = [];
  for (const thread of input.threads) {
    // Callers pass live (unarchived) shells; settled threads are among them
    // and partition into the tail via effectiveSettled.
    if (!threadMatchesListV2Scope(thread, input, query)) continue;
    const supportsSettlement = input.settlementEnvironmentIds?.has(thread.environmentId) ?? true;
    const changeRequestState =
      input.changeRequestStateByKey?.get(`${thread.environmentId}:${thread.id}`) ?? null;
    if (
      supportsSettlement &&
      effectiveSettled(thread, { now, autoSettleAfterDays, changeRequestState })
    ) {
      settled.push(thread);
    } else {
      active.push(thread);
    }
  }

  const orderedActive = sortThreadsForListV2(active);
  const orderedSettled = [...settled].sort(
    (left, right) =>
      firstValidTimestampMs(right.latestUserMessageAt, right.updatedAt) -
      firstValidTimestampMs(left.latestUserMessageAt, left.updatedAt),
  );
  const settledLimit = input.settledLimit ?? Number.POSITIVE_INFINITY;
  const visibleSettled =
    orderedSettled.length > settledLimit ? orderedSettled.slice(0, settledLimit) : orderedSettled;

  const items: ThreadListV2Item[] = [];
  for (const thread of orderedActive) {
    items.push({ thread, variant: "card", showSettledDivider: false, isLast: false });
  }
  for (const [index, thread] of visibleSettled.entries()) {
    items.push({
      thread,
      variant: "slim",
      showSettledDivider: index === 0,
      isLast: false,
    });
  }
  const last = items.at(-1);
  if (last) {
    items[items.length - 1] = { ...last, isLast: true };
  }
  return { items, hiddenSettledCount: orderedSettled.length - visibleSettled.length };
}
