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
export type ThreadListV2Status = "approval" | "input" | "working" | "failed" | "plan" | "ready";

export function resolveThreadListV2Status(
  thread: Pick<
    EnvironmentThreadShell,
    | "hasActionableProposedPlan"
    | "hasPendingApprovals"
    | "hasPendingUserInput"
    | "interactionMode"
    | "latestTurn"
    | "session"
  >,
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
  const latestTurnSettled =
    thread.latestTurn?.startedAt != null &&
    thread.latestTurn.completedAt != null &&
    !hasActiveThreadSession(thread.session);
  if (thread.interactionMode === "plan" && latestTurnSettled && thread.hasActionableProposedPlan) {
    return "plan";
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
  readonly treeDepth: number;
  readonly directChildCount: number;
  readonly unsettledDescendantCount: number;
  /** First settled row after the card block draws the SETTLED divider. */
  readonly showSettledDivider: boolean;
  readonly isLast: boolean;
}

export interface ThreadListV2Layout {
  readonly items: ThreadListV2Item[];
  /** Settled threads beyond the render limit (behind "Show more"). */
  readonly hiddenSettledCount: number;
}

interface ThreadListV2TreeRow {
  readonly thread: EnvironmentThreadShell;
  readonly depth: number;
  readonly directChildCount: number;
  readonly isSettled: boolean;
  readonly unsettledDescendantCount: number;
}

function threadTreeKey(thread: Pick<EnvironmentThreadShell, "environmentId" | "id">): string {
  return `${thread.environmentId}\u0000${thread.id}`;
}

function threadListKey(thread: Pick<EnvironmentThreadShell, "environmentId" | "id">): string {
  return `${thread.environmentId}:${thread.id}`;
}

function parentThreadTreeKey(thread: EnvironmentThreadShell): string | null {
  if (thread.parentThreadId === null) return null;
  return `${thread.parentEnvironmentId ?? thread.environmentId}\u0000${thread.parentThreadId}`;
}

/**
 * Builds stable parent-first rows, then keeps each root tree in one partition.
 * A mixed tree remains in the inbox while its settled members render slim in
 * place; only a wholly settled tree moves to history.
 */
function partitionThreadListV2Trees(
  sortedThreads: ReadonlyArray<EnvironmentThreadShell>,
  isSettled: (thread: EnvironmentThreadShell) => boolean,
): {
  readonly activeRows: ReadonlyArray<ThreadListV2TreeRow>;
  readonly settledGroups: ReadonlyArray<ReadonlyArray<ThreadListV2TreeRow>>;
} {
  const threadByKey = new Map(sortedThreads.map((thread) => [threadTreeKey(thread), thread]));
  const sortIndexByKey = new Map(
    sortedThreads.map((thread, index) => [threadTreeKey(thread), index]),
  );
  const childrenByParentKey = new Map<string, EnvironmentThreadShell[]>();
  const roots: EnvironmentThreadShell[] = [];
  for (const thread of sortedThreads) {
    const parentKey = parentThreadTreeKey(thread);
    if (parentKey === null || parentKey === threadTreeKey(thread) || !threadByKey.has(parentKey)) {
      roots.push(thread);
      continue;
    }
    const children = childrenByParentKey.get(parentKey);
    if (children) children.push(thread);
    else childrenByParentKey.set(parentKey, [thread]);
  }

  const bestSortIndexByKey = new Map<string, number>();
  const visiting = new Set<string>();
  const bestSortIndex = (thread: EnvironmentThreadShell): number => {
    const key = threadTreeKey(thread);
    const cached = bestSortIndexByKey.get(key);
    if (cached !== undefined) return cached;
    if (visiting.has(key)) return sortIndexByKey.get(key) ?? Number.MAX_SAFE_INTEGER;
    visiting.add(key);
    let best = sortIndexByKey.get(key) ?? Number.MAX_SAFE_INTEGER;
    for (const child of childrenByParentKey.get(key) ?? []) {
      best = Math.min(best, bestSortIndex(child));
    }
    visiting.delete(key);
    bestSortIndexByKey.set(key, best);
    return best;
  };
  const orderByTreeSort = (threads: ReadonlyArray<EnvironmentThreadShell>) =>
    [...threads].sort((left, right) => bestSortIndex(left) - bestSortIndex(right));

  const baseRows: Array<{
    readonly thread: EnvironmentThreadShell;
    readonly depth: number;
    readonly directChildCount: number;
  }> = [];
  const emitted = new Set<string>();
  const emit = (thread: EnvironmentThreadShell, depth: number) => {
    const key = threadTreeKey(thread);
    if (emitted.has(key)) return;
    emitted.add(key);
    const children = childrenByParentKey.get(key) ?? [];
    baseRows.push({ thread, depth, directChildCount: children.length });
    for (const child of orderByTreeSort(children)) emit(child, depth + 1);
  };
  for (const root of orderByTreeSort(roots)) emit(root, 0);
  // Cycle-safe fallback: malformed parent links must never drop a thread.
  for (const thread of sortedThreads) emit(thread, 0);

  const settledByIndex = baseRows.map((row) => isSettled(row.thread));
  const unsettledDescendantCountByIndex = baseRows.map(() => 0);
  const ancestors: number[] = [];
  for (const [index, row] of baseRows.entries()) {
    while (ancestors.length > 0 && (baseRows[ancestors.at(-1) ?? -1]?.depth ?? -1) >= row.depth) {
      ancestors.pop();
    }
    if (!settledByIndex[index]) {
      for (const ancestorIndex of ancestors) {
        unsettledDescendantCountByIndex[ancestorIndex] =
          (unsettledDescendantCountByIndex[ancestorIndex] ?? 0) + 1;
      }
    }
    ancestors.push(index);
  }
  const rows: ThreadListV2TreeRow[] = baseRows.map((row, index) => ({
    ...row,
    isSettled: settledByIndex[index] ?? false,
    unsettledDescendantCount: unsettledDescendantCountByIndex[index] ?? 0,
  }));
  const groups: ThreadListV2TreeRow[][] = [];
  for (const row of rows) {
    if (row.depth === 0 || groups.length === 0) groups.push([row]);
    else groups.at(-1)?.push(row);
  }
  const activeRows: ThreadListV2TreeRow[] = [];
  const settledGroups: ThreadListV2TreeRow[][] = [];
  for (const group of groups) {
    if (group.some((row) => !row.isSettled)) activeRows.push(...group);
    else settledGroups.push(group);
  }
  return { activeRows, settledGroups };
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
  /** Max settled root groups to render; groups stay atomic through paging. */
  readonly settledLimit?: number;
  /** Keeps the current settled tree visible beyond the first history page. */
  readonly selectedThreadKey?: string | null;
  /** Injectable for tests; defaults to now. */
  readonly now?: string;
}): ThreadListV2Layout {
  const now = input.now ?? new Date().toISOString();
  const autoSettleAfterDays = input.autoSettleAfterDays ?? null;
  const query = input.searchQuery.trim().toLocaleLowerCase();

  const visibleThreads: EnvironmentThreadShell[] = [];
  const settledByKey = new Map<string, boolean>();
  for (const thread of input.threads) {
    // Callers pass live (unarchived) shells; settled threads are among them
    // and partition into the tail via effectiveSettled.
    if (!threadMatchesListV2Scope(thread, input, query)) continue;
    const supportsSettlement = input.settlementEnvironmentIds?.has(thread.environmentId) ?? true;
    const changeRequestState =
      input.changeRequestStateByKey?.get(`${thread.environmentId}:${thread.id}`) ?? null;
    visibleThreads.push(thread);
    settledByKey.set(
      threadTreeKey(thread),
      supportsSettlement &&
        effectiveSettled(thread, { now, autoSettleAfterDays, changeRequestState }),
    );
  }

  const partition = partitionThreadListV2Trees(
    sortThreadsForListV2(visibleThreads),
    (thread) => settledByKey.get(threadTreeKey(thread)) ?? false,
  );
  const groupActivityAt = (group: ReadonlyArray<ThreadListV2TreeRow>) =>
    Math.max(
      ...group.map((row) =>
        firstValidTimestampMs(row.thread.latestUserMessageAt, row.thread.updatedAt),
      ),
    );
  const orderedSettledGroups = [...partition.settledGroups].sort(
    (left, right) => groupActivityAt(right) - groupActivityAt(left),
  );
  const settledLimit = input.settledLimit ?? Number.POSITIVE_INFINITY;
  const visibleSettledGroupIndexes = new Set<number>();
  for (
    let index = 0;
    index < Math.min(orderedSettledGroups.length, Math.max(0, settledLimit));
    index += 1
  ) {
    visibleSettledGroupIndexes.add(index);
  }
  if (input.selectedThreadKey != null) {
    const selectedGroupIndex = orderedSettledGroups.findIndex((group) =>
      group.some((row) => threadListKey(row.thread) === input.selectedThreadKey),
    );
    if (selectedGroupIndex >= 0) visibleSettledGroupIndexes.add(selectedGroupIndex);
  }
  const visibleSettledGroups = orderedSettledGroups.filter((_, index) =>
    visibleSettledGroupIndexes.has(index),
  );
  const hiddenSettledCount = orderedSettledGroups
    .filter((_, index) => !visibleSettledGroupIndexes.has(index))
    .reduce((count, group) => count + group.length, 0);

  const items: ThreadListV2Item[] = [];
  for (const row of partition.activeRows) {
    items.push({
      thread: row.thread,
      variant: row.isSettled ? "slim" : "card",
      treeDepth: row.depth,
      directChildCount: row.directChildCount,
      unsettledDescendantCount: row.unsettledDescendantCount,
      showSettledDivider: false,
      isLast: false,
    });
  }
  for (const [groupIndex, group] of visibleSettledGroups.entries()) {
    for (const [rowIndex, row] of group.entries()) {
      items.push({
        thread: row.thread,
        variant: "slim",
        treeDepth: row.depth,
        directChildCount: row.directChildCount,
        unsettledDescendantCount: 0,
        showSettledDivider: groupIndex === 0 && rowIndex === 0,
        isLast: false,
      });
    }
  }
  const last = items.at(-1);
  if (last) {
    items[items.length - 1] = { ...last, isLast: true };
  }
  return { items, hiddenSettledCount };
}
