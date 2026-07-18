import * as Equal from "effect/Equal";
import {
  formatDuration,
  workEntryIndicatesToolNeutralStatus,
  workLogEntryIsToolLike,
  type TimelineEntry,
  type WorkLogEntry,
} from "../../session-logic";
import { type ChatMessage, type ProposedPlan, type TurnDiffSummary } from "../../types";
import { type MessageId, type OrchestrationLatestTurn, type TurnId } from "@t3tools/contracts";

export const MAX_VISIBLE_WORK_LOG_ENTRIES = 1;
export const TIMELINE_MINIMAP_ITEM_SPACING = 8;
export const TIMELINE_MINIMAP_MIN_ITEMS = 2;
export const TIMELINE_MINIMAP_MAX_HEIGHT_CSS = "calc(100vh - 18rem)";
export const TIMELINE_CONTENT_MAX_WIDTH = 768;
export const TIMELINE_MINIMAP_PERSISTENT_GUTTER = 48;

export interface TimelineEndState {
  readonly isAtEnd?: boolean;
  readonly isNearEnd?: boolean;
}

export function resolveTimelineIsAtEnd(state: TimelineEndState | undefined): boolean | undefined {
  return state?.isNearEnd ?? state?.isAtEnd;
}

export function resolveTimelineMinimapHeightStyle(itemCount: number): string {
  const naturalHeight = Math.max(1, (itemCount - 1) * TIMELINE_MINIMAP_ITEM_SPACING);
  return `min(${naturalHeight}px, ${TIMELINE_MINIMAP_MAX_HEIGHT_CSS})`;
}

export function resolveTimelineMinimapTopPercent(index: number, itemCount: number): number {
  if (itemCount <= 1) {
    return 0;
  }
  return (Math.max(0, Math.min(index, itemCount - 1)) / (itemCount - 1)) * 100;
}

export function resolveTimelineMinimapIndexFromPointer(input: {
  readonly itemCount: number;
  readonly railTop: number;
  readonly railHeight: number;
  readonly pointerY: number;
}): number | null {
  if (input.itemCount <= 0 || input.railHeight <= 0) {
    return null;
  }
  if (input.itemCount === 1) {
    return 0;
  }

  const progress = Math.max(0, Math.min(1, (input.pointerY - input.railTop) / input.railHeight));
  return Math.max(0, Math.min(input.itemCount - 1, Math.round(progress * (input.itemCount - 1))));
}

export function resolveTimelineMinimapHasPersistentGutter(viewportWidth: number): boolean {
  if (!Number.isFinite(viewportWidth) || viewportWidth <= 0) {
    return false;
  }

  const contentWidth = Math.min(viewportWidth, TIMELINE_CONTENT_MAX_WIDTH);
  const sideGutter = Math.max(0, (viewportWidth - contentWidth) / 2);
  return sideGutter >= TIMELINE_MINIMAP_PERSISTENT_GUTTER;
}

function computeElapsedMs(startIso: string, endIso: string): number | null {
  const start = Date.parse(startIso);
  const end = Date.parse(endIso);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  return Math.max(0, end - start);
}

function maxIsoTimestamp(a: string | null, b: string | null): string | null {
  if (a === null) return b;
  if (b === null) return a;
  const aMs = Date.parse(a);
  const bMs = Date.parse(b);
  if (!Number.isFinite(aMs)) return b;
  if (!Number.isFinite(bMs)) return a;
  return bMs > aMs ? b : a;
}

export interface TimelineDurationMessage {
  id: string;
  role: "user" | "assistant" | "system";
  text?: string | null | undefined;
  createdAt: string;
  updatedAt: string;
  streaming: boolean;
}

export type TimelineLatestTurn = Pick<
  OrchestrationLatestTurn,
  "turnId" | "state" | "startedAt" | "completedAt" | "effectiveModel"
> &
  Partial<Pick<OrchestrationLatestTurn, "assistantMessageId">>;

export type MessagesTimelineRow =
  | {
      kind: "work";
      id: string;
      createdAt: string;
      groupedEntries: WorkLogEntry[];
    }
  | {
      kind: "work-toggle";
      id: string;
      createdAt: string;
      groupId: string;
      hiddenCount: number;
      expanded: boolean;
      onlyToolEntries: boolean;
    }
  | {
      kind: "turn-fold";
      id: string;
      createdAt: string;
      turnId: TurnId;
      label: string;
      expanded: boolean;
    }
  | {
      kind: "message";
      id: string;
      createdAt: string;
      message: ChatMessage;
      durationStart: string;
      showAssistantMeta: boolean;
      showAssistantCopyButton: boolean;
      assistantCopyStreaming: boolean;
      effectiveModel?: string | undefined;
      assistantTurnDiffSummary?: TurnDiffSummary | undefined;
      revertTurnCount?: number | undefined;
    }
  | {
      kind: "proposed-plan";
      id: string;
      createdAt: string;
      proposedPlan: ProposedPlan;
    }
  | { kind: "working"; id: string; createdAt: string | null };

export interface StableMessagesTimelineRowsState {
  byId: Map<string, MessagesTimelineRow>;
  result: MessagesTimelineRow[];
}

export function computeMessageDurationStart(
  messages: ReadonlyArray<TimelineDurationMessage>,
): Map<string, string> {
  const result = new Map<string, string>();
  let lastBoundary: string | null = null;

  for (const message of messages) {
    if (isTurnPromptMessage(message)) {
      lastBoundary = message.createdAt;
    }
    result.set(message.id, lastBoundary ?? message.createdAt);
    if (message.role === "assistant" && !message.streaming) {
      lastBoundary = message.updatedAt;
    }
  }

  return result;
}

export function isSubAgentWakeSystemMessageText(text: string | null | undefined): boolean {
  return text?.trimStart().startsWith("[sub-agent ") ?? false;
}

export function isTurnPromptMessage(
  message: Pick<TimelineDurationMessage, "role" | "text">,
): boolean {
  return (
    message.role === "user" ||
    (message.role === "system" && isSubAgentWakeSystemMessageText(message.text))
  );
}

export function deriveRevertTurnCountByPromptMessageId(input: {
  readonly timelineEntries: ReadonlyArray<TimelineEntry>;
  readonly turnDiffSummaryByAssistantMessageId: ReadonlyMap<MessageId, TurnDiffSummary>;
  readonly turnDiffSummaryByTurnId: ReadonlyMap<TurnId, TurnDiffSummary>;
  readonly inferredCheckpointTurnCountByTurnId: Readonly<Record<TurnId, number>>;
}): Map<MessageId, number> {
  const byPromptMessageId = new Map<MessageId, number>();

  const resolveTurnCount = (
    summary: TurnDiffSummary | undefined,
    turnId: TurnId,
  ): number | null => {
    const turnCount =
      summary?.checkpointTurnCount ?? input.inferredCheckpointTurnCountByTurnId[turnId];
    return typeof turnCount === "number" ? turnCount : null;
  };

  for (let index = 0; index < input.timelineEntries.length; index += 1) {
    const entry = input.timelineEntries[index];
    if (!entry || entry.kind !== "message" || !isTurnPromptMessage(entry.message)) {
      continue;
    }

    if (entry.message.turnId !== null) {
      const turnCount = resolveTurnCount(
        input.turnDiffSummaryByTurnId.get(entry.message.turnId),
        entry.message.turnId,
      );
      if (turnCount !== null) {
        byPromptMessageId.set(entry.message.id, Math.max(0, turnCount - 1));
        continue;
      }
    }

    for (let nextIndex = index + 1; nextIndex < input.timelineEntries.length; nextIndex += 1) {
      const nextEntry = input.timelineEntries[nextIndex];
      if (!nextEntry || nextEntry.kind !== "message") {
        continue;
      }
      if (isTurnPromptMessage(nextEntry.message)) {
        break;
      }
      const summary = input.turnDiffSummaryByAssistantMessageId.get(nextEntry.message.id);
      if (!summary) {
        continue;
      }
      const turnCount = resolveTurnCount(summary, summary.turnId);
      if (turnCount === null) {
        break;
      }
      byPromptMessageId.set(entry.message.id, Math.max(0, turnCount - 1));
      break;
    }
  }

  return byPromptMessageId;
}

export function normalizeCompactToolLabel(value: string): string {
  return value.replace(/\s+(?:complete|completed)\s*$/i, "").trim();
}

export function resolveAssistantMessageCopyState({
  text,
  showCopyButton,
  streaming,
}: {
  text: string | null;
  showCopyButton: boolean;
  streaming: boolean;
}) {
  const hasText = text !== null && text.trim().length > 0;
  return {
    text: hasText ? text : null,
    visible: showCopyButton && hasText && !streaming,
  };
}

function deriveFinalAssistantBoundariesByTurnId(input: {
  readonly latestTurn: TimelineLatestTurn | null;
  readonly turns: ReadonlyArray<TimelineLatestTurn>;
  readonly turnDiffSummaryByAssistantMessageId: ReadonlyMap<MessageId, TurnDiffSummary>;
  readonly turnDiffSummaryByTurnId: ReadonlyMap<TurnId, TurnDiffSummary>;
}): ReadonlyMap<string, ReadonlySet<string>> {
  const boundariesByTurnId = new Map<string, Set<string>>();
  const ensureBoundary = (turnId: TurnId): Set<string> => {
    const key = String(turnId);
    let boundary = boundariesByTurnId.get(key);
    if (!boundary) {
      boundary = new Set<string>();
      boundariesByTurnId.set(key, boundary);
    }
    return boundary;
  };

  const addTurnBoundary = (turn: TimelineLatestTurn): void => {
    if (
      turn.completedAt === null ||
      turn.state === "running" ||
      turn.assistantMessageId === undefined
    ) {
      return;
    }
    const boundary = ensureBoundary(turn.turnId);
    if (turn.assistantMessageId !== null) {
      boundary.add(String(turn.assistantMessageId));
    }
  };

  for (const turn of input.turns) {
    addTurnBoundary(turn);
  }

  for (const summary of input.turnDiffSummaryByTurnId.values()) {
    const boundary = ensureBoundary(summary.turnId);
    if (summary.assistantMessageId !== null) {
      boundary.add(String(summary.assistantMessageId));
    }
  }

  for (const [assistantMessageId, summary] of input.turnDiffSummaryByAssistantMessageId) {
    ensureBoundary(summary.turnId).add(String(assistantMessageId));
  }

  const latestTurn = input.latestTurn;
  if (latestTurn) {
    addTurnBoundary(latestTurn);
  }

  return boundariesByTurnId;
}

function deriveEffectiveFinalAssistantBoundariesByTurnId(
  timelineEntries: ReadonlyArray<TimelineEntry>,
  boundariesByTurnId: ReadonlyMap<string, ReadonlySet<string>>,
): ReadonlyMap<string, ReadonlySet<string>> {
  const entriesByTurnId = new Map<string, TimelineEntry[]>();
  for (const entry of timelineEntries) {
    const turnId =
      entry.kind === "message" && entry.message.role === "assistant"
        ? (entry.message.turnId ?? null)
        : entry.kind === "work"
          ? (entry.entry.turnId ?? null)
          : null;
    if (turnId === null) {
      continue;
    }
    const key = String(turnId);
    const entries = entriesByTurnId.get(key);
    if (entries) {
      entries.push(entry);
    } else {
      entriesByTurnId.set(key, [entry]);
    }
  }

  const effectiveBoundariesByTurnId = new Map<string, ReadonlySet<string>>(boundariesByTurnId);
  for (const [turnId, boundary] of boundariesByTurnId) {
    const entries = entriesByTurnId.get(turnId) ?? [];
    if (boundary.size === 0) {
      const fallbackAssistantMessageId = findTerminalAssistantAfterLastWork(entries);
      if (fallbackAssistantMessageId !== null) {
        effectiveBoundariesByTurnId.set(turnId, new Set([fallbackAssistantMessageId]));
      }
      continue;
    }
    const lastAssistantMessageId = findLastAssistantMessageId(entries);
    if (lastAssistantMessageId === null) {
      continue;
    }
    if (boundary.has(lastAssistantMessageId)) {
      continue;
    }
    const terminalAssistantMessageId = findTerminalAssistantAfterLastWork(entries);
    if (terminalAssistantMessageId !== null) {
      effectiveBoundariesByTurnId.set(turnId, new Set([terminalAssistantMessageId]));
      continue;
    }
    effectiveBoundariesByTurnId.set(turnId, new Set());
  }

  return effectiveBoundariesByTurnId;
}

function findLastAssistantMessageId(entries: ReadonlyArray<TimelineEntry>): string | null {
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    if (entry?.kind === "message" && entry.message.role === "assistant") {
      return String(entry.message.id);
    }
  }
  return null;
}

function findTerminalAssistantAfterLastWork(entries: ReadonlyArray<TimelineEntry>): string | null {
  let lastWorkIndex = -1;
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    if (entries[index]?.kind === "work") {
      lastWorkIndex = index;
      break;
    }
  }
  for (let index = entries.length - 1; index > lastWorkIndex; index -= 1) {
    const entry = entries[index];
    if (entry?.kind === "message" && entry.message.role === "assistant") {
      return String(entry.message.id);
    }
  }
  return null;
}

function deriveTerminalNullTurnAssistantMessageIds(
  timelineEntries: ReadonlyArray<TimelineEntry>,
): ReadonlySet<string> {
  const terminalAssistantMessageIdByResponseIndex = new Map<number, string>();
  let responseIndex = 0;

  for (const timelineEntry of timelineEntries) {
    if (timelineEntry.kind !== "message") {
      continue;
    }
    const { message } = timelineEntry;
    if (isTurnPromptMessage(message)) {
      responseIndex += 1;
      continue;
    }
    if (message.role === "assistant" && message.turnId === null) {
      terminalAssistantMessageIdByResponseIndex.set(responseIndex, message.id);
    }
  }

  return new Set(terminalAssistantMessageIdByResponseIndex.values());
}

function deriveFinalAssistantEntryIds(
  entries: ReadonlyArray<TimelineEntry>,
  finalAssistantMessageIds: ReadonlySet<string>,
): ReadonlySet<string> {
  const finalEntryIds = new Set<string>();
  let foundTerminalAssistant = false;

  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    if (!entry) continue;

    if (entry.kind === "work") {
      if (foundTerminalAssistant) {
        break;
      }
      continue;
    }

    if (entry.kind === "message" && entry.message.role === "assistant") {
      if (!foundTerminalAssistant) {
        if (!finalAssistantMessageIds.has(String(entry.message.id))) {
          continue;
        }
        foundTerminalAssistant = true;
      }
      finalEntryIds.add(entry.id);
      continue;
    }

    if (foundTerminalAssistant) {
      break;
    }
  }

  return finalEntryIds;
}

interface TurnFold {
  turnId: TurnId;
  anchorEntryId: string;
  createdAt: string;
  hiddenEntryIds: ReadonlySet<string>;
  label: string;
}

/**
 * The session's running turn is authoritative when latestTurn briefly lags or
 * regresses behind it. Otherwise, the latest turn counts as unsettled while it
 * is still running (or has not recorded a completion). This is deliberately
 * keyed on turn lifecycle rather than transient working state: right after the
 * user sends a message, the previous turn is still the "active" one until the
 * server creates the new turn, and folding must not flicker through that window.
 */
function deriveUnsettledTurnId(
  latestTurn: TimelineLatestTurn | null,
  runningTurnId: TurnId | null,
): TurnId | null {
  if (runningTurnId !== null) {
    return runningTurnId;
  }
  if (!latestTurn) {
    return null;
  }
  const isSettled = latestTurn.completedAt !== null && latestTurn.state !== "running";
  return isSettled ? null : latestTurn.turnId;
}

/**
 * Settled turns fold work activity behind a "Worked for ..." row anchored at
 * the turn's first work entry. Assistant text remains visible in chronological
 * order, including commentary emitted between tool calls.
 */
function deriveTurnFolds(input: {
  timelineEntries: ReadonlyArray<TimelineEntry>;
  finalAssistantBoundariesByTurnId: ReadonlyMap<string, ReadonlySet<string>>;
  latestTurn: TimelineLatestTurn | null;
  unsettledTurnId: TurnId | null;
}): ReadonlyMap<string, TurnFold> {
  interface TurnGroup {
    entries: Array<TimelineEntry>;
    terminalEntry: Extract<TimelineEntry, { kind: "message" }> | null;
    hasStreamingMessage: boolean;
    /**
     * The user message that kicked the turn off. Entry timestamps alone
     * undercount the duration (the first entry appears only once the
     * provider starts producing output), and a turn cut short by a steer may
     * hold a single instantaneous commentary message.
     */
    startBoundary: string | null;
  }
  const groupsByTurnId = new Map<TurnId, TurnGroup>();

  let pendingUserBoundary: string | null = null;
  for (const entry of input.timelineEntries) {
    if (entry.kind === "message" && isTurnPromptMessage(entry.message)) {
      pendingUserBoundary = entry.message.createdAt;
      continue;
    }
    const turnId =
      entry.kind === "message" && entry.message.role === "assistant"
        ? (entry.message.turnId ?? null)
        : entry.kind === "work"
          ? (entry.entry.turnId ?? null)
          : null;
    if (!turnId) {
      continue;
    }
    let group = groupsByTurnId.get(turnId);
    if (!group) {
      group = {
        entries: [],
        terminalEntry: null,
        hasStreamingMessage: false,
        // Each user boundary starts at most one turn; a second turn after the
        // same user message (e.g. a steer-superseded continuation) falls back
        // to its own first entry.
        startBoundary: pendingUserBoundary,
      };
      pendingUserBoundary = null;
      groupsByTurnId.set(turnId, group);
    }
    group.entries.push(entry);
    if (entry.kind === "message") {
      if (entry.message.streaming) {
        group.hasStreamingMessage = true;
      }
    }
  }

  const foldsByAnchorEntryId = new Map<string, TurnFold>();
  for (const [turnId, group] of groupsByTurnId) {
    if (turnId === input.unsettledTurnId) {
      continue;
    }
    if (group.hasStreamingMessage) {
      continue;
    }
    const finalAssistantBoundary = input.finalAssistantBoundariesByTurnId.get(String(turnId));
    const hasExplicitFinalBoundary = finalAssistantBoundary !== undefined;
    const finalAssistantEntryIds = hasExplicitFinalBoundary
      ? deriveFinalAssistantEntryIds(group.entries, finalAssistantBoundary)
      : new Set<string>();
    const hiddenEntryIds = new Set<string>();
    for (const entry of group.entries) {
      if (
        entry.kind === "work" ||
        (hasExplicitFinalBoundary &&
          entry.kind === "message" &&
          entry.message.role === "assistant" &&
          !finalAssistantEntryIds.has(entry.id))
      ) {
        hiddenEntryIds.add(entry.id);
        continue;
      }
      if (
        entry.kind === "message" &&
        entry.message.role === "assistant" &&
        finalAssistantEntryIds.has(entry.id)
      ) {
        group.terminalEntry = entry;
      }
    }
    if (hiddenEntryIds.size === 0) {
      continue;
    }

    const firstEntry = group.entries.find((entry) => hiddenEntryIds.has(entry.id));
    const lastEntry = group.entries.at(-1);
    if (!firstEntry || !lastEntry) {
      continue;
    }

    const isLatestInterruptedTurn =
      input.latestTurn?.turnId === turnId && input.latestTurn.state === "interrupted";
    // A turn cut short by a steer leaves trailing work entries behind its
    // terminal message — take whichever ended last.
    const lastEntryEnd =
      lastEntry.kind === "message" ? lastEntry.message.updatedAt : lastEntry.createdAt;
    const elapsedMs =
      input.latestTurn?.turnId === turnId &&
      input.latestTurn.startedAt &&
      input.latestTurn.completedAt
        ? computeElapsedMs(input.latestTurn.startedAt, input.latestTurn.completedAt)
        : computeElapsedMs(
            group.startBoundary ?? firstEntry.createdAt,
            maxIsoTimestamp(group.terminalEntry?.message.updatedAt ?? null, lastEntryEnd) ??
              lastEntryEnd,
          );
    const duration = elapsedMs !== null ? formatDuration(elapsedMs) : null;
    const label = isLatestInterruptedTurn
      ? duration
        ? `You stopped after ${duration}`
        : "You stopped this response"
      : duration
        ? `Worked for ${duration}`
        : "Worked";

    foldsByAnchorEntryId.set(firstEntry.id, {
      turnId,
      anchorEntryId: firstEntry.id,
      createdAt: firstEntry.createdAt,
      hiddenEntryIds,
      label,
    });
  }
  return foldsByAnchorEntryId;
}

export function deriveMessagesTimelineRows(input: {
  timelineEntries: ReadonlyArray<TimelineEntry>;
  latestTurn?: TimelineLatestTurn | null;
  runningTurnId?: TurnId | null;
  turns?: ReadonlyArray<TimelineLatestTurn>;
  expandedTurnIds?: ReadonlySet<TurnId>;
  expandedWorkGroupIds?: ReadonlySet<string>;
  isWorking: boolean;
  activeTurnStartedAt: string | null;
  turnDiffSummaryByAssistantMessageId: ReadonlyMap<MessageId, TurnDiffSummary>;
  turnDiffSummaryByTurnId?: ReadonlyMap<TurnId, TurnDiffSummary>;
  revertTurnCountByUserMessageId: ReadonlyMap<MessageId, number>;
}): MessagesTimelineRow[] {
  const nextRows: MessagesTimelineRow[] = [];
  const durationStartByMessageId = computeMessageDurationStart(
    input.timelineEntries.flatMap((entry) => (entry.kind === "message" ? [entry.message] : [])),
  );
  const explicitFinalAssistantBoundariesByTurnId = deriveFinalAssistantBoundariesByTurnId({
    latestTurn: input.latestTurn ?? null,
    turns: input.turns ?? [],
    turnDiffSummaryByAssistantMessageId: input.turnDiffSummaryByAssistantMessageId,
    turnDiffSummaryByTurnId: input.turnDiffSummaryByTurnId ?? new Map(),
  });
  const finalAssistantBoundariesByTurnId = deriveEffectiveFinalAssistantBoundariesByTurnId(
    input.timelineEntries,
    explicitFinalAssistantBoundariesByTurnId,
  );
  const effectiveModelByTurnId = new Map<string, string>();
  for (const turn of input.turns ?? []) {
    if (turn.effectiveModel) {
      effectiveModelByTurnId.set(String(turn.turnId), turn.effectiveModel);
    }
  }
  if (input.latestTurn?.effectiveModel) {
    effectiveModelByTurnId.set(String(input.latestTurn.turnId), input.latestTurn.effectiveModel);
  }
  const terminalNullTurnAssistantMessageIds = deriveTerminalNullTurnAssistantMessageIds(
    input.timelineEntries,
  );
  const unsettledTurnId = deriveUnsettledTurnId(
    input.latestTurn ?? null,
    input.runningTurnId ?? null,
  );
  const foldsByAnchorEntryId = deriveTurnFolds({
    timelineEntries: input.timelineEntries,
    finalAssistantBoundariesByTurnId,
    latestTurn: input.latestTurn ?? null,
    unsettledTurnId,
  });
  const collapsedEntryIds = new Set<string>();
  for (const fold of foldsByAnchorEntryId.values()) {
    if (!input.expandedTurnIds?.has(fold.turnId)) {
      for (const entryId of fold.hiddenEntryIds) {
        collapsedEntryIds.add(entryId);
      }
    }
  }

  for (let index = 0; index < input.timelineEntries.length; index += 1) {
    const timelineEntry = input.timelineEntries[index];
    if (!timelineEntry) {
      continue;
    }

    const turnFold = foldsByAnchorEntryId.get(timelineEntry.id);
    if (turnFold) {
      nextRows.push({
        kind: "turn-fold",
        id: `turn-fold:${turnFold.turnId}`,
        createdAt: turnFold.createdAt,
        turnId: turnFold.turnId,
        label: turnFold.label,
        expanded: input.expandedTurnIds?.has(turnFold.turnId) ?? false,
      });
    }

    if (collapsedEntryIds.has(timelineEntry.id)) {
      continue;
    }

    if (timelineEntry.kind === "work") {
      const groupedEntries = [timelineEntry.entry];
      let cursor = index + 1;
      while (cursor < input.timelineEntries.length) {
        const nextEntry = input.timelineEntries[cursor];
        if (
          !nextEntry ||
          nextEntry.kind !== "work" ||
          collapsedEntryIds.has(nextEntry.id) ||
          foldsByAnchorEntryId.has(nextEntry.id)
        ) {
          break;
        }
        groupedEntries.push(nextEntry.entry);
        cursor += 1;
      }
      const visibleGroupedEntries = groupedEntries.filter(
        (entry) => !workEntryIndicatesToolNeutralStatus(entry),
      );
      if (visibleGroupedEntries.length > 0) {
        if (visibleGroupedEntries.length <= MAX_VISIBLE_WORK_LOG_ENTRIES) {
          nextRows.push({
            kind: "work",
            id: timelineEntry.id,
            createdAt: timelineEntry.createdAt,
            groupedEntries: visibleGroupedEntries,
          });
        } else {
          const groupId = `work-group:${timelineEntry.id}`;
          const expanded = input.expandedWorkGroupIds?.has(groupId) ?? false;
          const hiddenEntries = visibleGroupedEntries.slice(0, -MAX_VISIBLE_WORK_LOG_ENTRIES);
          const visibleEntries = visibleGroupedEntries.slice(-MAX_VISIBLE_WORK_LOG_ENTRIES);
          const renderedEntries = expanded ? [...hiddenEntries, ...visibleEntries] : visibleEntries;

          for (const workEntry of renderedEntries) {
            nextRows.push({
              kind: "work",
              id: workEntry.id,
              createdAt: workEntry.createdAt,
              groupedEntries: [workEntry],
            });
          }

          nextRows.push({
            kind: "work-toggle",
            id: `work-toggle:${timelineEntry.id}`,
            createdAt: timelineEntry.createdAt,
            groupId,
            hiddenCount: hiddenEntries.length,
            expanded,
            onlyToolEntries: visibleGroupedEntries.every((entry) => workLogEntryIsToolLike(entry)),
          });
        }
      }
      index = cursor - 1;
      continue;
    }

    if (timelineEntry.kind === "proposed-plan") {
      nextRows.push({
        kind: "proposed-plan",
        id: timelineEntry.id,
        createdAt: timelineEntry.createdAt,
        proposedPlan: timelineEntry.proposedPlan,
      });
      continue;
    }

    const assistantTurnStillInProgress =
      timelineEntry.message.role === "assistant" &&
      unsettledTurnId !== null &&
      timelineEntry.message.turnId === unsettledTurnId;

    const durationStart =
      durationStartByMessageId.get(timelineEntry.message.id) ?? timelineEntry.message.createdAt;
    const finalAssistantBoundary =
      timelineEntry.message.turnId !== null
        ? finalAssistantBoundariesByTurnId.get(String(timelineEntry.message.turnId))
        : undefined;
    const isFinalAssistantMessage =
      timelineEntry.message.turnId === null
        ? terminalNullTurnAssistantMessageIds.has(String(timelineEntry.message.id))
        : finalAssistantBoundary !== undefined &&
          finalAssistantBoundary.has(String(timelineEntry.message.id));

    // While the turn is still running, the latest assistant message is only
    // provisionally terminal — withhold the metadata row until the turn
    // settles so commentary doesn't flash timestamps mid-work.
    const showAssistantMeta =
      timelineEntry.message.role === "assistant" &&
      isFinalAssistantMessage &&
      !assistantTurnStillInProgress;

    nextRows.push({
      kind: "message",
      id: timelineEntry.id,
      createdAt: timelineEntry.createdAt,
      message: timelineEntry.message,
      durationStart,
      showAssistantMeta,
      showAssistantCopyButton: showAssistantMeta,
      assistantCopyStreaming: timelineEntry.message.streaming || assistantTurnStillInProgress,
      effectiveModel:
        isFinalAssistantMessage && timelineEntry.message.turnId !== null
          ? effectiveModelByTurnId.get(String(timelineEntry.message.turnId))
          : undefined,
      assistantTurnDiffSummary:
        timelineEntry.message.role === "assistant"
          ? (input.turnDiffSummaryByAssistantMessageId.get(timelineEntry.message.id) ??
            (isFinalAssistantMessage && timelineEntry.message.turnId !== null
              ? input.turnDiffSummaryByTurnId?.get(timelineEntry.message.turnId)
              : undefined))
          : undefined,
      revertTurnCount: isTurnPromptMessage(timelineEntry.message)
        ? input.revertTurnCountByUserMessageId.get(timelineEntry.message.id)
        : undefined,
    });
  }

  if (input.isWorking) {
    nextRows.push({
      kind: "working",
      id: "working-indicator-row",
      createdAt: input.activeTurnStartedAt,
    });
  }

  return nextRows;
}

export function computeStableMessagesTimelineRows(
  rows: MessagesTimelineRow[],
  previous: StableMessagesTimelineRowsState,
): StableMessagesTimelineRowsState {
  const next = new Map<string, MessagesTimelineRow>();
  let anyChanged = rows.length !== previous.byId.size;

  const result = rows.map((row, index) => {
    const prevRow = previous.byId.get(row.id);
    const nextRow = prevRow && isRowUnchanged(prevRow, row) ? prevRow : row;
    next.set(row.id, nextRow);
    if (!anyChanged && previous.result[index] !== nextRow) {
      anyChanged = true;
    }
    return nextRow;
  });

  return anyChanged ? { byId: next, result } : previous;
}

/** Shallow field comparison per row variant — avoids deep equality cost. */
function isRowUnchanged(a: MessagesTimelineRow, b: MessagesTimelineRow): boolean {
  if (a.kind !== b.kind || a.id !== b.id) return false;

  switch (a.kind) {
    case "working":
      return a.createdAt === (b as typeof a).createdAt;

    case "turn-fold": {
      const bf = b as typeof a;
      return a.createdAt === bf.createdAt && a.label === bf.label && a.expanded === bf.expanded;
    }

    case "proposed-plan":
      return a.proposedPlan === (b as typeof a).proposedPlan;

    case "work":
      return Equal.equals(a.groupedEntries, (b as typeof a).groupedEntries);

    case "work-toggle": {
      const bw = b as typeof a;
      return (
        a.createdAt === bw.createdAt &&
        a.groupId === bw.groupId &&
        a.hiddenCount === bw.hiddenCount &&
        a.expanded === bw.expanded &&
        a.onlyToolEntries === bw.onlyToolEntries
      );
    }

    case "message": {
      const bm = b as typeof a;
      return (
        a.message === bm.message &&
        a.durationStart === bm.durationStart &&
        a.showAssistantMeta === bm.showAssistantMeta &&
        a.showAssistantCopyButton === bm.showAssistantCopyButton &&
        a.assistantCopyStreaming === bm.assistantCopyStreaming &&
        a.effectiveModel === bm.effectiveModel &&
        a.assistantTurnDiffSummary === bm.assistantTurnDiffSummary &&
        a.revertTurnCount === bm.revertTurnCount
      );
    }
  }
}
