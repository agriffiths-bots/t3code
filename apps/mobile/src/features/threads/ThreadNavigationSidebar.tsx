import { isLiquidGlassSupported, LiquidGlassView } from "@callstack/liquid-glass";
import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import type { EnvironmentId } from "@t3tools/contracts";
import { LegendList } from "@legendapp/list/react-native";
import type { MenuAction } from "@react-native-menu/menu";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/unstable/reactivity";
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { LayoutChangeEvent, NativeScrollEvent, NativeSyntheticEvent } from "react-native";
import {
  FlatList,
  Platform,
  Pressable,
  StyleSheet,
  TextInput,
  View,
  useColorScheme,
} from "react-native";
import { Gesture, GestureDetector } from "react-native-gesture-handler";
import type { SwipeableMethods } from "react-native-gesture-handler/ReanimatedSwipeable";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import type { SearchBarCommands } from "react-native-screens";
import Svg, { Defs, LinearGradient, Rect, Stop } from "react-native-svg";

import { AppText as Text } from "../../components/AppText";
import { ControlPillMenu } from "../../components/ControlPill";
import { SymbolView } from "../../components/AppSymbol";
import { NATIVE_LIQUID_GLASS_SUPPORTED } from "../../native/native-glass";
import { NativeStackScreenOptions } from "../../native/StackHeader";
import { scopedProjectKey, scopedThreadKey } from "../../lib/scopedEntities";
import { useThemeColor } from "../../lib/useThemeColor";
import { useProjects, useThreadShells } from "../../state/entities";
import { mobilePreferencesAtom } from "../../state/preferences";
import { environmentServerConfigsAtom } from "../../state/server";
import { usePendingNewTasks } from "../../state/use-pending-new-tasks";
import { useWorkspaceState } from "../../state/workspace";
import { useSavedRemoteConnections } from "../../state/use-remote-environment-registry";
import { useHardwareKeyboardCommand } from "../keyboard/hardwareKeyboardCommands";
import {
  hasCustomHomeListOptions,
  PROJECT_GROUPING_OPTIONS,
  PROJECT_SORT_OPTIONS,
  THREAD_SORT_OPTIONS,
  useHomeListOptions,
} from "../home/home-list-options";
import { buildHomeListFilterMenu } from "../home/home-list-filter-menu";
import {
  buildHomeListLayout,
  DEFAULT_GROUP_DISPLAY_STATE,
  homeListItemsAreEqual,
  nextGroupDisplayState,
  type HomeGroupDisplayAction,
  type HomeGroupDisplayState,
  type HomeListItem,
} from "../home/homeListItems";
import { buildHomeThreadGroups } from "../home/homeThreadList";
import { SwipeableScrollGateProvider, useSwipeableScrollGate } from "../home/thread-swipe-actions";
import { usePendingTaskListActions } from "../home/usePendingTaskListActions";
import { useThreadListActions } from "../home/useThreadListActions";
import { WorkspaceConnectionStatus } from "../home/WorkspaceConnectionStatus";
import { shouldShowWorkspaceConnectionStatus } from "../home/workspace-connection-status";
import { SidebarHeaderActions } from "./sidebar-header-actions";
import { SidebarFilterButton } from "./sidebar-filter-button";
import { createSidebarHeaderItems } from "./sidebar-native-header-items";
import { SidebarNavigationShell } from "./sidebar-navigation-shell";
import {
  PendingTaskListRow,
  ThreadListGroupHeader,
  ThreadListRow,
  ThreadListShowMoreRow,
} from "./thread-list-items";
import { ThreadListV2PrStateReporter, ThreadListV2Row } from "./thread-list-v2-items";
import { ThreadListV2ProjectScope } from "./thread-list-v2-project-scope";
import {
  buildThreadListV2Items,
  selectThreadListV2PrSettlementCandidates,
  type ThreadListV2Item,
} from "./threadListV2";

/**
 * Shared capsule behind the sidebar header buttons — a native liquid-glass
 * surface on iOS 26+, a tinted pill everywhere else.
 */
function SidebarHeaderButtonGroup(props: {
  readonly children: ReactNode;
  readonly colorScheme: "light" | "dark";
}) {
  if (isLiquidGlassSupported) {
    return (
      <LiquidGlassView
        colorScheme={props.colorScheme}
        effect="regular"
        interactive
        style={styles.headerButtonGroup}
      >
        {props.children}
      </LiquidGlassView>
    );
  }

  return (
    <View
      style={[
        styles.headerButtonGroup,
        props.colorScheme === "dark"
          ? { backgroundColor: "rgba(118,118,128,0.24)", borderColor: "rgba(255,255,255,0.08)" }
          : { backgroundColor: "rgba(255,255,255,0.72)", borderColor: "rgba(0,0,0,0.08)" },
        { borderWidth: StyleSheet.hairlineWidth },
      ]}
    >
      {props.children}
    </View>
  );
}

const SIDEBAR_STICKY_HEADER_HEIGHT = 106;
const SIDEBAR_STICKY_HEADER_FADE_HEIGHT = 44;
const THREAD_LIST_V2_SETTLED_INITIAL_COUNT = 10;
const THREAD_LIST_V2_SETTLED_PAGE_COUNT = 25;
const THREAD_LIST_V2_AUTO_SETTLE_AFTER_DAYS: number | null = null;
const SIDEBAR_HEADER_WASH_OPACITY = {
  dark: [0.22, 0.14, 0.04],
  light: [0.46, 0.3, 0.08],
} as const;

interface ThreadNavigationSidebarProps {
  readonly width: number;
  readonly visible: boolean;
  readonly selectedThreadKey: string | null;
  readonly onOpenSettings: () => void;
  readonly onOpenEnvironmentSettings: () => void;
  readonly onNewThreadInProject: (project: EnvironmentProject) => void;
  readonly onSearchQueryChange: (query: string) => void;
  readonly onSelectThread: (thread: EnvironmentThreadShell) => void;
  readonly onRequestVisibility: () => void;
  readonly searchQuery: string;
}

/**
 * iPad/large-width sidebar column.
 *
 * On iOS the pane is hosted inside its own navigation-inert single-screen
 * native stack (SidebarNavigationShell) so the header is a real
 * UINavigationBar: large title, native bar-button items, and a
 * UISearchController search field — the same chrome a UISplitViewController
 * column gets. Other platforms keep the custom header chrome.
 */
export function ThreadNavigationSidebar(props: ThreadNavigationSidebarProps) {
  if (Platform.OS !== "ios") {
    return <ThreadNavigationSidebarPane {...props} nativeChrome={false} />;
  }
  return <NativeSidebarContainer {...props} />;
}

function NativeSidebarContainer(props: ThreadNavigationSidebarProps) {
  const backgroundColor = useThemeColor("--color-drawer");
  const borderColor = useThemeColor("--color-border");

  return (
    <View
      testID="thread-navigation-sidebar"
      className="flex-1"
      style={{
        width: props.width,
        backgroundColor,
        borderRightColor: borderColor,
        borderRightWidth: StyleSheet.hairlineWidth,
      }}
    >
      <SidebarNavigationShell>
        <ThreadNavigationSidebarPane {...props} nativeChrome />
      </SidebarNavigationShell>
    </View>
  );
}

function ThreadNavigationSidebarPane(
  props: ThreadNavigationSidebarProps & { readonly nativeChrome: boolean },
) {
  const insets = useSafeAreaInsets();
  const colorScheme = useColorScheme() === "dark" ? "dark" : "light";
  const projects = useProjects();
  const threads = useThreadShells();
  const preferencesResult = useAtomValue(mobilePreferencesAtom);
  const threadListV2Enabled =
    AsyncResult.isSuccess(preferencesResult) &&
    preferencesResult.value.threadListV2Enabled === true;
  const { state: catalogState } = useWorkspaceState();
  const { savedConnectionsById } = useSavedRemoteConnections();
  const [headerIsOverContent, setHeaderIsOverContent] = useState(false);
  const searchInputRef = useRef<TextInput>(null);
  const searchBarRef = useRef<SearchBarCommands>(null);
  const openSwipeableRef = useRef<SwipeableMethods | null>(null);
  const headerIsOverContentRef = useRef(false);
  const sidebarScrollGesture = useMemo(() => Gesture.Native(), []);
  const { archiveThread, confirmDeleteThread, settleThread, unsettleThread } =
    useThreadListActions();
  const pendingTasks = usePendingNewTasks();
  const { openPendingTask, confirmDeletePendingTask } = usePendingTaskListActions();
  const environments = useMemo(
    () =>
      Object.values(savedConnectionsById)
        .map((connection) => ({
          environmentId: connection.environmentId,
          label: connection.environmentLabel,
        }))
        .sort((left, right) => left.label.localeCompare(right.label)),
    [savedConnectionsById],
  );
  const availableEnvironmentIds = useMemo(
    () => new Set(environments.map((environment) => environment.environmentId)),
    [environments],
  );
  const {
    options,
    setSelectedEnvironmentId,
    setProjectGroupingMode,
    setProjectSortOrder,
    setThreadSortOrder,
  } = useHomeListOptions(availableEnvironmentIds);
  const groups = useMemo(
    () =>
      buildHomeThreadGroups({
        projects,
        threads,
        pendingTasks,
        environmentId: options.selectedEnvironmentId,
        searchQuery: props.searchQuery,
        projectSortOrder: options.projectSortOrder,
        threadSortOrder: options.threadSortOrder,
        projectGroupingMode: options.projectGroupingMode,
      }),
    [options, pendingTasks, projects, props.searchQuery, threads],
  );
  const [groupDisplayStates, setGroupDisplayStates] = useState<
    ReadonlyMap<string, HomeGroupDisplayState>
  >(() => new Map());
  const updateGroupDisplay = useCallback((key: string, action: HomeGroupDisplayAction) => {
    setGroupDisplayStates((previous) => {
      const next = new Map(previous);
      next.set(
        key,
        nextGroupDisplayState(previous.get(key) ?? DEFAULT_GROUP_DISPLAY_STATE, action),
      );
      return next;
    });
  }, []);
  const hasSearchQuery = props.searchQuery.trim().length > 0;
  const listLayout = useMemo(
    () =>
      buildHomeListLayout({
        groups,
        displayStates: groupDisplayStates,
        showAllThreads: hasSearchQuery,
      }),
    [groups, groupDisplayStates, hasSearchQuery],
  );
  const projectCwdByKey = useMemo(() => {
    const map = new Map<string, string>();
    for (const project of projects) {
      map.set(scopedProjectKey(project.environmentId, project.id), project.workspaceRoot);
    }
    return map;
  }, [projects]);
  const showsConnectionStatus = shouldShowWorkspaceConnectionStatus(catalogState);
  const listMenuActions = useMemo<MenuAction[]>(
    () => [
      {
        id: "environment",
        title: "Environment",
        subactions: [
          {
            id: "environment:all",
            title: "All environments",
            subtitle: "Show threads from every environment",
            state: options.selectedEnvironmentId === null ? "on" : "off",
          },
          ...environments.map((environment) => ({
            id: `environment:${environment.environmentId}`,
            title: environment.label,
            state:
              options.selectedEnvironmentId === environment.environmentId
                ? ("on" as const)
                : ("off" as const),
          })),
        ],
      },
      {
        id: "project-sort",
        title: "Sort projects",
        subactions: PROJECT_SORT_OPTIONS.map((option) => ({
          id: `project-sort:${option.value}`,
          title: option.label,
          state: options.projectSortOrder === option.value ? "on" : "off",
        })),
      },
      {
        id: "thread-sort",
        title: "Sort threads",
        subactions: THREAD_SORT_OPTIONS.map((option) => ({
          id: `thread-sort:${option.value}`,
          title: option.label,
          state: options.threadSortOrder === option.value ? "on" : "off",
        })),
      },
      {
        id: "project-grouping",
        title: "Group projects",
        subactions: PROJECT_GROUPING_OPTIONS.map((option) => ({
          id: `project-grouping:${option.value}`,
          title: option.label,
          subtitle: option.subtitle,
          state: options.projectGroupingMode === option.value ? "on" : "off",
        })),
      },
    ],
    [environments, options],
  );
  const handleListMenuAction = useCallback(
    ({ nativeEvent }: { readonly nativeEvent: { readonly event: string } }) => {
      const event = nativeEvent.event;
      if (event === "environment:all") {
        setSelectedEnvironmentId(null);
        return;
      }
      if (event.startsWith("environment:")) {
        const environment = environments.find(
          (candidate) => String(candidate.environmentId) === event.slice("environment:".length),
        );
        if (environment) setSelectedEnvironmentId(environment.environmentId);
        return;
      }
      const projectSort = PROJECT_SORT_OPTIONS.find(
        (option) => `project-sort:${option.value}` === event,
      );
      if (projectSort) {
        setProjectSortOrder(projectSort.value);
        return;
      }
      const threadSort = THREAD_SORT_OPTIONS.find(
        (option) => `thread-sort:${option.value}` === event,
      );
      if (threadSort) {
        setThreadSortOrder(threadSort.value);
        return;
      }
      const grouping = PROJECT_GROUPING_OPTIONS.find(
        (option) => `project-grouping:${option.value}` === event,
      );
      if (grouping) setProjectGroupingMode(grouping.value);
    },
    [
      environments,
      setProjectGroupingMode,
      setProjectSortOrder,
      setSelectedEnvironmentId,
      setThreadSortOrder,
    ],
  );

  const backgroundColor = useThemeColor("--color-drawer");
  const borderColor = useThemeColor("--color-border");
  const mutedColor = useThemeColor("--color-foreground-muted");
  const placeholderColor = useThemeColor("--color-placeholder");
  const headerFadeColor = String(backgroundColor);
  const headerWashOpacity = SIDEBAR_HEADER_WASH_OPACITY[colorScheme];
  const [measuredHeaderHeight, setMeasuredHeaderHeight] = useState<number | null>(null);
  // The sticky header (title row, search field, optional connection status)
  // is measured so the list inset always matches its real height — no
  // hardcoded per-variant constants.
  const stickyHeaderHeight = measuredHeaderHeight ?? insets.top + SIDEBAR_STICKY_HEADER_HEIGHT;
  const topListInset = stickyHeaderHeight + 6;
  const handleStickyHeaderLayout = useCallback((event: LayoutChangeEvent) => {
    const nextHeight = Math.round(event.nativeEvent.layout.height);
    setMeasuredHeaderHeight((current) => (current === nextHeight ? current : nextHeight));
  }, []);
  const handleSwipeableWillOpen = useCallback((methods: SwipeableMethods) => {
    if (openSwipeableRef.current !== methods) {
      openSwipeableRef.current?.close();
      openSwipeableRef.current = methods;
    }
  }, []);
  const handleSwipeableClose = useCallback((methods: SwipeableMethods) => {
    if (openSwipeableRef.current === methods) {
      openSwipeableRef.current = null;
    }
  }, []);
  const handleSelectThread = useCallback(
    (thread: EnvironmentThreadShell) => {
      props.onSelectThread(thread);
      openSwipeableRef.current?.close();
    },
    [props.onSelectThread],
  );

  // The persistent split-view sidebar is the home thread list on iPad and
  // wide layouts, so it must honor the same v2 preference and settlement
  // model as HomeScreen instead of silently falling back to grouped v1.
  const serverConfigs = useAtomValue(environmentServerConfigsAtom);
  const settlementEnvironmentIds = useMemo(() => {
    const supported = new Set<EnvironmentId>();
    for (const [environmentId, config] of serverConfigs) {
      if (config.environment.capabilities.threadSettlement === true) {
        supported.add(environmentId);
      }
    }
    return supported;
  }, [serverConfigs]);
  const projectByKey = useMemo(() => {
    const map = new Map<string, EnvironmentProject>();
    for (const project of projects) {
      map.set(scopedProjectKey(project.environmentId, project.id), project);
    }
    return map;
  }, [projects]);
  const v2ScopeProjects = useMemo(
    () =>
      options.selectedEnvironmentId === null
        ? projects
        : projects.filter((project) => project.environmentId === options.selectedEnvironmentId),
    [options.selectedEnvironmentId, projects],
  );
  const [v2ProjectScopeKey, setV2ProjectScopeKey] = useState<string | null>(null);
  const v2ScopedProject = useMemo(
    () =>
      v2ProjectScopeKey === null
        ? null
        : (v2ScopeProjects.find(
            (project) => scopedProjectKey(project.environmentId, project.id) === v2ProjectScopeKey,
          ) ?? null),
    [v2ProjectScopeKey, v2ScopeProjects],
  );
  useEffect(() => {
    if (v2ProjectScopeKey !== null && v2ScopedProject === null) {
      setV2ProjectScopeKey(null);
    }
  }, [v2ProjectScopeKey, v2ScopedProject]);
  const [changeRequestStateByKey, setChangeRequestStateByKey] = useState<
    ReadonlyMap<string, "open" | "closed" | "merged">
  >(() => new Map());
  const handleChangeRequestState = useCallback(
    (threadKey: string, state: "open" | "closed" | "merged" | null) => {
      setChangeRequestStateByKey((current) => {
        if ((current.get(threadKey) ?? null) === state) return current;
        const next = new Map(current);
        if (state === null) next.delete(threadKey);
        else next.set(threadKey, state);
        return next;
      });
    },
    [],
  );
  const [settledVisibleCount, setSettledVisibleCount] = useState(
    THREAD_LIST_V2_SETTLED_INITIAL_COUNT,
  );
  const settledResetKey = `${options.selectedEnvironmentId ?? "all"}:${v2ProjectScopeKey ?? "all"}:${props.searchQuery.trim()}`;
  const lastSettledResetKeyRef = useRef(settledResetKey);
  if (lastSettledResetKeyRef.current !== settledResetKey) {
    lastSettledResetKeyRef.current = settledResetKey;
    setSettledVisibleCount(THREAD_LIST_V2_SETTLED_INITIAL_COUNT);
  }
  const showMoreSettled = useCallback(
    () => setSettledVisibleCount((count) => count + THREAD_LIST_V2_SETTLED_PAGE_COUNT),
    [],
  );
  const [nowMinute, setNowMinute] = useState(() => new Date().toISOString().slice(0, 16));
  useEffect(() => {
    if (!threadListV2Enabled) return;
    const id = setInterval(() => setNowMinute(new Date().toISOString().slice(0, 16)), 60_000);
    return () => clearInterval(id);
  }, [threadListV2Enabled]);
  const unarchivedThreads = useMemo(
    () => threads.filter((thread) => thread.archivedAt === null),
    [threads],
  );
  const v2ProjectRef = useMemo(
    () =>
      v2ScopedProject === null
        ? null
        : {
            environmentId: v2ScopedProject.environmentId,
            projectId: v2ScopedProject.id,
          },
    [v2ScopedProject],
  );
  const threadListV2Layout = useMemo(
    () =>
      threadListV2Enabled
        ? buildThreadListV2Items({
            threads: unarchivedThreads,
            environmentId: options.selectedEnvironmentId,
            projectRef: v2ProjectRef,
            searchQuery: props.searchQuery,
            changeRequestStateByKey,
            settlementEnvironmentIds,
            autoSettleAfterDays: THREAD_LIST_V2_AUTO_SETTLE_AFTER_DAYS,
            settledLimit: settledVisibleCount,
            now: `${nowMinute}:00.000Z`,
          })
        : { items: [], hiddenSettledCount: 0 },
    [
      changeRequestStateByKey,
      nowMinute,
      options.selectedEnvironmentId,
      props.searchQuery,
      settledVisibleCount,
      settlementEnvironmentIds,
      threadListV2Enabled,
      unarchivedThreads,
      v2ProjectRef,
    ],
  );
  const prStateReporterThreads = useMemo(
    () =>
      threadListV2Enabled
        ? selectThreadListV2PrSettlementCandidates({
            threads: unarchivedThreads,
            environmentId: options.selectedEnvironmentId,
            projectRef: v2ProjectRef,
            searchQuery: props.searchQuery,
            settlementEnvironmentIds,
            autoSettleAfterDays: THREAD_LIST_V2_AUTO_SETTLE_AFTER_DAYS,
            now: `${nowMinute}:00.000Z`,
          }).filter((thread) => {
            const projectCwd =
              projectCwdByKey.get(scopedProjectKey(thread.environmentId, thread.projectId)) ?? null;
            return thread.branch !== null && (thread.worktreePath ?? projectCwd) !== null;
          })
        : [],
    [
      nowMinute,
      options.selectedEnvironmentId,
      projectCwdByKey,
      props.searchQuery,
      settlementEnvironmentIds,
      threadListV2Enabled,
      unarchivedThreads,
      v2ProjectRef,
    ],
  );
  const handleSettleThread = useCallback(
    (thread: EnvironmentThreadShell) => {
      void settleThread(thread);
    },
    [settleThread],
  );
  const handleUnsettleThread = useCallback(
    (thread: EnvironmentThreadShell) => {
      void unsettleThread(thread);
    },
    [unsettleThread],
  );
  const renderV2Item = useCallback(
    ({ item }: { readonly item: ThreadListV2Item }) => (
      <ThreadListV2Row
        thread={item.thread}
        variant={item.variant}
        showSettledDivider={item.showSettledDivider}
        project={
          projectByKey.get(scopedProjectKey(item.thread.environmentId, item.thread.projectId)) ??
          null
        }
        providerDriver={
          serverConfigs
            .get(item.thread.environmentId)
            ?.providers.find(
              (provider) =>
                provider.instanceId ===
                (item.thread.session?.providerInstanceId ?? item.thread.modelSelection.instanceId),
            )?.driver ?? null
        }
        onSelectThread={handleSelectThread}
        onDeleteThread={confirmDeleteThread}
        onArchiveThread={archiveThread}
        settlementSupported={settlementEnvironmentIds.has(item.thread.environmentId)}
        onSettleThread={handleSettleThread}
        onUnsettleThread={handleUnsettleThread}
        onSwipeableClose={handleSwipeableClose}
        onSwipeableWillOpen={handleSwipeableWillOpen}
        simultaneousSwipeGesture={sidebarScrollGesture}
        selected={
          scopedThreadKey(item.thread.environmentId, item.thread.id) === props.selectedThreadKey
        }
        surface="sidebar"
        fullSwipeWidth={props.width - 20}
      />
    ),
    [
      archiveThread,
      confirmDeleteThread,
      handleSelectThread,
      handleSettleThread,
      handleSwipeableClose,
      handleSwipeableWillOpen,
      handleUnsettleThread,
      projectByKey,
      props.selectedThreadKey,
      props.width,
      serverConfigs,
      settlementEnvironmentIds,
      sidebarScrollGesture,
    ],
  );
  const v2PendingSearch = props.searchQuery.trim().toLocaleLowerCase();
  const v2PendingTasks = pendingTasks.filter(
    (pendingTask) =>
      (options.selectedEnvironmentId === null ||
        pendingTask.message.environmentId === options.selectedEnvironmentId) &&
      (v2ScopedProject === null ||
        (pendingTask.message.environmentId === v2ScopedProject.environmentId &&
          pendingTask.creation.projectId === v2ScopedProject.id)) &&
      (v2PendingSearch.length === 0 ||
        pendingTask.title.toLocaleLowerCase().includes(v2PendingSearch)),
  );
  const handleScroll = useCallback((event: NativeSyntheticEvent<NativeScrollEvent>) => {
    const next = event.nativeEvent.contentOffset.y > 6;
    if (headerIsOverContentRef.current === next) {
      return;
    }
    headerIsOverContentRef.current = next;
    setHeaderIsOverContent(next);
  }, []);
  const handleScrollBeginDrag = useCallback(() => {
    openSwipeableRef.current?.close();
  }, []);
  const { swipeEnabled, scrollGateHandlers } = useSwipeableScrollGate({
    onScroll: handleScroll,
    onScrollBeginDrag: handleScrollBeginDrag,
  });
  const listExtraData = props.selectedThreadKey ?? "";
  const focusSearch = useCallback(() => {
    const focus = () => {
      if (props.nativeChrome) {
        searchBarRef.current?.focus();
        return;
      }
      searchInputRef.current?.focus();
    };
    if (!props.visible) {
      props.onRequestVisibility();
      setTimeout(focus, 240);
    } else {
      focus();
    }
    return true;
  }, [props.nativeChrome, props.onRequestVisibility, props.visible]);
  useHardwareKeyboardCommand("focusSearch", focusSearch);
  const renderListItem = useCallback(
    ({ item }: { readonly item: HomeListItem }) => {
      switch (item.type) {
        case "header":
          return (
            <ThreadListGroupHeader
              variant="sidebar"
              collapsed={item.collapsed}
              isFirst={item.isFirst}
              groupKey={item.group.key}
              onGroupAction={updateGroupDisplay}
              // Same gating as the compact Home list: aggregated groups have no
              // single target project, and pending-project groups hold a
              // placeholder shell rather than a real project.
              newThreadTarget={item.group.newThreadTarget}
              onNewThread={props.onNewThreadInProject}
              project={item.group.representative}
              threadCount={item.group.threads.length + item.group.pendingTasks.length}
              title={item.group.title}
            />
          );
        case "pending-task":
          return (
            <PendingTaskListRow
              variant="sidebar"
              pendingTask={item.pendingTask}
              environmentLabel={
                savedConnectionsById[item.pendingTask.message.environmentId]?.environmentLabel ??
                null
              }
              isLast={item.isLast}
              onSelectPendingTask={openPendingTask}
              onDeletePendingTask={confirmDeletePendingTask}
            />
          );
        case "thread": {
          const thread = item.thread;
          return (
            <ThreadListRow
              variant="sidebar"
              thread={thread}
              environmentLabel={
                savedConnectionsById[thread.environmentId]?.environmentLabel ?? null
              }
              projectCwd={
                projectCwdByKey.get(scopedProjectKey(thread.environmentId, thread.projectId)) ??
                null
              }
              isLast={item.isLast}
              selected={
                scopedThreadKey(thread.environmentId, thread.id) === props.selectedThreadKey
              }
              fullSwipeWidth={props.width - 20}
              onArchiveThread={archiveThread}
              onDeleteThread={confirmDeleteThread}
              onSelectThread={handleSelectThread}
              onSwipeableClose={handleSwipeableClose}
              onSwipeableWillOpen={handleSwipeableWillOpen}
              simultaneousSwipeGesture={sidebarScrollGesture}
            />
          );
        }
        case "show-more":
          return (
            <ThreadListShowMoreRow
              variant="sidebar"
              hiddenCount={item.hiddenCount}
              canShowLess={item.canShowLess}
              groupKey={item.groupKey}
              onGroupAction={updateGroupDisplay}
            />
          );
      }
    },
    [
      archiveThread,
      confirmDeletePendingTask,
      confirmDeleteThread,
      handleSelectThread,
      handleSwipeableClose,
      handleSwipeableWillOpen,
      openPendingTask,
      projectCwdByKey,
      props.onNewThreadInProject,
      props.selectedThreadKey,
      props.width,
      savedConnectionsById,
      sidebarScrollGesture,
      updateGroupDisplay,
    ],
  );
  const filterIcon = hasCustomHomeListOptions(options)
    ? "line.3.horizontal.decrease.circle.fill"
    : "line.3.horizontal.decrease.circle";
  const filterMenu = useMemo(
    () =>
      buildHomeListFilterMenu({
        environments,
        selectedEnvironmentId: options.selectedEnvironmentId,
        projectSortOrder: options.projectSortOrder,
        threadSortOrder: options.threadSortOrder,
        projectGroupingMode: options.projectGroupingMode,
        onEnvironmentChange: setSelectedEnvironmentId,
        onProjectSortOrderChange: setProjectSortOrder,
        onThreadSortOrderChange: setThreadSortOrder,
        onProjectGroupingModeChange: setProjectGroupingMode,
      }),
    [
      environments,
      options,
      setProjectGroupingMode,
      setProjectSortOrder,
      setSelectedEnvironmentId,
      setThreadSortOrder,
    ],
  );
  const nativeHeaderItems = useMemo(
    () =>
      createSidebarHeaderItems({
        filterIcon,
        filterMenu,
        onOpenSettings: props.onOpenSettings,
      }),
    [filterIcon, filterMenu, props.onOpenSettings],
  );
  const listEmpty = (
    <Text className="px-2 py-4 text-sm text-foreground-muted">
      {catalogState.isLoadingConnections
        ? "Loading threads…"
        : props.searchQuery.trim().length > 0
          ? "No matching threads"
          : "No threads yet"}
    </Text>
  );
  const v2ListHeader = (
    <>
      {props.nativeChrome && showsConnectionStatus ? (
        <View className="px-1.5 pt-0.5 pb-2">
          <WorkspaceConnectionStatus
            onPress={props.onOpenEnvironmentSettings}
            state={catalogState}
            variant="sidebar"
          />
        </View>
      ) : null}
      <ThreadListV2ProjectScope
        projects={v2ScopeProjects}
        selectedKey={v2ProjectScopeKey}
        onChange={setV2ProjectScopeKey}
      />
      {v2PendingTasks.map((pendingTask, index) => (
        <PendingTaskListRow
          key={pendingTask.message.messageId}
          variant="sidebar"
          pendingTask={pendingTask}
          environmentLabel={
            savedConnectionsById[pendingTask.message.environmentId]?.environmentLabel ?? null
          }
          isLast={index === v2PendingTasks.length - 1}
          onSelectPendingTask={openPendingTask}
          onDeletePendingTask={confirmDeletePendingTask}
        />
      ))}
    </>
  );
  const sidebarList = threadListV2Enabled ? (
    <FlatList
      data={threadListV2Layout.items}
      renderItem={renderV2Item}
      keyExtractor={(item) => `${item.thread.environmentId}:${item.thread.id}`}
      extraData={{ props: listExtraData, projectByKey, serverConfigs }}
      automaticallyAdjustsScrollIndicatorInsets={
        props.nativeChrome && NATIVE_LIQUID_GLASS_SUPPORTED
      }
      contentInsetAdjustmentBehavior={
        props.nativeChrome && NATIVE_LIQUID_GLASS_SUPPORTED ? "automatic" : "never"
      }
      contentContainerStyle={[
        styles.threadListContent,
        {
          paddingBottom: props.nativeChrome ? Math.max(insets.bottom, 16) + 16 : 16 + insets.bottom,
          paddingTop: props.nativeChrome ? 6 : topListInset,
        },
      ]}
      keyboardDismissMode="on-drag"
      keyboardShouldPersistTaps="handled"
      {...scrollGateHandlers}
      scrollEventThrottle={16}
      showsVerticalScrollIndicator={false}
      style={styles.threadList}
      ListHeaderComponent={v2ListHeader}
      ListFooterComponent={
        threadListV2Layout.hiddenSettledCount > 0 ? (
          <Pressable
            accessibilityRole="button"
            accessibilityLabel={`Show ${Math.min(threadListV2Layout.hiddenSettledCount, THREAD_LIST_V2_SETTLED_PAGE_COUNT)} more settled threads`}
            onPress={showMoreSettled}
            className="mx-2 mt-2 items-center rounded-lg border border-dashed border-border py-2.5"
            style={({ pressed }) => ({ opacity: pressed ? 0.6 : 1 })}
          >
            <Text className="text-xs font-t3-medium text-foreground-muted">
              Show more ({threadListV2Layout.hiddenSettledCount} settled hidden)
            </Text>
          </Pressable>
        ) : null
      }
      ListEmptyComponent={v2PendingTasks.length > 0 ? null : listEmpty}
    />
  ) : (
    <LegendList
      data={listLayout.items}
      drawDistance={500}
      estimatedItemSize={64}
      extraData={listExtraData}
      getItemType={(item) => item.type}
      itemsAreEqual={homeListItemsAreEqual}
      keyExtractor={(item) => item.key}
      renderItem={renderListItem}
      automaticallyAdjustsScrollIndicatorInsets={
        props.nativeChrome && NATIVE_LIQUID_GLASS_SUPPORTED
      }
      contentInsetAdjustmentBehavior={
        props.nativeChrome && NATIVE_LIQUID_GLASS_SUPPORTED ? "automatic" : "never"
      }
      contentContainerStyle={[
        styles.threadListContent,
        {
          paddingBottom: props.nativeChrome ? Math.max(insets.bottom, 16) + 16 : 16 + insets.bottom,
          paddingTop: props.nativeChrome ? 6 : topListInset,
        },
      ]}
      keyboardDismissMode="on-drag"
      keyboardShouldPersistTaps="handled"
      {...scrollGateHandlers}
      recycleItems
      scrollEventThrottle={16}
      showsVerticalScrollIndicator={false}
      style={styles.threadList}
      ListHeaderComponent={
        props.nativeChrome && showsConnectionStatus ? (
          <View className="px-1.5 pt-0.5 pb-2">
            <WorkspaceConnectionStatus
              onPress={props.onOpenEnvironmentSettings}
              state={catalogState}
              variant="sidebar"
            />
          </View>
        ) : null
      }
      ListEmptyComponent={listEmpty}
    />
  );
  const prStateReporters = prStateReporterThreads.map((thread) => (
    <ThreadListV2PrStateReporter
      key={`pr-state:${thread.environmentId}:${thread.id}`}
      thread={thread}
      projectCwd={
        projectCwdByKey.get(scopedProjectKey(thread.environmentId, thread.projectId)) ?? null
      }
      onChangeRequestState={handleChangeRequestState}
    />
  ));

  if (props.nativeChrome) {
    return (
      <>
        <NativeStackScreenOptions
          options={{
            headerSearchBarOptions: {
              ref: searchBarRef,
              autoCapitalize: "none",
              hideNavigationBar: false,
              // Keep the search bar pinned under the title — UIKit's default
              // hidesSearchBarWhenScrolling collapses it on scroll.
              hideWhenScrolling: false,
              obscureBackground: false,
              placeholder: "Search",
              placement: "stacked",
              onCancelButtonPress: () => {
                props.onSearchQueryChange("");
              },
              onChangeText: (event) => {
                props.onSearchQueryChange(event.nativeEvent.text);
              },
            },
            unstable_headerRightItems: () => nativeHeaderItems,
          }}
        />
        <View className="flex-1">
          {prStateReporters}
          <SwipeableScrollGateProvider enabled={swipeEnabled}>
            <GestureDetector gesture={sidebarScrollGesture}>{sidebarList}</GestureDetector>
          </SwipeableScrollGateProvider>
        </View>
      </>
    );
  }

  return (
    <View
      testID="thread-navigation-sidebar"
      className="flex-1"
      style={{
        width: props.width,
        backgroundColor,
        borderRightColor: borderColor,
        borderRightWidth: StyleSheet.hairlineWidth,
      }}
    >
      <View className="flex-1" style={{ paddingBottom: insets.bottom }}>
        {prStateReporters}
        <SwipeableScrollGateProvider enabled={swipeEnabled}>
          <GestureDetector gesture={sidebarScrollGesture}>{sidebarList}</GestureDetector>
        </SwipeableScrollGateProvider>
      </View>

      <View
        className="absolute inset-x-0 top-0 z-[4]"
        onLayout={handleStickyHeaderLayout}
        pointerEvents="box-none"
        style={{ paddingTop: insets.top }}
      >
        <View
          className="absolute inset-x-0 top-0"
          pointerEvents="none"
          accessibilityElementsHidden
          importantForAccessibility="no-hide-descendants"
          style={{ height: stickyHeaderHeight + SIDEBAR_STICKY_HEADER_FADE_HEIGHT }}
        >
          <Svg width="100%" height="100%">
            <Defs>
              <LinearGradient id="sidebar-header-wash" x1="0%" x2="0%" y1="0%" y2="100%">
                <Stop
                  offset="0%"
                  stopColor={headerFadeColor}
                  stopOpacity={headerIsOverContent ? headerWashOpacity[0] : 0}
                />
                <Stop
                  offset="58%"
                  stopColor={headerFadeColor}
                  stopOpacity={headerIsOverContent ? headerWashOpacity[1] : 0}
                />
                <Stop
                  offset="88%"
                  stopColor={headerFadeColor}
                  stopOpacity={headerIsOverContent ? headerWashOpacity[2] : 0}
                />
                <Stop offset="100%" stopColor={headerFadeColor} stopOpacity={0} />
              </LinearGradient>
            </Defs>
            <Rect width="100%" height="100%" fill="url(#sidebar-header-wash)" />
          </Svg>
        </View>
        <View className="h-[50px] flex-row items-end gap-0.5 pr-2 pl-5">
          <Text className="flex-1 text-[34px] font-t3-bold text-foreground" numberOfLines={1}>
            Threads
          </Text>
          <SidebarHeaderButtonGroup colorScheme={colorScheme}>
            <ControlPillMenu actions={listMenuActions} onPressAction={handleListMenuAction}>
              <SidebarFilterButton
                grouped
                accessibilityLabel="Filter and sort threads"
                icon={filterIcon}
              />
            </ControlPillMenu>
            <SidebarHeaderActions grouped onOpenSettings={props.onOpenSettings} />
          </SidebarHeaderButtonGroup>
        </View>

        <View className="mx-4 mt-[9px] h-[38px] flex-row items-center gap-1.5 rounded-xl bg-sidebar-search pr-2.5 pl-[11px]">
          <SymbolView name="magnifyingglass" size={15} tintColor={mutedColor} type="monochrome" />
          <TextInput
            ref={searchInputRef}
            accessibilityLabel="Search threads"
            autoCapitalize="none"
            autoCorrect={false}
            clearButtonMode="while-editing"
            onChangeText={props.onSearchQueryChange}
            placeholder="Search"
            placeholderTextColor={placeholderColor}
            returnKeyType="search"
            className="h-[34px] flex-1 px-0 py-0 font-sans text-base text-foreground"
            value={props.searchQuery}
          />
        </View>

        {showsConnectionStatus ? (
          <View className="px-3.5 pt-2.5">
            <WorkspaceConnectionStatus
              onPress={props.onOpenEnvironmentSettings}
              state={catalogState}
              variant="sidebar"
            />
          </View>
        ) : null}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  headerButtonGroup: {
    alignItems: "center",
    borderRadius: 22,
    flexDirection: "row",
    overflow: "hidden",
  },
  threadList: {
    flex: 1,
  },
  threadListContent: {
    paddingHorizontal: 8,
  },
});
