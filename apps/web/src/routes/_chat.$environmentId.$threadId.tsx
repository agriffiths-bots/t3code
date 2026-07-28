import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { LoaderIcon } from "lucide-react";
import { useEffect, useState } from "react";

import ChatView from "../components/ChatView";
import { threadHasStarted } from "../components/ChatView.logic";
import { Button } from "../components/ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "../components/ui/empty";
import { SidebarInset } from "../components/ui/sidebar";
import { finalizePromotedDraftThreadByRef, useComposerDraftStore } from "../composerDraftStore";
import {
  SESSION_CONNECT_TIMEOUT_MS,
  SESSION_RESTORE_TIMEOUT_MS,
  resolveSessionDetailStatus,
  resolveSessionRestore,
  sessionRestoreWaitingStage,
  shouldSubscribeToServerThread,
  type SessionRestoreResolution,
} from "../sessionRestore";
import { useEnvironment, useEnvironments } from "../state/environments";
import { useEnvironmentThreadRefs, useThreadDetail, useThreadShell } from "../state/entities";
import { useEnvironmentQuery } from "../state/query";
import { environmentShell } from "../state/shell";
import { useEnvironmentThread } from "../state/threads";
import { resolveThreadRouteRef } from "../threadRoutes";

function useRestoreTimeout(
  stage: "connecting" | "restoring" | null,
  resetKey: string | null,
): boolean {
  const timeoutIdentity = stage !== null && resetKey !== null ? `${resetKey}:${stage}` : null;
  const [timedOutIdentity, setTimedOutIdentity] = useState<string | null>(null);

  useEffect(() => {
    if (timeoutIdentity === null || stage === null) {
      setTimedOutIdentity(null);
      return;
    }
    setTimedOutIdentity(null);
    const timeout = window.setTimeout(
      () => setTimedOutIdentity(timeoutIdentity),
      stage === "connecting" ? SESSION_CONNECT_TIMEOUT_MS : SESSION_RESTORE_TIMEOUT_MS,
    );
    return () => window.clearTimeout(timeout);
  }, [stage, timeoutIdentity]);

  return timeoutIdentity !== null && timedOutIdentity === timeoutIdentity;
}

function SessionRestoreStatus(props: {
  readonly resolution: Extract<
    SessionRestoreResolution,
    { readonly kind: "connecting" | "connection-error" | "restoring" | "restore-error" }
  >;
  readonly environmentLabel: string;
  readonly connectionError: string | null;
}) {
  const isWaiting = props.resolution.kind === "connecting" || props.resolution.kind === "restoring";
  const title =
    props.resolution.kind === "connecting"
      ? `Connecting to ${props.environmentLabel}`
      : props.resolution.kind === "restoring"
        ? "Restoring session"
        : props.resolution.kind === "connection-error"
          ? `Couldn't connect to ${props.environmentLabel}`
          : "Couldn't restore your last session";
  const description =
    props.resolution.kind === "connection-error"
      ? (props.connectionError ?? "Check the backend connection, then try again.")
      : props.resolution.kind === "restore-error"
        ? "Connected, but couldn't restore your last session — open a thread or start new."
        : null;

  return (
    <SidebarInset className="h-svh min-h-0 overflow-hidden bg-background text-foreground md:h-dvh">
      <Empty className="flex-1">
        <EmptyHeader>
          {isWaiting ? (
            <LoaderIcon className="mx-auto size-5 animate-spin text-muted-foreground" />
          ) : null}
          <EmptyTitle>{title}</EmptyTitle>
          {description ? <EmptyDescription>{description}</EmptyDescription> : null}
        </EmptyHeader>
        {!isWaiting ? (
          <div className="flex flex-wrap justify-center gap-2">
            <Button render={<Link to="/" />} size="sm">
              Open thread list
            </Button>
            {props.resolution.kind === "connection-error" ? (
              <Button render={<Link to="/settings/connections" />} size="sm" variant="outline">
                Connection settings
              </Button>
            ) : null}
          </div>
        ) : null}
      </Empty>
    </SidebarInset>
  );
}

function RestoringThread(props: {
  readonly threadRef: NonNullable<ReturnType<typeof resolveThreadRouteRef>>;
  readonly draftThreadExists: boolean;
  readonly draftThreadPromoted: boolean;
  readonly environmentLabel: string;
  readonly shellHasThread: boolean;
}) {
  const navigate = useNavigate();
  const shouldSubscribe = shouldSubscribeToServerThread({
    draftExists: props.draftThreadExists,
    draftPromoted: props.draftThreadPromoted,
    shellPresent: props.shellHasThread,
  });
  const serverThreadDetail = useThreadDetail(shouldSubscribe ? props.threadRef : null);
  const threadState = useEnvironmentThread(
    shouldSubscribe ? props.threadRef.environmentId : null,
    shouldSubscribe ? props.threadRef.threadId : null,
  );
  const draftThread = useComposerDraftStore((store) => store.getDraftThreadByRef(props.threadRef));
  const serverThreadStarted = threadHasStarted(serverThreadDetail);
  const detailStatus = resolveSessionDetailStatus({
    deleted: threadState.status === "deleted",
    hasDetail: serverThreadDetail !== null,
    hasError: threadState.error._tag === "Some",
  });
  const restoreInput = {
    catalogReady: true,
    environmentPresent: true,
    connectionPhase: "connected",
    shellAuthoritative: true,
    shellHasThread: true,
    draftExists: props.draftThreadExists,
    detailStatus,
  } as const;
  const pendingResolution = resolveSessionRestore({ ...restoreInput, timedOut: false });
  const timedOut = useRestoreTimeout(
    sessionRestoreWaitingStage(pendingResolution),
    `${props.threadRef.environmentId}:${props.threadRef.threadId}`,
  );
  const resolution = timedOut
    ? resolveSessionRestore({ ...restoreInput, timedOut: true })
    : pendingResolution;

  useEffect(() => {
    if (resolution.kind === "stale") {
      void navigate({ to: "/", replace: true });
    }
  }, [navigate, resolution.kind]);

  useEffect(() => {
    if (!serverThreadStarted || !draftThread) {
      return;
    }
    finalizePromotedDraftThreadByRef(props.threadRef);
  }, [draftThread, props.threadRef, serverThreadStarted]);

  if (resolution.kind === "stale") {
    return null;
  }
  if (resolution.kind === "restoring" || resolution.kind === "restore-error") {
    return (
      <SessionRestoreStatus
        connectionError={null}
        environmentLabel={props.environmentLabel}
        resolution={resolution}
      />
    );
  }

  return (
    <SidebarInset className="h-svh min-h-0 overflow-hidden overscroll-y-none bg-background text-foreground md:h-dvh">
      <ChatView
        environmentId={props.threadRef.environmentId}
        threadId={props.threadRef.threadId}
        routeKind="server"
      />
    </SidebarInset>
  );
}

function ChatThreadRouteView() {
  const navigate = useNavigate();
  const threadRef = Route.useParams({
    select: (params) => resolveThreadRouteRef(params),
  });
  const { isReady: catalogReady } = useEnvironments();
  const environment = useEnvironment(threadRef?.environmentId ?? null);
  const shell = useEnvironmentQuery(
    threadRef === null ? null : environmentShell.stateAtom(threadRef.environmentId),
  );
  const serverThreadShell = useThreadShell(threadRef);
  const environmentThreadRefs = useEnvironmentThreadRefs(threadRef?.environmentId ?? null);
  const shellAuthoritative = shell.data?.status === "live";
  const shellHasThread =
    serverThreadShell !== null ||
    (threadRef !== null &&
      environmentThreadRefs.some((candidate) => candidate.threadId === threadRef.threadId));
  const draftThread = useComposerDraftStore((store) =>
    threadRef ? store.getDraftThreadByRef(threadRef) : null,
  );
  const draftThreadExists = draftThread !== null;
  const restoreInput = {
    catalogReady,
    environmentPresent: environment !== null,
    connectionPhase: environment?.connection.phase ?? null,
    shellAuthoritative,
    shellHasThread,
    draftExists: draftThreadExists,
    ...(shellHasThread ? { detailStatus: "ready" as const } : {}),
  } as const;
  const pendingResolution = resolveSessionRestore({ ...restoreInput, timedOut: false });
  const timedOut = useRestoreTimeout(
    threadRef === null ? null : sessionRestoreWaitingStage(pendingResolution),
    threadRef === null ? null : `${threadRef.environmentId}:${threadRef.threadId}`,
  );
  const resolution = timedOut
    ? resolveSessionRestore({ ...restoreInput, timedOut: true })
    : pendingResolution;

  useEffect(() => {
    if (threadRef !== null && resolution.kind === "stale") {
      void navigate({ to: "/", replace: true });
    }
  }, [navigate, resolution.kind, threadRef]);

  if (!threadRef || resolution.kind === "stale") {
    return null;
  }
  if (
    resolution.kind === "connecting" ||
    resolution.kind === "connection-error" ||
    resolution.kind === "restoring" ||
    resolution.kind === "restore-error"
  ) {
    return (
      <SessionRestoreStatus
        connectionError={environment?.connection.error ?? null}
        environmentLabel={environment?.label ?? "backend"}
        resolution={resolution}
      />
    );
  }

  return (
    <RestoringThread
      draftThreadExists={draftThreadExists}
      draftThreadPromoted={draftThread?.promotedTo != null}
      environmentLabel={environment?.label ?? "backend"}
      shellHasThread={shellHasThread}
      threadRef={threadRef}
    />
  );
}

export const Route = createFileRoute("/_chat/$environmentId/$threadId")({
  component: ChatThreadRouteView,
});
