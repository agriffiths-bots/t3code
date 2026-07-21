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
  resolveSessionRestore,
  type SessionRestoreResolution,
} from "../sessionRestore";
import { useEnvironment, useEnvironments } from "../state/environments";
import { useEnvironmentThreadRefs, useThreadDetail, useThreadShell } from "../state/entities";
import { useEnvironmentQuery } from "../state/query";
import { environmentShell } from "../state/shell";
import { useEnvironmentThread } from "../state/threads";
import { resolveThreadRouteRef } from "../threadRoutes";

function useRestoreTimeout(stage: "connecting" | "restoring" | null): boolean {
  const [timedOutStage, setTimedOutStage] = useState<typeof stage>(null);

  useEffect(() => {
    if (stage === null) {
      setTimedOutStage(null);
      return;
    }
    setTimedOutStage(null);
    const timeout = window.setTimeout(
      () => setTimedOutStage(stage),
      stage === "connecting" ? SESSION_CONNECT_TIMEOUT_MS : SESSION_RESTORE_TIMEOUT_MS,
    );
    return () => window.clearTimeout(timeout);
  }, [stage]);

  return stage !== null && timedOutStage === stage;
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
  readonly environmentLabel: string;
}) {
  const navigate = useNavigate();
  const serverThreadDetail = useThreadDetail(props.threadRef);
  const threadState = useEnvironmentThread(props.threadRef.environmentId, props.threadRef.threadId);
  const draftThread = useComposerDraftStore((store) => store.getDraftThreadByRef(props.threadRef));
  const serverThreadStarted = threadHasStarted(serverThreadDetail);
  const stage = serverThreadDetail === null && !props.draftThreadExists ? "restoring" : null;
  const timedOut = useRestoreTimeout(stage);
  const detailStatus =
    threadState.status === "deleted"
      ? ("deleted" as const)
      : threadState.error._tag === "Some"
        ? ("error" as const)
        : serverThreadDetail !== null
          ? ("ready" as const)
          : ("pending" as const);
  const resolution = resolveSessionRestore({
    catalogReady: true,
    environmentPresent: true,
    connectionPhase: "connected",
    shellAuthoritative: true,
    shellHasThread: true,
    draftExists: props.draftThreadExists,
    detailStatus,
    timedOut,
  });

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
  const draftThreadExists = useComposerDraftStore((store) =>
    threadRef ? store.getDraftThreadByRef(threadRef) !== null : false,
  );
  const stage =
    environment?.connection.phase === "connected" ? "restoring" : ("connecting" as const);
  const timedOut = useRestoreTimeout(threadRef === null ? null : stage);
  const resolution = resolveSessionRestore({
    catalogReady,
    environmentPresent: environment !== null,
    connectionPhase: environment?.connection.phase ?? null,
    shellAuthoritative,
    shellHasThread,
    draftExists: draftThreadExists,
    ...(shellHasThread ? { detailStatus: "ready" as const } : {}),
    timedOut,
  });

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
      environmentLabel={environment?.label ?? "backend"}
      threadRef={threadRef}
    />
  );
}

export const Route = createFileRoute("/_chat/$environmentId/$threadId")({
  component: ChatThreadRouteView,
});
