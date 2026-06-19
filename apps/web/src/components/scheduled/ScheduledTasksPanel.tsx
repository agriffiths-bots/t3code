import { useAtomValue } from "@effect/atom-react";
import type { ProjectId, ThreadId } from "@t3tools/contracts";
import { ClockIcon } from "lucide-react";
import { useMemo } from "react";
import * as Option from "effect/Option";

import { EMPTY_SCHEDULED_TASKS_STATE } from "@t3tools/client-runtime/state/schedules";
import { Atom } from "effect/unstable/reactivity";

import { usePrimaryEnvironmentId } from "../../state/environments";
import { useProjects, useThreadShells } from "../../state/entities";
import {
  environmentScheduledTasks,
  isScheduleOverdue,
  lastStatusIsFailure,
  useScheduledTasks,
} from "../../state/schedules";
import { CardFrame } from "../ui/card";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "../ui/empty";
import { Skeleton } from "../ui/skeleton";
import { ScheduledTaskCard } from "./ScheduledTaskCard";

function ScheduledTasksLoading() {
  return (
    <CardFrame className="mx-auto w-full max-w-208">
      {[0, 1, 2].map((index) => (
        <div key={index} className="flex items-start gap-3 px-4 py-3.5">
          <Skeleton className="mt-0.5 size-4 rounded-full" />
          <div className="flex flex-1 flex-col gap-2">
            <Skeleton className="h-4 w-40" />
            <Skeleton className="h-3 w-56" />
            <Skeleton className="h-3 w-full max-w-96" />
          </div>
          <Skeleton className="h-5 w-9 rounded-full" />
        </div>
      ))}
    </CardFrame>
  );
}

function ScheduledTasksEmpty() {
  return (
    <Empty className="mx-auto w-full max-w-208">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <ClockIcon />
        </EmptyMedia>
        <EmptyTitle>No scheduled tasks</EmptyTitle>
        <EmptyDescription>
          Agents create schedules with the t3_schedule tools. They run in their thread and show up
          here.
        </EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

export function ScheduledTasksPanel() {
  const environmentId = usePrimaryEnvironmentId();
  const tasks = useScheduledTasks(environmentId);
  const projects = useProjects();
  const threadShells = useThreadShells();

  const scheduleState = useAtomValue(
    environmentId !== null
      ? environmentScheduledTasks.stateValueAtom(environmentId)
      : EMPTY_STATE_FALLBACK_ATOM,
  );

  // Join schedule.threadId -> threadShell.{title,projectId,branch} -> project.title,
  // scoped to the panel's environment (design.md SURFACE 1 workspace meta line).
  const projectTitleById = useMemo(() => {
    const map = new Map<ProjectId, string>();
    for (const project of projects) {
      if (project.environmentId === environmentId) {
        map.set(project.id, project.title);
      }
    }
    return map;
  }, [environmentId, projects]);

  const threadInfoById = useMemo(() => {
    const map = new Map<
      ThreadId,
      { title: string; projectId: ProjectId; branch: string | null }
    >();
    for (const shell of threadShells) {
      if (shell.environmentId === environmentId) {
        map.set(shell.id, {
          title: shell.title,
          projectId: shell.projectId,
          branch: shell.branch,
        });
      }
    }
    return map;
  }, [environmentId, threadShells]);

  const isLoading =
    environmentId !== null &&
    tasks.length === 0 &&
    Option.isNone(scheduleState.snapshot) &&
    scheduleState.status === "synchronizing";

  let body: React.ReactNode;
  if (isLoading) {
    body = <ScheduledTasksLoading />;
  } else if (tasks.length === 0) {
    body = <ScheduledTasksEmpty />;
  } else {
    body = (
      <CardFrame className="mx-auto w-full max-w-208">
        {tasks.map((task) => {
          const threadInfo = threadInfoById.get(task.threadId) ?? null;
          const projectTitle = threadInfo
            ? (projectTitleById.get(threadInfo.projectId) ?? null)
            : null;
          const workspaceLabel = [projectTitle, threadInfo?.branch]
            .filter((part): part is string => Boolean(part))
            .join(" · ");
          return (
            <ScheduledTaskCard
              key={task.taskId}
              environmentId={environmentId!}
              task={task}
              threadTitle={threadInfo?.title ?? null}
              workspaceLabel={workspaceLabel.length > 0 ? workspaceLabel : null}
              overdue={isScheduleOverdue(task)}
              lastStatusFailed={lastStatusIsFailure(task.lastStatus)}
            />
          );
        })}
      </CardFrame>
    );
  }

  return (
    <div className="min-h-0 flex-1 overflow-y-auto px-3 py-4 sm:px-5 sm:py-6">{body}</div>
  );
}

const EMPTY_STATE_FALLBACK_ATOM = Atom.make(EMPTY_SCHEDULED_TASKS_STATE).pipe(
  Atom.withLabel("environment-scheduled-tasks-state-value:empty"),
);
