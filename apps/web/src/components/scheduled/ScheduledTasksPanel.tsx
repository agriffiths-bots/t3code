import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, ProjectId, ThreadId } from "@t3tools/contracts";
import { ClockIcon } from "lucide-react";
import { useMemo } from "react";
import * as Option from "effect/Option";

import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";

import { useEnvironments } from "../../state/environments";
import { useProjects, useThreadShells } from "../../state/entities";
import {
  isScheduleOverdue,
  lastStatusIsFailure,
  scheduledTaskStatesByEnvironmentAtom,
  useScheduledTasksAcrossEnvironments,
  type ScopedScheduledTaskEntry,
} from "../../state/schedules";
import { CardFrame } from "../ui/card";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "../ui/empty";
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

const scopedEntityKey = (environmentId: EnvironmentId, entityId: ProjectId | ThreadId): string =>
  `${environmentId}\u0000${entityId}`;

export interface ScheduledTasksPanelContentProps {
  readonly tasks: ReadonlyArray<ScopedScheduledTaskEntry>;
  readonly projects: ReadonlyArray<EnvironmentProject>;
  readonly threadShells: ReadonlyArray<EnvironmentThreadShell>;
  readonly environmentLabelById: ReadonlyMap<EnvironmentId, string>;
  readonly loading: boolean;
}

export function ScheduledTasksPanelContent({
  tasks,
  projects,
  threadShells,
  environmentLabelById,
  loading,
}: ScheduledTasksPanelContentProps) {
  // Join schedule.threadId -> threadShell.{title,projectId,branch} -> project.title,
  // always scoped by environment so identical ids on separate backends do not collide.
  const projectTitleByScopedId = useMemo(() => {
    const map = new Map<string, string>();
    for (const project of projects) {
      map.set(scopedEntityKey(project.environmentId, project.id), project.title);
    }
    return map;
  }, [projects]);

  const threadInfoByScopedId = useMemo(() => {
    const map = new Map<string, { title: string; projectId: ProjectId; branch: string | null }>();
    for (const shell of threadShells) {
      map.set(scopedEntityKey(shell.environmentId, shell.id), {
        title: shell.title,
        projectId: shell.projectId,
        branch: shell.branch,
      });
    }
    return map;
  }, [threadShells]);

  let body: React.ReactNode;
  if (loading) {
    body = <ScheduledTasksLoading />;
  } else if (tasks.length === 0) {
    body = <ScheduledTasksEmpty />;
  } else {
    body = (
      <CardFrame className="mx-auto w-full max-w-208">
        {tasks.map(({ environmentId, task }) => {
          const threadInfo =
            threadInfoByScopedId.get(scopedEntityKey(environmentId, task.threadId)) ?? null;
          const projectTitle = threadInfo
            ? (projectTitleByScopedId.get(scopedEntityKey(environmentId, threadInfo.projectId)) ??
              null)
            : null;
          const workspaceLabel = [projectTitle, threadInfo?.branch]
            .filter((part): part is string => Boolean(part))
            .join(" · ");
          return (
            <ScheduledTaskCard
              key={`${environmentId}:${task.taskId}`}
              environmentId={environmentId}
              task={task}
              threadTitle={threadInfo?.title ?? null}
              workspaceLabel={workspaceLabel.length > 0 ? workspaceLabel : null}
              environmentLabel={environmentLabelById.get(environmentId) ?? null}
              overdue={isScheduleOverdue(task)}
              lastStatusFailed={lastStatusIsFailure(task.lastStatus)}
            />
          );
        })}
      </CardFrame>
    );
  }

  return <div className="min-h-0 flex-1 overflow-y-auto px-3 py-4 sm:px-5 sm:py-6">{body}</div>;
}

export function ScheduledTasksPanel() {
  const tasks = useScheduledTasksAcrossEnvironments();
  const projects = useProjects();
  const threadShells = useThreadShells();
  const scheduleStatesByEnvironment = useAtomValue(scheduledTaskStatesByEnvironmentAtom);
  const { environments } = useEnvironments();

  const environmentLabelById = useMemo(() => {
    const labels = new Map<EnvironmentId, string>();
    for (const environment of environments) {
      labels.set(environment.environmentId, environment.label);
    }
    return labels;
  }, [environments]);

  const loading =
    tasks.length === 0 &&
    [...scheduleStatesByEnvironment.values()].some(
      (state) => Option.isNone(state.snapshot) && state.status === "synchronizing",
    );

  return (
    <ScheduledTasksPanelContent
      tasks={tasks}
      projects={projects}
      threadShells={threadShells}
      environmentLabelById={environmentLabelById}
      loading={loading}
    />
  );
}
