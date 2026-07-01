import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentId, ScheduledTaskEntry } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import {
  CheckIcon,
  ClockIcon,
  EllipsisIcon,
  ExternalLinkIcon,
  Trash2Icon,
  TriangleAlertIcon,
} from "lucide-react";
import { useCallback } from "react";

import { formatRelativeTimeLabel, formatRelativeTimeUntilLabel } from "../../timestampFormat";
import { isCronSchedule, scheduleCadenceLabel } from "../../scheduled/formatCadence";
import { scheduledTasksEnvironment } from "../../state/schedules";
import { useAtomCommand } from "../../state/use-atom-command";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { Card, CardAction, CardFrameTitle } from "../ui/card";
import { Menu, MenuItem, MenuPopup, MenuSeparator, MenuTrigger } from "../ui/menu";
import { Switch } from "../ui/switch";
import { stackedThreadToast, toastManager } from "../ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

export interface ScheduledTaskCardProps {
  readonly environmentId: EnvironmentId;
  readonly task: ScheduledTaskEntry;
  /** Joined client-side: thread.title for the card title. */
  readonly threadTitle: string | null;
  /** Joined client-side: "project · branch" workspace meta line. */
  readonly workspaceLabel: string | null;
  readonly overdue: boolean;
  readonly lastStatusFailed: boolean;
}

function absoluteTime(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return iso;
  }
  return date.toLocaleString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    month: "short",
    day: "numeric",
  });
}

export function ScheduledTaskCard({
  environmentId,
  task,
  threadTitle,
  workspaceLabel,
  overdue,
  lastStatusFailed,
}: ScheduledTaskCardProps) {
  const navigate = useNavigate();
  const setEnabled = useAtomCommand(scheduledTasksEnvironment.setEnabled, {
    reportFailure: false,
  });
  const deleteTask = useAtomCommand(scheduledTasksEnvironment.delete, {
    reportFailure: false,
  });

  const title = threadTitle ?? "Scheduled task";
  const cadenceLabel = scheduleCadenceLabel(task);
  const isCron = isCronSchedule(task);

  const cadenceVariant = !task.enabled
    ? ("outline" as const)
    : overdue
      ? ("warning" as const)
      : isCron
        ? ("info" as const)
        : ("success" as const);

  const handleToggle = useCallback(
    (nextEnabled: boolean) => {
      void (async () => {
        const result = await setEnabled({
          environmentId,
          input: { taskId: task.taskId, enabled: nextEnabled },
        });
        if (result._tag === "Success" || isAtomCommandInterrupted(result)) {
          return;
        }
        const error = squashAtomCommandFailure(result);
        toastManager.add(
          stackedThreadToast({
            type: "error",
            title: nextEnabled ? "Unable to enable schedule" : "Unable to disable schedule",
            description:
              error instanceof Error ? error.message : "The schedule could not be updated.",
          }),
        );
      })();
    },
    [environmentId, setEnabled, task.taskId],
  );

  const handleDelete = useCallback(() => {
    void (async () => {
      const result = await deleteTask({
        environmentId,
        input: { taskId: task.taskId },
      });
      if (result._tag === "Success" || isAtomCommandInterrupted(result)) {
        return;
      }
      const error = squashAtomCommandFailure(result);
      toastManager.add(
        stackedThreadToast({
          type: "error",
          title: "Unable to delete schedule",
          description:
            error instanceof Error ? error.message : "The schedule could not be deleted.",
        }),
      );
    })();
  }, [deleteTask, environmentId, task.taskId]);

  const handleOpenThread = useCallback(() => {
    void navigate({
      to: "/$environmentId/$threadId",
      params: { environmentId, threadId: task.threadId },
    });
  }, [environmentId, navigate, task.threadId]);

  const LeadingIcon = overdue ? TriangleAlertIcon : ClockIcon;
  const leadingIconClass = !task.enabled
    ? "text-muted-foreground/50"
    : overdue
      ? "text-warning"
      : "text-info";

  return (
    <Card
      className="px-4 py-3.5"
      data-testid={`scheduled-task-card-${task.taskId}`}
      data-overdue={overdue ? "true" : undefined}
      data-enabled={task.enabled ? "true" : "false"}
    >
      <div className="flex min-w-0 items-start gap-3">
        <LeadingIcon className={`mt-0.5 size-4 shrink-0 ${leadingIconClass}`} aria-hidden="true" />

        <div className="flex min-w-0 flex-1 flex-col gap-1.5">
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <CardFrameTitle className="min-w-0 truncate">{title}</CardFrameTitle>
            {isCron && task.cronExpr ? (
              <Tooltip>
                <TooltipTrigger
                  render={
                    <Badge variant={cadenceVariant} size="sm">
                      {cadenceLabel}
                    </Badge>
                  }
                />
                <TooltipPopup side="top" className="font-mono text-[10px]">
                  {task.cronExpr}
                </TooltipPopup>
              </Tooltip>
            ) : (
              <Badge variant={cadenceVariant} size="sm">
                {cadenceLabel}
              </Badge>
            )}
            {!task.enabled ? (
              <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Paused
              </span>
            ) : null}
          </div>

          {workspaceLabel ? (
            <div className="truncate text-xs text-muted-foreground">{workspaceLabel}</div>
          ) : null}

          <p className="line-clamp-2 text-sm text-muted-foreground">{task.prompt}</p>

          {!task.enabled ? (
            <div className="text-xs text-muted-foreground">
              Paused — won&apos;t run while disabled
            </div>
          ) : (
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
              {task.nextRunAt ? (
                <span className="font-medium text-foreground">
                  {overdue
                    ? `Overdue · was due ${formatRelativeTimeLabel(task.nextRunAt)}`
                    : `Next run ${formatRelativeTimeUntilLabel(task.nextRunAt)}`}
                  <span className="ms-1 text-muted-foreground">{absoluteTime(task.nextRunAt)}</span>
                </span>
              ) : (
                <span className="text-muted-foreground">No upcoming run scheduled</span>
              )}
              {task.lastRunAt ? (
                lastStatusFailed ? (
                  <span className="inline-flex items-center gap-1">
                    <TriangleAlertIcon className="size-3 text-warning" aria-hidden="true" />
                    <span className="text-destructive-foreground/80 line-clamp-1">
                      last run failed {formatRelativeTimeLabel(task.lastRunAt)}
                    </span>
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 text-muted-foreground">
                    <CheckIcon className="size-3 text-success" aria-hidden="true" />
                    ran {formatRelativeTimeLabel(task.lastRunAt)}
                  </span>
                )
              ) : null}
            </div>
          )}
        </div>

        <CardAction className="items-center gap-1.5">
          <Switch
            checked={task.enabled}
            onCheckedChange={handleToggle}
            aria-label={`${task.enabled ? "Disable" : "Enable"} ${title}`}
          />
          <Menu>
            <MenuTrigger
              render={
                <Button size="icon-xs" variant="ghost" aria-label={`Schedule actions for ${title}`}>
                  <EllipsisIcon className="size-3.5" />
                </Button>
              }
            />
            <MenuPopup align="end" side="bottom" className="min-w-44">
              <MenuItem onClick={handleOpenThread}>
                <ExternalLinkIcon className="size-3.5" />
                Open thread
              </MenuItem>
              <MenuSeparator />
              <MenuItem variant="destructive" onClick={handleDelete}>
                <Trash2Icon className="size-3.5" />
                Delete
              </MenuItem>
            </MenuPopup>
          </Menu>
        </CardAction>
      </div>
    </Card>
  );
}
