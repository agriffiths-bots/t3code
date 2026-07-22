import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import type {
  EnvironmentId,
  ModelSelection,
  ProjectId,
  ScheduledTaskEntry,
  ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vite-plus/test";

vi.mock("@tanstack/react-router", () => ({ useNavigate: () => () => {} }));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: () => async () => ({ _tag: "Success" }),
}));
vi.mock("../../state/schedules", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../state/schedules")>();
  return {
    ...actual,
    scheduledTasksEnvironment: { setEnabled: {}, delete: {} },
  };
});

import { ScheduledTasksPanelContent } from "./ScheduledTasksPanel";

const NOW = "2030-01-01T00:00:00.000Z";
const MODEL: ModelSelection = {
  instanceId: "codex" as ModelSelection["instanceId"],
  model: "gpt-5-codex",
};

const envId = (value: string) => value as EnvironmentId;
const projectId = (value: string) => value as ProjectId;
const threadId = (value: string) => value as ThreadId;
const taskId = (value: string) => value as ScheduledTaskId;

function project(environmentId: EnvironmentId, title: string): EnvironmentProject {
  return {
    environmentId,
    id: projectId("project-shared"),
    title,
    workspaceRoot: `/tmp/${title}`,
    dataAudience: "private",
    repositoryIdentity: null,
    defaultModelSelection: MODEL,
    scripts: [],
    createdAt: NOW,
    updatedAt: NOW,
  };
}

function shell(
  environmentId: EnvironmentId,
  title: string,
  branch: string,
): EnvironmentThreadShell {
  return {
    environmentId,
    id: threadId("thread-shared"),
    projectId: projectId("project-shared"),
    dataAudience: "private",
    title,
    modelSelection: MODEL,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch,
    worktreePath: null,
    latestTurn: null,
    createdAt: NOW,
    updatedAt: NOW,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    parentThreadId: null,
  };
}

function task(environmentId: EnvironmentId): ScheduledTaskEntry {
  return {
    taskId: taskId(`task-${environmentId}`),
    threadId: threadId("thread-shared"),
    prompt: `run on ${environmentId}`,
    scheduleKind: "interval",
    intervalSeconds: 900,
    cronExpr: null,
    timezone: "UTC",
    enabled: true,
    busyPolicy: "skip",
    nextRunAt: "2030-01-01T00:15:00.000Z",
    lastRunAt: null,
    lastStatus: null,
    modelSelection: null,
  };
}

describe("ScheduledTasksPanelContent", () => {
  it("lists schedules from two backends and joins thread metadata per environment", () => {
    const windows = envId("windows-env");
    const wsl = envId("wsl-env");
    const html = renderToStaticMarkup(
      <ScheduledTasksPanelContent
        tasks={[
          { environmentId: windows, task: task(windows) },
          { environmentId: wsl, task: task(wsl) },
        ]}
        projects={[project(windows, "Windows Project"), project(wsl, "WSL Project")]}
        threadShells={[shell(windows, "Windows thread", "main"), shell(wsl, "WSL thread", "linux")]}
        environmentLabelById={
          new Map([
            [windows, "Windows"],
            [wsl, "WSL (Ubuntu)"],
          ])
        }
        loading={false}
      />,
    );

    expect(html).toContain("Windows thread");
    expect(html).toContain("WSL thread");
    expect(html).toContain("Windows Project · main");
    expect(html).toContain("WSL Project · linux");
    expect(html).toContain('data-environment-label="Windows"');
    expect(html).toContain('data-environment-label="WSL (Ubuntu)"');
  });
});
