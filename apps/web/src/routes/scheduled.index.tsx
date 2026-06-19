import { createFileRoute } from "@tanstack/react-router";

import { ScheduledTasksPanel } from "../components/scheduled/ScheduledTasksPanel";

function ScheduledIndexRoute() {
  return <ScheduledTasksPanel />;
}

export const Route = createFileRoute("/scheduled/")({
  component: ScheduledIndexRoute,
});
