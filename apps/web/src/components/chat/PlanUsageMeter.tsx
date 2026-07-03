import type { PlanUsageSnapshot } from "@t3tools/contracts";

import { cn } from "~/lib/utils";
import { Popover, PopoverPopup, PopoverTrigger } from "../ui/popover";
import {
  flattenPlanUsageWindows,
  formatPlanUsageReset,
  formatPlanUsageValue,
  planUsageColor,
  selectMostConstrainedPlanUsageWindow,
} from "./PlanUsageMeter.logic";

export function PlanUsageMeter(props: { snapshot: PlanUsageSnapshot | null }) {
  const selected = selectMostConstrainedPlanUsageWindow(props.snapshot);
  const windows = flattenPlanUsageWindows(props.snapshot);
  if (!selected || windows.length === 0) return null;

  const normalizedPercentage = Math.max(0, Math.min(100, selected.usedPercent));
  const radius = 9.75;
  const circumference = 2 * Math.PI * radius;
  const dashOffset = circumference - (normalizedPercentage / 100) * circumference;
  const usageColor = planUsageColor(selected);

  return (
    <Popover>
      <PopoverTrigger
        openOnHover
        delay={150}
        closeDelay={0}
        render={
          <button
            type="button"
            className={cn(
              "inline-flex size-6 cursor-pointer items-center justify-center rounded-full border border-transparent text-muted-foreground outline-none transition-colors",
              "hover:bg-accent data-[pressed]:bg-accent",
              "focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 focus-visible:ring-offset-background",
            )}
            aria-label={`Plan usage ${Math.round(normalizedPercentage)}% used`}
          >
            <span className="relative flex size-4 items-center justify-center">
              <svg
                viewBox="0 0 24 24"
                className="-rotate-90 absolute inset-0 size-full transform-gpu"
                aria-hidden="true"
              >
                <circle
                  cx="12"
                  cy="12"
                  r={radius}
                  fill="none"
                  stroke="color-mix(in oklab, var(--color-muted-foreground) 35%, transparent)"
                  strokeWidth="3"
                />
                <circle
                  cx="12"
                  cy="12"
                  r={radius}
                  fill="none"
                  stroke={usageColor}
                  strokeWidth="3"
                  strokeLinecap="round"
                  strokeDasharray={circumference}
                  strokeDashoffset={dashOffset}
                  className="transition-[stroke-dashoffset] duration-500 ease-out motion-reduce:transition-none"
                />
              </svg>
            </span>
          </button>
        }
      />
      <PopoverPopup tooltipStyle side="top" align="end" className="w-72 max-w-none p-0">
        <div className="flex flex-col gap-3 p-3">
          <div className="flex items-center justify-between gap-3">
            <div className="font-medium text-muted-foreground text-xs">Plan Usage</div>
            <div className="text-[11px] tabular-nums text-muted-foreground/70">
              {formatPlanUsageValue(selected)}
            </div>
          </div>
          <div className="flex flex-col gap-2">
            {windows.map((window) => {
              const percent = Math.max(0, Math.min(100, window.usedPercent));
              const color = planUsageColor(window);
              return (
                <div key={window.id} className="flex flex-col gap-1.5">
                  <div className="flex items-center justify-between gap-3 text-[11px] leading-4">
                    <span className="font-medium text-muted-foreground/80">{window.title}</span>
                    <span className="tabular-nums text-muted-foreground/70">
                      {formatPlanUsageValue(window)}
                    </span>
                  </div>
                  <div
                    className="h-1.5 w-full overflow-hidden rounded-full bg-muted/60"
                    role="progressbar"
                    aria-valuemin={0}
                    aria-valuemax={100}
                    aria-valuenow={Math.round(percent)}
                    aria-label={`${window.title} usage`}
                  >
                    <div
                      className="h-full rounded-full transition-[width,background-color] duration-500 ease-out motion-reduce:transition-none"
                      style={{ width: `${percent}%`, backgroundColor: color }}
                    />
                  </div>
                  <div className="text-[11px] leading-4 text-muted-foreground/60">
                    {formatPlanUsageReset(window.resetAt)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </PopoverPopup>
    </Popover>
  );
}
