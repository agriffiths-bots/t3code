import { memo, useEffect, useRef } from "react";
import { Loader2, Pause, Play, Volume2 } from "lucide-react";
import { Button } from "../ui/button";
import { cn } from "~/lib/utils";
import { anchoredToastManager } from "../ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { useDictationController } from "./useDictation";

const ANCHORED_TOAST_TIMEOUT_MS = 2000;

function DictationIcon({ mode }: { mode: ReturnType<typeof useDictationController>["mode"] }) {
  switch (mode) {
    case "loading":
      return <Loader2 className="size-3 animate-spin" />;
    case "playing":
      return <Pause className="size-3" />;
    case "paused":
      return <Play className="size-3" />;
    case "idle":
    case "error":
      return <Volume2 className="size-3" />;
  }
}

export const MessageDictateButton = memo(function MessageDictateButton({
  messageId,
  text,
  size = "xs",
  variant = "ghost",
  className,
}: {
  messageId: string;
  text: string;
  size?: "xs" | "icon-xs";
  variant?: "outline" | "ghost";
  className?: string;
}) {
  const ref = useRef<HTMLButtonElement>(null);
  const { mode, label, disabled, onClick } = useDictationController(messageId, text);

  useEffect(() => {
    if (mode === "error" && ref.current) {
      anchoredToastManager.add({
        data: { tooltipStyle: true },
        positionerProps: { anchor: ref.current },
        timeout: ANCHORED_TOAST_TIMEOUT_MS,
        title: label,
      });
    }
  }, [mode, label]);

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            aria-label={label}
            disabled={disabled}
            onClick={onClick}
            ref={ref}
            type="button"
            size={size}
            variant={variant}
            className={cn("text-muted-foreground hover:text-foreground", className)}
          />
        }
      >
        <DictationIcon mode={mode} />
      </TooltipTrigger>
      <TooltipPopup>
        <p>{label}</p>
      </TooltipPopup>
    </Tooltip>
  );
});
