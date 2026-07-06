import { memo } from "react";
import { Alert, AlertAction, AlertDescription } from "../ui/alert";
import { Button } from "../ui/button";
import { CircleAlertIcon, XIcon } from "lucide-react";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

export const ThreadErrorBanner = memo(function ThreadErrorBanner({
  error,
  onDismiss,
}: {
  error: string | null;
  onDismiss?: () => void;
}) {
  if (!error) return null;
  return (
    <div className="mx-auto w-full max-w-3xl px-3 pt-3 sm:px-0">
      <Alert variant="error" className="w-full min-w-0">
        <CircleAlertIcon />
        <Tooltip>
          <TooltipTrigger
            render={
              <AlertDescription className="line-clamp-3 min-w-0 whitespace-normal break-words" />
            }
          >
            {error}
          </TooltipTrigger>
          <TooltipPopup side="top" className="max-w-96 whitespace-pre-wrap break-words">
            {error}
          </TooltipPopup>
        </Tooltip>
        {onDismiss && (
          <AlertAction>
            <Button variant="ghost" size="icon-xs" aria-label="Dismiss error" onClick={onDismiss}>
              <XIcon className="text-destructive" />
            </Button>
          </AlertAction>
        )}
      </Alert>
    </div>
  );
});
