import { ShieldCheckIcon, SparklesIcon } from "lucide-react";
import { memo, useMemo } from "react";

import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

import {
  buildGenUiFenceSource,
  buildGenUiSrcdoc,
  GENUI_DEFAULT_HEIGHT,
  GENUI_MAX_HEIGHT,
  GENUI_SANDBOX,
  isGenUiHtmlWithinCap,
} from "./GenUiArtifact.logic";

interface GenUiArtifactProps {
  /** Raw, UNTRUSTED markup from the model's ```genui fenced block. */
  html: string;
  /** True while the assistant message is still streaming (fence may be partial). */
  isStreaming: boolean;
  /** Optional rendered height override; clamped to {@link GENUI_MAX_HEIGHT}. */
  height?: number;
}

function GenUiArtifactShell({
  children,
  copySource,
}: {
  children: React.ReactNode;
  // Original ```genui fence, so selecting + copying the message preserves the
  // source markup (see `data-markdown-copy` in markdown-clipboard.ts) rather
  // than the empty iframe / chrome text. Omitted for oversized markup so a
  // runaway payload is never serialized into a DOM attribute.
  copySource?: string | undefined;
}) {
  return (
    <div
      className="genui-artifact my-2 overflow-hidden rounded-lg border border-border/60 bg-background"
      data-genui-artifact=""
      data-markdown-copy={copySource}
    >
      <div className="flex items-center justify-between gap-2 border-b border-border/60 bg-muted/40 px-3 py-1.5">
        <span className="flex items-center gap-1.5 text-xs font-medium text-foreground/80">
          <SparklesIcon className="size-3.5" aria-hidden />
          Generated UI
        </span>
        <Tooltip>
          <TooltipTrigger
            render={
              <span className="flex items-center gap-1 text-[11px] text-muted-foreground">
                <ShieldCheckIcon className="size-3.5" aria-hidden />
                Sandboxed
              </span>
            }
          />
          <TooltipPopup side="top" className="max-w-80 whitespace-normal leading-tight">
            Rendered in a sandboxed, isolated-origin iframe with no access to the parent page,
            cookies, or storage
          </TooltipPopup>
        </Tooltip>
      </div>
      {children}
    </div>
  );
}

function GenUiArtifactNote({ children }: { children: React.ReactNode }) {
  return (
    <div className="px-3 py-6 text-center text-xs text-muted-foreground" data-genui-state="">
      {children}
    </div>
  );
}

/**
 * Renders untrusted, model-generated markup inside a hard-sandboxed iframe.
 * See `GenUiArtifact.logic.ts` for the full security model. The iframe sandbox is
 * fixed to {@link GENUI_SANDBOX} (empty — fully inert: no scripts, no
 * same-origin) and the document carries a strict CSP.
 */
function GenUiArtifact({ html, isStreaming, height }: GenUiArtifactProps) {
  const trimmed = html.trim();
  const withinCap = useMemo(() => isGenUiHtmlWithinCap(trimmed), [trimmed]);
  // Only build the document once the fence is complete and within the size
  // cap — never mount an iframe with half-streamed or oversized markup.
  const srcDoc = useMemo(
    () => (!isStreaming && withinCap && trimmed.length > 0 ? buildGenUiSrcdoc(trimmed) : null),
    [isStreaming, withinCap, trimmed],
  );
  const clampedHeight = Math.min(height ?? GENUI_DEFAULT_HEIGHT, GENUI_MAX_HEIGHT);
  // null (oversized) → undefined so no data-markdown-copy attribute is written.
  const copySource = useMemo(() => buildGenUiFenceSource(html) ?? undefined, [html]);

  if (isStreaming) {
    return (
      <GenUiArtifactShell copySource={copySource}>
        <GenUiArtifactNote>Generating UI…</GenUiArtifactNote>
      </GenUiArtifactShell>
    );
  }

  if (trimmed.length === 0) {
    return (
      <GenUiArtifactShell copySource={copySource}>
        <GenUiArtifactNote>Empty generative UI block.</GenUiArtifactNote>
      </GenUiArtifactShell>
    );
  }

  if (!withinCap || srcDoc === null) {
    return (
      <GenUiArtifactShell copySource={copySource}>
        <GenUiArtifactNote>
          Generated UI is too large to render safely and was not displayed.
        </GenUiArtifactNote>
      </GenUiArtifactShell>
    );
  }

  return (
    <GenUiArtifactShell copySource={copySource}>
      <iframe
        // SECURITY: fully inert sandbox (empty = every restriction on). Do NOT
        // add `allow-scripts` (enables self-navigation exfil of chat-derived
        // content) or `allow-same-origin` (collapses the origin barrier).
        sandbox={GENUI_SANDBOX}
        srcDoc={srcDoc}
        referrerPolicy="no-referrer"
        loading="lazy"
        title="Generated UI artifact"
        className="block w-full border-0 bg-white"
        style={{ height: clampedHeight }}
      />
    </GenUiArtifactShell>
  );
}

export default memo(GenUiArtifact);
