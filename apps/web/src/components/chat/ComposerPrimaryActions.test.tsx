import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { ComposerPrimaryActions } from "./ComposerPrimaryActions";

const noop = () => {};

function renderActions(props: Partial<Parameters<typeof ComposerPrimaryActions>[0]> = {}): string {
  return renderToStaticMarkup(
    <ComposerPrimaryActions
      compact={false}
      pendingAction={null}
      isRunning={false}
      showPlanFollowUpPrompt={false}
      promptHasText={false}
      isSendBusy={false}
      isConnecting={false}
      isEnvironmentUnavailable={false}
      isPreparingWorktree={false}
      hasSendableContent
      hasImageAttachments={false}
      sessionOnlySendReason={null}
      onPreviousPendingQuestion={noop}
      onInterrupt={noop}
      onImplementPlanInNewThread={noop}
      {...props}
    />,
  );
}

describe("ComposerPrimaryActions", () => {
  it("keeps text sends enabled while the environment is disconnected", () => {
    const markup = renderActions({ isEnvironmentUnavailable: true });

    expect(markup).toContain('aria-label="Queue text message"');
    expect(markup).not.toContain(' disabled=""');
  });

  it("labels disconnected image sends as session-only queued work", () => {
    const markup = renderActions({
      isEnvironmentUnavailable: true,
      hasImageAttachments: true,
    });

    expect(markup).toContain('aria-label="Queue attachments for this session"');
    expect(markup).not.toContain(' disabled=""');
  });

  it("labels disconnected thread setup sends as session-only queued work", () => {
    const markup = renderActions({
      isEnvironmentUnavailable: true,
      sessionOnlySendReason: "thread-setup",
    });

    expect(markup).toContain('aria-label="Queue thread setup for this session"');
    expect(markup).not.toContain(' disabled=""');
  });

  it("labels disconnected dependent sends as waiting on earlier queued work", () => {
    const markup = renderActions({
      isEnvironmentUnavailable: true,
      sessionOnlySendReason: "dependent",
    });

    expect(markup).toContain('aria-label="Queue send after earlier queued work for this session"');
    expect(markup).not.toContain(' disabled=""');
  });
});
