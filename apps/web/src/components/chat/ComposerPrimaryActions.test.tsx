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
      sendDisabledReason={null}
      isConnecting={false}
      isEnvironmentUnavailable={false}
      isPreparingWorktree={false}
      hasSendableContent
      onPreviousPendingQuestion={noop}
      onInterrupt={noop}
      onImplementPlanInNewThread={noop}
      {...props}
    />,
  );
}

describe("ComposerPrimaryActions", () => {
  it("disables sends while the environment is unavailable", () => {
    const markup = renderActions({ isEnvironmentUnavailable: true });

    expect(markup).toContain('aria-label="Environment disconnected"');
    expect(markup).toContain(' disabled=""');
  });

  it("surfaces the explicit send-disabled reason", () => {
    const markup = renderActions({ sendDisabledReason: "Choose a model" });

    expect(markup).toContain('aria-label="Choose a model"');
    expect(markup).toContain(' disabled=""');
  });

  it("labels and disables sends while connecting", () => {
    const markup = renderActions({ isConnecting: true });

    expect(markup).toContain('aria-label="Connecting"');
    expect(markup).toContain(' disabled=""');
  });

  it("labels worktree preparation without disabling sendable content", () => {
    const markup = renderActions({ isPreparingWorktree: true });

    expect(markup).toContain('aria-label="Preparing worktree"');
    expect(markup).not.toContain(' disabled=""');
  });

  it("keeps the default send button icon-only", () => {
    const markup = renderActions();

    expect(markup).toContain('aria-label="Send message"');
    expect(markup).not.toContain(">Send</span>");
  });

  it("shows stop while a turn is running with an empty draft", () => {
    const markup = renderActions({ isRunning: true, hasSendableContent: false });

    expect(markup).toContain('aria-label="Stop generation"');
    expect(markup).not.toContain('aria-label="Send message"');
  });

  it("keeps stop available while a turn is running with a steering draft", () => {
    const markup = renderActions({ isRunning: true, hasSendableContent: true });

    expect(markup).toContain('aria-label="Stop generation"');
    expect(markup).not.toContain('aria-label="Send message"');
  });

  it("disables an idle send when there is no sendable content", () => {
    const markup = renderActions({ hasSendableContent: false });

    expect(markup).toContain('aria-label="Send message"');
    expect(markup).toContain(' disabled=""');
  });
});
