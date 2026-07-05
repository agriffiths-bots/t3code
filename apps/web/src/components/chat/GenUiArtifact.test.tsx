import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import GenUiArtifact from "./GenUiArtifact";
import { MAX_GENUI_HTML_BYTES } from "./GenUiArtifact.logic";
import { SAMPLE_GENUI_CHART } from "./GenUiArtifact.sample";

describe("GenUiArtifact", () => {
  it("renders the sample chart in a fully-inert sandboxed iframe", () => {
    const markup = renderToStaticMarkup(
      <GenUiArtifact html={SAMPLE_GENUI_CHART} isStreaming={false} />,
    );
    expect(markup).toContain("<iframe");
    expect(markup).toContain('sandbox=""');
    // Scripts and the origin barrier must never be enabled.
    expect(markup).not.toContain("allow-scripts");
    expect(markup).not.toContain("allow-same-origin");
    expect(markup.toLowerCase()).toContain('referrerpolicy="no-referrer"');
    // The untrusted markup only ever appears inside the iframe srcdoc.
    expect(markup.toLowerCase()).toContain("srcdoc=");
    expect(markup).toContain("Weekly deploys");
    // And the CSP travels with it.
    expect(markup).toContain("Content-Security-Policy");
  });

  it("shows a placeholder and no iframe while streaming", () => {
    const markup = renderToStaticMarkup(<GenUiArtifact html={SAMPLE_GENUI_CHART} isStreaming />);
    expect(markup).toContain("Generating UI");
    expect(markup).not.toContain("<iframe");
  });

  it("refuses oversized markup instead of rendering it", () => {
    const huge = `<div>${"x".repeat(MAX_GENUI_HTML_BYTES + 1)}</div>`;
    const markup = renderToStaticMarkup(<GenUiArtifact html={huge} isStreaming={false} />);
    expect(markup).toContain("too large");
    expect(markup).not.toContain("<iframe");
  });

  it("renders a note for an empty block", () => {
    const markup = renderToStaticMarkup(<GenUiArtifact html="   " isStreaming={false} />);
    expect(markup).toContain("Empty generative UI");
    expect(markup).not.toContain("<iframe");
  });
});
