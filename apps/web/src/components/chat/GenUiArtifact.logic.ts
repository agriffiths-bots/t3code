/**
 * Generative-UI artifact — pure logic and the security policy for rendering
 * model-generated markup inside the chat transcript.
 *
 * The model emits a self-contained visual in a fenced ```genui code block.
 * `ChatMarkdown` detects that fence and renders the markup inside a
 * hard-sandboxed iframe using {@link buildGenUiSrcdoc}. Everything here is
 * framework-agnostic (pure JS, no DOM) so the security contract can be
 * unit-tested in isolation from React and runs identically in Node and browser.
 *
 * SECURITY MODEL — model output is UNTRUSTED. Three independent layers, so no
 * single failure opens a hole:
 *
 *   1. ALLOWLIST SANITIZATION (primary — {@link sanitizeGenUiHtml}): the markup
 *      is parsed and reduced to a safe, declarative subset before it is ever
 *      wrapped for rendering. The GitHub-derived {@link GENUI_SANITIZE_SCHEMA}
 *      DROPS every scripting/navigation/loading construct — `<script>`,
 *      `<meta>` (so no `<meta http-equiv="refresh">` auto-navigation),
 *      `<iframe>`, `<object>`, `<embed>`, `<base>`, `<link>`, `<form>`, `<a>`,
 *      `<img>`, and all `on*` event handlers and unsafe-protocol URLs — while
 *      allowing structural/text elements plus inline `<style>` and `style`/
 *      `class` attributes so charts and diagrams still render. This is what
 *      makes the "no network egress" claim hold: with `<meta refresh>` and
 *      anchors removed, an untrusted (possibly chat-derived) payload has no way
 *      to navigate itself off-origin, even statically.
 *
 *   2. A FULLY INERT iframe sandbox ({@link GENUI_SANDBOX} = `""`, i.e. every
 *      restriction on): no `allow-same-origin` (opaque, unique origin — no
 *      access to parent DOM, cookies, `localStorage`, IndexedDB, or the app's
 *      same-origin network context), no `allow-scripts` (belt-and-suspenders
 *      with layer 1 — no JS even if some slipped through), and no
 *      `allow-forms`/`allow-popups`/`allow-top-navigation`/`allow-modals`/
 *      `allow-downloads`.
 *
 *   3. The {@link GENUI_CSP} injected as the FIRST `<meta>` in `<head>`:
 *      `default-src 'none'` denies all resource/connection egress and script
 *      execution (no `connect-src`, no `script-src`, no remote origins), while
 *      `style-src`/`img-src`/`font-src`/`media-src` permit only inline styles
 *      and `data:` media (so CSS `url(...)` egress is blocked too). CSP via
 *      `<meta>` is monotonic — untrusted content could only tighten it, never
 *      relax it; and layer 1 has already removed any `<meta>` anyway.
 *
 * We never `eval` the markup or inject it into the parent document; it only
 * ever reaches the DOM as the `srcdoc` of the sandboxed iframe.
 */

import { fromHtml } from "hast-util-from-html";
import { type Schema, defaultSchema, sanitize } from "hast-util-sanitize";
import { toHtml } from "hast-util-to-html";

/**
 * Hard cap on the model-generated markup we will render, in UTF-8 bytes.
 * Larger payloads are refused (rendered as an inert fallback) so a runaway
 * generation cannot lock up the renderer or the iframe.
 */
export const MAX_GENUI_HTML_BYTES = 128 * 1024;

/** Default rendered height of the artifact iframe, in px. Content scrolls within. */
export const GENUI_DEFAULT_HEIGHT = 420;

/** Upper bound on the artifact iframe height, in px, so it stays a bounded box. */
export const GENUI_MAX_HEIGHT = 720;

/**
 * The iframe sandbox token set. Deliberately EMPTY — every sandbox restriction
 * applies, including no scripts and no same-origin. See the security model
 * above. Exported as a constant so tests can assert it never silently gains
 * `allow-scripts` (self-navigation exfil) or `allow-same-origin` (origin
 * barrier collapse).
 */
export const GENUI_SANDBOX = "";

/**
 * Content-Security-Policy for the artifact document. Permits inert,
 * self-contained rendering only (inline styles + `data:` media), denies script
 * execution and every network origin. Keep single-quoted keywords (no double
 * quotes) so it embeds safely in an HTML attribute.
 */
export const GENUI_CSP = [
  // default-src 'none' also governs script-src/connect-src (both absent),
  // so scripts and all network connections are denied by CSP as well.
  "default-src 'none'",
  "style-src 'unsafe-inline'",
  "img-src data:",
  "font-src data:",
  "media-src data:",
  "base-uri 'none'",
  "form-action 'none'",
  // Deny the document navigating itself off-origin (meta refresh).
  // Enforced where supported; see the residual note in the module doc.
  "navigate-to 'none'",
].join("; ");

/** Minimal reset so bare markup looks intentional without dictating the design. */
const GENUI_BASE_STYLE = [
  ":root{color-scheme:light dark}",
  "html,body{margin:0;padding:0}",
  "*,*::before,*::after{box-sizing:border-box}",
  "body{font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;padding:12px;line-height:1.5}",
  "svg{max-width:100%;height:auto}",
].join("");

/** UTF-8 byte length of the markup (real bytes when `TextEncoder` is present). */
export function genUiHtmlByteLength(html: string): number {
  if (typeof TextEncoder !== "undefined") {
    return new TextEncoder().encode(html).length;
  }
  // Conservative upper bound (every char at most 3 UTF-8 bytes in the BMP).
  return html.length * 3;
}

/** Whether the markup is small enough to render (see {@link MAX_GENUI_HTML_BYTES}). */
export function isGenUiHtmlWithinCap(html: string): boolean {
  return genUiHtmlByteLength(html) <= MAX_GENUI_HTML_BYTES;
}

/**
 * Allowlist for {@link sanitizeGenUiHtml}. Starts from the GitHub-derived
 * {@link defaultSchema} (which already strips `<script>`, `<meta>`, `<iframe>`,
 * `<object>`, `<embed>`, `<base>`, `<link>`, `<form>`, `on*` handlers, and
 * unsafe-protocol URLs — the whole scripting/navigation/loading class) and then:
 *
 *   - DROPS `<a>` and `<img>` (and `<picture>`/`<source>`/`<input>`) from the
 *     tag allowlist so there is no clickable-link or image-beacon egress vector
 *     at all. Removed elements are unwrapped, so their inert text survives.
 *   - ADDS `<style>` plus the `style` and `class` attributes on every element,
 *     so artifacts can actually look like charts. Inline CSS is inert (no
 *     scripting, no navigation); external `url(...)`/`@import` loads are blocked
 *     by {@link GENUI_CSP}.
 */
export const GENUI_SANITIZE_SCHEMA: Schema = {
  ...defaultSchema,
  tagNames: [
    ...(defaultSchema.tagNames ?? []).filter(
      (name) => !["a", "img", "picture", "source", "input"].includes(name),
    ),
    "style",
  ],
  attributes: {
    ...defaultSchema.attributes,
    "*": [...(defaultSchema.attributes?.["*"] ?? []), "style", "className"],
  },
};

/**
 * Reduce untrusted model markup to a safe, declarative subset — see
 * {@link GENUI_SANITIZE_SCHEMA}. Pure JS (no DOM): parse HTML fragment → HAST,
 * sanitize against the allowlist, serialize back to HTML.
 */
export function sanitizeGenUiHtml(html: string): string {
  const tree = fromHtml(html, { fragment: true });
  return toHtml(sanitize(tree, GENUI_SANITIZE_SCHEMA));
}

/**
 * Wrap untrusted markup in a complete, CSP-guarded HTML document suitable for
 * an iframe `srcdoc`. The markup is FIRST sanitized to the safe subset, then
 * placed in `<body>`; the CSP `<meta>` is emitted first in `<head>` so it
 * governs the whole document. Sanitization is applied here — the single sink —
 * so no caller can accidentally bypass it and feed raw markup to the iframe.
 */
export function buildGenUiSrcdoc(html: string): string {
  const safeHtml = sanitizeGenUiHtml(html);
  return [
    "<!doctype html>",
    '<html lang="en">',
    "<head>",
    '<meta charset="utf-8">',
    `<meta http-equiv="Content-Security-Policy" content="${GENUI_CSP}">`,
    '<meta name="referrer" content="no-referrer">',
    '<meta name="viewport" content="width=device-width, initial-scale=1">',
    `<style>${GENUI_BASE_STYLE}</style>`,
    "</head>",
    `<body>${safeHtml}</body>`,
    "</html>",
  ].join("");
}
