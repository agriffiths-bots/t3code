# Generative UI (prototype)

A prototype that lets a model render a small, self-contained visual —
a chart, diagram, or mockup — directly in the chat transcript, in the spirit
of Claude-Desktop artifacts. It is a proof of concept: focused, off by default,
and deliberately not production-hardened.

## The `genui` contract

The model emits a self-contained visual as a fenced code block tagged `genui`:

````markdown
```genui
<style>.bar{fill:#4338ca}</style>
<svg viewBox="0 0 120 60" width="100%">
  <rect class="bar" x="8" y="10" width="24" height="40" rx="4" />
  <rect class="bar" x="48" y="24" width="24" height="26" rx="4" />
  <rect class="bar" x="88" y="4" width="24" height="46" rx="4" />
</svg>
```
````

The block's body is treated as **untrusted**. It is sanitized to a safe,
declarative subset — structural/text HTML plus inline `<style>` and
`style`/`class` attributes — and rendered inside a hard-sandboxed iframe
(`apps/web/src/components/chat/GenUiArtifact.tsx`). Scripts, `<meta>`, `<a>`,
`<img>`, `<iframe>`, `<form>`, and network/navigation constructs are **removed**
(see below); charts and diagrams use HTML + CSS.

## Enabling it

Off by default. Toggle **Settings → General → Generative UI (experimental)**
(client setting `enableGenerativeUi`). When disabled, a `genui` block renders as
an ordinary, inert code block showing its source.

## Security model

Model output is untrusted. Rendering is defended in three independent layers,
all defined in `apps/web/src/components/chat/GenUiArtifact.logic.ts`:

1. **Allowlist sanitization (primary)** — `sanitizeGenUiHtml` parses the markup
   and reduces it to a safe subset using a GitHub-derived schema. It **drops**
   `<script>`, `<meta>` (so no `<meta http-equiv="refresh">` auto-navigation),
   `<iframe>`, `<object>`, `<embed>`, `<base>`, `<link>`, `<form>`, `<a>`,
   `<img>`, every `on*` handler, and unsafe-protocol URLs — while keeping
   structural/text elements plus inline `<style>` and `style`/`class`
   attributes. This is what makes "no egress" hold: with `<meta refresh>` and
   anchors gone, an untrusted (possibly chat-derived) payload has no way to
   navigate itself off-origin, statically or otherwise.

2. **Fully inert iframe sandbox** (`sandbox=""` — every restriction on) — no
   `allow-same-origin` (opaque origin: no parent DOM, cookies,
   `localStorage`/IndexedDB, or same-origin network), no `allow-scripts`
   (belt-and-suspenders with layer 1), and no `allow-top-navigation`,
   `allow-forms`, `allow-popups`, `allow-modals`, or `allow-downloads`.

3. **Strict CSP** (`<meta>`, first in `<head>`) — `default-src 'none'` denies
   script execution and all resource/connection egress (no `script-src`, no
   `connect-src`, no remote origins), plus `form-action 'none'` and
   `navigate-to 'none'`. Only inline styles and `data:` media are permitted, so
   even CSS `url(...)`/`@import` egress is blocked. CSP via `<meta>` is
   monotonic: markup could only tighten it, never relax it (and layer 1 already
   removed any `<meta>`).

Additional bounds: markup over `MAX_GENUI_HTML_BYTES` (128 KB) is refused
rather than rendered; the iframe is a bounded box (capped height, content
scrolls); the markup only ever reaches the DOM as the iframe `srcdoc` — it is
never `eval`'d or injected into the parent document; and partial markup is not
rendered while the message is still streaming.

## Not in this prototype

- No first-class MCP `render_ui` tool. The artifact travels in the assistant's
  own message, which streams to the web in full — a real MCP tool call's
  arguments can be truncated in provider transcripts, so the content-block
  approach is both simpler and more reliable for a PoC. Promoting to a tool the
  model discovers via its toolset is the natural follow-up.
- No script execution. The brief suggested `sandbox="allow-scripts"`, but the
  security review showed scripts over untrusted, chat-derived output create an
  exfiltration-by-self-navigation channel that current browsers cannot reliably
  close. Re-enabling scripts should wait for a real containment mechanism (a
  dedicated artifact origin, enforced `navigate-to`, or a navigation-intercept
  proxy).
- No SVG or `<img>` yet. The sanitizer allows an HTML + CSS subset only;
  allowlisting a safe static-SVG subset (no `<script>`/`<foreignObject>`/
  external `href`) is a natural follow-up.
- No auto-resize handshake between iframe and parent (kept fixed-height to
  avoid an untrusted → parent channel).
- No persistence of artifacts.
