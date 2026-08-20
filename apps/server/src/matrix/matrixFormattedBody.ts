/**
 * Matrix `m.text` rich-text helper. `body` stays the original markdown; when
 * rendering succeeds, `formatted_body` is HTML restricted to the Client-Server
 * spec's suggested m.room.message subset (spec v1.19,
 * https://spec.matrix.org/v1.19/client-server-api/#mroommessage-msgtypes).
 */
export const MATRIX_HTML_FORMAT = "org.matrix.custom.html";
/** UTF-8 cap on `formatted_body` itself. Oversize falls back to plain `body`. */
export const MATRIX_FORMATTED_BODY_MAX_BYTES = 32 * 1024;
/**
 * Cap on JSON.stringify of the full `m.text` content object, including
 * `formatted_body`. Encryption then base64-expands that payload, so this is
 * kept well under the common 64 KiB event limit. Oversize falls back to
 * plaintext `body` only, which is what the bridge sent before this change.
 */
export const MATRIX_FORMATTED_CONTENT_MAX_JSON_BYTES = 24 * 1024;
const MATRIX_HTML_MAX_NESTING = 100;
/** Operation cap as a multiple of input length; overage falls back to plaintext. */
const RENDER_OPS_PER_CHAR = 32;
const HTML_VOID_TAGS = new Set([
  "area",
  "base",
  "br",
  "col",
  "embed",
  "hr",
  "img",
  "input",
  "link",
  "meta",
  "param",
  "source",
  "track",
  "wbr",
]);

export type MatrixTextContent =
  | {
      readonly msgtype: "m.text";
      readonly body: string;
    }
  | {
      readonly msgtype: "m.text";
      readonly body: string;
      readonly format: typeof MATRIX_HTML_FORMAT;
      readonly formatted_body: string;
    };

const utf8Bytes = (value: string) => new TextEncoder().encode(value).byteLength;

export function matrixTextContent(body: string): MatrixTextContent {
  try {
    const formatted = formatMatrixBody(body);
    if (formatted === null) return { msgtype: "m.text", body };
    return {
      msgtype: "m.text",
      body,
      format: MATRIX_HTML_FORMAT,
      formatted_body: formatted,
    };
  } catch {
    return { msgtype: "m.text", body };
  }
}

function formatMatrixBody(markdown: string): string | null {
  // Body JSON is a lower bound on the formatted content object, so oversize
  // markdown never pays for a render that would be discarded. It also bounds
  // parser work on a single event-loop turn.
  if (utf8Bytes(markdown) > MATRIX_FORMATTED_CONTENT_MAX_JSON_BYTES) return null;
  const rendered = sanitizeMatrixHtml(renderMarkdownToHtml(markdown));
  if (rendered.length === 0) return null;
  if (utf8Bytes(rendered) > MATRIX_FORMATTED_BODY_MAX_BYTES) return null;
  const serialized = utf8Bytes(
    JSON.stringify({
      msgtype: "m.text",
      body: markdown,
      format: MATRIX_HTML_FORMAT,
      formatted_body: rendered,
    }),
  );
  if (serialized > MATRIX_FORMATTED_CONTENT_MAX_JSON_BYTES) return null;
  return rendered;
}

const ALLOWED_TAGS = new Set([
  "del",
  "h1",
  "h2",
  "h3",
  "h4",
  "h5",
  "h6",
  "blockquote",
  "p",
  "a",
  "ul",
  "ol",
  "sup",
  "sub",
  "li",
  "b",
  "i",
  "u",
  "strong",
  "em",
  "s",
  "code",
  "hr",
  "br",
  "div",
  "table",
  "thead",
  "tbody",
  "tr",
  "th",
  "td",
  "caption",
  "pre",
  "span",
  "img",
  "details",
  "summary",
]);
const VOID_TAGS = new Set(["br", "hr", "img"]);
const DROP_WITH_CONTENT = new Set([
  "script",
  "style",
  "iframe",
  "object",
  "embed",
  "link",
  "meta",
  "svg",
  "math",
  "template",
  "noscript",
  "form",
  "input",
  "button",
  "textarea",
  "select",
]);
const ALLOWED_ATTRS: Readonly<Record<string, ReadonlySet<string>>> = {
  span: new Set(["data-mx-bg-color", "data-mx-color", "data-mx-spoiler", "data-mx-maths"]),
  a: new Set(["href", "target"]),
  img: new Set(["width", "height", "alt", "title", "src"]),
  ol: new Set(["start"]),
  code: new Set(["class"]),
  div: new Set(["data-mx-maths"]),
};
const PERMITTED_HREF_SCHEMES = new Set(["https:", "http:", "ftp:", "mailto:", "magnet:"]);
const LANGUAGE_CLASS = /^language-[A-Za-z0-9_+-]+$/;
const HEX_COLOR = /^#[0-9A-Fa-f]{6}$/;
const INTEGER = /^-?\d+$/;
const TAG_NAME = /^[A-Za-z][A-Za-z0-9:-]*$/;

export function sanitizeMatrixHtml(html: string): string {
  let out = "";
  const stack: Array<{ readonly name: string; readonly emit: boolean }> = [];
  let i = 0;
  while (i < html.length) {
    if (html[i] !== "<") {
      const next = html.indexOf("<", i);
      out += html.slice(i, next === -1 ? html.length : next);
      i = next === -1 ? html.length : next;
      continue;
    }

    if (html.startsWith("<!--", i)) {
      const end = html.indexOf("-->", i + 4);
      i = end === -1 ? html.length : end + 3;
      continue;
    }
    if (html.startsWith("<![CDATA[", i)) {
      const end = html.indexOf("]]>", i + 9);
      i = end === -1 ? html.length : end + 3;
      continue;
    }
    if (html.startsWith("<!", i) || html.startsWith("<?", i)) {
      const end = html.indexOf(">", i + 2);
      i = end === -1 ? html.length : end + 1;
      continue;
    }

    const parsed = parseHtmlTag(html, i);
    if (parsed === null) {
      out += "&lt;";
      i += 1;
      continue;
    }
    i = parsed.end;
    const name = parsed.name;

    if (parsed.closing) {
      const match = stack.findLastIndex((open) => open.name === name);
      if (match === -1) continue;
      let emitClose = false;
      while (stack.length > match) {
        const open = stack.pop();
        if (open === undefined) continue;
        if (open.name === name) {
          emitClose = open.emit;
          continue;
        }
        if (open.emit) out += `</${open.name}>`;
      }
      if (emitClose) out += `</${name}>`;
      continue;
    }

    if (DROP_WITH_CONTENT.has(name)) {
      // HTML ignores the self-closing slash on non-void elements, so
      // `<script/>...` still runs to the matching `</script>`.
      if (!HTML_VOID_TAGS.has(name)) {
        i = skipDroppedElement(html, name, i);
      }
      continue;
    }
    if (!ALLOWED_TAGS.has(name)) continue;

    const attrs = serializeAllowedAttrs(name, parsed.attrs);
    const emit =
      stack.length < MATRIX_HTML_MAX_NESTING && !tagMissingRequiredAttr(name, parsed.attrs);
    if (VOID_TAGS.has(name) || parsed.selfClosing) {
      if (emit) {
        out += `<${name}${attrs}>`;
        if (parsed.selfClosing && !VOID_TAGS.has(name)) out += `</${name}>`;
      }
      continue;
    }
    stack.push({ name, emit });
    if (emit) out += `<${name}${attrs}>`;
  }

  while (stack.length > 0) {
    const open = stack.pop();
    if (open?.emit === true) out += `</${open.name}>`;
  }
  return out;
}

function skipDroppedElement(html: string, name: string, from: number): number {
  let i = from;
  while (i < html.length) {
    const next = html.indexOf("<", i);
    if (next === -1) return html.length;
    const parsed = parseHtmlTag(html, next);
    if (parsed === null) {
      i = next + 1;
      continue;
    }
    if (parsed.closing && parsed.name === name) return parsed.end;
    i = parsed.end;
  }
  return html.length;
}

type ParsedAttr = { readonly name: string; readonly value: string };
type ParsedTag = {
  readonly name: string;
  readonly attrs: ReadonlyArray<ParsedAttr>;
  readonly closing: boolean;
  readonly selfClosing: boolean;
  readonly end: number;
};

function parseHtmlTag(html: string, start: number): ParsedTag | null {
  if (html[start] !== "<") return null;
  let i = start + 1;
  const closing = html[i] === "/";
  if (closing) i += 1;
  const nameStart = i;
  while (i < html.length && /[A-Za-z0-9:-]/.test(html[i] ?? "")) i += 1;
  const name = html.slice(nameStart, i).toLowerCase();
  if (name.length === 0 || !TAG_NAME.test(name)) return null;
  const attrs: ParsedAttr[] = [];
  while (i < html.length) {
    while (i < html.length && /\s/.test(html[i] ?? "")) i += 1;
    if (i >= html.length) return null;
    if (html[i] === ">") return { name, attrs, closing, selfClosing: false, end: i + 1 };
    if (html[i] === "/" && html[i + 1] === ">") {
      return { name, attrs, closing, selfClosing: true, end: i + 2 };
    }
    const attrNameStart = i;
    while (i < html.length && /[^\s=/>]/.test(html[i] ?? "")) i += 1;
    if (i === attrNameStart) return null;
    const attrName = html.slice(attrNameStart, i).toLowerCase();
    while (i < html.length && /\s/.test(html[i] ?? "")) i += 1;
    let value = "";
    if (html[i] === "=") {
      i += 1;
      while (i < html.length && /\s/.test(html[i] ?? "")) i += 1;
      const quote = html[i];
      if (quote === '"' || quote === "'") {
        i += 1;
        const valueStart = i;
        const valueEnd = html.indexOf(quote, i);
        if (valueEnd === -1) return null;
        value = html.slice(valueStart, valueEnd);
        i = valueEnd + 1;
      } else {
        const valueStart = i;
        while (i < html.length && /[^\s>]/.test(html[i] ?? "")) i += 1;
        value = html.slice(valueStart, i);
      }
    }
    attrs.push({ name: attrName, value });
  }
  return null;
}

function tagMissingRequiredAttr(tag: string, attrs: ReadonlyArray<ParsedAttr>): boolean {
  const required = tag === "a" ? "href" : tag === "img" ? "src" : null;
  if (required === null) return false;
  for (const attr of attrs) {
    if (attr.name !== required) continue;
    const decoded = decodeHtmlEntities(attr.value);
    if (sanitizeAttrValue(tag, attr.name, decoded) !== null) return false;
  }
  return true;
}

function serializeAllowedAttrs(tag: string, attrs: ReadonlyArray<ParsedAttr>): string {
  const allowed = ALLOWED_ATTRS[tag];
  if (allowed === undefined) return "";
  let out = "";
  for (const attr of attrs) {
    if (attr.name.startsWith("on")) continue;
    if (!allowed.has(attr.name)) continue;
    const decoded = decodeHtmlEntities(attr.value);
    const sanitized = sanitizeAttrValue(tag, attr.name, decoded);
    if (sanitized === null) continue;
    out += ` ${attr.name}="${escapeAttr(sanitized)}"`;
  }
  return out;
}

function sanitizeAttrValue(tag: string, name: string, value: string): string | null {
  if (tag === "a" && name === "href") return isPermittedHref(value) ? value.trim() : null;
  if (tag === "a" && name === "target") return value.trim() === "_blank" ? "_blank" : null;
  if (tag === "img" && name === "src") return isMxcUrl(value) ? value.trim() : null;
  if (tag === "img" && (name === "width" || name === "height")) {
    return INTEGER.test(value.trim()) ? value.trim() : null;
  }
  if (tag === "img" && (name === "alt" || name === "title")) return value;
  if (tag === "ol" && name === "start") return INTEGER.test(value.trim()) ? value.trim() : null;
  if (tag === "code" && name === "class") {
    const cls = value.trim();
    return LANGUAGE_CLASS.test(cls) ? cls : null;
  }
  if (
    (tag === "span" || tag === "div") &&
    (name === "data-mx-bg-color" || name === "data-mx-color")
  ) {
    return HEX_COLOR.test(value.trim()) ? value.trim() : null;
  }
  if (tag === "span" && (name === "data-mx-spoiler" || name === "data-mx-maths")) return value;
  if (tag === "div" && name === "data-mx-maths") return value;
  return null;
}

export function isPermittedHref(value: string): boolean {
  const trimmed = value.trim();
  if (trimmed.length === 0) return false;
  for (const ch of trimmed) {
    const code = ch.charCodeAt(0);
    if (code <= 0x20 || code === 0x7f) return false;
  }
  try {
    const url = new URL(trimmed);
    return PERMITTED_HREF_SCHEMES.has(url.protocol);
  } catch {
    return false;
  }
}

function isMxcUrl(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed.toLowerCase().startsWith("mxc://")) return false;
  try {
    return new URL(trimmed).protocol === "mxc:";
  } catch {
    return false;
  }
}

function decodeHtmlEntities(value: string): string {
  return value.replace(/&(#x?[0-9A-Fa-f]+|[A-Za-z]+);/g, (entity, body: string) => {
    if (body[0] === "#") {
      const code =
        body[1] === "x" || body[1] === "X"
          ? Number.parseInt(body.slice(2), 16)
          : Number.parseInt(body.slice(1), 10);
      if (!Number.isFinite(code) || code <= 0 || code > 0x10ffff) return entity;
      return String.fromCodePoint(code);
    }
    switch (body) {
      case "amp":
        return "&";
      case "lt":
        return "<";
      case "gt":
        return ">";
      case "quot":
        return '"';
      case "apos":
        return "'";
      default:
        return entity;
    }
  });
}

function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function escapeAttr(value: string): string {
  return escapeHtml(value).replace(/"/g, "&quot;");
}

/** Hard cap on parser work. Every re-enterable scan step must call tick. */
class RenderBudget {
  private ops = 0;
  private readonly maxOps: number;
  constructor(chars: number) {
    this.maxOps = Math.max(4096, chars * RENDER_OPS_PER_CHAR);
  }
  tick(n = 1): void {
    this.ops += n;
    if (this.ops > this.maxOps) throw new Error("matrix render op budget exceeded");
  }
}

function renderMarkdownToHtml(markdown: string): string {
  const budget = new RenderBudget(markdown.length);
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  return renderLines(lines, 0, lines.length, 0, budget).html;
}

function renderLines(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  minIndent: number,
  budget: RenderBudget,
): { html: string; next: number } {
  let html = "";
  let i = from;
  while (i < to) {
    budget.tick();
    const line = lines[i] ?? "";
    if (line.trim() === "") {
      i += 1;
      continue;
    }
    const indent = leadingIndent(line);
    if (indent < minIndent) break;

    const openingFence = parseOpenFence(line);
    if (openingFence !== null) {
      const fence = parseFence(lines, i, to, openingFence);
      html += fence.html;
      i = fence.next;
      continue;
    }

    const heading = parseAtxHeading(line);
    if (heading !== null) {
      html += `<h${heading.level}>${renderInline(heading.text, budget)}</h${heading.level}>`;
      i += 1;
      continue;
    }

    if (isHorizontalRule(line)) {
      html += "<hr>";
      i += 1;
      continue;
    }

    if (/^[ \t]*>/.test(line)) {
      const quote = parseBlockquote(lines, i, to, budget);
      html += quote.html;
      i = quote.next;
      continue;
    }

    const list = listMarker(line);
    if (list !== null && list.indent >= minIndent) {
      const parsed = parseList(lines, i, to, list, budget);
      html += parsed.html;
      i = parsed.next;
      continue;
    }

    const para: string[] = [];
    while (i < to) {
      const current = lines[i] ?? "";
      if (current.trim() === "" || isBlockStart(current)) break;
      para.push(current.trimEnd());
      i += 1;
    }
    if (para.length === 0) {
      html += `<p>${renderInline(line, budget)}</p>`;
      i += 1;
      continue;
    }
    html += `<p>${renderInline(para.join("\n"), budget)}</p>`;
  }
  return { html, next: i };
}

function parseOpenFence(line: string): { marker: string; length: number; info: string } | null {
  const match = /^( {0,3})(`{3,}|~{3,})(.*)$/.exec(line);
  if (match?.[2] === undefined) return null;
  const fence = match[2];
  const marker = fence[0] ?? "";
  const info = match[3] ?? "";
  if (marker === "`" && info.includes("`")) return null;
  return {
    marker,
    length: fence.length,
    info: info.trim().split(/\s/)[0] ?? "",
  };
}

function isClosingFence(line: string, marker: string, length: number): boolean {
  const match = /^( {0,3})(`{3,}|~{3,})[ \t]*$/.exec(line);
  if (match?.[2] === undefined) return false;
  const fence = match[2];
  return fence[0] === marker && fence.length >= length;
}

function parseFence(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  opening: { marker: string; length: number; info: string },
): { html: string; next: number } {
  const code: string[] = [];
  let i = from + 1;
  while (i < to && !isClosingFence(lines[i] ?? "", opening.marker, opening.length)) {
    code.push(lines[i] ?? "");
    i += 1;
  }
  if (i < to) i += 1;
  const cls = LANGUAGE_CLASS.test(`language-${opening.info}`)
    ? ` class="language-${opening.info}"`
    : "";
  return {
    html: `<pre><code${cls}>${escapeHtml(code.join("\n"))}</code></pre>`,
    next: i,
  };
}

function parseBlockquote(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  budget: RenderBudget,
): { html: string; next: number } {
  const inner: string[] = [];
  let i = from;
  while (i < to) {
    const line = lines[i] ?? "";
    if (line.trim() === "") {
      inner.push("");
      i += 1;
      continue;
    }
    if (!/^[ \t]*>/.test(line)) break;
    inner.push(line.replace(/^[ \t]*>[ \t]?/, ""));
    i += 1;
  }
  while (inner.length > 0 && inner[inner.length - 1] === "") inner.pop();
  return {
    html: `<blockquote>${renderLines(inner, 0, inner.length, 0, budget).html}</blockquote>`,
    next: i,
  };
}

function parseList(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  first: ListMarker,
  budget: RenderBudget,
): { html: string; next: number } {
  const items: string[] = [];
  let i = from;
  const start: number | null = first.kind === "ol" ? first.start : null;
  while (i < to) {
    const line = lines[i] ?? "";
    if (line.trim() === "") {
      const next = peekNonEmpty(lines, i + 1, to, budget);
      if (next === null) break;
      const nextMarker = listMarker(next.line);
      if (
        nextMarker !== null &&
        nextMarker.indent === first.indent &&
        nextMarker.kind === first.kind
      ) {
        i = next.index;
        continue;
      }
      if (leadingIndent(next.line) > first.indent) {
        i = next.index;
        continue;
      }
      break;
    }
    const marker = listMarker(line);
    if (marker === null || marker.indent < first.indent) break;
    if (marker.indent === first.indent && marker.kind !== first.kind) break;
    if (marker.indent === first.indent) {
      const item = collectListItem(lines, i, to, marker, budget);
      items.push(`<li>${item.html}</li>`);
      i = item.next;
      continue;
    }
    break;
  }
  const startAttr =
    first.kind === "ol" && start !== null && start !== 1 ? ` start="${String(start)}"` : "";
  return { html: `<${first.kind}${startAttr}>${items.join("")}</${first.kind}>`, next: i };
}

function collectListItem(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  marker: ListMarker,
  budget: RenderBudget,
): { html: string; next: number } {
  const itemLines = [marker.text];
  let i = from + 1;
  while (i < to) {
    const line = lines[i] ?? "";
    if (line.trim() === "") {
      const next = peekNonEmpty(lines, i + 1, to, budget);
      if (next === null) break;
      if (leadingIndent(next.line) >= marker.contentIndent) {
        while (i < next.index) {
          itemLines.push("");
          i += 1;
        }
        continue;
      }
      break;
    }
    const nested = listMarker(line);
    if (nested !== null && nested.indent <= marker.indent) break;
    if (leadingIndent(line) >= marker.contentIndent) {
      itemLines.push(stripIndent(line, marker.contentIndent));
      i += 1;
      continue;
    }
    break;
  }
  const first = renderInline(itemLines[0] ?? "", budget);
  const html =
    itemLines.length === 1
      ? first
      : `${first}${renderLines(itemLines, 1, itemLines.length, 0, budget).html}`;
  return { html, next: i };
}

interface ListMarker {
  readonly kind: "ul" | "ol";
  readonly indent: number;
  readonly contentIndent: number;
  readonly start: number;
  readonly text: string;
}

function listMarker(line: string): ListMarker | null {
  const ul = /^([ \t]*)([-*+])[ \t]+(.*)$/.exec(line);
  if (ul?.[1] !== undefined && ul[3] !== undefined) {
    return {
      kind: "ul",
      indent: leadingIndent(ul[1]),
      contentIndent: line.length - ul[3].length,
      start: 1,
      text: ul[3],
    };
  }
  const ol = /^([ \t]*)(\d{1,9})\.[ \t]+(.*)$/.exec(line);
  if (ol?.[1] !== undefined && ol[2] !== undefined && ol[3] !== undefined) {
    return {
      kind: "ol",
      indent: leadingIndent(ol[1]),
      contentIndent: line.length - ol[3].length,
      start: Number.parseInt(ol[2], 10),
      text: ol[3],
    };
  }
  return null;
}

function stripIndent(line: string, count: number): string {
  let seen = 0;
  let i = 0;
  while (i < line.length && seen < count) {
    const ch = line[i];
    if (ch === " ") {
      seen += 1;
      i += 1;
      continue;
    }
    if (ch === "\t") {
      seen += 4;
      i += 1;
      continue;
    }
    break;
  }
  return line.slice(i);
}

function parseAtxHeading(line: string): { level: number; text: string } | null {
  const match = /^(#{1,6})(?:[ \t]+(.*))?$/.exec(line.trimEnd());
  if (match?.[1] === undefined) return null;
  return {
    level: match[1].length,
    text: trimTrailingHashes(match[2] ?? "").trim(),
  };
}

function isBlockStart(line: string): boolean {
  if (parseOpenFence(line) !== null) return true;
  if (parseAtxHeading(line) !== null) return true;
  if (isHorizontalRule(line)) return true;
  if (/^[ \t]*>/.test(line)) return true;
  return listMarker(line) !== null;
}

function isHorizontalRule(line: string): boolean {
  const trimmed = line.trim();
  return /^([-*_])\1{2,}$/.test(trimmed) && !/[^-_*]/.test(trimmed);
}

function peekNonEmpty(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  budget: RenderBudget,
): { index: number; line: string } | null {
  for (let i = from; i < to; i += 1) {
    budget.tick();
    const line = lines[i] ?? "";
    if (line.trim() !== "") return { index: i, line };
  }
  return null;
}

function leadingIndent(value: string): number {
  let indent = 0;
  for (const ch of value) {
    if (ch === " ") indent += 1;
    else if (ch === "\t") indent += 4;
    else break;
  }
  return indent;
}

function trimTrailingHashes(value: string): string {
  return value.replace(/[ \t]+#+$/, "").trim();
}

/**
 * Per-inline-pass memo so a closer or failed destination is never re-scanned.
 * Combined with RenderBudget.tick on every scan step, retry paths stay linear
 * (or abort to plaintext) instead of re-walking the same suffix.
 */
interface InlineScan {
  readonly noCloseFrom: Map<string, number>;
  readonly closerHit: Map<string, { from: number; pos: number }>;
  readonly destUnclosed: Set<number>;
}

function newInlineScan(): InlineScan {
  return { noCloseFrom: new Map(), closerHit: new Map(), destUnclosed: new Set() };
}

function indexOfOrMiss(
  src: string,
  needle: string,
  from: number,
  scan: InlineScan,
  budget: RenderBudget,
  key: string = needle,
): number {
  const missFrom = scan.noCloseFrom.get(key) ?? Number.POSITIVE_INFINITY;
  if (from >= missFrom) {
    budget.tick();
    return -1;
  }
  const hit = scan.closerHit.get(key);
  if (hit !== undefined && from >= hit.from && from <= hit.pos) {
    budget.tick();
    return hit.pos;
  }
  const found = src.indexOf(needle, from);
  if (found === -1) {
    budget.tick(Math.max(1, src.length - from));
    const current = scan.noCloseFrom.get(key);
    if (current === undefined || from < current) scan.noCloseFrom.set(key, from);
    return -1;
  }
  budget.tick(found - from + 1);
  scan.closerHit.set(key, { from, pos: found });
  return found;
}

interface InlineToken {
  html: string;
  tag: "em" | "strong" | "del" | null;
  nesting: -1 | 0 | 1;
}

interface Delim {
  marker: string;
  length: number;
  token: number;
  end: number;
  open: boolean;
  close: boolean;
}

function renderInline(
  src: string,
  budget: RenderBudget,
  scan: InlineScan = newInlineScan(),
): string {
  const tokens: InlineToken[] = [];
  const delimiters: Delim[] = [];
  let text = "";
  const flush = () => {
    if (text.length === 0) return;
    tokens.push({ html: text, tag: null, nesting: 0 });
    text = "";
  };
  const pushHtml = (html: string) => {
    if (html.length === 0) return;
    text += html;
  };

  let i = 0;
  while (i < src.length) {
    budget.tick();
    const ch = src[i] ?? "";
    if (ch === "\\" && i + 1 < src.length) {
      const next = src[i + 1] ?? "";
      if (isEscapableAsciiPunctuation(next)) {
        pushHtml(escapeHtml(next));
        i += 2;
        continue;
      }
      pushHtml("\\");
      i += 1;
      continue;
    }
    if (ch === "`") {
      const span = parseCodeSpan(src, i, scan, budget);
      if (span !== null) {
        pushHtml(`<code>${escapeHtml(span.inner)}</code>`);
        i = span.end;
        continue;
      }
      let run = 1;
      while (src[i + run] === "`") run += 1;
      pushHtml(escapeHtml("`".repeat(run)));
      i += run;
      continue;
    }
    if (ch === "*" || ch === "_" || ch === "~") {
      let run = 1;
      while (src[i + run] === ch) run += 1;
      const after = src[i + run];
      const before = src[i - 1];
      const afterSpace = after === undefined || after === " " || after === "\n";
      const beforeSpace = before === undefined || before === " " || before === "\n";
      if (ch === "~") {
        if (run < 2) {
          pushHtml("~");
          i += 1;
          continue;
        }
        flush();
        let remaining = run;
        if (remaining % 2 === 1) {
          tokens.push({ html: "~", tag: null, nesting: 0 });
          remaining -= 1;
        }
        while (remaining >= 2) {
          const token = tokens.length;
          tokens.push({ html: "~~", tag: null, nesting: 0 });
          delimiters.push({
            marker: "~",
            length: 0,
            token,
            end: -1,
            open: true,
            close: !beforeSpace,
          });
          remaining -= 2;
        }
        i += run;
        continue;
      }
      const canOpen = !afterSpace && (ch === "*" || !isWordChar(before));
      const canClose = !beforeSpace && (ch === "*" || !isWordChar(after));
      if (!canOpen && !canClose) {
        pushHtml(ch.repeat(run));
        i += run;
        continue;
      }
      flush();
      for (let n = 0; n < run; n += 1) {
        const token = tokens.length;
        tokens.push({ html: ch, tag: null, nesting: 0 });
        delimiters.push({
          marker: ch,
          length: run,
          token,
          end: -1,
          open: canOpen,
          close: canClose,
        });
      }
      i += run;
      continue;
    }
    if (ch === "!" && src[i + 1] === "[") {
      const link = parseInlineLink(src, i, true, scan, budget);
      if (link !== null) {
        pushHtml(link.html);
        i = link.end;
        continue;
      }
    }
    if (ch === "[") {
      const link = parseInlineLink(src, i, false, scan, budget);
      if (link !== null) {
        pushHtml(link.html);
        i = link.end;
        continue;
      }
    }
    if (ch === "<") {
      const autolink = parseAutolink(src, i, scan, budget);
      if (autolink !== null) {
        pushHtml(autolink.html);
        i = autolink.end;
        continue;
      }
      pushHtml("&lt;");
      i += 1;
      continue;
    }
    if (ch === "\n") {
      pushHtml("<br>");
      i += 1;
      continue;
    }
    pushHtml(escapeHtml(ch));
    i += 1;
  }
  flush();
  matchDelimiterPairs(delimiters, budget);
  applyEmphasis(delimiters, tokens);
  applyStrikethrough(delimiters, tokens);
  let out = "";
  for (const token of tokens) {
    if (token.nesting === 1 && token.tag !== null) out += `<${token.tag}>`;
    else if (token.nesting === -1 && token.tag !== null) out += `</${token.tag}>`;
    else out += token.html;
  }
  return out;
}

/**
 * CommonMark delimiter-stack pairing. Each closer walks back through unmatched
 * openers of the same marker using jump links and `openersBottom`, so the whole
 * pass is linear in the number of delimiter runs.
 */
function matchDelimiterPairs(delimiters: Delim[], budget: RenderBudget): void {
  const max = delimiters.length;
  if (max === 0) return;
  const openersBottom = new Map<string, number[]>();
  let headerIdx = 0;
  let lastTokenIdx = -2;
  const jumps = Array.from({ length: max }, () => 0);

  for (let closerIdx = 0; closerIdx < max; closerIdx += 1) {
    budget.tick();
    const closer = delimiters[closerIdx];
    if (closer === undefined) continue;

    if (delimiters[headerIdx]?.marker !== closer.marker || lastTokenIdx !== closer.token - 1) {
      headerIdx = closerIdx;
    }
    lastTokenIdx = closer.token;
    if (!closer.close) continue;

    let bottoms = openersBottom.get(closer.marker);
    if (bottoms === undefined) {
      bottoms = [-1, -1, -1, -1, -1, -1];
      openersBottom.set(closer.marker, bottoms);
    }
    const minOpenerIdx = bottoms[(closer.open ? 3 : 0) + (closer.length % 3)] ?? -1;
    let openerIdx = headerIdx - (jumps[headerIdx] ?? 0) - 1;
    let newMinOpenerIdx = openerIdx;

    for (; openerIdx > minOpenerIdx; openerIdx -= (jumps[openerIdx] ?? 0) + 1) {
      budget.tick();
      const opener = delimiters[openerIdx];
      if (opener === undefined || opener.marker !== closer.marker) continue;
      if (!(opener.open && opener.end < 0)) continue;

      let oddMatch = false;
      if (opener.close || closer.open) {
        if ((opener.length + closer.length) % 3 === 0) {
          if (opener.length % 3 !== 0 || closer.length % 3 !== 0) oddMatch = true;
        }
      }
      if (oddMatch) continue;

      const lastJump =
        openerIdx > 0 && delimiters[openerIdx - 1]?.open !== true
          ? (jumps[openerIdx - 1] ?? 0) + 1
          : 0;
      jumps[closerIdx] = closerIdx - openerIdx + lastJump;
      jumps[openerIdx] = lastJump;
      closer.open = false;
      opener.end = closerIdx;
      opener.close = false;
      newMinOpenerIdx = -1;
      lastTokenIdx = -2;
      break;
    }

    if (newMinOpenerIdx !== -1) {
      bottoms[(closer.open ? 3 : 0) + (closer.length % 3)] = newMinOpenerIdx;
    }
  }
}

function applyEmphasis(delimiters: Delim[], tokens: InlineToken[]): void {
  for (let i = delimiters.length - 1; i >= 0; i -= 1) {
    const start = delimiters[i];
    if (start === undefined || start.end < 0) continue;
    if (start.marker !== "*" && start.marker !== "_") continue;
    const end = delimiters[start.end];
    if (end === undefined) continue;

    const isStrong =
      i > 0 &&
      delimiters[i - 1]?.end === start.end + 1 &&
      delimiters[i - 1]?.marker === start.marker &&
      delimiters[i - 1]?.token === start.token - 1 &&
      delimiters[start.end + 1]?.token === end.token + 1;

    const opener = tokens[start.token];
    const closer = tokens[end.token];
    if (opener === undefined || closer === undefined) continue;
    const tag = isStrong ? "strong" : "em";
    opener.tag = tag;
    opener.nesting = 1;
    opener.html = "";
    closer.tag = tag;
    closer.nesting = -1;
    closer.html = "";
    if (isStrong) {
      const prev = delimiters[i - 1];
      const nextClose = delimiters[start.end + 1];
      if (prev !== undefined) {
        const adjacent = tokens[prev.token];
        if (adjacent !== undefined) adjacent.html = "";
      }
      if (nextClose !== undefined) {
        const adjacent = tokens[nextClose.token];
        if (adjacent !== undefined) adjacent.html = "";
      }
      i -= 1;
    }
  }
}

function applyStrikethrough(delimiters: Delim[], tokens: InlineToken[]): void {
  for (const start of delimiters) {
    if (start.marker !== "~" || start.end < 0) continue;
    const end = delimiters[start.end];
    if (end === undefined) continue;
    const opener = tokens[start.token];
    const closer = tokens[end.token];
    if (opener === undefined || closer === undefined) continue;
    opener.tag = "del";
    opener.nesting = 1;
    opener.html = "";
    closer.tag = "del";
    closer.nesting = -1;
    closer.html = "";
  }
}

const ESCAPABLE_ASCII_PUNCTUATION = new Set([
  "!",
  '"',
  "#",
  "$",
  "%",
  "&",
  "'",
  "(",
  ")",
  "*",
  "+",
  ",",
  "-",
  ".",
  "/",
  ":",
  ";",
  "<",
  "=",
  ">",
  "?",
  "@",
  "[",
  "\\",
  "]",
  "^",
  "_",
  "`",
  "{",
  "|",
  "}",
  "~",
]);

function isEscapableAsciiPunctuation(ch: string): boolean {
  return ESCAPABLE_ASCII_PUNCTUATION.has(ch);
}

function isWordChar(ch: string | undefined): boolean {
  return ch !== undefined && /[A-Za-z0-9]/.test(ch);
}

function parseCodeSpan(
  src: string,
  from: number,
  scan: InlineScan,
  budget: RenderBudget,
): { inner: string; end: number } | null {
  budget.tick();
  let n = 0;
  while (src[from + n] === "`") {
    budget.tick();
    n += 1;
  }
  if (n === 0) return null;
  const key = `code:${String(n)}`;
  if (from >= (scan.noCloseFrom.get(key) ?? Number.POSITIVE_INFINITY)) return null;
  let i = from + n;
  while (i < src.length) {
    budget.tick();
    if (src[i] !== "`") {
      i += 1;
      continue;
    }
    let m = 0;
    while (src[i + m] === "`") {
      budget.tick();
      m += 1;
    }
    if (m === n) {
      let inner = src.slice(from + n, i);
      if (inner.length >= 2 && inner.startsWith(" ") && inner.endsWith(" ")) {
        inner = inner.slice(1, -1);
      }
      return { inner, end: i + m };
    }
    i += m;
  }
  const current = scan.noCloseFrom.get(key);
  if (current === undefined || from < current) scan.noCloseFrom.set(key, from);
  return null;
}

function parseInlineLink(
  src: string,
  i: number,
  image: boolean,
  scan: InlineScan,
  budget: RenderBudget,
): { html: string; end: number } | null {
  budget.tick();
  const labelStart = image ? i + 1 : i;
  if (src[labelStart] !== "[") return null;
  const labelEnd = indexOfOrMiss(src, "]", labelStart + 1, scan, budget);
  if (labelEnd === -1 || src[labelEnd + 1] !== "(") return null;
  const destStart = labelEnd + 2;
  if (scan.destUnclosed.has(destStart)) {
    budget.tick();
    return null;
  }
  let depth = 1;
  let j = destStart;
  while (j < src.length && depth > 0) {
    const ch = src[j];
    if (ch === "\\" && j + 1 < src.length) {
      budget.tick(2);
      j += 2;
      continue;
    }
    budget.tick();
    if (ch === "(") depth += 1;
    else if (ch === ")") depth -= 1;
    j += 1;
  }
  if (depth !== 0) {
    scan.destUnclosed.add(destStart);
    return null;
  }
  const label = src.slice(labelStart + 1, labelEnd);
  const dest = src
    .slice(destStart, j - 1)
    .trim()
    .replace(/^<|>$/g, "")
    .split(/\s/)[0];
  if (dest === undefined) return null;
  if (image && isMxcUrl(dest)) {
    return {
      html: `<img src="${escapeAttr(dest)}" alt="${escapeAttr(label)}">`,
      end: j,
    };
  }
  const labelHtml = image ? escapeHtml(label) : renderInline(label, budget, newInlineScan());
  if (!isPermittedHref(dest))
    return { html: labelHtml.length > 0 ? labelHtml : escapeHtml(dest), end: j };
  const text = labelHtml.length > 0 ? labelHtml : escapeHtml(dest);
  return { html: `<a href="${escapeAttr(dest)}">${text}</a>`, end: j };
}

function parseAutolink(
  src: string,
  i: number,
  scan: InlineScan,
  budget: RenderBudget,
): { html: string; end: number } | null {
  budget.tick();
  if (src[i] !== "<") return null;
  const end = indexOfOrMiss(src, ">", i + 1, scan, budget);
  if (end === -1 || end - i > 2048) return null;
  const dest = src.slice(i + 1, end);
  for (let k = 0; k < dest.length; k += 1) {
    budget.tick();
    const ch = dest[k];
    if (ch === " " || ch === "<") return null;
  }
  if (!isPermittedHref(dest)) {
    budget.tick(Math.max(1, dest.length));
    return null;
  }
  return {
    html: `<a href="${escapeAttr(dest)}">${escapeHtml(dest)}</a>`,
    end: end + 1,
  };
}
