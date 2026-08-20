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
      i = skipDroppedElement(html, name, i);
      continue;
    }
    if (!ALLOWED_TAGS.has(name)) continue;
    if (stack.length >= MATRIX_HTML_MAX_NESTING) continue;

    const attrs = serializeAllowedAttrs(name, parsed.attrs);
    const emit = !tagMissingRequiredAttr(name, attrs);
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

function tagMissingRequiredAttr(tag: string, attrs: string): boolean {
  if (tag === "a") return !/\shref="/.test(attrs);
  if (tag === "img") return !/\ssrc="/.test(attrs);
  return false;
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

function renderMarkdownToHtml(markdown: string): string {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  return renderLines(lines, 0, lines.length, 0).html;
}

function renderLines(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  minIndent: number,
): { html: string; next: number } {
  let html = "";
  let i = from;
  while (i < to) {
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
      html += `<h${heading.level}>${renderInline(heading.text)}</h${heading.level}>`;
      i += 1;
      continue;
    }

    if (isHorizontalRule(line)) {
      html += "<hr>";
      i += 1;
      continue;
    }

    if (/^[ \t]*>/.test(line)) {
      const quote = parseBlockquote(lines, i, to);
      html += quote.html;
      i = quote.next;
      continue;
    }

    const list = listMarker(line);
    if (list !== null && list.indent >= minIndent) {
      const parsed = parseList(lines, i, to, list);
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
      html += `<p>${renderInline(line)}</p>`;
      i += 1;
      continue;
    }
    html += `<p>${renderInline(para.join("\n"))}</p>`;
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
    html: `<blockquote>${renderLines(inner, 0, inner.length, 0).html}</blockquote>`,
    next: i,
  };
}

function parseList(
  lines: ReadonlyArray<string>,
  from: number,
  to: number,
  first: ListMarker,
): { html: string; next: number } {
  const items: string[] = [];
  let i = from;
  let start: number | null = first.kind === "ol" ? first.start : null;
  while (i < to) {
    const line = lines[i] ?? "";
    if (line.trim() === "") {
      const next = peekNonEmpty(lines, i + 1, to);
      if (next === null) break;
      const nextMarker = listMarker(next);
      if (
        nextMarker !== null &&
        nextMarker.indent === first.indent &&
        nextMarker.kind === first.kind
      ) {
        i += 1;
        continue;
      }
      if (leadingIndent(next) > first.indent) {
        i += 1;
        continue;
      }
      break;
    }
    const marker = listMarker(line);
    if (marker === null || marker.indent < first.indent) break;
    if (marker.indent === first.indent && marker.kind !== first.kind) break;
    if (marker.indent === first.indent) {
      const item = collectListItem(lines, i, to, marker);
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
): { html: string; next: number } {
  const itemLines = [marker.text];
  let i = from + 1;
  while (i < to) {
    const line = lines[i] ?? "";
    if (line.trim() === "") {
      const next = peekNonEmpty(lines, i + 1, to);
      if (next === null) break;
      if (leadingIndent(next) >= marker.contentIndent) {
        itemLines.push("");
        i += 1;
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
  const first = renderInline(itemLines[0] ?? "");
  const html =
    itemLines.length === 1
      ? first
      : `${first}${renderLines(itemLines, 1, itemLines.length, 0).html}`;
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

function peekNonEmpty(lines: ReadonlyArray<string>, from: number, to: number): string | null {
  for (let i = from; i < to; i += 1) {
    const line = lines[i] ?? "";
    if (line.trim() !== "") return line;
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

interface InlineScan {
  readonly noCloseFrom: Map<string, number>;
}

function newInlineScan(): InlineScan {
  return { noCloseFrom: new Map() };
}

function indexOfOrMiss(
  src: string,
  needle: string,
  from: number,
  scan: InlineScan,
  key: string = needle,
): number {
  const missFrom = scan.noCloseFrom.get(key) ?? Number.POSITIVE_INFINITY;
  if (from >= missFrom) return -1;
  const found = src.indexOf(needle, from);
  if (found === -1) {
    const current = scan.noCloseFrom.get(key);
    if (current === undefined || from < current) scan.noCloseFrom.set(key, from);
    return -1;
  }
  return found;
}

function renderInline(src: string, scan: InlineScan = newInlineScan()): string {
  let out = "";
  let i = 0;
  while (i < src.length) {
    const ch = src[i] ?? "";
    if (ch === "_" && isWordChar(src[i - 1])) {
      let run = 1;
      while (src[i + run] === "_") run += 1;
      out += "_".repeat(run);
      i += run;
      continue;
    }
    if (ch === "\\" && i + 1 < src.length) {
      const next = src[i + 1] ?? "";
      if (isEscapableAsciiPunctuation(next)) {
        out += escapeHtml(next);
        i += 2;
        continue;
      }
      out += "\\";
      i += 1;
      continue;
    }
    if (ch === "`") {
      const span = parseCodeSpan(src, i, scan);
      if (span !== null) {
        out += `<code>${escapeHtml(span.inner)}</code>`;
        i = span.end;
        continue;
      }
      let run = 1;
      while (src[i + run] === "`") run += 1;
      out += escapeHtml("`".repeat(run));
      i += run;
      continue;
    }
    if (src.startsWith("***", i) || src.startsWith("___", i)) {
      const delim = src.slice(i, i + 3);
      if (canOpenRun(src, i, delim)) {
        const close = findClosing(src, i + 3, delim, scan);
        if (close !== -1) {
          out += `<strong><em>${renderInline(src.slice(i + 3, close))}</em></strong>`;
          i = close + 3;
          continue;
        }
      }
    }
    if (src.startsWith("**", i) || src.startsWith("__", i)) {
      const delim = src.slice(i, i + 2);
      if (canOpenRun(src, i, delim)) {
        const close = findClosing(src, i + 2, delim, scan);
        if (close !== -1) {
          out += `<strong>${renderInline(src.slice(i + 2, close))}</strong>`;
          i = close + 2;
          continue;
        }
      }
    }
    if (src.startsWith("~~", i)) {
      const close = findClosing(src, i + 2, "~~", scan);
      if (close !== -1) {
        out += `<del>${renderInline(src.slice(i + 2, close))}</del>`;
        i = close + 2;
        continue;
      }
    }
    if ((ch === "*" || ch === "_") && canOpenEm(src, i, ch)) {
      const close = findEmClose(src, i, ch, scan);
      if (close !== -1) {
        out += `<em>${renderInline(src.slice(i + 1, close))}</em>`;
        i = close + 1;
        continue;
      }
    }
    if (ch === "!" && src[i + 1] === "[") {
      const link = parseInlineLink(src, i, true, scan);
      if (link !== null) {
        out += link.html;
        i = link.end;
        continue;
      }
    }
    if (ch === "[") {
      const link = parseInlineLink(src, i, false, scan);
      if (link !== null) {
        out += link.html;
        i = link.end;
        continue;
      }
    }
    if (ch === "<") {
      const autolink = parseAutolink(src, i, scan);
      if (autolink !== null) {
        out += autolink.html;
        i = autolink.end;
        continue;
      }
      out += "&lt;";
      i += 1;
      continue;
    }
    if (ch === "\n") {
      out += "<br>";
      i += 1;
      continue;
    }
    out += escapeHtml(ch);
    i += 1;
  }
  return out;
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

function canOpenRun(src: string, i: number, delim: string): boolean {
  const after = src[i + delim.length];
  if (after === undefined || after === " " || after === "\n") return false;
  if (after === delim[0]) return false;
  if (delim.includes("_") && isWordChar(src[i - 1])) return false;
  return true;
}

function canCloseRun(src: string, close: number, delim: string): boolean {
  const before = src[close - 1];
  if (before === " " || before === "\n") return false;
  if (delim.includes("_") && isWordChar(src[close + delim.length])) return false;
  return true;
}

function canOpenEm(src: string, i: number, delim: string): boolean {
  return canOpenRun(src, i, delim);
}

function findEmClose(src: string, open: number, delim: string, scan: InlineScan): number {
  return findClosing(src, open + 1, delim, scan);
}

function parseCodeSpan(
  src: string,
  from: number,
  scan: InlineScan,
): { inner: string; end: number } | null {
  let n = 0;
  while (src[from + n] === "`") n += 1;
  if (n === 0) return null;
  const key = `code:${String(n)}`;
  if (from >= (scan.noCloseFrom.get(key) ?? Number.POSITIVE_INFINITY)) return null;
  let i = from + n;
  while (i < src.length) {
    if (src[i] !== "`") {
      i += 1;
      continue;
    }
    let m = 0;
    while (src[i + m] === "`") m += 1;
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

function findClosing(
  src: string,
  from: number,
  delim: string,
  scan: InlineScan,
  markMiss = true,
): number {
  if (from >= (scan.noCloseFrom.get(delim) ?? Number.POSITIVE_INFINITY)) return -1;
  const runChar = delim[0] ?? "";
  let i = from;
  while (i < src.length) {
    if (src[i] === "\\" && i + 1 < src.length) {
      i += 2;
      continue;
    }
    if (src[i] === "`") {
      const span = parseCodeSpan(src, i, scan);
      if (span === null) break;
      i = span.end;
      continue;
    }
    if (src.startsWith(delim, i)) {
      let run = 0;
      while (src[i + run] === runChar) run += 1;
      if (run > delim.length) {
        i += run;
        continue;
      }
      if (run === delim.length && canCloseRun(src, i, delim)) return i;
      if (run === delim.length && canOpenRun(src, i, delim)) {
        const nested = findClosing(src, i + delim.length, delim, scan, false);
        if (nested !== -1) {
          i = nested + delim.length;
          continue;
        }
      }
      i += Math.max(run, 1);
      continue;
    }
    i += 1;
  }
  if (markMiss) {
    const current = scan.noCloseFrom.get(delim);
    if (current === undefined || from < current) scan.noCloseFrom.set(delim, from);
  }
  return -1;
}

function parseInlineLink(
  src: string,
  i: number,
  image: boolean,
  scan: InlineScan,
): { html: string; end: number } | null {
  const labelStart = image ? i + 1 : i;
  if (src[labelStart] !== "[") return null;
  const labelEnd = indexOfOrMiss(src, "]", labelStart + 1, scan);
  if (labelEnd === -1 || src[labelEnd + 1] !== "(") return null;
  let depth = 1;
  let j = labelEnd + 2;
  while (j < src.length && depth > 0) {
    const ch = src[j];
    if (ch === "\\" && j + 1 < src.length) {
      j += 2;
      continue;
    }
    if (ch === "(") depth += 1;
    else if (ch === ")") depth -= 1;
    j += 1;
  }
  if (depth !== 0) return null;
  const label = src.slice(labelStart + 1, labelEnd);
  const dest = src
    .slice(labelEnd + 2, j - 1)
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
  const labelHtml = image ? escapeHtml(label) : renderInline(label);
  if (!isPermittedHref(dest))
    return { html: labelHtml.length > 0 ? labelHtml : escapeHtml(dest), end: j };
  const text = labelHtml.length > 0 ? labelHtml : escapeHtml(dest);
  return { html: `<a href="${escapeAttr(dest)}">${text}</a>`, end: j };
}

function parseAutolink(
  src: string,
  i: number,
  scan: InlineScan,
): { html: string; end: number } | null {
  if (src[i] !== "<") return null;
  const end = indexOfOrMiss(src, ">", i + 1, scan);
  if (end === -1 || end - i > 2048) return null;
  const dest = src.slice(i + 1, end);
  if (dest.includes(" ") || dest.includes("<")) return null;
  if (!isPermittedHref(dest)) return null;
  return {
    html: `<a href="${escapeAttr(dest)}">${escapeHtml(dest)}</a>`,
    end: end + 1,
  };
}
