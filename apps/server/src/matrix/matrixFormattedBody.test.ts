import { describe, expect, it } from "@effect/vitest";

import {
  MATRIX_FORMATTED_BODY_MAX_BYTES,
  MATRIX_FORMATTED_CONTENT_MAX_JSON_BYTES,
  MATRIX_HTML_FORMAT,
  isPermittedHref,
  matrixTextContent,
  sanitizeMatrixHtml,
} from "./matrixFormattedBody.ts";

const formatted = (body: string) => {
  const content = matrixTextContent(body);
  expect(content.body).toBe(body);
  expect(content.msgtype).toBe("m.text");
  if (!("formatted_body" in content) || content.formatted_body === undefined) {
    throw new Error(`expected formatted_body for ${JSON.stringify(body)}`);
  }
  expect(content.format).toBe(MATRIX_HTML_FORMAT);
  return content.formatted_body;
};

describe("matrixTextContent rendering", () => {
  it("keeps the original markdown on body", () => {
    const markdown = "**bold** and `code`";
    const content = matrixTextContent(markdown);
    expect(content).toEqual({
      msgtype: "m.text",
      body: markdown,
      format: MATRIX_HTML_FORMAT,
      formatted_body: "<p><strong>bold</strong> and <code>code</code></p>",
    });
  });

  it("renders headings h1-h6", () => {
    expect(formatted("# Title")).toBe("<h1>Title</h1>");
    expect(formatted("## Title")).toBe("<h2>Title</h2>");
    expect(formatted("### Title")).toBe("<h3>Title</h3>");
    expect(formatted("#### Title")).toBe("<h4>Title</h4>");
    expect(formatted("##### Title")).toBe("<h5>Title</h5>");
    expect(formatted("###### Title")).toBe("<h6>Title</h6>");
  });

  it("renders empty ATX headings instead of stalling", () => {
    expect(formatted("# ")).toBe("<h1></h1>");
    expect(formatted("##")).toBe("<h2></h2>");
    expect(formatted("# \ntext")).toBe("<h1></h1><p>text</p>");
  });

  it("renders paragraphs, line breaks, and horizontal rules", () => {
    expect(formatted("hello\nworld")).toBe("<p>hello<br>world</p>");
    expect(formatted("one\n\ntwo")).toBe("<p>one</p><p>two</p>");
    expect(formatted("---")).toBe("<hr>");
  });

  it("renders emphasis, strong, and strikethrough", () => {
    expect(formatted("*em* and _also_")).toBe("<p><em>em</em> and <em>also</em></p>");
    expect(formatted("**strong** and __also__")).toBe(
      "<p><strong>strong</strong> and <strong>also</strong></p>",
    );
    expect(formatted("~~gone~~")).toBe("<p><del>gone</del></p>");
    expect(formatted("***both***")).toBe("<p><strong><em>both</em></strong></p>");
    expect(formatted("*outer *inner* outer*")).toBe("<p><em>outer <em>inner</em> outer</em></p>");
  });

  it("does not treat underscores inside identifiers as emphasis", () => {
    expect(formatted("foo_bar_baz")).toBe("<p>foo_bar_baz</p>");
    expect(formatted("foo__bar__baz")).toBe("<p>foo__bar__baz</p>");
  });

  it("keeps literal backslashes that are not markdown escapes", () => {
    expect(formatted(String.raw`C:\Users\Adam`)).toBe("<p>C:\\Users\\Adam</p>");
    expect(formatted(String.raw`\d+\s`)).toBe("<p>\\d+\\s</p>");
    expect(formatted(String.raw`\*not em\*`)).toBe("<p>*not em*</p>");
  });

  it("renders inline code and fenced code with a language class", () => {
    expect(formatted("use `x < y`")).toBe("<p>use <code>x &lt; y</code></p>");
    expect(formatted("``**literal**``")).toBe("<p><code>**literal**</code></p>");
    expect(formatted("`` a ` b ``")).toBe("<p><code>a ` b</code></p>");
    expect(formatted("```ts\nconst x = 1;\n```")).toBe(
      '<pre><code class="language-ts">const x = 1;</code></pre>',
    );
    expect(formatted("```\nplain\n```")).toBe("<pre><code>plain</code></pre>");
    expect(formatted("```\n ```not-a-close\nstill code\n```")).toBe(
      "<pre><code> ```not-a-close\nstill code</code></pre>",
    );
  });

  it("does not interpret markdown inside fenced code", () => {
    expect(formatted("```\n**not bold**\n<script>x</script>\n```")).toBe(
      "<pre><code>**not bold**\n&lt;script&gt;x&lt;/script&gt;</code></pre>",
    );
  });

  it("renders unordered and ordered lists", () => {
    expect(formatted("- a\n- b")).toBe("<ul><li>a</li><li>b</li></ul>");
    expect(formatted("* a\n* b")).toBe("<ul><li>a</li><li>b</li></ul>");
    expect(formatted("1. a\n2. b")).toBe("<ol><li>a</li><li>b</li></ol>");
    expect(formatted("3. a\n4. b")).toBe('<ol start="3"><li>a</li><li>b</li></ol>');
  });

  it("renders nested lists inside list items", () => {
    expect(formatted("- a\n  - b\n  - c\n- d")).toBe(
      "<ul><li>a<ul><li>b</li><li>c</li></ul></li><li>d</li></ul>",
    );
  });

  it("renders blockquotes", () => {
    expect(formatted("> quoted **ok**")).toBe(
      "<blockquote><p>quoted <strong>ok</strong></p></blockquote>",
    );
    expect(formatted("> a\n> b")).toBe("<blockquote><p>a<br>b</p></blockquote>");
  });

  it("renders links with permitted schemes and drops unsafe ones", () => {
    expect(formatted("[ok](https://example.com/path)")).toBe(
      '<p><a href="https://example.com/path">ok</a></p>',
    );
    expect(formatted("[mail](mailto:a@b.com)")).toBe('<p><a href="mailto:a@b.com">mail</a></p>');
    expect(formatted("[bad](javascript:alert(1))")).toBe("<p>bad</p>");
    expect(formatted("[bad](data:text/html,hi)")).toBe("<p>bad</p>");
    expect(formatted("[bad](vbscript:alert(1))")).toBe("<p>bad</p>");
    expect(formatted("[rel](/relative)")).toBe("<p>rel</p>");
  });

  it("renders markdown images as links unless the src is mxc", () => {
    expect(formatted("![alt](https://example.com/a.png)")).toBe(
      '<p><a href="https://example.com/a.png">alt</a></p>',
    );
    expect(formatted("![](https://example.com/a.png)")).toBe(
      '<p><a href="https://example.com/a.png">https://example.com/a.png</a></p>',
    );
    expect(formatted("[](https://example.com)")).toBe(
      '<p><a href="https://example.com">https://example.com</a></p>',
    );
    expect(formatted("![alt](mxc://server/media)")).toBe(
      '<p><img src="mxc://server/media" alt="alt"></p>',
    );
  });

  it("escapes raw HTML from the model rather than passing it through", () => {
    expect(formatted("<script>alert(1)</script>")).toBe(
      "<p>&lt;script&gt;alert(1)&lt;/script&gt;</p>",
    );
    expect(formatted('click <img src=x onerror="alert(1)">')).toBe(
      '<p>click &lt;img src=x onerror="alert(1)"&gt;</p>',
    );
  });

  it("renders autolinks", () => {
    expect(formatted("<https://example.com>")).toBe(
      '<p><a href="https://example.com">https://example.com</a></p>',
    );
  });
});

describe("sanitizeMatrixHtml", () => {
  it("strips script, style, iframe, and their contents", () => {
    expect(sanitizeMatrixHtml("<p>ok</p><script>alert(1)</script><p>still</p>")).toBe(
      "<p>ok</p><p>still</p>",
    );
    expect(sanitizeMatrixHtml("<style>body{display:none}</style><p>x</p>")).toBe("<p>x</p>");
    expect(sanitizeMatrixHtml('<iframe src="https://evil.test"></iframe><p>x</p>')).toBe(
      "<p>x</p>",
    );
  });

  it("strips event handlers and unknown attributes", () => {
    expect(sanitizeMatrixHtml('<p onclick="alert(1)" style="color:red">x</p>')).toBe("<p>x</p>");
    expect(sanitizeMatrixHtml('<a href="https://ok.example" onclick="alert(1)">x</a>')).toBe(
      '<a href="https://ok.example">x</a>',
    );
  });

  it("strips javascript, data, and relative URLs after entity decoding", () => {
    expect(sanitizeMatrixHtml('<a href="javascript:alert(1)">x</a>')).toBe("x");
    expect(sanitizeMatrixHtml('<a href="JAVASCRIPT:alert(1)">x</a>')).toBe("x");
    expect(sanitizeMatrixHtml('<a href="data:text/html;base64,PHNjcmlwdD4=">x</a>')).toBe("x");
    expect(sanitizeMatrixHtml('<a href="&#106;avascript:alert(1)">x</a>')).toBe("x");
    expect(sanitizeMatrixHtml('<a href="/relative">x</a>')).toBe("x");
    expect(sanitizeMatrixHtml('<img src="https://example.com/a.png">')).toBe("");
    expect(sanitizeMatrixHtml('<img src="javascript:alert(1)">')).toBe("");
  });

  it("keeps only language- classes on code and mxc src on img", () => {
    expect(sanitizeMatrixHtml('<code class="language-ts">x</code>')).toBe(
      '<code class="language-ts">x</code>',
    );
    expect(sanitizeMatrixHtml('<code class="language-ts extra">x</code>')).toBe("<code>x</code>");
    expect(sanitizeMatrixHtml('<img src="mxc://server/id" alt="a">')).toBe(
      '<img src="mxc://server/id" alt="a">',
    );
  });

  it("drops comments and unknown tags while keeping inner text", () => {
    expect(sanitizeMatrixHtml("<!--secret--><p>x</p>")).toBe("<p>x</p>");
    expect(sanitizeMatrixHtml("<foo><strong>y</strong></foo>")).toBe("<strong>y</strong>");
  });
});

describe("matrixTextContent fallback", () => {
  it("falls back to plain body when formatted_body exceeds the cap", () => {
    const markdown = `**${"a".repeat(MATRIX_FORMATTED_BODY_MAX_BYTES)}**`;
    expect(matrixTextContent(markdown)).toEqual({ msgtype: "m.text", body: markdown });
  });

  it("falls back to plain body when the JSON content would exceed the event budget", () => {
    const markdown = "a".repeat(MATRIX_FORMATTED_CONTENT_MAX_JSON_BYTES);
    expect(matrixTextContent(markdown)).toEqual({ msgtype: "m.text", body: markdown });
  });

  it("falls back to plain body when JSON escaping would blow the encrypted-event budget", () => {
    const markdown = '"'.repeat(Math.floor(MATRIX_FORMATTED_CONTENT_MAX_JSON_BYTES / 2));
    expect(matrixTextContent(markdown)).toEqual({ msgtype: "m.text", body: markdown });
  });

  it("parses unmatched delimiters without hanging", () => {
    const markdown = `[${"<".repeat(8_000)}${"*".repeat(8_000)}`;
    const content = matrixTextContent(markdown);
    expect(content.body).toBe(markdown);
    expect(content.msgtype).toBe("m.text");
  });

  it("still formats a pairing-sized plain string", () => {
    const body = "Pairing complete. T3 bridging is active when a thread is selected.";
    expect(matrixTextContent(body)).toEqual({
      msgtype: "m.text",
      body,
      format: MATRIX_HTML_FORMAT,
      formatted_body: `<p>${body}</p>`,
    });
  });
});

describe("isPermittedHref", () => {
  it("accepts the spec schemes and rejects the rest", () => {
    expect(isPermittedHref("https://example.com")).toBe(true);
    expect(isPermittedHref("http://example.com")).toBe(true);
    expect(isPermittedHref("ftp://example.com/file")).toBe(true);
    expect(isPermittedHref("mailto:a@b.com")).toBe(true);
    expect(isPermittedHref("magnet:?xt=urn:btih:abc")).toBe(true);
    expect(isPermittedHref("javascript:alert(1)")).toBe(false);
    expect(isPermittedHref("data:text/plain,hi")).toBe(false);
    expect(isPermittedHref("file:///etc/passwd")).toBe(false);
    expect(isPermittedHref("//example.com")).toBe(false);
  });
});
