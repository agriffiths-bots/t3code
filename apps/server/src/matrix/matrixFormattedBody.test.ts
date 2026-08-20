import { describe, expect, it } from "@effect/vitest";

import {
  MATRIX_FORMATTED_BODY_MAX_BYTES,
  MATRIX_FORMATTED_CONTENT_MAX_JSON_BYTES,
  MATRIX_HTML_FORMAT,
  MATRIX_HTML_MAX_NESTING,
  MATRIX_MAX_BLOCKQUOTE_DEPTH,
  isPermittedHref,
  matrixRenderStats,
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
    expect(formatted("***both***")).toBe("<p><em><strong>both</strong></em></p>");
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
    expect(formatted("- a\n\n- b")).toBe("<ul><li>a</li><li>b</li></ul>");
    expect(formatted("- a\n\n\n- b")).toBe("<ul><li>a</li><li>b</li></ul>");
  });

  it("still formats a list after a long run of blank lines", () => {
    expect(formatted(`- a\n${"\n".repeat(200)}- b`)).toBe("<ul><li>a</li><li>b</li></ul>");
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

  it("caps nested blockquote depth and keeps the inner text", () => {
    const html = formatted(`${">".repeat(40)} inner`);
    expect(html.match(/<blockquote>/g)?.length).toBe(MATRIX_MAX_BLOCKQUOTE_DEPTH);
    expect(html.match(/<\/blockquote>/g)?.length).toBe(MATRIX_MAX_BLOCKQUOTE_DEPTH);
    expect(html.includes("inner")).toBe(true);
  });

  it("charges leftover quote markers at the depth cap instead of recursing", () => {
    const markdown = `${">".repeat(4_000)} x`;
    const stats = matrixRenderStats(markdown);
    expect(stats.exceeded).toBe(false);
    expect(stats.ops).toBeGreaterThan(4_000);
    expect(stats.ops).toBeLessThanOrEqual(stats.maxOps);
    const content = matrixTextContent(markdown);
    expect(content.body).toBe(markdown);
    expect(content.msgtype).toBe("m.text");
    if (!("formatted_body" in content) || content.formatted_body === undefined) {
      throw new Error("expected formatted_body for capped quote markers");
    }
    expect(content.formatted_body.match(/<blockquote>/g)?.length).toBe(MATRIX_MAX_BLOCKQUOTE_DEPTH);
    expect(content.formatted_body.includes("x")).toBe(true);
  });

  it("keeps literal quote markers inside a fenced block at the quote cap", () => {
    const prefix = ">".repeat(MATRIX_MAX_BLOCKQUOTE_DEPTH);
    expect(formatted(`${prefix} \`\`\`\n${prefix} >kept\n${prefix} \`\`\``)).toContain("&gt;kept");
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

  it("leaves GFM tables as a paragraph of pipe rows (follow-up, not this PR)", () => {
    expect(formatted("| a | b |\n| --- | --- |\n| 1 | 2 |")).toBe(
      "<p>| a | b |<br>| --- | --- |<br>| 1 | 2 |</p>",
    );
  });

  it("leaves setext headings as paragraph text (follow-up, not this PR)", () => {
    expect(formatted("Title\n=====")).toBe("<p>Title<br>=====</p>");
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

  it("does not let void dropped tags consume the rest of the document", () => {
    expect(sanitizeMatrixHtml('<p>a</p><input type="hidden"><p>b</p>')).toBe("<p>a</p><p>b</p>");
    expect(sanitizeMatrixHtml('<p>a</p><link rel="stylesheet"><p>b</p>')).toBe("<p>a</p><p>b</p>");
    expect(sanitizeMatrixHtml('<p>a</p><meta charset="utf-8"><p>b</p>')).toBe("<p>a</p><p>b</p>");
    expect(sanitizeMatrixHtml('<p>a</p><embed src="x"><p>b</p>')).toBe("<p>a</p><p>b</p>");
  });

  it("still drops script contents through to the matching close tag", () => {
    expect(sanitizeMatrixHtml("<p>a</p><script>evil</script><p>b</p>")).toBe("<p>a</p><p>b</p>");
  });

  it("still drops contents after a self-closing non-void dropped tag", () => {
    expect(
      sanitizeMatrixHtml('<script/><img src="mxc://server/id" alt="a"></script><p>ok</p>'),
    ).toBe("<p>ok</p>");
    expect(sanitizeMatrixHtml("<style/>hidden</style><p>ok</p>")).toBe("<p>ok</p>");
    expect(sanitizeMatrixHtml("<iframe/><p>x</p></iframe><p>ok</p>")).toBe("<p>ok</p>");
  });

  it("drops img/a tags whose required attr is only a substring of another value", () => {
    expect(sanitizeMatrixHtml('<img alt=" src=">')).toBe("");
    expect(sanitizeMatrixHtml('<img alt=" src=" src="javascript:alert(1)">')).toBe("");
    expect(sanitizeMatrixHtml('<a title=" href=" href="javascript:alert(1)">x</a>')).toBe("x");
  });

  it("does not let a skipped overflow open tag close an outer ancestor", () => {
    const depth = MATRIX_HTML_MAX_NESTING + 2;
    const html = `${"<blockquote>".repeat(depth)}keep${"</blockquote>".repeat(depth)}`;
    const sanitized = sanitizeMatrixHtml(html);
    expect(sanitized.startsWith("<blockquote>")).toBe(true);
    expect(sanitized.endsWith("</blockquote>")).toBe(true);
    expect(sanitized.includes("keep")).toBe(true);
    expect(sanitized.match(/<blockquote>/g)?.length).toBe(MATRIX_HTML_MAX_NESTING);
    expect(sanitized.match(/<\/blockquote>/g)?.length).toBe(MATRIX_HTML_MAX_NESTING);
    expect(sanitized).toBe(
      `${"<blockquote>".repeat(MATRIX_HTML_MAX_NESTING)}keep${"</blockquote>".repeat(MATRIX_HTML_MAX_NESTING)}`,
    );
  });

  it("unmatched close tags stay linear against a bounded stack", () => {
    const opens = 5_000;
    const html = `${"<p>".repeat(opens)}keep${"</b>".repeat(opens)}`;
    const sanitized = sanitizeMatrixHtml(html);
    expect(sanitized).toBe(
      `${"<p>".repeat(MATRIX_HTML_MAX_NESTING)}keep${"</p>".repeat(MATRIX_HTML_MAX_NESTING)}`,
    );
  });

  it("does not let overflow closes steal a later emitted tag of the same name", () => {
    const html = `${"<blockquote>".repeat(MATRIX_HTML_MAX_NESTING)}<b></blockquote><b>x</b>y${"</blockquote>".repeat(MATRIX_HTML_MAX_NESTING - 1)}`;
    expect(sanitizeMatrixHtml(html)).toBe(
      `${"<blockquote>".repeat(MATRIX_HTML_MAX_NESTING)}</blockquote><b>x</b>y${"</blockquote>".repeat(MATRIX_HTML_MAX_NESTING - 1)}`,
    );
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

  it("still formats a short malformed link instead of throwing", () => {
    const markdown = `${"[".repeat(20)}](${"(".repeat(20)}`;
    const content = matrixTextContent(markdown);
    expect(content.body).toBe(markdown);
    expect(content.msgtype).toBe("m.text");
    expect("formatted_body" in content).toBe(true);
  });

  it("finishes adversarial inline constructs well under a hard time budget", () => {
    matrixTextContent("warmup **ok** `[link](https://example.com)`");
    let unmatchedFenceLengths = "";
    for (let n = 1; n <= 180; n += 1) unmatchedFenceLengths += `${"`".repeat(n)}x`;
    const cases: Array<{ name: string; markdown: string }> = [
      { name: "unmatched *a runs", markdown: `${"*a ".repeat(400)}z` },
      { name: "long * run", markdown: `${"*".repeat(2_000)}z` },
      { name: "interleaved * and _", markdown: `${"*_".repeat(1_000)}z` },
      {
        name: "nested-looking openers then closers",
        markdown: `${"*a ".repeat(2_000)}x${" b*".repeat(2_000)}`,
      },
      { name: "unmatched mixed runs", markdown: `${"***a _".repeat(500)}z` },
      {
        name: "malformed links with unclosed destinations",
        markdown: `${"[".repeat(12_000)}](${"(".repeat(12_000)}`,
      },
      {
        name: "many distinct unclosed link destinations",
        markdown: "[x](".repeat(4_000),
      },
      { name: "deeply nested brackets", markdown: `${"[".repeat(12_000)}${"]".repeat(12_000)}` },
      { name: "malformed images", markdown: `${"![".repeat(6_000)}](${"(".repeat(12_000)}` },
      { name: "long unmatched code-span run", markdown: `${"`".repeat(12_000)}x` },
      { name: "unmatched code-span fence lengths", markdown: unmatchedFenceLengths },
      { name: "autolink opener run", markdown: `${"<".repeat(12_000)}>` },
      {
        name: "nested emphasis inside a link label",
        markdown: `[${"*a ".repeat(2_000)}x${" b*".repeat(2_000)}](https://example.com)`,
      },
      { name: "reference-style lookalikes", markdown: "[x][y]".repeat(4_000) },
      { name: "blank lines between list items", markdown: `- a\n${"\n".repeat(20_000)}- b` },
      {
        name: "mixed pathological inlines",
        markdown: `${"*a [`<".repeat(3_000)}z`,
      },
    ];
    // Deterministic bound is the renderer's own op counter (linear in input
    // length). Exceeding the budget and falling back to plaintext is the
    // designed safety valve, not a failure. Wall-clock is only a generous
    // smoke ceiling so a CI runner cannot flake the way a 50ms budget did
    // after the temp-dir teardown flakes in #263. The last tick may overshoot
    // maxOps by one scan, so allow one extra input-length of ops.
    const smokeMs = 2_000;
    for (const { name, markdown } of cases) {
      const started = performance.now();
      const stats = matrixRenderStats(markdown);
      const content = matrixTextContent(markdown);
      const elapsed = performance.now() - started;
      expect(stats.ops, `${name} ops ${stats.ops}/${stats.maxOps}`).toBeLessThanOrEqual(
        stats.maxOps + markdown.length,
      );
      expect(elapsed, `${name} took ${elapsed.toFixed(2)}ms`).toBeLessThan(smokeMs);
      expect(content.body).toBe(markdown);
      expect(content.msgtype).toBe("m.text");
    }
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
