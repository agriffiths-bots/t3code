---
name: t3-e2e-testing
description: Reproduce a bug report or verify a fix by driving the REAL T3 web UI with Playwright against a disposable (ephemeral) T3 server. Use for issue repro/verification from user-level descriptions ("clicking X does nothing", "thread list doesn't update"). Never touches the live server. For backend-only assertions (routing, scheduler, SQLite projections) see the t3-test-server skill.
---

# T3 end-to-end UI testing (Playwright + ephemeral server)

You drive the real web app in a real (headless) browser, following the steps a
user described — not a pre-baked script. Take screenshots as evidence at each
key step.

## 0. Build once (only if dist is stale/missing)

The ephemeral server runs `apps/server/dist/bin.mjs` and serves the web UI
from `apps/web/dist`. If you changed server or web code, rebuild first:

```bash
node_modules/.bin/vp run build:desktop   # rebuilds server bundle + web dist
```

## 1. Spin up a disposable server

Pick a unique name (e.g. the issue id) so parallel agents don't collide:

```bash
exports="$(.claude/skills/t3-test-server/scripts/t3-up.sh --name ISSUE-123)" \
  || { echo "t3-up failed"; exit 1; }   # progress/errors stay visible on stderr
eval "$exports"
echo "$T3_ORIGIN"   # e.g. http://127.0.0.1:13912
```

Any later shell can reload the same instance's env:

```bash
. ~/.cache/t3-ephemeral/instances/ISSUE-123/instance.env
```

## 2. Drive the UI with Playwright

`e2e/ui.mjs` handles browser launch + auth pairing (system Chrome, headless).
Write a small ESM script (or use `node --input-type=module -e '...'`):

```js
import { openT3 } from "./e2e/ui.mjs";

const { browser, page } = await openT3(); // reads T3_ORIGIN/T3_HOME from env
await page.waitForLoadState("networkidle");

// Follow the reported repro steps with normal Playwright:
//   await page.getByRole("button", { name: "New project" }).click();
//   await page.getByPlaceholder("Search").fill("foo");
//   await page.keyboard.press("Enter");
// Prefer getByRole/getByText/getByPlaceholder over CSS selectors.

await page.screenshot({ path: "/tmp/ISSUE-123-step1.png" }); // evidence
await browser.close();
```

Read screenshots with your image tool to SEE what a user sees. If an
interaction misbehaves, capture `page.on("console")` and
`page.on("pageerror")` output too.

## 3. Backend assertions (optional)

The instance env also carries `T3_TOKEN` (API bearer) and `T3_DB` (SQLite,
read-only). HTTP + projection helpers live in `e2e/` — see the
`t3-test-server` skill for `drive.mjs` / `assert.mjs` usage.

## 4. ALWAYS tear down

```bash
.claude/skills/t3-test-server/scripts/t3-down.sh ISSUE-123
```

If you may have leaked instances: `.claude/skills/t3-test-server/scripts/t3-down.sh --all`.

## Report format

State clearly: REPRODUCED / NOT REPRODUCED / FIXED / STILL BROKEN, the exact
steps you drove, what you observed (with screenshot paths), and any console or
server-log errors (`$T3_HOME/server.log`).

## Rules

- NEVER point Playwright or the API at the live server — only at `$T3_ORIGIN`
  from your own `t3-up.sh` instance.
- One instance per task, named after the task; tear it down even on failure.
- The browser is real: waits matter. Use `waitForLoadState` /
  `getByRole(...).waitFor()` instead of sleeps where possible.
