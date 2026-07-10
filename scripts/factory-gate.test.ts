// @effect-diagnostics nodeBuiltinImport:off - Shell-level Git hook integration test.
import { assert, describe, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const repoRoot = NodePath.dirname(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)));

function cleanGitEnvironment(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  for (const key of Object.keys(env)) {
    if (key.startsWith("GIT_")) {
      delete env[key];
    }
  }
  env.GIT_CONFIG_GLOBAL = "/dev/null";
  env.GIT_CONFIG_NOSYSTEM = "1";
  env.GIT_PAGER = "cat";
  env.GIT_TERMINAL_PROMPT = "0";
  return env;
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function run(
  cwd: string,
  args: ReadonlyArray<string>,
  options: { readonly env?: NodeJS.ProcessEnv; readonly expectFailure?: boolean } = {},
): NodeChildProcess.SpawnSyncReturns<string> {
  const result = NodeChildProcess.spawnSync(args[0]!, args.slice(1), {
    cwd,
    encoding: "utf8",
    env: { ...cleanGitEnvironment(), ...options.env },
  });

  if (!options.expectFailure && result.status !== 0) {
    assert.fail(
      `Command failed: ${args.join(" ")}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
    );
  }

  return result;
}

function copyRepoFile(root: string, relativePath: string): void {
  const destination = NodePath.join(root, relativePath);
  NodeFS.mkdirSync(NodePath.dirname(destination), { recursive: true });
  NodeFS.copyFileSync(NodePath.join(repoRoot, relativePath), destination);
  NodeFS.chmodSync(destination, 0o755);
}

interface FactoryFixtureOptions {
  readonly parallelStaticChecks?: boolean;
  readonly staticChecks?: ReadonlyArray<string>;
}

function installFactoryFixture(
  root: string,
  options: FactoryFixtureOptions = {},
): { readonly audit: string; readonly repo: string } {
  const repo = NodePath.join(root, "repo");
  const hooks = NodePath.join(root, "hooks");
  const autoreview = NodePath.join(root, "autoreview-stub.sh");
  const audit = NodePath.join(root, "audit", "factory-precommit.jsonl");

  NodeFS.mkdirSync(repo, { recursive: true });
  NodeFS.mkdirSync(hooks, { recursive: true });
  NodeFS.mkdirSync(NodePath.dirname(audit), { recursive: true });

  NodeFS.writeFileSync(
    autoreview,
    `#!/usr/bin/env bash
set -euo pipefail
out=""
extra_context=""
args=("$@")
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --json-output) out="$2"; shift 2 ;;
    --extra-context) extra_context="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[[ -z "\${FACTORY_TEST_REVIEW_ARGS_FILE:-}" ]] || printf '%s\n' "\${args[*]}" > "$FACTORY_TEST_REVIEW_ARGS_FILE"
[[ -z "\${FACTORY_TEST_EXTRA_CONTEXT_FILE:-}" || -z "$extra_context" ]] || cp "$extra_context" "$FACTORY_TEST_EXTRA_CONTEXT_FILE"
[[ -n "$out" ]] || { echo "missing --json-output" >&2; exit 2; }
cat > "$out" <<'JSON'
{"findings":[],"overall_correctness":"patch is correct","overall_explanation":"fixture clean review","overall_confidence":1}
JSON
`,
  );
  NodeFS.chmodSync(autoreview, 0o755);

  copyRepoFile(repo, "scripts/factory/precommit-gate.sh");
  copyRepoFile(repo, "scripts/factory/install-hooks.sh");
  copyRepoFile(repo, ".githooks/pre-commit");
  copyRepoFile(repo, ".githooks/pre-merge-commit");
  copyRepoFile(repo, ".githooks/prepare-commit-msg");

  const staticChecks = options.staticChecks ?? ["true"];
  const staticChecksConfig = staticChecks.map(shellQuote).join(" ");

  NodeFS.writeFileSync(
    NodePath.join(repo, "scripts/factory/factory.conf"),
    `FACTORY_STATIC_CHECKS=(${staticChecksConfig})
FACTORY_STATIC_CHECKS_PARALLEL=${options.parallelStaticChecks ? "1" : "0"}
FACTORY_AUTOREVIEW_BIN=${shellQuote(autoreview)}
FACTORY_REVIEW_ARGS=(--reviewers codex --model codex=gpt-5.6-sol --thinking codex=high)
FACTORY_UPSTREAM_REF="upstream/main"
FACTORY_AUDIT_LOG=${shellQuote(audit)}
`,
  );

  NodeFS.copyFileSync(
    NodePath.join(repo, ".githooks/pre-commit"),
    NodePath.join(hooks, "pre-commit"),
  );
  NodeFS.copyFileSync(
    NodePath.join(repo, ".githooks/pre-merge-commit"),
    NodePath.join(hooks, "pre-merge-commit"),
  );
  NodeFS.copyFileSync(
    NodePath.join(repo, ".githooks/prepare-commit-msg"),
    NodePath.join(hooks, "prepare-commit-msg"),
  );
  NodeFS.chmodSync(NodePath.join(hooks, "pre-commit"), 0o755);
  NodeFS.chmodSync(NodePath.join(hooks, "pre-merge-commit"), 0o755);
  NodeFS.chmodSync(NodePath.join(hooks, "prepare-commit-msg"), 0o755);

  run(repo, ["git", "init", "-q"]);
  run(repo, ["git", "config", "user.email", "factory-test@example.invalid"]);
  run(repo, ["git", "config", "user.name", "Factory Test"]);
  run(repo, ["git", "add", "-A"]);
  run(repo, ["git", "commit", "-qm", "bootstrap factory gate"]);
  run(repo, ["git", "config", "core.hooksPath", hooks]);

  return { audit, repo };
}

function createTrackedFiles(repo: string): void {
  NodeFS.writeFileSync(NodePath.join(repo, "a.txt"), "a\n");
  NodeFS.writeFileSync(NodePath.join(repo, "b.txt"), "b\n");
  run(repo, ["git", "add", "-A"]);
  run(repo, ["git", "commit", "-qm", "add tracked files"]);
}

function readAuditRecords(audit: string): ReadonlyArray<Record<string, unknown>> {
  return NodeFS.readFileSync(audit, "utf8")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function expectPartialTreeCommitRefused(args: ReadonlyArray<string>): void {
  const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
  try {
    const { audit, repo } = installFactoryFixture(root);
    createTrackedFiles(repo);

    NodeFS.writeFileSync(NodePath.join(repo, "a.txt"), "a changed\n");
    NodeFS.writeFileSync(NodePath.join(repo, "b.txt"), "b changed\n");
    run(repo, ["git", "add", "-A"]);

    const result = run(repo, ["git", "commit", "-qm", "partial tree", ...args], {
      expectFailure: true,
    });
    const output = `${result.stdout}\n${result.stderr}`;

    assert.notEqual(result.status, 0);
    assert.match(output, /COMMIT REFUSED/);
    assert.match(output, /working tree differs from the index/);
    assert.match(output, /b\.txt/);
    assert.match(NodeFS.readFileSync(audit, "utf8"), /"verdict":"scope-unstaged"/);
    assert.deepStrictEqual(
      run(repo, ["git", "diff", "--cached", "--name-only"]).stdout.trim(),
      ["a.txt", "b.txt"].join("\n"),
    );
  } finally {
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
}

describe("factory pre-commit gate", () => {
  it("passes no reviewer-memory context on the first branch round", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const { repo } = installFactoryFixture(root);
      const reviewArgs = NodePath.join(root, "review-args.txt");
      NodeFS.writeFileSync(NodePath.join(repo, "first-round.txt"), "first round\n");
      run(repo, ["git", "add", "-A"]);

      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: { FACTORY_TEST_REVIEW_ARGS_FILE: reviewArgs },
      });

      assert.ok(!/--extra-context/.test(NodeFS.readFileSync(reviewArgs, "utf8")));
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("passes prior branch findings to the second review round as settled context", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const { audit, repo } = installFactoryFixture(root);
      const reviewArgs = NodePath.join(root, "review-args.txt");
      const extraContext = NodePath.join(root, "extra-context.txt");
      const branch = run(repo, ["git", "branch", "--show-current"]).stdout.trim();
      NodeFS.appendFileSync(
        audit,
        `${JSON.stringify({
          ts: "2026-07-10T08:00:00Z",
          kind: "factory_precommit",
          repo,
          branch,
          verdict: "review-findings",
          findings: [{ title: "First round bug", priority: "P1", file: "src/bug.ts", line: 42 }],
        })}\n`,
      );
      NodeFS.writeFileSync(NodePath.join(repo, "second-round.txt"), "second round\n");
      run(repo, ["git", "add", "-A"]);

      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: {
          FACTORY_TEST_REVIEW_ARGS_FILE: reviewArgs,
          FACTORY_TEST_EXTRA_CONTEXT_FILE: extraContext,
        },
      });

      assert.match(NodeFS.readFileSync(reviewArgs, "utf8"), /--extra-context/);
      const ledger = NodeFS.readFileSync(extraContext, "utf8");
      assert.match(
        ledger,
        /\[P1\] First round bug \(src\/bug\.ts:42\) -> addressed in a subsequent commit/,
      );
      assert.match(ledger, /reworded equivalents as SETTLED/);
      assert.match(ledger, /finding NEW, previously unreported issues/);
      assert.match(ledger, /not that the code is safe/);
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("includes audited dismissals in reviewer memory with settled framing", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const { audit, repo } = installFactoryFixture(root);
      const extraContext = NodePath.join(root, "extra-context.txt");
      const branch = run(repo, ["git", "branch", "--show-current"]).stdout.trim();
      NodeFS.appendFileSync(
        audit,
        `${JSON.stringify({
          ts: "2026-07-10T08:00:00Z",
          kind: "factory_precommit",
          repo,
          branch,
          verdict: "pass-with-dismissals",
          dismissals: [{ title: "Intentional design choice", file: "src/design.ts", line: 17 }],
        })}\n`,
      );
      NodeFS.writeFileSync(NodePath.join(repo, "after-dismissal.txt"), "after dismissal\n");
      run(repo, ["git", "add", "-A"]);

      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: { FACTORY_TEST_EXTRA_CONTEXT_FILE: extraContext },
      });

      assert.match(
        NodeFS.readFileSync(extraContext, "utf8"),
        /Intentional design choice \(src\/design\.ts:17\) -> DISMISSED with audited rationale \(settled design decision\)/,
      );
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("refuses pathspec commits that would exclude staged files from the commit tree", () => {
    expectPartialTreeCommitRefused(["a.txt"]);
  });

  it("refuses --only commits that would exclude staged files from the commit tree", () => {
    expectPartialTreeCommitRefused(["--only", "a.txt"]);
  });

  it("records phase timings and static check results in audit records", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const { audit, repo } = installFactoryFixture(root, {
        staticChecks: ["true", "true"],
      });
      NodeFS.writeFileSync(NodePath.join(repo, "timed.txt"), "timed\n");
      run(repo, ["git", "add", "-A"]);

      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"]);

      const records = readAuditRecords(audit);
      const pass = records.at(-1);
      assert.equal(pass?.verdict, "pass");
      assert.equal(
        typeof (pass?.timings as { readonly total_secs?: unknown })?.total_secs,
        "number",
      );
      assert.equal(
        typeof (pass?.timings as { readonly scope_secs?: unknown })?.scope_secs,
        "number",
      );
      assert.equal(
        typeof (pass?.timings as { readonly static_secs?: unknown })?.static_secs,
        "number",
      );
      assert.equal(
        typeof (pass?.timings as { readonly review_secs?: unknown })?.review_secs,
        "number",
      );
      assert.deepStrictEqual(
        (
          pass?.timings as { readonly static_checks?: ReadonlyArray<{ readonly cmd: string }> }
        )?.static_checks?.map((check) => check.cmd),
        ["true", "true"],
      );
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("runs static checks in parallel when configured", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const firstReady = NodePath.join(root, "first.ready");
      const secondReady = NodePath.join(root, "second.ready");
      const waitForPeer = (own: string, peer: string) =>
        `touch ${shellQuote(own)}; for i in $(seq 1 200); do [ -f ${shellQuote(peer)} ] && exit 0; sleep 0.01; done; exit 17`;
      const { audit, repo } = installFactoryFixture(root, {
        parallelStaticChecks: true,
        staticChecks: [waitForPeer(firstReady, secondReady), waitForPeer(secondReady, firstReady)],
      });
      NodeFS.writeFileSync(NodePath.join(repo, "parallel.txt"), "parallel\n");
      run(repo, ["git", "add", "-A"]);

      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"]);

      const pass = readAuditRecords(audit).at(-1);
      assert.equal(pass?.verdict, "pass");
      assert.deepStrictEqual(
        (
          pass?.timings as { readonly static_checks?: ReadonlyArray<{ readonly rc: number }> }
        )?.static_checks?.map((check) => check.rc),
        [0, 0],
      );
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("falls back when date does not support nanosecond formatting", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const fakeBin = NodePath.join(root, "fake-bin");
      NodeFS.mkdirSync(fakeBin, { recursive: true });
      const realDate = NodeChildProcess.spawnSync("bash", ["-lc", "command -v date"], {
        encoding: "utf8",
        env: cleanGitEnvironment(),
      }).stdout.trim();
      assert.notEqual(realDate, "");
      NodeFS.writeFileSync(
        NodePath.join(fakeBin, "date"),
        `#!/usr/bin/env bash
if [[ "\${1:-}" == "+%s%N" ]]; then
  echo "1760000000N"
  exit 0
fi
exec ${shellQuote(realDate)} "$@"
`,
      );
      NodeFS.chmodSync(NodePath.join(fakeBin, "date"), 0o755);

      const { audit, repo } = installFactoryFixture(root);
      NodeFS.writeFileSync(NodePath.join(repo, "date-fallback.txt"), "date fallback\n");
      run(repo, ["git", "add", "-A"]);

      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: { PATH: `${fakeBin}:${process.env.PATH ?? ""}` },
      });

      const pass = readAuditRecords(audit).at(-1);
      assert.equal(pass?.verdict, "pass");
      assert.equal(
        typeof (pass?.timings as { readonly total_secs?: unknown })?.total_secs,
        "number",
      );
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("uses and audits a valid reviewer override", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const { audit, repo } = installFactoryFixture(root);
      const override = NodePath.join(root, ".openclaw", "factory-reviewer-override.conf");
      const reviewArgs = NodePath.join(root, "review-args.txt");
      NodeFS.mkdirSync(NodePath.dirname(override), { recursive: true });
      NodeFS.writeFileSync(
        override,
        "--reviewers claude --model claude=opus-4.8 --thinking claude=high\n",
      );
      NodeFS.writeFileSync(NodePath.join(repo, "override.txt"), "override\n");
      run(repo, ["git", "add", "-A"]);

      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: { HOME: root, FACTORY_TEST_REVIEW_ARGS_FILE: reviewArgs },
      });

      assert.match(
        NodeFS.readFileSync(reviewArgs, "utf8"),
        /--reviewers claude --model claude=opus-4\.8 --thinking claude=high/,
      );
      assert.ok(
        readAuditRecords(audit).some(
          (record) =>
            record.kind === "reviewer-override" &&
            record.args === "--reviewers claude --model claude=opus-4.8 --thinking claude=high",
        ),
      );
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("invalidates a cached pass when the reviewer override changes", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const { audit, repo } = installFactoryFixture(root);
      const override = NodePath.join(root, ".openclaw", "factory-reviewer-override.conf");
      const reviewArgs = NodePath.join(root, "review-args.txt");
      NodeFS.writeFileSync(NodePath.join(repo, "cachekey.txt"), "cachekey\n");
      run(repo, ["git", "add", "-A"]);

      // First run: default reviewer, PASS gets cached for this staged tree.
      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: { HOME: root, FACTORY_TEST_REVIEW_ARGS_FILE: reviewArgs },
      });
      assert.ok(!/--reviewers claude/.test(NodeFS.readFileSync(reviewArgs, "utf8")));

      // Adding the override must invalidate the cached pass: the same staged
      // tree re-reviews under the overridden reviewer instead of reusing PASS.
      NodeFS.mkdirSync(NodePath.dirname(override), { recursive: true });
      NodeFS.writeFileSync(
        override,
        "--reviewers claude --model claude=opus-4.8 --thinking claude=high\n",
      );
      run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: { HOME: root, FACTORY_TEST_REVIEW_ARGS_FILE: reviewArgs },
      });
      assert.match(
        NodeFS.readFileSync(reviewArgs, "utf8"),
        /--reviewers claude --model claude=opus-4\.8 --thinking claude=high/,
      );
      assert.ok(
        readAuditRecords(audit).some(
          (record) => record.kind === "reviewer-override" && record.status === "used",
        ),
      );
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects a malformed reviewer override and uses the pinned default", () => {
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-factory-gate-"));
    try {
      const { audit, repo } = installFactoryFixture(root);
      const override = NodePath.join(root, ".openclaw", "factory-reviewer-override.conf");
      const reviewArgs = NodePath.join(root, "review-args.txt");
      NodeFS.mkdirSync(NodePath.dirname(override), { recursive: true });
      NodeFS.writeFileSync(
        override,
        "--reviewers claude --mode branch --model claude=opus-4.8 --thinking claude=high\n",
      );
      NodeFS.writeFileSync(NodePath.join(repo, "malformed.txt"), "malformed\n");
      run(repo, ["git", "add", "-A"]);

      const result = run(repo, ["scripts/factory/precommit-gate.sh", "--prepare"], {
        env: { HOME: root, FACTORY_TEST_REVIEW_ARGS_FILE: reviewArgs },
      });

      assert.match(result.stderr, /reviewer override rejected/);
      assert.match(
        NodeFS.readFileSync(reviewArgs, "utf8"),
        /--reviewers codex --model codex=gpt-5\.6-sol --thinking codex=high/,
      );
      assert.ok(
        readAuditRecords(audit).some(
          (record) => record.kind === "reviewer-override" && record.status === "rejected",
        ),
      );
    } finally {
      NodeFS.rmSync(root, { recursive: true, force: true });
    }
  });
});
