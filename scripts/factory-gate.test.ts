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
  options: { readonly expectFailure?: boolean } = {},
): NodeChildProcess.SpawnSyncReturns<string> {
  const result = NodeChildProcess.spawnSync(args[0]!, args.slice(1), {
    cwd,
    encoding: "utf8",
    env: cleanGitEnvironment(),
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

function installFactoryFixture(root: string): { readonly audit: string; readonly repo: string } {
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
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --json-output) out="$2"; shift 2 ;;
    *) shift ;;
  esac
done
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

  NodeFS.writeFileSync(
    NodePath.join(repo, "scripts/factory/factory.conf"),
    `FACTORY_STATIC_CHECKS=("true")
FACTORY_AUTOREVIEW_BIN=${shellQuote(autoreview)}
FACTORY_REVIEW_ARGS=(--reviewers codex)
FACTORY_REVIEW_CONFIRM_ARGS=()
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
  it("refuses pathspec commits that would exclude staged files from the commit tree", () => {
    expectPartialTreeCommitRefused(["a.txt"]);
  });

  it("refuses --only commits that would exclude staged files from the commit tree", () => {
    expectPartialTreeCommitRefused(["--only", "a.txt"]);
  });
});
