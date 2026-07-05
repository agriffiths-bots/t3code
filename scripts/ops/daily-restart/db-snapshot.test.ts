// @effect-diagnostics nodeBuiltinImport:off - Tests exercise shell tools and sqlite3.
import { assert, describe, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const repoRoot = NodePath.resolve(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "../../..",
);
const FULL_PATH =
  "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/linuxbrew/.linuxbrew/bin:/home/adam/.local/bin";
const snapshotTool = NodePath.join(repoRoot, "scripts/ops/daily-restart/t3-db-snapshot");
const restoreTool = NodePath.join(repoRoot, "scripts/ops/daily-restart/t3-db-restore");

function makeTempDir(): string {
  return NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-db-snapshot-'"));
}

function commandEnv(): NodeJS.ProcessEnv {
  return { ...process.env, PATH: FULL_PATH };
}

function run(
  args: ReadonlyArray<string>,
  options: { readonly expectFailure?: boolean; readonly timeoutMs?: number } = {},
): NodeChildProcess.SpawnSyncReturns<string> {
  const result = NodeChildProcess.spawnSync(args[0]!, args.slice(1), {
    encoding: "utf8",
    env: commandEnv(),
    timeout: options.timeoutMs,
  });

  if (!options.expectFailure && result.status !== 0) {
    assert.fail(
      `Command failed: ${args.join(" ")}\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
    );
  }

  return result;
}

function sqlite(db: string, sql: string): string {
  return run(["sqlite3", db, sql]).stdout.trim();
}

function waitForSqliteOutput(
  child: NodeChildProcess.ChildProcess,
  expected: string,
): Promise<void> {
  const stdoutStream = child.stdout;
  const stderrStream = child.stderr;
  if (!stdoutStream || !stderrStream) {
    return Promise.reject(new Error("sqlite3 child process did not expose stdout/stderr"));
  }

  return new Promise((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    let settled = false;

    const cleanup = (): void => {
      stdoutStream.off("data", onStdout);
      stderrStream.off("data", onStderr);
      child.off("error", onError);
      child.off("exit", onExit);
    };

    const finish = (error?: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    };

    const onStdout = (chunk: Buffer | string): void => {
      stdout += chunk.toString();
      if (stdout.includes(expected)) {
        finish();
      }
    };
    const onStderr = (chunk: Buffer | string): void => {
      stderr += chunk.toString();
    };
    const onError = (error: Error): void => {
      finish(error);
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null): void => {
      finish(
        new Error(
          `sqlite3 exited before ${expected}: code=${code ?? "null"} signal=${
            signal ?? "null"
          }\nstdout:\n${stdout}\nstderr:\n${stderr}`,
        ),
      );
    };

    stdoutStream.on("data", onStdout);
    stderrStream.on("data", onStderr);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}

function createWalDatabase(db: string): void {
  sqlite(
    db,
    [
      "PRAGMA journal_mode=WAL;",
      "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
      "INSERT INTO items(name) VALUES ('alpha'), ('beta');",
    ].join(" "),
  );
}

describe("daily restart database tools", () => {
  it("snapshots a WAL database while another writer has it open", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const outDir = NodePath.join(dir, "snapshots");
    createWalDatabase(db);

    const writer = NodeChildProcess.spawn("sqlite3", [db], {
      stdio: ["pipe", "pipe", "pipe"],
      env: commandEnv(),
    });
    writer.stdin.write("BEGIN IMMEDIATE;\n");
    writer.stdin.write("INSERT INTO items(name) VALUES ('held-open');\n");

    try {
      const result = run([snapshotTool, "--db", db, "--out-dir", outDir]);
      const snapshot = result.stdout.trim();

      assert.equal(result.stderr, "");
      assert.equal(NodePath.isAbsolute(snapshot), true);
      assert.match(NodePath.basename(snapshot), /^t3-state-\d{8}-\d{6}\.sqlite$/);
      assert.equal(sqlite(snapshot, "PRAGMA integrity_check;"), "ok");
      assert.equal(
        sqlite(snapshot, "SELECT group_concat(name, ',') FROM items ORDER BY id;"),
        "alpha,beta",
      );
    } finally {
      writer.stdin.end(".quit\n");
      writer.kill();
    }
  });

  it("snapshots when the output directory is the database directory", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    createWalDatabase(db);

    const result = run([snapshotTool, "--db", db, "--out-dir", dir], { timeoutMs: 5_000 });
    const snapshot = result.stdout.trim();

    assert.equal(result.stderr, "");
    assert.equal(NodePath.dirname(snapshot), dir);
    assert.equal(
      sqlite(snapshot, "SELECT group_concat(name, ',') FROM items ORDER BY id;"),
      "alpha,beta",
    );
  });

  it("fails on a corrupt source database", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "corrupt.sqlite");
    NodeFS.writeFileSync(db, "not sqlite");

    const result = run([snapshotTool, "--db", db, "--out-dir", NodePath.join(dir, "snapshots")], {
      expectFailure: true,
    });

    assert.notEqual(result.status, 0);
    assert.equal(result.stdout, "");
    assert.match(
      result.stderr,
      /sqlite backup failed|file is not a database|database disk image is malformed/,
    );
  });

  it("does not follow a stale snapshot lock symlink", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const outDir = NodePath.join(dir, "snapshots");
    createWalDatabase(db);
    NodeFS.mkdirSync(outDir);
    NodeFS.symlinkSync(db, NodePath.join(outDir, ".t3-db-snapshot.lock"));

    const snapshot = run([snapshotTool, "--db", db, "--out-dir", outDir]).stdout.trim();

    assert.equal(
      sqlite(db, "SELECT group_concat(name, ',') FROM items ORDER BY id;"),
      "alpha,beta",
    );
    assert.equal(
      sqlite(snapshot, "SELECT group_concat(name, ',') FROM items ORDER BY id;"),
      "alpha,beta",
    );
  });

  it("restores a verified snapshot and moves the current database aside", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const outDir = NodePath.join(dir, "snapshots");
    createWalDatabase(db);
    const snapshot = run([snapshotTool, "--db", db, "--out-dir", outDir]).stdout.trim();

    sqlite(db, "INSERT INTO items(name) VALUES ('after-snapshot');");
    NodeFS.chmodSync(db, 0o644);
    NodeFS.writeFileSync(`${db}-wal`, "");
    NodeFS.writeFileSync(`${db}-shm`, "");
    NodeFS.chmodSync(`${db}-wal`, 0o644);
    NodeFS.chmodSync(`${db}-shm`, 0o644);

    const result = run([restoreTool, "--snapshot", snapshot, "--db", db]);

    assert.equal(result.stdout.trim(), db);
    assert.match(result.stderr, /warning: WAL\/SHM files exist/);
    assert.equal(sqlite(db, "PRAGMA integrity_check;"), "ok");
    assert.equal(
      sqlite(db, "SELECT group_concat(name, ',') FROM items ORDER BY id;"),
      "alpha,beta",
    );
    assert.equal(NodeFS.existsSync(`${db}-wal`), false);
    assert.equal(NodeFS.existsSync(`${db}-shm`), false);
    const backups = NodeFS.readdirSync(dir).filter((entry) =>
      /^state\.sqlite\.before-restore\.\d{8}-\d{6}$/.test(entry),
    );
    assert.equal(backups.length, 1);
    assert.equal(NodeFS.existsSync(NodePath.join(dir, `${backups[0]!}-wal`)), true);
    assert.equal(NodeFS.existsSync(NodePath.join(dir, `${backups[0]!}-shm`)), true);
    assert.equal(
      (NodeFS.statSync(NodePath.join(dir, backups[0]!)).mode & 0o777).toString(8),
      "600",
    );
    assert.equal(
      (NodeFS.statSync(NodePath.join(dir, `${backups[0]!}-wal`)).mode & 0o777).toString(8),
      "600",
    );
    assert.equal(
      (NodeFS.statSync(NodePath.join(dir, `${backups[0]!}-shm`)).mode & 0o777).toString(8),
      "600",
    );
    assert.equal(sqlite(NodePath.join(dir, backups[0]!), "SELECT count(*) FROM items;"), "3");
    assert.equal((NodeFS.statSync(db).mode & 0o777).toString(8), "600");
  });

  it("restores committed frames from a snapshot WAL companion", async () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const snapshot = NodePath.join(dir, "snapshot.sqlite");
    createWalDatabase(db);
    sqlite(db, `VACUUM INTO '${snapshot.replaceAll("'", "''")}';`);

    const snapshotConnection = NodeChildProcess.spawn("sqlite3", [snapshot], {
      stdio: ["pipe", "pipe", "pipe"],
      env: commandEnv(),
    });

    try {
      const walReady = waitForSqliteOutput(snapshotConnection, "snapshot-wal-ready");
      snapshotConnection.stdin.write(".bail on\n");
      snapshotConnection.stdin.write("PRAGMA journal_mode=WAL;\n");
      snapshotConnection.stdin.write("PRAGMA wal_autocheckpoint=0;\n");
      snapshotConnection.stdin.write("INSERT INTO items(name) VALUES ('snapshot-wal-only');\n");
      snapshotConnection.stdin.write("SELECT 'snapshot-wal-ready';\n");
      await walReady;

      assert.equal(NodeFS.existsSync(`${snapshot}-wal`), true);

      const result = run([restoreTool, "--snapshot", snapshot, "--db", db]);

      assert.equal(result.status, 0);
      assert.equal(
        sqlite(db, "SELECT group_concat(name, ',') FROM items ORDER BY id;"),
        "alpha,beta,snapshot-wal-only",
      );
    } finally {
      snapshotConnection.stdin.end(".quit\n");
      snapshotConnection.kill();
    }
  });

  it("does not follow stale restore temp symlinks", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const outDir = NodePath.join(dir, "snapshots");
    createWalDatabase(db);
    const snapshot = run([snapshotTool, "--db", db, "--out-dir", outDir]).stdout.trim();
    NodeFS.symlinkSync(db, NodePath.join(dir, ".restore-state.sqlite.stale"));

    run([restoreTool, "--snapshot", snapshot, "--db", db]);

    assert.equal(
      sqlite(db, "SELECT group_concat(name, ',') FROM items ORDER BY id;"),
      "alpha,beta",
    );
    assert.equal(
      NodeFS.lstatSync(NodePath.join(dir, ".restore-state.sqlite.stale")).isSymbolicLink(),
      true,
    );
  });

  it("prunes old snapshots by retention count", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const outDir = NodePath.join(dir, "snapshots");
    createWalDatabase(db);
    NodeFS.mkdirSync(outDir);

    NodeFS.writeFileSync(NodePath.join(outDir, "t3-state-20260703-000000.sqlite"), "");
    NodeFS.writeFileSync(NodePath.join(outDir, "t3-state-20260703-000001.sqlite"), "");
    const futureSnapshot = NodePath.join(outDir, "t3-state-20990101-000000.sqlite");
    NodeFS.writeFileSync(futureSnapshot, "");
    NodeFS.utimesSync(futureSnapshot, 4_070_908_800, 4_070_908_800);

    const keptSnapshot = run([
      snapshotTool,
      "--db",
      db,
      "--out-dir",
      outDir,
      "--keep",
      "1",
    ]).stdout.trim();
    assert.equal(NodeFS.existsSync(keptSnapshot), true);
    assert.deepStrictEqual(
      NodeFS.readdirSync(outDir).filter((entry) => entry.startsWith("t3-state-")),
      [NodePath.basename(keptSnapshot)],
    );
  });

  it("keeps the current snapshot when the output path contains find metacharacters", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const outDir = NodePath.join(dir, "snapshots-[x]");
    createWalDatabase(db);
    NodeFS.mkdirSync(outDir);
    NodeFS.writeFileSync(NodePath.join(outDir, "t3-state-20260703-000000.sqlite"), "");

    const keptSnapshot = run([
      snapshotTool,
      "--db",
      db,
      "--out-dir",
      outDir,
      "--keep",
      "1",
    ]).stdout.trim();

    assert.equal(NodeFS.existsSync(keptSnapshot), true);
    assert.deepStrictEqual(
      NodeFS.readdirSync(outDir).filter((entry) => entry.startsWith("t3-state-")),
      [NodePath.basename(keptSnapshot)],
    );
  });
});
