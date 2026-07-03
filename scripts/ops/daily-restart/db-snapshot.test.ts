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
const snapshotTool = NodePath.join(repoRoot, "scripts/ops/daily-restart/t3-db-snapshot");
const restoreTool = NodePath.join(repoRoot, "scripts/ops/daily-restart/t3-db-restore");

function makeTempDir(): string {
  return NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-db-snapshot-"));
}

function run(
  args: ReadonlyArray<string>,
  options: { readonly expectFailure?: boolean } = {},
): NodeChildProcess.SpawnSyncReturns<string> {
  const result = NodeChildProcess.spawnSync(args[0]!, args.slice(1), {
    encoding: "utf8",
    env: {
      ...process.env,
      PATH: "/home/linuxbrew/.linuxbrew/bin:/home/adam/.local/bin:/usr/local/bin:/usr/bin:/bin",
    },
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
      env: {
        ...process.env,
        PATH: "/home/linuxbrew/.linuxbrew/bin:/home/adam/.local/bin:/usr/local/bin:/usr/bin:/bin",
      },
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

  it("restores committed frames from a snapshot WAL companion", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const snapshot = NodePath.join(dir, "snapshot.sqlite");
    createWalDatabase(db);
    sqlite(db, `VACUUM INTO '${snapshot.replaceAll("'", "''")}';`);

    const snapshotConnection = NodeChildProcess.spawn("sqlite3", [snapshot], {
      stdio: ["pipe", "pipe", "pipe"],
      env: {
        ...process.env,
        PATH: "/home/linuxbrew/.linuxbrew/bin:/home/adam/.local/bin:/usr/local/bin:/usr/bin:/bin",
      },
    });
    snapshotConnection.stdin.write("PRAGMA journal_mode=WAL;\n");
    snapshotConnection.stdin.write("PRAGMA wal_autocheckpoint=0;\n");
    snapshotConnection.stdin.write("INSERT INTO items(name) VALUES ('snapshot-wal-only');\n");
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 500);

    try {
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

  it("fails restore when the snapshot is corrupt", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const snapshot = NodePath.join(dir, "bad.sqlite");
    createWalDatabase(db);
    NodeFS.writeFileSync(snapshot, "not sqlite");

    const result = run([restoreTool, "--snapshot", snapshot, "--db", db], { expectFailure: true });

    assert.notEqual(result.status, 0);
    assert.equal(result.stdout, "");
    assert.match(result.stderr, /integrity check failed|file is not a database/);
    assert.equal(sqlite(db, "SELECT count(*) FROM items;"), "2");
  });

  it("prunes old snapshots by retention count", () => {
    const dir = makeTempDir();
    const db = NodePath.join(dir, "state.sqlite");
    const outDir = NodePath.join(dir, "snapshots");
    createWalDatabase(db);

    for (let index = 0; index < 3; index++) {
      run([snapshotTool, "--db", db, "--out-dir", outDir, "--keep", "2"]);
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1100);
    }

    const snapshots = NodeFS.readdirSync(outDir).filter((entry) => entry.startsWith("t3-state-"));
    assert.equal(snapshots.length, 2);
  });
});
