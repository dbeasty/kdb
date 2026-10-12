/**
 * The persistent filesystem on its own, driven the way Go's runtime drives it (callbacks), without
 * the engine. Pins the POSIX semantics the file engine relies on, and the persistence contract.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { CHUNK_SIZE, MemoryBackend, OPEN_FLAGS as F, PersistentFs, type FsBackend } from "../src/fs.ts";

type Api = Record<string, (...args: unknown[]) => void>;

/** Calls an fs method and returns a promise of its result, rejecting with the callback's error. */
const call = <T = unknown>(api: Api, method: string, ...args: unknown[]): Promise<T> =>
  new Promise((resolve, reject) => {
    api[method]!(...args, (error: Error | null, value: T) => (error ? reject(error) : resolve(value)));
  });
const failsWith = (promise: Promise<unknown>, code: string) =>
  assert.rejects(promise, (e: Error & { code?: string }) => e.code === code, `expected ${code}`);

const open = async (backend: FsBackend = new MemoryBackend()) => {
  const fs = await PersistentFs.open(backend);
  return { fs, api: fs.api(undefined) as Api, backend };
};
const bytes = (s: string) => new TextEncoder().encode(s);
const text = (b: Uint8Array | undefined) => (b === undefined ? undefined : new TextDecoder().decode(b));

async function writeFile(api: Api, path: string, data: Uint8Array, flags = F.O_WRONLY | F.O_CREAT | F.O_TRUNC) {
  const fd = await call<number>(api, "open", path, flags, 0o644);
  await call(api, "write", fd, data, 0, data.length, null);
  await call(api, "fsync", fd);
  await call(api, "close", fd);
}

describe("PersistentFs semantics", () => {
  it("creates, writes, reads, and reports sizes", async () => {
    const { api } = await open();
    await call(api, "mkdir", "/d", 0o755);
    await writeFile(api, "/d/a.txt", bytes("hello world"));
    const fd = await call<number>(api, "open", "/d/a.txt", F.O_RDONLY, 0);
    const buf = new Uint8Array(5);
    assert.equal(await call(api, "read", fd, buf, 0, 5, 6), 5);
    assert.equal(text(buf), "world");
    const st = await call<{ size: number; isDirectory(): boolean; mode: number }>(api, "fstat", fd);
    assert.equal(st.size, 11);
    assert.equal(st.isDirectory(), false);
    assert.equal(st.mode & 0o170000, 0o100000);
    assert.equal((await call<{ isDirectory(): boolean }>(api, "stat", "/d")).isDirectory(), true);
  });

  it("returns POSIX error codes Go maps to errno", async () => {
    const { api } = await open();
    await failsWith(call(api, "open", "/nope", F.O_RDONLY, 0), "ENOENT");
    await failsWith(call(api, "open", "/no/parent/f", F.O_CREAT | F.O_WRONLY, 0o644), "ENOENT");
    await call(api, "mkdir", "/d", 0o755);
    await failsWith(call(api, "mkdir", "/d", 0o755), "EEXIST");
    await writeFile(api, "/f", bytes("x"));
    await failsWith(call(api, "open", "/f", F.O_CREAT | F.O_EXCL | F.O_WRONLY, 0o644), "EEXIST");
    await failsWith(call(api, "readdir", "/f"), "ENOTDIR");
    await failsWith(call(api, "open", "/d", F.O_WRONLY, 0), "EISDIR");
    await failsWith(call(api, "unlink", "/d"), "EISDIR");
    await writeFile(api, "/d/x", bytes("x"));
    await failsWith(call(api, "rmdir", "/d"), "ENOTEMPTY");
    await failsWith(call(api, "close", 999), "EBADF");
    await failsWith(call(api, "mkdir", "/f/sub", 0o755), "ENOTDIR");
  });

  it("honours O_EXCL as a lock primitive (what the engine's directory lock uses)", async () => {
    const { api } = await open();
    const fd = await call<number>(api, "open", "/.kdb.lock", F.O_CREAT | F.O_EXCL | F.O_WRONLY, 0o644);
    await failsWith(call(api, "open", "/.kdb.lock", F.O_CREAT | F.O_EXCL | F.O_WRONLY, 0o644), "EEXIST");
    await call(api, "close", fd);
    await call(api, "unlink", "/.kdb.lock");
    await call(api, "close", await call(api, "open", "/.kdb.lock", F.O_CREAT | F.O_EXCL | F.O_WRONLY, 0o644));
  });

  it("appends, and reads sequentially when position is null", async () => {
    const { api } = await open();
    await writeFile(api, "/log", bytes("one"));
    const a = await call<number>(api, "open", "/log", F.O_WRONLY | F.O_APPEND, 0);
    await call(api, "write", a, bytes("two"), 0, 3, null);
    await call(api, "write", a, bytes("three"), 0, 5, null);
    const r = await call<number>(api, "open", "/log", F.O_RDONLY, 0);
    const first = new Uint8Array(4);
    await call(api, "read", r, first, 0, 4, null);
    const rest = new Uint8Array(20);
    const n = await call<number>(api, "read", r, rest, 0, 20, null);
    assert.equal(text(first) + text(rest.subarray(0, n)), "onetwothree");
  });

  it("truncates down and up, zero-filling growth", async () => {
    const { fs, api } = await open();
    await writeFile(api, "/t", bytes("0123456789"));
    await call(api, "truncate", "/t", 4);
    assert.equal(text(fs.readFile("/t")), "0123");
    await call(api, "truncate", "/t", 8);
    assert.deepEqual([...fs.readFile("/t")!], [48, 49, 50, 51, 0, 0, 0, 0]);
    const fd = await call<number>(api, "open", "/t", F.O_RDWR, 0);
    await call(api, "ftruncate", fd, 2);
    assert.equal(text(fs.readFile("/t")), "01");
    await failsWith(call(api, "truncate", "/t", -1), "EINVAL");
  });

  it("lists directories, renames files over targets, and moves directory trees", async () => {
    const { fs, api } = await open();
    await call(api, "mkdir", "/a", 0o755);
    await call(api, "mkdir", "/a/sub", 0o755);
    await writeFile(api, "/a/f1", bytes("1"));
    await writeFile(api, "/a/sub/f2", bytes("2"));
    assert.deepEqual(await call(api, "readdir", "/a"), ["f1", "sub"]);
    assert.deepEqual(await call(api, "readdir", "/"), ["a"]);

    await writeFile(api, "/a/new", bytes("new"));
    await call(api, "rename", "/a/new", "/a/f1");
    assert.equal(text(fs.readFile("/a/f1")), "new");
    assert.equal(fs.readFile("/a/new"), undefined);

    await call(api, "rename", "/a", "/b");
    assert.equal(text(fs.readFile("/b/sub/f2")), "2");
    assert.equal(fs.readFile("/a/sub/f2"), undefined);
    await failsWith(call(api, "stat", "/a"), "ENOENT");
  });

  it("normalizes paths", async () => {
    const { fs, api } = await open();
    await call(api, "mkdir", "/d", 0o755);
    await writeFile(api, "/d//./x/../f", bytes("ok"));
    assert.equal(text(fs.readFile("/d/f")), "ok");
  });
});

describe("PersistentFs durability", () => {
  it("reloads exactly what was fsynced, across chunk boundaries", async () => {
    const backend = new MemoryBackend();
    const big = new Uint8Array(CHUNK_SIZE * 2 + 123).map((_, i) => (i * 7) & 0xff);
    {
      const { api } = await open(backend);
      await call(api, "mkdir", "/d", 0o755);
      await writeFile(api, "/d/big", big);
      await writeFile(api, "/d/small", bytes("s"));
    }
    const { fs } = await open(backend);
    assert.deepEqual(fs.readFile("/d/big"), big);
    assert.equal(text(fs.readFile("/d/small")), "s");
  });

  it("writes only the chunks that changed", async () => {
    const backend = new MemoryBackend();
    const { api } = await open(backend);
    const fd = await call<number>(api, "open", "/log", F.O_RDWR | F.O_CREAT, 0o644);
    const block = new Uint8Array(CHUNK_SIZE).fill(1);
    for (let i = 0; i < 4; i++) await call(api, "write", fd, block, 0, block.length, i * CHUNK_SIZE);
    await call(api, "fsync", fd);
    const before = backend.size;
    await call(api, "write", fd, bytes("!"), 0, 1, 4 * CHUNK_SIZE);
    await call(api, "fsync", fd);
    assert.equal(backend.size, before + 1, "appending one byte adds one chunk, not a rewrite");
  });

  it("persists renames, deletes and truncation atomically with the data they refer to", async () => {
    const backend = new MemoryBackend();
    {
      const { api } = await open(backend);
      await writeFile(api, "/tmp", bytes("checkpoint-v2"));
      await writeFile(api, "/live", bytes("checkpoint-v1"));
      await call(api, "rename", "/tmp", "/live");
      await writeFile(api, "/gone", bytes("x".repeat(CHUNK_SIZE + 5)));
      await call(api, "unlink", "/gone");
      await writeFile(api, "/shrunk", new Uint8Array(CHUNK_SIZE * 3).fill(9));
      await call(api, "truncate", "/shrunk", 10);
      const fd = await call<number>(api, "open", "/live", F.O_RDONLY, 0);
      await call(api, "fsync", fd);
    }
    const { fs } = await open(backend);
    assert.equal(text(fs.readFile("/live")), "checkpoint-v2");
    assert.equal(fs.readFile("/tmp"), undefined);
    assert.equal(fs.readFile("/gone"), undefined);
    assert.deepEqual(fs.readFile("/shrunk"), new Uint8Array(10).fill(9));
    for (const key of [...(await backend.loadAll()).keys()]) assert.ok(!key.includes("/gone"), `leftover key ${key}`);
  });

  it("does not persist lock files, so a crashed holder cannot lock the database forever", async () => {
    const backend = new MemoryBackend();
    {
      const { api } = await open(backend);
      const fd = await call<number>(api, "open", "/kdb/.kdb.lock", F.O_CREAT | F.O_EXCL | F.O_WRONLY, 0o644).catch(() => -1);
      assert.equal(fd, -1, "parent does not exist yet");
      await call(api, "mkdir", "/kdb", 0o755);
      const lock = await call<number>(api, "open", "/kdb/.kdb.lock", F.O_CREAT | F.O_EXCL | F.O_WRONLY, 0o644);
      await call(api, "fsync", lock);
    }
    const { fs } = await open(backend);
    assert.equal(fs.readFile("/kdb/.kdb.lock"), undefined);
    for (const key of (await backend.loadAll()).keys()) assert.ok(!key.includes(".lock"), key);
  });

  it("does not report fsync success unless the backend committed", async () => {
    class Failing extends MemoryBackend {
      failing = false;
      override async apply(puts: Map<string, Uint8Array>, deletes: string[]): Promise<void> {
        if (this.failing) throw new Error("quota exceeded");
        return super.apply(puts, deletes);
      }
    }
    const backend = new Failing();
    const { api } = await open(backend);
    const fd = await call<number>(api, "open", "/f", F.O_RDWR | F.O_CREAT, 0o644);
    await call(api, "write", fd, bytes("a"), 0, 1, null);
    await call(api, "fsync", fd);
    backend.failing = true;
    await call(api, "write", fd, bytes("b"), 0, 1, null);
    await assert.rejects(call(api, "fsync", fd), /quota exceeded/);
  });

  it("flushes un-fsynced changes in the background so metadata operations do land", async () => {
    const backend = new MemoryBackend();
    const fs = await PersistentFs.open(backend, { flushDelayMs: 5 });
    const api = fs.api(undefined) as Api;
    await call(api, "mkdir", "/later", 0o755);
    await new Promise((r) => setTimeout(r, 60));
    const reloaded = await PersistentFs.open(backend);
    assert.equal(((await call(reloaded.api(undefined) as Api, "stat", "/later")) as { isDirectory(): boolean }).isDirectory(), true);
  });
});
