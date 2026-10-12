/**
 * A POSIX-ish filesystem for the Go runtime's `fs` object, persisted to a key-value backend.
 *
 * Why this layer: the file-backed engine (delta log, checkpoints, the cross-namespace transaction
 * log, namespace metadata) reaches the disk through `os.*` in about a dozen places, not through one
 * storage interface. Go's js/wasm runtime routes every `os.*` call to a JavaScript `fs` object, so
 * implementing that object makes the real, unmodified file engine run - rather than a second,
 * browser-only persistence path that could drift from it.
 *
 * Model: the whole tree lives in memory (reads and writes are synchronous and cheap); persistence is
 * write-behind in 64 KiB chunks. **`fsync` is the durability boundary**: its callback fires only after
 * every change made so far has been committed to the backend in one atomic batch. The engine already
 * acks a commit only after fsync, so "acknowledged" means "survives a reload or a crash". A crash
 * between fsyncs loses at most un-fsynced writes, and because each batch is atomic the backend is
 * always some real prefix of the history, never a torn mix.
 */

export const CHUNK_SIZE = 64 * 1024;

/**
 * Where the filesystem is kept. `apply` must be atomic: all of `puts` and `deletes`, or none.
 * Keys are opaque strings; values are bytes.
 */
export interface FsBackend {
  loadAll(): Promise<Map<string, Uint8Array>>;
  apply(puts: Map<string, Uint8Array>, deletes: string[]): Promise<void>;
}

/** An in-memory backend. Survives as long as the object does, so it can stand in for a disk across "reloads" in tests. */
export class MemoryBackend implements FsBackend {
  readonly #data = new Map<string, Uint8Array>();
  /** Counts atomic batches, so a test can see how much persistence work an operation caused. */
  batches = 0;

  async loadAll(): Promise<Map<string, Uint8Array>> {
    return new Map([...this.#data].map(([k, v]) => [k, v.slice()]));
  }

  async apply(puts: Map<string, Uint8Array>, deletes: string[]): Promise<void> {
    this.batches++;
    for (const k of deletes) this.#data.delete(k);
    for (const [k, v] of puts) this.#data.set(k, v.slice());
  }

  /** A copy of what is on "disk" right now: the state a crash at this instant would leave. */
  clone(): MemoryBackend {
    const copy = new MemoryBackend();
    for (const [k, v] of this.#data) copy.#data.set(k, v.slice());
    return copy;
  }

  get size(): number {
    return this.#data.size;
  }
}

/**
 * IndexedDB backend (browsers, and any runtime with `indexedDB`). One object store, one readwrite
 * transaction per batch - IndexedDB transactions are atomic, which is the property this needs.
 */
export class IndexedDbBackend implements FsBackend {
  readonly #name: string;
  readonly #durability: IDBTransactionDurability;
  #db: Promise<IDBDatabase> | undefined;

  /**
   * @param durability "strict" (default) asks the browser to flush to disk before the transaction
   *   completes, so an acknowledged commit survives power loss; "relaxed" is faster and survives a
   *   tab crash but not necessarily a power cut.
   */
  constructor(name: string, durability: IDBTransactionDurability = "strict") {
    this.#name = `kdb-embed:${name}`;
    this.#durability = durability;
  }

  #open(): Promise<IDBDatabase> {
    this.#db ??= new Promise((resolve, reject) => {
      const request = indexedDB.open(this.#name, 1);
      request.onupgradeneeded = () => request.result.createObjectStore("kv");
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error ?? new Error("indexedDB.open failed"));
      request.onblocked = () => reject(new Error(`kdb embed: database ${this.#name} is blocked by another connection`));
    });
    return this.#db;
  }

  async loadAll(): Promise<Map<string, Uint8Array>> {
    const db = await this.#open();
    return new Promise((resolve, reject) => {
      const out = new Map<string, Uint8Array>();
      const request = db.transaction("kv", "readonly").objectStore("kv").openCursor();
      request.onsuccess = () => {
        const cursor = request.result;
        if (cursor === null) return resolve(out);
        out.set(String(cursor.key), new Uint8Array(cursor.value as ArrayBuffer));
        cursor.continue();
      };
      request.onerror = () => reject(request.error ?? new Error("indexedDB cursor failed"));
    });
  }

  async apply(puts: Map<string, Uint8Array>, deletes: string[]): Promise<void> {
    const db = await this.#open();
    return new Promise((resolve, reject) => {
      const tx = db.transaction("kv", "readwrite", { durability: this.#durability });
      const store = tx.objectStore("kv");
      for (const k of deletes) store.delete(k);
      for (const [k, v] of puts) {
        // A copy, not a view: the filesystem keeps mutating its buffers after this returns.
        store.put(v.slice().buffer, k);
      }
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error ?? new Error("indexedDB transaction failed"));
      tx.onabort = () => reject(tx.error ?? new Error("indexedDB transaction aborted"));
    });
  }

  close(): void {
    void this.#db?.then((db) => db.close());
  }
}

// --- the filesystem ------------------------------------------------------------------------------

type Callback<T = undefined> = (error: FsError | null, value?: T) => void;

export interface FsError extends Error {
  code: string;
  errno: number;
}

const ERRNO: Record<string, number> = {
  ENOENT: 2, EBADF: 9, EEXIST: 17, ENOTDIR: 20, EISDIR: 21, EINVAL: 22, ENOTEMPTY: 39, ENOSYS: 38, EPERM: 1,
};

function fsError(code: string, syscall: string, path?: string): FsError {
  const error = new Error(`${code}: ${syscall}${path === undefined ? "" : ` '${path}'`}`) as FsError;
  error.code = code;
  error.errno = ERRNO[code] ?? 0;
  return error;
}

const S_IFDIR = 0o040000;
const S_IFREG = 0o100000;

export const OPEN_FLAGS = {
  O_RDONLY: 0, O_WRONLY: 1, O_RDWR: 2, O_CREAT: 64, O_EXCL: 128, O_TRUNC: 512, O_APPEND: 1024, O_DIRECTORY: 65536,
} as const;

interface FileNode {
  kind: "file";
  ino: number;
  data: Uint8Array;
  size: number;
  mode: number;
  mtimeMs: number;
}
interface DirNode {
  kind: "dir";
  ino: number;
  mode: number;
  mtimeMs: number;
}
type Node = FileNode | DirNode;

interface OpenFile {
  path: string;
  node: Node;
  flags: number;
  position: number;
}

/** Files whose lifetime is a process, not a database. A lock must not outlive the holder that crashed. */
const isEphemeral = (path: string): boolean => path.endsWith(".lock");

const norm = (path: string): string => {
  const parts: string[] = [];
  for (const part of path.split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") parts.pop();
    else parts.push(part);
  }
  return "/" + parts.join("/");
};
const parentOf = (path: string): string => (path === "/" ? "/" : norm(path.slice(0, path.lastIndexOf("/"))));
const baseOf = (path: string): string => path.slice(path.lastIndexOf("/") + 1);

const metaKey = (path: string): string => `m:${path}`;
const chunkKey = (path: string, index: number): string => `c:${path}\u0000${index}`;

interface FsOptions {
  /** How long after a change that nothing fsynced it is written back anyway. */
  flushDelayMs?: number;
}

export class PersistentFs {
  readonly #backend: FsBackend;
  readonly #nodes = new Map<string, Node>();
  readonly #fds = new Map<number, OpenFile>();
  #nextFd = 4;
  #nextIno = 2;

  // Write-behind state, cleared by each flush.
  readonly #dirtyMeta = new Set<string>();
  readonly #dirtyChunks = new Map<string, Set<number>>();
  readonly #deletedKeys = new Set<string>();
  #flushChain: Promise<void> = Promise.resolve();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #failure: Error | undefined;
  readonly #flushDelayMs: number;

  /** Counts backend batches this filesystem wrote. */
  flushes = 0;

  private constructor(backend: FsBackend, options: FsOptions) {
    this.#backend = backend;
    this.#flushDelayMs = options.flushDelayMs ?? 50;
    this.#nodes.set("/", { kind: "dir", ino: 1, mode: 0o755, mtimeMs: Date.now() });
  }

  static async open(backend: FsBackend, options: FsOptions = {}): Promise<PersistentFs> {
    const fs = new PersistentFs(backend, options);
    const stored = await backend.loadAll();
    const sizes = new Map<string, number>();
    for (const [key, value] of stored) {
      if (!key.startsWith("m:")) continue;
      const path = key.slice(2);
      const meta = JSON.parse(new TextDecoder().decode(value)) as { kind: "file" | "dir"; size?: number; mode: number; mtimeMs: number };
      if (meta.kind === "dir") {
        fs.#nodes.set(path, { kind: "dir", ino: fs.#nextIno++, mode: meta.mode, mtimeMs: meta.mtimeMs });
      } else {
        const size = meta.size ?? 0;
        sizes.set(path, size);
        fs.#nodes.set(path, { kind: "file", ino: fs.#nextIno++, data: new Uint8Array(size), size, mode: meta.mode, mtimeMs: meta.mtimeMs });
      }
    }
    for (const [key, value] of stored) {
      if (!key.startsWith("c:")) continue;
      const split = key.lastIndexOf("\u0000");
      const path = key.slice(2, split);
      const index = Number(key.slice(split + 1));
      const node = fs.#nodes.get(path);
      // A chunk with no metadata is the leftover of a delete whose batch never landed; ignore it.
      if (node?.kind !== "file") continue;
      node.data.set(value.subarray(0, Math.max(0, Math.min(value.length, node.size - index * CHUNK_SIZE))), index * CHUNK_SIZE);
    }
    return fs;
  }

  // --- persistence --------------------------------------------------------------------------------

  #touchMeta(path: string): void {
    if (isEphemeral(path)) return;
    this.#dirtyMeta.add(path);
    this.#schedule();
  }

  #touchChunk(path: string, index: number): void {
    if (isEphemeral(path)) return;
    let set = this.#dirtyChunks.get(path);
    if (set === undefined) this.#dirtyChunks.set(path, (set = new Set()));
    set.add(index);
    this.#schedule();
  }

  #schedule(): void {
    if (this.#timer !== undefined) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.flush().catch(() => undefined); // the failure is held and reported by the next fsync
    }, this.#flushDelayMs);
    (this.#timer as { unref?: () => void }).unref?.();
  }

  /** Commits everything changed so far, atomically. Resolves once it is in the backend. */
  flush(): Promise<void> {
    if (this.#timer !== undefined) {
      clearTimeout(this.#timer);
      this.#timer = undefined;
    }
    // Snapshot synchronously: later writes belong to the next batch.
    const puts = new Map<string, Uint8Array>();
    const encoder = new TextEncoder();
    for (const path of this.#dirtyMeta) {
      const node = this.#nodes.get(path);
      if (node === undefined) continue;
      const meta = node.kind === "file"
        ? { kind: "file", size: node.size, mode: node.mode, mtimeMs: node.mtimeMs }
        : { kind: "dir", mode: node.mode, mtimeMs: node.mtimeMs };
      puts.set(metaKey(path), encoder.encode(JSON.stringify(meta)));
    }
    for (const [path, indexes] of this.#dirtyChunks) {
      const node = this.#nodes.get(path);
      if (node?.kind !== "file") continue;
      for (const index of indexes) {
        const start = index * CHUNK_SIZE;
        if (start >= node.size) continue; // truncated away since it was dirtied
        puts.set(chunkKey(path, index), node.data.slice(start, Math.min(start + CHUNK_SIZE, node.size)));
      }
    }
    const deletes = [...this.#deletedKeys].filter((k) => !puts.has(k));
    this.#dirtyMeta.clear();
    this.#dirtyChunks.clear();
    this.#deletedKeys.clear();
    if (puts.size === 0 && deletes.length === 0) return this.#flushChain;

    this.#flushChain = this.#flushChain.then(async () => {
      try {
        await this.#backend.apply(puts, deletes);
        this.flushes++;
      } catch (error) {
        this.#failure = error instanceof Error ? error : new Error(String(error));
        throw this.#failure;
      }
    });
    return this.#flushChain;
  }

  #deleteFileKeys(path: string, node: FileNode): void {
    if (isEphemeral(path)) return;
    this.#deletedKeys.add(metaKey(path));
    for (let i = 0; i * CHUNK_SIZE < node.size; i++) this.#deletedKeys.add(chunkKey(path, i));
    this.#dirtyChunks.delete(path);
    this.#dirtyMeta.delete(path);
  }

  #markAllDirty(path: string, node: Node): void {
    this.#touchMeta(path);
    if (node.kind === "file") for (let i = 0; i * CHUNK_SIZE < node.size; i++) this.#touchChunk(path, i);
  }

  // --- helpers --------------------------------------------------------------------------------------

  #lookup(path: string, syscall: string): Node {
    const node = this.#nodes.get(norm(path));
    if (node === undefined) throw fsError("ENOENT", syscall, path);
    return node;
  }

  #parentDir(path: string, syscall: string): void {
    const parent = this.#nodes.get(parentOf(path));
    if (parent === undefined) throw fsError("ENOENT", syscall, path);
    if (parent.kind !== "dir") throw fsError("ENOTDIR", syscall, path);
  }

  #stats(node: Node): Record<string, unknown> {
    const size = node.kind === "file" ? node.size : 4096;
    return {
      dev: 1, ino: node.ino, nlink: 1, uid: 0, gid: 0, rdev: 0, size, blksize: 4096,
      blocks: Math.ceil(size / 512),
      mode: (node.kind === "dir" ? S_IFDIR : S_IFREG) | node.mode,
      atimeMs: node.mtimeMs, mtimeMs: node.mtimeMs, ctimeMs: node.mtimeMs,
      isDirectory: () => node.kind === "dir",
      isFile: () => node.kind === "file",
    };
  }

  #file(fd: number, syscall: string): OpenFile {
    const file = this.#fds.get(fd);
    if (file === undefined) throw fsError("EBADF", syscall);
    return file;
  }

  #resize(path: string, node: FileNode, length: number): void {
    if (length < 0) throw fsError("EINVAL", "truncate", path);
    if (length > node.data.length) {
      const grown = new Uint8Array(Math.max(length, node.data.length * 2, 4096));
      grown.set(node.data.subarray(0, node.size));
      node.data = grown;
    }
    if (length < node.size) {
      node.data.fill(0, length, node.size);
      if (!isEphemeral(path)) {
        for (let i = Math.ceil(length / CHUNK_SIZE); i * CHUNK_SIZE < node.size; i++) {
          this.#deletedKeys.add(chunkKey(path, i));
          this.#dirtyChunks.get(path)?.delete(i);
        }
        if (length % CHUNK_SIZE !== 0) this.#touchChunk(path, Math.floor(length / CHUNK_SIZE));
      }
    }
    node.size = length;
    node.mtimeMs = Date.now();
    this.#touchMeta(path);
  }

  // --- the object Go's syscall package calls ----------------------------------------------------------

  /** Builds the `fs` object for one Go runtime. `base` supplies stdout/stderr (fds 1 and 2). */
  api(base: Record<string, unknown> | undefined): Record<string, unknown> {
    // Wraps a synchronous operation in the (error, value) callback shape Go's runtime expects.
    const call = <T>(callback: Callback<T>, run: () => T): void => {
      let value: T;
      try {
        value = run();
      } catch (error) {
        callback(error as FsError);
        return;
      }
      callback(null, value);
    };

    const api: Record<string, unknown> = {
      constants: { ...OPEN_FLAGS },

      open: (path: string, flags: number, mode: number, callback: Callback<number>) =>
        call(callback, () => {
          const p = norm(path);
          let node = this.#nodes.get(p);
          if (node === undefined) {
            if ((flags & OPEN_FLAGS.O_CREAT) === 0) throw fsError("ENOENT", "open", path);
            this.#parentDir(p, "open");
            node = { kind: "file", ino: this.#nextIno++, data: new Uint8Array(0), size: 0, mode: mode & 0o777, mtimeMs: Date.now() };
            this.#nodes.set(p, node);
            this.#touchMeta(p);
            this.#touchMeta(parentOf(p));
          } else {
            if ((flags & OPEN_FLAGS.O_CREAT) !== 0 && (flags & OPEN_FLAGS.O_EXCL) !== 0) throw fsError("EEXIST", "open", path);
            if (node.kind === "dir" && (flags & (OPEN_FLAGS.O_WRONLY | OPEN_FLAGS.O_RDWR)) !== 0) throw fsError("EISDIR", "open", path);
            if ((flags & OPEN_FLAGS.O_DIRECTORY) !== 0 && node.kind !== "dir") throw fsError("ENOTDIR", "open", path);
            if ((flags & OPEN_FLAGS.O_TRUNC) !== 0 && node.kind === "file") this.#resize(p, node, 0);
          }
          const fd = this.#nextFd++;
          this.#fds.set(fd, { path: p, node, flags, position: 0 });
          return fd;
        }),

      close: (fd: number, callback: Callback) =>
        call(callback, () => {
          this.#file(fd, "close");
          this.#fds.delete(fd);
          return undefined;
        }),

      read: (fd: number, buffer: Uint8Array, offset: number, length: number, position: number | null, callback: Callback<number>) =>
        call(callback, () => {
          const file = this.#file(fd, "read");
          if (file.node.kind === "dir") throw fsError("EISDIR", "read");
          const at = position ?? file.position;
          const n = Math.max(0, Math.min(length, file.node.size - at));
          buffer.set(file.node.data.subarray(at, at + n), offset);
          if (position === null) file.position += n;
          return n;
        }),

      write: (fd: number, buffer: Uint8Array, offset: number, length: number, position: number | null, callback: Callback<number>) => {
        if (fd === 1 || fd === 2) {
          (base?.["write"] as ((...a: unknown[]) => void) | undefined)?.(fd, buffer, offset, length, position, callback);
          return;
        }
        call(callback, () => {
          const file = this.#file(fd, "write");
          const node = file.node;
          if (node.kind === "dir") throw fsError("EISDIR", "write");
          if ((file.flags & (OPEN_FLAGS.O_WRONLY | OPEN_FLAGS.O_RDWR)) === 0) throw fsError("EBADF", "write");
          const at = (file.flags & OPEN_FLAGS.O_APPEND) !== 0 ? node.size : (position ?? file.position);
          const end = at + length;
          if (end > node.data.length) {
            const grown = new Uint8Array(Math.max(end, node.data.length * 2, 4096));
            grown.set(node.data.subarray(0, node.size));
            node.data = grown;
          }
          node.data.set(buffer.subarray(offset, offset + length), at);
          if (end > node.size) node.size = end;
          node.mtimeMs = Date.now();
          if (position === null) file.position = end;
          if (length > 0) {
            for (let i = Math.floor(at / CHUNK_SIZE); i <= Math.floor((end - 1) / CHUNK_SIZE); i++) this.#touchChunk(file.path, i);
          }
          this.#touchMeta(file.path);
          return length;
        });
      },

      // The durability boundary: the callback fires only once the backend has committed everything.
      fsync: (fd: number, callback: Callback) => {
        if (fd === 1 || fd === 2) return callback(null);
        try {
          this.#file(fd, "fsync");
        } catch (error) {
          return callback(error as FsError);
        }
        this.flush().then(
          () => callback(null),
          (error: unknown) => callback(Object.assign(error instanceof Error ? error : new Error(String(error)), { code: "EIO", errno: 5 }) as FsError),
        );
      },

      fstat: (fd: number, callback: Callback<Record<string, unknown>>) => call(callback, () => this.#stats(this.#file(fd, "fstat").node)),
      stat: (path: string, callback: Callback<Record<string, unknown>>) => call(callback, () => this.#stats(this.#lookup(path, "stat"))),
      lstat: (path: string, callback: Callback<Record<string, unknown>>) => call(callback, () => this.#stats(this.#lookup(path, "lstat"))),

      ftruncate: (fd: number, length: number, callback: Callback) =>
        call(callback, () => {
          const file = this.#file(fd, "ftruncate");
          if (file.node.kind !== "file") throw fsError("EINVAL", "ftruncate");
          this.#resize(file.path, file.node, length);
          return undefined;
        }),
      truncate: (path: string, length: number, callback: Callback) =>
        call(callback, () => {
          const node = this.#lookup(path, "truncate");
          if (node.kind !== "file") throw fsError("EISDIR", "truncate", path);
          this.#resize(norm(path), node, length);
          return undefined;
        }),

      mkdir: (path: string, perm: number, callback: Callback) =>
        call(callback, () => {
          const p = norm(path);
          if (this.#nodes.has(p)) throw fsError("EEXIST", "mkdir", path);
          this.#parentDir(p, "mkdir");
          this.#nodes.set(p, { kind: "dir", ino: this.#nextIno++, mode: perm & 0o777, mtimeMs: Date.now() });
          this.#touchMeta(p);
          return undefined;
        }),

      readdir: (path: string, callback: Callback<string[]>) =>
        call(callback, () => {
          const p = norm(path);
          const node = this.#lookup(p, "readdir");
          if (node.kind !== "dir") throw fsError("ENOTDIR", "readdir", path);
          const prefix = p === "/" ? "/" : `${p}/`;
          const names: string[] = [];
          for (const key of this.#nodes.keys()) {
            if (key !== p && key.startsWith(prefix) && !key.slice(prefix.length).includes("/")) names.push(key.slice(prefix.length));
          }
          return names.sort();
        }),

      rmdir: (path: string, callback: Callback) =>
        call(callback, () => {
          const p = norm(path);
          const node = this.#lookup(p, "rmdir");
          if (node.kind !== "dir") throw fsError("ENOTDIR", "rmdir", path);
          const prefix = `${p}/`;
          for (const key of this.#nodes.keys()) if (key.startsWith(prefix)) throw fsError("ENOTEMPTY", "rmdir", path);
          this.#nodes.delete(p);
          if (!isEphemeral(p)) {
            this.#deletedKeys.add(metaKey(p));
            this.#dirtyMeta.delete(p);
            this.#schedule();
          }
          return undefined;
        }),

      unlink: (path: string, callback: Callback) =>
        call(callback, () => {
          const p = norm(path);
          const node = this.#lookup(p, "unlink");
          if (node.kind === "dir") throw fsError("EISDIR", "unlink", path);
          this.#nodes.delete(p);
          this.#deleteFileKeys(p, node);
          this.#schedule();
          return undefined;
        }),

      rename: (from: string, to: string, callback: Callback) =>
        call(callback, () => {
          const a = norm(from);
          const b = norm(to);
          const node = this.#lookup(a, "rename");
          this.#parentDir(b, "rename");
          if (a === b) return undefined;
          const target = this.#nodes.get(b);
          if (target !== undefined) {
            if (target.kind === "dir" && node.kind !== "dir") throw fsError("EISDIR", "rename", to);
            if (target.kind !== "dir" && node.kind === "dir") throw fsError("ENOTDIR", "rename", to);
            if (target.kind === "dir") {
              for (const key of this.#nodes.keys()) if (key.startsWith(`${b}/`)) throw fsError("ENOTEMPTY", "rename", to);
            }
            this.#nodes.delete(b);
            if (target.kind === "file") this.#deleteFileKeys(b, target);
            else if (!isEphemeral(b)) this.#deletedKeys.add(metaKey(b));
          }
          // A directory moves with everything under it.
          const moves: Array<[string, string]> = [[a, b]];
          if (node.kind === "dir") for (const key of this.#nodes.keys()) if (key.startsWith(`${a}/`)) moves.push([key, b + key.slice(a.length)]);
          for (const [oldPath, newPath] of moves) {
            const moved = this.#nodes.get(oldPath) as Node;
            this.#nodes.delete(oldPath);
            if (moved.kind === "file") this.#deleteFileKeys(oldPath, moved);
            else if (!isEphemeral(oldPath)) {
              this.#deletedKeys.add(metaKey(oldPath));
              this.#dirtyMeta.delete(oldPath);
            }
            this.#nodes.set(newPath, moved);
            this.#markAllDirty(newPath, moved);
          }
          return undefined;
        }),

      utimes: (path: string, _atime: number, mtime: number, callback: Callback) =>
        call(callback, () => {
          const node = this.#lookup(path, "utimes");
          node.mtimeMs = mtime * 1000;
          this.#touchMeta(norm(path));
          return undefined;
        }),
      chmod: (path: string, mode: number, callback: Callback) =>
        call(callback, () => {
          this.#lookup(path, "chmod").mode = mode & 0o777;
          this.#touchMeta(norm(path));
          return undefined;
        }),
      fchmod: (fd: number, mode: number, callback: Callback) =>
        call(callback, () => {
          const file = this.#file(fd, "fchmod");
          file.node.mode = mode & 0o777;
          this.#touchMeta(file.path);
          return undefined;
        }),
      chown: (_p: string, _u: number, _g: number, callback: Callback) => callback(null),
      fchown: (_f: number, _u: number, _g: number, callback: Callback) => callback(null),
      lchown: (_p: string, _u: number, _g: number, callback: Callback) => callback(null),

      link: (_a: string, _b: string, callback: Callback) => callback(fsError("ENOSYS", "link")),
      symlink: (_a: string, _b: string, callback: Callback) => callback(fsError("ENOSYS", "symlink")),
      readlink: (path: string, callback: Callback) => callback(fsError("EINVAL", "readlink", path)),
    };

    // Everything else Go might call (writeSync for stdout) falls through to the runtime's own.
    for (const [key, value] of Object.entries(base ?? {})) if (!(key in api)) api[key] = value;
    return api;
  }

  /** Flushes and reports any earlier background-flush failure instead of losing it. */
  async close(): Promise<void> {
    await this.flush();
    if (this.#failure !== undefined) throw this.#failure;
  }

  /** Test seam: the number of nodes, files and directories included. */
  get nodeCount(): number {
    return this.#nodes.size;
  }

  /** Test seam: reads a whole file's current in-memory contents. */
  readFile(path: string): Uint8Array | undefined {
    const node = this.#nodes.get(norm(path));
    return node?.kind === "file" ? node.data.slice(0, node.size) : undefined;
  }
}
