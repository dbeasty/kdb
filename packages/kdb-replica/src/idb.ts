/**
 * The IndexedDB store: one object store of string keys, one readwrite transaction per apply,
 * with `durability: "strict"` so a resolved apply has reached the disk. This is the discipline of
 * `@kdb/embed`'s IndexedDbBackend, without its whole-image load.
 */

import type { Batch, KvStore } from "./store.ts";

const STORE = "kv";

function req<T>(r: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    r.onsuccess = () => resolve(r.result);
    r.onerror = () => reject(r.error);
  });
}

function done(tx: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error ?? new Error("kdb replica: IndexedDB transaction aborted"));
  });
}

function toBytes(v: unknown): Uint8Array | undefined {
  if (v === undefined) return undefined;
  if (v instanceof Uint8Array) return v;
  if (v instanceof ArrayBuffer) return new Uint8Array(v);
  throw new Error("kdb replica: unexpected value type in IndexedDB");
}

/** Where IndexedDB comes from: the runtime's globals by default, or an implementation (tests). */
export interface IndexedDbImpl {
  factory: IDBFactory;
  keyRange: typeof IDBKeyRange;
}

function globalImpl(): IndexedDbImpl {
  const g = globalThis as { indexedDB?: IDBFactory; IDBKeyRange?: typeof IDBKeyRange };
  if (!g.indexedDB || !g.IDBKeyRange) throw new Error("kdb replica: no IndexedDB in this runtime");
  return { factory: g.indexedDB, keyRange: g.IDBKeyRange };
}

export class IndexedDbStore implements KvStore {
  readonly #db: IDBDatabase;
  readonly #keyRange: typeof IDBKeyRange;

  private constructor(db: IDBDatabase, keyRange: typeof IDBKeyRange) {
    this.#db = db;
    this.#keyRange = keyRange;
  }

  /** Opens (creating) the database `name`. */
  static async open(name: string, impl: IndexedDbImpl = globalImpl()): Promise<IndexedDbStore> {
    const open = impl.factory.open(name, 1);
    open.onupgradeneeded = () => {
      if (!open.result.objectStoreNames.contains(STORE)) open.result.createObjectStore(STORE);
    };
    return new IndexedDbStore(await req(open), impl.keyRange);
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    const tx = this.#db.transaction(STORE, "readonly");
    return toBytes(await req(tx.objectStore(STORE).get(key)));
  }

  async getMany(keys: readonly string[]): Promise<(Uint8Array | undefined)[]> {
    const tx = this.#db.transaction(STORE, "readonly");
    const os = tx.objectStore(STORE);
    return (await Promise.all(keys.map((k) => req(os.get(k))))).map(toBytes);
  }

  async scan(start: string, end: string, limit?: number): Promise<[string, Uint8Array][]> {
    const tx = this.#db.transaction(STORE, "readonly");
    const os = tx.objectStore(STORE);
    const range = this.#keyRange.bound(start, end, false, true);
    const [keys, values] = await Promise.all([req(os.getAllKeys(range, limit)), req(os.getAll(range, limit))]);
    return keys.map((k, i) => [k as string, toBytes(values[i])!]);
  }

  async apply(batch: Batch): Promise<void> {
    if (batch.size === 0) return;
    const tx = this.#db.transaction(STORE, "readwrite", { durability: "strict" });
    const os = tx.objectStore(STORE);
    for (const [k, v] of batch) {
      if (v === null) os.delete(k);
      else os.put(v, k);
    }
    await done(tx);
  }

  async close(): Promise<void> {
    this.#db.close();
  }

  /** Deletes the database `name`, for logout. */
  static async destroy(name: string, impl: IndexedDbImpl = globalImpl()): Promise<void> {
    await req(impl.factory.deleteDatabase(name) as unknown as IDBRequest<unknown>);
  }
}
