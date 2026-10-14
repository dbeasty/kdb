/**
 * The replica's storage: a small ordered key-value interface, so the same engine runs over
 * IndexedDB in a browser, a directory on Node, and memory in tests.
 *
 * It is deliberately not `@kdb/embed`'s `FsBackend`, which loads a whole file tree into memory.
 * A replica must hold namespaces larger than its memory, so nothing here loads the store whole.
 * What it keeps from that design is the durability rule: `apply` is one atomic transaction, and
 * its promise resolves only once the transaction is durable.
 */

/** A set of writes applied atomically; null deletes the key. */
export type Batch = Map<string, Uint8Array | null>;

export interface KvStore {
  get(key: string): Promise<Uint8Array | undefined>;
  getMany(keys: readonly string[]): Promise<(Uint8Array | undefined)[]>;
  /** Entries with start <= key < end, in key order, at most limit of them. */
  scan(start: string, end: string, limit?: number): Promise<[string, Uint8Array][]>;
  /** Applies batch atomically; resolves once it is durable. */
  apply(batch: Batch): Promise<void>;
  close(): Promise<void>;
}

/** The smallest key greater than every key with this prefix. */
export function prefixEnd(prefix: string): string {
  return prefix + "￿";
}

/** An in-memory store, for tests and for replicas that need no persistence. */
export class MemoryStore implements KvStore {
  readonly #data = new Map<string, Uint8Array>();
  #sorted: string[] | null = null;
  /** Counts apply calls; tests use it to check atomicity. */
  applies = 0;
  /** Set to make the next apply fail before writing anything (crash and failure tests). */
  failNextApply: Error | null = null;

  async get(key: string): Promise<Uint8Array | undefined> {
    return this.#data.get(key);
  }

  async getMany(keys: readonly string[]): Promise<(Uint8Array | undefined)[]> {
    return keys.map((k) => this.#data.get(k));
  }

  async scan(start: string, end: string, limit = Infinity): Promise<[string, Uint8Array][]> {
    this.#sorted ??= [...this.#data.keys()].sort();
    const keys = this.#sorted;
    let lo = 0, hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (keys[mid]! < start) lo = mid + 1;
      else hi = mid;
    }
    const out: [string, Uint8Array][] = [];
    for (let i = lo; i < keys.length && out.length < limit; i++) {
      const k = keys[i]!;
      if (k >= end) break;
      out.push([k, this.#data.get(k)!]);
    }
    return out;
  }

  async apply(batch: Batch): Promise<void> {
    if (this.failNextApply) {
      const err = this.failNextApply;
      this.failNextApply = null;
      throw err;
    }
    this.applies++;
    for (const [k, v] of batch) {
      if (v === null) {
        if (this.#data.delete(k)) this.#sorted = null;
      } else {
        if (!this.#data.has(k)) this.#sorted = null;
        this.#data.set(k, v);
      }
    }
  }

  async close(): Promise<void> {}

  /** The total bytes held (keys as UTF-16 length, values as bytes): what a test compares with memory. */
  get size(): number {
    let n = 0;
    for (const [k, v] of this.#data) n += k.length + v.length;
    return n;
  }
}

/**
 * A read cache in front of a store, bounded by bytes, least recently used out first. Every read
 * of the trie and of documents goes through one of these, so the replica's resident set is the
 * cache's budget plus whatever the operation in progress holds, whatever the store's size.
 */
export class CachedStore implements KvStore {
  readonly #inner: KvStore;
  readonly #budget: number;
  // Map iteration order is insertion order, so re-inserting on a hit makes the first key the least
  // recently used one.
  readonly #cache = new Map<string, Uint8Array | null>();
  #bytes = 0;
  hits = 0;
  misses = 0;

  constructor(inner: KvStore, budgetBytes: number) {
    this.#inner = inner;
    this.#budget = budgetBytes;
  }

  get residentBytes(): number {
    return this.#bytes;
  }

  #cost(key: string, v: Uint8Array | null): number {
    return key.length * 2 + (v?.length ?? 0) + 48;
  }

  #remember(key: string, v: Uint8Array | null): void {
    const prev = this.#cache.get(key);
    if (prev !== undefined) {
      this.#bytes -= this.#cost(key, prev);
      this.#cache.delete(key);
    }
    const cost = this.#cost(key, v);
    if (cost > this.#budget / 4) return; // one entry never takes a large share of the cache
    this.#cache.set(key, v);
    this.#bytes += cost;
    for (const [k, old] of this.#cache) {
      if (this.#bytes <= this.#budget) break;
      this.#cache.delete(k);
      this.#bytes -= this.#cost(k, old);
    }
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    const hit = this.#cache.get(key);
    if (hit !== undefined) {
      this.hits++;
      this.#cache.delete(key);
      this.#cache.set(key, hit);
      return hit ?? undefined;
    }
    this.misses++;
    const v = await this.#inner.get(key);
    this.#remember(key, v ?? null);
    return v;
  }

  async getMany(keys: readonly string[]): Promise<(Uint8Array | undefined)[]> {
    return Promise.all(keys.map((k) => this.get(k)));
  }

  scan(start: string, end: string, limit?: number): Promise<[string, Uint8Array][]> {
    return this.#inner.scan(start, end, limit);
  }

  async apply(batch: Batch): Promise<void> {
    await this.#inner.apply(batch);
    for (const [k, v] of batch) this.#remember(k, v);
  }

  close(): Promise<void> {
    this.#cache.clear();
    this.#bytes = 0;
    return this.#inner.close();
  }
}
