/**
 * Serializes use of one namespace's server-side session.
 *
 * A session carries one pending transaction. `exec` builds it with a `sqlExec` and finishes it
 * with a `txCommit` that carries no bytes, and a byte-carrying `txCommit` (putJSON, commit,
 * the conditional writes) clears whatever is pending as it lands. Two `exec`s interleaving
 * therefore fail with "no pending transaction to commit" - the first commit drains the pending
 * builder the second one's delete was added to - even though both deletes were applied.
 *
 * The lock is read/write: a DML `exec` is exclusive (its two messages must reach the server
 * back to back with nothing on the session in between), while byte-carrying commits are shared,
 * because each is a single self-contained request and the server already orders writes. That
 * keeps putJSON pipelined while still never letting one land inside an `exec`.
 */
export class SessionLock {
  #readers = 0;
  #writer = false;
  readonly #queue: Array<{ exclusive: boolean; grant: () => void }> = [];

  async exclusive<T>(run: () => Promise<T>): Promise<T> {
    await this.#acquire(true);
    try {
      return await run();
    } finally {
      this.#release(true);
    }
  }

  async shared<T>(run: () => Promise<T>): Promise<T> {
    await this.#acquire(false);
    try {
      return await run();
    } finally {
      this.#release(false);
    }
  }

  #acquire(exclusive: boolean): Promise<void> {
    // FIFO: anyone already waiting goes first, so a stream of shared holders cannot starve an
    // exclusive waiter.
    if (this.#queue.length === 0 && this.#canGrant(exclusive)) {
      this.#take(exclusive);
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.#queue.push({
        exclusive,
        grant: () => {
          this.#take(exclusive);
          resolve();
        },
      });
    });
  }

  #canGrant(exclusive: boolean): boolean {
    return exclusive ? !this.#writer && this.#readers === 0 : !this.#writer;
  }

  #take(exclusive: boolean): void {
    if (exclusive) this.#writer = true;
    else this.#readers++;
  }

  #release(exclusive: boolean): void {
    if (exclusive) this.#writer = false;
    else this.#readers--;
    while (this.#queue.length > 0) {
      const next = this.#queue[0]!;
      if (!this.#canGrant(next.exclusive)) break;
      this.#queue.shift();
      next.grant();
    }
  }
}
