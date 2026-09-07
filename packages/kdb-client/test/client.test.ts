/**
 * Client behaviour: the flows from Component 63 §5.3 and the contracts from §7, against the
 * in-memory FakeServer.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Client, backoffDelay } from "../src/client.ts";
import {
  KdbAbortedError,
  KdbClosedError,
  KdbConflictError,
  KdbError,
  KdbLockError,
  KdbNotFoundError,
  KdbPreconditionError,
  KdbUnauthenticatedError,
  KdbUnsupportedError,
} from "../src/errors.ts";
import { encodeFrame } from "../src/wire/frame.ts";
import { MSG, type TransactionDto } from "../src/wire/messages.ts";
import {
  FakeServer,
  HEAD_ONE,
  HEAD_ZERO,
  conflictReport,
  sessionAck,
  sqlOk,
  type Request,
} from "./fake-server.ts";

const NS = "app/users";
const DOC = "6f9619ff-8b86-d011-b42d-00cf4fc964ff";

function decodeTx(request: Request): TransactionDto {
  const bytes = Uint8Array.from(request.payload["transactionBytes"] as number[]);
  return JSON.parse(new TextDecoder().decode(bytes)) as TransactionDto;
}

/** The raw JSON a txCommit put on the wire, for assertions about key presence. */
function rawTxJson(request: Request): string {
  const bytes = Uint8Array.from(request.payload["transactionBytes"] as number[]);
  return new TextDecoder().decode(bytes);
}

describe("handshake", () => {
  it("sends [] and {} rather than null for namespaces and localHeads", async () => {
    // Kotlin declares both as non-nullable with no default, so a JSON null makes the JVM's
    // strict decoder throw and the connection dies with no response at all - not a clean
    // rejection, just silence. Go hit this and fixed it the same way.
    const server = new FakeServer(() => ({
      kind: "handshakeAck",
      payload: { accepted: true, negotiatedEncoding: "JSON", protocolVersion: 1, remoteHeads: {} },
    }));
    const client = new Client(server);
    await client.handshake();

    const handshake = server.requests[0]!;
    assert.deepEqual(handshake.payload["namespaces"], []);
    assert.deepEqual(handshake.payload["localHeads"], {});
    assert.equal(handshake.payload["clientMode"], "SQL_CLIENT");
    assert.equal(handshake.payload["protocolVersion"], 1);
  });

  it("advertises JSON only, since it implements no other payload codec", async () => {
    const server = new FakeServer(() => ({
      kind: "handshakeAck",
      payload: { accepted: true, negotiatedEncoding: "JSON", protocolVersion: 1, remoteHeads: {} },
    }));
    await new Client(server).handshake();
    assert.deepEqual(server.requests[0]!.payload["preferredEncodings"], ["JSON"]);
  });

  it("refuses a server that negotiated a codec this client cannot speak", async () => {
    const server = new FakeServer(() => ({
      kind: "handshakeAck",
      payload: {
        accepted: true,
        negotiatedEncoding: "KDB_BINARY",
        protocolVersion: 1,
        remoteHeads: {},
      },
    }));
    await assert.rejects(new Client(server).handshake(), /implements JSON only/);
  });

  it("throws KdbUnauthenticatedError with the server's reason when rejected", async () => {
    const server = new FakeServer(() => ({
      kind: "handshakeAck",
      payload: {
        accepted: false,
        negotiatedEncoding: "JSON",
        protocolVersion: 1,
        remoteHeads: {},
        rejectionReason: "bad token",
      },
    }));
    await assert.rejects(
      new Client(server).handshake(),
      (error: unknown) => error instanceof KdbUnauthenticatedError && /bad token/.test(String(error)),
    );
  });
});

describe("sessions", () => {
  it("opens one session per namespace and caches it", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(request.payload["namespace"] as string)
        : sqlOk(request.payload["namespace"] as string),
    );
    const client = new Client(server);

    await client.putJSON(NS, DOC, { n: 1 });
    await client.putJSON(NS, DOC, { n: 2 });

    const sessionBegins = server.requests.filter((r) => r.kind === "sessionBegin");
    assert.equal(sessionBegins.length, 1, "second write should reuse the cached session");
    assert.equal(sessionBegins[0]!.payload["readConsistency"], "READ_COMMITTED");
    assert.equal(sessionBegins[0]!.payload["sessionId"], null);
  });

  it("opens exactly one session when concurrent operations race on a cold namespace", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(request.payload["namespace"] as string)
        : sqlOk(request.payload["namespace"] as string),
    );
    const client = new Client(server);

    await Promise.all([
      client.putJSON(NS, DOC, { n: 1 }),
      client.putJSON(NS, DOC, { n: 2 }),
      client.putJSON(NS, DOC, { n: 3 }),
    ]);

    assert.equal(server.requests.filter((r) => r.kind === "sessionBegin").length, 1);
  });

  it("rejects an empty sessionId as unauthenticated, carrying the server's error", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? {
            kind: "sessionBeginAck",
            payload: {
              namespace: NS,
              sessionId: "",
              headHex: HEAD_ZERO,
              readConsistency: "READ_COMMITTED",
              error: "no read grant for app/users",
            },
          }
        : undefined,
    );
    await assert.rejects(
      new Client(server).putJSON(NS, DOC, { n: 1 }),
      (error: unknown) =>
        error instanceof KdbUnauthenticatedError && /no read grant/.test(String(error)),
    );
  });

  it("advances the cached head to the commit a write resolved to", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS, HEAD_ZERO) : sqlOk(NS, HEAD_ONE),
    );
    const client = new Client(server);

    await client.putJSON(NS, DOC, { n: 1 });
    await client.putJSON(NS, DOC, { n: 2 });

    const commits = server.requests.filter((r) => r.kind === "txCommit");
    assert.equal(decodeTx(commits[0]!).baseVersionHex, HEAD_ZERO);
    // Without this the second write would still anchor on HEAD_ZERO and conflict spuriously.
    assert.equal(decodeTx(commits[1]!).baseVersionHex, HEAD_ONE);
  });
});

describe("transaction encoding", () => {
  it("omits `preconditions` entirely when there are none", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS),
    );
    await new Client(server).putJSON(NS, DOC, { n: 1 });

    const commit = server.requests.find((r) => r.kind === "txCommit")!;
    // Not `"preconditions":[]` - the Go encoder tags it omitempty so a transaction without any
    // encodes exactly as it did before the field existed, which is what keeps an older peer
    // decoding it unchanged.
    assert.ok(!rawTxJson(commit).includes("preconditions"), rawTxJson(commit));
    assert.equal(decodeTx(commit).preconditions, undefined);
  });

  it("carries the document body as a JSON string inside the write op's patch", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS),
    );
    await new Client(server).putJSON(NS, DOC, { name: "ada", score: 42 });

    const tx = decodeTx(server.requests.find((r) => r.kind === "txCommit")!);
    assert.equal(tx.operations.length, 1);
    assert.equal(tx.operations[0]!.kind, "write");
    assert.equal(tx.operations[0]!.docId, DOC);
    assert.equal(typeof tx.operations[0]!.patch, "string");
    assert.deepEqual(JSON.parse(tx.operations[0]!.patch!), { name: "ada", score: 42 });
  });

  it("sends each conditional write's precondition kind as its enum constant name", async () => {
    const cases: Array<[string, (c: Client) => Promise<string>]> = [
      ["EXPECT_ABSENT", (c) => c.putIfAbsent(NS, DOC, { n: 1 })],
      ["EXPECT_PRESENT", (c) => c.replaceIfPresent(NS, DOC, { n: 1 })],
      ["EXPECT_CONTENT_HASH", (c) => c.replaceIf(NS, DOC, { n: 1 }, "ab".repeat(32))],
    ];

    for (const [kind, call] of cases) {
      const server = new FakeServer((request) =>
        request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS),
      );
      await call(new Client(server));

      const tx = decodeTx(server.requests.find((r) => r.kind === "txCommit")!);
      assert.deepEqual(
        tx.preconditions,
        kind === "EXPECT_CONTENT_HASH"
          ? [{ opIndex: 0, kind, contentHashHex: "ab".repeat(32) }]
          : [{ opIndex: 0, kind }],
      );
    }
  });

  it("sends a multi-document commit as one transaction", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS),
    );
    const other = "00000000-0000-0000-0000-000000000001";
    await new Client(server).commit({
      namespace: NS,
      baseVersion: HEAD_ZERO,
      writes: [
        { docId: DOC, body: { n: 1 } },
        { docId: other, body: { n: 2 } },
      ],
    });

    const commits = server.requests.filter((r) => r.kind === "txCommit");
    assert.equal(commits.length, 1, "all-or-nothing means one frame, not one per write");
    assert.equal(decodeTx(commits[0]!).operations.length, 2);
  });
});

describe("documents", () => {
  it("throws KdbNotFoundError when the server reports no document", async () => {
    const server = new FakeServer((request) =>
      request.kind === "documentGet"
        ? {
            kind: "documentGetResult",
            payload: { namespace: NS, docId: DOC, json: null, commitHex: HEAD_ZERO },
          }
        : undefined,
    );
    await assert.rejects(new Client(server).get(NS, DOC), KdbNotFoundError);
  });

  it("returns the parsed body and the commit it was read at", async () => {
    const server = new FakeServer((request) =>
      request.kind === "documentGet"
        ? {
            kind: "documentGetResult",
            payload: {
              namespace: NS,
              docId: DOC,
              json: '{"name":"ada"}',
              commitHex: HEAD_ONE,
            },
          }
        : undefined,
    );
    const result = await new Client(server).get(NS, DOC);
    assert.deepEqual(result.body, { name: "ada" });
    assert.equal(result.commit, HEAD_ONE);
  });

  it("always sends a sessionId on upsert, so a lease holder is distinguishable", async () => {
    // An empty value reads as "not the holder" server-side, so an upsert that omitted it would
    // be refused while any lease is out.
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : { kind: "upsertResult", payload: { namespace: NS, commitHex: HEAD_ONE } },
    );
    await new Client(server).upsert(NS, DOC, { n: 1 });

    const upsert = server.requests.find((r) => r.kind === "upsert")!;
    assert.equal(upsert.payload["sessionId"], "sess-1");
    assert.notEqual(upsert.payload["sessionId"], "");
  });

  it("rejects a non-UUID document id before sending anything", async () => {
    const server = new FakeServer(() => undefined);
    await assert.rejects(new Client(server).putJSON(NS, "nope", {}), /is not a UUID/);
    assert.equal(server.requests.length, 0, "should not reach the wire");
  });
});

describe("conflicts", () => {
  it("throws KdbConflictError with the report's details", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : conflictReport(NS, [{ documentId: DOC, operationType: "CONCURRENT_WRITE" }], 50),
    );

    await assert.rejects(
      new Client(server).putJSON(NS, DOC, { n: 1 }),
      (error: unknown) => {
        assert.ok(error instanceof KdbConflictError);
        assert.equal(error.conflicts.length, 1);
        assert.equal(error.conflicts[0]!.operationType, "CONCURRENT_WRITE");
        assert.equal(error.retryAfterMs, 50);
        assert.equal(error.code, "CONFLICT");
        return true;
      },
    );
  });

  it("reports PRECONDITION_FAILED as its own error even alongside ordinary conflicts", async () => {
    // The ordering rule from go/kdb/client/client.go:596-608: a failed assertion and a lost
    // race arrive on the same wire message but mean different things, and the assertion wins.
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : conflictReport(NS, [
            { documentId: DOC, operationType: "CONCURRENT_WRITE" },
            { documentId: DOC, operationType: "PRECONDITION_FAILED", actualContentHash: "ff".repeat(32) },
          ]),
    );

    await assert.rejects(
      new Client(server).replaceIf(NS, DOC, { n: 1 }, "ab".repeat(32)),
      (error: unknown) => {
        assert.ok(error instanceof KdbPreconditionError, `got ${error}`);
        assert.equal(error.actualHash, "ff".repeat(32));
        return true;
      },
    );
  });
});

describe("compareAndSwap", () => {
  it("seeds a missing document, passing null to the mutator", async () => {
    const server = new FakeServer((request) => {
      if (request.kind === "sessionBegin") return sessionAck(NS);
      if (request.kind === "documentGet") {
        return {
          kind: "documentGetResult",
          payload: { namespace: NS, docId: DOC, json: null, commitHex: HEAD_ZERO },
        };
      }
      return sqlOk(NS);
    });

    let seen: unknown = "unset";
    const commit = await new Client(server).compareAndSwap(NS, DOC, (current) => {
      seen = current;
      return { n: 1 };
    });

    assert.equal(seen, null);
    assert.equal(commit, HEAD_ONE);
    // A missing document means putIfAbsent, so EXPECT_ABSENT rather than a content hash.
    const tx = decodeTx(server.requests.find((r) => r.kind === "txCommit")!);
    assert.equal(tx.preconditions?.[0]?.kind, "EXPECT_ABSENT");
  });

  it("retries a precondition failure and re-reads before each attempt", async () => {
    let attempt = 0;
    const server = new FakeServer((request) => {
      if (request.kind === "sessionBegin") return sessionAck(NS);
      if (request.kind === "documentGet") {
        return {
          kind: "documentGetResult",
          payload: {
            namespace: NS,
            docId: DOC,
            json: JSON.stringify({ n: attempt }),
            commitHex: HEAD_ZERO,
          },
        };
      }
      attempt += 1;
      // Lose the first race, win the second.
      return attempt === 1
        ? conflictReport(NS, [{ documentId: DOC, operationType: "PRECONDITION_FAILED" }], 1)
        : sqlOk(NS);
    });

    const seen: unknown[] = [];
    const commit = await new Client(server).compareAndSwap(NS, DOC, (current) => {
      seen.push(current);
      return { n: (current as { n: number }).n + 1 };
    });

    assert.equal(commit, HEAD_ONE);
    assert.equal(seen.length, 2, "each attempt must re-read, not reuse the stale value");
    assert.deepEqual(seen, [{ n: 0 }, { n: 1 }]);
  });

  it("does not retry an error that will fail identically next time", async () => {
    let commits = 0;
    const server = new FakeServer((request) => {
      if (request.kind === "sessionBegin") return sessionAck(NS);
      if (request.kind === "documentGet") {
        return {
          kind: "documentGetResult",
          payload: { namespace: NS, docId: DOC, json: "{}", commitHex: HEAD_ZERO },
        };
      }
      commits += 1;
      return {
        kind: "sqlResult",
        payload: {
          namespace: NS,
          sessionId: "sess-1",
          columns: [],
          rows: [],
          rowsAffected: 0,
          resolvedCommitHex: "",
          readOnly: false,
          generatedIds: [],
          error: "unique constraint violated",
          errorCode: "UNIQUE_VIOLATION",
        },
      };
    });

    await assert.rejects(new Client(server).compareAndSwap(NS, DOC, () => ({ n: 1 })), KdbError);
    assert.equal(commits, 1, "a schema/unique violation must not burn the attempt budget");
  });

  it("aborts without writing when the mutator returns null", async () => {
    const server = new FakeServer((request) => {
      if (request.kind === "sessionBegin") return sessionAck(NS);
      if (request.kind === "documentGet") {
        return {
          kind: "documentGetResult",
          payload: { namespace: NS, docId: DOC, json: "{}", commitHex: HEAD_ZERO },
        };
      }
      return sqlOk(NS);
    });

    const client = new Client(server);
    await assert.rejects(client.compareAndSwap(NS, DOC, () => null), KdbAbortedError);
    assert.equal(server.requests.filter((r) => r.kind === "txCommit").length, 0);
  });

  it("gives up after the attempt budget", async () => {
    const server = new FakeServer((request) => {
      if (request.kind === "sessionBegin") return sessionAck(NS);
      if (request.kind === "documentGet") {
        return {
          kind: "documentGetResult",
          payload: { namespace: NS, docId: DOC, json: "{}", commitHex: HEAD_ZERO },
        };
      }
      return conflictReport(NS, [{ documentId: DOC, operationType: "CONCURRENT_WRITE" }], 1);
    });

    await assert.rejects(
      new Client(server).compareAndSwap(NS, DOC, () => ({ n: 1 }), { attempts: 2 }),
      /gave up after 2 attempts/,
    );
  });
});

describe("backoffDelay", () => {
  it("prefers the server's own retry hint", () => {
    const error = new KdbError("busy", { retryAfterMs: 37 });
    for (let i = 0; i < 5; i++) assert.equal(backoffDelay(i, error), 37);
  });

  it("draws full jitter from a capped exponential window", () => {
    const error = new KdbError("conflict");
    // attempt 0 -> [0, 2], attempt 3 -> [0, 16], far attempts -> [0, 250].
    for (let i = 0; i < 200; i++) {
      assert.ok(backoffDelay(0, error) <= 2);
      assert.ok(backoffDelay(3, error) <= 16);
      assert.ok(backoffDelay(20, error) <= 250);
    }
  });
});

describe("SQL", () => {
  it("maps rows onto column names, leaving values as the strings the wire carried", () => {
    // No coercion here by design: the schema lives in @kdb/codegen, and a driver that parsed
    // "0123" as 123 would corrupt data.
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : {
            kind: "sqlResult",
            payload: {
              namespace: NS,
              sessionId: "sess-1",
              columns: ["id", "score"],
              rows: [
                ["u1", "0123"],
                ["u2", "7"],
              ],
              rowsAffected: 0,
              resolvedCommitHex: HEAD_ZERO,
              readOnly: true,
              generatedIds: [],
            },
          },
    );

    return new Client(server).query(NS, "SELECT id, score FROM users").then((rows) => {
      assert.deepEqual(rows, [
        { id: "u1", score: "0123" },
        { id: "u2", score: "7" },
      ]);
    });
  });

  it("sends parameters as a JSON array, and null when there are none", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS, HEAD_ZERO, true),
    );
    const client = new Client(server);

    await client.query(NS, "SELECT 1", ["u1", 7]);
    await client.query(NS, "SELECT 1");

    const execs = server.requests.filter((r) => r.kind === "sqlExec");
    assert.equal(execs[0]!.payload["parametersJson"], '["u1",7]');
    assert.equal(execs[1]!.payload["parametersJson"], null);
  });

  it("auto-commits a write statement, and does not commit a read", async () => {
    const server = new FakeServer((request) => {
      if (request.kind === "sessionBegin") return sessionAck(NS);
      if (request.kind === "sqlExec") {
        const sql = request.payload["sql"] as string;
        return sqlOk(NS, HEAD_ONE, /^SELECT/i.test(sql));
      }
      return sqlOk(NS, HEAD_ONE);
    });
    const client = new Client(server);

    await client.exec(NS, "INSERT INTO users VALUES (1)");
    assert.equal(
      server.requests.filter((r) => r.kind === "txCommit").length,
      1,
      "a write left uncommitted is the bug that passes unit tests and loses data in production",
    );

    await client.exec(NS, "SELECT 1");
    assert.equal(server.requests.filter((r) => r.kind === "txCommit").length, 1);
  });

  it("surfaces a server error with its classification", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : {
            kind: "sqlResult",
            payload: {
              namespace: NS,
              sessionId: "sess-1",
              columns: [],
              rows: [],
              rowsAffected: 0,
              resolvedCommitHex: "",
              readOnly: true,
              generatedIds: [],
              error: "server busy",
              errorCode: "BUSY",
              retryAfterMs: 25,
            },
          },
    );

    await assert.rejects(new Client(server).query(NS, "SELECT 1"), (error: unknown) => {
      assert.ok(error instanceof KdbError);
      assert.equal(error.code, "BUSY");
      assert.equal(error.retryAfterMs, 25);
      return true;
    });
  });
});

describe("leases", () => {
  it("returns the fence and expiry from a granted lease", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : {
            kind: "lockResult",
            payload: {
              namespace: NS,
              sessionId: "sess-1",
              docId: DOC,
              granted: true,
              fence: 7,
              expiresAtMillis: 1750000000000,
            },
          },
    );

    const lease = await new Client(server).acquireLock(NS, DOC, 30_000);
    assert.equal(lease.fence, 7);
    assert.equal(lease.expiresAtMs, 1750000000000);
    assert.equal(server.requests.find((r) => r.kind === "lockAcquire")!.payload["ttlMillis"], 30000);
  });

  it("asks for the server's default TTL rather than an unbounded lease", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : {
            kind: "lockResult",
            payload: {
              namespace: NS,
              sessionId: "sess-1",
              docId: DOC,
              granted: true,
              fence: 1,
              expiresAtMillis: 0,
            },
          },
    );
    await new Client(server).acquireLock(NS, DOC);
    assert.equal(server.requests.find((r) => r.kind === "lockAcquire")!.payload["ttlMillis"], 0);
  });

  it("throws KdbLockError naming the holder when the lease is refused", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin"
        ? sessionAck(NS)
        : {
            kind: "lockResult",
            payload: {
              namespace: NS,
              sessionId: "sess-1",
              docId: DOC,
              granted: false,
              fence: 0,
              expiresAtMillis: 0,
              holderSessionId: "sess-other",
              error: "held by another session",
            },
          },
    );

    await assert.rejects(new Client(server).acquireLock(NS, DOC), (error: unknown) => {
      assert.ok(error instanceof KdbLockError);
      assert.equal(error.holderSessionId, "sess-other");
      return true;
    });
  });
});

describe("deadlines and cancellation", () => {
  it("turns a silent server into KdbUnsupportedError for a Go-only operation", async () => {
    // Some server paths answer an unrecognized message with nothing at all rather than an error
    // frame (finish-up plan 4.H), which is how the JVM server behaves for 0x14-0x1C. Without a
    // deadline the caller hangs forever.
    const server = new FakeServer(() => undefined);
    await assert.rejects(
      new Client(server, { requestTimeoutMs: 20 }).get(NS, DOC),
      (error: unknown) => {
        assert.ok(error instanceof KdbUnsupportedError, `got ${error}`);
        assert.equal(error.operation, "get");
        return true;
      },
    );
  });

  it("times out an ordinary silent request with DEADLINE_EXCEEDED", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : undefined,
    );
    await assert.rejects(
      new Client(server, { requestTimeoutMs: 20 }).query(NS, "SELECT 1"),
      (error: unknown) => {
        assert.ok(error instanceof KdbError);
        assert.equal(error.code, "DEADLINE_EXCEEDED");
        return true;
      },
    );
  });

  it("rejects promptly when the caller's signal aborts", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : undefined,
    );
    const controller = new AbortController();
    const pending = new Client(server).query(NS, "SELECT 1", [], { signal: controller.signal });
    controller.abort();
    await assert.rejects(pending, KdbAbortedError);
  });

  it("fails every in-flight request when the connection drops", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS, HEAD_ONE, true),
    );
    const client = new Client(server);
    // Warm the session first, so the request left in flight below is a bare sqlExec.
    await client.query(NS, "SELECT 1");

    server.respondWith(() => undefined);
    const pending = client.query(NS, "SELECT 1");
    server.drop(new KdbClosedError("kdb: socket closed by peer"));

    // Rejects on the drop, not by waiting out its own deadline - a caller should not be left
    // hanging on a dead socket until an unrelated timeout fires.
    await assert.rejects(pending, /socket closed by peer/);
  });

  it("rejects a new request made after the connection has closed", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS, HEAD_ONE, true),
    );
    const client = new Client(server);
    await client.query(NS, "SELECT 1");

    server.drop(new KdbClosedError("kdb: socket closed by peer"));
    await assert.rejects(client.query(NS, "SELECT 1"), /socket closed by peer/);
  });
});

describe("frame dispatch", () => {
  it("ignores a peer-sync broadcast rather than mismatching it to a pending request", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS, HEAD_ONE, true),
    );
    const client = new Client(server);
    await client.query(NS, "SELECT 1");

    // A stream/DAG message arriving on the same connection must not be mistaken for a reply.
    // Correlation id 1 is deliberately one the client has already used.
    server.deliver(
      encodeFrame(MSG.DELTA_COMMIT, 1, {
        kind: "deltaCommit",
        deltaCommit: { namespace: NS, commitHashHex: HEAD_ONE },
      }),
    );

    const rows = await client.query(NS, "SELECT 1");
    assert.deepEqual(rows, []);
  });

  it("ignores an undecodable frame rather than tearing down a live connection", async () => {
    const server = new FakeServer((request) =>
      request.kind === "sessionBegin" ? sessionAck(NS) : sqlOk(NS, HEAD_ONE, true),
    );
    const client = new Client(server);
    await client.query(NS, "SELECT 1");

    const garbage = new Uint8Array(24);
    new DataView(garbage.buffer).setInt32(0, 24, true);
    assert.doesNotThrow(() => server.deliver(garbage));

    assert.deepEqual(await client.query(NS, "SELECT 1"), []);
  });
});
