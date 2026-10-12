/**
 * One claimant for the two-process claim test in interop.test.ts. Connects, waits until the
 * agreed instant so both processes fire together, tries to replace the document under the
 * content hash it was told, and prints "won" or "lost <reason>".
 *
 *   node claim-worker.ts <uri> <namespace> <docId> <contentHash> <name> <startAtEpochMs>
 */
import { connect } from "../src/index.ts";
import { KdbConflictError, KdbPreconditionError } from "../src/errors.ts";

const [uri, ns, docId, hash, name, startAt] = process.argv.slice(2) as [
  string, string, string, string, string, string,
];

const client = await connect(uri, { requestTimeoutMs: 15_000 });
// Warm the namespace session so the race is on the commit, not on session setup.
await client.getWithHash(ns, docId);
const wait = Number(startAt) - Date.now();
if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
try {
  await client.replaceIf(ns, docId, { owner: name }, hash);
  console.log("won");
} catch (error) {
  if (error instanceof KdbPreconditionError || error instanceof KdbConflictError) {
    console.log("lost");
  } else {
    console.log(`error ${String(error)}`);
  }
} finally {
  await client.close();
}
