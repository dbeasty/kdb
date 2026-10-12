/**
 * @kdb/codegen's generated types and SQL builder, running on the embedded engine.
 *
 * The suite is the same one that runs against the Go server over a WebSocket
 * (packages/kdb-codegen/test/e2e.test.ts): identical assertions, two ways of reaching one engine.
 */

import { typedLayerSuite } from "../../kdb-codegen/test/suite.ts";
import { openEmbedded } from "../src/index.ts";

typedLayerSuite("the embedded WASM engine", async () => {
  const db = await openEmbedded({ namespace: "app/users" });
  return { ops: db, close: () => db.close() };
});
