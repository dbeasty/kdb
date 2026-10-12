/** The same typed-layer suite as typed.test.ts, on the persistent engine: durability must not change behaviour. */

import { typedLayerSuite } from "../../kdb-codegen/test/suite.ts";
import { MemoryBackend, openEmbedded } from "../src/index.ts";

typedLayerSuite("the persistent embedded WASM engine", async () => {
  const db = await openEmbedded({ namespace: "app/users", persist: { name: "typed", backend: new MemoryBackend() } });
  return { ops: db, close: () => db.close() };
});
