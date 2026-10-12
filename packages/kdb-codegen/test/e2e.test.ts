/**
 * The generated typed layer against a real Go server, over a real WebSocket.
 * Skipped without a Go toolchain, like @kdb/client's own interop test.
 */

import { connect } from "@kdb/client";
import { goAvailable, startGoServer } from "./server.ts";
import { typedLayerSuite } from "./suite.ts";

typedLayerSuite(
  "the Go server",
  async () => {
    const server = await startGoServer("app/users");
    const client = await connect(server.uri);
    return {
      ops: client,
      close: async () => {
        await client.close();
        await server.stop();
      },
    };
  },
  { skip: goAvailable() ? false : "go toolchain not found" },
);
