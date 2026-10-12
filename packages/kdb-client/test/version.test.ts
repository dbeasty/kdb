import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";

import { Client } from "../src/client.ts";
import { KdbProtocolError } from "../src/errors.ts";
import { CLIENT_VERSION, checkServerCompatible } from "../src/version.ts";
import { FakeServer } from "./fake-server.ts";

const ack = (serverVersion?: string) => ({
  kind: "handshakeAck" as const,
  payload: {
    accepted: true,
    negotiatedEncoding: "JSON",
    protocolVersion: 1,
    remoteHeads: {},
    ...(serverVersion === undefined ? {} : { serverVersion }),
  },
});

describe("release pairing", () => {
  it("is published at the version of the repo's VERSION file", () => {
    const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
    const repoVersion = readFileSync(new URL("../../../VERSION", import.meta.url), "utf8").trim();
    assert.equal(pkg.version, repoVersion);
    assert.equal(CLIENT_VERSION, repoVersion);
  });

  it("accepts the same release, a patch of it, and a from-source server", () => {
    assert.equal(checkServerCompatible("0.7.0", "0.7.0"), undefined);
    assert.equal(checkServerCompatible("0.7.9", "0.7.0"), undefined);
    assert.equal(checkServerCompatible("0.0.0-dev", "0.7.0"), undefined);
  });

  it("refuses another minor while the major is 0, and another major after", () => {
    assert.match(checkServerCompatible("0.6.2", "0.7.0") ?? "", /not compatible/);
    assert.match(checkServerCompatible("0.8.0", "0.7.0") ?? "", /not compatible/);
    assert.match(checkServerCompatible("2.0.0", "1.4.0") ?? "", /not compatible/);
    assert.equal(checkServerCompatible("1.5.0", "1.4.0"), undefined);
  });

  it("refuses a server that reports no version", () => {
    assert.match(checkServerCompatible(undefined) ?? "", /did not report a version/);
  });
});

describe("handshake version check", () => {
  it("fails the handshake against an incompatible server", async () => {
    const client = new Client(new FakeServer(() => ack("0.6.2")));
    await assert.rejects(client.handshake(), (e: unknown) => e instanceof KdbProtocolError);
  });

  it("fails against a server that predates version reporting", async () => {
    await assert.rejects(new Client(new FakeServer(() => ack())).handshake(), /did not report a version/);
  });

  it("passes against the matching release, and can be skipped", async () => {
    await new Client(new FakeServer(() => ack(CLIENT_VERSION))).handshake();
    await new Client(new FakeServer(() => ack("0.6.2"))).handshake({ skipVersionCheck: true });
  });
});
