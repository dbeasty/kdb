/**
 * The package version, which is also the server release it speaks to.
 *
 * `@kdb/client` is published once per KDB release with the same version number as the server
 * (the repo's VERSION file); `test/version.test.ts` and the release workflow both fail if
 * package.json drifts from it. The client trusts that pairing only as far as the handshake
 * confirms it: see `checkServerCompatible`.
 */
export const CLIENT_VERSION = "0.9.0";

interface Parsed {
  major: number;
  minor: number;
  dev: boolean;
}

function parse(version: string): Parsed | undefined {
  const m = /^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+.*)?$/.exec(version.trim());
  if (!m) return undefined;
  return { major: Number(m[1]), minor: Number(m[2]), dev: m[4] === "dev" };
}

/**
 * Returns a problem description when a server reporting `serverVersion` cannot be spoken to by
 * this client, or `undefined` when it can.
 *
 * Releases are compatible when they share a major version and, while the major is 0, a minor
 * version: before 1.0 a minor release may change the wire format. A from-source server build
 * ("0.0.0-dev") is accepted, since it has no release to compare against. A server that reports
 * no version at all predates this check and cannot be vouched for.
 */
export function checkServerCompatible(
  serverVersion: string | undefined | null,
  clientVersion: string = CLIENT_VERSION,
): string | undefined {
  if (!serverVersion) {
    return (
      `the server did not report a version, so @kdb/client ${clientVersion} cannot confirm it ` +
      "speaks this wire format (a server older than 0.7.0, or a non-Go engine)"
    );
  }
  const server = parse(serverVersion);
  const client = parse(clientVersion);
  if (!server) return `the server reported an unrecognised version ${JSON.stringify(serverVersion)}`;
  if (!client) return `this client has an unrecognised version ${JSON.stringify(clientVersion)}`;
  if (server.dev) return undefined;
  if (server.major !== client.major || (client.major === 0 && server.minor !== client.minor)) {
    return (
      `@kdb/client ${clientVersion} is not compatible with KDB server ${serverVersion}; ` +
      `install the @kdb/client release that matches the server`
    );
  }
  return undefined;
}
