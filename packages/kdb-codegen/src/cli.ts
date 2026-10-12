#!/usr/bin/env node
/**
 * kdb-codegen --schema kdb.schema.json --out src/kdb.ts [--namespace app/users] [--check]
 *
 * `--check` regenerates in memory and exits non-zero if the file on disk differs: the CI gate
 * that makes schema drift a build failure rather than a runtime write rejection (§10.2).
 */

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname, basename } from "node:path";
import { parseArgs } from "node:util";
import { generate } from "./emit.ts";
import { parseSchemaFile, SchemaError } from "./schema.ts";

const USAGE = `usage: kdb-codegen --schema <file.json> --out <file.ts> [--namespace <ns>] [--check]

  --schema     schema JSON: { "namespaces": { "<ns>": { "fields": [...] } } } or a bare { "fields": [...] }
  --out        generated TypeScript file
  --namespace  namespace a bare { "fields": [...] } schema describes
  --check      do not write; exit 1 if --out is missing or stale`;

async function main(): Promise<number> {
  const { values } = parseArgs({
    options: {
      schema: { type: "string" },
      out: { type: "string" },
      namespace: { type: "string" },
      check: { type: "boolean", default: false },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  if (values.help) {
    console.log(USAGE);
    return 0;
  }
  if (values.schema === undefined || values.out === undefined) {
    console.error(USAGE);
    return 2;
  }

  let raw: unknown;
  try {
    raw = JSON.parse(await readFile(values.schema, "utf8"));
  } catch (error) {
    console.error(`kdb-codegen: cannot read ${values.schema}: ${(error as Error).message}`);
    return 2;
  }
  const output = generate(parseSchemaFile(raw, values.namespace), { source: basename(values.schema) });

  if (values.check) {
    let current: string | undefined;
    try {
      current = await readFile(values.out, "utf8");
    } catch {
      current = undefined;
    }
    if (current !== output) {
      console.error(`kdb-codegen: ${values.out} is ${current === undefined ? "missing" : "stale"}; run kdb-codegen to regenerate it`);
      return 1;
    }
    return 0;
  }
  await mkdir(dirname(values.out), { recursive: true });
  await writeFile(values.out, output);
  return 0;
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error instanceof SchemaError ? error.message : error);
    process.exit(error instanceof SchemaError ? 2 : 1);
  },
);
