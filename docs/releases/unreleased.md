# Unreleased

## Changed

- **The TypeScript packages are renamed into the `@limidus` npm scope.** `@kdb/client` is now
  `@limidus/kdb-client`, and likewise `@limidus/kdb-codegen`, `@limidus/kdb-embed` and
  `@limidus/kdb-replica`. The npm name `kdb` belongs to someone else, so `@kdb/client` could never
  have been published. Update imports (`from "@kdb/client"` → `from "@limidus/kdb-client"`,
  `"@kdb/client/tcp"` → `"@limidus/kdb-client/tcp"`) and the dependency name. The release tarball
  is renamed with it: `kdb-client-<version>.tgz` becomes `limidus-kdb-client-<version>.tgz`, so an
  install by URL needs the new file name.
- **`@limidus/kdb-client` is published to the npm registry** from the release workflow through npm
  trusted publishing - no npm token is stored. See `docs/kdb-release-plan.md` §4 for the one-time
  setup.
