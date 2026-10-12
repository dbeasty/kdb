# Windows compatibility

Status as of 2026-10-12. The Go module (`go/`) already cross-compiled for `GOOS=windows`. It is the
deploy target. It still behaved wrongly at runtime in the places listed below. This branch fixes
those places and adds a `windows-latest` CI job to keep them fixed. Kotlin has no Windows changes
yet; its audit is under [Kotlin](#kotlin-not-changed-on-this-branch).

## What was wrong in Go, and what changed

| Problem on Windows | Effect | Fix |
|---|---|---|
| `os.OpenFile` opens without `FILE_SHARE_DELETE` | While any handle is open, a segment can't be deleted or renamed. One example is an uncached reader racing `OSByteStore.Delete`. Compaction and retention fail with a sharing violation. | `kdb/fsx.OpenFile`/`Open` add `FILE_SHARE_DELETE`. `OSByteStore` opens every segment through them. |
| Scanners and the indexer briefly hold new files | `os.Rename`/`os.Remove` fail intermittently with `ERROR_SHARING_VIOLATION`/`ACCESS_DENIED` | `fsx.Rename`/`Remove`/`RemoveAll` retry those errors for up to 2 s. Every data-path rename and remove now goes through them. |
| A directory can't be fsynced (`FlushFileBuffers` is refused) | `syncDir` returned an error, so every cross-namespace transaction (`txn_coordinator`), node-identity write and backup txn write **failed** | `fsx.SyncDir` does nothing on Windows. `fsx.Rename` uses `MOVEFILE_WRITE_THROUGH`, and that is where the durability comes from. |
| No `flock(2)` | The fallback was an `O_EXCL` lock file. It isn't released if the process crashes, so the directory stays locked until someone deletes the file by hand. It also has no shared mode, so read-only runtimes were refused. | `LockFileEx`, shared or exclusive, on one byte past end of file. The kernel releases it when the process dies. The holder hint stays readable because Windows locks are mandatory and the locked byte is outside the hint. `dir_lock.go` is now shared by unix and windows. |
| No free-space reading | `AvailableBytes` returned an error, so promotion could not check whether its copy would fit | `GetDiskFreeSpaceEx` |
| No total-memory reading | `PercentOfAvailable` hot-tier sizing fell back to the default | `GlobalMemoryStatusEx` |
| No filesystem identity | `recovery.SameFilesystem` reported "unavailable" | Volume serial number via `GetVolumePathName` + `GetVolumeInformation`. `deviceID` now takes a path. |
| Namespace ids `con`, `nul.x`, `com1`, … and segments ending in `.` | Device names resolve to devices. Win32 strips a trailing dot, so `a.` and `a` would share one directory. | `ValidateNamespaceID` refuses them, **on Windows only**, because they were always valid elsewhere |
| `OSByteStore.Flush` (no cached handle) fsynced through a read-only handle | `FlushFileBuffers` needs write access, so the fsync failed | Opens read-write, and falls back to read-only only if it lacks write permission |
| `kdb unlock` built its path with `"/"` | Cosmetic | `filepath.Join` |

Supporting changes:
- `golang.org/x/sys` is now a direct dependency at v0.47.0. It was already in the module graph via `x/crypto`, so no version moved.
- `.gitattributes` forces LF on checkout. Otherwise a Windows checkout (where `core.autocrlf` is on by default on runners) rewrites golden fixtures and breaks the gofmt check.
- CI has a new `go-windows` job (`windows-latest`, `go build` + `go test ./...`, no `-race`). The Linux `go-test` job also runs `GOOS=windows go vet ./...`.
- New tests: `kdb/fsx/fsx_test.go` covers removing or renaming a file while it is open, rename-replace, and the flag mapping. `TestWindowsUnsafeSegment` covers the namespace names. `embed/readonly_test.go` (shared/exclusive lock relationships) now runs on Windows too.

## Not verified yet

Nothing here has run on Windows yet. The Windows code cross-compiles and vets, and the unchanged unix paths pass the full suite on macOS. Expect the first `go-windows` CI run to fail in some **tests** that assume unix, such as tests that shell out to `sh` or check permission bits. Those should be fixed or skipped one at a time, and each one checked to confirm it is really a test assumption and not a product bug.

## Known gaps (Go), not done here

- **Case-insensitive directories.** On NTFS, and on default APFS, namespaces `App/x` and `app/x` share one directory. This was already true on macOS. Fixing it means deciding whether ids are case-folded, which changes behaviour on Linux.
- **Release artifacts.** `RELEASE_PLATFORMS` in the `Makefile` has no `windows/amd64`. Adding it needs the `.exe` suffix and an update to the release-plan artifact counts.
- **Windows service.** `kdb-service` runs as a console process and handles Ctrl+C (`os.Interrupt`). Running it under the Service Control Manager needs `x/sys/windows/svc`.
- **Durability tests.** `syncMode=fast` is plain `FlushFileBuffers` on Windows, which is the same as full sync. Preallocation zero-fills. Neither has been benchmarked.
- **Dev tooling.** `Makefile` and `scripts/*.sh` are bash-only. Use WSL or Git Bash.

## Kotlin (not changed on this branch)

These come from an audit. Line references are to `main` at 0f9e3ed.

High:
- `JvmSegmentByteStore` opens segments with `RandomAccessFile(..,"rw")`, which does not allow delete-sharing. It also never closes its cached channels, because `SegmentByteStore` has no `close()`. Its deletes ignore the result of `File.delete()`, so on Windows a delete of an open segment fails silently and the file leaks. Fix: open with `FileChannel.open`, add `close()`, and use `Files.deleteIfExists`.
- `kdb-jdbc` `DataDirectoryLock` locks the same bytes that hold the holder info. On Windows the lock is mandatory, so another process can't read that info. `releaseStaleLock` then decides the lock is stale and tries to delete a file that is held.
- No namespace-id validation exists in Kotlin, even though comments claim `[A-Za-z0-9._-]`. On Windows `:` creates an NTFS alternate data stream, `<>|?*"` throw, and device names and trailing dots behave as described above.

Medium:
- `KdbJdbcUrl` turns `jdbc:kdb:file:C:/…` into a URI whose authority is `C:`.
- `FileIndexBlobPointers` uses `ATOMIC_MOVE` with no retry.
- `writeSnapshot` truncates the file in place: not atomic and no fsync.

Native:
- No `mingwX64` target is declared.
- Adding one needs three `actual` implementations: `PlatformRandom` (reads `/dev/urandom`), `MemoryBudget` (uses `sysconf`) and the `TMPDIR` fallback.
