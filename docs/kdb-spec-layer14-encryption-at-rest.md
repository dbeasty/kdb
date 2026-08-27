# KDB Component Spec — Layer 14
## Encryption at Rest, User-Held Keys, and the Zero-Knowledge Sync Hub
### Components 53–57

**File:** `kdb-spec-layer14-encryption-at-rest.md`
**Layer:** 14 — Data Encryption
**Status:** Design — for review
**Modules:** `:kdb-compression` (crypto shim), new `:kdb-crypto`, `:kdb-storage`, `:kdb-storage-io`, `:kdb-storage-wal`, `:kdb-storage-sstable`, `:kdb-storage-delta`, `:kdb-embed`, `:kdb-peer-sync`, `:kdb-cli` + Go twins (`go/kdb/{storage,embed,peersync}`, new `go/kdb/crypto`, new `go/cmd/kdb-key`)
**Depends on:** Layer 4a (WAL, SSTable, delta writer, platform IO), Layer 8 (peer sync), Layer 12 (embed runtimes), **Layer 13 Component 47 (replay-order fix — hard prerequisite, see §2.9)**
**Surveyed against:** working tree at `984d429` (2026-08-25)

-----

## 1. Purpose

Two related but distinct guarantees, delivered as two independent layers that compose:

1. **Encryption at rest (local).** Every durable byte a KDB node writes — WAL records, delta
   segments, SSTables, snapshots, ice bundles, and their S3 replicas — is ciphertext. An attacker
   with the disk (or the S3 bucket) learns only metadata (§9). This is Component 55, keyed by
   Components 53–54.

2. **End-to-end encryption with a user-held key.** Document payloads are encrypted *by the
   client, under a key the user physically holds on a FIDO2 USB security key*, before they ever
   leave the client. The server — including a fully compromised server, its disks, and its
   S3 bucket — never possesses plaintext or key material. The server operates as a
   **zero-knowledge sync hub**: it orders, deduplicates, replicates, and merges commits using
   only metadata. This is Components 56–57.

The master spec already lists this as open question #1
(`docs/kdb-spec.md:1208` — *"Encryption at rest — AES-256 per namespace; browser key management
is non-trivial"*), and the platform-IO spec parks it as a named future layer
(`docs/kdb-spec-layer4a-component10g-platform-io-shim.md:255`). This document resolves that
open question.

**The "second instance" model.** The intuitive design — run a second, plaintext KDB instance on
the client and encrypt between the two — is the right *user-facing* shape, but running two
literal instances with separate commit DAGs does not survive contact with the commit model:
`PutCommit` recomputes `SHA-256(canonical payload)` on receipt and rejects mismatches
(`go/kdb/dag/in_memory_commit_dag.go:154-190`), so a plaintext commit and its encrypted twin are
*different commits with different hashes*, and the two instances could never agree on heads or
ancestry. §6.1 shows the resolution: **commits are ciphertext-native everywhere** — one hash
space, identical commit bytes on client, server, and S3 — and the client vault decrypts at the
*materialization* boundary, so the client's realized store, indexes, and SQL run over plaintext
while its DAG and delta log stay byte-identical with the server's. The user's mental model
("my machine has the decrypted database, the server has the locked one") is preserved exactly;
only the plumbing differs.

-----

## 2. Findings

Surveyed against the code, with file/line references. Findings marked **BUG** are pre-existing
defects that gate this layer; **FACT** findings are load-bearing constraints the design must
honor; **LEAK** findings are places plaintext or metadata escapes today.

### 2.1 FACT: there is exactly one byte-level seam per port, and it already supports decorators

All durable I/O flows through `SegmentByteStore` — ten methods, identical names in both ports
(`kdb-storage-io/src/commonMain/kotlin/dev/kdb/storage/io/FileBackedPlatformIoShimBase.kt:133-144`,
`go/kdb/storage/io/segment_store.go:4-15`). Go already proves decoration works there:
`PrimaryWithReplicas` *is* a `SegmentByteStore` wrapping another
(`go/kdb/storage/io/primary_replicas.go:22-27`). No mmap anywhere; every read is
`(segmentName, offset, length)` whole-buffer positional I/O, and every frame/block is read whole
with its length taken from an on-disk header — so per-frame AEAD with a fixed size overhead is
feasible without offset virtualization, provided the stored length fields count ciphertext.

However, the byte store sees an *opaque append stream with no record boundaries*, while the S3
replica sink uploads on-disk bytes verbatim (`primary_replicas.go:41-52`,
`go/kdb/storage/io/s3/replica_sink.go:41-55`). Consequence: **frame-level encryption belongs in
the codecs** (`DeltaPageCodec.frame/parse`, `SsTableCodec.encodeBlock/decodeBlock`,
`WalCodec.encodeRecord/decodeRecords`) — one symmetric function pair per format — and the S3
replica is then covered automatically because it ships the already-encrypted disk bytes.
Whole-blob artifacts (snapshots, ice bundles) are read/written in one piece and are encrypted at
the byte-store decorator instead (§5.4).

### 2.2 FACT: no format has an algorithm/version field; the codec is not recorded on disk

The delta `KDBP` frame header (16 bytes: magic, compressedSize, uncompressedSize, CRC —
`kdb-storage-delta/.../DefaultDeltaSegmentWriter.kt:83-115`, `go/kdb/storage/delta/page_codec.go:11-45`),
the WAL `KDBW` record, and the SSTable block header have **no spare or reserved fields**. The
compression codec is not recorded either: delta reads take it from live config
(`writer.go:227`, `DefaultDeltaSegmentWriter.kt:223`) and SSTable infers it from
`compSize != uncompSize` (`SsTableCodec.kt:28`). Encryption therefore requires an explicit
format-versioning mechanism; the only one available without breaking old files is **a new magic
number per format**, with readers dispatching on magic (§5.1). The per-segment persistence of
`compressionCodec` into the segment descriptor
(`DefaultDeltaSegmentWriter.kt:77,239,249`) is the precedent to extend with a key-id.

### 2.3 FACT: ciphertext must be byte-stable and encrypted exactly once, client-side

`PutCommit` re-encodes and re-hashes every received commit; `DocumentTree` content hashes are
SHA-256 over `DocumentBody{id, json}` (`go/kdb/document/kdb_document.go:106-114`). Any
re-encryption, nonce change, or non-canonical re-serialization in transit breaks the hash.
The E2EE envelope must be produced once at the writing client with deterministic serialization,
and carried verbatim forever after.

### 2.4 FACT: the write path forces a JSON-object envelope, and root-level writes are shallow merges

`validateObjectJSON` rejects any document whose root is not a JSON object
(`go/kdb/document/kdb_document.go:95-104`), and `runSchemaPhase` runs it on every local write
(`go/kdb/transaction/default_engine.go:370-417`). Furthermore, writing to an existing document
performs a *shallow root-level key merge*, not a replace (`kdb_document.go:28-41`, applied at
`default_engine.go:388`) — stale keys survive. The ciphertext envelope must therefore be a JSON
object that carries every field it wants to control on every write (§6.2).

### 2.5 FACT: peer sync and conflict detection never look inside payloads

The sync unit is whole commits; the host inspects only hashes, parents, namespace, timestamps,
transaction IDs, op *kinds*, and doc IDs (`go/kdb/peersync/conflict_detection.go`, explicit
design note at lines 236-238). Divergence resolution's non-conflicting merge stages raw patch
bytes without parsing (`conflict_detection.go:198`). The encryptable surface is exactly
`OpWrite.patch`, `OpSchemaMigration.migrationPayload`, and blob bytes behind
`OpFileWrite.blobHash`. A server with `materializeCommit = nil`, `KdbSchema.NONE`, and no index
registry is already a payload-blind relay — `kdb-service` runs schema NONE today
(`kdb-service/.../KdbServiceMain.kt:279`).

### 2.6 LEAK: five places plaintext or metadata escapes that encryption must close

1. **Debug sidecar** appends full commit contents as plaintext JSONL outside the shim
   (`kdb-inspect/.../FileSidecarWriter.jvm.kt:13-32`); opt-in via
   `StorageEngineConfig.debugSidecar`. Must be a hard error alongside encryption (§5.6).
2. **SSTable footer index** is plaintext ASCII hex of every content-hash key even when blocks
   are compressed (`SsTableCodec.kt:31-40`). The footer must be encrypted as a unit (§5.3).
3. **Stream-mode index hints** ship `{indexId, fieldName, key, docId}` in cleartext to
   subscribers (`go/kdb/wire/payload_dto.go:185-193`). Must be suppressed for E2EE namespaces (§6.4).
4. **Segment filenames** leak namespace ids (user-meaningful, e.g. `catalog/table`,
   `_system/users`), LSM level, and exact commit-batch counts
   (`kdb-storage-io/.../SegmentNameBuilder.kt:3-52`). Accepted v1 leak with optional
   pseudonymization later (§9).
5. **Session tokens are stored in plaintext document fields**
   (`kdb-auth/.../token/SessionIssuer.kt:51-61`) — meaning at-rest encryption of `_system`
   namespaces also encrypts the credential store, and key-availability must precede auth (§5.7).

### 2.7 BUG: SSTable integrity fields are written but never verified

Block CRC32 (offset 8) is ignored by `decodeBlock` in both ports (`SsTableCodec.kt:24-29`,
`go/kdb/storage/sstable/codec.go:36-44`); the footer's file-level SHA-256 is never read back.
This mirrors Layer 13's delta-CRC finding (§2.3 there). AEAD fixes this as a side effect —
decryption fails closed on any tampering — but the CRC fields should also be verified for
plaintext-mode stores.

### 2.8 BUG: Go peer-sync host never calls its auth engine

`defaultHost` stores an `auth.Engine` that is dead code (`go/kdb/peersync/host.go:22-104`);
Kotlin's handler enforces `AuthAction.PeerSync` (`PeerSyncFrameHandler.kt:45-69`). A
zero-knowledge hub is still an *authenticated* hub — this must be fixed before exposing a Go
sync host. Related: Go transports have no TLS at all (`go/kdb/transport/ws/transport.go:62`
returns "wss:// not yet implemented"); with E2EE the payloads are sealed regardless, but
credentials and metadata still need TLS (§6.5).

### 2.9 Prerequisite: Layer 13 Component 47

Delta replay order is currently derived from random segment UUIDs and can render a store
permanently unopenable after a handful of restarts (Layer 13 §2.1). Encryption multiplies the
cost of any replay defect (a mis-ordered replay under AEAD surfaces as a decryption failure
several layers removed from the cause), and Component 55's migration path (§5.5) rewrites
segments via compaction, which leans on deterministic replay. **Component 47 must land first.**

### 2.10 FACT: platform crypto reality

- Storage modules target `jvm`, `js(IR)`, `linuxX64`, `macosArm64` — **no `javax.crypto` in
  commonMain** is an existing rule (`docs/kdb-spec-layer1-component3-document-commit-model.md:490`).
- The exact shim pattern to copy exists twice: `secureRandomBytes` expect/actual
  (`kdb-codec/.../PlatformRandom.kt` + 3 actuals) and `ZstdCompression` expect object
  (`kdb-compression/.../ZstdCompression.kt:4`).
- **Caveat:** Kotlin/Native and JS zstd actuals are identity passthroughs
  (`ZstdCompression.native.kt`, `.js.kt`) — segment files are already not portable across
  platforms; the crypto shim must not repeat this (an identity "cipher" fallback is forbidden;
  a platform without a real AEAD fails closed at config time).
- Go has no AEAD usage today; `golang.org/x/crypto` is already in `go.mod` (indirect).
- Browser: WebCrypto provides AES-GCM natively; XChaCha20-Poly1305 is not available. This
  decides the cipher (§4.1).
- No FIDO/WebAuthn/PKCS#11/HSM/KMS code exists anywhere in the repo — Component 54 is greenfield.

-----

## 3. Threat model

| Adversary | Layer 55 (at rest) | Layers 56–57 (E2EE) |
|---|---|---|
| Stolen disk / decommissioned volume | Defeated | Defeated |
| Compromised or curious S3 bucket | Defeated (replicas ship disk bytes) | Defeated |
| Compromised **server process** (reads memory, holds server keys) | **Not defeated** — server holds its own DEKs | Defeated for payloads; metadata visible (§9) |
| Malicious server operator serving stale/forked history | Out of scope | Partially: commits are hash-chained; rollback to an old head is detectable by clients that remember their last head, not prevented |
| Compromised **client** endpoint (malware with user logged in, key plugged) | Out of scope | Out of scope |
| Metadata traffic analysis (who wrote what doc-id when, sizes, graph shape) | Leaks | Leaks — enumerated in §9 |
| Lost/destroyed USB key | n/a (server-held key) | **Data loss unless a second keyslot exists** — §4.4 makes a recovery slot mandatory |

What is deliberately **not** claimed: protection of plaintext in RAM (JVM cannot reliably zeroize
`String`s; documented residual risk), forward secrecy of individual commits (one DEK per
namespace epoch, rotated by re-encryption §4.5), and hiding of write patterns.

-----

## 4. Component 53 — Crypto Primitives Shim, and Component 54 — Keyring & USB Key

### 4.1 Cipher and primitives (Component 53)

**AES-256-GCM** everywhere. Rationale: it is the only AEAD available natively on all five
runtimes (JVM `javax.crypto`, browser WebCrypto `AES-GCM`, Go `crypto/aes`+`cipher.NewGCM`,
Kotlin/Native via provider, and it matches the master spec's stated direction). New modules:

- `:kdb-crypto` (commonMain): `AeadCipher` (`seal(key, nonce, aad, plaintext)` /
  `open(...)` — open fails closed), `Hkdf.sha256(ikm, salt, info, len)`, re-export of
  `secureRandomBytes`. Actuals: JVM → `javax.crypto`; JS → WebCrypto (async wrapped — note
  WebCrypto is Promise-based, so the JS actual needs the same suspend-bridging treatment the
  browser snapshot store already gets); Native → the `cryptography-kotlin` multiplatform
  library (OpenSSL/Apple providers) as a new catalog entry, or cinterop against
  CommonCrypto/OpenSSL directly. **No identity fallback, ever** (finding 2.10): a target
  without a real provider throws at `EncryptionConfig` construction.
- `go/kdb/crypto`: thin wrappers over stdlib (`aes`, `cipher`, `hkdf` from `x/crypto`).
- HKDF-SHA256 is the only KDF for key derivation; PBKDF2-HMAC-SHA256 (existing portable params
  precedent, `go/kdb/auth/password_hasher.go:17-21`) for the passphrase keyslot, at ≥600k
  iterations; Argon2id noted as a later upgrade behind the keyslot `kdf` field.
- Cross-port parity tests are normative, like `Crc32`/CRC32: same key/nonce/aad/plaintext must
  produce identical ciphertext bytes in Kotlin(JVM/JS/Native) and Go.

### 4.2 Key hierarchy

```
FIDO2 hmac-secret (32B, on USB key)        passphrase (recovery)
        │ HKDF info="kdb/kek/v1"                  │ PBKDF2
        ▼                                         ▼
      KEK ──────────── wraps (AES-GCM) ────────► keyslots in keyring file
        ▼
  DEK_ns  (random 256-bit, one per namespace, generated at namespace creation)
        │
        ├─ at-rest (55): segment subkey = HKDF(DEK_ns, salt=∅, info="kdb/seg/v1|"+segmentName)
        └─ E2EE (56):    doc key       = HKDF(DEK_ns, salt=∅, info="kdb/doc/v1")
```

- The KEK never touches disk. DEKs touch disk only wrapped.
- Per-segment subkeys mean nonce scope is one segment — random 96-bit nonces per frame are
  comfortably inside GCM bounds (a segment holds far fewer than 2³² frames; rotation seals
  segments long before that, `DefaultDeltaSegmentWriter.kt:64-80`).
- Separate HKDF `info` strings partition the at-rest and E2EE domains so the same DEK can serve
  a locally-encrypted vault and its E2EE envelopes without nonce/key reuse across domains.

### 4.3 Keyring file (Component 54)

`{dataRoot}/keyring.kdbkeys` — versioned JSON, the only new durable artifact, itself plaintext
(it contains only wrapped keys and public parameters), LUKS-style multi-slot:

```json
{
  "version": 1,
  "slots": [
    { "type": "fido2-hmac-secret", "label": "blue yubikey",
      "credentialId": "<b64>", "rpId": "kdb.local", "salt": "<b64 32B>",
      "requireUv": false, "kekKdf": "hkdf-sha256/kdb-kek-v1" },
    { "type": "passphrase", "label": "recovery",
      "kdf": "pbkdf2-hmac-sha256", "iterations": 600000, "salt": "<b64 16B>" }
  ],
  "namespaces": [
    { "namespaceId": "catalog/table",
      "wrappedDek": "<b64 nonce||ct||tag per slot, keyed by slot index>",
      "createdAt": "...", "epoch": 1 }
  ]
}
```

Every slot wraps every DEK (adding a slot = unlock with an existing slot, rewrap). CLI surface
(`kdb-cli`, new `key` verb group): `key init`, `key enroll-fido2`, `key enroll-passphrase`,
`key list`, `key remove-slot`, `key rotate-kek`, `key rotate-dek --namespace`.

### 4.4 FIDO2 provider — the "standard USB key"

The standard mechanism on ordinary FIDO2 keys (YubiKey 5+, SoloKey, Nitrokey 3, Google Titan)
is the **CTAP2 `hmac-secret` extension**: at enrollment we create a *non-discoverable* credential
with `hmac-secret` enabled and store its credential-id in the keyring; at unlock we request an
assertion carrying a fixed 32-byte salt, and the authenticator returns
`HMAC-SHA-256(CredRandom, salt)` — a stable 32-byte secret that never leaves the key's secure
element un-derived, gated on physical touch (and PIN when `requireUv`). This is the same pattern
used by `systemd-cryptenroll`, `age-plugin-fido2-hmac`, and Yubico's own reference material.

Implementation per platform:

- **Go (server-side tooling, desktop, gomobile):** `libfido2` via the `keys-pub/go-libfido2`
  cgo binding, isolated in a new `go/cmd/kdb-key` helper binary so cgo stays out of the main
  service build.
- **Kotlin/JVM:** no credible JVM CTAP library exists — the JVM desktop path shells out to the
  same `kdb-key` helper binary (single implementation, both ports; precedent: the repo already
  treats Go as the systems layer).
- **Browser:** WebAuthn **PRF extension** (`navigator.credentials.get({publicKey: {extensions:
  {prf: {eval: {first: salt}}}}})`), which is the web-exposed form of `hmac-secret`.
  **Interop rule (normative):** the PRF extension hashes its input as
  `SHA-256("WebAuthn PRF" || 0x00 || salt)` before it reaches the authenticator. So that one
  enrolled credential works from both browser and native, the native (`libfido2`) path MUST
  apply the same prefix-hash to the keyring salt before issuing the CTAP request. The keyring
  stores the raw application salt; both paths derive identically.
- **Two-salt rotation:** an assertion may carry two salts; `key rotate-kek` requests
  `(oldSalt, newSalt)` in one touch, unwraps with the old KEK, rewraps with the new.

**Lost-key policy (normative):** `key init` refuses to finish with fewer than two slots unless
`--i-accept-data-loss` is passed. A FIDO2 secret is non-extractable and non-recoverable by
design; without a second slot (backup key or passphrase), losing the USB key loses the data.

### 4.5 Rotation

- **KEK rotation** (new USB key, revoked passphrase): rewrap all DEKs — cheap, keyring-only.
- **DEK rotation** (suspected DEK exposure): bump the namespace `epoch`, generate DEK v(n+1);
  new segments/envelopes use the new epoch (recorded per-segment alongside the codec, finding
  2.2 precedent; and in the envelope `kid`, §6.2); old data re-encrypts opportunistically via
  compaction (`:kdb-storage-compaction` rewrite path). Until compaction completes, both epochs
  stay unlocked.

-----

## 5. Component 55 — At-Rest Frame Encryption

### 5.1 Format changes — new magics, ciphertext-counting length fields

Readers dispatch on magic; plaintext magics remain readable forever (finding 2.2 makes a new
magic the only version mechanism). Encrypted body = `nonce(12) ‖ AES-256-GCM(ct ‖ tag(16))` over
the *post-compression* body; **CRC32 is retained, computed over the encrypted body** — it keeps
its load-bearing torn-tail-detection role during recovery (`DeltaSegmentScanner.kt:57-68`,
Layer 13 §2.3) while the AEAD tag carries authenticity.

| Format | Plain magic | Encrypted magic | Frame layout (encrypted) |
|---|---|---|---|
| Delta | `KDBP` | `KDEP` (0x4B444550) | `[magic 4][ctLen 4][uncompressedLen 4][CRC32(encBody) 4][encBody]` |
| WAL | `KDBW` | `KDEW` (0x4B444557) | unchanged header; payload → encBody; both CRCs over encrypted bytes |
| SSTable block | (none) | via footer magic | `[ctLen 4][uncompressedLen 4][CRC32(encBody) 4][encBody]` |
| SSTable footer | `KDBS` | `KDES` (0x4B444553) | `[magic 4][indexLen 4][encrypted index blob][fileHash 32 over ciphertext blocks]` |

- Length fields count **ciphertext** (nonce+ct+tag), so existing offset arithmetic
  (`compressedSize + headerSize` reads, e.g. `SsTableCodec.kt:118`) is untouched.
- **AAD binds ciphertext to its location:** delta/WAL frames use
  `aad = segmentName ‖ frameIndex`; SSTable blocks use `aad = segmentName ‖ blockOffset`;
  the footer uses `aad = segmentName ‖ "footer"`. Splicing a valid frame into another segment
  or position fails authentication.
- The SSTable footer index (plaintext key hex today — leak 2.6.2) is encrypted as a single AEAD
  blob; the "last 8 bytes give indexLen" discovery contract is preserved.
- The `compSize != uncompSize` compression inference (`SsTableCodec.kt:28`) breaks under
  encryption (lengths always differ); the encrypted block gains an explicit codec byte inside
  the encrypted body's first position instead.

### 5.2 Where the code changes

One symmetric function pair per format, plus config threading:

- `DeltaPageCodec.frame/parse` (`DefaultDeltaSegmentWriter.kt:83-115`; `page_codec.go:11-45`)
- `WalCodec.encodeRecord/decodeRecords` (`WalCodec.kt:26-84`; `wal/codec.go:35-106`)
- `SsTableCodec.encodeBlock/decodeBlock/buildFooter/parseFooter` (`SsTableCodec.kt:14-54`;
  `sstable/codec.go:17-87`)
- `StorageEngineConfig` gains `encryption: EncryptionConfig? = null` — exactly the
  `debugSidecar` pattern (`kdb-storage/.../StorageEngineConfig.kt:50`), holding the unlocked
  namespace DEK provider, epoch, and policy flags. Go: `FileRuntimeOptions.Encryption`
  (`go/kdb/embed/storage_options.go`), env-configured like S3 (`KDB_ENCRYPTION=required`),
  but **key material never via env** — the runtime receives an unlocked `Keyring` handle from
  `kdb-key`/the embedding app.
- Segment descriptors persist `(codec, encryptionEpoch)` where they persist `compressionCodec`
  today (`DefaultDeltaSegmentWriter.kt:77,239,249`).

### 5.3 Block cache decision

`BlockCache` is currently dead code (zero non-test callers, `SsTableTypes.kt:60-66`,
`go/kdb/storage/sstable/types.go:104-112`). Normative for whoever wires it up: it caches
**plaintext** blocks keyed `(fileHash, offset)` as the 10c spec assumes — decrypt-on-fill —
because decrypting on every cache hit would erase the cache's purpose. Memory-resident
plaintext is inside the threat model's accepted residual (§3).

### 5.4 Snapshots, ice bundles, and everything without a codec

`writeSnapshot`/`readSnapshot` and tier-backend puts bypass the frame codecs (whole-blob,
`JvmSegmentByteStore.kt:114-132`, `os_store.go:215-240`, `PlatformIoShimTierBackend.kt:17-24`).
These get an `EncryptingSegmentByteStore` decorator — whole-blob AEAD with a 4-byte `KDEB`
prefix, `aad = snapshotKey` — inserted **below** `PrimaryWithReplicas` in
`buildSegmentByteStore` (`go/kdb/embed/segment_store.go:11-24`) so S3 replica uploads carry
ciphertext (finding 2.1). The decorator passes segment append/read through untouched (frames
already encrypted by 5.2) and transforms only the snapshot surface. `meta.json` (namespace id
only — already leaked by the directory name) and `.kdb.lock` stay plaintext.

### 5.5 Migration and mixed mode

Opening an existing plaintext store with encryption configured enters **migrating** mode: reads
dispatch on magic (both accepted); all new frames encrypted; `kdb-cli storage reencrypt`
drives compaction/rewrite of remaining plaintext segments and reports progress; when
`listSegments` shows no plaintext magics the store flips to **sealed** mode, where a plaintext
magic on read is a hard integrity error. `KDB_ENCRYPTION=required` refuses to open a store
containing plaintext frames except in explicit migrating mode.

### 5.6 Policy hard lines

- `encryption != null && debugSidecar != null` → construction-time error (leak 2.6.1) unless
  `debugSidecar.allowPlaintext = true` is set explicitly.
- No identity cipher on any platform (finding 2.10).
- Browser JS: segments are RAM-only today, but `BrowserSnapshotStore` base64s into localStorage
  (`FileBackedPlatformIoShimFactory.js.kt`) — snapshots there go through the same 5.4 decorator.

### 5.7 Boot ordering

Session tokens and users live in `_system` namespaces as ordinary documents (leak 2.6.5), so on
an encrypted server the keyring must unlock **before** the auth engine can validate anyone.
`OpenFileRuntime`/`openFileRuntime` ordering: lock dir → load keyring → unlock (operator
passphrase slot or `kdb-key` at service start) → open engines → replay → listeners. A server
that cannot unlock refuses to start (crash-only, consistent with Layer 13's no-zombie rule).

-----

## 6. Component 56 — E2EE Document Envelope & Zero-Knowledge Hub Profile

### 6.1 Ciphertext-native commits

E2EE namespaces hold ciphertext in the commit itself: `OpWrite.patch` is the envelope (§6.2),
in every DAG, delta log, and wire frame, on client and server alike. There is **one hash space**
(finding 2.3): the commit hash covers ciphertext, so client and server agree on heads, ancestry,
fast-forward, and dedup with zero knowledge of contents. Plaintext exists only where a key-holding
client *materializes* documents (§7).

### 6.2 The envelope

Written as the document body, satisfying findings 2.3/2.4:

```json
{ "_e2ee": 1, "alg": "A256GCM", "kid": "catalog/table#1", "n": "<b64 12B>", "ct": "<b64>" }
```

- **Canonical serialization (normative):** exactly these five keys, this order, no whitespace,
  base64 without padding — byte-stable forever (2.3).
- **Every write carries the whole envelope** — root-merge-proof (2.4): all five keys present on
  every write, so a shallow merge fully replaces the previous envelope. Mixed
  plaintext-then-E2EE histories on one document would still leak old root keys via merge; the
  migration rule is therefore per-namespace, never per-document (§6.6).
- `aad = namespaceId ‖ 0x00 ‖ docId` — a ciphertext moved to another doc or namespace fails
  authentication at decrypt.
- `kid = namespaceId#epoch` selects the DEK epoch (rotation, §4.5); key = HKDF(DEK_ns,
  info="kdb/doc/v1").
- `ct` decrypts to the true document JSON object.
- Schema migrations: `OpSchemaMigration.migrationPayload` gets the same envelope; file blobs
  (`OpFileWrite`) are AEAD-sealed bytes with `aad = blobHash-domain` — they already ride the
  WAL/SSTable path as opaque bytes (attachments have no separate format,
  `kdb-file/.../FileAttachmentIngest.kt:51,121`).

### 6.3 Hub profile (server configuration, mostly existing switches)

A namespace served in **hub mode** means: `KdbSchema.NONE` (already the service default,
`KdbServiceMain.kt:279`), **no index registry**, `materializeCommit = nil` for pure relay — or
the ciphertext-tolerant materialize (constructors don't validate JSON,
`EmbedOperations.kt:100-125`) when the hub must serve `DocumentGet`/`Upsert` — and SQL access
**rejected with a typed error** for E2EE namespaces (predicates would silently match nothing,
`go/kdb/sql/predicate.go:79-90`; silent-empty is worse than refusal). Conflict machinery works
unchanged: fast-forward/diverged from hashes, non-conflicting auto-merge stages opaque bytes,
and a genuine per-document conflict produces a `ConflictReport` whose `LocalDoc`/`IncomingDoc`
carry ciphertext — resolvable only by a key-holding client (§7.3). The spec-39 §9
branch-per-writer recommendation (`match-results/<hostID>`) is the recommended topology for
multi-writer E2EE namespaces since it minimizes hub-side merges.

### 6.4 Metadata hygiene at the hub

- Index hints suppressed for E2EE namespaces (leak 2.6.3).
- `DeltaAuthorshipEnvelope{Principal, RightsToken, ClientContext}`
  (`go/kdb/storage/delta_types.go:10-25`) is hub-side metadata; it remains visible (it is how
  the hub audits) — enumerated as an accepted leak in §9.

### 6.5 Prerequisites on the wire

Fix finding 2.8 (Go host auth is dead code) before any Go-hosted hub; implement `wss`/TLS in Go
transports or front the Go service with a TLS terminator — E2EE seals payloads, but credentials
(`handshakeDto.User/Password/Token` are plaintext JSON fields) and metadata still need transport
protection.

### 6.6 Enabling E2EE

Per-namespace, at creation (`e2ee: true` in namespace metadata, carried in `meta.json` and the
keyring entry). Retrofitting an existing plaintext namespace = create a new E2EE namespace and
copy documents through a key-holding client (which re-commits them encrypted); in-place
per-document conversion is forbidden (merge-leak, §6.2).

-----

## 7. Component 57 — Client Vault Runtime

### 7.1 Shape

The user-facing "decrypted second instance": an embed runtime
(`FileEmbeddedRuntime`/JDBC on JVM, `OpenFileRuntime` via gomobile on mobile, `KdbBrowser` in
JS) opened in **vault mode** with an unlocked keyring:

- **DAG + delta log:** ciphertext-native commits, byte-identical with the hub. Sync is the
  existing peer-sync client (`syncEmbeddedWithPeer`, `kdb-embed/.../RemotePeerSync.kt`) with
  no crypto awareness at all.
- **Materialization boundary = decryption boundary:** the vault's `materializeCommit`
  decrypts envelopes and stores plaintext documents into the realized store; local schema
  validation, SQL, and every index (hash/btree/fulltext/vector) run over plaintext, locally.
- **Local writes:** the transaction engine gains a pre-commit encrypt hook — schema validation
  runs on the plaintext patch, then the envelope is built and the commit is hashed over
  ciphertext.
- **Local disk:** the vault's own segments are protected by Component 55 with the same DEK
  (separate HKDF domain, §4.2) — so the laptop's disk holds ciphertext too, and stealing the
  laptop without the USB key yields nothing. A `plaintextLocal` opt-out exists for machines
  already covered by FDE, per the Component 43 precedent
  (`docs/kdb-spec-layer12-component43-embed-durable-storage.md:192-194`).

### 7.2 Unlock lifecycle

Open vault → `kdb-key` assertion (one USB touch) → KEK → DEKs unwrapped into memory → runtime
opens, replays (decrypting at materialize), serves. DEKs stay in memory for the session; an
idle-lock timer closes the runtime and drops key material (best-effort zeroization; JVM caveat
documented). One touch per unlock, not per operation.

### 7.3 Conflict resolution with the key

When the hub reports a genuine conflict, the vault decrypts both `ConflictItem` sides, presents
plaintext, and commits the resolution as a normal (encrypted) merge commit — the existing
`acceptRemoteChanges`/`rejectRemoteChanges`/`mergeBranches` surface
(`kdb-embed/.../RemoteBranchSync.kt`) is already the right raw material.

### 7.4 Browser vault

`KdbBrowser` + WebAuthn PRF (§4.4) covers the browser: segments are RAM-only, snapshots
localStorage — both under 55's decorator. The PRF interop rule (§4.4) is what lets the same
physical key unlock the desktop and browser vaults.

-----

## 8. What breaks in an E2EE namespace (and where it runs instead)

| Capability | Hub (no key) | Vault (key) |
|---|---|---|
| Peer sync, ancestry, dedup, fast-forward | ✅ | ✅ |
| Non-conflicting auto-merge | ✅ (opaque staging) | ✅ |
| Durability, replay, S3 replication, compaction | ✅ | ✅ |
| SQL `WHERE` / projections | ❌ typed rejection | ✅ local |
| All indexes (hash/btree/fulltext/vector) | ❌ never built | ✅ local |
| Schema validation | ❌ (schema NONE) | ✅ pre-encrypt |
| Stream-mode index hints | ❌ suppressed | ✅ local |
| Conflict *resolution* (content-level) | ❌ report only | ✅ |

This split is the product trade: the server loses server-side query on E2EE namespaces. Ordinary
(non-E2EE) namespaces on the same server are unaffected and may still use Component 55 with
server-held keys for plain at-rest protection.

-----

## 9. Residual metadata (accepted leaks, v1)

Visible to a hub or disk-holder even with both layers on: namespace ids (paths + protocol),
doc ids, op kinds (write/delete), commit graph shape and timestamps (µs), `transactionId`,
`authorNodeId`, authorship envelope principal, ciphertext sizes, write cadence, LSM level
(data age), exact commit counts via `%020d` delta names (`SegmentNameBuilder`). Filename
pseudonymization (HMAC(DEK, name) with a manifest for replay ordering) is sketched but
deferred — the `%020d` ordering is load-bearing for recovery (`delta/writer.go:176-213`) and a
manifest adds a new critical durable artifact; not worth it for v1.

-----

## 10. Execution plan

**Phase 0 (prereqs):** Layer 13 Component 47 (replay order); verify SSTable CRCs on read (2.7);
Go peer-host auth wired (2.8).

**Phase 1 — at rest:** 53 (crypto shim + parity tests) → 54 (keyring, passphrase slot, CLI) →
55 (delta first — it is the file-runtime's sole persistence path today — then WAL, SSTable,
snapshots/decorator, migration tool). Server usable with operator-held keys at this point.

**Phase 2 — USB key:** 54's FIDO2 slot (`kdb-key` helper, go-libfido2), browser PRF, interop
tests (same physical key, desktop + browser).

**Phase 3 — E2EE:** 56 (envelope, hub profile, SQL rejection, hint suppression) → 57 (vault
materialize/encrypt hooks, unlock lifecycle, conflict UX) → integration: two vaults + one blind
hub, kill-and-restore-from-S3, conflict round-trip, key-rotation under load.

**Testing normative:** cross-port ciphertext parity (Kotlin×Go byte-equality); tamper matrix
(bit-flip body/nonce/tag/AAD-move → typed failures); torn-tail under encryption; hub
compromise drill (assert no plaintext in any hub artifact: disk, S3, logs, sidecar-off);
lost-key drill (recovery slot restores everything).

-----

## 11. Alternatives considered

- **Filesystem/FDE only (LUKS, FileVault, iOS Data Protection).** Rejected as the whole answer:
  keys are held by the *server operator*, not the user; S3 replicas leave the encrypted volume;
  Component 43's "defer to OS FDE" stance remains valid for the *mobile vault's local* disk only.
- **Two literal instances with plaintext local commits.** Rejected — splits the hash space
  (finding 2.3); ancestry and idempotent-retry break; a translation table between plaintext and
  ciphertext commit hashes would itself be a consistency-critical durable artifact.
- **Encrypt at `SegmentByteStore` only (no codec changes).** Rejected as the primary mechanism —
  the store sees no record boundaries, so random-access reads would force a length-preserving
  scheme (CTR/XTS) with no authentication, and the SSTable footer key leak would survive.
  Retained only for whole-blob snapshots (§5.4).
- **XChaCha20-Poly1305.** Preferable nonce properties, but unavailable in WebCrypto; a
  browser-side JS implementation would violate the no-hand-rolled-crypto rule. AES-256-GCM with
  per-segment subkeys reaches the same safety margin.
- **FIDO2 `largeBlob`/resident-key storage of the DEK.** Less portable (resident keys consume
  scarce authenticator slots; largeBlob support is spotty); `hmac-secret` is the ecosystem
  standard (systemd, age, KeePassXC) and works with non-discoverable credentials.

## 12. Open questions

1. Should hub-mode `DocumentGet`/`Upsert` be allowed at all, or is peer-sync-only the cleaner
   contract? (Upsert from a keyless writer produces a valid but undecryptable envelope only if
   the writer has the DEK — i.e. wire upsert of E2EE docs requires a vault anyway.)
2. `cryptography-kotlin` dependency vs. hand-cinterop for Kotlin/Native — spike needed.
3. Multi-user namespaces: v1 is one DEK per namespace shared by all key-holders (shared-vault
   model). Per-user access with server-enforced revocation requires envelope-per-recipient
   (à la age) — deferred.
4. Does the browser vault need IndexedDB-backed segments (currently RAM-only) before it is a
   credible durable vault? Likely yes; that is Component 43's deferred scope, not this layer's.
