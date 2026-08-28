// Package recovery implements kdb-spec-layer15 Component 61: restore and
// hybrid restore. There is exactly one algorithm - a verified union of
// whatever sources are available, applied topologically by commit hash
// (see HybridRestore's doc comment and kdb-spec-layer15 P6). A "plain"
// restore from a single backup and a "hybrid" restore that also salvages
// a damaged local log are the same call with a different Sources slice.
//
// This first implementation supports directory-backed sources only (a
// damaged local data directory, or another directory holding a backup
// copy of segments). Peer and S3-backed sources are kdb-spec-layer15
// Components 60 and 62 and are follow-up work - see that spec's §10 and
// execution plan.
package recovery

import (
	"fmt"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
)

// Source is one input to a restore - a namespace-scoped, directory-backed
// shim that may hold some of the commits a restore needs. Label is
// diagnostic only (surfaced in Result.SourcesUsed).
type Source struct {
	Label string
	Shim  storage.PlatformIOShim
}

// Result reports what a restore produced.
type Result struct {
	Namespace     string
	SourcesUsed   []string // labels of sources that contributed at least one commit
	AppliedCount  int
	MissingHashes []string // non-empty only if the union could not resolve every parent
}

// HybridRestore unions every source's CRC-verified commits by hash (see
// integrity.ScanVerifiedCommits - only frames that pass L1 verification
// ever contribute, per kdb-spec-layer15 P4/P5: an unverified byte is
// never trusted just because it happens to be the only copy available),
// orders the union topologically, and writes the result as a fresh
// sequenced delta log to out.
//
// A commit whose parent hash is present in no source at all can never be
// safely applied - it and everything depending on it are reported in
// Result.MissingHashes instead of being applied out of dependency order,
// which is the same "erroring only if a genuine parent is missing" rule
// kdb-spec-layer13 Component 47 §4.2 already uses for ordinary replay.
// comp selects the codec the restored segments are *written* with; sources are
// read using whatever codec each frame records (see delta.PageCodec).
func HybridRestore(sources []Source, namespaceID string, comp storage.CompressionCodec, out storage.PlatformIOShim) (*Result, error) {
	union := make(map[string]document.Commit)
	var used []string
	for _, src := range sources {
		commits, err := integrity.ScanVerifiedCommits(src.Shim, namespaceID)
		if err != nil {
			return nil, fmt.Errorf("scanning restore source %q: %w", src.Label, err)
		}
		if len(commits) > 0 {
			used = append(used, src.Label)
		}
		for hex, c := range commits {
			if _, ok := union[hex]; !ok {
				union[hex] = c
			}
		}
	}

	genesis, err := integrity.GenesisCommitHash(namespaceID)
	if err != nil {
		return nil, fmt.Errorf("computing genesis hash: %w", err)
	}
	ordered, missing := topologicalOrder(union, genesis)

	writer, err := (delta.Factory{Config: storage.StorageEngineConfig{CompressionCodec: comp, IOShim: out}}).OpenWriter(namespaceID)
	if err != nil {
		return nil, fmt.Errorf("opening restore output segment: %w", err)
	}
	for _, c := range ordered {
		payload, err := c.ToPayloadBytes()
		if err != nil {
			return nil, fmt.Errorf("encoding commit %s: %w", c.Hash.Hex(), err)
		}
		if _, err := writer.Append(storage.DeltaRecord{
			CommitHash:    c.Hash,
			NamespaceID:   namespaceID,
			CommitPayload: payload,
		}); err != nil {
			return nil, fmt.Errorf("appending commit %s to restored log: %w", c.Hash.Hex(), err)
		}
	}
	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("flushing restored log: %w", err)
	}
	if _, err := writer.Seal(); err != nil {
		return nil, fmt.Errorf("sealing restored log: %w", err)
	}

	sort.Strings(used)
	return &Result{
		Namespace:     namespaceID,
		SourcesUsed:   used,
		AppliedCount:  len(ordered),
		MissingHashes: missing,
	}, nil
}

// topologicalOrder mirrors embed.applyCommitsTopologically's round-based
// algorithm exactly (kdb-spec-layer13 Component 47 §4.2): a commit is
// ordered only once every parent it references has already been ordered.
// A parent absent from the union entirely never becomes "applied", so
// anything depending on it - directly or transitively - never progresses
// and lands in missing once a round makes no further progress, rather
// than being guessed at. genesis is exempt from that rule: it is never
// persisted to any log by design (see integrity.GenesisCommitHash), so a
// commit whose sole parent is genesis is ready immediately.
func topologicalOrder(union map[string]document.Commit, genesis codec.Hash) (ordered []document.Commit, missing []string) {
	applied := make(map[string]bool, len(union))
	pending := make([]document.Commit, 0, len(union))
	for _, c := range union {
		pending = append(pending, c)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Hash.Hex() < pending[j].Hash.Hex() })

	for len(pending) > 0 {
		var next []document.Commit
		progressed := false
		for _, c := range pending {
			ready := true
			for _, p := range c.ParentHashes {
				if p == genesis {
					continue
				}
				if !applied[p.Hex()] {
					ready = false
					break
				}
			}
			if !ready {
				next = append(next, c)
				continue
			}
			ordered = append(ordered, c)
			applied[c.Hash.Hex()] = true
			progressed = true
		}
		if !progressed {
			for _, c := range next {
				missing = append(missing, c.Hash.Hex())
			}
			break
		}
		pending = next
	}
	sort.Strings(missing)
	return ordered, missing
}
