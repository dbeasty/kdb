package server

import (
	"runtime/debug"

	"github.com/limidus/kdb/go/kdb/storage"
)

// DefaultBudgetFractionOfSystemMemory is how much of a host's physical memory kdb-service claims
// as its budget when there is no cgroup limit to read. Not a reservation and not an allocation -
// purely the denominator the pressure zones are computed against, so that a server on a bare host
// still degrades gracefully instead of running until the kernel's OOM killer picks it.
//
// 75% leaves real room for the page cache, other processes, and the kernel itself. Sizing the
// budget at ~100% of physical memory would be the same mistake as having no budget at all: the
// zones would only start shedding once the machine was already in trouble.
const DefaultBudgetFractionOfSystemMemory = 0.75

// DetectMemoryBudgetBytes returns the memory budget to govern against when the operator has not
// specified one, in preference order (kdb-spec-layer13 §13, "Default: cgroup limit if
// detectable"):
//
//  1. The cgroup memory limit, when one is actually set. This is the number the kernel will
//     enforce by killing the process, so it is the only fully correct answer where it exists.
//  2. DefaultBudgetFractionOfSystemMemory of the host's physical memory.
//  3. Zero - governance disabled - only when the platform can tell us neither.
//
// Having a default at all is the point. The guard shipped disabled, with nothing in the
// Dockerfile, the systemd unit, or the config defaults turning it on, which meant the single
// mechanism protecting against OOM was inert in every deployment that did not know to ask for it.
// A protection that must be discovered before it works is not a protection; the numbers above are
// chosen so that switching it on by default is safe rather than surprising.
func DetectMemoryBudgetBytes() uint64 {
	if v, ok := cgroupMemoryLimitBytes(); ok {
		return v
	}
	if total, err := storage.TotalSystemMemoryBytes(); err == nil && total > 0 {
		return uint64(float64(total) * DefaultBudgetFractionOfSystemMemory)
	}
	return 0
}

// GoMemLimitFraction is the fraction of the memory budget handed to Go's own soft memory limit
// (kdb-spec-layer13 §5.6). It lands deliberately *between* the two zone boundaries that matter:
// shedding writes begins at ZoneHigh (85%), the GC escalates here (90%), and ZoneCritical (93%)
// is where the rescue reserve is dropped and the abort timer starts.
//
// That ordering is the point. Refusing new work is cheap and reversible, so it goes first; making
// the collector burn CPU is expensive and degrades every request, so it goes second, once
// shedding alone has not been enough. Setting this below the shed point would invert that and
// make the node slow before it made it selective.
//
// Note that the two numbers govern different quantities and are not directly comparable: the
// zones measure the whole process against its cgroup budget, while GOMEMLIMIT bounds the Go heap
// specifically. Expressing both as fractions of the same budget is what keeps their *relative*
// order meaningful even though the underlying measurements differ.
const GoMemLimitFraction = 0.90

// ApplyGoMemoryLimit sets Go's soft memory limit to GoMemLimitFraction of budgetBytes and returns
// the value applied (0, having changed nothing, when budgetBytes is 0).
//
// This closes kdb-spec-layer13 §2.9. debug.SetMemoryLimit was never called anywhere in the tree,
// which meant the garbage collector had no idea a ceiling existed: it kept using its default
// heap-growth heuristic right up to the point the kernel killed the process. Under a soft limit
// the GC instead becomes progressively more aggressive as the heap approaches it, trading CPU for
// memory - which is precisely the trade a server that is about to be OOM-killed wants to make,
// and which buys the admission system time to shed load before anything is refused at all.
//
// A soft limit, not a hard one: exceeding it makes Go collect harder, never allocate-fail. That
// is why it can be set safely without risking spurious failures in a process that legitimately
// needs a burst.
func ApplyGoMemoryLimit(budgetBytes uint64) int64 {
	if budgetBytes == 0 {
		return 0
	}
	limit := int64(float64(budgetBytes) * GoMemLimitFraction)
	if limit <= 0 {
		return 0
	}
	debug.SetMemoryLimit(limit)
	return limit
}

// DefaultMaxConnections caps concurrently-accepted connections per listener (kdb-spec-layer13
// §13). Each accepted connection costs a goroutine stack and a frame buffer whether or not it
// ever sends a request, and none of that is charged against the grant system - so without a cap,
// connections are a way to consume the budget that admission control cannot see or refuse.
const DefaultMaxConnections = 256

// DefaultPeerSyncMaxConnections caps concurrently-accepted peer-sync connections when
// KdbServerRuntime.PeerSyncMaxConnections is 0. Lower than DefaultMaxConnections because a peer
// session is heavier than a client's: it can hold a FETCH cursor (32 B per missing commit), a
// pushed graft being staged, and a snapshot page in flight. A cluster has a handful of peers, each
// with one session per direction; 64 leaves ample room for reconnects and transient overlap.
const DefaultPeerSyncMaxConnections = 64

// peerSyncMaxConnections is the listener cap for a runtime's PeerSyncMaxConnections setting.
func peerSyncMaxConnections(setting int) int {
	switch {
	case setting < 0:
		return 0 // unlimited, in the transport's terms
	case setting == 0:
		return DefaultPeerSyncMaxConnections
	default:
		return setting
	}
}

// The storage cache pool kdb-service gives its embed.Host when --cache-mb is 0: an eighth of the
// memory the process may use, between 64 MiB and 4 GiB. That is Cassandra's split: its heap is a
// quarter of RAM (cassandra-env.sh, at most 8 GB), and its caches and memtables take about half of
// the heap (chunk cache and memtables a quarter each, the key cache 5%) - the other half is left to
// everything else the process holds. KDB's other residents are the same kind of thing: secondary
// index history, the live tree and commit graph, and per-request garbage, which together held about
// as much as the pool in qtask's write-heavy benchmark. A first cut at a quarter of the machine
// for the pool alone put that benchmark's container at its limit and the memory guard shed writes.
// An embedded runtime keeps embed.DefaultHostMemoryBudgetBytes (64 MiB) - it shares its process
// with an application that owns the memory - but a server process owns its container. At the old
// fixed 64 MiB, qtask's 517k-document benchmark ran with a 4 MiB frame cache and a 9 MiB block
// cache in a 4 GiB container and decompressed the same frames on every read.
const (
	CachePoolFractionOfMemory       = 0.125
	MinCachePoolBytes         int64 = 64 << 20
	MaxCachePoolBytes         int64 = 4 << 30
)

// CachePoolBytes resolves --cache-mb to bytes and says where the number came from: a positive
// cacheMB is taken as given; 0 sizes the pool from the cgroup memory limit, else from host RAM,
// and falls back to MinCachePoolBytes when neither can be read.
func CachePoolBytes(cacheMB int) (int64, string) {
	if cacheMB > 0 {
		return int64(cacheMB) << 20, "explicit"
	}
	machine, source := int64(0), ""
	if v, ok := cgroupMemoryLimitBytes(); ok {
		machine, source = int64(v), "an eighth of the cgroup memory limit"
	} else if total, err := storage.TotalSystemMemoryBytes(); err == nil && total > 0 {
		machine, source = int64(total), "an eighth of host RAM"
	}
	if machine <= 0 {
		return MinCachePoolBytes, "default (no memory limit or host RAM to size from)"
	}
	return clampCachePool(int64(float64(machine) * CachePoolFractionOfMemory)), source
}

func clampCachePool(b int64) int64 {
	switch {
	case b < MinCachePoolBytes:
		return MinCachePoolBytes
	case b > MaxCachePoolBytes:
		return MaxCachePoolBytes
	}
	return b
}
