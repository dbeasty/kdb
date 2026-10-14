package sstable

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Sequence hands out the ages a namespace's new tables are named with (see tiering.go): each one
// past the highest the namespace holds. It learns that from the namespace's listing - the first
// time it is asked, unless DiscoverTables already listed - and from every table it is shown after.
// Kotlin's SsTableSequence does the same, so either runtime can carry on a namespace the other
// wrote.
type Sequence struct {
	io          storage.PlatformIOShim
	namespaceID string

	mu     sync.Mutex
	listed bool
	last   uint64
}

// NewSequence returns the sequence for namespaceID's tables.
func NewSequence(io storage.PlatformIOShim, namespaceID string) *Sequence {
	return &Sequence{io: io, namespaceID: namespaceID}
}

// Next is the age of the next table: later than every table the namespace holds.
func (q *Sequence) Next() (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.listed {
		names, err := q.io.ListSegments(q.namespaceID)
		if err != nil {
			return "", fmt.Errorf("sstable: listing %s for the next table's age: %w", q.namespaceID, err)
		}
		prefix := storio.SegmentNameBuilder.NamespacePrefix(q.namespaceID) + "sstable/"
		for _, name := range names {
			if strings.HasPrefix(name, prefix) {
				q.observeLocked(name)
			}
		}
		q.listed = true
	}
	q.last++
	return fmt.Sprintf("%016x", q.last), nil
}

// Observe makes every later Next newer than the table named segmentName.
func (q *Sequence) Observe(segmentName string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.observeLocked(segmentName)
}

// listedAll records that every table on disk has been observed, so Next need not list.
func (q *Sequence) listedAll() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.listed = true
}

func (q *Sequence) observeLocked(segmentName string) {
	if age, err := strconv.ParseUint(ageOf(segmentName), 16, 64); err == nil && age > q.last {
		q.last = age
	}
}
