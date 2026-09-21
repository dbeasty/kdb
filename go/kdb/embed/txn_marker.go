package embed

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

// groupMarkerPrefix opens the commit message of every participant commit of a cross-namespace
// group. The message is inside the commit hash and is persisted by both implementations, so a
// group needs no new field in the commit format - see docs/kdb-cross-namespace-transactions-plan.md
// §4.1. The version number is there so the shape can change without guessing.
const groupMarkerPrefix = "kdb:xns/1 "

// GroupMarker is what a participant commit records about the group it belongs to.
type GroupMarker struct {
	// Host identifies the data root whose decision log decides this group. A commit that reached
	// another data root - by peer sync, or a copied log - carries a marker naming a host that is
	// not the local one, and the local decision log has nothing to say about it.
	Host  string
	Epoch uint64
	Group codec.UUID
	// Parts are the participating namespaces, sorted. Informational: recovery never needs them,
	// because the decision log alone says whether a group committed.
	Parts []string
}

// Message renders the marker as a commit message.
func (m GroupMarker) Message() string {
	return fmt.Sprintf("%shost=%s epoch=%d group=%s parts=%s", groupMarkerPrefix, m.Host, m.Epoch, m.Group.String(), strings.Join(m.Parts, ","))
}

// ParseGroupMarker reads a marker back out of a commit message. ok is false for every ordinary
// commit, which is the overwhelmingly common case and costs one prefix comparison.
func ParseGroupMarker(message string) (GroupMarker, bool) {
	if !strings.HasPrefix(message, groupMarkerPrefix) {
		return GroupMarker{}, false
	}
	var m GroupMarker
	var sawEpoch, sawGroup bool
	for _, field := range strings.Fields(strings.TrimPrefix(message, groupMarkerPrefix)) {
		key, value, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		switch key {
		case "host":
			m.Host = value
		case "epoch":
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return GroupMarker{}, false
			}
			m.Epoch, sawEpoch = n, true
		case "group":
			id, err := codec.UUIDFromString(value)
			if err != nil {
				return GroupMarker{}, false
			}
			m.Group, sawGroup = id, true
		case "parts":
			if value != "" {
				m.Parts = strings.Split(value, ",")
			}
		}
	}
	if !sawEpoch || !sawGroup {
		return GroupMarker{}, false
	}
	return m, true
}
