package stream

import "github.com/limidus/kdb/go/kdb/wire"

type IndexHintApplier interface {
	Apply(namespaceID string, hints []wire.IndexHint)
}

// NoopHintApplier ignores index hints (v1 default).
type NoopHintApplier struct{}

func (NoopHintApplier) Apply(string, []wire.IndexHint) {}

func DefaultHintApplier() IndexHintApplier { return NoopHintApplier{} }
