package server

import (
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
)

var processNodeID atomic.Pointer[codec.UUID]

// SetProcessNodeID sets the identity every KdbServerRuntime constructed afterwards is given -
// kdb-service calls it with embed.LoadOrCreateNodeID(dataDir) before opening any namespace.
func SetProcessNodeID(id codec.UUID) { processNodeID.Store(&id) }

// ProcessNodeID is this process's node identity: whatever SetProcessNodeID last set, or a random
// id chosen once per process for runtimes with no data root (memory runtimes, tests).
func ProcessNodeID() codec.UUID {
	if p := processNodeID.Load(); p != nil {
		return *p
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return codec.UUID{}
	}
	processNodeID.CompareAndSwap(nil, &id)
	return *processNodeID.Load()
}
