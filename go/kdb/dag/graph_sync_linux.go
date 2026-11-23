package dag

import "os"

func syncGraphFile(f *os.File) error { return f.Sync() }

// graphTrickleBytes is how often a segment write syncs what it has written so far: Cassandra's
// trickle_fsync, as the storage layer's SSTable and spare-segment writers do (storage/io,
// trickleSyncBytes). A segment written whole and synced once puts megabytes in the device queue
// in front of the commit log's next fdatasync; a megabyte at a time bounds what that sync can
// find waiting.
const graphTrickleBytes = 1 << 20
