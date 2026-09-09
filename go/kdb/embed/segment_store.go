package embed

import (
	"context"
	"fmt"

	storio "github.com/limidus/kdb/go/kdb/storage/io"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

// buildSegmentByteStore assembles the primary store and whatever copies of it a deployment has
// asked for.
//
// Two kinds of copy, and the difference decides whether reclaiming data is recoverable:
//
//   - A *replica* mirrors the primary, deletions included. It exists so the data is somewhere
//     else, and a segment the primary has dropped is one it should drop too.
//   - An *archive* mirrors writes and never deletions. It exists to hold what the primary lets
//     go, which is exactly what history=none creates: with one configured, a compaction is
//     eviction from local disk rather than destruction of the commits.
//
// Both can be on at once, and usually should be for a deployment that wants both durability and
// a bounded local footprint.
func buildSegmentByteStore(config storio.PlatformIOConfig, s3Cfg *s3io.Config, archiveCfg *s3io.Config, archiveBlobs s3io.BlobStore, policy storio.ReplicationPolicy) (storio.SegmentByteStore, error) {
	primary, err := storio.NewOSByteStore(config)
	if err != nil {
		return nil, err
	}
	if s3Cfg == nil && archiveCfg == nil {
		return primary, nil
	}
	sinks := make([]storio.RoledSink, 0, 2)
	if s3Cfg != nil {
		replica, err := s3io.OpenReplicaSink(context.Background(), *s3Cfg)
		if err != nil {
			return nil, fmt.Errorf("s3 replica: %w", err)
		}
		sinks = append(sinks, storio.RoledSink{Sink: replica, Role: storio.SinkReplica})
	}
	if archiveCfg != nil {
		var archive storio.ReplicaSink
		if archiveBlobs != nil {
			// A caller-supplied object store, which is how a test exercises the archive without a
			// bucket. Same sink and the same code path; only the bytes' destination differs.
			archive = s3io.NewReplicaSink(archiveBlobs, *archiveCfg)
		} else {
			opened, err := s3io.OpenReplicaSink(context.Background(), *archiveCfg)
			if err != nil {
				return nil, fmt.Errorf("s3 archive: %w", err)
			}
			archive = opened
		}
		sinks = append(sinks, storio.RoledSink{Sink: archive, Role: storio.SinkArchive})
	}
	return storio.NewPrimaryWithSinks(primary, sinks, policy), nil
}
