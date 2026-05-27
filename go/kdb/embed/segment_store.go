package embed

import (
	"context"
	"fmt"

	storio "github.com/limidus/kdb/go/kdb/storage/io"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

func buildSegmentByteStore(config storio.PlatformIOConfig, s3Cfg *s3io.Config, policy storio.ReplicationPolicy) (storio.SegmentByteStore, error) {
	primary, err := storio.NewOSByteStore(config)
	if err != nil {
		return nil, err
	}
	if s3Cfg == nil {
		return primary, nil
	}
	replica, err := s3io.OpenReplicaSink(context.Background(), *s3Cfg)
	if err != nil {
		return nil, fmt.Errorf("s3 replica: %w", err)
	}
	return storio.NewPrimaryWithReplicas(primary, []storio.ReplicaSink{replica}, policy), nil
}
