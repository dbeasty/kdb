package embed

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/codec/schema"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// checkpointFormatVersion is bumped whenever a change would make an older
// reader misread a newer file. A checkpoint whose version is not exactly
// this is ignored and the namespace opens by replaying the log, so raising
// it costs one slow start and never costs correctness.
const checkpointFormatVersion = 1

const checkpointNS = "dev.kdb.checkpoint"

var (
	fqnCheckpoint       = checkpointNS + ".Checkpoint"
	fqnCheckpointCommit = checkpointNS + ".CheckpointCommit"
	fqnCheckpointBranch = checkpointNS + ".CheckpointBranch"
	fqnCheckpointTag    = checkpointNS + ".CheckpointTag"
	fqnCheckpointEntry  = checkpointNS + ".CheckpointTreeEntry"
	fqnCheckpointSeg    = checkpointNS + ".CheckpointSegment"
	fqnCheckpointHash   = checkpointNS + ".Hash32"
)

var (
	cpUUID = schema.Primitive{Physical: schema.PhysicalFixed, Logical: schema.LogicalUUID{}}
	cpHash = schema.Ref{FullyQualifiedName: fqnCheckpointHash}
	cpTs   = schema.Primitive{Physical: schema.PhysicalInt64, Logical: schema.LogicalTimestampMicros{}}
	cpStr  = schema.Prim(schema.PhysicalString)
	cpI64  = schema.Prim(schema.PhysicalInt64)
	cpI32  = schema.Prim(schema.PhysicalInt32)
)

var (
	checkpointOnce sync.Once
	checkpointReg  *schema.Registry
)

// checkpointRegistry describes the on-disk checkpoint.
//
// A local cache format, deliberately its own schema rather than a reuse of
// dev.kdb.document's: that one is a cross-language wire contract with
// golden tests behind it, and a checkpoint is neither exchanged with peers
// nor read by the Kotlin tree. Coupling them would make a change to a
// private cache into a change to a published format.
func checkpointRegistry() *schema.Registry {
	checkpointOnce.Do(func() {
		r := schema.NewRegistry()
		r.RegisterFixed(&schema.FixedSchema{Name: "Hash32", Namespace: checkpointNS, Size: 32})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CheckpointTreeEntry", Namespace: checkpointNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "docId", Type: cpUUID},
				{ID: 2, Name: "contentHash", Type: cpHash},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CheckpointCommit", Namespace: checkpointNS,
			Fields: []schema.FieldSchema{
				// The hash is stored rather than recomputed: a commit hash
				// covers its operations, and those are exactly what a
				// checkpoint leaves out.
				{ID: 1, Name: "hash", Type: cpHash},
				{ID: 2, Name: "parentHashes", Type: schema.Array{Element: cpHash}},
				{ID: 3, Name: "transactionId", Type: cpUUID},
				{ID: 4, Name: "timestamp", Type: cpTs},
				{ID: 5, Name: "authorNodeId", Type: cpUUID},
				{ID: 6, Name: "documentTreeHash", Type: cpHash},
				{ID: 7, Name: "schemaHash", Type: schema.Nullable{Inner: cpHash}, Default: codec.Null},
				{ID: 8, Name: "message", Type: cpStr},
				{ID: 9, Name: "operationCount", Type: cpI32},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CheckpointBranch", Namespace: checkpointNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "name", Type: cpStr},
				{ID: 2, Name: "headHash", Type: cpHash},
				{ID: 3, Name: "createdAt", Type: cpTs},
				{ID: 4, Name: "updatedAt", Type: cpTs},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CheckpointTag", Namespace: checkpointNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "name", Type: cpStr},
				{ID: 2, Name: "commitHash", Type: cpHash},
				{ID: 3, Name: "createdAt", Type: cpTs},
				{ID: 4, Name: "message", Type: cpStr},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CheckpointSegment", Namespace: checkpointNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "sequence", Type: cpI64},
				{ID: 2, Name: "sizeBytes", Type: cpI64},
				{ID: 3, Name: "lastCommitHash", Type: cpHash},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "Checkpoint", Namespace: checkpointNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "formatVersion", Type: cpI32},
				{ID: 2, Name: "namespaceId", Type: cpStr},
				// Delta segments with sequence <= this are fully accounted
				// for by this checkpoint; replay resumes after it.
				{ID: 3, Name: "throughSequence", Type: cpI64},
				{ID: 4, Name: "commits", Type: schema.Array{Element: schema.Ref{FullyQualifiedName: fqnCheckpointCommit}}},
				{ID: 5, Name: "branches", Type: schema.Array{Element: schema.Ref{FullyQualifiedName: fqnCheckpointBranch}}},
				{ID: 6, Name: "tags", Type: schema.Array{Element: schema.Ref{FullyQualifiedName: fqnCheckpointTag}}},
				// The live document tree, flattened. Historical trees are
				// not stored - see engine.SetTreeRebuilder.
				{ID: 7, Name: "liveTree", Type: schema.Array{Element: schema.Ref{FullyQualifiedName: fqnCheckpointEntry}}},
				// A fingerprint of every delta segment this checkpoint
				// claims to account for, so open can tell that the log
				// still says what it said when the checkpoint was written.
				{ID: 8, Name: "segments", Type: schema.Array{Element: schema.Ref{FullyQualifiedName: fqnCheckpointSeg}}},
				// Canonical commit payloads for the branch heads, the only
				// commits whose operations have to be resident the moment
				// the namespace opens. Stored as payload bytes so decoding
				// one both reconstructs the commit and re-derives its hash.
				{ID: 9, Name: "headCommits", Type: schema.Array{Element: schema.Prim(schema.PhysicalBytes)}},
			},
		})
		r.Freeze()
		checkpointReg = r
	})
	return checkpointReg
}

var checkpointType = schema.Ref{FullyQualifiedName: fqnCheckpoint}

// namespaceCheckpoint is the checkpoint of one namespace: enough to
// rebuild the commit graph, the refs and the live tree without reading the
// delta log, plus the point in the log where replay must resume.
type namespaceCheckpoint struct {
	NamespaceID     string
	ThroughSequence int64
	State           dag.CheckpointState
	LiveTree        document.DocumentTree
	Segments        []checkpointSegment
	HeadCommits     [][]byte
}

// checkpointSegment fingerprints one delta segment a checkpoint covers.
//
// This is what keeps a checkpoint from masking damage. Skipping the
// segments a checkpoint accounts for also skips noticing that one of them
// has been truncated, corrupted or rolled back to an older copy - and the
// old behaviour, a hard failure at open, is a much better outcome than
// serving a namespace whose history has quietly gone missing. Size and
// last commit come from the segment listing that open already performs,
// so checking them costs nothing extra.
type checkpointSegment struct {
	Sequence       int64
	SizeBytes      int64
	LastCommitHash codec.Hash
}

func checkpointKey(namespaceID string) string {
	return "kdb:checkpoint:" + namespaceID
}

func cpHashVal(h codec.Hash) codec.Value { return codec.FixedValue{V: h.Bytes[:]} }
func cpUUIDVal(u codec.UUID) codec.Value { return codec.UUIDValue{MSB: u.MSB, LSB: u.LSB} }
func cpTsVal(t codec.Timestamp) codec.Value {
	return codec.TimestampValue{EpochMicros: t.EpochMicros()}
}

func cpHashFrom(v codec.Value) (codec.Hash, error) {
	f, ok := v.(codec.FixedValue)
	if !ok {
		return codec.Hash{}, fmt.Errorf("kdb: checkpoint: expected a 32-byte hash, got %T", v)
	}
	return codec.HashFromBytes(f.V)
}

func cpUUIDFrom(v codec.Value) (codec.UUID, error) {
	u, ok := v.(codec.UUIDValue)
	if !ok {
		return codec.UUID{}, fmt.Errorf("kdb: checkpoint: expected a uuid, got %T", v)
	}
	return codec.UUID{MSB: u.MSB, LSB: u.LSB}, nil
}

func cpTsFrom(v codec.Value) (codec.Timestamp, error) {
	t, ok := v.(codec.TimestampValue)
	if !ok {
		return codec.Timestamp{}, fmt.Errorf("kdb: checkpoint: expected a timestamp, got %T", v)
	}
	return codec.TimestampFromEpochMicros(t.EpochMicros), nil
}

func cpStrFrom(v codec.Value) (string, error) {
	s, ok := v.(codec.StringValue)
	if !ok {
		return "", fmt.Errorf("kdb: checkpoint: expected a string, got %T", v)
	}
	return s.V, nil
}

func encodeCheckpoint(cp namespaceCheckpoint) ([]byte, error) {
	commits := make([]codec.Value, 0, len(cp.State.Commits))
	for _, cc := range cp.State.Commits {
		parents := make([]codec.Value, len(cc.Commit.ParentHashes))
		for i, p := range cc.Commit.ParentHashes {
			parents[i] = cpHashVal(p)
		}
		fields := map[int]codec.Value{
			1: cpHashVal(cc.Commit.Hash),
			2: codec.ArrayValue{Elements: parents},
			3: cpUUIDVal(cc.Commit.TransactionID),
			4: cpTsVal(cc.Commit.Timestamp),
			5: cpUUIDVal(cc.Commit.AuthorNodeID),
			6: cpHashVal(cc.Commit.DocumentTreeHash),
			8: codec.StringValue{V: cc.Commit.Message},
			9: codec.Int32Value{V: int32(cc.OperationCount)},
		}
		if cc.Commit.SchemaHash != nil {
			fields[7] = cpHashVal(*cc.Commit.SchemaHash)
		}
		commits = append(commits, codec.RecordValue{Fields: fields})
	}

	branches := make([]codec.Value, 0, len(cp.State.Branches))
	for _, b := range cp.State.Branches {
		branches = append(branches, codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: b.Name},
			2: cpHashVal(b.HeadHash),
			3: cpTsVal(b.CreatedAt),
			4: cpTsVal(b.UpdatedAt),
		}})
	}

	tags := make([]codec.Value, 0, len(cp.State.Tags))
	for _, t := range cp.State.Tags {
		tags = append(tags, codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: t.Name},
			2: cpHashVal(t.CommitHash),
			3: cpTsVal(t.CreatedAt),
			4: codec.StringValue{V: t.Message},
		}})
	}

	var entries []codec.Value
	cp.LiveTree.Walk(func(id codec.UUID, h codec.Hash) bool {
		entries = append(entries, codec.RecordValue{Fields: map[int]codec.Value{
			1: cpUUIDVal(id),
			2: cpHashVal(h),
		}})
		return true
	})

	segs := make([]codec.Value, 0, len(cp.Segments))
	for _, sg := range cp.Segments {
		segs = append(segs, codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.Int64Value{V: sg.Sequence},
			2: codec.Int64Value{V: sg.SizeBytes},
			3: cpHashVal(sg.LastCommitHash),
		}})
	}

	heads := make([]codec.Value, 0, len(cp.HeadCommits))
	for _, b := range cp.HeadCommits {
		heads = append(heads, codec.BytesValue{V: b})
	}

	root := codec.RecordValue{Fields: map[int]codec.Value{
		1: codec.Int32Value{V: checkpointFormatVersion},
		2: codec.StringValue{V: cp.NamespaceID},
		3: codec.Int64Value{V: cp.ThroughSequence},
		4: codec.ArrayValue{Elements: commits},
		5: codec.ArrayValue{Elements: branches},
		6: codec.ArrayValue{Elements: tags},
		7: codec.ArrayValue{Elements: entries},
		8: codec.ArrayValue{Elements: segs},
		9: codec.ArrayValue{Elements: heads},
	}}
	return codec.EncodeBytes(root, checkpointType, checkpointRegistry())
}

func decodeCheckpoint(namespaceID string, raw []byte) (namespaceCheckpoint, error) {
	var out namespaceCheckpoint
	v, err := codec.DecodeBytes(raw, checkpointType, checkpointRegistry())
	if err != nil {
		return out, err
	}
	rec, ok := v.(codec.RecordValue)
	if !ok {
		return out, fmt.Errorf("kdb: checkpoint: expected a record at the root, got %T", v)
	}
	version, ok := rec.Fields[1].(codec.Int32Value)
	if !ok {
		return out, fmt.Errorf("kdb: checkpoint: missing format version")
	}
	if version.V != checkpointFormatVersion {
		return out, fmt.Errorf(
			"kdb: checkpoint is format version %d, this build reads version %d",
			version.V, checkpointFormatVersion)
	}
	ns, err := cpStrFrom(rec.Fields[2])
	if err != nil {
		return out, err
	}
	if ns != namespaceID {
		// A checkpoint filed under one namespace but describing another
		// would restore the wrong graph. Cheap to check, and the failure it
		// prevents is silent.
		return out, fmt.Errorf(
			"kdb: checkpoint describes namespace %q but was read for %q", ns, namespaceID)
	}
	through, ok := rec.Fields[3].(codec.Int64Value)
	if !ok {
		return out, fmt.Errorf("kdb: checkpoint: missing throughSequence")
	}
	out.NamespaceID = ns
	out.ThroughSequence = through.V

	commitsArr, ok := rec.Fields[4].(codec.ArrayValue)
	if !ok {
		return out, fmt.Errorf("kdb: checkpoint: missing commits")
	}
	out.State.Commits = make([]dag.CheckpointCommit, 0, len(commitsArr.Elements))
	for _, el := range commitsArr.Elements {
		cr, ok := el.(codec.RecordValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: malformed commit entry")
		}
		var c document.Commit
		c.NamespaceID = ns
		if c.Hash, err = cpHashFrom(cr.Fields[1]); err != nil {
			return out, err
		}
		parentsArr, ok := cr.Fields[2].(codec.ArrayValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: commit %s has malformed parents", c.Hash.Hex())
		}
		c.ParentHashes = make([]codec.Hash, len(parentsArr.Elements))
		for i, pv := range parentsArr.Elements {
			if c.ParentHashes[i], err = cpHashFrom(pv); err != nil {
				return out, err
			}
		}
		if c.TransactionID, err = cpUUIDFrom(cr.Fields[3]); err != nil {
			return out, err
		}
		if c.Timestamp, err = cpTsFrom(cr.Fields[4]); err != nil {
			return out, err
		}
		if c.AuthorNodeID, err = cpUUIDFrom(cr.Fields[5]); err != nil {
			return out, err
		}
		if c.DocumentTreeHash, err = cpHashFrom(cr.Fields[6]); err != nil {
			return out, err
		}
		switch sf := cr.Fields[7].(type) {
		case nil, codec.NullValue:
			c.SchemaHash = nil
		case codec.FixedValue:
			h, err := codec.HashFromBytes(sf.V)
			if err != nil {
				return out, err
			}
			c.SchemaHash = &h
		default:
			return out, fmt.Errorf("kdb: checkpoint: commit %s has a malformed schema hash", c.Hash.Hex())
		}
		if c.Message, err = cpStrFrom(cr.Fields[8]); err != nil {
			return out, err
		}
		count, ok := cr.Fields[9].(codec.Int32Value)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: commit %s has no operation count", c.Hash.Hex())
		}
		out.State.Commits = append(out.State.Commits,
			dag.CheckpointCommit{Commit: c, OperationCount: int(count.V)})
	}

	branchesArr, _ := rec.Fields[5].(codec.ArrayValue)
	for _, el := range branchesArr.Elements {
		br, ok := el.(codec.RecordValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: malformed branch entry")
		}
		var b document.Branch
		b.NamespaceID = ns
		if b.Name, err = cpStrFrom(br.Fields[1]); err != nil {
			return out, err
		}
		if b.HeadHash, err = cpHashFrom(br.Fields[2]); err != nil {
			return out, err
		}
		if b.CreatedAt, err = cpTsFrom(br.Fields[3]); err != nil {
			return out, err
		}
		if b.UpdatedAt, err = cpTsFrom(br.Fields[4]); err != nil {
			return out, err
		}
		out.State.Branches = append(out.State.Branches, b)
	}

	tagsArr, _ := rec.Fields[6].(codec.ArrayValue)
	for _, el := range tagsArr.Elements {
		tr, ok := el.(codec.RecordValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: malformed tag entry")
		}
		var t document.Tag
		t.NamespaceID = ns
		if t.Name, err = cpStrFrom(tr.Fields[1]); err != nil {
			return out, err
		}
		if t.CommitHash, err = cpHashFrom(tr.Fields[2]); err != nil {
			return out, err
		}
		if t.CreatedAt, err = cpTsFrom(tr.Fields[3]); err != nil {
			return out, err
		}
		if t.Message, err = cpStrFrom(tr.Fields[4]); err != nil {
			return out, err
		}
		out.State.Tags = append(out.State.Tags, t)
	}

	entriesArr, _ := rec.Fields[7].(codec.ArrayValue)
	live := make(map[codec.UUID]codec.Hash, len(entriesArr.Elements))
	for _, el := range entriesArr.Elements {
		er, ok := el.(codec.RecordValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: malformed tree entry")
		}
		id, err := cpUUIDFrom(er.Fields[1])
		if err != nil {
			return out, err
		}
		h, err := cpHashFrom(er.Fields[2])
		if err != nil {
			return out, err
		}
		live[id] = h
	}
	tree, err := document.BuildDocumentTree(live)
	if err != nil {
		return out, err
	}
	out.LiveTree = tree

	segsArr, _ := rec.Fields[8].(codec.ArrayValue)
	for _, el := range segsArr.Elements {
		sr, ok := el.(codec.RecordValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: malformed segment entry")
		}
		seq, ok := sr.Fields[1].(codec.Int64Value)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: segment entry has no sequence")
		}
		size, ok := sr.Fields[2].(codec.Int64Value)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: segment entry has no size")
		}
		last, err := cpHashFrom(sr.Fields[3])
		if err != nil {
			return out, err
		}
		out.Segments = append(out.Segments, checkpointSegment{
			Sequence: seq.V, SizeBytes: size.V, LastCommitHash: last,
		})
	}
	headsArr, _ := rec.Fields[9].(codec.ArrayValue)
	for _, el := range headsArr.Elements {
		b, ok := el.(codec.BytesValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: malformed head commit payload")
		}
		out.HeadCommits = append(out.HeadCommits, b.V)
	}
	return out, nil
}

// segmentFingerprints reads the current shape of every delta segment up to
// and including through. Uses the reader's own listing, which open already
// performs, so this adds no I/O of its own.
func segmentFingerprints(r storage.DeltaSegmentReader, through int64) ([]checkpointSegment, error) {
	if r == nil {
		return nil, nil
	}
	segments, err := r.ListSegments()
	if err != nil {
		return nil, err
	}
	var out []checkpointSegment
	for _, seg := range segments {
		if seg.SequenceNumber > through {
			continue
		}
		out = append(out, checkpointSegment{
			Sequence:       seg.SequenceNumber,
			SizeBytes:      seg.SizeBytes,
			LastCommitHash: seg.LastCommitHash,
		})
	}
	return out, nil
}

// checkpointMatchesLog reports whether every segment the checkpoint claims
// to cover is still present and unchanged. A false here means the
// checkpoint is not trustworthy and the caller must replay the log, which
// is also what surfaces the underlying damage.
func checkpointMatchesLog(cp namespaceCheckpoint, r storage.DeltaSegmentReader) (bool, string) {
	current, err := segmentFingerprints(r, cp.ThroughSequence)
	if err != nil {
		return false, "the delta segments could not be listed"
	}
	bySeq := make(map[int64]checkpointSegment, len(current))
	for _, sg := range current {
		bySeq[sg.Sequence] = sg
	}
	for _, want := range cp.Segments {
		got, ok := bySeq[want.Sequence]
		if !ok {
			return false, fmt.Sprintf("delta segment %d is missing", want.Sequence)
		}
		if got.SizeBytes != want.SizeBytes {
			return false, fmt.Sprintf(
				"delta segment %d is %d bytes but was %d when the checkpoint was written",
				want.Sequence, got.SizeBytes, want.SizeBytes)
		}
		if got.LastCommitHash != want.LastCommitHash {
			return false, fmt.Sprintf("delta segment %d ends at a different commit than it did", want.Sequence)
		}
	}
	return true, ""
}

// readCheckpoint loads the namespace's checkpoint, or reports that there
// is nothing usable there.
//
// Every failure short of an I/O fault is "nothing usable" rather than an
// error, and that is the whole safety story for this feature: the delta
// log remains the source of truth, a checkpoint is only ever a way to
// avoid re-reading it, and anything unreadable, stale-format, corrupt or
// mismatched simply costs one slow start.
func readCheckpoint(shim storage.PlatformIOShim, namespaceID string) (namespaceCheckpoint, bool) {
	raw, err := shim.ReadSnapshot(checkpointKey(namespaceID))
	if err != nil || len(raw) == 0 {
		return namespaceCheckpoint{}, false
	}
	cp, err := decodeCheckpoint(namespaceID, raw)
	if err != nil {
		return namespaceCheckpoint{}, false
	}
	return cp, true
}

func writeCheckpoint(shim storage.PlatformIOShim, cp namespaceCheckpoint) error {
	raw, err := encodeCheckpoint(cp)
	if err != nil {
		return err
	}
	return shim.WriteSnapshot(checkpointKey(cp.NamespaceID), raw)
}

// highestSegmentSequence reports the newest delta segment sequence a
// reader can currently see, which is the point a checkpoint taken now is
// good through. -1 when the namespace has no segments at all.
func highestSegmentSequence(r storage.DeltaSegmentReader) int64 {
	if r == nil {
		return -1
	}
	segments, err := r.ListSegments()
	if err != nil {
		return -1
	}
	highest := int64(-1)
	for _, seg := range segments {
		if seg.SequenceNumber > highest {
			highest = seg.SequenceNumber
		}
	}
	return highest
}
