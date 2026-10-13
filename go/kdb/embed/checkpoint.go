package embed

import (
	"fmt"
	"strings"
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
const checkpointFormatVersion = 2

// checkpointGraphFormatVersion is a checkpoint that references graph segments instead of carrying
// the commits in them (fields 11 and 12). Its own version so that a build that predates it reads
// it as unusable and replays the log, rather than restoring a graph with most of its commits
// missing. Written only when there are segments to reference; otherwise version 2, unchanged.
const checkpointGraphFormatVersion = 3

// checkpointPackedFormatVersion stores the live tree (13) and the commits (14) as packed byte
// fields instead of a codec record per entry (7, 4), and makes the graph fields (11, 12)
// optional. A million live documents as records cost ~900 MB to encode at every checkpoint and
// most of an open's peak heap to decode; packed they are 48 MB and decode straight into the bulk
// trie build. Commits likewise: two million as records were gigabytes each way; packed they are
// ~150 bytes each and restored one at a time.
// What this build writes. Its own version so an older build replays the log rather than restoring
// an empty live tree from a field it does not know.
const checkpointPackedFormatVersion = 4

const checkpointNS = "dev.kdb.checkpoint"

var (
	fqnCheckpoint       = checkpointNS + ".Checkpoint"
	fqnCheckpointCommit = checkpointNS + ".CheckpointCommit"
	fqnCheckpointBranch = checkpointNS + ".CheckpointBranch"
	fqnCheckpointTag    = checkpointNS + ".CheckpointTag"
	fqnCheckpointEntry  = checkpointNS + ".CheckpointTreeEntry"
	fqnCheckpointSeg    = checkpointNS + ".CheckpointSegment"
	fqnCheckpointHash   = checkpointNS + ".Hash32"
	fqnCheckpointGraph  = checkpointNS + ".CheckpointGraphSegment"
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
				// Optional, absent from checkpoints written before stamps: see checkpointSegment.
				{ID: 4, Name: "stampSize", Type: cpI64, Default: codec.Int64Value{V: -1}},
				{ID: 5, Name: "stampModTimeNanos", Type: cpI64, Default: codec.Int64Value{V: 0}},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CheckpointGraphSegment", Namespace: checkpointNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "sequence", Type: cpI64},
				{ID: 2, Name: "crc", Type: cpI64},
				{ID: 3, Name: "records", Type: cpI64},
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
				// The lowest delta segment sequence still on disk when
				// this checkpoint was written. Zero for a namespace that
				// has never truncated, which is every namespace under
				// history=full.
				//
				// This is what lets open tell a deliberately truncated log
				// from a damaged one. Without it, a missing low segment is
				// indistinguishable from a deleted file, and the only safe
				// reading of an ambiguous log is the pessimistic one.
				{ID: 10, Name: "floorSequence", Type: cpI64},
				// Version 3 only: the graph segments holding every commit not in "commits", and
				// the records in them for commits since deleted. See dag.CheckpointSnapshotWithGraph.
				{ID: 11, Name: "graphSegments", Type: schema.Array{Element: schema.Ref{FullyQualifiedName: fqnCheckpointGraph}}, Default: codec.ArrayValue{}},
				{ID: 12, Name: "graphHidden", Type: schema.Array{Element: cpHash}, Default: codec.ArrayValue{}},
				// Version 4: the live tree, packed - see checkpointPackedFormatVersion.
				{ID: 13, Name: "liveTreePacked", Type: schema.Prim(schema.PhysicalBytes), Default: codec.BytesValue{}},
				// Version 4: the commits, packed - see dag/checkpoint_packed.go.
				{ID: 14, Name: "commitsPacked", Type: schema.Prim(schema.PhysicalBytes), Default: codec.BytesValue{}},
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
	// FloorSequence is the lowest segment sequence still on disk. Anything
	// below it was deleted on purpose; anything missing above it is
	// damage. See checkpointMatchesLog.
	FloorSequence int64
	State         dag.CheckpointState
	LiveTree      document.DocumentTree
	Segments      []checkpointSegment
	HeadCommits   [][]byte
	// GraphRefs, when set, are the graph segments holding every commit State.Commits leaves
	// out; GraphHidden the records in them for commits since deleted. History=full only.
	GraphRefs   []dag.GraphSegmentRef
	GraphHidden []codec.Hash
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
	// Stamp is the segment's size and modification time when the checkpoint was written
	// (storage.SegmentStamp), when the store could vouch for it. A segment whose stamp is still
	// the same at open is the segment that was fingerprinted, so open need not scan it again -
	// which, before stamps, meant reading the whole delta log on every open.
	Stamp    storage.SegmentStamp
	HasStamp bool
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
	// Commits are packed (dag/checkpoint_packed.go): a snapshot hands them over packed already, and
	// any listed as structs are packed here, so a checkpoint never holds a codec record per commit.
	packedCommits := cp.State.PackedCommits
	if len(cp.State.Commits) > 0 {
		more, err := dag.PackCheckpointCommits(cp.State.Commits)
		if err != nil {
			return nil, err
		}
		packedCommits = append(append([]byte(nil), packedCommits...), more...)
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

	packedTree := cp.LiveTree.AppendPacked(make([]byte, 0, cp.LiveTree.Size()*document.PackedTreeEntrySize))

	segs := make([]codec.Value, 0, len(cp.Segments))
	for _, sg := range cp.Segments {
		fields := map[int]codec.Value{
			1: codec.Int64Value{V: sg.Sequence},
			2: codec.Int64Value{V: sg.SizeBytes},
			3: cpHashVal(sg.LastCommitHash),
		}
		if sg.HasStamp {
			fields[4] = codec.Int64Value{V: sg.Stamp.Size}
			fields[5] = codec.Int64Value{V: sg.Stamp.ModTimeNanos}
		}
		segs = append(segs, codec.RecordValue{Fields: fields})
	}

	heads := make([]codec.Value, 0, len(cp.HeadCommits))
	for _, b := range cp.HeadCommits {
		heads = append(heads, codec.BytesValue{V: b})
	}

	version := int32(checkpointPackedFormatVersion)
	root := codec.RecordValue{Fields: map[int]codec.Value{
		1:  codec.Int32Value{V: version},
		2:  codec.StringValue{V: cp.NamespaceID},
		3:  codec.Int64Value{V: cp.ThroughSequence},
		4:  codec.ArrayValue{},
		5:  codec.ArrayValue{Elements: branches},
		6:  codec.ArrayValue{Elements: tags},
		7:  codec.ArrayValue{},
		8:  codec.ArrayValue{Elements: segs},
		9:  codec.ArrayValue{Elements: heads},
		10: codec.Int64Value{V: cp.FloorSequence},
		13: codec.BytesValue{V: packedTree},
		14: codec.BytesValue{V: packedCommits},
	}}
	if len(cp.GraphRefs) > 0 {
		refs := make([]codec.Value, 0, len(cp.GraphRefs))
		for _, r := range cp.GraphRefs {
			refs = append(refs, codec.RecordValue{Fields: map[int]codec.Value{
				1: codec.Int64Value{V: int64(r.Seq)},
				2: codec.Int64Value{V: int64(r.CRC)},
				3: codec.Int64Value{V: int64(r.Records)},
			}})
		}
		hidden := make([]codec.Value, 0, len(cp.GraphHidden))
		for _, h := range cp.GraphHidden {
			hidden = append(hidden, cpHashVal(h))
		}
		root.Fields[11] = codec.ArrayValue{Elements: refs}
		root.Fields[12] = codec.ArrayValue{Elements: hidden}
	}
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
	if version.V != checkpointFormatVersion && version.V != checkpointGraphFormatVersion && version.V != checkpointPackedFormatVersion {
		return out, fmt.Errorf(
			"kdb: checkpoint is format version %d, this build reads versions %d-%d",
			version.V, checkpointFormatVersion, checkpointPackedFormatVersion)
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

	if pc, ok := rec.Fields[14].(codec.BytesValue); ok && version.V >= checkpointPackedFormatVersion {
		// Kept packed: RestoreCheckpoint decodes one commit at a time, so the list never exists
		// as structs.
		out.State.PackedCommits = pc.V
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
	if packed, ok := rec.Fields[13].(codec.BytesValue); ok && version.V >= checkpointPackedFormatVersion {
		if len(live) > 0 {
			return out, fmt.Errorf("kdb: checkpoint: live tree stored both packed and as records")
		}
		tree, err := document.BuildTrieDocumentTreePacked(packed.V)
		if err != nil {
			return out, err
		}
		out.LiveTree = tree
	} else {
		tree, err := document.BuildTrieDocumentTree(live)
		if err != nil {
			return out, err
		}
		out.LiveTree = tree
	}

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
		fp := checkpointSegment{Sequence: seq.V, SizeBytes: size.V, LastCommitHash: last}
		if st, ok := sr.Fields[4].(codec.Int64Value); ok && st.V >= 0 {
			if mt, ok := sr.Fields[5].(codec.Int64Value); ok {
				fp.Stamp = storage.SegmentStamp{Size: st.V, ModTimeNanos: mt.V}
				fp.HasStamp = true
			}
		}
		out.Segments = append(out.Segments, fp)
	}
	headsArr, _ := rec.Fields[9].(codec.ArrayValue)
	for _, el := range headsArr.Elements {
		b, ok := el.(codec.BytesValue)
		if !ok {
			return out, fmt.Errorf("kdb: checkpoint: malformed head commit payload")
		}
		out.HeadCommits = append(out.HeadCommits, b.V)
	}
	if floor, ok := rec.Fields[10].(codec.Int64Value); ok {
		out.FloorSequence = floor.V
	}
	if version.V >= checkpointGraphFormatVersion {
		refsArr, _ := rec.Fields[11].(codec.ArrayValue)
		for _, el := range refsArr.Elements {
			rr, ok := el.(codec.RecordValue)
			if !ok {
				return out, fmt.Errorf("kdb: checkpoint: malformed graph segment entry")
			}
			seq, ok1 := rr.Fields[1].(codec.Int64Value)
			crc, ok2 := rr.Fields[2].(codec.Int64Value)
			n, ok3 := rr.Fields[3].(codec.Int64Value)
			if !ok1 || !ok2 || !ok3 {
				return out, fmt.Errorf("kdb: checkpoint: malformed graph segment entry")
			}
			out.GraphRefs = append(out.GraphRefs, dag.GraphSegmentRef{Seq: uint64(seq.V), CRC: uint32(crc.V), Records: int(n.V)})
		}
		if len(out.GraphRefs) == 0 && version.V == checkpointGraphFormatVersion {
			return out, fmt.Errorf("kdb: checkpoint: version %d with no graph segments", version.V)
		}
		hiddenArr, _ := rec.Fields[12].(codec.ArrayValue)
		for _, el := range hiddenArr.Elements {
			h, err := cpHashFrom(el)
			if err != nil {
				return out, err
			}
			out.GraphHidden = append(out.GraphHidden, h)
		}
	}
	return out, nil
}

// segmentFingerprints reads the current shape of every delta segment up to
// and including through. Uses the reader's own listing, which open already
// performs, so this adds no I/O of its own.
// rememberedFingerprints fingerprints the segments through `through` reading only those whose
// fingerprint the reader cannot vouch for from a stamp (storage.DeltaFingerprintMemo). ok is false
// when the reader cannot do this, or any segment could not be scanned on its own - the caller then
// takes the full listing, which owns every rule about missing and unreadable segments.
func rememberedFingerprints(r storage.DeltaSegmentReader, through int64) ([]checkpointSegment, bool) {
	memo, ok1 := r.(storage.DeltaFingerprintMemo)
	lister, ok2 := r.(storage.DeltaSegmentSequenceLister)
	stamper, ok3 := r.(storage.DeltaSegmentStamper)
	if !ok1 || !ok2 || !ok3 {
		return nil, false
	}
	seqs, err := lister.SegmentSequences()
	if err != nil {
		return nil, false
	}
	var out []checkpointSegment
	for _, seq := range seqs {
		if seq > through {
			continue
		}
		if stamp, size, last, ok := memo.RememberedFingerprint(seq); ok {
			out = append(out, checkpointSegment{Sequence: seq, SizeBytes: size, LastCommitHash: last, Stamp: stamp, HasStamp: true})
			continue
		}
		ref, err := memo.SegmentRef(seq)
		if err != nil {
			return nil, false
		}
		fp := checkpointSegment{Sequence: seq, SizeBytes: ref.SizeBytes, LastCommitHash: ref.LastCommitHash}
		if st, ok, err := stamper.StampSegment(seq); err == nil && ok && st.Size >= ref.SizeBytes {
			fp.Stamp, fp.HasStamp = st, true
		}
		out = append(out, fp)
	}
	return out, true
}

// rememberCheckpointFingerprints tells the reader what the stamped segments of cp looked like, so
// later checkpoints in this process need not scan them while their stamps hold.
func rememberCheckpointFingerprints(r storage.DeltaSegmentReader, segs []checkpointSegment) {
	memo, ok := r.(storage.DeltaFingerprintMemo)
	if !ok {
		return
	}
	for _, sg := range segs {
		if sg.HasStamp {
			memo.RememberFingerprint(sg.Sequence, sg.Stamp, sg.SizeBytes, sg.LastCommitHash)
		}
	}
}

func segmentFingerprints(r storage.DeltaSegmentReader, through int64) ([]checkpointSegment, error) {
	if r == nil {
		return nil, nil
	}
	if fps, ok := rememberedFingerprints(r, through); ok {
		return fps, nil
	}
	segments, err := r.ListSegments()
	if err != nil {
		return nil, err
	}
	stamper, canStamp := r.(storage.DeltaSegmentStamper)
	var out []checkpointSegment
	for _, seg := range segments {
		if seg.SequenceNumber > through {
			continue
		}
		fp := checkpointSegment{
			Sequence:       seg.SequenceNumber,
			SizeBytes:      seg.SizeBytes,
			LastCommitHash: seg.LastCommitHash,
		}
		if canStamp {
			// Stamped after listing: a segment that changed in between gets a stamp that will
			// not match the fingerprint's contents at open, which costs a scan, never a miss -
			// the mismatch is on the side of scanning.
			if st, ok, err := stamper.StampSegment(seg.SequenceNumber); err == nil && ok && st.Size >= seg.SizeBytes {
				fp.Stamp, fp.HasStamp = st, true
			}
		}
		out = append(out, fp)
	}
	return out, nil
}

// checkpointMatchesLog reports whether every segment the checkpoint claims
// to cover is still present and unchanged. A false here means the
// checkpoint is not trustworthy and the caller must replay the log, which
// is also what surfaces the underlying damage.
// A segment below the checkpoint's recorded floor is *expected* to be
// absent: that is what truncation under history=none does, and the floor
// is the record of it. A segment missing at or above the floor is damage,
// and still fails the check. Getting this distinction wrong in either
// direction is the worst failure available here - too strict and a
// truncated namespace replays its whole (now incomplete) log on every
// open, too loose and real damage opens cleanly with data silently gone.
// unchangedSinceCheckpoint reports, without reading any segment, that every segment the checkpoint
// fingerprinted is still there with the stamp it had then, and that no segment it covers has
// appeared since - in which case the log says exactly what it said and the fingerprints match.
// Any doubt (a missing stamp, a store that cannot stamp, any difference at all) is false, and the
// caller does the full comparison, damage handling included.
func unchangedSinceCheckpoint(cp namespaceCheckpoint, r storage.DeltaSegmentReader) bool {
	stamper, ok1 := r.(storage.DeltaSegmentStamper)
	lister, ok2 := r.(storage.DeltaSegmentSequenceLister)
	if !ok1 || !ok2 {
		return false
	}
	seqs, err := lister.SegmentSequences()
	if err != nil {
		return false
	}
	wanted := make(map[int64]bool, len(cp.Segments))
	for _, want := range cp.Segments {
		if want.Sequence < cp.FloorSequence {
			wanted[want.Sequence] = true
			continue // deliberately truncated; checkpointMatchesLog skips these too
		}
		if !want.HasStamp {
			return false
		}
		st, ok, err := stamper.StampSegment(want.Sequence)
		if err != nil || !ok || st != want.Stamp {
			return false
		}
		wanted[want.Sequence] = true
	}
	for _, seq := range seqs {
		if seq <= cp.ThroughSequence && seq >= cp.FloorSequence && !wanted[seq] {
			return false // a segment the checkpoint covers but never fingerprinted
		}
	}
	return true
}

func checkpointMatchesLog(cp namespaceCheckpoint, r storage.DeltaSegmentReader) (bool, string) {
	if unchangedSinceCheckpoint(cp, r) {
		// Trusted on stamps, so nothing was scanned; hand the fingerprints to the reader, or the
		// next checkpoint would scan every segment to rediscover them.
		rememberCheckpointFingerprints(r, cp.Segments)
		return true, ""
	}
	current, err := segmentFingerprints(r, cp.ThroughSequence)
	if err != nil {
		return false, "the delta segments could not be listed"
	}
	bySeq := make(map[int64]checkpointSegment, len(current))
	for _, sg := range current {
		bySeq[sg.Sequence] = sg
	}
	var damaged []string
	for _, want := range cp.Segments {
		if want.Sequence < cp.FloorSequence {
			// Deliberately truncated. Its commits are accounted for by the
			// checkpoint's own state and its documents by the blob store.
			continue
		}
		got, ok := bySeq[want.Sequence]
		if !ok {
			return false, fmt.Sprintf("delta segment %d is missing", want.Sequence)
		}
		if got.SizeBytes == want.SizeBytes && got.LastCommitHash == want.LastCommitHash {
			continue
		}
		// A segment's listing stops at its first damaged frame, so a flipped bit in the middle of
		// it reads here exactly like truncation. Tell them apart before distrusting the
		// checkpoint: if the file is the same length and its intact frames still end at the
		// commit the checkpoint recorded, the log was damaged in place, not replaced - and the
		// checkpoint, which already holds everything the damaged frames said, is the better
		// source. Replaying instead would stop at the damage and silently drop every commit after
		// it (and the next checkpoint would make that permanent). What the damage costs is only
		// the bodies in the damaged frames, which a scrub finds and repairs from a peer.
		if skim, ok := damagedInPlace(r, want); ok {
			what := fmt.Sprintf("delta segment %d has %d damaged frame(s) at offset(s) %v", want.Sequence, len(skim.DamagedFrames), skim.DamagedFrames)
			if skim.TrailingUnreadable {
				what += fmt.Sprintf(" and unreadable bytes after its last frame (from offset %d)", skim.FramesEnd)
			}
			damaged = append(damaged, what)
			continue
		}
		if got.SizeBytes != want.SizeBytes {
			return false, fmt.Sprintf(
				"delta segment %d is %d bytes but was %d when the checkpoint was written",
				want.Sequence, got.SizeBytes, want.SizeBytes)
		}
		return false, fmt.Sprintf("delta segment %d ends at a different commit than it did", want.Sequence)
	}
	if len(damaged) > 0 {
		return true, strings.Join(damaged, "; ") +
			" - opening from the checkpoint, which holds what they recorded; documents whose bodies were only in them are unreadable until a scrub repairs them from a peer"
	}
	return true, ""
}

// damagedInPlace reports whether the segment the checkpoint recorded as want is still that
// segment, with frames damaged in place: the same length on disk, damaged frames, and intact
// frames that still end at the checkpoint's commit.
func damagedInPlace(r storage.DeltaSegmentReader, want checkpointSegment) (storage.SegmentSkim, bool) {
	dr, ok := r.(storage.DamageTolerantReader)
	if !ok {
		return storage.SegmentSkim{}, false
	}
	segments, err := r.ListSegments()
	if err != nil {
		return storage.SegmentSkim{}, false
	}
	for _, seg := range segments {
		if seg.SequenceNumber != want.Sequence {
			continue
		}
		skim, err := dr.SkimSegment(seg)
		if err != nil {
			return skim, false
		}
		// The checkpoint recorded where this segment's intact frames ended when it was written
		// (want.SizeBytes). A torn tail already there then lies past that point, so unreadable
		// bytes before it were changed in place, after the checkpoint read them.
		//
		// The file must still be that segment, not a shorter or older copy: the same length (or,
		// for a preallocated segment, whose file is always full size, frames still ending at the
		// same byte), and the intact frames read past the damage must still end at the
		// checkpoint's last commit. Or the last frame is itself the damaged one: as a complete
		// frame failing its CRC, or - a header too damaged to read as a frame - as unreadable bytes
		// up to the end of a file of the recorded length.
		samePlace := skim.PhysicalSize == want.SizeBytes || skim.FramesEnd == want.SizeBytes
		damagedTail := skim.TrailingUnreadable && skim.PhysicalSize == want.SizeBytes
		if len(skim.DamagedFrames) == 0 && !damagedTail {
			return skim, false
		}
		return skim, samePlace &&
			(skim.LastCommitHash == want.LastCommitHash || skim.LastFrameDamaged || damagedTail)
	}
	return storage.SegmentSkim{}, false
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

// lowestSegmentSequence reports the oldest delta segment sequence still on
// disk, or -1 when there are none.
//
// Sequences are assigned from zero and never reused
// (delta.scanExistingDeltaSequence), so a lowest sequence above zero means
// segments have been deleted - which is the one thing open needs to know
// before it decides whether replaying "the whole log" would actually be
// replaying the whole log.
func lowestSegmentSequence(r storage.DeltaSegmentReader) int64 {
	if r == nil {
		return -1
	}
	if sq, ok := r.(storage.DeltaSegmentSequenceLister); ok {
		// Names only. Listing scanned every segment to answer a question the file names
		// already answer, and this runs several times per checkpoint.
		if seqs, err := sq.SegmentSequences(); err == nil {
			if len(seqs) == 0 {
				return -1
			}
			return seqs[0]
		}
	}
	segments, err := r.ListSegments()
	if err != nil || len(segments) == 0 {
		return -1
	}
	low := segments[0].SequenceNumber
	for _, s := range segments[1:] {
		if s.SequenceNumber < low {
			low = s.SequenceNumber
		}
	}
	return low
}

// highestSegmentSequence reports the newest delta segment sequence a
// reader can currently see, which is the point a checkpoint taken now is
// good through. -1 when the namespace has no segments at all.
func highestSegmentSequence(r storage.DeltaSegmentReader) int64 {
	if r == nil {
		return -1
	}
	if sq, ok := r.(storage.DeltaSegmentSequenceLister); ok {
		if seqs, err := sq.SegmentSequences(); err == nil {
			if len(seqs) == 0 {
				return -1
			}
			return seqs[len(seqs)-1]
		}
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
