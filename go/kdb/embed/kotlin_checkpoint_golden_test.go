package embed

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Golden checkpoints for Kotlin's checkpoint writer (kdb-jdbc GoCheckpointWriter, its test
// GoCheckpointWriterGoldenTest): for the same state, Kotlin must write the very bytes writeCheckpoint
// writes - the record, and for version 6 its section. Version 2 is a small namespace without
// attachments; version 6 one with them (a snapshot installed from a peer, say). Set
// KDB_PRINT_GOLDEN=1 to print the snapshots after a deliberate change.

func goldenCPHash(seed byte) codec.Hash {
	return codec.Hash{Bytes: sha256.Sum256([]byte{seed})}
}

// goldenCheckpointState is the state both goldens checkpoint: genesis-like base, a root with
// parents this node lacks (a shallow root), a side branch, a tag, five live documents, and the
// root's payload as a head commit. The version 6 one has five documents, the version 2 one one.
func goldenCheckpointState(t *testing.T, attach bool) namespaceCheckpoint {
	t.Helper()
	ns := "app/data"
	// One live document for version 2: it lists the tree in walk order, which for a small tree in
	// memory is map order - any order reads the same, but only one is a golden.
	docs := 5
	if !attach {
		docs = 1
	}
	live := map[codec.UUID]codec.Hash{}
	for i := 1; i <= docs; i++ {
		live[codec.UUID{MSB: int64(0x1000 + i), LSB: int64(i) * -7}] = goldenCPHash(byte(i))
	}
	tree, err := document.BuildDocumentTree(live)
	if err != nil {
		t.Fatal(err)
	}
	schema := goldenCPHash(50)
	var attachRoot *codec.Hash
	if attach {
		h := goldenCPHash(60)
		attachRoot = &h
	}
	ts := codec.TimestampFromEpochMicros(1_760_000_000_123_456)
	base, err := document.BuildCommit(nil, ns, codec.UUID{MSB: 1, LSB: 1}, ts, codec.UUID{MSB: 2, LSB: 2}, nil, goldenCPHash(40), nil, "base")
	if err != nil {
		t.Fatal(err)
	}
	root, err := document.BuildCommitWithAttachments([]codec.Hash{goldenCPHash(41), goldenCPHash(42)}, ns, codec.UUID{MSB: 3, LSB: -3},
		codec.TimestampFromEpochMicros(1_760_000_001_000_999), codec.UUID{MSB: -4, LSB: 4},
		[]document.Op{document.WriteOp{DocID: codec.UUID{MSB: 0x1001, LSB: -7}, Patch: `{"a":1}`}},
		tree.TreeHash, &schema, "snapshot root \u00e9", attachRoot)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := root.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	cp := namespaceCheckpoint{
		NamespaceID: ns, ThroughSequence: 7, FloorSequence: 2,
		State: dag.CheckpointState{
			Commits: []dag.CheckpointCommit{{Commit: base, OperationCount: 0}, {Commit: root, OperationCount: 1}},
			Branches: []document.Branch{
				{Name: "main", NamespaceID: ns, HeadHash: root.Hash, CreatedAt: ts, UpdatedAt: codec.TimestampFromEpochMicros(1_760_000_002_000_000)},
				{Name: "side", NamespaceID: ns, HeadHash: base.Hash, CreatedAt: ts, UpdatedAt: ts},
			},
			Tags: []document.Tag{{Name: "v1", NamespaceID: ns, CommitHash: base.Hash, CreatedAt: ts, Message: "first"}},
		},
		LiveTree: tree,
		Segments: []checkpointSegment{
			{Sequence: 2, SizeBytes: 4096, LastCommitHash: base.Hash},
			{Sequence: 7, SizeBytes: 100, LastCommitHash: root.Hash, HasStamp: true,
				Stamp: storage.SegmentStamp{Size: 123, ModTimeNanos: 1_760_000_003_000_000_001}},
		},
		HeadCommits: [][]byte{payload},
	}
	if attach {
		cp.HasAttachments = true
		cp.AttachmentTables = []byte{1, 2, 3, 4} // opaque here: Kotlin copies field 15 as given
	}
	return cp
}

// kotlinCheckpointGolden is every snapshot each golden checkpoint writes, by key.
var kotlinCheckpointGolden = map[string]map[string]string{
	"v2": {
		"kdb:checkpoint:app/data": "94080104020000000209086170702f6461746103050700000000000000040a028201010f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f020a00030f00000000000000010000000000000001040540e2cfeeb5400600050f00000000000000020000000000000002060f32ebb1abcc1c601ceb9c4e3c4faba0caa5b85bb98c4f1e6612c40faa528a91c908090462617365090400000000f101010f90aebbef33e0c7a5aeb2d566e1181c97a6abf1934183592b44ea47b77429402c020a02ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc1030f0000000000000003fffffffffffffffd04052746ddeeb5400600050ffffffffffffffffc0000000000000004060f950cfa706e1e39c8d7f57676fe67c41698921c82b35b5d971842d283cd7680f1070e01d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35080910736e617073686f7420726f6f7420c3a9090401000000050a023d0109046d61696e020f90aebbef33e0c7a5aeb2d566e1181c97a6abf1934183592b44ea47b77429402c030540e2cfeeb540060004058084eceeb54006003d01090473696465020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb5400600040540e2cfeeb5400600060a01390109027631020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb54006000409056669727374070a0134010f0000000000001001fffffffffffffff9020f4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a080a02360105020000000000000002050010000000000000030f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f4a0105070000000000000002056400000000000000030f90aebbef33e0c7a5aeb2d566e1181c97a6abf1934183592b44ea47b77429402c04057b000000000000000505015e8087adc66c18090a01f701f501010a02ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc10209086170702f64617461030f0000000000000003fffffffffffffffd04052746ddeeb5400600050ffffffffffffffffc0000000000000004060a01001c010f0000000000001001fffffffffffffff90209077b2261223a317d070f950cfa706e1e39c8d7f57676fe67c41698921c82b35b5d971842d283cd7680f1080e01d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35090910736e617073686f7420726f6f7420c3a90a050200000000000000",
	},
	"v6": {
		"kdb:checkpoint-section:app/data:ea3cb52d8ddcd46456784a391b43be81": "4b44424350533031f0000000000000007c010000000000000000000000001001fffffffffffffff94bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a0000000000001002fffffffffffffff2dbc1b4c900ffe48d575b5da5c638040125f65db0fe3e24494b76ea986457d9860000000000001003ffffffffffffffeb084fed08b978af4d7d196a7446a86b58009e636b611db16211b65a9aadff29c50000000000001004ffffffffffffffe4e52d9c508c502347344d8c07ad91cbd6068afc75ff6292f062a09ca381c89e710000000000001005ffffffffffffffdde77b9a9ae9e30b0dbdb6f510a264ef9de781501d7b6b92ae89eb059c5ab743db17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f32ebb1abcc1c601ceb9c4e3c4faba0caa5b85bb98c4f1e6612c40faa528a91c901000000000000000100000000000000020000000000000002000000000000007bc02cc899010000c8010000000000000400000062617365df8ea8449970bba5b6f9f7fa59ce7d60734ae902ec24633713653f6effdfde953da8e3d2db6e75d782544592ac555787cd6069356ac93667b5de9f96f2bb7abe0300000000000000fdfffffffffffffffcffffffffffffff0400000000000000e8c32cc899010000e70303020100000010000000ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc1d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35dabd3aff769f07eb2965401eb029974ebba3407afd02b26ddb564ea5f8efae72736e617073686f7420726f6f7420c3a92a39ad74",
		"kdb:checkpoint:app/data": "98060104060000000209086170702f6461746103050700000000000000040a00050a023d0109046d61696e020fdf8ea8449970bba5b6f9f7fa59ce7d60734ae902ec24633713653f6effdfde95030540e2cfeeb540060004058084eceeb54006003d01090473696465020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb5400600040540e2cfeeb5400600060a01390109027631020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb54006000409056669727374070a00080a02360105020000000000000002050010000000000000030f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f4a0105070000000000000002056400000000000000030fdf8ea8449970bba5b6f9f7fa59ce7d60734ae902ec24633713653f6effdfde9504057b000000000000000505015e8087adc66c18090a019a029802010a02ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc10209086170702f64617461030f0000000000000003fffffffffffffffd04052746ddeeb5400600050ffffffffffffffffc0000000000000004060a01001c010f0000000000001001fffffffffffffff90209077b2261223a317d070f3da8e3d2db6e75d782544592ac555787cd6069356ac93667b5de9f96f2bb7abe080e01d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35090910736e617073686f7420726f6f7420c3a90a0e01dabd3aff769f07eb2965401eb029974ebba3407afd02b26ddb564ea5f8efae720a0502000000000000000f0804010203041009406b64623a636865636b706f696e742d73656374696f6e3a6170702f646174613a6561336362353264386464636434363435363738346133393162343362653831110fa235a37fa92db1b451c907150bea0d8d55d4acd0f0f87833b5298d09f127bcae120505000000000000001305f00000000000000014057c01000000000000150101",
	},
}

func TestKotlinCheckpointWriterGoldens(t *testing.T) {
	print := os.Getenv("KDB_PRINT_GOLDEN") != ""
	for _, c := range []struct {
		name   string
		attach bool
	}{{"v2", false}, {"v6", true}} {
		shim := storio.NewInMemoryPlatformIO()
		if err := writeCheckpoint(shim, goldenCheckpointState(t, c.attach)); err != nil {
			t.Fatal(err)
		}
		keys := []string{checkpointKey("app/data")}
		if sk := currentSectionKey(shim, "app/data"); sk != "" {
			keys = append(keys, sk)
		}
		sort.Strings(keys)
		got := map[string]string{}
		for _, k := range keys {
			b, err := shim.ReadSnapshot(k)
			if err != nil {
				t.Fatal(err)
			}
			got[k] = hex.EncodeToString(b)
		}
		if print {
			var lines []string
			for _, k := range keys {
				lines = append(lines, k+"="+got[k])
			}
			t.Logf("%s:\n%s", c.name, strings.Join(lines, "\n"))
			continue
		}
		want := kotlinCheckpointGolden[c.name]
		if len(want) != len(got) {
			t.Fatalf("%s: wrote %d snapshots, golden has %d", c.name, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s: snapshot %s changed:\n %s", c.name, k, got[k])
			}
		}
		// And Go reads back what it wrote.
		if _, ok := readCheckpoint(shim, "app/data"); !ok {
			t.Errorf("%s: the golden checkpoint does not read back", c.name)
		}
	}
}
