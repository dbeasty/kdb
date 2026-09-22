package document

import (
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func TestTreeProofsProvePresenceAndAbsence(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for _, n := range []int{0, 1, 2, 3, 50, 2000} {
		entries := randomEntries(r, n)
		tree := mustTree(t, entries)
		for id, h := range entries {
			got, present, err := VerifyTreeProof(tree.TreeHash, tree.Proof(id))
			if err != nil || !present || got != h {
				t.Fatalf("n=%d: %s should prove present with %s: %s %v %v", n, id, h.Hex(), got.Hex(), present, err)
			}
		}
		for i := 0; i < 50; i++ {
			id := codec.UUID{MSB: r.Int63(), LSB: r.Int63()}
			if _, in := entries[id]; in {
				continue
			}
			if _, present, err := VerifyTreeProof(tree.TreeHash, tree.Proof(id)); err != nil || present {
				t.Fatalf("n=%d: %s should prove absent: present=%v err=%v", n, id, present, err)
			}
		}
	}
}

// A neighbour sharing all but the last nibble forces the deepest possible path.
func TestTreeProofAtFullDepth(t *testing.T) {
	a := codec.UUID{MSB: 0x0123456789abcdef, LSB: 0x0fedcba987654320}
	b := codec.UUID{MSB: a.MSB, LSB: a.LSB | 1}
	var h codec.Hash
	h.Bytes[0] = 9
	tree := mustTree(t, map[codec.UUID]codec.Hash{a: h, b: h})
	for _, id := range []codec.UUID{a, b} {
		p := tree.Proof(id)
		if len(p.Levels) != 32 {
			t.Fatalf("expected a 32-level path, got %d", len(p.Levels))
		}
		if got, present, err := VerifyTreeProof(tree.TreeHash, p); err != nil || !present || got != h {
			t.Fatalf("full-depth proof: %v %v", present, err)
		}
	}
}

func TestTamperedProofsAreRejected(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	entries := randomEntries(r, 300)
	tree := mustTree(t, entries)
	var id codec.UUID
	var h codec.Hash
	for id, h = range entries {
		break
	}
	good := tree.Proof(id)

	// Claiming a different content hash for a present document.
	forged := good
	forged.LeafHash.Bytes[0] ^= 1
	if _, _, err := VerifyTreeProof(tree.TreeHash, forged); err == nil {
		t.Fatal("a forged leaf hash must not verify")
	}
	// Altering any sibling hash on the path.
	for d := range good.Levels {
		bad := good
		bad.Levels = append([][16]codec.Hash(nil), good.Levels...)
		slot := (nibbleAt(id.Bytes(), d) + 1) % 16
		bad.Levels[d][slot].Bytes[5] ^= 1
		if _, _, err := VerifyTreeProof(tree.TreeHash, bad); err == nil {
			t.Fatalf("an altered sibling at depth %d must not verify", d)
		}
	}
	// Presenting a present document as absent by dropping its leaf.
	hidden := good
	hidden.HasLeaf = false
	if _, present, err := VerifyTreeProof(tree.TreeHash, hidden); err == nil || present {
		t.Fatal("hiding a present document must not verify as absent")
	}
	// A proof checked against another tree.
	other := mustTree(t, randomEntries(r, 10))
	if _, _, err := VerifyTreeProof(other.TreeHash, good); err == nil {
		t.Fatal("a proof must not verify against a different tree")
	}
	// Moving the leaf off the path.
	moved := good
	moved.LeafID = codec.UUID{MSB: ^id.MSB, LSB: id.LSB}
	if _, _, err := VerifyTreeProof(tree.TreeHash, moved); err == nil {
		t.Fatal("a leaf off the id's path must not verify")
	}
	_ = h
}
