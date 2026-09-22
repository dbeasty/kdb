package peersync

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

// genuineDocFetch builds the answer an honest host gives for ids at a's head.
func genuineDocFetch(t *testing.T, ns string, a authNode, ids []codec.UUID) (wire.DocFetchResultMessage, map[string]codec.UUID) {
	t.Helper()
	_, commit, _, err := a.dag.HeadCommit()
	if err != nil {
		t.Fatal(err)
	}
	tree, err := treeAt(a.storage, ns, commit.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	res := wire.DocFetchResultMessage{Namespace: ns, Commit: commit}
	asked := map[string]codec.UUID{}
	for _, id := range ids {
		asked[id.String()] = id
		fd := wire.FetchedDoc{DocID: id.String(), Proof: proofToWire(tree.Proof(id))}
		if _, ok := tree.HashFor(id); ok {
			doc, err := a.storage.GetDocument(ns, id, commit.DocumentTreeHash)
			if err != nil || doc == nil {
				t.Fatal(err)
			}
			fd.Body, fd.Present = doc.JSON, true
		}
		res.Docs = append(res.Docs, fd)
	}
	return res, asked
}

func cloneDocFetch(r wire.DocFetchResultMessage) wire.DocFetchResultMessage {
	out := r
	out.Docs = append([]wire.FetchedDoc(nil), r.Docs...)
	for i := range out.Docs {
		p := out.Docs[i].Proof
		levels := make([][]string, len(p.Levels))
		for j := range p.Levels {
			levels[j] = append([]string(nil), p.Levels[j]...)
		}
		out.Docs[i].Proof.Levels = levels
	}
	return out
}

// Every way a source could lie in a DOC_FETCH answer is refused, whole.
func TestDocFetchAnswersAreVerified(t *testing.T) {
	ns := "app/docfetch"
	a := newAuthNodes(t, ns, 1)[0]
	present, gone := newUUID(t), newUUID(t)
	for i := 0; i < 40; i++ {
		a.write(t, ns, newUUID(t), `{"filler":true}`)
	}
	a.write(t, ns, present, `{"v":"real"}`)
	good, asked := genuineDocFetch(t, ns, a, []codec.UUID{present, gone})
	at := good.Commit.Hash.Hex()

	docs, err := verifyDocFetch(good, at, asked)
	if err != nil || !docs[present].Present || docs[present].Body != `{"v":"real"}` || docs[gone].Present {
		t.Fatalf("an honest answer must verify: %+v %v", docs, err)
	}

	for name, tamper := range map[string]func(*wire.DocFetchResultMessage){
		"a different body": func(r *wire.DocFetchResultMessage) { r.Docs[0].Body = `{"v":"forged"}` },
		"a present document hidden": func(r *wire.DocFetchResultMessage) {
			r.Docs[0].Present, r.Docs[0].Body = false, ""
		},
		"an absent document invented": func(r *wire.DocFetchResultMessage) {
			r.Docs[1].Present, r.Docs[1].Body = true, `{"v":"invented"}`
		},
		"a sibling hash altered": func(r *wire.DocFetchResultMessage) {
			row := r.Docs[0].Proof.Levels[0]
			for i := range row {
				if row[i] != "" {
					row[i] = "00" + row[i][2:]
					return
				}
			}
		},
		"an unasked document added": func(r *wire.DocFetchResultMessage) {
			r.Docs = append(r.Docs, wire.FetchedDoc{DocID: newUUID(t).String()})
		},
		"a document left out": func(r *wire.DocFetchResultMessage) { r.Docs = r.Docs[:1] },
		"a commit that is not itself": func(r *wire.DocFetchResultMessage) {
			r.Commit.Message = "tampered"
		},
	} {
		bad := cloneDocFetch(good)
		tamper(&bad)
		if _, err := verifyDocFetch(bad, at, asked); !errors.Is(err, ErrUnproven) {
			t.Errorf("%s: expected ErrUnproven, got %v", name, err)
		}
	}

	// An answer for a different commit than the one asked for.
	a.write(t, ns, newUUID(t), `{"later":true}`)
	later, _ := genuineDocFetch(t, ns, a, []codec.UUID{present, gone})
	if _, err := verifyDocFetch(later, at, asked); !errors.Is(err, ErrUnproven) {
		t.Errorf("an answer at another commit must be refused, got %v", err)
	}
}
