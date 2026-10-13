package engine_test

import (
	"bytes"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
)

// ScanDocumentsAfter streams the same documents ScanDocuments does, in id order, and resumes after
// any key: concatenating the scans after successive keys gives every document exactly once.
func TestServerEngineScanDocumentsAfter(t *testing.T) {
	e, at := smallScanFixture(t, 1000)
	var ordered storage.OrderedScanner = e
	var all []document.Document
	if err := ordered.ScanDocumentsAfter("ns", at, nil, 64, func(batch []document.Document) error {
		all = append(all, batch...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(all) != scanAll(t, e, at) || len(all) != 1000 {
		t.Fatalf("ordered scan saw %d documents, the plain scan %d", len(all), scanAll(t, e, at))
	}
	for i := 1; i < len(all); i++ {
		if bytes.Compare(all[i-1].ID.Bytes(), all[i].ID.Bytes()) >= 0 {
			t.Fatalf("document %d is not after document %d in id order", i, i-1)
		}
	}
	// Resume after every 137th document: each resumed scan starts at the next one.
	var paged []codec.UUID
	var after *codec.UUID
	for {
		var page []codec.UUID
		err := ordered.ScanDocumentsAfter("ns", at, after, 64, func(batch []document.Document) error {
			for _, d := range batch {
				page = append(page, d.ID)
				if len(page) == 137 {
					return errStopPage
				}
			}
			return nil
		})
		if err != nil && err != errStopPage {
			t.Fatal(err)
		}
		paged = append(paged, page...)
		if len(page) < 137 {
			break
		}
		last := page[len(page)-1]
		after = &last
	}
	if len(paged) != 1000 {
		t.Fatalf("resumed scans returned %d documents, want 1000", len(paged))
	}
	for i := range paged {
		if paged[i] != all[i].ID {
			t.Fatalf("resumed scans diverge from one scan at document %d", i)
		}
	}
}

type stopPage struct{}

func (stopPage) Error() string { return "page full" }

var errStopPage error = stopPage{}
