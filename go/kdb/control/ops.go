package control

import "github.com/limidus/kdb/go/kdb/document"

// opView is one operation inside a commit, as the commit detail pane shows it.
//
// A WriteOp's payload is the document body, which for a bulk write is the entire document - so
// the body is deliberately not included here. Bytes is, because "this op wrote 4 KB" is what an
// operator scanning a commit actually wants, and the document itself is one request away by id.
type opView struct {
	Kind  string `json:"kind"`
	DocID string `json:"docId,omitempty"`
	Path  string `json:"path,omitempty"`
	Bytes int    `json:"bytes,omitempty"`
}

func describeOp(op document.Op) opView {
	switch o := op.(type) {
	case document.WriteOp:
		return opView{Kind: "write", DocID: o.DocID.String(), Bytes: len(o.Patch)}
	case document.DeleteOp:
		return opView{Kind: "delete", DocID: o.DocID.String()}
	case document.FileWriteOp:
		return opView{Kind: "fileWrite", Path: o.Path}
	default:
		// A new op kind should show up as something rather than vanish from the list; an operator
		// counting operations against OpCount would otherwise see a silent discrepancy.
		return opView{Kind: "unknown"}
	}
}
