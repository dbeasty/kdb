package sql

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// Attachments in SQL (docs/kdb-attachments-plan.md, PR 2).
//
// Writing:
//
//	INSERT INTO t (cols) VALUES (...) WITH ATTACHMENT ['name' =] <bytes> [MIME 'type'] [, ATTACHMENT ...]
//	UPDATE t SET col = v, ATTACHMENT <sel> = <bytes> [MIME 'type'], DROP ATTACHMENT <sel>,
//	             RENAME ATTACHMENT <sel> TO 'new' WHERE ...
//
// <sel> is a name ('photo.png', or a parameter) or ID '<uuid>'. <bytes> is X'hex', FROM_BASE64('..'),
// a parameter bound to bytes (ParamBytes) or to an already uploaded blob (ParamAttachmentRef), or
// STREAM ? - a parameter that must be an uploaded blob, so a statement never carries large bytes
// itself. Each attachment clause of an INSERT applies to every row it inserts. SET ATTACHMENT
// replaces the attachment it selects or, if the document has none, creates one under that name (or
// with that ID); DROP and RENAME of an attachment a document does not have leave it alone.
//
// Reading: the virtual column _attachments is a JSON array of {id, name, size, mime, hash};
// ATTACHMENT_COUNT() counts them; ATTACHMENT(<sel>) is the bytes of one (NULL when absent), up to
// MaxSqlAttachmentReadBytes - bigger ones are read through the attachment API, which streams;
// ATTACHMENT_SIZE(<sel>) and ATTACHMENT_MIME(<sel>) read its metadata.

// MaxSqlAttachmentReadBytes caps what ATTACHMENT() materializes into a result cell.
var MaxSqlAttachmentReadBytes int64 = 16 << 20

// MaxSqlLiteralBytes caps an X'..' or FROM_BASE64 literal: statement text is held whole, so big
// payloads go as parameters or, better, as uploads (STREAM ?).
const MaxSqlLiteralBytes = 1 << 20

const colAttachments = "_attachments"

// CellBytes is a binary value: an attachment read by ATTACHMENT(), an X'..' literal.
type CellBytes struct{ Value []byte }

func (CellBytes) isCell() {}

// ParamBytes binds bytes - an attachment's content, sent inline with the statement.
type ParamBytes struct{ Value []byte }

func (ParamBytes) isParameter() {}

// ParamAttachmentRef binds a blob already uploaded to the namespace's attachment store
// (ATTACHMENT_UPLOAD), by its content hash and size; STREAM ? requires one.
type ParamAttachmentRef struct {
	ContentHash codec.Hash
	Size        int64
}

func (ParamAttachmentRef) isParameter() {}

// cellAttachmentRef is a ParamAttachmentRef in evaluation.
type cellAttachmentRef struct{ ref ParamAttachmentRef }

func (cellAttachmentRef) isCell() {}

// AttachmentBlobs is the namespace attachment store the engine writes and reads blobs through. The
// embedded runtime implements it.
type AttachmentBlobs interface {
	// PutAttachmentBlob stores r's bytes (durably) and returns their content hash and size.
	PutAttachmentBlob(namespaceID string, r io.Reader) (codec.Hash, int64, error)
	// HasAttachmentBlob reports whether a blob is stored.
	HasAttachmentBlob(namespaceID string, h codec.Hash) (bool, error)
	// ReadAttachmentBlob returns a blob's bytes, failing if it is over limit bytes.
	ReadAttachmentBlob(namespaceID string, h codec.Hash, limit int64) ([]byte, error)
}

// attachmentConfigurable is implemented by engines that can be given an attachment store.
type attachmentConfigurable interface {
	setAttachmentBlobs(AttachmentBlobs)
}

// SetAttachmentBlobs gives an engine its namespace attachment store. Without one, statements that
// write or read attachment bytes fail; _attachments and ATTACHMENT_COUNT() still work.
func SetAttachmentBlobs(e Engine, blobs AttachmentBlobs) {
	if c, ok := e.(attachmentConfigurable); ok {
		c.setAttachmentBlobs(blobs)
	}
}

func (e *defaultEngine) setAttachmentBlobs(b AttachmentBlobs) { e.executor.Attachments = b }

// ErrNoAttachmentStore is returned for attachment bytes on an engine without an attachment store.
var ErrNoAttachmentStore = errors.New("sql: this engine has no attachment store")

// AttachmentSel selects one attachment of a document: by Name (an expression yielding a string) or
// by ID (an expression yielding a UUID string). Exactly one is set.
type AttachmentSel struct {
	Name Expr
	ID   Expr
}

// AttachmentSpec is one WITH ATTACHMENT clause of an INSERT.
type AttachmentSpec struct {
	Name  Expr // nil: the attachment's ID becomes its name
	Value Expr
	Mime  Expr // nil: none
}

// AttachmentChangeKind is what an UPDATE does to an attachment.
type AttachmentChangeKind int

const (
	AttachmentSet AttachmentChangeKind = iota
	AttachmentDrop
	AttachmentRename
)

// AttachmentChange is one attachment item of an UPDATE's SET list.
type AttachmentChange struct {
	Kind    AttachmentChangeKind
	Sel     AttachmentSel
	Value   Expr // AttachmentSet
	Mime    Expr // AttachmentSet, optional
	NewName Expr // AttachmentRename
}

// ExprStream is STREAM ?: a parameter that must be an uploaded blob reference.
type ExprStream struct{ Param ExprParameter }

func (ExprStream) isExpr() {}

// --- parsing -----------------------------------------------------------------------------

// parseBytesLiteral reads X'hex' (the X already consumed and the quote next).
func (p *rdParser) parseBytesLiteral() (Expr, error) {
	s, err := p.readStringLiteral()
	if err != nil {
		return nil, err
	}
	s = strings.Join(strings.Fields(s), "")
	if len(s)/2 > MaxSqlLiteralBytes {
		return nil, p.parseError(fmt.Sprintf("binary literal over %d bytes: bind it as a parameter or upload it", MaxSqlLiteralBytes))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, p.parseError("invalid hex in binary literal")
	}
	return ExprLiteral{Cell: CellBytes{Value: b}}, nil
}

// parseAttachmentSel reads a name expression, or ID <expr>.
func (p *rdParser) parseAttachmentSel() (AttachmentSel, error) {
	if p.matchKeyword("ID") {
		id, err := p.parsePrimary()
		if err != nil {
			return AttachmentSel{}, err
		}
		return AttachmentSel{ID: id}, nil
	}
	name, err := p.parsePrimary()
	if err != nil {
		return AttachmentSel{}, err
	}
	return AttachmentSel{Name: name}, nil
}

// parseAttachmentValue reads <bytes> [MIME <expr>].
func (p *rdParser) parseAttachmentValue() (value, mime Expr, err error) {
	if p.matchKeyword("STREAM") {
		p.skipWS()
		if p.peek() != '?' {
			return nil, nil, p.parseError("STREAM takes a parameter: STREAM ?")
		}
		param, err := p.parsePrimary()
		if err != nil {
			return nil, nil, err
		}
		value = ExprStream{Param: param.(ExprParameter)}
	} else if value, err = p.parsePrimary(); err != nil {
		return nil, nil, err
	}
	if p.matchKeyword("MIME") {
		if mime, err = p.parsePrimary(); err != nil {
			return nil, nil, err
		}
	}
	return value, mime, nil
}

// parseWithAttachments reads the INSERT's WITH ATTACHMENT clauses (WITH already consumed).
func (p *rdParser) parseWithAttachments() ([]AttachmentSpec, error) {
	var out []AttachmentSpec
	for {
		if err := p.expectKeyword("ATTACHMENT"); err != nil {
			return nil, err
		}
		var spec AttachmentSpec
		// 'name' = <bytes>, or <bytes> alone.
		save := p.pos
		first, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if p.matchOp("=") {
			spec.Name = first
			if spec.Value, spec.Mime, err = p.parseAttachmentValue(); err != nil {
				return nil, err
			}
		} else {
			p.pos = save
			if _, isParam := first.(ExprParameter); isParam {
				p.nextParamIndex-- // re-read below
			}
			if spec.Value, spec.Mime, err = p.parseAttachmentValue(); err != nil {
				return nil, err
			}
		}
		out = append(out, spec)
		if !p.matchChar(',') {
			return out, nil
		}
	}
}

// parseAttachmentChange reads one attachment item of an UPDATE's SET list, after its leading
// keyword: ATTACHMENT <sel> = <bytes>, DROP ATTACHMENT <sel>, RENAME ATTACHMENT <sel> TO <name>.
func (p *rdParser) parseAttachmentChange(kind AttachmentChangeKind) (AttachmentChange, error) {
	if kind != AttachmentSet {
		if err := p.expectKeyword("ATTACHMENT"); err != nil {
			return AttachmentChange{}, err
		}
	}
	sel, err := p.parseAttachmentSel()
	if err != nil {
		return AttachmentChange{}, err
	}
	ch := AttachmentChange{Kind: kind, Sel: sel}
	switch kind {
	case AttachmentSet:
		if !p.matchOp("=") {
			return AttachmentChange{}, p.parseError("expected '='")
		}
		if ch.Value, ch.Mime, err = p.parseAttachmentValue(); err != nil {
			return AttachmentChange{}, err
		}
	case AttachmentRename:
		if err := p.expectKeyword("TO"); err != nil {
			return AttachmentChange{}, err
		}
		if ch.NewName, err = p.parsePrimary(); err != nil {
			return AttachmentChange{}, err
		}
	}
	return ch, nil
}

// --- evaluation --------------------------------------------------------------------------

// attachEnv gives a query's evaluation the attachment table at its commit, loaded on first use.
type attachEnv struct {
	dag    *dag.InMemoryCommitDag
	at     *codec.Hash
	ns     string
	blobs  AttachmentBlobs
	table  document.AttachmentTable
	loaded bool
	err    error // the first failure; the query fails with it
}

func (e *Executor) newAttachEnv(ctx QueryContext) *attachEnv {
	return &attachEnv{dag: e.DAG, at: ctx.AtCommit, ns: ctx.NamespaceID, blobs: e.Attachments}
}

func (a *attachEnv) fail(err error) {
	if a != nil && a.err == nil {
		a.err = err
	}
}

// set is doc's attachments at the query's commit.
func (a *attachEnv) set(doc codec.UUID) document.AttachmentSet {
	if a == nil {
		return nil
	}
	if !a.loaded {
		a.loaded = true
		if a.dag != nil && a.at != nil {
			t, err := a.dag.AttachmentTableAt(*a.at)
			if err != nil {
				a.fail(err)
			}
			a.table = t
		}
	}
	return a.table.List(doc)
}

// attachmentsJSON is the _attachments value of a document.
func attachmentsJSON(set document.AttachmentSet) kdbjson.Value {
	arr := kdbjson.ArrayValue{Elements: make([]kdbjson.Value, 0, len(set))}
	for _, a := range set {
		arr.Elements = append(arr.Elements, kdbjson.ObjectValue{
			Keys: []string{"id", "name", "size", "mime", "hash"},
			Fields: map[string]kdbjson.Value{
				"id":   kdbjson.StringValue{V: a.ID.String()},
				"name": kdbjson.StringValue{V: a.Name},
				"size": kdbjson.IntValue{V: a.Size},
				"mime": kdbjson.StringValue{V: a.Mime},
				"hash": kdbjson.StringValue{V: a.ContentHash.Hex()},
			},
		})
	}
	return arr
}

// selectAttachment resolves sel against set.
func (env *evalEnv) selectAttachment(sel AttachmentSel, set document.AttachmentSet, d *evalDoc) (document.Attachment, bool, error) {
	if sel.ID != nil {
		id, err := uuidCell(env.cell(sel.ID, d))
		if err != nil {
			return document.Attachment{}, false, err
		}
		for _, a := range set {
			if a.ID == id {
				return a, true, nil
			}
		}
		return document.Attachment{}, false, nil
	}
	name, ok := env.cell(sel.Name, d).(CellString)
	if !ok {
		return document.Attachment{}, false, NewPlanningError("an attachment name must be a string", "")
	}
	a, found := set.ByName(name.Value)
	return a, found, nil
}

func uuidCell(c Cell) (codec.UUID, error) {
	s, ok := c.(CellString)
	if !ok {
		return codec.UUID{}, NewPlanningError("an attachment ID must be a UUID string", "")
	}
	id, err := codec.ParseUUID(s.Value)
	if err != nil {
		return codec.UUID{}, NewPlanningError("invalid attachment ID: "+s.Value, "")
	}
	return id, nil
}

// attachmentFunctionCell evaluates ATTACHMENT, ATTACHMENT_COUNT, ATTACHMENT_SIZE, ATTACHMENT_MIME
// and FROM_BASE64; ok=false for any other name.
func (env *evalEnv) attachmentFunctionCell(name string, call ExprFunctionCall, d *evalDoc) (Cell, bool) {
	if !isAttachmentFunction(name) {
		return nil, false
	}
	if env.attach == nil {
		env.attach = &attachEnv{} // evaluated outside a query: no table, no store
	}
	switch name {
	case "from_base64":
		if len(call.Args) != 1 {
			return CellNull{}, true
		}
		s, ok := env.cell(call.Args[0], d).(CellString)
		if !ok {
			return CellNull{}, true
		}
		b, err := base64.StdEncoding.DecodeString(s.Value)
		if err != nil {
			env.attach.fail(NewPlanningError("FROM_BASE64: invalid base64", ""))
			return CellNull{}, true
		}
		return CellBytes{Value: b}, true
	case "attachment_count":
		return CellLong{Value: int64(len(env.attach.set(d.doc.ID)))}, true
	case "attachment", "attachment_size", "attachment_mime":
	default:
		return nil, false
	}
	if len(call.Args) != 1 {
		return CellNull{}, true
	}
	sel := AttachmentSel{Name: call.Args[0]}
	if s, ok := env.cell(call.Args[0], d).(CellString); ok {
		if id, err := codec.ParseUUID(s.Value); err == nil && len(s.Value) == 36 {
			// a UUID selects by ID first, then by name (a name may look like a UUID - the default)
			for _, a := range env.attach.set(d.doc.ID) {
				if a.ID == id {
					sel = AttachmentSel{ID: call.Args[0]}
				}
			}
		}
	}
	a, found, err := env.selectAttachment(sel, env.attach.set(d.doc.ID), d)
	if err != nil {
		env.attach.fail(err)
		return CellNull{}, true
	}
	if !found {
		return CellNull{}, true
	}
	switch name {
	case "attachment_size":
		return CellLong{Value: a.Size}, true
	case "attachment_mime":
		return CellString{Value: a.Mime}, true
	}
	if env.attach.blobs == nil {
		env.attach.fail(ErrNoAttachmentStore)
		return CellNull{}, true
	}
	if a.Size > MaxSqlAttachmentReadBytes {
		env.attach.fail(NewPlanningError(fmt.Sprintf(
			"ATTACHMENT(%q) is %d bytes, over the %d a result cell holds: read it through the attachment API",
			a.Name, a.Size, MaxSqlAttachmentReadBytes), ""))
		return CellNull{}, true
	}
	b, err := env.attach.blobs.ReadAttachmentBlob(env.attach.ns, a.ContentHash, MaxSqlAttachmentReadBytes)
	if err != nil {
		// Wrapped, not flattened: the server classifies a damaged stored copy by errors.Is.
		env.attach.fail(fmt.Errorf("ATTACHMENT(%q) (ID %s) of document %s: %w", a.Name, a.ID, d.doc.ID, err))
		return CellNull{}, true
	}
	return CellBytes{Value: b}, true
}

func isAttachmentFunction(name string) bool {
	switch name {
	case "attachment", "attachment_count", "attachment_size", "attachment_mime", "from_base64":
		return true
	}
	return false
}

func checkAttachmentFunctionArity(name string, n int) error {
	want := 1
	if name == "attachment_count" {
		want = 0
	}
	if n != want {
		return NewPlanningError(fmt.Sprintf("%s takes %d argument(s)", strings.ToUpper(name), want), "")
	}
	return nil
}

// --- DML lowering ------------------------------------------------------------------------

// attachmentWriter stores the bytes a statement writes, each distinct value once however many
// rows it lands on.
type attachmentWriter struct {
	exec   *Executor
	ns     string
	stored map[string]document.AttachmentRef
}

func newAttachmentWriter(e *Executor, ns string) *attachmentWriter {
	return &attachmentWriter{exec: e, ns: ns, stored: map[string]document.AttachmentRef{}}
}

// ref evaluates an attachment value to a stored blob reference.
func (w *attachmentWriter) ref(env *evalEnv, value, mime Expr, d *evalDoc) (document.AttachmentRef, error) {
	var ref document.AttachmentRef
	var cell Cell
	if s, ok := value.(ExprStream); ok {
		cell = env.cell(s.Param, d)
		if _, ok := cell.(cellAttachmentRef); !ok {
			return ref, NewPlanningError("STREAM ? must be bound to an uploaded attachment", "")
		}
	} else {
		cell = env.cell(value, d)
	}
	if s, ok := cell.(CellString); ok {
		// a string's bytes: database/sql binds []byte as a string, and text is bytes too
		cell = CellBytes{Value: []byte(s.Value)}
	}
	switch c := cell.(type) {
	case cellAttachmentRef:
		if w.exec.Attachments == nil {
			return ref, ErrNoAttachmentStore
		}
		has, err := w.exec.Attachments.HasAttachmentBlob(w.ns, c.ref.ContentHash)
		if err != nil {
			return ref, err
		}
		if !has {
			return ref, NewPlanningError("uploaded attachment not found: "+c.ref.ContentHash.Hex(), "")
		}
		ref = document.AttachmentRef{ContentHash: c.ref.ContentHash, Size: c.ref.Size}
	case CellBytes:
		key := string(c.Value)
		if stored, ok := w.stored[key]; ok {
			ref = stored
			break
		}
		if w.exec.Attachments == nil {
			return ref, ErrNoAttachmentStore
		}
		h, n, err := w.exec.Attachments.PutAttachmentBlob(w.ns, bytes.NewReader(c.Value))
		if err != nil {
			return ref, err
		}
		ref = document.AttachmentRef{ContentHash: h, Size: n}
		w.stored[key] = ref
	case CellNull:
		return ref, NewPlanningError("an attachment value cannot be NULL (use DROP ATTACHMENT)", "")
	default:
		return ref, NewPlanningError("an attachment value must be bytes: X'..', FROM_BASE64(..), a string, or a bytes parameter", "")
	}
	ref.Scheme = document.AttachmentHashSchemeChunked1
	if mime != nil {
		m, ok := env.cell(mime, d).(CellString)
		if !ok {
			return ref, NewPlanningError("MIME must be a string", "")
		}
		ref.Mime = m.Value
	}
	return ref, nil
}

func nameCell(env *evalEnv, e Expr, d *evalDoc) (string, error) {
	s, ok := env.cell(e, d).(CellString)
	if !ok {
		return "", NewPlanningError("an attachment name must be a string", "")
	}
	if err := document.ValidateAttachmentName(s.Value); err != nil {
		return "", NewPlanningError(err.Error(), "")
	}
	return s.Value, nil
}

// insertAttachmentOps are the AttachmentPutOps for a freshly inserted document.
func (w *attachmentWriter) insertAttachmentOps(env *evalEnv, specs []AttachmentSpec, docID codec.UUID) ([]document.Op, error) {
	d := newEvalDoc(emptyDoc)
	var ops []document.Op
	names := map[string]bool{}
	for _, spec := range specs {
		ref, err := w.ref(env, spec.Value, spec.Mime, d)
		if err != nil {
			return nil, err
		}
		id, err := codec.RandomUUID()
		if err != nil {
			return nil, err
		}
		name := id.String()
		if spec.Name != nil {
			if name, err = nameCell(env, spec.Name, d); err != nil {
				return nil, err
			}
		}
		if names[name] {
			return nil, NewPlanningError("two attachments named "+name, "")
		}
		names[name] = true
		ops = append(ops, document.AttachmentPutOp{DocID: docID, AttachmentID: id, Name: name,
			Scheme: ref.Scheme, ContentHash: ref.ContentHash, Size: ref.Size, Mime: ref.Mime})
	}
	return ops, nil
}

// updateAttachmentOps are the attachment ops of an UPDATE for one matched document, given its
// attachments at the statement's read commit.
func (w *attachmentWriter) updateAttachmentOps(env *evalEnv, changes []AttachmentChange, ed *evalDoc, set document.AttachmentSet) ([]document.Op, error) {
	var ops []document.Op
	for _, ch := range changes {
		a, found, err := env.selectAttachment(ch.Sel, set, ed)
		if err != nil {
			return nil, err
		}
		switch ch.Kind {
		case AttachmentDrop:
			if found {
				ops = append(ops, document.AttachmentDeleteOp{DocID: ed.doc.ID, AttachmentID: a.ID})
				set = withoutAttachment(set, a.ID)
			}
		case AttachmentRename:
			if !found {
				continue
			}
			name, err := nameCell(env, ch.NewName, ed)
			if err != nil {
				return nil, err
			}
			a.Name = name
			ops = append(ops, putOp(ed.doc.ID, a))
			set = append(withoutAttachment(set, a.ID), a)
		case AttachmentSet:
			ref, err := w.ref(env, ch.Value, ch.Mime, ed)
			if err != nil {
				return nil, err
			}
			if !found {
				a = document.Attachment{}
				if ch.Sel.ID != nil {
					if a.ID, err = uuidCell(env.cell(ch.Sel.ID, ed)); err != nil {
						return nil, err
					}
				} else if a.ID, err = codec.RandomUUID(); err != nil {
					return nil, err
				}
				a.Name = a.ID.String()
				if ch.Sel.Name != nil {
					if a.Name, err = nameCell(env, ch.Sel.Name, ed); err != nil {
						return nil, err
					}
				}
			}
			if ch.Mime == nil && found {
				ref.Mime = a.Mime // keep the type unless the statement names one
			}
			a.AttachmentRef = ref
			ops = append(ops, putOp(ed.doc.ID, a))
			set = append(withoutAttachment(set, a.ID), a)
		}
	}
	return ops, nil
}

func putOp(docID codec.UUID, a document.Attachment) document.AttachmentPutOp {
	return document.AttachmentPutOp{DocID: docID, AttachmentID: a.ID, Name: a.Name,
		Scheme: a.Scheme, ContentHash: a.ContentHash, Size: a.Size, Mime: a.Mime}
}

func withoutAttachment(set document.AttachmentSet, id codec.UUID) document.AttachmentSet {
	out := make(document.AttachmentSet, 0, len(set))
	for _, a := range set {
		if a.ID != id {
			out = append(out, a)
		}
	}
	return out
}

// rowsAffected counts the documents ops change: an UPDATE adds attachment ops beside a document's
// write, so ops alone overcount.
func rowsAffected(ops []document.Op) int {
	seen := map[codec.UUID]bool{}
	for _, op := range ops {
		if id, ok := document.OpDocID(op); ok {
			seen[id] = true
		}
	}
	return len(seen)
}
