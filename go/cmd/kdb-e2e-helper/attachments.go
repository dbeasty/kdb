package main

// Attachment commands for the e2e harness (docs/kdb-attachments-plan.md). Every command prints one
// JSON object. Expected failures (not found, corrupt, conflict) are printed as {"error", "kind"}
// with exit status 0, so a test can assert which failure it got; anything else is a real error.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
)

var attachmentCommands = map[string]bool{
	"attach": true, "tx-attach": true, "sql-attach": true, "read-attachment": true, "list-attachments": true,
}

type attachFlags struct {
	cf                   commonFlags
	docID, name, attID   string
	file, mime, jsonBody string
	asOf, out, sqlText   string
	dropAt, pauseAfter   int64
	readyFile, goFile    string
	pauseTimeout         time.Duration
	commitDelay          time.Duration
	offset, length       int64
	resumeAt             int64
}

func runAttachment(cmd string, args []string) error {
	fs := flag.NewFlagSet("kdb-e2e-helper "+cmd, flag.ExitOnError)
	var f attachFlags
	f.cf.register(fs)
	fs.StringVar(&f.docID, "doc-id", "", "document id (32 hex chars)")
	fs.StringVar(&f.name, "name", "", "the attachment's name")
	fs.StringVar(&f.attID, "attachment-id", "", "the attachment's ID (instead of, or with, --name)")
	fs.StringVar(&f.file, "file", "", "attach, tx-attach: the file whose bytes to attach")
	fs.StringVar(&f.mime, "mime", "", "attach, tx-attach: media type")
	fs.StringVar(&f.jsonBody, "json", "", "tx-attach: the document JSON written in the same commit")
	fs.Int64Var(&f.dropAt, "drop-at", 0, "attach: fail the upload after this many bytes, then resume it")
	fs.StringVar(&f.sqlText, "sql", "", "sql-attach: a statement whose one STREAM ? (or ?) binds the uploaded file")
	fs.StringVar(&f.asOf, "as-of", "", "read-attachment, list-attachments: commit to read at (empty: head)")
	fs.StringVar(&f.out, "out", "", "read-attachment: also write the bytes to this file")
	fs.Int64Var(&f.pauseAfter, "pause-after", 0, "read-attachment: after this many bytes, create --ready-file and wait for --go-file")
	fs.StringVar(&f.readyFile, "ready-file", "", "read-attachment: created when the read pauses")
	fs.StringVar(&f.goFile, "go-file", "", "read-attachment: the read resumes once this file exists")
	fs.Int64Var(&f.offset, "offset", 0, "read-attachment: first byte of a ranged read")
	fs.Int64Var(&f.length, "length", -1, "read-attachment: bytes in a ranged read (-1: to the end)")
	fs.Int64Var(&f.resumeAt, "resume-at", 0, "read-attachment: read this many bytes, drop the connection, reconnect and resume the same version from there")
	fs.DurationVar(&f.commitDelay, "commit-delay", 0, "attach: wait this long between the finished upload and the commit")
	fs.DurationVar(&f.pauseTimeout, "pause-timeout", 5*time.Minute, "read-attachment: longest pause")
	_ = fs.Parse(args)
	if f.cf.addr == "" || f.cf.namespace == "" || (f.docID == "" && cmd != "sql-attach") {
		return fmt.Errorf("--addr, --namespace and --doc-id are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.cf.timeout)
	defer cancel()
	cl, err := f.cf.dial(ctx)
	if err != nil {
		return err
	}
	defer cl.Close()

	var res map[string]any
	switch cmd {
	case "attach":
		res, err = attachCmd(ctx, cl, f)
	case "tx-attach":
		res, err = txAttachCmd(ctx, cl, f)
	case "sql-attach":
		res, err = sqlAttachCmd(ctx, cl, f)
	case "read-attachment":
		res, err = readAttachmentCmd(ctx, cl, f)
	case "list-attachments":
		res, err = listAttachmentsCmd(ctx, cl, f)
	}
	if err != nil {
		kind := expectedFailure(err)
		if kind == "" {
			return err
		}
		res = map[string]any{"error": err.Error(), "kind": kind}
		if kind == "not_found" {
			res["missing"] = true
		}
	}
	return json.NewEncoder(os.Stdout).Encode(res)
}

func expectedFailure(err error) string {
	switch {
	case errors.Is(err, client.ErrNotFound):
		return "not_found"
	case errors.Is(err, client.ErrAttachmentCorrupt):
		return "corrupt"
	case errors.Is(err, client.ErrConflict):
		return "conflict"
	case errors.Is(err, client.ErrAttachmentRange):
		return "range"
	}
	return ""
}

func (f attachFlags) ref() client.AttachmentRef {
	if f.attID != "" {
		return client.ByAttachmentID(f.attID)
	}
	return client.ByAttachmentName(f.name)
}

// failingReader yields r's bytes up to limit, then fails: a client that loses its source (or its
// connection) part way through an upload.
type failingReader struct {
	r     io.Reader
	limit int64
}

func (fr *failingReader) Read(p []byte) (int, error) {
	if fr.limit <= 0 {
		return 0, errors.New("e2e: upload source dropped")
	}
	if int64(len(p)) > fr.limit {
		p = p[:fr.limit]
	}
	n, err := fr.r.Read(p)
	fr.limit -= int64(n)
	return n, err
}

// upload stores the file's bytes. With --drop-at it first fails part way and then resumes from
// what the server says it received.
func upload(ctx context.Context, cl *client.Client, f attachFlags) (client.Blob, bool, error) {
	file, err := os.Open(f.file)
	if err != nil {
		return client.Blob{}, false, err
	}
	defer file.Close()
	if f.dropAt <= 0 {
		blob, err := cl.UploadAttachment(ctx, f.cf.namespace, file)
		return blob, false, err
	}
	_, err = cl.UploadAttachment(ctx, f.cf.namespace, &failingReader{r: file, limit: f.dropAt})
	var ue *client.UploadError
	if !errors.As(err, &ue) {
		return client.Blob{}, false, fmt.Errorf("e2e: expected an UploadError from the dropped upload, got %v", err)
	}
	if ue.UploadID == "" {
		return client.Blob{}, false, fmt.Errorf("e2e: the dropped upload never reached the server (--drop-at too small?)")
	}
	if _, err := file.Seek(ue.Received, io.SeekStart); err != nil {
		return client.Blob{}, false, err
	}
	blob, err := cl.ResumeUpload(ctx, f.cf.namespace, ue.UploadID, ue.Received, file)
	return blob, true, err
}

// attachmentID is the ID of docID's attachment called name at the head (or a fresh one), and the
// head commit it was read at - the transaction's BaseVersion.
func attachmentID(ctx context.Context, cl *client.Client, f attachFlags) (string, string, error) {
	list, base, err := cl.ListAttachments(ctx, f.cf.namespace, f.docID, "")
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return "", "", err
	}
	if base == "" {
		// A document that does not exist yet lists nothing: take the head from a read instead.
		_, base, err = cl.GetJSON(ctx, f.cf.namespace, f.docID)
		if err != nil && !errors.Is(err, client.ErrNotFound) {
			return "", "", err
		}
	}
	if f.attID != "" {
		return f.attID, base, nil
	}
	for _, a := range list {
		if a.Name == f.name {
			return a.ID, base, nil
		}
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return "", "", err
	}
	return id.String(), base, nil
}

func attachCmd(ctx context.Context, cl *client.Client, f attachFlags) (map[string]any, error) {
	blob, resumed, err := upload(ctx, cl, f)
	if err != nil {
		return nil, err
	}
	time.Sleep(f.commitDelay)
	id, base, err := attachmentID(ctx, cl, f)
	if err != nil {
		return nil, err
	}
	commit, err := cl.Commit(ctx, client.Transaction{Namespace: f.cf.namespace, BaseVersion: base, Attachments: []client.AttachmentChange{{
		DocID: f.docID, ID: id, Name: f.name, Hash: blob.Hash, Size: blob.Size, Mime: f.mime,
	}}})
	if err != nil {
		return nil, err
	}
	return map[string]any{"commit": commit, "id": id, "hash": blob.Hash, "size": blob.Size, "resumed": resumed}, nil
}

// txAttachCmd writes the document's JSON and one attachment in a single commit.
func txAttachCmd(ctx context.Context, cl *client.Client, f attachFlags) (map[string]any, error) {
	blob, _, err := upload(ctx, cl, f)
	if err != nil {
		return nil, err
	}
	id, base, err := attachmentID(ctx, cl, f)
	if err != nil {
		return nil, err
	}
	commit, err := cl.Commit(ctx, client.Transaction{
		Namespace:   f.cf.namespace,
		BaseVersion: base,
		Writes:      []client.DocWrite{{DocID: f.docID, JSON: []byte(f.jsonBody), Replace: true}},
		Attachments: []client.AttachmentChange{{DocID: f.docID, ID: id, Name: f.name, Hash: blob.Hash, Size: blob.Size, Mime: f.mime}},
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"commit": commit, "id": id, "hash": blob.Hash, "size": blob.Size}, nil
}

// sqlAttachCmd uploads the file, then runs the statement with the uploaded blob bound to its
// parameter: the SQL path for bytes of any size (INSERT ... WITH ATTACHMENT 'n' = STREAM ?).
func sqlAttachCmd(ctx context.Context, cl *client.Client, f attachFlags) (map[string]any, error) {
	blob, _, err := upload(ctx, cl, f)
	if err != nil {
		return nil, err
	}
	n, err := cl.Exec(ctx, f.cf.namespace, f.sqlText, []any{blob})
	if err != nil {
		return nil, err
	}
	return map[string]any{"rows": n, "hash": blob.Hash, "size": blob.Size}, nil
}

// readAttachmentCmd streams the attachment (never holding it whole), hashing it and optionally
// writing it to --out. With --pause-after it stops part way, announces the pause through
// --ready-file and waits for --go-file, so a test can commit, compact or collect mid-read.
func readAttachmentCmd(ctx context.Context, cl *client.Client, f attachFlags) (map[string]any, error) {
	if f.resumeAt > 0 {
		return resumedReadCmd(ctx, cl, f)
	}
	var r *client.AttachmentReader
	var err error
	if f.offset != 0 || f.length >= 0 {
		r, err = cl.OpenAttachmentRange(ctx, f.cf.namespace, f.docID, f.ref(), f.asOf, f.offset, f.length)
	} else {
		r, err = cl.OpenAttachment(ctx, f.cf.namespace, f.docID, f.ref(), f.asOf)
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()
	h := sha256.New()
	var w io.Writer = h
	if f.out != "" {
		out, err := os.Create(f.out)
		if err != nil {
			return nil, err
		}
		defer out.Close()
		w = io.MultiWriter(h, out)
	}
	var n int64
	if f.pauseAfter > 0 {
		m, err := io.CopyN(w, r, f.pauseAfter)
		n += m
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if err := pause(f); err != nil {
			return nil, err
		}
	}
	m, err := io.Copy(w, r)
	n += m
	if err != nil {
		return nil, err
	}
	info := r.Info()
	return map[string]any{
		"sha256": hex.EncodeToString(h.Sum(nil)), "size": n, "hash": info.Hash, "id": info.ID,
		"name": info.Name, "mime": info.Mime, "commit": r.Commit(), "superseded": r.Superseded(),
	}, nil
}

// resumedReadCmd reads the first --resume-at bytes, then drops the whole connection (a client that
// lost its network), reconnects and continues with a ranged read of the same version - the commit
// the first read was pinned to - from where it stopped.
func resumedReadCmd(ctx context.Context, cl *client.Client, f attachFlags) (map[string]any, error) {
	h := sha256.New()
	first, err := cl.OpenAttachment(ctx, f.cf.namespace, f.docID, f.ref(), f.asOf)
	if err != nil {
		return nil, err
	}
	n, err := io.CopyN(h, first, f.resumeAt)
	if err != nil {
		return nil, err
	}
	commit, info := first.Commit(), first.Info()
	cl.Close() // no Close of the reader: the connection is simply gone
	if err := pause(f); err != nil {
		return nil, err
	}
	again, err := f.cf.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer again.Close()
	rest, err := again.OpenAttachmentRange(ctx, f.cf.namespace, f.docID, client.ByAttachmentID(info.ID), commit, n, -1)
	if err != nil {
		return nil, err
	}
	defer rest.Close()
	m, err := io.Copy(h, rest)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"sha256": hex.EncodeToString(h.Sum(nil)), "size": n + m, "hash": info.Hash, "id": info.ID,
		"commit": commit, "resumed_commit": rest.Commit(), "superseded": rest.Superseded(),
	}, nil
}

func pause(f attachFlags) error {
	if f.readyFile != "" {
		if err := os.WriteFile(f.readyFile, []byte("paused\n"), 0o644); err != nil {
			return err
		}
	}
	if f.goFile == "" {
		return nil
	}
	deadline := time.Now().Add(f.pauseTimeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(f.goFile); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("e2e: %s never appeared", f.goFile)
}

func listAttachmentsCmd(ctx context.Context, cl *client.Client, f attachFlags) (map[string]any, error) {
	list, commit, err := cl.ListAttachments(ctx, f.cf.namespace, f.docID, f.asOf)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, len(list))
	for i, a := range list {
		items[i] = map[string]any{"id": a.ID, "name": a.Name, "hash": a.Hash, "size": a.Size, "mime": a.Mime}
	}
	return map[string]any{"commit": commit, "attachments": items}, nil
}
