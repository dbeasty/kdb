package embed

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Migrating file attachments of the earlier design (docs/kdb-spec-layer1-component3b-file-
// attachments.md) to attachments: there a file was a metadata document - its JSON naming the
// stored bytes by "blobHash" (SHA-256 of the blob-store payload), with "name" and "mime" - and the
// bytes sat in the namespace blob store. MigrateFileAttachments gives each such document an
// attachment holding those bytes, named by its "name" (else "file") and typed by its "mime", unless
// it already has one with the same bytes. The metadata JSON stays as it was unless strip is set,
// which then removes "blobHash" in the same commit. A ZIP-encoded record ("encoding": "zip") moves
// as the stored ZIP bytes, typed application/zip.

// FileMigration reports what MigrateFileAttachments did, or would do.
type FileMigration struct {
	Scanned  int      // documents looked at
	Migrated int      // documents given an attachment
	Skipped  int      // already migrated
	Missing  []string // documents whose blobHash names no stored blob
	Commit   codec.Hash
}

type fileRecord struct {
	BlobHash string `json:"blobHash"`
	Name     string `json:"name"`
	Mime     string `json:"mime"`
	Encoding string `json:"encoding"`
}

// MigrateFileAttachments migrates namespaceID's file records at main's head in one commit. dryRun
// reports without writing.
func (rt *EmbeddedKdbRuntime) MigrateFileAttachments(namespaceID string, strip, dryRun bool) (FileMigration, error) {
	var out FileMigration
	if !dryRun {
		if err := rt.AssertWritable(); err != nil {
			return out, err
		}
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return out, err
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return out, err
	}
	table, err := rt.AttachmentTableAt(head)
	if err != nil {
		return out, err
	}
	var ops []document.Op
	err = rt.Storage.ScanDocuments(namespaceID, commit.DocumentTreeHash, 256, func(docs []document.Document) error {
		for _, d := range docs {
			out.Scanned++
			if !bytes.Contains([]byte(d.JSON), []byte(`"blobHash"`)) {
				continue
			}
			var rec fileRecord
			if json.Unmarshal([]byte(d.JSON), &rec) != nil || len(rec.BlobHash) != 64 {
				continue
			}
			h, err := codec.HashFromHex(rec.BlobHash)
			if err != nil {
				continue
			}
			data, err := rt.Storage.ReadBlob(h)
			if err != nil || data == nil {
				out.Missing = append(out.Missing, d.ID.String())
				continue
			}
			content := document.AttachmentContentHash(data)
			name := rec.Name
			if document.ValidateAttachmentName(name) != nil {
				name = "file"
			}
			mime := rec.Mime
			if rec.Encoding == "zip" {
				mime = "application/zip"
			}
			done := false
			for _, a := range table.List(d.ID) {
				if a.ContentHash == content {
					done = true
				}
			}
			if done {
				out.Skipped++
				continue
			}
			if _, clash := table.List(d.ID).ByName(name); clash {
				name = name + "~migrated"
			}
			out.Migrated++
			if dryRun {
				continue
			}
			info, err := rt.AttachmentStore(namespaceID).Put(bytes.NewReader(data))
			if err != nil {
				return err
			}
			id, err := codec.RandomUUID()
			if err != nil {
				return err
			}
			ops = append(ops, document.AttachmentPutOp{DocID: d.ID, AttachmentID: id, Name: name,
				Scheme: document.AttachmentHashSchemeChunked1, ContentHash: info.Hash, Size: info.Size, Mime: mime})
			if strip {
				var obj map[string]json.RawMessage
				if json.Unmarshal([]byte(d.JSON), &obj) == nil {
					delete(obj, "blobHash")
					body, err := json.Marshal(obj)
					if err != nil {
						return err
					}
					ops = append(ops, document.WriteOp{DocID: d.ID, Patch: string(body), Replace: true})
				}
			}
		}
		return nil
	})
	if err != nil || dryRun || len(ops) == 0 {
		return out, err
	}
	out.Commit, err = rt.commitOps(namespaceID, head, ops, "kdb:file-migrate")
	return out, err
}

// commitOps commits ops on head through the runtime's DAG and storage, as PutAttachment does.
func (rt *EmbeddedKdbRuntime) commitOps(namespaceID string, head codec.Hash, ops []document.Op, message string) (codec.Hash, error) {
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return codec.Hash{}, err
	}
	table, err := rt.AttachmentTableAt(head)
	if err != nil {
		return codec.Hash{}, err
	}
	if _, err := table.Apply(ops); err != nil {
		return codec.Hash{}, err
	}
	for _, op := range ops {
		if w, ok := op.(document.WriteOp); ok {
			if err := rt.Storage.PutDocument(namespaceID, document.Document{ID: w.DocID, JSON: w.Patch}); err != nil {
				return codec.Hash{}, err
			}
		}
	}
	tree, err := rt.Storage.CommitTree(namespaceID, commit.DocumentTreeHash)
	if err != nil {
		return codec.Hash{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return codec.Hash{}, err
	}
	author, err := codec.RandomUUID()
	if err != nil {
		return codec.Hash{}, err
	}
	tx := document.Transaction{ID: txID, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	c, err := rt.DAG.AppendCommit(tx, head, tree, commit.SchemaHash, message)
	if err != nil {
		return codec.Hash{}, fmt.Errorf("kdb: file migration commit: %w", err)
	}
	return c.Hash, nil
}
