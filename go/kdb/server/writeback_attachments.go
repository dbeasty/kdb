package server

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Attachments in write-back projections (docs/kdb-attachments-plan.md): a projection's write
// carries a document's whole attachment set after it, guarded by the set the projection held, and
// the source turns that into attachment operations against what it holds now.

// toTreeAttachments is set as the wire lists attachments.
func toTreeAttachments(set document.AttachmentSet) []wire.TreeAttachment {
	out := make([]wire.TreeAttachment, 0, len(set))
	for _, a := range set {
		out = append(out, wire.TreeAttachment{ID: a.ID.String(), Name: a.Name, Hash: a.ContentHash.Hex(), Size: a.Size, Mime: a.Mime})
	}
	return out
}

// fromTreeAttachments parses a wire attachment list.
func fromTreeAttachments(items []wire.TreeAttachment) (document.AttachmentSet, error) {
	out := make(document.AttachmentSet, 0, len(items))
	names := map[string]bool{}
	for _, it := range items {
		id, err := codec.ParseUUID(it.ID)
		if err != nil {
			return nil, fmt.Errorf("attachment id %q: %w", it.ID, err)
		}
		h, err := codec.HashFromHex(it.Hash)
		if err != nil {
			return nil, fmt.Errorf("attachment %s hash: %w", it.ID, err)
		}
		if err := document.ValidateAttachmentName(it.Name); err != nil {
			return nil, err
		}
		if names[it.Name] {
			return nil, fmt.Errorf("two attachments named %q", it.Name)
		}
		names[it.Name] = true
		if it.Size < 0 {
			return nil, fmt.Errorf("attachment %s: negative size", it.ID)
		}
		out = append(out, document.Attachment{ID: id, Name: it.Name, AttachmentRef: document.AttachmentRef{
			Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h, Size: it.Size, Mime: it.Mime}})
	}
	return out, nil
}

// setHashHex is the hash of a document's attachment set as a write-back names it: "" for none.
func setHashHex(set document.AttachmentSet) string {
	if len(set) == 0 {
		return ""
	}
	return set.Hash().Hex()
}

// parseCurrentAttachments reads a refused write's report of the source's attachments.
func parseCurrentAttachments(items []wire.TreeAttachment) (document.AttachmentSet, error) {
	out := make(document.AttachmentSet, 0, len(items))
	for _, it := range items {
		id, err := codec.ParseUUID(it.ID)
		if err != nil {
			return nil, err
		}
		h, err := codec.HashFromHex(it.Hash)
		if err != nil {
			return nil, err
		}
		out = append(out, document.Attachment{ID: id, Name: it.Name, AttachmentRef: document.AttachmentRef{
			Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h, Size: it.Size, Mime: it.Mime}})
	}
	return out, nil
}
