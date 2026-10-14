package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
)

// Attachment segments in a backup.
//
// Small attachment blobs live in the namespace's delta log and are backed up with it. Large ones
// are segments of the attachment store (storage/attachstore): bytes, leaves and pointer segments,
// each written once and never changed. They go up after the log, so every blob a logged commit
// names is in the backup (a blob is durable before the commit that names it), cut into parts of at
// most attachmentPartBytes - the object API takes whole byte slices, and a blob can be terabytes -
// each stored under the SHA-256 of its bytes. A part another backup already stored is not uploaded
// again, which is what makes an incremental backup of an unchanged attachment free.

// attachmentPartBytes is the largest object one attachment segment part is stored as.
var attachmentPartBytes = 64 << 20

// AttachmentSegment is one attachment-store segment in a backup: its name and its parts in order.
type AttachmentSegment struct {
	Name  string           `json:"name"`
	Size  int64            `json:"size"`
	Parts []AttachmentPart `json:"parts"`
}

// AttachmentPart is one stored piece of an attachment segment.
type AttachmentPart struct {
	Key    string `json:"key"`
	Sha256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

func attachmentPartKey(namespaceID, sumHex string) string {
	return "attachments/" + namespaceID + "/" + sumHex
}

// backupAttachments uploads namespaceID's attachment segments.
func backupAttachments(shim storage.PlatformIOShim, namespaceID string, store ObjectStore, base *Manifest) ([]AttachmentSegment, error) {
	ctx := context.Background()
	names, err := shim.ListSegments(namespaceID)
	if err != nil {
		return nil, err
	}
	prefix := attachstore.SegmentPrefix(namespaceID)
	stored := map[string]bool{}
	if base != nil {
		for _, s := range base.Attachments {
			for _, p := range s.Parts {
				stored[p.Sha256] = true
			}
		}
	}
	var out []AttachmentSegment
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		seg := AttachmentSegment{Name: name}
		for off := int64(0); ; {
			data, err := shim.ReadFromSegment(name, off, attachmentPartBytes)
			if err != nil {
				if off == 0 && isNotExist(err) {
					break // deleted since the listing (collected): nothing to keep
				}
				return nil, fmt.Errorf("backup: attachment segment %s: %w", name, err)
			}
			if len(data) == 0 {
				break
			}
			sum := sha256.Sum256(data)
			sumHex := hex.EncodeToString(sum[:])
			key := attachmentPartKey(namespaceID, sumHex)
			if !stored[sumHex] {
				if err := store.Put(ctx, key, data); err != nil {
					return nil, fmt.Errorf("backup: uploading attachment segment %s: %w", name, err)
				}
				stored[sumHex] = true
			}
			seg.Parts = append(seg.Parts, AttachmentPart{Key: key, Sha256: sumHex, Size: int64(len(data))})
			seg.Size += int64(len(data))
			off += int64(len(data))
			if len(data) < attachmentPartBytes {
				break
			}
		}
		if len(seg.Parts) > 0 {
			out = append(out, seg)
		}
	}
	return out, nil
}

func isNotExist(err error) bool {
	return os.IsNotExist(err) || strings.Contains(err.Error(), "not found")
}

// fetchAttachments writes m's attachment segments under outDir, checking every part.
func fetchAttachments(store ObjectStore, m *Manifest, outDir string) error {
	for _, seg := range m.Attachments {
		dest := filepath.Join(outDir, filepath.FromSlash(seg.Name))
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		f, err := os.Create(dest)
		if err != nil {
			return err
		}
		for _, p := range seg.Parts {
			data, err := store.Get(context.Background(), p.Key)
			if err == nil {
				sum := sha256.Sum256(data)
				if hex.EncodeToString(sum[:]) != p.Sha256 {
					err = fmt.Errorf("SHA-256 mismatch")
				}
			}
			if err != nil {
				f.Close()
				return fmt.Errorf("backup fetch: attachment segment %s part %s: %w", seg.Name, p.Key, err)
			}
			if _, err := f.Write(data); err != nil {
				f.Close()
				return err
			}
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// verifyAttachments re-checks every attachment part m names.
func verifyAttachments(store ObjectStore, m *Manifest, res *VerifyResult) {
	for _, seg := range m.Attachments {
		for _, p := range seg.Parts {
			res.Objects++
			data, err := store.Get(context.Background(), p.Key)
			if err != nil {
				res.Problems = append(res.Problems, fmt.Sprintf("attachment segment %s part %s: missing: %v", seg.Name, p.Key, err))
				continue
			}
			sum := sha256.Sum256(data)
			if int64(len(data)) != p.Size || hex.EncodeToString(sum[:]) != p.Sha256 {
				res.Problems = append(res.Problems, fmt.Sprintf("attachment segment %s part %s: content does not match the manifest", seg.Name, p.Key))
			}
		}
	}
}
