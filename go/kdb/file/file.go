package file

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// Metadata describes a file attachment in a namespace.
type Metadata struct {
	Name        string
	ContentType string
	Size        int64
	SHA256      string
}

// Ingest reads r, hashes content, returns metadata and bytes.
func Ingest(name, contentType string, r io.Reader) (Metadata, []byte, error) {
	h := sha256.New()
	data, err := io.ReadAll(io.TeeReader(r, h))
	if err != nil {
		return Metadata{}, nil, err
	}
	return Metadata{
		Name:        name,
		ContentType: contentType,
		Size:        int64(len(data)),
		SHA256:      hex.EncodeToString(h.Sum(nil)),
	}, data, nil
}
