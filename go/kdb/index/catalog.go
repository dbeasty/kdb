package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
)

// CatalogFileName is the index catalog's file name under the namespace's index directory
// (Layer 16 §9.2: `<dataRoot>/<catalog>/index/catalog.json`).
const CatalogFileName = "catalog.json"

// CatalogFormatVersion is bumped when the on-disk catalog shape changes incompatibly.
const CatalogFormatVersion = 1

// Catalog is the persisted list of a namespace's index descriptors.
type Catalog struct {
	NamespaceID string
	Indexes     []Descriptor
}

// DescriptorJSON is the on-disk form of a Descriptor: ids as canonical strings, the type by
// name, options as a sorted object (encoding/json orders map keys).
type DescriptorJSON struct {
	IndexID       string            `json:"indexId"`
	NamespaceID   string            `json:"namespaceId"`
	FieldName     string            `json:"fieldName"`
	Fields        []string          `json:"fields"`
	Type          string            `json:"type"`
	Unique        bool              `json:"unique"`
	SchemaVersion int               `json:"schemaVersion"`
	CreatedAtHex  string            `json:"createdAtHex"`
	Options       map[string]string `json:"options"`
}

type catalogFile struct {
	FormatVersion int              `json:"formatVersion"`
	NamespaceID   string           `json:"namespaceId"`
	Indexes       []DescriptorJSON `json:"indexes"`
}

// DescriptorToJSON converts a descriptor to its on-disk form.
func DescriptorToJSON(d Descriptor) DescriptorJSON {
	fields := d.Fields
	if fields == nil {
		fields = []string{}
	}
	opts := d.Options
	if opts == nil {
		opts = map[string]string{}
	}
	return DescriptorJSON{
		IndexID:       d.IndexID.String(),
		NamespaceID:   d.NamespaceID,
		FieldName:     d.FieldName,
		Fields:        append([]string(nil), fields...),
		Type:          d.Type.String(),
		Unique:        d.Unique,
		SchemaVersion: d.SchemaVersion,
		CreatedAtHex:  d.CreatedAtHash.Hex(),
		Options:       opts,
	}
}

// DescriptorFromJSON parses the on-disk form back into a Descriptor.
func DescriptorFromJSON(j DescriptorJSON) (Descriptor, error) {
	id, err := codec.UUIDFromString(j.IndexID)
	if err != nil {
		return Descriptor{}, fmt.Errorf("catalog: index id %q: %w", j.IndexID, err)
	}
	typ, err := ParseIndexType(j.Type)
	if err != nil {
		return Descriptor{}, fmt.Errorf("catalog: %w", err)
	}
	var created codec.Hash
	if j.CreatedAtHex != "" {
		created, err = codec.HashFromHex(j.CreatedAtHex)
		if err != nil {
			return Descriptor{}, fmt.Errorf("catalog: createdAtHex %q: %w", j.CreatedAtHex, err)
		}
	}
	opts := make(map[string]string, len(j.Options))
	for k, v := range j.Options {
		opts[k] = v
	}
	return Descriptor{
		IndexID:       id,
		NamespaceID:   j.NamespaceID,
		FieldName:     j.FieldName,
		Fields:        append([]string(nil), j.Fields...),
		Type:          typ,
		Unique:        j.Unique,
		SchemaVersion: j.SchemaVersion,
		CreatedAtHash: created,
		Options:       opts,
	}, nil
}

// MarshalCatalog encodes a catalog as indented JSON, indexes ordered by index id so the bytes
// are a function of the content alone.
func MarshalCatalog(c Catalog) ([]byte, error) {
	f := catalogFile{FormatVersion: CatalogFormatVersion, NamespaceID: c.NamespaceID, Indexes: []DescriptorJSON{}}
	for _, d := range c.Indexes {
		f.Indexes = append(f.Indexes, DescriptorToJSON(d))
	}
	sort.Slice(f.Indexes, func(i, j int) bool { return f.Indexes[i].IndexID < f.Indexes[j].IndexID })
	return json.MarshalIndent(f, "", "  ")
}

// UnmarshalCatalog decodes MarshalCatalog's output.
func UnmarshalCatalog(data []byte) (Catalog, error) {
	var f catalogFile
	if err := json.Unmarshal(data, &f); err != nil {
		return Catalog{}, fmt.Errorf("catalog: %w", err)
	}
	if f.FormatVersion != CatalogFormatVersion {
		return Catalog{}, fmt.Errorf("catalog: unsupported format version %d", f.FormatVersion)
	}
	c := Catalog{NamespaceID: f.NamespaceID}
	for _, j := range f.Indexes {
		d, err := DescriptorFromJSON(j)
		if err != nil {
			return Catalog{}, err
		}
		c.Indexes = append(c.Indexes, d)
	}
	return c, nil
}

// SaveCatalog writes `<dir>/catalog.json` atomically (temp file + rename), creating dir.
func SaveCatalog(dir string, c Catalog) error {
	data, err := MarshalCatalog(c)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return WriteFileAtomic(filepath.Join(dir, CatalogFileName), data)
}

// LoadCatalog reads `<dir>/catalog.json`. A missing file is reported through the returned
// error, which satisfies errors.Is(err, fs.ErrNotExist).
func LoadCatalog(dir string) (Catalog, error) {
	data, err := os.ReadFile(filepath.Join(dir, CatalogFileName))
	if err != nil {
		return Catalog{}, err
	}
	return UnmarshalCatalog(data)
}
