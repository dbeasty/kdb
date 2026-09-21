package embed

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

// nodeFileName names the file at a data root that holds the node's identity.
const nodeFileName = "NODE"

// NodeIDEnv overrides the persisted node id - for tests and for containers whose identity is
// assigned by the orchestrator. It is not written back to the data root.
const NodeIDEnv = "KDB_NODE_ID"

// LoadOrCreateNodeID returns the identity of the node that owns dataRoot, creating and durably
// recording one on first use. A data root has exactly one: it is what peers record progress
// against, what authors this node's commits, and what names this node in cross-namespace group
// markers. Before it existed, every write was authored by a fresh random UUID and every server
// introduced itself to peers as "kdb-service-go".
//
// A data root that already ran a cross-namespace transaction adopts that host id, so a node's
// identity does not change underneath markers it has already written.
//
// Cloning a data root clones its identity. Two live nodes with one identity is refused at the
// peer handshake; a clone meant to become a separate node must delete NODE first.
func LoadOrCreateNodeID(dataRoot string) (codec.UUID, error) {
	if v := strings.TrimSpace(os.Getenv(NodeIDEnv)); v != "" {
		id, err := codec.ParseUUID(v)
		if err != nil {
			return codec.UUID{}, fmt.Errorf("%s: %w", NodeIDEnv, err)
		}
		return id, nil
	}
	if id, ok, err := readNodeID(dataRoot); err != nil || ok {
		return id, err
	}
	var id codec.UUID
	host, err := readHostID(dataRoot)
	if err != nil {
		return codec.UUID{}, err
	}
	if host != "" {
		if id, err = codec.ParseUUID(host); err != nil {
			return codec.UUID{}, fmt.Errorf("adopting %s as node id: %w", filepath.Join(txnDir(dataRoot), hostFileName), err)
		}
	} else if id, err = codec.RandomUUID(); err != nil {
		return codec.UUID{}, err
	}
	if err := writeNodeID(dataRoot, id); err != nil {
		return codec.UUID{}, err
	}
	return id, nil
}

// readNodeID returns the recorded node id, if any.
func readNodeID(dataRoot string) (codec.UUID, bool, error) {
	raw, err := os.ReadFile(filepath.Join(dataRoot, nodeFileName))
	if errors.Is(err, os.ErrNotExist) {
		return codec.UUID{}, false, nil
	}
	if err != nil {
		return codec.UUID{}, false, err
	}
	id, err := codec.ParseUUID(strings.TrimSpace(string(raw)))
	if err != nil {
		return codec.UUID{}, false, fmt.Errorf("%s: %w", filepath.Join(dataRoot, nodeFileName), err)
	}
	return id, true, nil
}

func writeNodeID(dataRoot string, id codec.UUID) error {
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dataRoot, nodeFileName+".tmp")
	if err := os.WriteFile(tmp, []byte(id.String()+"\n"), 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	serr := f.Sync()
	f.Close()
	if serr != nil {
		return serr
	}
	if err := os.Rename(tmp, filepath.Join(dataRoot, nodeFileName)); err != nil {
		return err
	}
	return syncDir(dataRoot)
}
