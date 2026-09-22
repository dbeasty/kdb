package wire

// Anti-entropy and repair frames (Go-only, v2): a peer compares document trees by subtree hash
// (TREE_NODES) and fetches document bodies it lacks or holds damaged, by content hash
// (OBJECT_FETCH). Bodies are self-verifying - the receiver checks each against the content hash it
// asked for - so a repair source need not be trusted.

// SyncCapRepair: the host answers TREE_NODES and OBJECT_FETCH.
const SyncCapRepair = "repair"

// TreeNodesMessage asks for the subtree nodes of one tree under the given nibble prefixes
// (TREE_NODES 0x34).
type TreeNodesMessage struct {
	H         Header
	Namespace string
	// TreeHex is the tree to describe; empty means the tree at the host's main head, which the
	// result names.
	TreeHex  string
	Prefixes []string
	// EntryLimit lists a subtree's entries when it holds at most this many.
	EntryLimit int
}

func (m TreeNodesMessage) Header() Header { return m.H }

// TreeNodeInfo is one subtree: its hash, its 16 children's hashes (hex, empty for an empty
// child), and its entries (document id to content hash) when HasEntries.
type TreeNodeInfo struct {
	Prefix     string
	Hash       string
	Children   []string
	HasEntries bool
	Entries    map[string]string
}

// TreeNodesResultMessage answers TREE_NODES (TREE_NODES_RESULT 0x35), one node per prefix asked
// for, in order.
type TreeNodesResultMessage struct {
	H         Header
	Namespace string
	TreeHex   string
	Nodes     []TreeNodeInfo
}

func (m TreeNodesResultMessage) Header() Header { return m.H }

// ObjectRef names one document body: the document and the content hash it must have. TreeHex, when
// set, is a tree the host may find it in; the host otherwise looks in its main head's tree.
type ObjectRef struct {
	DocID      string
	ContentHex string
	TreeHex    string
}

// ObjectFetchMessage asks for document bodies by content hash (OBJECT_FETCH 0x36).
type ObjectFetchMessage struct {
	H         Header
	Namespace string
	Items     []ObjectRef
}

func (m ObjectFetchMessage) Header() Header { return m.H }

// ObjectFetchResultMessage answers OBJECT_FETCH (OBJECT_FETCH_RESULT 0x37): every body the host
// holds with the asked-for content hash, and the ids it could not supply.
type ObjectFetchResultMessage struct {
	H         Header
	Namespace string
	Docs      []SnapshotDoc
	Missing   []string
}

func (m ObjectFetchResultMessage) Header() Header { return m.H }

type treeNodesDto struct {
	Namespace  string   `json:"namespace"`
	TreeHex    string   `json:"treeHex,omitempty"`
	Prefixes   []string `json:"prefixes"`
	EntryLimit int      `json:"entryLimit,omitempty"`
}

type treeNodeInfoDto struct {
	Prefix     string            `json:"prefix"`
	Hash       string            `json:"hash,omitempty"`
	Children   []string          `json:"children,omitempty"`
	HasEntries bool              `json:"hasEntries,omitempty"`
	Entries    map[string]string `json:"entries,omitempty"`
}

type treeNodesResultDto struct {
	Namespace string            `json:"namespace"`
	TreeHex   string            `json:"treeHex"`
	Nodes     []treeNodeInfoDto `json:"nodes"`
}

type objectRefDto struct {
	DocID      string `json:"id"`
	ContentHex string `json:"contentHash"`
	TreeHex    string `json:"treeHex,omitempty"`
}

type objectFetchDto struct {
	Namespace string         `json:"namespace"`
	Items     []objectRefDto `json:"items"`
}

type objectFetchResultDto struct {
	Namespace string           `json:"namespace"`
	Docs      []snapshotDocDto `json:"docs"`
	Missing   []string         `json:"missing,omitempty"`
}

func encodeRepairMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case TreeNodesMessage:
		return payloadEnvelope{Kind: "treeNodes", TreeNodes: &treeNodesDto{
			Namespace: m.Namespace, TreeHex: m.TreeHex, Prefixes: m.Prefixes, EntryLimit: m.EntryLimit,
		}}, true, nil
	case TreeNodesResultMessage:
		nodes := make([]treeNodeInfoDto, len(m.Nodes))
		for i, n := range m.Nodes {
			nodes[i] = treeNodeInfoDto(n)
		}
		return payloadEnvelope{Kind: "treeNodesResult", TreeNodesResult: &treeNodesResultDto{
			Namespace: m.Namespace, TreeHex: m.TreeHex, Nodes: nodes,
		}}, true, nil
	case ObjectFetchMessage:
		items := make([]objectRefDto, len(m.Items))
		for i, it := range m.Items {
			items[i] = objectRefDto(it)
		}
		return payloadEnvelope{Kind: "objectFetch", ObjectFetch: &objectFetchDto{Namespace: m.Namespace, Items: items}}, true, nil
	case ObjectFetchResultMessage:
		docs := make([]snapshotDocDto, len(m.Docs))
		for i, d := range m.Docs {
			docs[i] = snapshotDocDto{DocID: d.DocID, Body: d.Body}
		}
		return payloadEnvelope{Kind: "objectFetchResult", ObjectFetchResult: &objectFetchResultDto{
			Namespace: m.Namespace, Docs: docs, Missing: m.Missing,
		}}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeRepairMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "treeNodes":
		d := env.TreeNodes
		if d == nil {
			return nil, true, newDecodeError("missing treeNodes body")
		}
		return TreeNodesMessage{H: header, Namespace: d.Namespace, TreeHex: d.TreeHex, Prefixes: d.Prefixes, EntryLimit: d.EntryLimit}, true, nil
	case "treeNodesResult":
		d := env.TreeNodesResult
		if d == nil {
			return nil, true, newDecodeError("missing treeNodesResult body")
		}
		nodes := make([]TreeNodeInfo, len(d.Nodes))
		for i, n := range d.Nodes {
			nodes[i] = TreeNodeInfo(n)
		}
		return TreeNodesResultMessage{H: header, Namespace: d.Namespace, TreeHex: d.TreeHex, Nodes: nodes}, true, nil
	case "objectFetch":
		d := env.ObjectFetch
		if d == nil {
			return nil, true, newDecodeError("missing objectFetch body")
		}
		items := make([]ObjectRef, len(d.Items))
		for i, it := range d.Items {
			items[i] = ObjectRef(it)
		}
		return ObjectFetchMessage{H: header, Namespace: d.Namespace, Items: items}, true, nil
	case "objectFetchResult":
		d := env.ObjectFetchResult
		if d == nil {
			return nil, true, newDecodeError("missing objectFetchResult body")
		}
		docs := make([]SnapshotDoc, len(d.Docs))
		for i, x := range d.Docs {
			docs[i] = SnapshotDoc{DocID: x.DocID, Body: x.Body}
		}
		return ObjectFetchResultMessage{H: header, Namespace: d.Namespace, Docs: docs, Missing: d.Missing}, true, nil
	}
	return nil, false, nil
}
