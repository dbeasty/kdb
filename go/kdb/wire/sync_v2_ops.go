package wire

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Peer sync protocol v2 (SYNC_HELLO 0x26 .. REF_UPDATE_ACK 0x2D). Go-only, like 0x14-0x25.
//
// v1 (HANDSHAKE / COMMIT_FETCH / COMMIT_PUSH) serves one namespace's "main" branch per
// connection. v2 serves every namespace the connecting peer may sync, and every branch and tag in
// each, over one connection:
//
//	SYNC_HELLO      -> SYNC_HELLO_ACK    authenticate, grant namespaces, advertise their refs
//	REFS_REQUEST    -> REFS_RESULT       re-read refs mid-session
//	FETCH_REQUEST   -> PACK_PAGE         commits the fetcher lacks, parents first, paged by bytes
//	REF_UPDATE      -> REF_UPDATE_ACK    push a page of commits; on the last page, propose a ref
//
// Every request gets a reply; failures are PEER_ERROR (0x25).

// SyncProtocolVersion is the peer sync protocol a v2 node speaks.
const SyncProtocolVersion = 2

// Sync capabilities a node may advertise in SYNC_HELLO / SYNC_HELLO_ACK.
const (
	SyncCapBranches = "branches"
	SyncCapTags     = "tags"
	SyncCapStubs    = "stubs"
	SyncCapSnapshot = "snapshot"
	SyncCapFilter   = "filter"
	// SyncCapWriteBack: the host accepts PROJECT_WRITE, a projection's local writes sent back.
	SyncCapWriteBack = "writeback"
)

// RefKind distinguishes the two kinds of ref a namespace has.
type RefKind string

const (
	RefBranch RefKind = "branch"
	RefTag    RefKind = "tag"
)

// NamespaceRefs is one namespace's refs as a node advertises them.
type NamespaceRefs struct {
	Namespace string
	// Branches maps branch name to head commit hex; always contains "main".
	Branches map[string]string
	// Tags maps tag name to the commit hex it names.
	Tags map[string]string
	// HistoryFloorHex, when set, is the oldest commit this node can still send; a fetcher whose
	// history ends below it needs a snapshot instead (Phase 4).
	HistoryFloorHex string
	// Shallow lists commits this node holds without their parents.
	Shallow []string
	// ResolutionHash identifies the namespace's conflict resolution chain (empty: none). Peers
	// whose hashes differ would settle the same conflict differently, so neither merges.
	ResolutionHash string
	// Access is what the session may do with the namespace: "" both directions, AccessPull or
	// AccessPush one only. A client skips the other direction instead of being refused in it.
	Access string
}

// Access values of NamespaceRefs.
const (
	AccessPull = "pull"
	AccessPush = "push"
)

// SyncHelloMessage opens a v2 session.
type SyncHelloMessage struct {
	H        Header
	NodeID   string
	Protocol int
	// Capabilities are what this node can do; the session uses the intersection.
	Capabilities []string
	// Namespaces are the namespaces (or patterns: "*" one segment, "**" any depth, "!" prefix
	// excludes) the connecting node wants to sync.
	Namespaces []string
	User       *string
	Password   *string
	Token      *string
}

func (m SyncHelloMessage) Header() Header { return m.H }

// SyncHelloAckMessage answers SYNC_HELLO.
type SyncHelloAckMessage struct {
	H            Header
	Accepted     bool
	NodeID       string
	Protocol     int
	Capabilities []string
	// Refs holds every namespace the session was granted, with its refs.
	Refs   []NamespaceRefs
	Reason string
}

func (m SyncHelloAckMessage) Header() Header { return m.H }

// RefsRequestMessage asks for the current refs of namespaces already granted this session.
type RefsRequestMessage struct {
	H          Header
	Namespaces []string
}

func (m RefsRequestMessage) Header() Header { return m.H }

// RefsResultMessage answers REFS_REQUEST.
type RefsResultMessage struct {
	H    Header
	Refs []NamespaceRefs
}

func (m RefsResultMessage) Header() Header { return m.H }

// FetchRequestMessage asks for every commit reachable from Wants and from none of Haves.
type FetchRequestMessage struct {
	H         Header
	Namespace string
	Wants     []codec.Hash
	Haves     []codec.Hash
	// MaxBytes caps the page's encoded commit bytes; a single commit larger than it is still
	// sent alone. 0 means the host's default.
	MaxBytes int
}

func (m FetchRequestMessage) Header() Header { return m.H }

// PackPageMessage answers FETCH_REQUEST with one page of commits, parents first.
type PackPageMessage struct {
	H         Header
	Namespace string
	Commits   []document.Commit
	Stubs     []document.CommitStub
	// Done is true when nothing further is missing; otherwise fetch again naming the tips of
	// what has been received as haves.
	Done bool
}

func (m PackPageMessage) Header() Header { return m.H }

// RefUpdateMessage pushes a page of commits and, on its last page, proposes a ref value.
type RefUpdateMessage struct {
	H         Header
	Namespace string
	Kind      RefKind
	Ref       string
	// OldHex is what the pusher last saw the ref at; informational - the receiver decides by
	// ancestry, never by compare-and-swap on this value.
	OldHex  string
	NewHex  string
	Commits []document.Commit
	Stubs   []document.CommitStub
	// More means further pages follow: store these, decide nothing yet.
	More bool
}

func (m RefUpdateMessage) Header() Header { return m.H }

// RefUpdateOutcome is what the receiver did with a proposed ref.
type RefUpdateOutcome string

const (
	RefStored        RefUpdateOutcome = "stored" // a More page: commits stored, nothing decided
	RefFastForwarded RefUpdateOutcome = "fast-forwarded"
	RefMerged        RefUpdateOutcome = "merged"
	RefNoOp          RefUpdateOutcome = "noop"
	RefCreated       RefUpdateOutcome = "created"
	RefConflict      RefUpdateOutcome = "conflict"
)

// RefUpdateAckMessage answers REF_UPDATE.
type RefUpdateAckMessage struct {
	H         Header
	Namespace string
	Kind      RefKind
	Ref       string
	Outcome   RefUpdateOutcome
	// HeadHex is the ref's value on the receiver after the decision.
	HeadHex string
	// Stored counts commits this page added.
	Stored int
	// ConflictBytes is the JSON ConflictReport when Outcome is conflict.
	ConflictBytes []byte
}

func (m RefUpdateAckMessage) Header() Header { return m.H }

// DTOs.

type namespaceRefsDto struct {
	Namespace       string            `json:"namespace"`
	Branches        map[string]string `json:"branches"`
	Tags            map[string]string `json:"tags,omitempty"`
	HistoryFloorHex string            `json:"historyFloorHex,omitempty"`
	ResolutionHash  string            `json:"resolutionHash,omitempty"`
	Shallow         []string          `json:"shallow,omitempty"`
	Access          string            `json:"access,omitempty"`
}

type syncHelloDto struct {
	NodeID       string   `json:"nodeId"`
	Protocol     int      `json:"protocol"`
	Capabilities []string `json:"capabilities,omitempty"`
	Namespaces   []string `json:"namespaces"`
	User         *string  `json:"user,omitempty"`
	Password     *string  `json:"password,omitempty"`
	Token        *string  `json:"token,omitempty"`
}

type syncHelloAckDto struct {
	Accepted     bool               `json:"accepted"`
	NodeID       string             `json:"nodeId"`
	Protocol     int                `json:"protocol"`
	Capabilities []string           `json:"capabilities,omitempty"`
	Refs         []namespaceRefsDto `json:"refs,omitempty"`
	Reason       string             `json:"reason,omitempty"`
}

type refsRequestDto struct {
	Namespaces []string `json:"namespaces"`
}

type refsResultDto struct {
	Refs []namespaceRefsDto `json:"refs"`
}

type fetchRequestDto struct {
	Namespace string   `json:"namespace"`
	Wants     []string `json:"wants"`
	Haves     []string `json:"haves,omitempty"`
	MaxBytes  int      `json:"maxBytes,omitempty"`
}

type packPageDto struct {
	Namespace string `json:"namespace"`
	// []byte (base64), not jsonByteArray: v2 is Go-only, so it does not need the integer-array
	// form Kotlin's decoder requires, which is three to four times larger on the wire.
	CommitsPayload []byte          `json:"commitsPayload"`
	Stubs          []commitStubDto `json:"stubs,omitempty"`
	Done           bool            `json:"done"`
}

type refUpdateDto struct {
	Namespace      string          `json:"namespace"`
	Kind           RefKind         `json:"kind"`
	Ref            string          `json:"ref"`
	OldHex         string          `json:"oldHex,omitempty"`
	NewHex         string          `json:"newHex"`
	CommitsPayload []byte          `json:"commitsPayload"`
	Stubs          []commitStubDto `json:"stubs,omitempty"`
	More           bool            `json:"more,omitempty"`
}

type refUpdateAckDto struct {
	Namespace     string           `json:"namespace"`
	Kind          RefKind          `json:"kind"`
	Ref           string           `json:"ref"`
	Outcome       RefUpdateOutcome `json:"outcome"`
	HeadHex       string           `json:"headHex"`
	Stored        int              `json:"stored"`
	ConflictBytes []byte           `json:"conflictBytes,omitempty"`
}

func refsToDto(refs []NamespaceRefs) []namespaceRefsDto {
	out := make([]namespaceRefsDto, len(refs))
	for i, r := range refs {
		out[i] = namespaceRefsDto{
			Namespace: r.Namespace, Branches: r.Branches, Tags: r.Tags,
			HistoryFloorHex: r.HistoryFloorHex, Shallow: r.Shallow, ResolutionHash: r.ResolutionHash,
			Access: r.Access,
		}
	}
	return out
}

func refsFromDto(dtos []namespaceRefsDto) []NamespaceRefs {
	out := make([]NamespaceRefs, len(dtos))
	for i, d := range dtos {
		out[i] = NamespaceRefs{
			Namespace: d.Namespace, Branches: d.Branches, Tags: d.Tags,
			HistoryFloorHex: d.HistoryFloorHex, Shallow: d.Shallow, ResolutionHash: d.ResolutionHash,
			Access: d.Access,
		}
	}
	return out
}

func encodeSyncV2Message(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case SyncHelloMessage:
		return payloadEnvelope{Kind: "syncHello", SyncHello: &syncHelloDto{
			NodeID: m.NodeID, Protocol: m.Protocol, Capabilities: m.Capabilities, Namespaces: m.Namespaces,
			User: m.User, Password: m.Password, Token: m.Token,
		}}, true, nil
	case SyncHelloAckMessage:
		return payloadEnvelope{Kind: "syncHelloAck", SyncHelloAck: &syncHelloAckDto{
			Accepted: m.Accepted, NodeID: m.NodeID, Protocol: m.Protocol, Capabilities: m.Capabilities,
			Refs: refsToDto(m.Refs), Reason: m.Reason,
		}}, true, nil
	case RefsRequestMessage:
		return payloadEnvelope{Kind: "refsRequest", RefsRequest: &refsRequestDto{Namespaces: m.Namespaces}}, true, nil
	case RefsResultMessage:
		return payloadEnvelope{Kind: "refsResult", RefsResult: &refsResultDto{Refs: refsToDto(m.Refs)}}, true, nil
	case FetchRequestMessage:
		return payloadEnvelope{Kind: "fetchRequest", FetchRequest: &fetchRequestDto{
			Namespace: m.Namespace, Wants: hashesToHex(m.Wants), Haves: hashesToHex(m.Haves), MaxBytes: m.MaxBytes,
		}}, true, nil
	case PackPageMessage:
		payload, err := EncodeCommits(m.Commits)
		if err != nil {
			return payloadEnvelope{}, true, err
		}
		return payloadEnvelope{Kind: "packPage", PackPage: &packPageDto{
			Namespace: m.Namespace, CommitsPayload: payload, Stubs: stubsToDto(m.Stubs), Done: m.Done,
		}}, true, nil
	case RefUpdateMessage:
		payload, err := EncodeCommits(m.Commits)
		if err != nil {
			return payloadEnvelope{}, true, err
		}
		return payloadEnvelope{Kind: "refUpdate", RefUpdate: &refUpdateDto{
			Namespace: m.Namespace, Kind: m.Kind, Ref: m.Ref, OldHex: m.OldHex, NewHex: m.NewHex,
			CommitsPayload: payload, Stubs: stubsToDto(m.Stubs), More: m.More,
		}}, true, nil
	case RefUpdateAckMessage:
		return payloadEnvelope{Kind: "refUpdateAck", RefUpdateAck: &refUpdateAckDto{
			Namespace: m.Namespace, Kind: m.Kind, Ref: m.Ref, Outcome: m.Outcome, HeadHex: m.HeadHex,
			Stored: m.Stored, ConflictBytes: m.ConflictBytes,
		}}, true, nil
	default:
		return payloadEnvelope{}, false, nil
	}
}

func decodeSyncV2Message(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "syncHello":
		d := env.SyncHello
		if d == nil {
			return nil, true, newDecodeError("missing syncHello body")
		}
		return SyncHelloMessage{
			H: header, NodeID: d.NodeID, Protocol: d.Protocol, Capabilities: d.Capabilities, Namespaces: d.Namespaces,
			User: d.User, Password: d.Password, Token: d.Token,
		}, true, nil
	case "syncHelloAck":
		d := env.SyncHelloAck
		if d == nil {
			return nil, true, newDecodeError("missing syncHelloAck body")
		}
		return SyncHelloAckMessage{
			H: header, Accepted: d.Accepted, NodeID: d.NodeID, Protocol: d.Protocol, Capabilities: d.Capabilities,
			Refs: refsFromDto(d.Refs), Reason: d.Reason,
		}, true, nil
	case "refsRequest":
		d := env.RefsRequest
		if d == nil {
			return nil, true, newDecodeError("missing refsRequest body")
		}
		return RefsRequestMessage{H: header, Namespaces: d.Namespaces}, true, nil
	case "refsResult":
		d := env.RefsResult
		if d == nil {
			return nil, true, newDecodeError("missing refsResult body")
		}
		return RefsResultMessage{H: header, Refs: refsFromDto(d.Refs)}, true, nil
	case "fetchRequest":
		d := env.FetchRequest
		if d == nil {
			return nil, true, newDecodeError("missing fetchRequest body")
		}
		wants, err := hashesFromHex(d.Wants)
		if err != nil {
			return nil, true, err
		}
		haves, err := hashesFromHex(d.Haves)
		if err != nil {
			return nil, true, err
		}
		return FetchRequestMessage{H: header, Namespace: d.Namespace, Wants: wants, Haves: haves, MaxBytes: d.MaxBytes}, true, nil
	case "packPage":
		d := env.PackPage
		if d == nil {
			return nil, true, newDecodeError("missing packPage body")
		}
		commits, err := DecodeCommits(d.CommitsPayload)
		if err != nil {
			return nil, true, err
		}
		stubs, err := stubsFromDto(d.Stubs)
		if err != nil {
			return nil, true, err
		}
		return PackPageMessage{H: header, Namespace: d.Namespace, Commits: commits, Stubs: stubs, Done: d.Done}, true, nil
	case "refUpdate":
		d := env.RefUpdate
		if d == nil {
			return nil, true, newDecodeError("missing refUpdate body")
		}
		commits, err := DecodeCommits(d.CommitsPayload)
		if err != nil {
			return nil, true, err
		}
		stubs, err := stubsFromDto(d.Stubs)
		if err != nil {
			return nil, true, err
		}
		return RefUpdateMessage{
			H: header, Namespace: d.Namespace, Kind: d.Kind, Ref: d.Ref, OldHex: d.OldHex, NewHex: d.NewHex,
			Commits: commits, Stubs: stubs, More: d.More,
		}, true, nil
	case "refUpdateAck":
		d := env.RefUpdateAck
		if d == nil {
			return nil, true, newDecodeError("missing refUpdateAck body")
		}
		return RefUpdateAckMessage{
			H: header, Namespace: d.Namespace, Kind: d.Kind, Ref: d.Ref, Outcome: d.Outcome, HeadHex: d.HeadHex,
			Stored: d.Stored, ConflictBytes: d.ConflictBytes,
		}, true, nil
	default:
		return nil, false, nil
	}
}

// SnapshotFetchMessage asks for the documents of the tree at a commit, in document-id order,
// starting after a cursor - one page of a snapshot bootstrap (SNAPSHOT_FETCH 0x2E).
type SnapshotFetchMessage struct {
	H         Header
	Namespace string
	// AtHex is the commit whose tree to send; empty means the host's main head, and the page
	// says which commit that was.
	AtHex string
	// After is the last document id received; empty starts from the first.
	After    string
	MaxBytes int
}

func (m SnapshotFetchMessage) Header() Header { return m.H }

// SnapshotDoc is one document of a snapshot page.
type SnapshotDoc struct {
	DocID string
	Body  string
}

// SnapshotPageMessage answers SNAPSHOT_FETCH (SNAPSHOT_PAGE 0x2F).
type SnapshotPageMessage struct {
	H         Header
	Namespace string
	// Commit is the commit the snapshot is of. It travels on every page so a receiver can check
	// every page belongs to the same snapshot.
	Commit document.Commit
	Docs   []SnapshotDoc
	// Next is the cursor for the following page; Done means there is none.
	Next string
	Done bool
	// Total is how many documents the whole snapshot holds.
	Total int
}

func (m SnapshotPageMessage) Header() Header { return m.H }

type snapshotFetchDto struct {
	Namespace string `json:"namespace"`
	AtHex     string `json:"atHex,omitempty"`
	After     string `json:"after,omitempty"`
	MaxBytes  int    `json:"maxBytes,omitempty"`
}

type snapshotDocDto struct {
	DocID string `json:"id"`
	Body  string `json:"body"`
}

type snapshotPageDto struct {
	Namespace     string           `json:"namespace"`
	CommitPayload []byte           `json:"commitPayload"`
	Docs          []snapshotDocDto `json:"docs"`
	Next          string           `json:"next,omitempty"`
	Done          bool             `json:"done"`
	Total         int              `json:"total"`
}

func encodeSnapshotV2Message(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case SnapshotFetchMessage:
		return payloadEnvelope{Kind: "snapshotFetch", SnapshotFetch: &snapshotFetchDto{
			Namespace: m.Namespace, AtHex: m.AtHex, After: m.After, MaxBytes: m.MaxBytes,
		}}, true, nil
	case SnapshotPageMessage:
		payload, err := EncodeCommits([]document.Commit{m.Commit})
		if err != nil {
			return payloadEnvelope{}, true, err
		}
		docs := make([]snapshotDocDto, len(m.Docs))
		for i, d := range m.Docs {
			docs[i] = snapshotDocDto{DocID: d.DocID, Body: d.Body}
		}
		return payloadEnvelope{Kind: "snapshotPage", SnapshotPage: &snapshotPageDto{
			Namespace: m.Namespace, CommitPayload: payload, Docs: docs, Next: m.Next, Done: m.Done, Total: m.Total,
		}}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeSnapshotV2Message(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "snapshotFetch":
		d := env.SnapshotFetch
		if d == nil {
			return nil, true, newDecodeError("missing snapshotFetch body")
		}
		return SnapshotFetchMessage{H: header, Namespace: d.Namespace, AtHex: d.AtHex, After: d.After, MaxBytes: d.MaxBytes}, true, nil
	case "snapshotPage":
		d := env.SnapshotPage
		if d == nil {
			return nil, true, newDecodeError("missing snapshotPage body")
		}
		commits, err := DecodeCommits(d.CommitPayload)
		if err != nil {
			return nil, true, err
		}
		if len(commits) != 1 {
			return nil, true, newDecodeError("snapshotPage must carry exactly one commit")
		}
		docs := make([]SnapshotDoc, len(d.Docs))
		for i, x := range d.Docs {
			docs[i] = SnapshotDoc{DocID: x.DocID, Body: x.Body}
		}
		return SnapshotPageMessage{H: header, Namespace: d.Namespace, Commit: commits[0], Docs: docs, Next: d.Next, Done: d.Done, Total: d.Total}, true, nil
	}
	return nil, false, nil
}

// ProjectFetchMessage asks for one page of a filtered projection of a namespace (PROJECT_FETCH
// 0x30): what changed in the set of documents matching Filter between FromHex and AtHex.
type ProjectFetchMessage struct {
	H         Header
	Namespace string
	// Filter is a KDB-SQL boolean expression over document fields.
	Filter string
	// FromHex is the source commit the replica's projection last reached; empty, or a commit the
	// host cannot relate to AtHex, gets a full filtered snapshot instead of a delta.
	FromHex string
	// AtHex fixes the source commit being projected to across the pages of one transfer; empty
	// on the first page means the host's head, which the page reports.
	AtHex    string
	After    string
	MaxBytes int
}

func (m ProjectFetchMessage) Header() Header { return m.H }

// ProjectPageMessage answers PROJECT_FETCH (PROJECT_PAGE 0x31).
type ProjectPageMessage struct {
	H         Header
	Namespace string
	// AtHex is the source commit this transfer projects to.
	AtHex string
	// Reset means this is a full snapshot of the filtered set: the replica replaces its
	// projection with exactly the documents the transfer delivers.
	Reset   bool
	Writes  []SnapshotDoc
	Deletes []string
	Next    string
	Done    bool
}

func (m ProjectPageMessage) Header() Header { return m.H }

// WriteBackDoc is one document's part of a write-back: its final state on the projection, and the
// state the projection held when the write was made.
type WriteBackDoc struct {
	DocID string
	// Body is the document's final JSON; empty when Deleted.
	Body    string
	Deleted bool
	// BaseHash is the content hash of the document the write replaced, "" if it was absent. The
	// host applies the write only if it still holds exactly that.
	BaseHash string
}

// ProjectWriteMessage sends one local transaction of a projection back to its source
// (PROJECT_WRITE 0x32). TxID makes it idempotent: a resend of an applied write answers applied.
type ProjectWriteMessage struct {
	H         Header
	Namespace string
	TxID      string
	Docs      []WriteBackDoc
}

func (m ProjectWriteMessage) Header() Header { return m.H }

// Write-back outcomes.
const (
	// WriteBackApplied: the source committed the write (or had already).
	WriteBackApplied = "applied"
	// WriteBackConflict: a document had changed at the source since the projection's base.
	WriteBackConflict = "conflict"
	// WriteBackRefused: the source will never accept this write as sent - not authorized, a
	// schema violation, a namespace that takes no writes. Resending it cannot help.
	WriteBackRefused = "refused"
)

// WriteBackCurrent is the source's current state of one document after a write-back it did not
// apply, so the projection can put it back. Absent when the document is gone or not readable.
type WriteBackCurrent struct {
	DocID  string
	Body   string
	Absent bool
}

// ProjectWriteResultMessage answers PROJECT_WRITE (PROJECT_WRITE_RESULT 0x33). A failure that
// may pass - the source unavailable, not the home - is a PEER_ERROR instead, and the write stays
// pending.
type ProjectWriteResultMessage struct {
	H         Header
	Namespace string
	Outcome   string
	CommitHex string
	Reason    string
	// Current is set unless Outcome is applied: the source's state of every document the write
	// named.
	Current []WriteBackCurrent
}

func (m ProjectWriteResultMessage) Header() Header { return m.H }

type writeBackDocDto struct {
	DocID    string `json:"docId"`
	Body     string `json:"body,omitempty"`
	Deleted  bool   `json:"deleted,omitempty"`
	BaseHash string `json:"baseHash,omitempty"`
}

type projectWriteDto struct {
	Namespace string            `json:"namespace"`
	TxID      string            `json:"txId"`
	Docs      []writeBackDocDto `json:"docs"`
}

type writeBackCurrentDto struct {
	DocID  string `json:"docId"`
	Body   string `json:"body,omitempty"`
	Absent bool   `json:"absent,omitempty"`
}

type projectWriteResultDto struct {
	Namespace string                `json:"namespace"`
	Outcome   string                `json:"outcome"`
	CommitHex string                `json:"commitHex,omitempty"`
	Reason    string                `json:"reason,omitempty"`
	Current   []writeBackCurrentDto `json:"current,omitempty"`
}

type projectFetchDto struct {
	Namespace string `json:"namespace"`
	Filter    string `json:"filter"`
	FromHex   string `json:"fromHex,omitempty"`
	AtHex     string `json:"atHex,omitempty"`
	After     string `json:"after,omitempty"`
	MaxBytes  int    `json:"maxBytes,omitempty"`
}

type projectPageDto struct {
	Namespace string           `json:"namespace"`
	AtHex     string           `json:"atHex"`
	Reset     bool             `json:"reset,omitempty"`
	Writes    []snapshotDocDto `json:"writes,omitempty"`
	Deletes   []string         `json:"deletes,omitempty"`
	Next      string           `json:"next,omitempty"`
	Done      bool             `json:"done"`
}

func encodeProjectionMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case ProjectFetchMessage:
		return payloadEnvelope{Kind: "projectFetch", ProjectFetch: &projectFetchDto{
			Namespace: m.Namespace, Filter: m.Filter, FromHex: m.FromHex, AtHex: m.AtHex, After: m.After, MaxBytes: m.MaxBytes,
		}}, true, nil
	case ProjectPageMessage:
		writes := make([]snapshotDocDto, len(m.Writes))
		for i, d := range m.Writes {
			writes[i] = snapshotDocDto{DocID: d.DocID, Body: d.Body}
		}
		return payloadEnvelope{Kind: "projectPage", ProjectPage: &projectPageDto{
			Namespace: m.Namespace, AtHex: m.AtHex, Reset: m.Reset, Writes: writes, Deletes: m.Deletes, Next: m.Next, Done: m.Done,
		}}, true, nil
	case ProjectWriteMessage:
		docs := make([]writeBackDocDto, len(m.Docs))
		for i, d := range m.Docs {
			docs[i] = writeBackDocDto{DocID: d.DocID, Body: d.Body, Deleted: d.Deleted, BaseHash: d.BaseHash}
		}
		return payloadEnvelope{Kind: "projectWrite", ProjectWrite: &projectWriteDto{Namespace: m.Namespace, TxID: m.TxID, Docs: docs}}, true, nil
	case ProjectWriteResultMessage:
		current := make([]writeBackCurrentDto, len(m.Current))
		for i, c := range m.Current {
			current[i] = writeBackCurrentDto{DocID: c.DocID, Body: c.Body, Absent: c.Absent}
		}
		return payloadEnvelope{Kind: "projectWriteResult", ProjectWriteResult: &projectWriteResultDto{
			Namespace: m.Namespace, Outcome: m.Outcome, CommitHex: m.CommitHex, Reason: m.Reason, Current: current,
		}}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeProjectionMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "projectFetch":
		d := env.ProjectFetch
		if d == nil {
			return nil, true, newDecodeError("missing projectFetch body")
		}
		return ProjectFetchMessage{H: header, Namespace: d.Namespace, Filter: d.Filter, FromHex: d.FromHex, AtHex: d.AtHex, After: d.After, MaxBytes: d.MaxBytes}, true, nil
	case "projectPage":
		d := env.ProjectPage
		if d == nil {
			return nil, true, newDecodeError("missing projectPage body")
		}
		writes := make([]SnapshotDoc, len(d.Writes))
		for i, x := range d.Writes {
			writes[i] = SnapshotDoc{DocID: x.DocID, Body: x.Body}
		}
		return ProjectPageMessage{H: header, Namespace: d.Namespace, AtHex: d.AtHex, Reset: d.Reset, Writes: writes, Deletes: d.Deletes, Next: d.Next, Done: d.Done}, true, nil
	case "projectWrite":
		d := env.ProjectWrite
		if d == nil {
			return nil, true, newDecodeError("missing projectWrite body")
		}
		docs := make([]WriteBackDoc, len(d.Docs))
		for i, x := range d.Docs {
			docs[i] = WriteBackDoc{DocID: x.DocID, Body: x.Body, Deleted: x.Deleted, BaseHash: x.BaseHash}
		}
		return ProjectWriteMessage{H: header, Namespace: d.Namespace, TxID: d.TxID, Docs: docs}, true, nil
	case "projectWriteResult":
		d := env.ProjectWriteResult
		if d == nil {
			return nil, true, newDecodeError("missing projectWriteResult body")
		}
		var current []WriteBackCurrent
		for _, x := range d.Current {
			current = append(current, WriteBackCurrent{DocID: x.DocID, Body: x.Body, Absent: x.Absent})
		}
		return ProjectWriteResultMessage{H: header, Namespace: d.Namespace, Outcome: d.Outcome, CommitHex: d.CommitHex, Reason: d.Reason, Current: current}, true, nil
	}
	return nil, false, nil
}
