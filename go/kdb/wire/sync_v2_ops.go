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
}

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
	Shallow         []string          `json:"shallow,omitempty"`
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
			HistoryFloorHex: r.HistoryFloorHex, Shallow: r.Shallow,
		}
	}
	return out
}

func refsFromDto(dtos []namespaceRefsDto) []NamespaceRefs {
	out := make([]NamespaceRefs, len(dtos))
	for i, d := range dtos {
		out[i] = NamespaceRefs{
			Namespace: d.Namespace, Branches: d.Branches, Tags: d.Tags,
			HistoryFloorHex: d.HistoryFloorHex, Shallow: d.Shallow,
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
