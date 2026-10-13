package wire

import "github.com/limidus/kdb/go/kdb/document"

// SyncCapSquash: the host answers SQUASH_HEAD, the first step of a squash catch-up
// (docs/kdb-memory-cassandra-plan.md C7). The receiver then takes the documents that differ by
// TREE_NODES and OBJECT_FETCH (SyncCapRepair) and jumps its main straight to the host's head.
const SyncCapSquash = "squash"

// SquashHeadMessage asks the host for its main head of Namespace, and whether that head descends
// from HaveHex - the receiver's own head - so the receiver can jump to it as a fast-forward
// (SQUASH_HEAD 0x40).
type SquashHeadMessage struct {
	H         Header
	Namespace string
	HaveHex   string
}

func (m SquashHeadMessage) Header() Header { return m.H }

// SquashHeadResultMessage answers SQUASH_HEAD (SQUASH_HEAD_RESULT 0x41). Commit is the host's main
// head. Descends reports that it descends from the receiver's head, and Backlog how many commits
// lie between them (by generation; 0 when it does not descend). Refused, when set, says why the
// host will not serve a squash - a namespace that requires full history on every replica - and
// the receiver replays instead.
type SquashHeadResultMessage struct {
	H         Header
	Namespace string
	Commit    document.Commit
	Descends  bool
	Backlog   int64
	Refused   string
}

func (m SquashHeadResultMessage) Header() Header { return m.H }

type squashHeadDto struct {
	Namespace string `json:"namespace"`
	HaveHex   string `json:"have"`
}

type squashHeadResultDto struct {
	Namespace     string `json:"namespace"`
	CommitPayload []byte `json:"commitPayload,omitempty"`
	Descends      bool   `json:"descends,omitempty"`
	Backlog       int64  `json:"backlog,omitempty"`
	Refused       string `json:"refused,omitempty"`
}

func encodeSquashMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case SquashHeadMessage:
		return payloadEnvelope{Kind: "squashHead", SquashHead: &squashHeadDto{Namespace: m.Namespace, HaveHex: m.HaveHex}}, true, nil
	case SquashHeadResultMessage:
		d := &squashHeadResultDto{Namespace: m.Namespace, Descends: m.Descends, Backlog: m.Backlog, Refused: m.Refused}
		if m.Refused == "" {
			payload, err := EncodeCommits([]document.Commit{m.Commit})
			if err != nil {
				return payloadEnvelope{}, true, err
			}
			d.CommitPayload = payload
		}
		return payloadEnvelope{Kind: "squashHeadResult", SquashHeadResult: d}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeSquashMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "squashHead":
		d := env.SquashHead
		if d == nil {
			return nil, true, newDecodeError("missing squashHead body")
		}
		return SquashHeadMessage{H: header, Namespace: d.Namespace, HaveHex: d.HaveHex}, true, nil
	case "squashHeadResult":
		d := env.SquashHeadResult
		if d == nil {
			return nil, true, newDecodeError("missing squashHeadResult body")
		}
		out := SquashHeadResultMessage{H: header, Namespace: d.Namespace, Descends: d.Descends, Backlog: d.Backlog, Refused: d.Refused}
		if len(d.CommitPayload) > 0 {
			commits, err := DecodeCommits(d.CommitPayload)
			if err != nil {
				return nil, true, err
			}
			if len(commits) != 1 {
				return nil, true, newDecodeError("squashHeadResult must carry exactly one commit")
			}
			out.Commit = commits[0]
		}
		return out, true, nil
	}
	return nil, false, nil
}
