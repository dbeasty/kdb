package wire

// SyncCapGraft: the host takes GRAFT_PUSH - a peer's shallow root, pushed with its state, for a
// namespace that merges unrelated histories (peersync.Graft).
const SyncCapGraft = "graft"

// GraftPushMessage pushes one page of the state at a shallow root of the sender's that the
// receiver lacks, and whose parents neither holds (GRAFT_PUSH 0x3A). Page is exactly what
// SNAPSHOT_PAGE would carry for that commit: the receiver stages pages until Done, then verifies
// and grafts the root, after which a push of the history above it stores normally. The graft is
// the reverse of a pull's: how a node that is only ever dialled takes in a history that is
// rooted where its own is not.
type GraftPushMessage struct {
	H    Header
	Page SnapshotPageMessage
}

func (m GraftPushMessage) Header() Header { return m.H }

// GraftPushResultMessage answers GRAFT_PUSH (GRAFT_PUSH_RESULT 0x3B). Grafted is set on the answer
// to the last page once the root is grafted (or was already held).
type GraftPushResultMessage struct {
	H         Header
	Namespace string
	Grafted   bool
}

func (m GraftPushResultMessage) Header() Header { return m.H }

type graftPushResultDto struct {
	Namespace string `json:"namespace"`
	Grafted   bool   `json:"grafted,omitempty"`
}

func encodeGraftMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case GraftPushMessage:
		env, _, err := encodeSnapshotV2Message(m.Page)
		if err != nil {
			return payloadEnvelope{}, true, err
		}
		return payloadEnvelope{Kind: "graftPush", GraftPush: env.SnapshotPage}, true, nil
	case GraftPushResultMessage:
		return payloadEnvelope{Kind: "graftPushResult", GraftPushResult: &graftPushResultDto{Namespace: m.Namespace, Grafted: m.Grafted}}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeGraftMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "graftPush":
		if env.GraftPush == nil {
			return nil, true, newDecodeError("missing graftPush body")
		}
		msg, _, err := decodeSnapshotV2Message(header, payloadEnvelope{Kind: "snapshotPage", SnapshotPage: env.GraftPush})
		if err != nil {
			return nil, true, err
		}
		page, _ := msg.(SnapshotPageMessage)
		return GraftPushMessage{H: header, Page: page}, true, nil
	case "graftPushResult":
		d := env.GraftPushResult
		if d == nil {
			return nil, true, newDecodeError("missing graftPushResult body")
		}
		return GraftPushResultMessage{H: header, Namespace: d.Namespace, Grafted: d.Grafted}, true, nil
	}
	return nil, false, nil
}
