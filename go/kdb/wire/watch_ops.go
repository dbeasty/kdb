package wire

// SyncCapWatch: the host answers REFS_WATCH, and then tells the session whenever a watched
// namespace's refs move, so a client replica learns of new commits without polling
// (docs/kdb-client-replica-plan.md §6). Codes 0x50-0x57 are reserved for the replica.
const SyncCapWatch = "watch"

// RefsWatchMessage asks the host to report the refs of Namespaces now and whenever they change,
// on this session, until it ends (REFS_WATCH 0x50). A new watch replaces the session's previous one.
type RefsWatchMessage struct {
	H          Header
	Namespaces []string
}

func (m RefsWatchMessage) Header() Header { return m.H }

// RefsChangedMessage carries refs of watched namespaces (REFS_CHANGED 0x51): first as the reply to
// REFS_WATCH, with every watched namespace, then unsolicited - under the REFS_WATCH's correlation
// id - with the namespaces whose refs moved. Changes close together are reported once.
type RefsChangedMessage struct {
	H    Header
	Refs []NamespaceRefs
}

func (m RefsChangedMessage) Header() Header { return m.H }

type refsWatchDto struct {
	Namespaces []string `json:"namespaces"`
}

type refsChangedDto struct {
	Refs []namespaceRefsDto `json:"refs"`
}

func encodeWatchMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case RefsWatchMessage:
		return payloadEnvelope{Kind: "refsWatch", RefsWatch: &refsWatchDto{Namespaces: m.Namespaces}}, true, nil
	case RefsChangedMessage:
		return payloadEnvelope{Kind: "refsChanged", RefsChanged: &refsChangedDto{Refs: refsToDto(m.Refs)}}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeWatchMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "refsWatch":
		if env.RefsWatch == nil {
			return nil, true, newDecodeError("missing refsWatch body")
		}
		return RefsWatchMessage{H: header, Namespaces: env.RefsWatch.Namespaces}, true, nil
	case "refsChanged":
		if env.RefsChanged == nil {
			return nil, true, newDecodeError("missing refsChanged body")
		}
		return RefsChangedMessage{H: header, Refs: refsFromDto(env.RefsChanged.Refs)}, true, nil
	}
	return nil, false, nil
}
