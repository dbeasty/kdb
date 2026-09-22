package wire

// SyncCapMetaView: the host answers META_VIEW.
const SyncCapMetaView = "metaview"

// MetaDefinition is one definition document of the metadata namespace, as stored.
type MetaDefinition struct {
	ID   string
	Body string
}

// MetaViewMessage asks for the definitions this session may see (META_VIEW 0x3C): every pattern
// definition, and those of the namespaces the session was granted at hello. How a peer that must
// not learn other namespaces' names - a phone - gets the resolution chains and homes it needs
// without syncing the whole metadata namespace.
type MetaViewMessage struct {
	H Header
}

func (m MetaViewMessage) Header() Header { return m.H }

// MetaViewResultMessage answers META_VIEW (META_VIEW_RESULT 0x3D).
type MetaViewResultMessage struct {
	H           Header
	Definitions []MetaDefinition
}

func (m MetaViewResultMessage) Header() Header { return m.H }

type metaDefinitionDto struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

type metaViewResultDto struct {
	Definitions []metaDefinitionDto `json:"definitions"`
}

type metaViewDto struct{}

func encodeMetaViewMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case MetaViewMessage:
		return payloadEnvelope{Kind: "metaView", MetaView: &metaViewDto{}}, true, nil
	case MetaViewResultMessage:
		defs := make([]metaDefinitionDto, len(m.Definitions))
		for i, d := range m.Definitions {
			defs[i] = metaDefinitionDto(d)
		}
		return payloadEnvelope{Kind: "metaViewResult", MetaViewResult: &metaViewResultDto{Definitions: defs}}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeMetaViewMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "metaView":
		return MetaViewMessage{H: header}, true, nil
	case "metaViewResult":
		d := env.MetaViewResult
		if d == nil {
			return nil, true, newDecodeError("missing metaViewResult body")
		}
		defs := make([]MetaDefinition, len(d.Definitions))
		for i, x := range d.Definitions {
			defs[i] = MetaDefinition(x)
		}
		return MetaViewResultMessage{H: header, Definitions: defs}, true, nil
	}
	return nil, false, nil
}
