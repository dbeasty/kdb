package wire

// SyncCapHome: the host answers HOME_REQUEST.
const SyncCapHome = "home"

// HomeRequestMessage asks the host - a single-home namespace's current home - to hand the
// namespace to the requesting node (HOME_REQUEST 0x3E). Node must be the session's own node: a
// node asks for itself. Force asks a node the namespace's resolution chain names as its authority
// to reassign the home without the current home, when that home is unreachable; the fence makes
// the old home's later writes unadoptable.
type HomeRequestMessage struct {
	H         Header
	Namespace string
	Node      string
	Addr      string
	Force     bool
	// Reason is passed to the host's handover policy, for the application's decision and logs.
	Reason string
}

func (m HomeRequestMessage) Header() Header { return m.H }

// HomeRequestResultMessage answers HOME_REQUEST (HOME_REQUEST_RESULT 0x3F). Granted with the new
// assignment, or refused with Reason - and, when the host is not the home, the current assignment
// in Home* so the requester can ask the right node.
type HomeRequestResultMessage struct {
	H         Header
	Namespace string
	Granted   bool
	Reason    string
	HomeNode  string
	HomeAddr  string
	Fence     int64
	Since     string
}

func (m HomeRequestResultMessage) Header() Header { return m.H }

type homeRequestDto struct {
	Namespace string `json:"namespace"`
	Node      string `json:"node"`
	Addr      string `json:"addr,omitempty"`
	Force     bool   `json:"force,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type homeRequestResultDto struct {
	Namespace string `json:"namespace"`
	Granted   bool   `json:"granted,omitempty"`
	Reason    string `json:"reason,omitempty"`
	HomeNode  string `json:"homeNode,omitempty"`
	HomeAddr  string `json:"homeAddr,omitempty"`
	Fence     int64  `json:"fence,omitempty"`
	Since     string `json:"since,omitempty"`
}

func encodeHomeRequestMessage(msg Message) (payloadEnvelope, bool, error) {
	switch m := msg.(type) {
	case HomeRequestMessage:
		return payloadEnvelope{Kind: "homeRequest", HomeRequest: &homeRequestDto{
			Namespace: m.Namespace, Node: m.Node, Addr: m.Addr, Force: m.Force, Reason: m.Reason,
		}}, true, nil
	case HomeRequestResultMessage:
		return payloadEnvelope{Kind: "homeRequestResult", HomeRequestResult: &homeRequestResultDto{
			Namespace: m.Namespace, Granted: m.Granted, Reason: m.Reason,
			HomeNode: m.HomeNode, HomeAddr: m.HomeAddr, Fence: m.Fence, Since: m.Since,
		}}, true, nil
	}
	return payloadEnvelope{}, false, nil
}

func decodeHomeRequestMessage(header Header, env payloadEnvelope) (Message, bool, error) {
	switch env.Kind {
	case "homeRequest":
		d := env.HomeRequest
		if d == nil {
			return nil, true, newDecodeError("missing homeRequest body")
		}
		return HomeRequestMessage{H: header, Namespace: d.Namespace, Node: d.Node, Addr: d.Addr, Force: d.Force, Reason: d.Reason}, true, nil
	case "homeRequestResult":
		d := env.HomeRequestResult
		if d == nil {
			return nil, true, newDecodeError("missing homeRequestResult body")
		}
		return HomeRequestResultMessage{H: header, Namespace: d.Namespace, Granted: d.Granted, Reason: d.Reason,
			HomeNode: d.HomeNode, HomeAddr: d.HomeAddr, Fence: d.Fence, Since: d.Since}, true, nil
	}
	return nil, false, nil
}
