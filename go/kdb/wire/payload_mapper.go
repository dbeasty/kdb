package wire

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func messageToEnvelope(msg Message) (PayloadEnvelope, error) {
	switch m := msg.(type) {
	case HandshakeMessage:
		caps := m.Request.Capabilities
		if caps.MaxFrameBytes == 0 {
			caps = DefaultCapabilities()
		}
		encs := m.Request.PreferredEncodings
		if len(encs) == 0 {
			encs = []PayloadEncoding{EncodingKdbBinary, EncodingJSON}
		}
		encNames := make([]string, len(encs))
		for i, e := range encs {
			encNames[i] = e.String()
		}
		pv := m.Request.ProtocolVersion
		if pv == 0 {
			pv = KdbWireProtocolVersion
		}
		localHeads := m.Request.LocalHeads
		if localHeads == nil {
			// Kotlin's HandshakeDto.localHeads is a non-nullable Map<String, String> with no
			// default - a Go SQL client (which never populates this peer-sync/stream-only
			// field) marshals a nil map as JSON null, which the JVM's strict kotlinx.serialization
			// decoder rejects outright (crashes the connection with no response at all, not a
			// clean rejection). {} round-trips as an empty map on both sides.
			localHeads = map[string]string{}
		}
		namespaces := m.Request.Namespaces
		if namespaces == nil {
			// Same class of bug as localHeads above, same fix: Kotlin's HandshakeDto.namespaces
			// is a non-nullable List<String>, so a Go SQL client that never sets this field (e.g.
			// go/kdb/client's Connect, which legitimately has no single target namespace yet at
			// handshake time) must not let it marshal as JSON null.
			namespaces = []string{}
		}
		return payloadEnvelope{
			Kind: "handshake",
			Handshake: &handshakeDto{
				NodeID:                    m.Request.NodeID,
				Namespaces:                namespaces,
				LocalHeads:                localHeads,
				SupportsZstd:              caps.SupportsZstd,
				SupportsIndexHints:        caps.SupportsIndexHints,
				SupportsDirectDeltaIngest: caps.SupportsDirectDeltaIngest,
				MaxFrameBytes:             caps.MaxFrameBytes,
				PreferredEncodings:        encNames,
				ClientMode:                m.Request.ClientMode.String(),
				ProtocolVersion:           pv,
				User:                      m.Request.User,
				Password:                  m.Request.Password,
				Token:                     m.Request.Token,
			},
		}, nil
	case HandshakeAckMessage:
		return payloadEnvelope{
			Kind: "handshakeAck",
			HandshakeAck: &handshakeAckDto{
				Accepted:           m.Response.Accepted,
				NegotiatedEncoding: m.Response.NegotiatedEncoding.String(),
				ProtocolVersion:    m.Response.ProtocolVersion,
				RemoteHeads:        m.Response.RemoteHeads,
				RejectionReason:    m.Response.RejectionReason,
			},
		}, nil
	case DeltaCommitMessage:
		ops := make([]opDto, len(m.Payload.Operations))
		for i, op := range m.Payload.Operations {
			ops[i] = opToDto(op)
		}
		hints := make([]indexHintDto, len(m.Payload.IndexHints))
		for i, h := range m.Payload.IndexHints {
			hints[i] = hintToDto(h)
		}
		return payloadEnvelope{
			Kind: "deltaCommit",
			DeltaCommit: &deltaCommitDto{
				Namespace:        m.Payload.Namespace,
				CommitHashHex:    m.Payload.CommitHash.Hex(),
				ParentHashHex:    m.Payload.ParentHash.Hex(),
				TimestampMicros:  m.Payload.TimestampMicros,
				Operations:       ops,
				IndexHints:       hints,
				SchemaDeltaBytes: m.Payload.SchemaDeltaBytes,
			},
		}, nil
	case CommitFetchMessage:
		var since *string
		if m.SinceHash != nil {
			h := m.SinceHash.Hex()
			since = &h
		}
		return payloadEnvelope{
			Kind: "commitFetch",
			CommitFetch: &commitFetchDto{
				Namespace:    m.Namespace,
				SinceHashHex: since,
				MaxCommits:   m.MaxCommits,
			},
		}, nil
	case CommitPushMessage:
		payload, err := EncodeCommits(m.Commits)
		if err != nil {
			return payloadEnvelope{}, err
		}
		return payloadEnvelope{
			Kind: "commitPush",
			CommitPush: &commitPushDto{
				Namespace:      m.Namespace,
				CommitsPayload: payload,
			},
		}, nil
	case CommitPushAckMessage:
		return payloadEnvelope{
			Kind: "commitPushAck",
			CommitPushAck: &commitPushAckDto{
				Namespace:      m.Namespace,
				AppliedCommits: m.AppliedCommits,
				HeadHex:        m.HeadHex,
			},
		}, nil
	case DagDiffMessage:
		return payloadEnvelope{
			Kind: "dagDiff",
			DagDiff: &dagDiffDto{
				Namespace:     m.Namespace,
				LocalHeadHex:  m.LocalHead.Hex(),
				RemoteHeadHex: m.RemoteHead.Hex(),
			},
		}, nil
	case TransactionReplayMessage:
		return payloadEnvelope{
			Kind: "transactionReplay",
			TransactionReplay: &transactionReplayDto{
				Namespace:        m.Namespace,
				BaseVersionHex:   m.BaseVersion.Hex(),
				TransactionBytes: m.TransactionBytes,
			},
		}, nil
	case ConflictReportMessage:
		return payloadEnvelope{
			Kind: "conflictReport",
			ConflictReport: &conflictReportDto{
				Namespace:   m.Namespace,
				ReportBytes: m.ReportBytes,
			},
		}, nil
	case CompactionNoticeMessage:
		return payloadEnvelope{
			Kind: "compactionNotice",
			CompactionNotice: &compactionNoticeDto{
				NamespaceID:    m.Intent.NamespaceID,
				BoundaryHex:    m.Intent.Boundary.Hex(),
				IssuedAtMillis: m.Intent.IssuedAtMillis,
			},
		}, nil
	case IceArchiveNoticeMessage:
		return payloadEnvelope{
			Kind: "iceArchiveNotice",
			IceArchiveNotice: &iceArchiveNoticeDto{
				Namespace:       m.Namespace,
				OriginalHashHex: m.OriginalHash.Hex(),
				ArchiveLocation: m.ArchiveLocation,
				BundleHashHex:   m.BundleHash.Hex(),
			},
		}, nil
	case SnapshotRequestMessage:
		var anchor *string
		if m.AnchorHash != nil {
			h := m.AnchorHash.Hex()
			anchor = &h
		}
		return payloadEnvelope{
			Kind: "snapshotRequest",
			SnapshotRequest: &snapshotRequestDto{
				Namespace:     m.Namespace,
				AnchorHashHex: anchor,
			},
		}, nil
	case SnapshotResponseMessage:
		return payloadEnvelope{
			Kind: "snapshotResponse",
			SnapshotResponse: &snapshotResponseDto{
				Namespace:     m.Namespace,
				AnchorHashHex: m.AnchorHash.Hex(),
				SnapshotBytes: m.SnapshotBytes,
				Compressed:    m.Compressed,
			},
		}, nil
	case PositionAckMessage:
		return payloadEnvelope{
			Kind: "positionAck",
			PositionAck: &positionAckDto{
				Namespace:     m.Namespace,
				CommitHashHex: m.CommitHash.Hex(),
			},
		}, nil
	case SchemaPushMessage:
		return payloadEnvelope{
			Kind: "schemaPush",
			SchemaPush: &schemaPushDto{
				Namespace:   m.Namespace,
				SchemaBytes: m.SchemaBytes,
				Revision:    m.Revision,
			},
		}, nil
	case SessionBeginMessage:
		return payloadEnvelope{
			Kind: "sessionBegin",
			SessionBegin: &sessionBeginDto{
				Namespace:       m.Namespace,
				SessionID:       m.SessionID,
				ReadConsistency: m.ReadConsistency,
				BaseVersionHex:  m.BaseVersionHex,
			},
		}, nil
	case SessionBeginAckMessage:
		return payloadEnvelope{
			Kind: "sessionBeginAck",
			SessionBeginAck: &sessionBeginAckDto{
				Namespace:       m.Namespace,
				SessionID:       m.SessionID,
				HeadHex:         m.HeadHex,
				ReadConsistency: m.ReadConsistency,
				Error:           m.Error,
			},
		}, nil
	case SqlExecMessage:
		return payloadEnvelope{
			Kind: "sqlExec",
			SqlExec: &sqlExecDto{
				Namespace:      m.Namespace,
				SessionID:      m.SessionID,
				SQL:            m.SQL,
				ParametersJSON: m.ParametersJSON,
			},
		}, nil
	case SqlResultMessage:
		return payloadEnvelope{
			Kind: "sqlResult",
			SqlResult: &sqlResultDto{
				Namespace:         m.Namespace,
				SessionID:         m.SessionID,
				Columns:           m.Columns,
				Rows:              m.Rows,
				RowsAffected:      m.RowsAffected,
				ResolvedCommitHex: m.ResolvedCommitHex,
				ReadOnly:          m.ReadOnly,
				Error:             m.Error,
				GeneratedIDs:      m.GeneratedIDs,
				ErrorCode:         m.ErrorCode,
				RetryAfterMs:      m.RetryAfterMs,
			},
		}, nil
	case TxCommitMessage:
		return payloadEnvelope{
			Kind: "txCommit",
			TxCommit: &txCommitDto{
				Namespace:        m.Namespace,
				SessionID:        m.SessionID,
				TransactionBytes: m.TransactionBytes,
			},
		}, nil
	case TxRollbackMessage:
		return payloadEnvelope{
			Kind: "txRollback",
			TxRollback: &txRollbackDto{
				Namespace: m.Namespace,
				SessionID: m.SessionID,
			},
		}, nil
	default:
		if env, ok, err := encodeDocumentOpMessage(msg); ok || err != nil {
			return env, err
		}
		if env, ok, err := encodeLockOpMessage(msg); ok || err != nil {
			return env, err
		}
		return payloadEnvelope{}, fmt.Errorf("unsupported message type")
	}
}

func envelopeToMessage(header Header, env payloadEnvelope) (Message, error) {
	switch env.Kind {
	case "handshake":
		h := env.Handshake
		if h == nil {
			return nil, newDecodeError("missing handshake body")
		}
		encs := make([]PayloadEncoding, len(h.PreferredEncodings))
		for i, name := range h.PreferredEncodings {
			encs[i] = PayloadEncodingFromName(name)
		}
		return HandshakeMessage{
			H: header,
			Request: HandshakePayload{
				NodeID:     h.NodeID,
				Namespaces: h.Namespaces,
				LocalHeads: h.LocalHeads,
				Capabilities: CapabilitySet{
					SupportsZstd:              h.SupportsZstd,
					SupportsIndexHints:        h.SupportsIndexHints,
					SupportsDirectDeltaIngest: h.SupportsDirectDeltaIngest,
					MaxFrameBytes:             h.MaxFrameBytes,
				},
				PreferredEncodings: encs,
				ClientMode:         ClientModeFromName(h.ClientMode),
				ProtocolVersion:    h.ProtocolVersion,
				User:               h.User,
				Password:           h.Password,
				Token:              h.Token,
			},
		}, nil
	case "handshakeAck":
		a := env.HandshakeAck
		if a == nil {
			return nil, newDecodeError("missing handshakeAck body")
		}
		return HandshakeAckMessage{
			H: header,
			Response: HandshakeAckPayload{
				Accepted:           a.Accepted,
				NegotiatedEncoding: PayloadEncodingFromName(a.NegotiatedEncoding),
				ProtocolVersion:    a.ProtocolVersion,
				RemoteHeads:        a.RemoteHeads,
				RejectionReason:    a.RejectionReason,
			},
		}, nil
	case "deltaCommit":
		d := env.DeltaCommit
		if d == nil {
			return nil, newDecodeError("missing deltaCommit body")
		}
		commitHash, err := codec.HashFromHex(d.CommitHashHex)
		if err != nil {
			return nil, err
		}
		parentHash, err := codec.HashFromHex(d.ParentHashHex)
		if err != nil {
			return nil, err
		}
		ops := make([]document.Op, 0, len(d.Operations))
		for _, od := range d.Operations {
			op, err := opFromDto(od)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		}
		hints := make([]IndexHint, 0, len(d.IndexHints))
		for _, hd := range d.IndexHints {
			hint, err := hintFromDto(hd)
			if err != nil {
				return nil, err
			}
			hints = append(hints, hint)
		}
		return DeltaCommitMessage{
			H: header,
			Payload: DeltaCommitPayload{
				Namespace:        d.Namespace,
				CommitHash:       commitHash,
				ParentHash:       parentHash,
				TimestampMicros:  d.TimestampMicros,
				Operations:       ops,
				IndexHints:       hints,
				SchemaDeltaBytes: d.SchemaDeltaBytes,
			},
		}, nil
	case "commitFetch":
		c := env.CommitFetch
		if c == nil {
			return nil, newDecodeError("missing commitFetch body")
		}
		var since *codec.Hash
		if c.SinceHashHex != nil {
			h, err := codec.HashFromHex(*c.SinceHashHex)
			if err != nil {
				return nil, err
			}
			since = &h
		}
		return CommitFetchMessage{H: header, Namespace: c.Namespace, SinceHash: since, MaxCommits: c.MaxCommits}, nil
	case "commitPush":
		c := env.CommitPush
		if c == nil {
			return nil, newDecodeError("missing commitPush body")
		}
		commits, err := DecodeCommits(c.CommitsPayload)
		if err != nil {
			return nil, err
		}
		return CommitPushMessage{H: header, Namespace: c.Namespace, Commits: commits}, nil
	case "commitPushAck":
		a := env.CommitPushAck
		if a == nil {
			return nil, newDecodeError("missing commitPushAck body")
		}
		return CommitPushAckMessage{
			H: header, Namespace: a.Namespace, AppliedCommits: a.AppliedCommits, HeadHex: a.HeadHex,
		}, nil
	case "dagDiff":
		d := env.DagDiff
		if d == nil {
			return nil, newDecodeError("missing dagDiff body")
		}
		local, err := codec.HashFromHex(d.LocalHeadHex)
		if err != nil {
			return nil, err
		}
		remote, err := codec.HashFromHex(d.RemoteHeadHex)
		if err != nil {
			return nil, err
		}
		return DagDiffMessage{H: header, Namespace: d.Namespace, LocalHead: local, RemoteHead: remote}, nil
	case "transactionReplay":
		t := env.TransactionReplay
		if t == nil {
			return nil, newDecodeError("missing transactionReplay body")
		}
		base, err := codec.HashFromHex(t.BaseVersionHex)
		if err != nil {
			return nil, err
		}
		return TransactionReplayMessage{
			H: header, Namespace: t.Namespace, BaseVersion: base, TransactionBytes: t.TransactionBytes,
		}, nil
	case "conflictReport":
		c := env.ConflictReport
		if c == nil {
			return nil, newDecodeError("missing conflictReport body")
		}
		return ConflictReportMessage{H: header, Namespace: c.Namespace, ReportBytes: c.ReportBytes}, nil
	case "compactionNotice":
		c := env.CompactionNotice
		if c == nil {
			return nil, newDecodeError("missing compactionNotice body")
		}
		boundary, err := codec.HashFromHex(c.BoundaryHex)
		if err != nil {
			return nil, err
		}
		return CompactionNoticeMessage{
			H: header,
			Intent: CompactionIntent{
				NamespaceID: c.NamespaceID, Boundary: boundary, IssuedAtMillis: c.IssuedAtMillis,
			},
		}, nil
	case "iceArchiveNotice":
		i := env.IceArchiveNotice
		if i == nil {
			return nil, newDecodeError("missing iceArchiveNotice body")
		}
		orig, err := codec.HashFromHex(i.OriginalHashHex)
		if err != nil {
			return nil, err
		}
		bundle, err := codec.HashFromHex(i.BundleHashHex)
		if err != nil {
			return nil, err
		}
		return IceArchiveNoticeMessage{
			H: header, Namespace: i.Namespace, OriginalHash: orig,
			ArchiveLocation: i.ArchiveLocation, BundleHash: bundle,
		}, nil
	case "snapshotRequest":
		s := env.SnapshotRequest
		if s == nil {
			return nil, newDecodeError("missing snapshotRequest body")
		}
		var anchor *codec.Hash
		if s.AnchorHashHex != nil {
			h, err := codec.HashFromHex(*s.AnchorHashHex)
			if err != nil {
				return nil, err
			}
			anchor = &h
		}
		return SnapshotRequestMessage{H: header, Namespace: s.Namespace, AnchorHash: anchor}, nil
	case "snapshotResponse":
		s := env.SnapshotResponse
		if s == nil {
			return nil, newDecodeError("missing snapshotResponse body")
		}
		anchor, err := codec.HashFromHex(s.AnchorHashHex)
		if err != nil {
			return nil, err
		}
		return SnapshotResponseMessage{
			H: header, Namespace: s.Namespace, AnchorHash: anchor,
			SnapshotBytes: s.SnapshotBytes, Compressed: s.Compressed,
		}, nil
	case "positionAck":
		p := env.PositionAck
		if p == nil {
			return nil, newDecodeError("missing positionAck body")
		}
		hash, err := codec.HashFromHex(p.CommitHashHex)
		if err != nil {
			return nil, err
		}
		return PositionAckMessage{H: header, Namespace: p.Namespace, CommitHash: hash}, nil
	case "schemaPush":
		s := env.SchemaPush
		if s == nil {
			return nil, newDecodeError("missing schemaPush body")
		}
		return SchemaPushMessage{
			H: header, Namespace: s.Namespace, SchemaBytes: s.SchemaBytes, Revision: s.Revision,
		}, nil
	case "sessionBegin":
		s := env.SessionBegin
		if s == nil {
			return nil, newDecodeError("missing sessionBegin body")
		}
		return SessionBeginMessage{
			H: header, Namespace: s.Namespace, SessionID: s.SessionID,
			ReadConsistency: s.ReadConsistency, BaseVersionHex: s.BaseVersionHex,
		}, nil
	case "sessionBeginAck":
		s := env.SessionBeginAck
		if s == nil {
			return nil, newDecodeError("missing sessionBeginAck body")
		}
		return SessionBeginAckMessage{
			H: header, Namespace: s.Namespace, SessionID: s.SessionID,
			HeadHex: s.HeadHex, ReadConsistency: s.ReadConsistency, Error: s.Error,
		}, nil
	case "sqlExec":
		s := env.SqlExec
		if s == nil {
			return nil, newDecodeError("missing sqlExec body")
		}
		return SqlExecMessage{
			H: header, Namespace: s.Namespace, SessionID: s.SessionID,
			SQL: s.SQL, ParametersJSON: s.ParametersJSON,
		}, nil
	case "sqlResult":
		s := env.SqlResult
		if s == nil {
			return nil, newDecodeError("missing sqlResult body")
		}
		return SqlResultMessage{
			H: header, Namespace: s.Namespace, SessionID: s.SessionID,
			Columns: s.Columns, Rows: s.Rows, RowsAffected: s.RowsAffected,
			ResolvedCommitHex: s.ResolvedCommitHex, ReadOnly: s.ReadOnly,
			Error: s.Error, GeneratedIDs: s.GeneratedIDs,
			ErrorCode: s.ErrorCode, RetryAfterMs: s.RetryAfterMs,
		}, nil
	case "txCommit":
		c := env.TxCommit
		if c == nil {
			return nil, newDecodeError("missing txCommit body")
		}
		return TxCommitMessage{
			H: header, Namespace: c.Namespace, SessionID: c.SessionID, TransactionBytes: c.TransactionBytes,
		}, nil
	case "txRollback":
		r := env.TxRollback
		if r == nil {
			return nil, newDecodeError("missing txRollback body")
		}
		return TxRollbackMessage{H: header, Namespace: r.Namespace, SessionID: r.SessionID}, nil
	default:
		if msg, ok, err := decodeDocumentOpMessage(header, env); ok || err != nil {
			return msg, err
		}
		if msg, ok, err := decodeLockOpMessage(header, env); ok || err != nil {
			return msg, err
		}
		return nil, newDecodeError("unknown payload kind: " + env.Kind)
	}
}
