package wire

import (
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// transactionDto matches kdb-wire's TransactionWireCodec.kt WireKdbTransactionDto field-for-
// field (same JSON key names) - this is the wire-compatibility contract Component 40 depends on:
// a transaction encoded by a Go client must decode identically on the JVM server and vice versa.
type transactionDto struct {
	ID              string  `json:"id"`
	BaseVersionHex  string  `json:"baseVersionHex"`
	TimestampMicros int64   `json:"timestampMicros"`
	AuthorNodeID    string  `json:"authorNodeId"`
	Operations      []opDto `json:"operations"`
	// Preconditions is additive and omitempty: a transaction with no preconditions encodes
	// byte-for-byte as it did before this field existed, so an older JVM peer decoding a new
	// Go-produced transaction sees exactly what it saw before, and a new decoder reading an old
	// producer's bytes gets a nil slice - the "assert nothing" default. That keeps the Component
	// 40 wire-compatibility contract intact in both directions without a version negotiation.
	Preconditions []preconditionDto `json:"preconditions,omitempty"`
}

// preconditionDto is one entry of transactionDto.Preconditions. Kind travels as its name rather
// than its ordinal, matching the convention ConflictOperationType already established for
// enums crossing this wire (kotlinx.serialization emits enum constant names).
type preconditionDto struct {
	OpIndex        int    `json:"opIndex"`
	Kind           string `json:"kind"`
	ContentHashHex string `json:"contentHashHex,omitempty"`
}

// EncodeTransaction serializes tx for TxCommitMessage.TransactionBytes, matching Kotlin's
// TransactionWireCodec.encode byte-for-byte in shape (field names/JSON encoding; not
// byte-identical since Go and Kotlin JSON encoders don't guarantee identical field ordering/
// whitespace, but semantically equivalent and mutually decodable).
func EncodeTransaction(tx document.Transaction) ([]byte, error) {
	ops := make([]opDto, len(tx.Operations))
	for i, op := range tx.Operations {
		ops[i] = opToDto(op)
	}
	var pres []preconditionDto
	for _, p := range tx.Preconditions {
		d := preconditionDto{OpIndex: p.OpIndex, Kind: p.Kind.String()}
		if p.Kind == document.ExpectContentHash {
			d.ContentHashHex = p.ContentHash.Hex()
		}
		pres = append(pres, d)
	}
	dto := transactionDto{
		ID:              tx.ID.String(),
		BaseVersionHex:  tx.BaseVersion.Hex(),
		TimestampMicros: tx.Timestamp.EpochMicros(),
		AuthorNodeID:    tx.AuthorNodeID.String(),
		Operations:      ops,
		Preconditions:   pres,
	}
	return json.Marshal(dto)
}

// DecodeTransaction is EncodeTransaction's inverse.
func DecodeTransaction(bytes []byte) (document.Transaction, error) {
	var dto transactionDto
	if err := json.Unmarshal(bytes, &dto); err != nil {
		return document.Transaction{}, err
	}
	id, err := codec.UUIDFromString(dto.ID)
	if err != nil {
		return document.Transaction{}, err
	}
	baseVersion, err := codec.HashFromHex(dto.BaseVersionHex)
	if err != nil {
		return document.Transaction{}, err
	}
	authorNodeID, err := codec.UUIDFromString(dto.AuthorNodeID)
	if err != nil {
		return document.Transaction{}, err
	}
	ops := make([]document.Op, len(dto.Operations))
	for i, opd := range dto.Operations {
		op, err := opFromDto(opd)
		if err != nil {
			return document.Transaction{}, err
		}
		ops[i] = op
	}
	var pres []document.Precondition
	for _, pd := range dto.Preconditions {
		kind, err := preconditionKindFromName(pd.Kind)
		if err != nil {
			return document.Transaction{}, err
		}
		p := document.Precondition{OpIndex: pd.OpIndex, Kind: kind}
		if kind == document.ExpectContentHash {
			h, err := codec.HashFromHex(pd.ContentHashHex)
			if err != nil {
				return document.Transaction{}, err
			}
			p.ContentHash = h
		}
		pres = append(pres, p)
	}
	return document.Transaction{
		ID:            id,
		BaseVersion:   baseVersion,
		Operations:    ops,
		Timestamp:     codec.TimestampFromEpochMicros(dto.TimestampMicros),
		AuthorNodeID:  authorNodeID,
		Preconditions: pres,
	}, nil
}

// preconditionKindFromName is the strict inverse of PreconditionKind.String. An unrecognized
// name is an error rather than a silent fall back to ExpectAny: a peer asserting a precondition
// this build does not understand must not have that assertion quietly dropped and its write
// committed anyway - that is precisely the lost update the precondition was preventing.
func preconditionKindFromName(name string) (document.PreconditionKind, error) {
	switch name {
	case "EXPECT_ANY":
		return document.ExpectAny, nil
	case "EXPECT_ABSENT":
		return document.ExpectAbsent, nil
	case "EXPECT_PRESENT":
		return document.ExpectPresent, nil
	case "EXPECT_CONTENT_HASH":
		return document.ExpectContentHash, nil
	default:
		return document.ExpectAny, fmt.Errorf("kdb wire: unrecognized precondition kind %q", name)
	}
}
