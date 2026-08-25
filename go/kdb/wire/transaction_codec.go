package wire

import (
	"encoding/json"

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
	dto := transactionDto{
		ID:              tx.ID.String(),
		BaseVersionHex:  tx.BaseVersion.Hex(),
		TimestampMicros: tx.Timestamp.EpochMicros(),
		AuthorNodeID:    tx.AuthorNodeID.String(),
		Operations:      ops,
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
	return document.Transaction{
		ID:           id,
		BaseVersion:  baseVersion,
		Operations:   ops,
		Timestamp:    codec.TimestampFromEpochMicros(dto.TimestampMicros),
		AuthorNodeID: authorNodeID,
	}, nil
}
