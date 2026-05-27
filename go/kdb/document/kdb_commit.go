package document

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// Commit is an immutable content-addressed DAG node.
type Commit struct {
	Hash             codec.Hash
	ParentHashes     []codec.Hash
	NamespaceID      string
	TransactionID    codec.UUID
	Timestamp        codec.Timestamp
	AuthorNodeID     codec.UUID
	Operations       []Op
	DocumentTreeHash codec.Hash
	SchemaHash       *codec.Hash
	Message          string
}

func commitPayloadRecord(
	parentHashes []codec.Hash,
	namespaceID string,
	transactionID codec.UUID,
	timestamp codec.Timestamp,
	authorNodeID codec.UUID,
	operations []Op,
	documentTreeHash codec.Hash,
	schemaHash *codec.Hash,
	message string,
) codec.Value {
	parentEls := make([]codec.Value, len(parentHashes))
	for i, h := range parentHashes {
		parentEls[i] = hashVal(h)
	}
	opEls := make([]codec.Value, len(operations))
	for i, op := range operations {
		opEls[i] = op.toValue()
	}
	fields := map[int]codec.Value{
		1: codec.ArrayValue{Elements: parentEls},
		2: codec.StringValue{V: namespaceID},
		3: uuidVal(transactionID),
		4: timestampVal(timestamp),
		5: uuidVal(authorNodeID),
		6: codec.ArrayValue{Elements: opEls},
		7: hashVal(documentTreeHash),
		9: codec.StringValue{V: message},
	}
	// Match Kotlin wire: omit field 8 when null so commit hashes match JVM genesis/delta.
	if schemaHash != nil {
		fields[8] = hashVal(*schemaHash)
	}
	return codec.RecordValue{Fields: fields}
}

func (c *Commit) ToCommitPayloadValue() codec.Value {
	return commitPayloadRecord(
		c.ParentHashes, c.NamespaceID, c.TransactionID, c.Timestamp, c.AuthorNodeID,
		c.Operations, c.DocumentTreeHash, c.SchemaHash, c.Message,
	)
}

func (c *Commit) ToPayloadBytes() ([]byte, error) {
	reg := WireRegistry()
	return codec.EncodeBytes(c.ToCommitPayloadValue(), CommitPayloadType, reg)
}

// BuildCommit constructs a commit whose Hash is SHA-256 of canonical payload bytes.
func BuildCommit(
	parentHashes []codec.Hash,
	namespaceID string,
	transactionID codec.UUID,
	timestamp codec.Timestamp,
	authorNodeID codec.UUID,
	operations []Op,
	documentTreeHash codec.Hash,
	schemaHash *codec.Hash,
	message string,
) (Commit, error) {
	reg := WireRegistry()
	payload := commitPayloadRecord(
		parentHashes, namespaceID, transactionID, timestamp, authorNodeID,
		operations, documentTreeHash, schemaHash, message,
	)
	bytes, err := codec.EncodeBytes(payload, CommitPayloadType, reg)
	if err != nil {
		return Commit{}, err
	}
	h, err := codec.HashFromBytes(SHA256Digest(bytes))
	if err != nil {
		return Commit{}, err
	}
	return Commit{
		Hash: h, ParentHashes: parentHashes, NamespaceID: namespaceID,
		TransactionID: transactionID, Timestamp: timestamp, AuthorNodeID: authorNodeID,
		Operations: operations, DocumentTreeHash: documentTreeHash, SchemaHash: schemaHash,
		Message: message,
	}, nil
}

// FromPayloadBytes decodes a commit from canonical payload bytes.
func FromPayloadBytes(bytes []byte) (Commit, error) {
	reg := WireRegistry()
	v, err := codec.DecodeBytes(bytes, CommitPayloadType, reg)
	if err != nil {
		return Commit{}, NewCommitDecodeError("commit payload decode failed", err)
	}
	rec, ok := v.(codec.RecordValue)
	if !ok {
		return Commit{}, NewCommitDecodeError("commit payload: expected record", nil)
	}
	h, err := codec.HashFromBytes(SHA256Digest(bytes))
	if err != nil {
		return Commit{}, err
	}
	return parseCommitFromRecord(rec, h)
}

func parseCommitFromRecord(rec codec.RecordValue, hash codec.Hash) (Commit, error) {
	parentsArr, ok := rec.Fields[1].(codec.ArrayValue)
	if !ok {
		return Commit{}, NewCommitDecodeError("parentHashes", nil)
	}
	parents := make([]codec.Hash, len(parentsArr.Elements))
	for i, el := range parentsArr.Elements {
		h, err := hashFromVal(el)
		if err != nil {
			return Commit{}, NewCommitDecodeError("parent hash", nil)
		}
		parents[i] = h
	}
	ns, ok := rec.Fields[2].(codec.StringValue)
	if !ok {
		return Commit{}, NewCommitDecodeError("namespaceId", nil)
	}
	txID, err := uuidFromVal(rec.Fields[3])
	if err != nil {
		return Commit{}, NewCommitDecodeError("transactionId", nil)
	}
	ts, err := timestampFromVal(rec.Fields[4])
	if err != nil {
		return Commit{}, NewCommitDecodeError("timestamp", nil)
	}
	author, err := uuidFromVal(rec.Fields[5])
	if err != nil {
		return Commit{}, NewCommitDecodeError("authorNodeId", nil)
	}
	opsArr, ok := rec.Fields[6].(codec.ArrayValue)
	if !ok {
		return Commit{}, NewCommitDecodeError("operations", nil)
	}
	ops := make([]Op, len(opsArr.Elements))
	for i, el := range opsArr.Elements {
		op, err := OpFromValue(el)
		if err != nil {
			return Commit{}, err
		}
		ops[i] = op
	}
	docTreeH, err := hashFromVal(rec.Fields[7])
	if err != nil {
		return Commit{}, NewCommitDecodeError("documentTreeHash", nil)
	}
	var schemaH *codec.Hash
	// Kotlin omits nullable fields at default (null); map lookup yields nil, not NullValue.
	switch sf := rec.Fields[8].(type) {
	case nil:
		schemaH = nil
	case codec.NullValue:
		schemaH = nil
	case codec.FixedValue:
		h, err := codec.HashFromBytes(sf.V)
		if err != nil {
			return Commit{}, NewCommitDecodeError("schemaHash", nil)
		}
		schemaH = &h
	default:
		return Commit{}, NewCommitDecodeError("schemaHash", nil)
	}
	msg, ok := rec.Fields[9].(codec.StringValue)
	if !ok {
		return Commit{}, NewCommitDecodeError("message", nil)
	}
	return Commit{
		Hash: hash, ParentHashes: parents, NamespaceID: ns.V,
		TransactionID: txID, Timestamp: ts, AuthorNodeID: author,
		Operations: ops, DocumentTreeHash: docTreeH, SchemaHash: schemaH,
		Message: msg.V,
	}, nil
}

// ComputeCommitHash returns SHA-256 of canonical CommitPayload bytes.
func ComputeCommitHash(c Commit) (codec.Hash, error) {
	b, err := c.ToPayloadBytes()
	if err != nil {
		return codec.Hash{}, err
	}
	return codec.HashFromBytes(SHA256Digest(b))
}
