package wal

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
)

func kindOrdinal(kind RecordKind) int {
	switch kind {
	case RecordKindPutBlob:
		return 0
	case RecordKindDeleteBlob:
		return 1
	case RecordKindFlushCheckpoint:
		return 2
	default:
		return 3
	}
}

func kindFromOrdinal(o int) RecordKind {
	switch o {
	case 0:
		return RecordKindPutBlob
	case 1:
		return RecordKindDeleteBlob
	case 2:
		return RecordKindFlushCheckpoint
	default:
		return RecordKindMarker
	}
}

// EncodeRecord serializes a WAL record with header and trailing CRC.
func EncodeRecord(record Record) []byte {
	payload := record.Payload
	bodyLen := 8 + 1 + 4 + len(payload)
	total := 4 + 4 + bodyLen + 4
	arr := make([]byte, total)
	writeInt(arr, 0, Magic)
	writeInt(arr, 4, bodyLen)
	writeLong(arr, 8, record.Sequence)
	arr[16] = byte(kindOrdinal(record.Kind))
	writeInt(arr, 17, int(compression.CRC32All(payload)))
	copy(arr[21:], payload)
	writeInt(arr, total-4, int(compression.CRC32(arr, 0, total-4)))
	return arr
}

// DecodeRecords parses WAL frames from a segment byte slice.
func DecodeRecords(bytes []byte, partitionKey, segmentName string, skipCorrupt bool) ([]Record, error) {
	var out []Record
	offset := 0
	for offset+12 <= len(bytes) {
		magic := readInt(bytes, offset)
		if magic == BatchMagic {
			break
		}
		if magic != Magic {
			if skipCorrupt {
				break
			}
			return nil, &CorruptionError{
				Message: "bad magic", PartitionKey: partitionKey, SegmentName: segmentName, Offset: int64(offset),
			}
		}
		recordLen := readInt(bytes, offset+4)
		recordEnd := offset + 12 + recordLen
		if recordEnd > len(bytes) {
			break
		}
		headerCrc := readInt(bytes, recordEnd-4)
		if headerCrc != int(compression.CRC32(bytes, offset, recordEnd-offset-4)) {
			if skipCorrupt {
				offset = recordEnd
				continue
			}
			return nil, &CorruptionError{
				Message: "header crc", PartitionKey: partitionKey, SegmentName: segmentName, Offset: int64(offset),
			}
		}
		seq := readLong(bytes, offset+8)
		kind := kindFromOrdinal(int(bytes[offset+16]))
		payloadCrc := readInt(bytes, offset+17)
		payloadOff := offset + 21
		payloadLen := recordLen - 13
		payload := append([]byte(nil), bytes[payloadOff:payloadOff+payloadLen]...)
		if int(compression.CRC32All(payload)) != payloadCrc {
			if skipCorrupt {
				offset = recordEnd
				continue
			}
			return nil, &CorruptionError{
				Message: "payload crc", PartitionKey: partitionKey, SegmentName: segmentName, Offset: int64(offset),
			}
		}
		out = append(out, Record{
			Sequence:  seq,
			Timestamp: codec.TimestampNow(),
			Kind:      kind,
			Payload:   payload,
		})
		offset = recordEnd
	}
	return out, nil
}

func writeInt(arr []byte, off, v int) {
	arr[off] = byte(v >> 24)
	arr[off+1] = byte(v >> 16)
	arr[off+2] = byte(v >> 8)
	arr[off+3] = byte(v)
}

func writeLong(arr []byte, off int, v int64) {
	for i := 0; i < 8; i++ {
		arr[off+i] = byte(v >> (56 - i*8))
	}
}

func readInt(b []byte, off int) int {
	return (int(b[off])&0xFF)<<24 |
		(int(b[off+1])&0xFF)<<16 |
		(int(b[off+2])&0xFF)<<8 |
		int(b[off+3])&0xFF
}

func readLong(b []byte, off int) int64 {
	var v int64
	for i := 0; i < 8; i++ {
		v = (v << 8) | int64(b[off+i]&0xFF)
	}
	return v
}
