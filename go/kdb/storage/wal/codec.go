package wal

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
)

// headerLen is the per-record header this format writes between the frame length and the
// payload: sequence(8) + epochMicros(8) + kind(1) + payloadCrc(4). It must stay identical to
// Kotlin's WalCodec.HEADER_LEN - see Magic's comment in types.go for the v1 format it replaced.
const headerLen = 21

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
	bodyLen := headerLen + len(payload)
	total := 4 + 4 + bodyLen + 4
	arr := make([]byte, total)
	writeInt(arr, 0, Magic)
	writeInt(arr, 4, bodyLen)
	writeLong(arr, 8, record.Sequence)
	writeLong(arr, 16, record.Timestamp.EpochMicros())
	arr[24] = byte(kindOrdinal(record.Kind))
	writeInt(arr, 25, int(compression.CRC32All(payload)))
	copy(arr[29:], payload)
	writeInt(arr, total-4, int(compression.CRC32(arr, 0, total-4)))
	return arr
}

// DecodeResult is what DecodeRecords recovered from a segment: the records that decoded cleanly,
// plus how many frames were skipped as corrupt. The skip count is not cosmetic - it is what
// RecoverySummary.RecordsSkippedCorrupt reports, and it used to be unobtainable here (the
// function returned only records), so that field was silently always zero however damaged the
// segment was.
type DecodeResult struct {
	Records        []Record
	SkippedCorrupt int64
}

// DecodeRecords parses WAL frames from a segment byte slice.
func DecodeRecords(bytes []byte, partitionKey, segmentName string, skipCorrupt bool) (DecodeResult, error) {
	var out []Record
	var skipped int64
	offset := 0
	for offset+12 <= len(bytes) {
		magic := readInt(bytes, offset)
		if magic == BatchMagic {
			break
		}
		if magic != Magic {
			if skipCorrupt {
				// Resync: this record's length is unknown (its header is exactly what is
				// corrupt), so scan forward byte-by-byte for the next plausible frame start
				// rather than abandoning the rest of the segment - which is what breaking out
				// here used to do, throwing away every intact record after the damaged one.
				scan := offset + 1
				for scan+4 <= len(bytes) {
					candidate := readInt(bytes, scan)
					if candidate == Magic || candidate == BatchMagic {
						break
					}
					scan++
				}
				skipped++
				offset = scan
				continue
			}
			return DecodeResult{Records: out}, &CorruptionError{
				Message: "bad magic", PartitionKey: partitionKey, SegmentName: segmentName, Offset: int64(offset),
			}
		}
		recordLen := readInt(bytes, offset+4)
		recordEnd := offset + 12 + recordLen
		// recordLen < headerLen is not merely "no payload" - it means payloadLen below would go
		// negative and slice out of range. Reached from any truncated or hostile segment; the
		// bound was missing here while Kotlin had it.
		if recordEnd > len(bytes) || recordLen < headerLen {
			break
		}
		headerCrc := readInt(bytes, recordEnd-4)
		if headerCrc != int(compression.CRC32(bytes, offset, recordEnd-offset-4)) {
			if skipCorrupt {
				skipped++
				offset = recordEnd
				continue
			}
			return DecodeResult{Records: out}, &CorruptionError{
				Message: "header crc", PartitionKey: partitionKey, SegmentName: segmentName, Offset: int64(offset),
			}
		}
		seq := readLong(bytes, offset+8)
		epochMicros := readLong(bytes, offset+16)
		kind := kindFromOrdinal(int(bytes[offset+24]))
		payloadCrc := readInt(bytes, offset+25)
		payloadOff := offset + 29
		payloadLen := recordLen - headerLen
		payload := append([]byte(nil), bytes[payloadOff:payloadOff+payloadLen]...)
		if int(compression.CRC32All(payload)) != payloadCrc {
			if skipCorrupt {
				skipped++
				offset = recordEnd
				continue
			}
			return DecodeResult{Records: out}, &CorruptionError{
				Message: "payload crc", PartitionKey: partitionKey, SegmentName: segmentName, Offset: int64(offset),
			}
		}
		out = append(out, Record{
			Sequence: seq,
			// The record's own timestamp, as written. This used to be codec.TimestampNow():
			// the v1 frame had nowhere to put a timestamp, so replay fabricated one for every
			// record and the original commit time was lost on every recovery.
			Timestamp: codec.TimestampFromEpochMicros(epochMicros),
			Kind:      kind,
			Payload:   payload,
		})
		offset = recordEnd
	}
	return DecodeResult{Records: out, SkippedCorrupt: skipped}, nil
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
