package attachstore

import (
	"encoding/binary"
	"errors"
)

// Binary deltas, git-pack style: a target is a sequence of instructions against a base -
// copy a run of the base, or insert literal bytes. A version of an attachment is stored as a delta
// against the version that replaced it (deltaFor), so the newest bytes are always whole and an
// older version costs what changed.
//
// Encoding: uvarint target length, then instructions until the end:
//
//	0x00 uvarint n, then n literal bytes
//	0x01 uvarint offset, uvarint n: copy base[offset : offset+n]
//
// Matching indexes the base at every deltaBlock-aligned offset by a hash of the deltaBlock bytes
// there, then rolls the same hash over every offset of the target; a hit is verified, extended
// forwards as far as the bytes agree and backwards into the pending literal, and emitted as a copy.

const (
	deltaBlock  = 32
	opLiteral   = 0x00
	opCopy      = 0x01
	deltaPrime  = 0x100000001b3
	minCopySpan = deltaBlock
)

var errDeltaCorrupt = errors.New("attachstore: corrupt delta")

// encodeDelta returns target as a delta against base.
func encodeDelta(base, target []byte) []byte {
	out := binary.AppendUvarint(nil, uint64(len(target)))
	if len(base) < deltaBlock || len(target) < deltaBlock {
		return appendLiteral(out, target)
	}
	index := make(map[uint64]int, len(base)/deltaBlock)
	for off := 0; off+deltaBlock <= len(base); off += deltaBlock {
		h := blockHash(base[off : off+deltaBlock])
		if _, ok := index[h]; !ok {
			index[h] = off
		}
	}
	// pow is deltaPrime^(deltaBlock-1), to roll the oldest byte out.
	pow := uint64(1)
	for i := 0; i < deltaBlock-1; i++ {
		pow *= deltaPrime
	}
	litStart := 0
	i := 0
	h := blockHash(target[0:deltaBlock])
	for i+deltaBlock <= len(target) {
		if bo, ok := index[h]; ok && string(base[bo:bo+deltaBlock]) == string(target[i:i+deltaBlock]) {
			// Extend backwards into the pending literal, and forwards past the block.
			start, bstart := i, bo
			for start > litStart && bstart > 0 && base[bstart-1] == target[start-1] {
				start--
				bstart--
			}
			end, bend := i+deltaBlock, bo+deltaBlock
			for end < len(target) && bend < len(base) && base[bend] == target[end] {
				end++
				bend++
			}
			if end-start >= minCopySpan {
				out = appendLiteral(out, target[litStart:start])
				out = append(out, opCopy)
				out = binary.AppendUvarint(out, uint64(bstart))
				out = binary.AppendUvarint(out, uint64(end-start))
				litStart, i = end, end
				if i+deltaBlock <= len(target) {
					h = blockHash(target[i : i+deltaBlock])
				}
				continue
			}
		}
		if i+deltaBlock >= len(target) {
			break
		}
		h = (h-uint64(target[i])*pow)*deltaPrime + uint64(target[i+deltaBlock])
		i++
	}
	return appendLiteral(out, target[litStart:])
}

func blockHash(b []byte) uint64 {
	var h uint64
	for _, c := range b {
		h = h*deltaPrime + uint64(c)
	}
	return h
}

func appendLiteral(out, lit []byte) []byte {
	if len(lit) == 0 {
		return out
	}
	out = append(out, opLiteral)
	out = binary.AppendUvarint(out, uint64(len(lit)))
	return append(out, lit...)
}

// applyDelta rebuilds the target from base and a delta, refusing a delta whose instructions run
// outside base or past the length it declares.
func applyDelta(base, delta []byte, maxBytes int64) ([]byte, error) {
	n, k := binary.Uvarint(delta)
	if k <= 0 || int64(n) > maxBytes {
		return nil, errDeltaCorrupt
	}
	out := make([]byte, 0, n)
	off := k
	for off < len(delta) {
		op := delta[off]
		off++
		switch op {
		case opLiteral:
			l, k := binary.Uvarint(delta[off:])
			if k <= 0 || l > uint64(len(delta)-off-k) || uint64(len(out))+l > n {
				return nil, errDeltaCorrupt
			}
			off += k
			out = append(out, delta[off:off+int(l)]...)
			off += int(l)
		case opCopy:
			bo, k1 := binary.Uvarint(delta[off:])
			if k1 <= 0 {
				return nil, errDeltaCorrupt
			}
			l, k2 := binary.Uvarint(delta[off+k1:])
			if k2 <= 0 || bo > uint64(len(base)) || l > uint64(len(base))-bo || uint64(len(out))+l > n {
				return nil, errDeltaCorrupt
			}
			off += k1 + k2
			out = append(out, base[bo:bo+l]...)
		default:
			return nil, errDeltaCorrupt
		}
	}
	if uint64(len(out)) != n {
		return nil, errDeltaCorrupt
	}
	return out, nil
}
