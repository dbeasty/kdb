package wire

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// EncodeCommits serializes commits for CommitPush payloads.
func EncodeCommits(commits []document.Commit) ([]byte, error) {
	return appendCommits(nil, commits, nil)
}

// EncodeCommitsWithPayloads is EncodeCommits taking known[i], when set, as commits[i]'s encoding
// instead of encoding it again. The caller vouches that known[i] is commits[i].ToPayloadBytes() -
// a payload whose SHA-256 is the commit's hash; the receiver checks that hash as for any commit.
func EncodeCommitsWithPayloads(commits []document.Commit, known [][]byte) ([]byte, error) {
	return appendCommits(nil, commits, known)
}

// appendCommits appends EncodeCommits's bytes to dst, taking known[i], when set, as commits[i]'s
// encoding. Sized first, then each commit's payload is written straight into the one buffer: a
// page used to cost an allocation per commit for bytes that were copied once and dropped.
func appendCommits(dst []byte, commits []document.Commit, known [][]byte) ([]byte, error) {
	total := 4
	for i := range commits {
		n := -1
		if i < len(known) && known[i] != nil {
			n = len(known[i])
		} else {
			n = commits[i].PayloadSize()
		}
		if n < 0 {
			each, err := encodeCommitsEach(commits, known)
			return append(dst, each...), err
		}
		total += 4 + n
	}
	out := slices.Grow(dst, total)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(commits)))
	for i := range commits {
		o := len(out)
		out = append(out, 0, 0, 0, 0)
		if i < len(known) && known[i] != nil {
			out = append(out, known[i]...)
		} else {
			var err error
			if out, err = commits[i].AppendPayloadBytes(out); err != nil {
				return dst, err
			}
		}
		binary.LittleEndian.PutUint32(out[o:], uint32(len(out)-o-4))
	}
	return out, nil
}

// encodeCommitsEach is appendCommits for a page with a commit PayloadSize cannot size.
func encodeCommitsEach(commits []document.Commit, known [][]byte) ([]byte, error) {
	payloads := make([][]byte, len(commits))
	total := 4
	for i, c := range commits {
		var b []byte
		if i < len(known) && known[i] != nil {
			b = known[i]
		} else {
			var err error
			if b, err = c.ToPayloadBytes(); err != nil {
				return nil, err
			}
		}
		payloads[i] = b
		total += 4 + len(b)
	}
	out := make([]byte, total)
	o := 0
	binary.LittleEndian.PutUint32(out[o:], uint32(len(commits)))
	o += 4
	for _, p := range payloads {
		binary.LittleEndian.PutUint32(out[o:], uint32(len(p)))
		o += 4
		copy(out[o:], p)
		o += len(p)
	}
	return out, nil
}

// DecodeCommits deserializes commits from CommitPush payload bytes. The commits share no memory
// with bytes, which the caller may reuse once it returns (decodeCommitsPayload pools it).
func DecodeCommits(bytes []byte) ([]document.Commit, error) {
	if len(bytes) < 4 {
		return nil, nil
	}
	o := 0
	count := int(binary.LittleEndian.Uint32(bytes[o:]))
	o += 4
	// The count comes from a peer, so it cannot be used to size the allocation on its own: every
	// commit costs at least its own 4-byte length prefix, so a payload of len(bytes) can back at
	// most (len(bytes)-4)/4 of them. Without this bound a four-byte CommitPush declaring
	// 0xFFFFFFFF commits made this reserve ~800 GiB before discovering, one iteration later,
	// that there were no commit bodies at all - fatal under any memory limit (which is to say,
	// under the container and systemd deployments), and reachable by any peer that can send a
	// commitPush frame.
	if maxPossible := (len(bytes) - 4) / 4; count > maxPossible {
		return nil, fmt.Errorf("commit push declares %d commits, payload can hold at most %d", count, maxPossible)
	}
	result := make([]document.Commit, 0, count)
	for i := 0; i < count; i++ {
		if o+4 > len(bytes) {
			return nil, fmt.Errorf("truncated commit push payload")
		}
		ln := int(binary.LittleEndian.Uint32(bytes[o:]))
		o += 4
		if o+ln > len(bytes) {
			return nil, fmt.Errorf("truncated commit bytes")
		}
		c, err := document.FromPayloadBytes(bytes[o : o+ln])
		if err != nil {
			return nil, err
		}
		result = append(result, c)
		o += ln
	}
	return result, nil
}

func hashesToHex(hs []codec.Hash) []string {
	if len(hs) == 0 {
		return nil
	}
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.Hex()
	}
	return out
}

func hashesFromHex(hexes []string) ([]codec.Hash, error) {
	if len(hexes) == 0 {
		return nil, nil
	}
	out := make([]codec.Hash, len(hexes))
	for i, s := range hexes {
		h, err := codec.HashFromHex(s)
		if err != nil {
			return nil, err
		}
		out[i] = h
	}
	return out, nil
}

func stubsToDto(stubs []document.CommitStub) []commitStubDto {
	if len(stubs) == 0 {
		return nil
	}
	out := make([]commitStubDto, len(stubs))
	for i, s := range stubs {
		out[i] = commitStubDto{
			OriginalHashHex: s.OriginalHash.Hex(),
			ArchiveLocation: s.ArchiveLocation,
			StubbedAtMicros: s.StubbedAt.EpochMicros(),
		}
	}
	return out
}

func stubsFromDto(dtos []commitStubDto) ([]document.CommitStub, error) {
	if len(dtos) == 0 {
		return nil, nil
	}
	out := make([]document.CommitStub, len(dtos))
	for i, d := range dtos {
		h, err := codec.HashFromHex(d.OriginalHashHex)
		if err != nil {
			return nil, err
		}
		out[i] = document.CommitStub{
			OriginalHash:    h,
			ArchiveLocation: d.ArchiveLocation,
			StubbedAt:       codec.TimestampFromEpochMicros(d.StubbedAtMicros),
		}
	}
	return out, nil
}
