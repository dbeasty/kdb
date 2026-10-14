package wire_test

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/wire"
)

func attachmentSamples(t *testing.T) []wire.Message {
	doc := mustUUID(t, "6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	att := mustUUID(t, "0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
	info := wire.AttachmentInfo{ID: att, Name: "photo.png", Hash: repeatHex(7), Size: 1 << 33, Mime: "image/png"}
	return []wire.Message{
		wire.AttachmentUploadMessage{H: testHeader(1, wire.MsgAttachmentUpload), Namespace: "app/data", SessionID: "s1",
			UploadID: "u1", Offset: 1 << 20, Data: []byte{0, 1, 2, 255}, Final: true},
		wire.AttachmentUploadResultMessage{H: testHeader(1, wire.MsgAttachmentUploadResult), Namespace: "app/data",
			UploadID: "u1", Received: 1<<20 + 4, Hash: repeatHex(7), Size: 1<<20 + 4},
		wire.AttachmentUploadResultMessage{H: testHeader(1, wire.MsgAttachmentUploadResult), Namespace: "app/data",
			Error: "offset 5 does not follow 4", ErrorCode: wire.ErrorCode("CONFLICT")},
		wire.AttachmentGetMessage{H: testHeader(2, wire.MsgAttachmentGet), Namespace: "app/data", SessionID: "s1",
			DocID: doc, AttachmentID: att, Name: "photo.png", AsOfHex: repeatHex(3).Hex(), Offset: 1 << 20, MaxBytes: 4 << 20, ReadID: "r9", Close: true},
		wire.AttachmentDataMessage{H: testHeader(2, wire.MsgAttachmentData), Namespace: "app/data", ReadID: "r9", DocID: doc,
			Info: info, CommitHex: repeatHex(3).Hex(), Offset: 1 << 20, Data: []byte("bytes"), EOF: true, Superseded: true},
		wire.AttachmentListMessage{H: testHeader(3, wire.MsgAttachmentList), Namespace: "app/data", SessionID: "s1", DocID: doc, AsOfHex: ""},
		wire.AttachmentListResultMessage{H: testHeader(3, wire.MsgAttachmentListResult), Namespace: "app/data", CommitHex: repeatHex(3).Hex(),
			Items: []wire.AttachmentInfo{info, {ID: doc, Name: "b", Hash: repeatHex(8)}}},
		wire.BlobFetchMessage{H: testHeader(4, wire.MsgBlobFetch), Namespace: "app/data", MaxBytes: 8 << 20,
			Entries: []wire.BlobFetchEntry{{DocID: doc, AttachmentID: att, CommitHex: repeatHex(3).Hex(), Hash: repeatHex(7), Offset: 2 << 20, MaxBytes: 1 << 20}}},
		wire.BlobFetchResultMessage{H: testHeader(4, wire.MsgBlobFetchResult), Namespace: "app/data", More: true,
			Parts: []wire.BlobFetchPart{
				{Index: 0, Hash: repeatHex(7), Size: 3 << 20, Tree: repeatHex(6), Offset: 2 << 20, Data: []byte{9, 9},
					Chunks: []wire.BlobChunkProof{{Leaf: 2, Path: []codec.Hash{repeatHex(1), repeatHex(2)}}}, EOF: true},
				{Index: 1, Error: "not retained", ErrorCode: wire.ErrorCode("NOT_FOUND")},
			}},
		// A verified ranged read (WireFeatureAttachmentProofs): the get asks for proofs, the page
		// covers whole chunks and proves each - leaf 0 with an empty-field leaf, leaf 1 short.
		wire.AttachmentGetMessage{H: testHeader(5, wire.MsgAttachmentGet), Namespace: "app/data", DocID: doc, Name: "photo.png",
			Offset: 1<<20 + 7, MaxBytes: 100, Proofs: true},
		wire.AttachmentDataMessage{H: testHeader(5, wire.MsgAttachmentData), Namespace: "app/data", ReadID: "r10", DocID: doc,
			Info: wire.AttachmentInfo{ID: att, Name: "photo.png", Hash: repeatHex(7), Size: 1<<20 + 2}, CommitHex: repeatHex(3).Hex(),
			Data: []byte{4, 5, 6}, EOF: true, Tree: repeatHex(6), Scheme: 1,
			Chunks: []wire.BlobChunkProof{{Leaf: 0, Path: []codec.Hash{repeatHex(1)}}, {Leaf: 1, Path: []codec.Hash{repeatHex(2)}}}},
	}
}

func TestAttachmentMessagesRoundTrip(t *testing.T) {
	for _, msg := range attachmentSamples(t) {
		// Always binary, whatever the codec negotiated.
		for _, c := range []*wire.DefaultCodec{wire.NewCodec(wire.EncodingJSON), wire.NewCodec(wire.EncodingJSON).WithWireFeatures(wire.SupportedWireFeatures)} {
			frame, err := c.Encode(msg)
			if err != nil {
				t.Fatalf("%T: %v", msg, err)
			}
			if frame[wire.FrameHeaderSize] != 2 {
				t.Fatalf("%T: tag %d, want binary", msg, frame[wire.FrameHeaderSize])
			}
			back, err := c.Decode(frame)
			if err != nil {
				t.Fatalf("%T: decode: %v", msg, err)
			}
			if !reflect.DeepEqual(withoutLength(back), msg) {
				t.Fatalf("%T:\n got %+v\nwant %+v", msg, back, msg)
			}
		}
	}
}

// AttachmentGoldenFrames are the frames of attachmentSamples, in order: other implementations (TS,
// Kotlin) must decode each to the same message and encode it to the same bytes.
var attachmentGoldenFrames = []string{
	"2b0000004400010001000000020a086170702f64617461120273311a027531208080402a04000102ff3001",
	"450000004500010001000000020a086170702f6461746112027531188480402220070707070707070707070707070707070707070707070707070707070707070728848040",
	"3d0000004500010001000000020a086170702f64617461321a6f6666736574203520646f6573206e6f7420666f6c6c6f7720343a08434f4e464c494354",
	"9b0000004600010002000000020a086170702f64617461120273311a106f9619ff8b86d011b42d00cf4fc964ff22100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b2a0970686f746f2e706e673240303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333880804040808080024a0272395001",
	"d00000004700010002000000020a086170702f64617461120272391a106f9619ff8b86d011b42d00cf4fc964ff22500a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a2007070707070707070707070707070707070707070707070707070707070707072080808080202a09696d6167652f706e672a4030333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033308080403a05627974657340014801",
	"2d0000004800010003000000020a086170702f64617461120273311a106f9619ff8b86d011b42d00cf4fc964ff",
	"e40000004900010003000000020a086170702f646174611240303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330331a500a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a2007070707070707070707070707070707070707070707070707070707070707072080808080202a09696d6167652f706e671a370a106f9619ff8b86d011b42d00cf4fc964ff1201621a200808080808080808080808080808080808080808080808080808080808080808",
	"b00000004200010004000000020a086170702f646174611291010a106f9619ff8b86d011b42d00cf4fc964ff12100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b1a4030333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033222007070707070707070707070707070707070707070707070707070707070707072880808001308080401880808004",
	"d50000004300010004000000020a086170702f64617461129c0112200707070707070707070707070707070707070707070707070707070707070707188080c001222006060606060606060606060606060606060606060606060606060606060606062880808001320209093a46080212200101010101010101010101010101010101010101010101010101010101010101122002020202020202020202020202020202020202020202020202020202020202024001121b0801520c6e6f742072657461696e65645a094e4f545f464f554e441801",
	"3c0000004600010005000000020a086170702f646174611a106f9619ff8b86d011b42d00cf4fc964ff2a0970686f746f2e706e673887804040645801",
	"2a0100004700010005000000020a086170702f6461746112037231301a106f9619ff8b86d011b42d00cf4fc964ff22430a100b6f3c1e2a4d4e8f9a1b3c5d7e9f1a2b120970686f746f2e706e671a200707070707070707070707070707070707070707070707070707070707070707208280402a40303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333a030405064001622006060606060606060606060606060606060606060606060606060606060606066a22122001010101010101010101010101010101010101010101010101010101010101016a240801122002020202020202020202020202020202020202020202020202020202020202027001",
}

func TestAttachmentMessageGoldenFrames(t *testing.T) {
	samples := attachmentSamples(t)
	if len(samples) != len(attachmentGoldenFrames) {
		t.Fatalf("%d samples, %d golden frames", len(samples), len(attachmentGoldenFrames))
	}
	for i, msg := range samples {
		frame, err := wire.NewCodec(wire.EncodingJSON).Encode(msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(frame); got != attachmentGoldenFrames[i] {
			t.Errorf("%s: frame\n %s\nwant\n %s", msg.Header().MessageType, got, attachmentGoldenFrames[i])
		}
	}
}

func TestAttachmentWireNames(t *testing.T) {
	for code, name := range map[uint16]string{0x42: "BLOB_FETCH", 0x43: "BLOB_FETCH_RESULT", 0x44: "ATTACHMENT_UPLOAD",
		0x45: "ATTACHMENT_UPLOAD_RESULT", 0x46: "ATTACHMENT_GET", 0x47: "ATTACHMENT_DATA", 0x48: "ATTACHMENT_LIST", 0x49: "ATTACHMENT_LIST_RESULT"} {
		mt, ok := wire.MessageTypeFromCode(code)
		if !ok || mt.String() != name {
			t.Errorf("%#x: %v %q", code, ok, mt.String())
		}
	}
}

// withoutLength clears the decoded header's payload length, which an encoded message leaves zero.
func withoutLength(m wire.Message) wire.Message {
	v := reflect.ValueOf(&m).Elem().Elem()
	cp := reflect.New(v.Type()).Elem()
	cp.Set(v)
	h := cp.FieldByName("H")
	h.FieldByName("PayloadLength").SetInt(0)
	return cp.Interface().(wire.Message)
}
