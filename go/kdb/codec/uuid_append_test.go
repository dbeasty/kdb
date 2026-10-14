package codec

import (
	"bytes"
	"testing"
)

func TestUUIDAppendBytesMatchesBytes(t *testing.T) {
	for i := 0; i < 100; i++ {
		u, err := RandomUUID()
		if err != nil {
			t.Fatal(err)
		}
		if got := u.AppendBytes([]byte{9}); !bytes.Equal(got[1:], u.Bytes()) || got[0] != 9 {
			t.Fatalf("AppendBytes %x, Bytes %x", got, u.Bytes())
		}
	}
}
