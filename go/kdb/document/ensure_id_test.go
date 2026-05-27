package document_test

import (
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func TestEnsureIDInJSON_injectsWhenMissing(t *testing.T) {
	id, err := codec.UUIDFromString("12345678-1234-4123-8123-123456789012")
	if err != nil {
		t.Fatal(err)
	}
	out, err := document.EnsureIDInJSON(`{"name":"Ada"}`, id)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"id":"12345678-1234-4123-8123-123456789012"`) {
		t.Fatalf("out %q", out)
	}
	if !strings.Contains(out, `"name":"Ada"`) {
		t.Fatalf("out %q", out)
	}
}

func TestEnsureIDInJSON_unchangedWhenPresent(t *testing.T) {
	id, _ := codec.UUIDFromString("12345678-1234-4123-8123-123456789012")
	orig := `{"id":"00000000-0000-0000-0000-000000000001","name":"Ada"}`
	out, err := document.EnsureIDInJSON(orig, id)
	if err != nil {
		t.Fatal(err)
	}
	if out != orig {
		t.Fatalf("got %q want %q", out, orig)
	}
}

func TestEnsureIDInJSON_rejectsNonObject(t *testing.T) {
	id, _ := codec.RandomUUID()
	_, err := document.EnsureIDInJSON(`[1]`, id)
	if err == nil {
		t.Fatal("expected error")
	}
}
