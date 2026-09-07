package document_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TestResolveIDUUIDStringIsIdentity: a UUID-shaped "id" is the document's identity verbatim.
func TestResolveIDUUIDStringIsIdentity(t *testing.T) {
	id, supplied, err := document.ResolveID(`{"id":"12345678-1234-4123-8123-123456789012","n":1}`)
	if err != nil || !supplied {
		t.Fatalf("err=%v supplied=%v", err, supplied)
	}
	if id.String() != "12345678-1234-4123-8123-123456789012" {
		t.Fatalf("id = %s", id)
	}
}

// TestResolveIDNonUUIDStringIsDerived: any other non-empty string maps through DerivedUUID.
func TestResolveIDNonUUIDStringIsDerived(t *testing.T) {
	id, supplied, err := document.ResolveID(`{"n":1,"id":"order-1"}`)
	if err != nil || !supplied {
		t.Fatalf("err=%v supplied=%v", err, supplied)
	}
	if id != codec.DerivedUUID("order-1") {
		t.Fatalf("id = %s, want DerivedUUID(order-1) = %s", id, codec.DerivedUUID("order-1"))
	}
}

// TestResolveIDAbsentIsNotSupplied: no "id" means the caller mints one; not an error.
func TestResolveIDAbsentIsNotSupplied(t *testing.T) {
	_, supplied, err := document.ResolveID(`{"n":1}`)
	if err != nil || supplied {
		t.Fatalf("err=%v supplied=%v", err, supplied)
	}
}

// TestResolveIDRejectsNonStringAndEmpty pins §9.4's rejection cases and the non-object root.
func TestResolveIDRejectsNonStringAndEmpty(t *testing.T) {
	for _, body := range []string{`{"id":12345}`, `{"id":""}`, `{"id":null}`, `{"id":{"x":1}}`, `[1]`, `{`} {
		if _, _, err := document.ResolveID(body); err == nil {
			t.Errorf("%s: expected an error", body)
		}
	}
}
