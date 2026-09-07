package policy_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/policy"
)

// TestParseJSONDocumentExpiry reads the documentExpiry block with its spec defaults: grace 0 and
// a 60 s sweep interval when the JSON leaves them out.
func TestParseJSONDocumentExpiry(t *testing.T) {
	p := policy.NewDefaultParser()
	got, err := p.ParseJSON(`{"namespaceId":"app/data","documentExpiry":{"fieldPath":"expiresAt"}}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.DocumentExpiry == nil || got.DocumentExpiry.FieldPath != "expiresAt" {
		t.Fatalf("expiry = %+v", got.DocumentExpiry)
	}
	if got.DocumentExpiry.GraceMillis != 0 || got.DocumentExpiry.SweepIntervalMillis != policy.DefaultSweepIntervalMillis {
		t.Fatalf("defaults not applied: %+v", *got.DocumentExpiry)
	}
	explicit, err := p.ParseJSON(`{"documentExpiry":{"fieldPath":"meta.ttl","graceMillis":5000,"sweepIntervalMillis":250}}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if e := explicit.DocumentExpiry; e.FieldPath != "meta.ttl" || e.GraceMillis != 5000 || e.SweepIntervalMillis != 250 {
		t.Fatalf("explicit values lost: %+v", *e)
	}
}

// TestParseJSONDocumentExpiryAbsentIsNil: no block, no expiry - and a block without a field path
// is a configuration error rather than an expiry that can never match.
func TestParseJSONDocumentExpiryAbsentIsNil(t *testing.T) {
	p := policy.NewDefaultParser()
	got, err := p.ParseJSON(`{"namespaceId":"app/data"}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.DocumentExpiry != nil {
		t.Fatalf("expected nil expiry, got %+v", got.DocumentExpiry)
	}
	if _, err := p.ParseJSON(`{"documentExpiry":{"graceMillis":1}}`, nil); err == nil {
		t.Fatal("expected an error for documentExpiry without fieldPath")
	}
}
