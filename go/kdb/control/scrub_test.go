package control

import (
	"net/http"
	"testing"
)

// A scrub of a healthy namespace answers 200 with what it checked; "repair": false only reports.
// Comparing with a peer needs replication peers.
func TestScrubAndPeerDiffEndpoints(t *testing.T) {
	cs, base := newFixture(t, func(o *Options) { o.AllowWrites = true })
	seed(t, cs, `{"id":"doc-a","v":1}`, `{"id":"doc-b","v":2}`)
	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/scrub", ``)
	if res.StatusCode != http.StatusOK || body["checked"].(float64) != 2 || body["damaged"] != nil {
		t.Fatalf("scrub: %d %v", res.StatusCode, body)
	}
	if res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/scrub", `{"repair":false}`); res.StatusCode != http.StatusOK {
		t.Fatalf("report-only scrub: %d %v", res.StatusCode, body)
	}
	if res, _ := get(t, base, "/v1/ns/demo%2Fusers/peers/nobody/diff"); res.StatusCode != http.StatusConflict {
		t.Fatalf("peer diff without peers: %d", res.StatusCode)
	}
	_, ro := newFixture(t)
	if res, _ := postJSON(t, ro, "/v1/ns/demo%2Fusers/scrub", ``); res.StatusCode != http.StatusForbidden {
		t.Fatalf("scrub can write a repair, so a read-only control plane refuses it: %d", res.StatusCode)
	}
}
