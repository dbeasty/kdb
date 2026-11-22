package control

import (
	"net/http"
	"testing"
)

// A skip past maxSkip used to be clamped to maxSkip: the caller got the page at 20000 whatever it
// asked for, with nextSkip pointing past the cap again - so a client following nextSkip was served
// the same page forever. Refusing it is the only answer that is not a wrong page.
func TestSkipPastTheBoundIsRefusedNotClamped(t *testing.T) {
	cs, base := newFixture(t)
	put := seed(t, cs, `{"name":"ada"}`)[0]
	for _, path := range []string{
		"/v1/ns/demo%2Fusers/log?skip=20001",
		"/v1/ns/demo%2Fusers/docs/" + put.DocID.String() + "/history?skip=20001",
	} {
		res, body := get(t, base, path)
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: got %d (%v), want 400", path, res.StatusCode, body)
		}
	}
	if res, body := get(t, base, "/v1/ns/demo%2Fusers/log?skip=20000"); res.StatusCode != http.StatusOK {
		t.Errorf("skip at the bound must still be served, got %d (%v)", res.StatusCode, body)
	}
}
