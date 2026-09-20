package control

import (
	"net/http"
	"strings"
	"testing"
)

// A ref that the namespace will not keep is refused, not created.
//
// The CLI has always asked AssertRetainsHistory before making a tag or a
// branch; this package never did. So the same DAG call meant two different
// things depending on which door you came in by, and the door with a UI on it
// was the permissive one. Under history=none nothing consults a ref before
// reclaiming, so a tag made here became a name for a commit whose documents
// can stop being producible — silently, and only discovered when somebody
// tried to use it.

func TestCreatingATagIsRefusedWhenHistoryIsNotKept(t *testing.T) {
	_, base := retentionFixture(t)

	if res, body := sendJSON(t, http.MethodPut, base,
		"/v1/ns/demo%2Fusers/retention/history-mode", `{"mode":"none"}`); res.StatusCode != http.StatusOK {
		t.Fatalf("switching to history=none: %d %v", res.StatusCode, body)
	}

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags", `{"name":"before-import"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — a tag that cannot be kept was created anyway (%v)",
			res.StatusCode, body)
	}
	err, _ := body["error"].(map[string]any)
	if err["code"] != "history_not_retained" {
		t.Errorf("code = %v, want history_not_retained", err["code"])
	}
	// The remedy travels with the refusal, because an operator reading this
	// has no other way to learn what to do about it.
	if msg, _ := err["message"].(string); !strings.Contains(msg, "migrate-history") {
		t.Errorf("the refusal does not say how to fix it: %q", msg)
	}
}

func TestCreatingABranchIsRefusedWhenHistoryIsNotKept(t *testing.T) {
	_, base := retentionFixture(t)

	if res, _ := sendJSON(t, http.MethodPut, base,
		"/v1/ns/demo%2Fusers/retention/history-mode", `{"mode":"none"}`); res.StatusCode != http.StatusOK {
		t.Fatalf("switching to history=none: %d", res.StatusCode)
	}

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/branches", `{"name":"wip"}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (%v)", res.StatusCode, body)
	}
}

// And the ordinary case still works: a namespace that keeps its history makes
// tags exactly as it did before.
func TestCreatingATagStillWorksWhenHistoryIsKept(t *testing.T) {
	_, base := retentionFixture(t)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags", `{"name":"before-import"}`)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%v)", res.StatusCode, body)
	}
	// The name is not on disk until a checkpoint, and the response says so
	// rather than leaving a client to read the prose note for it.
	if body["checkpointPending"] != true {
		t.Errorf("checkpointPending = %v, want true", body["checkpointPending"])
	}
}

// durable: true buys a checkpoint, and reports what it cost.
func TestADurableTagIsCheckpointedBeforeAnswering(t *testing.T) {
	_, base := retentionFixture(t)

	res, body := postJSON(t, base, "/v1/ns/demo%2Fusers/refs/tags",
		`{"name":"nightly","durable":true}`)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%v)", res.StatusCode, body)
	}
	if body["checkpointPending"] != false {
		t.Errorf("checkpointPending = %v, want false after a durable create", body["checkpointPending"])
	}
	d, ok := body["durability"].(map[string]any)
	if !ok || d["checkpointWritten"] != true {
		t.Errorf("durability = %v, want a written checkpoint", body["durability"])
	}
}
