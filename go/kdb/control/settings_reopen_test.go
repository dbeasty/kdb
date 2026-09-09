package control

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/embed"
)

// fakeReopener records what a reopen was asked to change, so a test can assert the edit reached
// the storage options rather than only that the request was accepted.
type fakeReopener struct {
	mu        sync.Mutex
	namespace string
	applied   embed.StorageOptions
	calls     int
	fail      error
}

func (f *fakeReopener) ReopenableNamespaces() []string {
	if f.namespace == "" {
		return nil
	}
	return []string{f.namespace}
}

func (f *fakeReopener) ReopenNamespace(_ string, edit func(*embed.StorageOptions)) (time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return 0, f.fail
	}
	f.calls++
	edit(&f.applied)
	return 25 * time.Millisecond, nil
}

func withReopener(r NamespaceReopener) func(*Options) {
	return func(o *Options) {
		o.AllowWrites = true
		o.Settings = append(
			config.Describe(nil, noEnvLookup, noFlagSet, config.DefaultServiceSettings()),
			config.EnvOnlyDescriptors(noEnvLookup)...)
		o.Reopener = r
	}
}

// A namespace-reopen setting has to actually reach the storage options, and the outcome has to
// say what it cost - a reopen makes the namespace briefly unavailable and an operator should not
// have to infer that from a latency graph.
func TestNamespaceReopenSettingApplies(t *testing.T) {
	r := &fakeReopener{namespace: "demo/users"}
	_, base := newFixture(t, withReopener(r))

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"cache.documentBytes","value":1048576}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	outcome := body["changes"].([]any)[0].(map[string]any)
	if outcome["applied"] != true {
		t.Fatalf("a reopenable setting was refused: %v", outcome)
	}
	if r.calls != 1 {
		t.Fatalf("the namespace was reopened %d times, want 1", r.calls)
	}
	if r.applied.DocumentCacheBytes != 1048576 {
		t.Fatalf("the edit reached the options as %d, want 1048576", r.applied.DocumentCacheBytes)
	}
	if w, _ := outcome["warning"].(string); w == "" {
		t.Error("a reopen reported no cost; the unavailability is the price of the operation")
	}
}

// storage.checkpoints reads as "on" and writes the inverted DisableCheckpoints, which is exactly
// the kind of mapping that is easy to get backwards.
func TestCheckpointsSettingInvertsCorrectly(t *testing.T) {
	r := &fakeReopener{namespace: "demo/users"}
	_, base := newFixture(t, withReopener(r))

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"storage.checkpoints","value":"off"}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	if !r.applied.DisableCheckpoints {
		t.Fatal("turning checkpoints off did not disable them")
	}

	r2 := &fakeReopener{namespace: "demo/users"}
	_, base2 := newFixture(t, withReopener(r2))
	if res, body := sendJSON(t, http.MethodPatch, base2, "/v1/settings",
		`{"changes":[{"key":"storage.checkpoints","value":"on"}]}`); res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	if r2.applied.DisableCheckpoints {
		t.Fatal("turning checkpoints on disabled them")
	}
}

// A dry run must not reopen anything, and must say that applying would.
func TestNamespaceReopenDryRunChangesNothing(t *testing.T) {
	r := &fakeReopener{namespace: "demo/users"}
	_, base := newFixture(t, withReopener(r))

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"dryRun":true,"changes":[{"key":"cache.documentBytes","value":1048576}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	if r.calls != 0 {
		t.Fatalf("a dry run reopened the namespace %d times", r.calls)
	}
	outcome := body["changes"].([]any)[0].(map[string]any)
	w, _ := outcome["warning"].(string)
	if w == "" {
		t.Fatal("a dry run of a reopen said nothing about what applying would do")
	}
}

// A process with no reopener refuses rather than reporting a hollow success, and the refusal says
// a restart is the way.
func TestNamespaceReopenRefusedWithoutAReopener(t *testing.T) {
	_, base := newFixture(t, func(o *Options) {
		o.AllowWrites = true
		o.Settings = append(
			config.Describe(nil, noEnvLookup, noFlagSet, config.DefaultServiceSettings()),
			config.EnvOnlyDescriptors(noEnvLookup)...)
	})
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"cache.documentBytes","value":1048576}]}`)
	if res.StatusCode == http.StatusOK {
		outcome := body["changes"].([]any)[0].(map[string]any)
		if outcome["applied"] == true {
			t.Fatal("a process with no reopener reported the change as applied")
		}
		if refused, _ := outcome["refused"].(string); refused == "" {
			t.Fatal("the refusal said nothing")
		}
	}
}
