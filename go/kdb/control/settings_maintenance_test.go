package control

import (
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

// staticMaintenance is a MaintenanceSource over a fixed set of loops.
type staticMaintenance map[string]*embed.MaintenanceScheduler

func (m staticMaintenance) Schedulers() map[string]*embed.MaintenanceScheduler { return m }

// withMaintenance gives the fixture a real, running maintenance loop over a temporary
// file-backed runtime - the settings under test reach a scheduler, not a stub, so a change that
// the scheduler itself would refuse still fails here. It returns the scheduler so a test can
// assert against the loop rather than against the reported value.
func withMaintenance(t *testing.T) (func(*Options), *embed.MaintenanceScheduler) {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	rt, err := embed.OpenFileRuntimeWithOptions(t.TempDir(), "demo", "demo/users", schema.None(), opts)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(rt.Close)
	sched := embed.StartMaintenance(rt, embed.MaintenanceOptions{Interval: time.Hour})
	if sched == nil {
		t.Fatal("no scheduler started for a writable file-backed runtime")
	}
	t.Cleanup(sched.Stop)
	return func(o *Options) {
		o.AllowWrites = true
		o.Settings = append(
			config.Describe(nil, noEnvLookup, noFlagSet, config.DefaultServiceSettings()),
			config.EnvOnlyDescriptors(noEnvLookup)...)
		o.Maintenance = staticMaintenance{"demo/users": sched}
	}, sched
}

// The maintenance cadence is live: a patch has to reach the running loop, not merely be recorded
// as the value in force. Asserting on the scheduler itself is the difference between the two.
func TestMaintenanceCadenceIsLive(t *testing.T) {
	opt, sched := withMaintenance(t)
	_, base := newFixture(t, opt)

	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"governance.maintenanceInterval","value":"90s"},`+
			`{"key":"governance.maintenanceSweep","value":"45m"},`+
			`{"key":"governance.maintenanceMaxDefer","value":3}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("patch: %d (%v)", res.StatusCode, body)
	}
	for _, o := range body["changes"].([]any) {
		outcome := o.(map[string]any)
		if outcome["applied"] != true {
			t.Fatalf("%v did not apply: %v", outcome["key"], outcome)
		}
	}
	if got := sched.Interval(); got != 90*time.Second {
		t.Errorf("the running loop's interval is %v, want 90s", got)
	}
	if got := sched.Sweep(); got != 45*time.Minute {
		t.Errorf("the running loop's sweep is %v, want 45m", got)
	}
	if got := sched.MaxDefer(); got != 3 {
		t.Errorf("the running loop's maxDefer is %d, want 3", got)
	}
}

// Zero has two readings for these - "as fast as possible" and "stop maintaining" - so it is
// refused rather than guessed at. A settings patch is not the place to stop a loop.
func TestMaintenanceIntervalRejectsZero(t *testing.T) {
	opt, _ := withMaintenance(t)
	_, base := newFixture(t, opt)
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"governance.maintenanceInterval","value":"0s"}]}`)
	if res.StatusCode == http.StatusOK {
		outcome := body["changes"].([]any)[0].(map[string]any)
		if outcome["applied"] == true {
			t.Fatal("a zero maintenance interval was accepted")
		}
	}
}

// maxDefer=0 would read as "never defer" from the UI but means "use the default" in the struct,
// so it has to be refused rather than silently turned into 6.
func TestMaintenanceMaxDeferRejectsAmbiguousZero(t *testing.T) {
	opt, sched := withMaintenance(t)
	_, base := newFixture(t, opt)
	before := sched.MaxDefer()
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"governance.maintenanceMaxDefer","value":0}]}`)
	if res.StatusCode == http.StatusOK {
		outcome := body["changes"].([]any)[0].(map[string]any)
		if outcome["applied"] == true {
			t.Fatal("maxDefer=0 was accepted despite being ambiguous")
		}
	}
	if got := sched.MaxDefer(); got != before {
		t.Errorf("a refused change still moved maxDefer to %d", got)
	}
}

// A process running no maintenance loops must say so rather than report a hollow success.
func TestMaintenanceSettingsRefusedWithoutLoops(t *testing.T) {
	level := new(slog.LevelVar)
	_, base := newFixture(t, withSettings(level, false))
	res, body := sendJSON(t, http.MethodPatch, base, "/v1/settings",
		`{"changes":[{"key":"governance.maintenanceInterval","value":"90s"}]}`)
	if res.StatusCode == http.StatusOK {
		outcome := body["changes"].([]any)[0].(map[string]any)
		if outcome["applied"] == true {
			t.Fatal("a process with no maintenance loops reported the change as applied")
		}
	}
}
