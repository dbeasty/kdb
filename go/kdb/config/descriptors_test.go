package config

import (
	"reflect"
	"strings"
	"testing"
)

// fieldToKey maps every ServiceSettings field to the descriptor key that describes it. It exists
// so TestEverySettingIsDescribed can fail loudly when a field is added to ServiceSettings and not
// to serviceSpecs - a settings view that silently omits a setting is worse than no settings view,
// because an operator reads absence as "not configured".
var fieldToKey = map[string]string{
	"DataDir":                "storage.dataDir",
	"Memory":                 "storage.memory",
	"Namespace":              "namespace.default",
	"SQLAddr":                "listener.sqlAddr",
	"PeerAddr":               "listener.peerAddr",
	"StreamAddr":             "listener.streamAddr",
	"WSAddr":                 "listener.wsAddr",
	"GRPCAddr":               "listener.grpcAddr",
	"AdminAddr":              "listener.adminAddr",
	"RBAC":                   "auth.rbac",
	"MemoryBudgetMB":         "memory.budgetMB",
	"MemoryLimitMB":          "memory.limitMB",
	"MemoryReserveMB":        "memory.reserveMB",
	"MaxConnections":         "governance.maxConnections",
	"ScanRowBudget":          "governance.scanRowBudget",
	"AbortAfter":             "governance.abortAfter",
	"DrainTimeout":           "governance.drainTimeout",
	"TLSCert":                "tls.certFile",
	"TLSKey":                 "tls.keyFile",
	"TLSCA":                  "tls.caFile",
	"TLSClientAuth":          "tls.clientAuth",
	"ControlAddr":            "control.addr",
	"ControlWrite":           "control.write",
	"ControlUI":              "control.ui",
	"ControlSettingsPersist": "control.settingsPersist",
	"LogLevel":               "log.level",
	"LogFormat":              "log.format",
	"Durability":             "storage.durability",
	"AsyncSyncIntervalMS":    "storage.asyncSyncIntervalMS",
	"Compression":            "storage.compression",
	"SyncMode":               "storage.syncMode",
}

func TestEverySettingIsDescribed(t *testing.T) {
	described := map[string]bool{}
	for _, d := range Describe(nil, nil, nil, DefaultServiceSettings()) {
		described[d.Key] = true
	}
	typ := reflect.TypeOf(ServiceSettings{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i).Name
		key, ok := fieldToKey[field]
		if !ok {
			t.Errorf("ServiceSettings.%s has no descriptor key: add it to serviceSpecs() in "+
				"descriptors.go and to fieldToKey here", field)
			continue
		}
		if !described[key] {
			t.Errorf("ServiceSettings.%s maps to key %q, which Describe never returns", field, key)
		}
	}
}

// TestDescribedEnvNamesAreHonoured guards the other direction: a descriptor that claims a value
// came from KDB_FOO is lying unless ResolveService actually reads KDB_FOO. Both halves are driven
// off the same spec table, so the check is that every env name a spec names is one the resolver
// responds to.
func TestDescribedEnvNamesAreHonoured(t *testing.T) {
	for _, spec := range serviceSpecs() {
		if spec.env == "" {
			continue
		}
		env := map[string]string{spec.env: envProbeValue(spec.key)}
		lookup := func(name string) (string, bool) {
			v, ok := env[name]
			return v, ok
		}
		resolved, err := ResolveService(nil, lookup, noFlags, ServiceSettings{})
		if err != nil {
			t.Fatalf("%s: resolving with %s set: %v", spec.key, spec.env, err)
		}
		def := spec.value(DefaultServiceSettings())
		if got := spec.value(resolved); got == def {
			t.Errorf("%s: setting %s had no effect on the resolved value (still %v) - either the "+
				"resolver does not read that variable, or the spec names the wrong one",
				spec.key, spec.env, got)
		}
	}
}

// envProbeValue returns a value that is valid for the setting's type and differs from its default,
// so TestDescribedEnvNamesAreHonoured can tell "the resolver read it" from "nothing happened".
func envProbeValue(key string) string {
	switch key {
	case "storage.memory", "auth.rbac", "tls.clientAuth", "control.write":
		return "true"
	case "control.settingsPersist":
		return "true"
	case "control.ui":
		// Defaults to on, so "true" would be indistinguishable from nothing happening.
		return "false"
	case "memory.budgetMB", "memory.limitMB", "memory.reserveMB",
		"governance.maxConnections", "governance.scanRowBudget", "storage.asyncSyncIntervalMS":
		return "4242"
	case "governance.abortAfter", "governance.drainTimeout":
		return "7m"
	case "log.level":
		return "debug"
	case "log.format":
		return "json"
	case "storage.durability":
		return "async"
	case "storage.compression":
		return "none"
	case "storage.syncMode":
		return "fast"
	default:
		return "probe-value"
	}
}

func TestDescribeAttributesEachPrecedenceLayer(t *testing.T) {
	fileWS := "ws://127.0.0.1:1111"
	file := &ServiceFile{WSAddr: &fileWS, LogLevel: strPtr("warn")}

	// Layer by layer, the same order ResolveService applies them.
	t.Run("default", func(t *testing.T) {
		d := findKey(t, Describe(nil, nil, nil, DefaultServiceSettings()), "listener.sqlAddr")
		if d.Source != SourceDefault {
			t.Fatalf("want %q, got %q (%s)", SourceDefault, d.Source, d.SourceDetail)
		}
	})

	t.Run("config file", func(t *testing.T) {
		resolved, err := ResolveService(file, noEnv, noFlags, ServiceSettings{})
		if err != nil {
			t.Fatal(err)
		}
		d := findKey(t, Describe(file, nil, nil, resolved), "listener.wsAddr")
		if d.Source != SourceConfigFile {
			t.Fatalf("want %q, got %q", SourceConfigFile, d.Source)
		}
		if d.Value != fileWS {
			t.Fatalf("value %v does not match what the resolver produced (%q)", d.Value, fileWS)
		}
	})

	t.Run("env beats file", func(t *testing.T) {
		lookup := staticEnv(map[string]string{"KDB_WS_ADDR": "ws://127.0.0.1:2222"})
		resolved, err := ResolveService(file, lookup, noFlags, ServiceSettings{})
		if err != nil {
			t.Fatal(err)
		}
		d := findKey(t, Describe(file, lookup, nil, resolved), "listener.wsAddr")
		if d.Source != SourceEnv || d.SourceDetail != "KDB_WS_ADDR" {
			t.Fatalf("want env/KDB_WS_ADDR, got %q/%q", d.Source, d.SourceDetail)
		}
		if d.Value != "ws://127.0.0.1:2222" {
			t.Fatalf("value %v is not the environment's", d.Value)
		}
	})

	t.Run("explicit flag beats env", func(t *testing.T) {
		lookup := staticEnv(map[string]string{"KDB_WS_ADDR": "ws://127.0.0.1:2222"})
		wasSet := func(name string) bool { return name == "ws-addr" }
		flags := ServiceSettings{WSAddr: "ws://127.0.0.1:3333"}
		resolved, err := ResolveService(file, lookup, wasSet, flags)
		if err != nil {
			t.Fatal(err)
		}
		d := findKey(t, Describe(file, lookup, wasSet, resolved), "listener.wsAddr")
		if d.Source != SourceFlag || d.SourceDetail != "--ws-addr" {
			t.Fatalf("want flag/--ws-addr, got %q/%q", d.Source, d.SourceDetail)
		}
		if d.Value != "ws://127.0.0.1:3333" {
			t.Fatalf("value %v is not the flag's", d.Value)
		}
	})

	t.Run("an untouched setting still reports its own source", func(t *testing.T) {
		lookup := staticEnv(map[string]string{"KDB_WS_ADDR": "ws://127.0.0.1:2222"})
		resolved, err := ResolveService(file, lookup, noFlags, ServiceSettings{})
		if err != nil {
			t.Fatal(err)
		}
		d := findKey(t, Describe(file, lookup, nil, resolved), "log.level")
		if d.Source != SourceConfigFile {
			t.Fatalf("log.level came from the file; got %q", d.Source)
		}
	})
}

func TestMarkAutoDetectedOnlyClaimsUnsetValues(t *testing.T) {
	t.Run("claims a default", func(t *testing.T) {
		ds := Describe(nil, nil, nil, DefaultServiceSettings())
		MarkAutoDetected(ds, "memory.budgetMB", 2048, "cgroup limit")
		d := findKey(t, ds, "memory.budgetMB")
		if d.Source != SourceAuto || d.Value != 2048 {
			t.Fatalf("want auto/2048, got %q/%v", d.Source, d.Value)
		}
	})

	t.Run("leaves an explicit value alone", func(t *testing.T) {
		wasSet := func(name string) bool { return name == "memory-budget-mb" }
		flags := ServiceSettings{MemoryBudgetMB: 512}
		resolved, err := ResolveService(nil, noEnv, wasSet, flags)
		if err != nil {
			t.Fatal(err)
		}
		ds := Describe(nil, nil, wasSet, resolved)
		MarkAutoDetected(ds, "memory.budgetMB", 2048, "cgroup limit")
		d := findKey(t, ds, "memory.budgetMB")
		if d.Source != SourceFlag {
			t.Fatalf("an operator named this value; auto-detection must not claim it (got %q)", d.Source)
		}
		if d.Value != 512 {
			t.Fatalf("want the operator's 512, got %v", d.Value)
		}
	})
}

// TestEnvOnlyDescriptorsSurfaceDiscardedValues is the point of the whole env-only surface: the
// engine throws an unparseable KDB_* value away without a word, and today an operator has no way
// to find out. A warning is the entire fix.
func TestEnvOnlyDescriptorsSurfaceDiscardedValues(t *testing.T) {
	lookup := staticEnv(map[string]string{
		"KDB_DOCUMENT_CACHE_BYTES":  "not-a-number",
		"KDB_ANCESTRY_PRUNING":      "on",
		"KDB_GRAPH_REBUILD_COMMITS": "-5",
	})
	ds := EnvOnlyDescriptors(lookup)

	bad := findKey(t, ds, "cache.documentBytes")
	if len(bad.Warnings) == 0 {
		t.Fatal("a KDB_DOCUMENT_CACHE_BYTES that will not parse must produce a warning")
	}
	if !strings.Contains(bad.Warnings[0], "not-a-number") {
		t.Errorf("the warning should quote what was actually set: %q", bad.Warnings[0])
	}
	if bad.Source != SourceDefault {
		t.Errorf("a discarded value did not come from the environment; source was %q", bad.Source)
	}

	good := findKey(t, ds, "graph.ancestryPruning")
	if good.Source != SourceEnv || good.Value != true {
		t.Errorf("want env/true, got %q/%v", good.Source, good.Value)
	}
	if len(good.Warnings) != 0 {
		t.Errorf("a value that parsed cleanly must not warn: %v", good.Warnings)
	}

	negative := findKey(t, ds, "graph.rebuildCommits")
	if len(negative.Warnings) == 0 {
		t.Error("a negative commit count is discarded by envInt and must warn")
	}
}

func TestEnvOnlyDescriptorsMaskSecrets(t *testing.T) {
	lookup := staticEnv(map[string]string{"KDB_S3_SECRET_ACCESS_KEY": "hunter2"})
	d := findKey(t, EnvOnlyDescriptors(lookup), "s3.secretAccessKey")
	if !d.Sensitive {
		t.Fatal("the S3 secret key must be marked sensitive")
	}
	if d.Value == "hunter2" {
		t.Fatal("a sensitive value must never be returned in the clear")
	}
	if d.Source != SourceEnv {
		t.Errorf("the source is still reportable even when the value is not: got %q", d.Source)
	}
}

func TestUnsetEnvOnlySettingHasNoValue(t *testing.T) {
	d := findKey(t, EnvOnlyDescriptors(staticEnv(nil)), "cache.documentBytes")
	if d.Value != nil {
		t.Fatalf("an unset cache budget is derived from the hot-tier budget at open, not a "+
			"constant, so reporting a number here would be a guess: got %v", d.Value)
	}
}

func staticEnv(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
}

func findKey(t *testing.T, ds []SettingDescriptor, key string) SettingDescriptor {
	t.Helper()
	for _, d := range ds {
		if d.Key == key {
			return d
		}
	}
	t.Fatalf("no descriptor for %q", key)
	return SettingDescriptor{}
}

func strPtr(s string) *string { return &s }

// TestResolveServiceToleratesNilCallbacks: both callbacks were dereferenced unconditionally, so a
// caller with no environment and no parsed command line - which is every programmatic caller -
// panicked instead of getting the defaults.
func TestResolveServiceToleratesNilCallbacks(t *testing.T) {
	cases := map[string]struct {
		env   func(string) (string, bool)
		flags func(string) bool
	}{
		"both nil":        {nil, nil},
		"nil environment": {nil, noFlags},
		"nil flag lookup": {noEnv, nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ResolveService(nil, tc.env, tc.flags, ServiceSettings{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != DefaultServiceSettings() {
				t.Errorf("with nothing to consult, the result must be the defaults; got %+v", got)
			}
		})
	}
}
