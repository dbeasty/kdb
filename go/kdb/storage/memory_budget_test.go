package storage

import "testing"

func TestResolveHotTierBytes_DefaultsWhenUnconfigured(t *testing.T) {
	got := ResolveHotTierBytes(HotTierMemoryConfig{})
	if got != DefaultHotTierBytes {
		t.Fatalf("got %d, want DefaultHotTierBytes=%d", got, DefaultHotTierBytes)
	}
}

func TestResolveHotTierBytes_FixedBytesWins(t *testing.T) {
	got := ResolveHotTierBytes(HotTierMemoryConfig{FixedBytes: 1 << 30, PercentOfAvailable: 50})
	if got != 1<<30 {
		t.Fatalf("got %d, want FixedBytes to take precedence (%d)", got, int64(1<<30))
	}
}

func TestResolveHotTierBytes_PercentOfAvailable(t *testing.T) {
	total, err := totalSystemMemoryBytes()
	if err != nil {
		t.Skipf("totalSystemMemoryBytes unavailable on this platform: %v", err)
	}
	got := ResolveHotTierBytes(HotTierMemoryConfig{PercentOfAvailable: 10})
	want := int64(float64(total) * 10 / 100)
	if got != want {
		t.Fatalf("got %d, want %d (10%% of %d)", got, want, total)
	}
	if got <= DefaultHotTierBytes {
		t.Fatalf("got %d, expected 10%% of a real machine's memory to exceed the %d default", got, DefaultHotTierBytes)
	}
}

func TestResolveHotTierBytes_PercentClampedAt100(t *testing.T) {
	total, err := totalSystemMemoryBytes()
	if err != nil {
		t.Skipf("totalSystemMemoryBytes unavailable on this platform: %v", err)
	}
	got := ResolveHotTierBytes(HotTierMemoryConfig{PercentOfAvailable: 250})
	if got != total {
		t.Fatalf("got %d, want clamped to total system memory %d", got, total)
	}
}

func TestValidateHotTierMemoryConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     HotTierMemoryConfig
		wantErr bool
	}{
		{"zero value ok", HotTierMemoryConfig{}, false},
		{"fixed bytes ok", HotTierMemoryConfig{FixedBytes: 1024}, false},
		{"percent ok", HotTierMemoryConfig{PercentOfAvailable: 25}, false},
		{"negative fixed bytes", HotTierMemoryConfig{FixedBytes: -1}, true},
		{"negative percent", HotTierMemoryConfig{PercentOfAvailable: -1}, true},
		{"percent over 100", HotTierMemoryConfig{PercentOfAvailable: 101}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateHotTierMemoryConfig(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateHotTierMemoryConfig(%+v) err=%v, wantErr=%v", tc.cfg, err, tc.wantErr)
			}
		})
	}
}

func TestResolvedGlobalMemoryBudgetBytes(t *testing.T) {
	explicit := StorageEngineConfig{GlobalMemoryBudgetBytes: 42}
	if got := explicit.ResolvedGlobalMemoryBudgetBytes(); got != 42 {
		t.Fatalf("explicit GlobalMemoryBudgetBytes: got %d, want 42", got)
	}

	unset := StorageEngineConfig{}
	if got := unset.ResolvedGlobalMemoryBudgetBytes(); got != DefaultHotTierBytes {
		t.Fatalf("unset GlobalMemoryBudgetBytes: got %d, want default %d", got, DefaultHotTierBytes)
	}
}
