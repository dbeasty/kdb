package storage

import (
	"testing"
	"time"
)

func TestParseHistoryMode(t *testing.T) {
	cases := map[string]HistoryMode{
		"":       HistoryModeUnset,
		"full":   HistoryModeFull,
		"FULL":   HistoryModeFull,
		"none":   HistoryModeNone,
		" None ": HistoryModeNone,
	}
	for in, want := range cases {
		got, err := ParseHistoryMode(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got != want {
			t.Fatalf("%q parsed to %v, want %v", in, got, want)
		}
	}
	if _, err := ParseHistoryMode("some"); err == nil {
		t.Fatal("an unknown mode should be an error, not a default")
	}
}

func TestHistoryModeRoundTripsThroughItsName(t *testing.T) {
	for _, m := range []HistoryMode{HistoryModeFull, HistoryModeNone} {
		got, err := ParseHistoryMode(m.String())
		if err != nil || got != m {
			t.Fatalf("%v did not round-trip: %v (%v)", m, got, err)
		}
	}
	if HistoryModeUnset.String() != "" {
		t.Fatal("unset must render as the empty string so an absent marker and an absent setting agree")
	}
}

func TestParseRetentionDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"":     0,
		"24h":  24 * time.Hour,
		"80h":  80 * time.Hour,
		"90m":  90 * time.Minute,
		"7d":   7 * 24 * time.Hour,
		"1.5d": 36 * time.Hour,
		"0":    RetainNothing,
	}
	for in, want := range cases {
		got, err := ParseRetentionDuration(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got != want {
			t.Fatalf("%q parsed to %v, want %v", in, got, want)
		}
	}
	for _, bad := range []string{"forever", "-3h", "3 hours", "d"} {
		if _, err := ParseRetentionDuration(bad); err == nil {
			t.Fatalf("%q should not parse", bad)
		}
	}
}

// "0" and unset must stay distinguishable: one keeps nothing, the other
// takes the default, and collapsing them would silently give a namespace
// that asked for no retention a day of it.
func TestExplicitZeroIsNotUnset(t *testing.T) {
	zero, err := ParseRetentionDuration("0")
	if err != nil {
		t.Fatal(err)
	}
	unset, err := ParseRetentionDuration("")
	if err != nil {
		t.Fatal(err)
	}
	if zero == unset {
		t.Fatal("explicit zero and unset must not be the same value")
	}
	if got := (RetentionWindow{Duration: zero}).Resolve(); got.Duration > 0 || got.Commits != 0 {
		t.Fatalf("explicit zero should resolve to no retention, got %v", got)
	}
	if got := (RetentionWindow{Duration: unset}).Resolve(); got.Duration != DefaultRetentionDuration {
		t.Fatalf("unset should resolve to the default, got %v", got)
	}
}

func TestRetentionWindowResolve(t *testing.T) {
	if !(RetentionWindow{}).IsZero() {
		t.Fatal("the zero window should report itself zero")
	}
	if (RetentionWindow{Commits: 10}).IsZero() {
		t.Fatal("a commit floor alone is not a zero window")
	}
	// A commit floor alone must not silently acquire the default duration
	// as well: that would keep more than the operator asked for, which is
	// the safe direction but not the requested one.
	got := RetentionWindow{Commits: 10}.Resolve()
	if got.Duration != 0 || got.Commits != 10 {
		t.Fatalf("a commit-only window resolved to %v", got)
	}
}

// Resolve has to be idempotent.
//
// It was not: it normalized RetainNothing to a plain zero, and a second
// Resolve then read that zero as "unset" and applied the 24h default - so
// a namespace configured to keep nothing quietly kept a day, and only when
// its window happened to pass through two resolvers. The sentinel now
// survives, and this asserts it does however many times it is applied.
func TestResolveIsIdempotentForKeepNothing(t *testing.T) {
	w := RetentionWindow{Duration: RetainNothing}
	for i := 0; i < 5; i++ {
		w = w.Resolve()
		if w.Duration > 0 {
			t.Fatalf("after %d resolves, keep-nothing became %v", i+1, w.Duration)
		}
	}
	if got := (RetentionWindow{Duration: RetainNothing}).String(); got != "nothing beyond the checkpoint" {
		t.Fatalf("keep-nothing renders as %q", got)
	}
	// And the default still survives repeated resolution unchanged.
	d := RetentionWindow{}.Resolve()
	if d.Resolve().Duration != DefaultRetentionDuration {
		t.Fatal("resolving the default twice changed it")
	}
}
