package config

import (
	"strings"
	"testing"
)

// The headline behavior change: governance is on unless explicitly disabled. Previously the
// single mechanism protecting against an OOM kill defaulted to off, and nothing in the shipped
// Dockerfile, systemd unit, or these defaults turned it on.
func TestGovernanceIsOnByDefault(t *testing.T) {
	s, err := ResolveService(nil, noEnv, noFlags, ServiceSettings{})
	if err != nil {
		t.Fatal(err)
	}
	if s.MemoryBudgetMB != 0 {
		t.Errorf("default MemoryBudgetMB = %d, want 0 (auto-detect)", s.MemoryBudgetMB)
	}
	if s.MemoryReserveMB <= 0 {
		t.Errorf("a rescue reserve must be configured by default, got %d", s.MemoryReserveMB)
	}
	if s.MaxConnections <= 0 {
		t.Errorf("a connection cap must be configured by default, got %d", s.MaxConnections)
	}
	if s.ScanRowBudget <= 0 {
		t.Errorf("a scan row budget must be configured by default, got %d", s.ScanRowBudget)
	}
}

func TestMemoryBudgetExplicitDisable(t *testing.T) {
	s, err := ResolveService(nil, noEnv, flagsOf("memory-budget-mb"), ServiceSettings{MemoryBudgetMB: -1})
	if err != nil {
		t.Fatal(err)
	}
	if s.MemoryBudgetMB != -1 {
		t.Errorf("expected governance disabled, got %d", s.MemoryBudgetMB)
	}
}

// The deprecated flag has to keep meaning what it always meant. Someone who wrote
// --memory-limit-mb=0 meant "off"; silently upgrading them to "on, with a budget we picked" is
// exactly the surprise an alias exists to prevent.
func TestDeprecatedMemoryLimitZeroStillDisables(t *testing.T) {
	s, err := ResolveService(nil, noEnv, flagsOf("memory-limit-mb"), ServiceSettings{MemoryLimitMB: 0})
	if err != nil {
		t.Fatal(err)
	}
	if s.MemoryBudgetMB != -1 {
		t.Errorf("an explicit --memory-limit-mb=0 must disable governance, got budget %d", s.MemoryBudgetMB)
	}
}

func TestDeprecatedMemoryLimitSuppliesBudget(t *testing.T) {
	s, err := ResolveService(nil, noEnv, flagsOf("memory-limit-mb"), ServiceSettings{MemoryLimitMB: 512})
	if err != nil {
		t.Fatal(err)
	}
	if s.MemoryBudgetMB != 512 {
		t.Errorf("deprecated flag should supply the budget, got %d", s.MemoryBudgetMB)
	}
}

// When both spellings are given the modern one wins, rather than the resolution depending on
// which layer each happened to come from.
func TestModernBudgetFlagBeatsDeprecatedAlias(t *testing.T) {
	s, err := ResolveService(nil, noEnv, flagsOf("memory-budget-mb", "memory-limit-mb"),
		ServiceSettings{MemoryBudgetMB: 1024, MemoryLimitMB: 256})
	if err != nil {
		t.Fatal(err)
	}
	if s.MemoryBudgetMB != 1024 {
		t.Errorf("--memory-budget-mb should win over the deprecated alias, got %d", s.MemoryBudgetMB)
	}
}

func TestDeprecatedAliasFromConfigFile(t *testing.T) {
	zero := 0
	s, err := ResolveService(&ServiceFile{MemoryLimitMB: &zero}, noEnv, noFlags, ServiceSettings{})
	if err != nil {
		t.Fatal(err)
	}
	if s.MemoryBudgetMB != -1 {
		t.Errorf("memoryLimitMb: 0 in a config file must disable governance, got %d", s.MemoryBudgetMB)
	}
}

func TestGovernanceEnvVars(t *testing.T) {
	s, err := ResolveService(nil, envOf(map[string]string{
		"KDB_MEMORY_BUDGET_MB":  "2048",
		"KDB_MEMORY_RESERVE_MB": "64",
		"KDB_MAX_CONNECTIONS":   "512",
		"KDB_SCAN_ROW_BUDGET":   "250000",
	}), noFlags, ServiceSettings{})
	if err != nil {
		t.Fatal(err)
	}
	if s.MemoryBudgetMB != 2048 || s.MemoryReserveMB != 64 || s.MaxConnections != 512 || s.ScanRowBudget != 250000 {
		t.Errorf("environment overrides not applied: %+v", s)
	}
}

func TestGovernanceValidationRejectsNonsense(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags ServiceSettings
		set   string
		want  string
	}{
		{"budget below -1", ServiceSettings{MemoryBudgetMB: -5}, "memory-budget-mb", "memory-budget-mb"},
		{"negative reserve", ServiceSettings{MemoryReserveMB: -1}, "memory-reserve-mb", "memory-reserve-mb"},
		{"negative connections", ServiceSettings{MaxConnections: -1}, "max-connections", "max-connections"},
		{"negative row budget", ServiceSettings{ScanRowBudget: -1}, "scan-row-budget", "scan-row-budget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveService(nil, noEnv, flagsOf(tc.set), tc.flags)
			if err == nil {
				t.Fatal("expected a validation error at startup rather than a surprising runtime value")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should name the offending setting, got %q", err)
			}
		})
	}
}
