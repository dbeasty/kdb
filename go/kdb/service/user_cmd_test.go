package service

import "testing"

// TestRunSubcommandBareOrUnknownPrintsUsage pins that the admin subcommands never fall through
// to a server start: a bare "token" (or "user"), or an unknown subcommand, prints usage and
// exits 2 before touching any registry - no --data-dir is given, so reaching the registry would
// fail with a different message, and falling through would report ok=false.
func TestRunSubcommandBareOrUnknownPrintsUsage(t *testing.T) {
	for _, args := range [][]string{
		{"token"},
		{"user"},
		{"token", "bogus"},
		{"user", "bogus"},
		{"token", "--data-dir", t.TempDir()},
	} {
		code, ok := runSubcommand(args)
		if !ok || code != 2 {
			t.Errorf("runSubcommand(%q) = (%d, %v), want (2, true)", args, code, ok)
		}
	}
}

func TestRunSubcommandLeavesServiceArgsAlone(t *testing.T) {
	for _, args := range [][]string{nil, {"--http", ":0"}, {"-version"}} {
		if _, ok := runSubcommand(args); ok {
			t.Errorf("runSubcommand(%q) handled service args", args)
		}
	}
}

func TestRunSubcommandTokenListReachesRegistry(t *testing.T) {
	code, ok := runSubcommand([]string{"token", "list", "--data-dir", t.TempDir()})
	if !ok || code != 0 {
		t.Fatalf("token list = (%d, %v), want (0, true)", code, ok)
	}
}
