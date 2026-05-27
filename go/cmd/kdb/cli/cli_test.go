package cli_test

import (
	"testing"

	"github.com/limidus/kdb/go/cmd/kdb/cli"
)

func TestParseInitCommand(t *testing.T) {
	cfg, cmd, err := cli.ParseArgsForTest([]string{"init", "demo/users"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir == "" {
		t.Fatal("expected default data dir")
	}
	init, ok := cmd.(cli.InitCmd)
	if !ok || init.Namespace != "demo/users" {
		t.Fatalf("cmd %+v", cmd)
	}
}

func TestParseBranchCreate(t *testing.T) {
	_, cmd, err := cli.ParseArgsForTest([]string{"branch", "create", "demo/users", "feature-a"})
	if err != nil {
		t.Fatal(err)
	}
	c, ok := cmd.(cli.BranchCreateCmd)
	if !ok || c.Name != "feature-a" {
		t.Fatalf("cmd %+v", cmd)
	}
}

func TestMissingCommandReturnsNil(t *testing.T) {
	_, cmd, err := cli.ParseArgsForTest(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != nil {
		t.Fatal("expected nil command")
	}
}
