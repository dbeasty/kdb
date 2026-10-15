package service

import "testing"

func TestIsLoopbackAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:7702": true, "localhost:7702": true, "[::1]:7702": true,
		":7702": false, "0.0.0.0:7702": false, "10.0.0.5:7702": false, "example.com:80": false, "bad": false,
	} {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
	if !namespaceMatchesAny("a/b, qtask/*", "qtask/ttl") || namespaceMatchesAny("qtask/*", "other/ttl") || namespaceMatchesAny("", "qtask/x") {
		t.Fatal("namespaceMatchesAny")
	}
}
