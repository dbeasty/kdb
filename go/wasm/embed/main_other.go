//go:build !(js && wasm)

// Command embed only does something under GOOS=js GOARCH=wasm; this stub keeps `go build ./...`
// and `go vet ./...` green on every other target.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "wasm/embed: build with GOOS=js GOARCH=wasm (see packages/kdb-embed)")
	os.Exit(2)
}
