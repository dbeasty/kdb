package main

import (
	"os"

	"github.com/limidus/kdb/go/cmd/kdb/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:]))
}
