//go:build js && wasm

package main

import (
	"syscall/js"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
)

func main() {
	api := map[string]any{
		"openMemory": js.FuncOf(openMemory),
	}
	js.Global().Set("KdbBrowser", api)
	select {}
}

func openMemory(_ js.Value, args []js.Value) any {
	if len(args) < 2 {
		return errVal("openMemory(catalog, namespace) requires 2 arguments")
	}
	rt, err := embed.OpenMemoryRuntime(args[0].String(), args[1].String(), schema.None())
	if err != nil {
		return errVal(err.Error())
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return errVal(err.Error())
	}
	return map[string]any{
		"catalog":   rt.Catalog,
		"namespace": rt.DefaultNamespace,
		"head":      head.Hex(),
	}
}

func errVal(msg string) map[string]any {
	return map[string]any{"error": msg}
}
