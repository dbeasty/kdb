package service

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/wire"
)

// adoptRestoredDefinitions adopts the definitions a restore left for namespace
// (backup.RestoredDefinitionsFile) into the metadata namespace - which applies them: the schema
// and its unique constraints, and every index, built from the restored documents - and removes
// the file. A failure is logged and the file kept, so the next start tries again; the documents
// are served either way.
func adoptRestoredDefinitions(dataDir, namespace string, meta *server.MetaStore) {
	raw, ok, err := backup.ReadRestoredDefinitions(dataDir, namespace)
	if err != nil {
		slog.Warn("could not read restored definitions", "namespace", namespace, "error", err)
		return
	}
	if !ok {
		return
	}
	var defs []wire.MetaDefinition
	if err := json.Unmarshal(raw, &defs); err != nil {
		slog.Warn("restored definitions are not valid; ignoring them", "namespace", namespace, "error", err)
		return
	}
	start := time.Now()
	n, err := meta.AdoptView(defs)
	if err != nil {
		slog.Warn("could not adopt restored definitions; will retry at the next start", "namespace", namespace, "error", err)
		return
	}
	if err := backup.RemoveRestoredDefinitions(dataDir, namespace); err != nil {
		slog.Warn("could not remove adopted definitions file", "namespace", namespace, "error", err)
	}
	slog.Info("adopted the definitions restored with the namespace", "namespace", namespace,
		"definitions", len(defs), "changed", n, "took", time.Since(start).Round(time.Millisecond))
}

// namespaceMatchesAny reports whether ns is named by the comma-separated list of namespaces and
// patterns (peersync.MatchNamespace: "*" matches one segment, "**" any number).
func namespaceMatchesAny(list, ns string) bool {
	for _, p := range strings.Split(list, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p == ns || peersync.MatchNamespace(p, ns) {
			return true
		}
	}
	return false
}
