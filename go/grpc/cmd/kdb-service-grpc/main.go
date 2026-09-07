// Command kdb-service-grpc is kdb-service with the gRPC transport linked in.
//
// Same service, same configuration, same listeners - plus --grpc-addr, which the default
// kdb-service binary refuses because it does not link gRPC at all. The split is at the module
// boundary rather than behind a build tag because a build tag would not keep
// google.golang.org/grpc out of the core module's dependency graph, and the core module is what
// gomobile and embedbundle build from. See docs/kdb-spec-layer17-multi-namespace-runtime.md §3.2.
package main

import (
	_ "github.com/limidus/kdb/go/grpc/register"
	"github.com/limidus/kdb/go/kdb/service"
)

func main() { service.Main() }
