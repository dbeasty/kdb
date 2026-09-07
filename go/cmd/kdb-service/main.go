// Command kdb-service runs the KDB server.
//
// The service itself lives in kdb/service, so that a build linking an out-of-tree transport can
// reuse it - go/grpc/cmd/kdb-service-grpc is the same service with the gRPC listener compiled in.
// This binary deliberately links no gRPC: its dependencies have no business in a build that most
// deployments run without it. See docs/kdb-spec-layer17-multi-namespace-runtime.md §3.2.
package main

import "github.com/limidus/kdb/go/kdb/service"

func main() { service.Main() }
