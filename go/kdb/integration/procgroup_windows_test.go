package integration

import "os/exec"

// Windows has no process groups to signal: the process itself is killed, which is enough for the
// tests that build here (the gradle-driven interop cases need a Unix host).
func inOwnProcessGroup(cmd *exec.Cmd) {}

func signalGroup(cmd *exec.Cmd, force bool) { _ = cmd.Process.Kill() }
