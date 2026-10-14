//go:build !windows

package integration

import (
	"os/exec"
	"syscall"
)

// inOwnProcessGroup starts cmd in a process group of its own, so signalGroup reaches the JVM that
// gradle starts as well as gradle itself.
func inOwnProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// signalGroup asks cmd's process group to stop, or kills it outright when force is set.
func signalGroup(cmd *exec.Cmd, force bool) {
	sig := syscall.SIGTERM
	if force {
		sig = syscall.SIGKILL
	}
	_ = syscall.Kill(-cmd.Process.Pid, sig)
}
