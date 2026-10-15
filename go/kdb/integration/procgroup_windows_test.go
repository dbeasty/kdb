package integration

import (
	"os/exec"
	"strconv"
)

// Windows has no process groups to signal. The interop tests start gradle, which starts the JVM
// that holds the data dir's lock, so killing just the process gradle runs as leaves the JVM up:
// the next Kotlin CLI call finds the workspace "already open" and the test's TempDir cannot be
// removed. taskkill /T takes the whole tree; Windows has no graceful equivalent of SIGTERM for a
// console tree, so both a stop and a forced stop kill it.
func inOwnProcessGroup(cmd *exec.Cmd) {}

func signalGroup(cmd *exec.Cmd, force bool) {
	if err := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run(); err != nil {
		_ = cmd.Process.Kill()
	}
}
