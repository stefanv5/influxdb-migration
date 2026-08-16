//go:build !windows

package checkpoint

import "syscall"

// pidIsAlive reports whether the given pid is currently running.
//
// On Unix, syscall.Kill with signal 0 returns no error if the process exists
// (and we have permission to signal it) and ESRCH if it does not. EPERM means
// the process exists but is owned by another user; we treat that as alive.
func pidIsAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
