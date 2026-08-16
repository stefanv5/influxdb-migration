//go:build windows

package checkpoint

import (
	"syscall"
)

// Windows does not support the Unix signal-0 liveness check. os.FindProcess
// on Windows calls OpenProcess, which PANICS (nil-pointer dereference) on a
// non-existent pid, and (*Process).Signal returns "not supported by windows"
// even for the current process. Neither is usable.
//
// Instead we call the Win32 OpenProcess API directly via syscall.NewLazyDLL.
// OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid) returns a
// non-zero handle iff the process exists and is accessible; it returns NULL
// (with ERROR_INVALID_PARAMETER, 87) when the pid does not correspond to a
// running process. This is reliable and stdlib-only.

var (
	modKernel32     = syscall.NewLazyDLL("kernel32.dll")
	procOpenProcess = modKernel32.NewProc("OpenProcess")
	procCloseHandle = modKernel32.NewProc("CloseHandle")
)

// PROCESS_QUERY_LIMITED_INFORMATION: query basic info without needing full
// access rights; succeeds for processes owned by other users/sessions in
// common cases where PROCESS_QUERY_INFORMATION would be denied.
const processQueryLimitedInformation = 0x1000

// pidIsAlive reports whether the given pid is currently running on Windows.
//
// Returns false for non-positive pids. For positive pids it opens a query
// handle; a non-zero handle means alive. A zero handle means the pid is not
// running (ERROR_INVALID_PARAMETER) or is otherwise inaccessible — we treat
// "inaccessible" as not-alive so a stale lock from an inaccessible holder can
// be reclaimed. The tradeoff: a pid owned by a more privileged user (very rare
// for two `migrate` invocations by the same user) would be considered dead and
// its lock reclaimed, which is the safer failure mode for a checkpoint store
// (re-running migration is always safe; double-running is not).
func pidIsAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	handle, _, _ := procOpenProcess.Call(
		uintptr(processQueryLimitedInformation),
		uintptr(0), // bInheritHandle = FALSE
		uintptr(pid),
	)
	if handle == 0 {
		return false
	}
	_, _, _ = procCloseHandle.Call(handle)
	return true
}
