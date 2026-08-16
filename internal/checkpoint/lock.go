package checkpoint

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// lockFileInfo is the content written to the lock file so a later process can
// identify the holder and decide whether the lock is stale.
type lockFileInfo struct {
	pid     int
	host    string
	started time.Time
}

func (l lockFileInfo) String() string {
	return fmt.Sprintf("pid=%d\nhost=%s\nstarted=%s\n", l.pid, l.host, l.started.UTC().Format(time.RFC3339))
}

// parseLockFile parses the pid/host/started fields from a lock file's content.
// Missing or malformed fields yield their zero values; pid is -1 if unset so
// the caller can distinguish "no pid" from "pid 0".
func parseLockFile(data []byte) lockFileInfo {
	info := lockFileInfo{pid: -1}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		switch key {
		case "pid":
			if p, err := strconv.Atoi(val); err == nil {
				info.pid = p
			}
		case "host":
			info.host = val
		case "started":
			if t, err := time.Parse(time.RFC3339, val); err == nil {
				info.started = t
			}
		}
	}
	return info
}

// acquireLockFile acquires an exclusive cross-process lock on the checkpoint
// directory by atomically creating a sibling file `<dir>/checkpoints.db.lock`.
//
// Approach (stdlib only, no gofrs/flock):
//
//  1. Try to create the lock file with O_CREATE|O_EXCL. This is atomic on both
//     POSIX and Windows: if the file already exists, creation fails.
//  2. If creation fails because the file exists, read its content (pid/host/
//     started) and ask the OS whether that pid is still alive.
//     - If the holder is dead, steal the lock: remove the stale file and
//       recreate it. This guards against crashed `migrate` processes.
//     - If the holder is alive, return an error. Only one migrate process may
//       use a checkpoint_dir at a time.
//  3. Write the current pid, hostname, and start time into the lock file.
//
// Limitation: O_EXCL is not a true advisory flock. A process killed with
// SIGKILL (or a Windows hard crash) leaves a stale lock file. The pid-liveness
// check recovers from this in the common case. On Windows, pid-liveness is
// detected via OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION); see
// lock_windows.go. On Unix, syscall.Kill(pid, 0) is used; see lock_unix.go.
//
// The returned release function removes the lock file. It is best-effort: a
// failed removal is ignored (the next process will reclaim via the liveness
// check).
func acquireLockFile(dir string) (release func() error, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint dir: %w", err)
	}
	lockPath := filepath.Join(dir, "checkpoints.db.lock")

	host, hostErr := os.Hostname()
	if hostErr != nil || host == "" {
		host = "unknown"
	}
	info := lockFileInfo{pid: os.Getpid(), host: host, started: time.Now().UTC()}

	// Fast path: atomic create.
	f, createErr := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if createErr == nil {
		if _, werr := f.WriteString(info.String()); werr != nil {
			f.Close()
			_ = os.Remove(lockPath)
			return nil, fmt.Errorf("failed to write lock file %s: %w", lockPath, werr)
		}
		if cerr := f.Close(); cerr != nil {
			_ = os.Remove(lockPath)
			return nil, fmt.Errorf("failed to close lock file %s: %w", lockPath, cerr)
		}
		return buildRelease(lockPath), nil
	}

	// File exists: inspect the holder.
	if !os.IsExist(createErr) {
		return nil, fmt.Errorf("failed to create lock file %s: %w", lockPath, createErr)
	}

	existing, readErr := os.ReadFile(lockPath)
	if readErr != nil {
		// Could not read the holder info; refuse to steal blindly. The user
		// must inspect and remove the lock file manually.
		return nil, fmt.Errorf("checkpoint dir %s is locked by an unknown process and the lock file %s is unreadable: %w; remove it manually if the holder is no longer running", dir, lockPath, readErr)
	}
	holder := parseLockFile(existing)

	if holder.pid > 0 && pidIsAlive(holder.pid) {
		return nil, fmt.Errorf("checkpoint directory %s is locked by another process (pid=%d host=%s started=%s); only one migrate process may use a checkpoint_dir at a time", dir, holder.pid, holder.host, holder.started.Format(time.RFC3339))
	}

	// Holder is dead (or pid missing): reclaim the lock. Remove then recreate
	// atomically. There is a small TOCTOU window here if two processes race to
	// reclaim the same stale lock; the O_EXCL on recreate resolves it (the
	// loser's OpenFile fails with an exist error and this function returns that
	// error immediately — there is no re-read loop here, so the loser must
	// retry by re-invoking NewSQLiteStore, or the operator inspects the error).
	if rerr := os.Remove(lockPath); rerr != nil {
		return nil, fmt.Errorf("failed to remove stale lock file %s: %w", lockPath, rerr)
	}
	f, createErr2 := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if createErr2 != nil {
		return nil, fmt.Errorf("failed to recreate lock file %s after reclaim: %w", lockPath, createErr2)
	}
	if _, werr := f.WriteString(info.String()); werr != nil {
		f.Close()
		_ = os.Remove(lockPath)
		return nil, fmt.Errorf("failed to write lock file %s: %w", lockPath, werr)
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(lockPath)
		return nil, fmt.Errorf("failed to close lock file %s: %w", lockPath, cerr)
	}
	return buildRelease(lockPath), nil
}

// buildRelease returns a best-effort release function that removes the lock
// file. Removal errors are ignored: a stale lock is recoverable via the
// pid-liveness check on the next acquire.
func buildRelease(lockPath string) func() error {
	return func() error {
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
}
