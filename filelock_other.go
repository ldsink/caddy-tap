//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows

package tap

import (
	"errors"
	"os"
)

func tryLock(*os.File) error {
	return errors.New("tap log file locking is unsupported on this platform")
}
