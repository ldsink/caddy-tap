package tap

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const fileTimeFormat = "20060102T150405.000000000Z"

// Config reloads can briefly leave two workers writing for the same identity.
var activeFiles = struct {
	sync.Mutex
	paths map[string]bool
}{paths: make(map[string]bool)}

type fileSink struct {
	dir, prefix string
	sizeLimit   int64
	interval    time.Duration
	keep        int
	maxAge      time.Duration
	strict      bool
	logger      *zap.Logger

	file    *os.File
	opened  time.Time
	written int64
}

func (s *fileSink) write(now time.Time, record exchange) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	size := int64(len(data))
	if s.file != nil && s.written > 0 &&
		(size > s.sizeLimit-s.written || (s.interval > 0 && now.Sub(s.opened) >= s.interval)) {
		s.reportClose()
	}
	if s.file == nil {
		if err := s.open(now); err != nil {
			return err
		}
	}
	n, err := s.file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		rollback := s.file.Truncate(s.written)
		s.reportClose()
		return errors.Join(err, rollback)
	}
	s.written += int64(n)
	if s.strict {
		if err := s.file.Sync(); err != nil {
			s.reportClose()
			return err
		}
	}
	return nil
}

func (s *fileSink) open(now time.Time) error {
	activeFiles.Lock()
	defer activeFiles.Unlock()
	file, err := os.CreateTemp(s.dir, s.prefix+now.UTC().Format(fileTimeFormat)+"-*.ndjson")
	if err != nil {
		return err
	}
	if s.strict {
		dir, err := os.Open(s.dir)
		if err == nil {
			err = errors.Join(dir.Sync(), dir.Close())
		}
		if err != nil {
			return errors.Join(err, file.Close(), os.Remove(file.Name()))
		}
	}
	s.file = file
	s.opened = now
	s.written = 0
	activeFiles.paths[file.Name()] = true
	if err := s.prune(now); err != nil {
		s.logger.Warn("could not prune tap logs", zap.Error(err))
	}
	return nil
}

func (s *fileSink) reportClose() {
	if s.file == nil {
		return
	}
	activeFiles.Lock()
	err := s.file.Close()
	delete(activeFiles.paths, s.file.Name())
	s.file = nil
	activeFiles.Unlock()
	if err != nil {
		s.logger.Error("could not close tap log", zap.Error(err))
	}
}

// prune runs while holding activeFiles and relies on ReadDir's filename order.
func (s *fileSink) prune(now time.Time) error {
	if s.keep < 0 && s.maxAge < 0 {
		return nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	kept := 0
	var removeErr error
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		suffix, ok := strings.CutPrefix(entry.Name(), s.prefix)
		if !ok || !entry.Type().IsRegular() || !strings.HasSuffix(suffix, ".ndjson") ||
			len(suffix) <= len(fileTimeFormat)+1 || suffix[len(fileTimeFormat)] != '-' {
			continue
		}
		stamp, err := time.Parse(fileTimeFormat, suffix[:len(fileTimeFormat)])
		if err != nil {
			continue
		}
		token := strings.TrimSuffix(suffix[len(fileTimeFormat)+1:], ".ndjson")
		if token == "" || strings.Trim(token, "0123456789") != "" {
			continue
		}
		path := filepath.Join(s.dir, entry.Name())
		if activeFiles.paths[path] {
			continue
		}
		if (s.maxAge > 0 && now.Sub(stamp) > s.maxAge) || (s.keep >= 0 && kept >= s.keep) {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				removeErr = errors.Join(removeErr, err)
			}
		} else {
			kept++
		}
	}
	return removeErr
}
