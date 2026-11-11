package tap

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
	data, err := encodeExchange(record)
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
	firstRecord := s.written == 0
	n, err := s.file.Write(data)
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
	if firstRecord {
		activeFiles.Lock()
		err := s.prune(now)
		activeFiles.Unlock()
		if err != nil {
			s.logger.Warn("could not prune tap logs", zap.Error(err))
		}
	}
	return nil
}

func encodeExchange(record exchange) ([]byte, error) {
	req, resp := &record.Request, &record.Response
	if !utf8.ValidString(req.URL) {
		req.URLBase64 = []byte(req.URL)
	}
	if !utf8.ValidString(req.RequestURI) {
		req.RequestURIBase64 = []byte(req.RequestURI)
	}
	req.HeaderBase64 = originalHeaderBytes(req.Header)
	req.TrailerBase64 = originalHeaderBytes(req.Trailer)
	resp.HeaderBase64 = originalHeaderBytes(resp.Header)
	resp.TrailerBase64 = originalHeaderBytes(resp.Trailer)
	resp.Informational = slices.Clone(resp.Informational)
	for i := range resp.Informational {
		head := &resp.Informational[i]
		head.HeaderBase64 = originalHeaderBytes(head.Header)
	}
	return json.Marshal(record)
}

// JSON replaces invalid UTF-8 in strings. Save all values for any affected key.
func originalHeaderBytes(header http.Header) map[string][][]byte {
	var original map[string][][]byte
	for key, values := range header {
		for _, value := range values {
			if utf8.ValidString(value) {
				continue
			}
			if original == nil {
				original = make(map[string][][]byte)
			}
			raw := make([][]byte, len(values))
			for i, value := range values {
				raw[i] = []byte(value)
			}
			original[key] = raw
			break
		}
	}
	return original
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
	return nil
}

func (s *fileSink) reportClose() {
	if s.file == nil {
		return
	}
	activeFiles.Lock()
	err := s.file.Close()
	if s.written == 0 {
		if removeErr := os.Remove(s.file.Name()); !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}
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
