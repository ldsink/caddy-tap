package tap

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"
)

const (
	fileTimeFormat = "20060102T150405"
	partSuffix     = ".part"
	logSuffix      = ".ndjson"
)

type fileSink struct {
	dir, prefix string
	sizeLimit   int64
	interval    time.Duration
	keep        int
	maxAge      time.Duration
	strict      bool
	logger      *zap.Logger

	file      *os.File
	opened    time.Time
	written   int64
	syncedDir bool
}

func (s *fileSink) write(now time.Time, record exchange, omitFields ...string) error {
	data, err := encodeExchange(record)
	if err != nil {
		return err
	}
	if len(omitFields) > 0 {
		data, err = omitJSONFields(data, omitFields)
		if err != nil {
			return fmt.Errorf("omit_fields: %w", err)
		}
	}
	data = append(data, '\n')
	size := int64(len(data))
	if s.file != nil && s.written > 0 &&
		(size > s.sizeLimit-s.written || (s.interval > 0 && now.Sub(s.opened) >= s.interval)) {
		s.reportClose(now)
	}
	if s.file == nil {
		if err := s.open(now); err != nil {
			return err
		}
	}
	firstRecord := s.written == 0
	n, err := s.file.Write(data)
	if err == nil && s.strict {
		err = s.file.Sync()
		// Windows cannot flush the read-only directory handle used by syncDir.
		if err == nil && !s.syncedDir && runtime.GOOS != "windows" {
			err = s.syncDir()
			s.syncedDir = err == nil
		}
	}
	if err != nil {
		rollback := s.file.Truncate(s.written)
		s.finish(now, rollback == nil)
		return errors.Join(err, rollback)
	}
	s.written += int64(n)
	if firstRecord {
		s.reportPrune(now)
	}
	return nil
}

func omitJSONFields(data []byte, paths []string) ([]byte, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	children := make(map[string][]string)
	for _, path := range paths {
		key, rest, nested := strings.Cut(path, ".")
		if !nested {
			delete(object, key)
			continue
		}
		children[key] = append(children[key], rest)
	}
	for key, paths := range children {
		value := object[key]
		if len(value) == 0 || value[0] != '{' {
			continue
		}
		filtered, err := omitJSONFields(value, paths)
		if err != nil {
			return nil, err
		}
		object[key] = filtered
	}
	return json.Marshal(object)
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
	for {
		file, err := os.CreateTemp(s.dir, s.prefix+now.UTC().Format(fileTimeFormat)+"-*"+logSuffix+partSuffix)
		if err != nil {
			return err
		}
		err = checkFinalName(file.Name())
		if err == nil {
			s.file = file
			s.opened = now
			s.written = 0
			s.syncedDir = false
			return nil
		}
		cleanup := errors.Join(file.Close(), os.Remove(file.Name()))
		if cleanup != nil || !errors.Is(err, os.ErrExist) {
			return errors.Join(err, cleanup)
		}
	}
}

func checkFinalName(part string) error {
	final := strings.TrimSuffix(part, partSuffix)
	_, err := os.Lstat(final)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	default:
		return &os.PathError{Op: "publish tap log", Path: final, Err: os.ErrExist}
	}
}

func (s *fileSink) syncDir() error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (s *fileSink) reportClose(now time.Time) {
	s.finish(now, true)
}

func (s *fileSink) finish(now time.Time, publish bool) {
	if s.file == nil {
		return
	}
	file := s.file
	s.file = nil
	var err error
	if s.written > 0 && publish {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	path := file.Name()
	if s.written == 0 {
		if removeErr := os.Remove(path); !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	} else if publish && err == nil {
		if err = checkFinalName(path); err == nil {
			err = os.Rename(path, strings.TrimSuffix(path, partSuffix))
		}
		if err == nil {
			if s.syncedDir {
				err = s.syncDir()
			}
			s.reportPrune(now)
		}
	}
	if err != nil {
		s.logger.Error("could not finalize tap log", zap.String("path", path), zap.Error(err))
	}
}

func (s *fileSink) recover(now time.Time) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		s.logger.Error("could not scan tap logs for recovery", zap.Error(err))
		return
	}
	type part struct {
		name             string
		opened, modified time.Time
	}
	var parts []part
	for _, entry := range entries {
		stamp, ok := s.fileStamp(entry, logSuffix+partSuffix)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			s.logger.Warn("could not inspect tap log part", zap.String("name", entry.Name()), zap.Error(err))
			continue
		}
		parts = append(parts, part{entry.Name(), stamp, info.ModTime()})
	}
	// ReadDir orders by filename, which breaks ties between equal modification times.
	slices.SortStableFunc(parts, func(a, b part) int { return b.modified.Compare(a.modified) })
	for _, part := range parts {
		path := filepath.Join(s.dir, part.name)
		file, size, err := recoverPart(path)
		if err != nil {
			s.logger.Warn("could not recover tap log part", zap.String("path", path), zap.Error(err))
			continue
		}
		if size > 0 && s.file == nil {
			s.file, s.written, s.opened = file, size, part.opened
			continue
		}
		older := *s
		older.file, older.written = file, size
		older.reportClose(now)
	}
}

func recoverPart(path string) (*os.File, int64, error) {
	if err := checkFinalName(path); err != nil {
		return nil, 0, err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, 0, err
	}
	reader := bufio.NewReader(file)
	var size int64
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(line) != 0 {
				err = file.Truncate(size)
			} else {
				err = nil
			}
			if err == nil {
				if _, err = file.Seek(size, io.SeekStart); err == nil {
					return file, size, nil
				}
			}
		}
		if err != nil {
			return nil, 0, errors.Join(err, file.Close())
		}
		record := bytes.TrimSpace(line)
		if len(record) == 0 || record[0] != '{' || !utf8.Valid(record) || !json.Valid(record) {
			return nil, 0, errors.Join(fmt.Errorf("invalid NDJSON record at offset %d", size), file.Close())
		}
		size += int64(len(line))
	}
}

func (s *fileSink) fileStamp(entry os.DirEntry, extension string) (time.Time, bool) {
	suffix, matchesIdentity := strings.CutPrefix(entry.Name(), s.prefix)
	stem, matchesExtension := strings.CutSuffix(suffix, extension)
	if !matchesIdentity || !matchesExtension || !entry.Type().IsRegular() ||
		len(stem) <= len(fileTimeFormat)+1 || stem[len(fileTimeFormat)] != '-' ||
		strings.Trim(stem[len(fileTimeFormat)+1:], "0123456789") != "" {
		return time.Time{}, false
	}
	stamp, err := time.Parse(fileTimeFormat, stem[:len(fileTimeFormat)])
	return stamp, err == nil
}

func (s *fileSink) reportPrune(now time.Time) {
	if err := s.prune(now); err != nil {
		s.logger.Warn("could not prune tap logs", zap.Error(err))
	}
}

func (s *fileSink) prune(now time.Time) error {
	if s.keep < 0 && s.maxAge < 0 {
		return nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	type logFile struct {
		name            string
		stamp, modified time.Time
	}
	var files []logFile
	var removeErr error
	remove := func(name string) {
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	for _, entry := range entries {
		stamp, ok := s.fileStamp(entry, logSuffix)
		if !ok {
			continue
		}
		if s.maxAge > 0 && now.Truncate(time.Second).Sub(stamp) > s.maxAge {
			remove(entry.Name())
			continue
		}
		if s.keep < 0 {
			continue
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.Join(removeErr, err)
		}
		files = append(files, logFile{entry.Name(), stamp, info.ModTime()})
	}
	slices.SortFunc(files, func(a, b logFile) int {
		if order := b.stamp.Compare(a.stamp); order != 0 {
			return order
		}
		if order := b.modified.Compare(a.modified); order != 0 {
			return order
		}
		return cmp.Compare(b.name, a.name)
	})
	if s.keep >= 0 && len(files) > s.keep {
		for _, file := range files[s.keep:] {
			remove(file.name)
		}
	}
	return removeErr
}
