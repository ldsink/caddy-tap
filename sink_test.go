package tap

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRotationBySizeAndTime(t *testing.T) {
	record := exchange{Identity: "api-v2", Response: responseRecord{bodyRecord: bodyRecord{Data: []byte("binary\x00\xff")}}}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name              string
		limit             int64
		interval, elapsed time.Duration
		wantFiles         int
	}{
		{"exact threshold", int64(2 * (len(data) + 1)), 0, 0, 1},
		{"size", int64(2*(len(data)+1) - 1), 0, 0, 2},
		{"time", 1 << 20, time.Hour, time.Hour, 2},
		{"oversize intact", 1, 0, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fileSink{dir: t.TempDir(), prefix: "api-v2-", sizeLimit: tc.limit, interval: tc.interval, keep: -1, maxAge: -1, logger: zap.NewNop()}
			t.Cleanup(func() { s.reportClose(time.Now()) })
			now := time.Date(2026, 11, 11, 20, 0, 0, 987654321, time.FixedZone("UTC+8", 8*60*60))
			if err := s.write(now, record); err != nil {
				t.Fatal(err)
			}
			if err := s.write(now.Add(tc.elapsed), record); err != nil {
				t.Fatal(err)
			}
			parts, err := filepath.Glob(filepath.Join(s.dir, "*.ndjson.part"))
			if err != nil || len(parts) != 1 || parts[0] != s.file.Name() {
				t.Fatalf("active parts %v, error %v", parts, err)
			}
			sealed, err := filepath.Glob(filepath.Join(s.dir, "*.ndjson"))
			if err != nil || len(sealed) != tc.wantFiles-1 {
				t.Fatalf("sealed files %v, error %v", sealed, err)
			}
			s.reportClose(now.Add(tc.elapsed))
			if _, err := os.Stat(parts[0]); !os.IsNotExist(err) {
				t.Fatalf("active part still exists after close: %v", err)
			}
			paths, err := filepath.Glob(filepath.Join(s.dir, "*.ndjson"))
			if err != nil || len(paths) != tc.wantFiles {
				t.Fatalf("paths %v, error %v", paths, err)
			}
			namePattern := regexp.MustCompile(`^api-v2-20261111T120000-[0-9]+\.ndjson$`)
			if tc.elapsed > 0 {
				namePattern = regexp.MustCompile(`^api-v2-20261111T1[23]0000-[0-9]+\.ndjson$`)
			}
			for _, path := range paths {
				st, err := os.Stat(path)
				if err != nil || (runtime.GOOS != "windows" && st.Mode().Perm()&0077 != 0) || !namePattern.MatchString(st.Name()) {
					t.Fatalf("stat %v, error %v", st, err)
				}
			}
			records, err := readExchanges(s.dir)
			if err != nil || len(records) != 2 || string(records[0].Response.Data) != "binary\x00\xff" {
				t.Fatalf("records %+v, error %v", records, err)
			}
		})
	}
}

func TestRetentionScopeAndRestart(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	filename := func(identity string, when time.Time) string {
		return identity + "-" + when.Format(fileTimeFormat) + "-123.ndjson"
	}
	expired := filename("api", now.Add(-48*time.Hour))
	excess := filename("api", now.Add(-2*time.Hour))
	latest := filename("api", now.Add(-time.Hour))
	other := filename("api-v2", now.Add(-48*time.Hour))
	legacy := "tap-api--20261108T120000.000000000Z-123.ndjson"
	for _, name := range []string{expired, excess, latest, other, legacy} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := "api-" + now.Add(-72*time.Hour).Format(fileTimeFormat) + "-notes.ndjson"
	if err := os.WriteFile(filepath.Join(dir, unrelated), nil, 0600); err != nil {
		t.Fatal(err)
	}
	s := fileSink{dir: dir, prefix: "api-", sizeLimit: 1, keep: 1, maxAge: 24 * time.Hour, logger: zap.NewNop()}
	t.Cleanup(func() { s.reportClose(time.Now()) })
	if err := s.write(now, exchange{Identity: "api"}); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.ndjson"))
	if err != nil || len(paths) != 4 {
		t.Fatalf("paths %v, error %v", paths, err)
	}
	for _, path := range paths {
		if filepath.Base(path) == expired || filepath.Base(path) == excess {
			t.Fatalf("kept expired or excess file %s", path)
		}
	}
}

func TestRetentionByAgeAlone(t *testing.T) {
	s := fileSink{dir: t.TempDir(), prefix: "api-", sizeLimit: 1, keep: -1, maxAge: time.Hour, logger: zap.NewNop()}
	t.Cleanup(func() { s.reportClose(time.Now()) })
	now := time.Date(2026, 11, 11, 12, 0, 0, 987654321, time.UTC)
	for _, stamp := range []time.Time{now.Add(-2 * time.Hour), now.Add(-time.Hour), now} {
		if err := s.write(stamp, exchange{}); err != nil {
			t.Fatal(err)
		}
	}
	s.reportClose(now)
	paths, err := filepath.Glob(filepath.Join(s.dir, "*.ndjson"))
	if err != nil || len(paths) != 2 {
		t.Fatalf("paths %v, error %v", paths, err)
	}
}

func TestRetentionWaitsForFirstSuccessfulRecord(t *testing.T) {
	dir := t.TempDir()
	s := fileSink{dir: dir, prefix: "api-", sizeLimit: 1 << 20, keep: 1, maxAge: time.Hour, strict: true, logger: zap.NewNop()}
	t.Cleanup(func() { s.reportClose(time.Now()) })
	now := time.Now()
	expired := filepath.Join(dir, s.prefix+now.Add(-2*time.Hour).UTC().Format(fileTimeFormat)+"-123.ndjson")
	if err := os.WriteFile(expired, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.open(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(expired); err != nil {
		t.Fatalf("old record disappeared before a replacement was written: %v", err)
	}
	path := s.file.Name()
	if err := s.file.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.file = readOnly
	if err := s.write(now, exchange{Timestamp: now}); err == nil || s.file != nil {
		t.Fatalf("failed write left open file %v, error %v", s.file, err)
	}
	if _, err := os.Stat(expired); err != nil {
		t.Fatalf("old record disappeared after a failed write: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("failed first write left an empty file: %v", err)
	}
	if err := s.write(now, exchange{Timestamp: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(expired); !os.IsNotExist(err) {
		t.Fatalf("old record still present after a successful write: %v", err)
	}
}

func TestFilePreservesNonUTF8Metadata(t *testing.T) {
	dir := t.TempDir()
	s := fileSink{dir: dir, prefix: "api-", sizeLimit: 1 << 20, keep: -1, maxAge: -1, logger: zap.NewNop()}
	t.Cleanup(func() { s.reportClose(time.Now()) })
	raw := string([]byte{0xff, 'x'})
	header := http.Header{"X-Raw": {"plain", raw}, "X-Text": {"中文"}}
	record := exchange{
		Timestamp: time.Now(),
		Request:   requestRecord{URL: "/?q=" + raw, RequestURI: "/?q=" + raw, Header: header, Trailer: header},
		Response: responseRecord{
			responseHead:  responseHead{Status: 200, Header: header},
			Trailer:       header,
			Informational: []responseHead{{Status: 103, Header: header}},
		},
	}
	if err := s.write(record.Timestamp, record); err != nil {
		t.Fatal(err)
	}
	s.reportClose(record.Timestamp)
	records, err := readExchanges(dir)
	if err != nil || len(records) != 1 {
		t.Fatalf("records %+v, error %v", records, err)
	}
	e := records[0]
	if string(e.Request.URLBase64) != record.Request.URL || string(e.Request.RequestURIBase64) != record.Request.RequestURI ||
		e.Request.Header.Get("X-Text") != "中文" || len(e.Response.Informational) != 1 {
		t.Fatalf("record %+v", e)
	}
	want := map[string][][]byte{"X-Raw": {[]byte("plain"), []byte(raw)}}
	for _, original := range []map[string][][]byte{
		e.Request.HeaderBase64, e.Request.TrailerBase64, e.Response.HeaderBase64,
		e.Response.TrailerBase64, e.Response.Informational[0].HeaderBase64,
	} {
		if !reflect.DeepEqual(original, want) {
			t.Fatalf("original header bytes %v, want %v", original, want)
		}
	}
	if record.Response.Informational[0].HeaderBase64 != nil {
		t.Fatal("encoding mutated the caller's informational response")
	}
}

func TestInvalidRecordDoesNotCreateFile(t *testing.T) {
	dir := t.TempDir()
	s := fileSink{dir: dir, prefix: "api-", sizeLimit: 1 << 20, keep: -1, maxAge: -1, logger: zap.NewNop()}
	invalidTime := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := s.write(time.Now(), exchange{Timestamp: invalidTime}); err == nil {
		t.Fatal("encoded a timestamp outside JSON's supported range")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("invalid record created a file: entries %v, error %v", entries, err)
	}
}

func TestFileSinkRecoversAfterMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logs")
	core, logs := observer.New(zap.WarnLevel)
	s := fileSink{dir: dir, prefix: "api-", sizeLimit: 1 << 20, keep: 1, maxAge: -1, logger: zap.New(core)}
	now := time.Now()
	if err := s.write(now, exchange{}); !errors.Is(err, os.ErrNotExist) || s.file != nil || s.written != 0 {
		t.Fatalf("write error %v, file %v, bytes %d", err, s.file, s.written)
	}
	if err := s.syncDir(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sync error %v", err)
	}
	s.recover(now)
	s.reportPrune(now)
	for _, message := range []string{"could not scan tap logs for recovery", "could not prune tap logs"} {
		if got := logs.FilterMessage(message).Len(); got != 1 {
			t.Errorf("logged %q %d times", message, got)
		}
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.reportClose(now) })
	if err := s.write(now, exchange{Identity: "api"}); err != nil {
		t.Fatal(err)
	}
	s.reportClose(now)
	records, err := readExchanges(dir)
	if err != nil || len(records) != 1 || records[0].Identity != "api" {
		t.Fatalf("records %+v, error %v", records, err)
	}
}

func TestRecoverPartMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.ndjson.part")
	file, size, err := recoverPart(path)
	if file != nil || size != 0 || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered file %v, size %d, error %v", file, size, err)
	}
}

func TestStrictFirstSyncFailureDiscardsNewFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "unsyncable")
	if err := os.Symlink(os.DevNull, path); err != nil {
		t.Skipf("cannot create sync failure fixture: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := file.Sync(); err == nil {
		t.Skip("null device supports sync")
	}
	s := fileSink{file: file, strict: true, logger: zap.NewNop()}
	if err := s.write(time.Now(), exchange{}); err == nil {
		t.Fatal("first record succeeded despite sync failure")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("failed file remains eligible for retention: %v", err)
	}
}

func TestRecoverySelectsNewestUsablePart(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().Truncate(time.Second)
	s := fileSink{dir: dir, prefix: "api-", sizeLimit: 1 << 20, keep: -1, maxAge: -1, logger: zap.NewNop()}
	part := func(prefix string, offset time.Duration, data string) string {
		t.Helper()
		stamp := now.Add(offset)
		path := filepath.Join(dir, prefix+stamp.UTC().Format(fileTimeFormat)+"-123.ndjson.part")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
		return path
	}
	older := part(s.prefix, -2*time.Hour, "{}\n")
	usable := part(s.prefix, -time.Hour, "{}\n{")
	corrupt := part(s.prefix, 0, "{}\n{\n")
	empty := part(s.prefix, time.Hour, "{")
	other := part("api-v2-", 0, "{}\n{")
	s.recover(now)
	t.Cleanup(func() { s.reportClose(now) })
	if s.file == nil || s.file.Name() != usable || s.written != 3 || !s.opened.Equal(now.Add(-time.Hour)) {
		t.Fatalf("recovered file %v, bytes %d, opened %v", s.file, s.written, s.opened)
	}
	for path, want := range map[string]string{
		strings.TrimSuffix(older, partSuffix): "{}\n",
		usable:                                "{}\n",
		corrupt:                               "{}\n{\n",
		other:                                 "{}\n{",
	} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != want {
			t.Fatalf("%s contains %q, want %q, error %v", path, got, want, err)
		}
	}
	for _, path := range []string{older, empty} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("unexpected part %s: %v", path, err)
		}
	}
	s.interval = time.Hour
	if err := s.write(now, exchange{}); err != nil {
		t.Fatal(err)
	}
	if s.file.Name() == usable {
		t.Fatal("expired recovered file was reopened for append")
	}
	if got, err := os.ReadFile(strings.TrimSuffix(usable, partSuffix)); err != nil || string(got) != "{}\n" {
		t.Fatalf("expired file contains %q, error %v", got, err)
	}
}

func TestFinalizationDoesNotOverwriteExistingLog(t *testing.T) {
	now := time.Now()
	s := fileSink{dir: t.TempDir(), prefix: "api-", sizeLimit: 1 << 20, keep: -1, maxAge: -1, logger: zap.NewNop()}
	if err := s.write(now, exchange{}); err != nil {
		t.Fatal(err)
	}
	part := s.file.Name()
	final := strings.TrimSuffix(part, partSuffix)
	if err := os.WriteFile(final, []byte("previous\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.reportClose(now)
	s.recover(now)
	if s.file != nil {
		t.Fatal("recovered a part whose final name is in use")
	}
	if data, err := os.ReadFile(final); err != nil || string(data) != "previous\n" {
		t.Fatalf("existing final contains %q, error %v", data, err)
	}
	if data, err := os.ReadFile(part); err != nil || !json.Valid(data) {
		t.Fatalf("part contains %q, error %v", data, err)
	}
}

func TestRetentionKeepsLatestWriteWithinSameSecond(t *testing.T) {
	s := fileSink{dir: t.TempDir(), prefix: "api-", keep: 1, maxAge: -1}
	now := time.Now().UTC().Truncate(time.Second)
	stem := s.prefix + now.Format(fileTimeFormat)
	older := filepath.Join(s.dir, stem+"-999.ndjson")
	newer := filepath.Join(s.dir, stem+"-111.ndjson")
	for i, path := range []string{older, newer} {
		if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
			t.Fatal(err)
		}
		stamp := now.Add(time.Duration(i) * 500 * time.Millisecond)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.prune(now); err != nil {
		t.Fatal(err)
	}
	if got := oneLogFile(t, s.dir, "*.ndjson"); got != newer {
		t.Fatalf("retained %q, want %q", got, newer)
	}
}

func TestRetentionBreaksTimestampTieByName(t *testing.T) {
	s := fileSink{dir: t.TempDir(), prefix: "api-", keep: 1, maxAge: -1}
	now := time.Now().UTC().Truncate(time.Second)
	stem := s.prefix + now.Format(fileTimeFormat)
	for _, suffix := range []string{"-111.ndjson", "-999.ndjson"} {
		path := filepath.Join(s.dir, stem+suffix)
		if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, now, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.prune(now); err != nil {
		t.Fatal(err)
	}
	if got, want := oneLogFile(t, s.dir, "*.ndjson"), filepath.Join(s.dir, stem+"-999.ndjson"); got != want {
		t.Fatalf("retained %q, want %q", got, want)
	}
}

func TestRecoveryRejectsNonUTF8Record(t *testing.T) {
	part := filepath.Join(t.TempDir(), "api.ndjson.part")
	original := []byte("{}\n{\"value\":\"\xff\"}\n{")
	if err := os.WriteFile(part, original, 0600); err != nil {
		t.Fatal(err)
	}
	file, _, err := recoverPart(part)
	if file != nil {
		_ = file.Close()
	}
	if err == nil {
		t.Fatal("recovery accepted a record with invalid UTF-8")
	}
	got, err := os.ReadFile(part)
	if err != nil || string(got) != string(original) {
		t.Fatalf("damaged part contains %q, error %v", got, err)
	}
}

func TestStrictWriteSyncsAndSealsFile(t *testing.T) {
	s := fileSink{dir: t.TempDir(), prefix: "api-", sizeLimit: 1 << 20, strict: true, keep: -1, maxAge: -1, logger: zap.NewNop()}
	now := time.Now()
	t.Cleanup(func() { s.reportClose(now) })
	if err := s.write(now, exchange{Identity: "api"}); err != nil {
		t.Fatal(err)
	}
	if s.syncedDir != (runtime.GOOS != "windows") {
		t.Fatalf("directory sync state was %t on %s", s.syncedDir, runtime.GOOS)
	}
	s.reportClose(now)
	records, err := readExchanges(s.dir)
	if err != nil || len(records) != 1 || records[0].Identity != "api" {
		t.Fatalf("strict records %+v, error %v", records, err)
	}
}
