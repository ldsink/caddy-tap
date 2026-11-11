package tap

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestRotationBySizeAndTime(t *testing.T) {
	record := exchange{Identity: "api", Response: responseRecord{bodyRecord: bodyRecord{Data: []byte("binary\x00\xff")}}}
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
			s := fileSink{dir: t.TempDir(), prefix: "tap-api--", sizeLimit: tc.limit, interval: tc.interval, keep: -1, maxAge: -1, logger: zap.NewNop()}
			t.Cleanup(s.reportClose)
			now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
			if err := s.write(now, record); err != nil {
				t.Fatal(err)
			}
			if err := s.write(now.Add(tc.elapsed), record); err != nil {
				t.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(s.dir, "*.ndjson"))
			if err != nil || len(paths) != tc.wantFiles {
				t.Fatalf("paths %v, error %v", paths, err)
			}
			for _, path := range paths {
				st, err := os.Stat(path)
				if err != nil || st.Mode().Perm()&0077 != 0 || !strings.HasPrefix(st.Name(), "tap-api--20261111T") {
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
		return "tap-" + identity + "--" + when.Format(fileTimeFormat) + "-123.ndjson"
	}
	expired := filename("api", now.Add(-48*time.Hour))
	excess := filename("api", now.Add(-2*time.Hour))
	latest := filename("api", now.Add(-time.Hour))
	other := filename("api2", now.Add(-48*time.Hour))
	for _, name := range []string{expired, excess, latest, other} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := "tap-api--" + now.Add(-72*time.Hour).Format(fileTimeFormat) + "-notes.ndjson"
	if err := os.WriteFile(filepath.Join(dir, unrelated), nil, 0600); err != nil {
		t.Fatal(err)
	}
	s := fileSink{dir: dir, prefix: "tap-api--", sizeLimit: 1, keep: 1, maxAge: 24 * time.Hour, logger: zap.NewNop()}
	t.Cleanup(s.reportClose)
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
	s := fileSink{dir: t.TempDir(), prefix: "tap-api--", sizeLimit: 1, keep: -1, maxAge: time.Hour, logger: zap.NewNop()}
	t.Cleanup(s.reportClose)
	now := time.Now()
	for _, stamp := range []time.Time{now.Add(-2 * time.Hour), now.Add(-time.Hour), now} {
		if err := s.write(stamp, exchange{}); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := filepath.Glob(filepath.Join(s.dir, "*.ndjson"))
	if err != nil || len(paths) != 2 {
		t.Fatalf("paths %v, error %v", paths, err)
	}
}

func TestRetentionWaitsForFirstSuccessfulRecord(t *testing.T) {
	dir := t.TempDir()
	s := fileSink{dir: dir, prefix: "tap-api--", sizeLimit: 1 << 20, keep: 1, maxAge: time.Hour, strict: true, logger: zap.NewNop()}
	t.Cleanup(s.reportClose)
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
	s := fileSink{dir: dir, prefix: "tap-api--", sizeLimit: 1 << 20, keep: -1, maxAge: -1, logger: zap.NewNop()}
	t.Cleanup(s.reportClose)
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
	s := fileSink{dir: dir, prefix: "tap-api--", sizeLimit: 1 << 20, keep: -1, maxAge: -1, logger: zap.NewNop()}
	invalidTime := time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := s.write(time.Now(), exchange{Timestamp: invalidTime}); err == nil {
		t.Fatal("encoded a timestamp outside JSON's supported range")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("invalid record created a file: entries %v, error %v", entries, err)
	}
}
