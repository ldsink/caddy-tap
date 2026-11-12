package tap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func provision(t *testing.T, tap *Tap) {
	t.Helper()
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)
	if err := tap.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tap.Cleanup(); err != nil {
			t.Error(err)
		}
	})
}

func readExchanges(dir string) ([]exchange, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.ndjson"))
	if err != nil {
		return nil, err
	}
	var records []exchange
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		for {
			var record exchange
			if err := decoder.Decode(&record); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}
			records = append(records, record)
		}
	}
	return records, nil
}

func TestDropModeCapacityAndCleanup(t *testing.T) {
	dir := t.TempDir()
	tap := &Tap{Identity: "api", Directory: dir, QueueSize: 1}
	provision(t, tap)
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		returned <- tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/record", nil),
			caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
				close(entered)
				<-release
				_, err := io.WriteString(w, "first")
				return err
			}))
	}()
	<-entered
	w := httptest.NewRecorder()
	if err := tap.ServeHTTP(w, httptest.NewRequest("GET", "/dropped", nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
			_, err := io.WriteString(w, "still served")
			return err
		})); err != nil || w.Body.String() != "still served" || tap.runtime.dropped.Load() != 1 {
		t.Errorf("dropped exchange response %q, count %d, error %v", w.Body, tap.runtime.dropped.Load(), err)
	}
	close(release)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	if err := tap.Cleanup(); err != nil {
		t.Fatal(err)
	}
	records, err := readExchanges(dir)
	if err != nil || len(records) != 1 || records[0].Request.URL != "/record" || string(records[0].Response.Data) != "first" {
		t.Fatalf("records %+v, error %v", records, err)
	}
}

func TestOmitFieldsAffectsOnlyPersistedLog(t *testing.T) {
	for _, mode := range []string{"drop", "strict"} {
		t.Run(mode, func(t *testing.T) {
			var tap Tap
			if err := json.Unmarshal([]byte(`{"identity":"api","omit_fields":["identity","request.header","response.header.Set-Cookie","response.body_base64"]}`), &tap); err != nil {
				t.Fatal(err)
			}
			tap.Directory, tap.Mode = t.TempDir(), mode
			provision(t, &tap)
			r := httptest.NewRequest(http.MethodGet, "/capture", nil)
			r.Header.Set("Authorization", "Bearer fixture")
			w := httptest.NewRecorder()
			err := tap.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
				if got := r.Header.Get("Authorization"); got != "Bearer fixture" {
					t.Errorf("downstream authorization = %q", got)
				}
				w.Header().Add("Set-Cookie", "first=1")
				w.Header().Add("Set-Cookie", "second=2")
				w.Header().Set("X-Keep", "visible")
				_, err := io.WriteString(w, "response")
				return err
			}))
			if err != nil || w.Code != http.StatusOK || w.Body.String() != "response" ||
				strings.Join(w.Result().Header.Values("Set-Cookie"), ",") != "first=1,second=2" {
				t.Fatalf("response status %d, headers %v, body %q, error %v", w.Code, w.Header(), w.Body, err)
			}
			if err := tap.Cleanup(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(oneLogFile(t, tap.Directory, "api-*.ndjson"))
			if err != nil {
				t.Fatal(err)
			}
			var record map[string]json.RawMessage
			if err := json.Unmarshal(data, &record); err != nil {
				t.Fatal(err)
			}
			var request, response map[string]json.RawMessage
			if err := json.Unmarshal(record["request"], &request); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(record["response"], &response); err != nil {
				t.Fatal(err)
			}
			if record["identity"] != nil || request["header"] != nil || response["body_base64"] != nil ||
				string(response["header"]) != `{"X-Keep":["visible"]}` || string(response["body_bytes"]) != "8" {
				t.Fatalf("unexpected log record: %s", data)
			}
		})
	}
}

func TestStrictModeWaitsForPersistence(t *testing.T) {
	tap := &Tap{Identity: "api", Mode: "strict", runtime: &tapRuntime{
		slots: make(chan struct{}, 1), queue: make(chan queuedRecord, 1), logger: zap.NewNop(),
	}}
	finished := make(chan error, 1)
	body := bytes.Repeat([]byte{0xff}, (1<<20)+1)
	go func() {
		finished <- tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", bytes.NewReader(body)),
			caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
				_, err := io.Copy(w, r.Body)
				return err
			}))
	}()
	select {
	case item := <-tap.runtime.queue:
		if !bytes.Equal(item.record.Request.Data, body) || !bytes.Equal(item.record.Response.Data, body) ||
			item.record.Request.Truncated || item.record.Response.Truncated {
			t.Errorf("strict capture request=%d response=%d", len(item.record.Request.Data), len(item.record.Response.Data))
		}
		select {
		case err := <-finished:
			t.Fatalf("returned before the write result: %v", err)
		default:
		}
		<-tap.runtime.slots
		item.result <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for strict exchange")
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestStrictPersistenceFailureBeforeCommit(t *testing.T) {
	dir := t.TempDir()
	tap := &Tap{Identity: "api", Directory: filepath.Join(dir, "removed"), Mode: "strict"}
	provision(t, tap)
	if err := os.Remove(tap.Directory); err != nil {
		t.Fatal(err)
	}
	err := tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil),
		caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error { return nil }))
	var httpErr caddyhttp.HandlerError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 500 {
		t.Fatalf("got error %v, want Caddy HTTP 500", err)
	}
}

func TestConcurrentPathsShareWriter(t *testing.T) {
	dir := t.TempDir()
	first := &Tap{Identity: "api", Directory: dir, Mode: "strict", QueueSize: 4, OmitFields: []string{"identity", "request.header"}}
	second := &Tap{Identity: "api", Directory: dir, Mode: "strict", QueueSize: 4}
	provision(t, first)
	provision(t, second)
	if first.runtime != second.runtime {
		t.Fatal("paths use different writers")
	}
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() { captureTestPath(t, first, "/first") })
		wg.Go(func() { captureTestPath(t, second, "/second") })
	}
	wg.Wait()
	wg.Go(func() { _ = first.Cleanup() })
	wg.Go(func() { _ = first.Cleanup() })
	wg.Wait()
	captureTestPath(t, second, "/second")
	part := oneLogFile(t, dir, "*.ndjson.part")
	if paths, err := filepath.Glob(filepath.Join(dir, "*.ndjson")); err != nil || len(paths) != 0 {
		t.Fatalf("a path cleanup published the shared file early: %v, %v", paths, err)
	}
	if err := second.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if got := oneLogFile(t, dir, "*.ndjson"); got+partSuffix != part {
		t.Fatalf("published %q, want %q", got, strings.TrimSuffix(part, partSuffix))
	}
	records, err := readExchanges(dir)
	counts := make(map[string]int)
	for _, record := range records {
		if string(record.Response.Data) != "done" {
			t.Fatalf("incomplete record: %+v", record)
		}
		switch record.Request.URL {
		case "/first":
			if record.Identity != "" || record.Request.Header != nil {
				t.Fatalf("fields were retained: %+v", record)
			}
		case "/second":
			if record.Identity != "api" || record.Request.Header == nil {
				t.Fatalf("fields were omitted: %+v", record)
			}
		}
		counts[record.Request.URL]++
	}
	if err != nil || len(counts) != 2 || counts["/first"] != 24 || counts["/second"] != 25 {
		t.Fatalf("records by path %v, error %v", counts, err)
	}
}

func captureTestPath(t *testing.T, tap *Tap, path string) {
	t.Helper()
	err := tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
			_, err := io.WriteString(w, "done")
			return err
		}))
	if err != nil {
		t.Error(err)
	}
}

func oneLogFile(t *testing.T, dir, pattern string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil || len(paths) != 1 {
		t.Fatalf("want one %s file, got %v, error %v", pattern, paths, err)
	}
	return paths[0]
}

func TestStrictAdmissionHonorsCancellation(t *testing.T) {
	for _, tc := range []struct {
		name string
		busy bool
	}{
		{"available", false},
		{"busy", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tap := &Tap{Identity: "api", Directory: t.TempDir(), Mode: "strict", QueueSize: 1}
			provision(t, tap)
			if tc.busy {
				tap.runtime.slots <- struct{}{}
				defer func() { <-tap.runtime.slots }()
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			returned := make(chan error, 1)
			go func() {
				returned <- tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodPost, "/", nil),
					caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error {
						t.Error("a canceled request reached the downstream handler")
						return nil
					}))
			}()
			select {
			case err := <-returned:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("admission returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled request stuck waiting for a slot")
			}
		})
	}
}

func TestRepeatedNamedRouteDoesNotWaitOnItself(t *testing.T) {
	dir := t.TempDir()
	tap := &Tap{Identity: "api", Directory: dir, Mode: "strict", QueueSize: 1}
	provision(t, tap)
	nested := &Tap{Identity: "api", Directory: dir, Mode: "strict", QueueSize: 1}
	provision(t, nested)
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), caddyhttp.VarsCtxKey, make(map[string]any)), time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	err := tap.ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
			return nested.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
				_, err := io.WriteString(w, "done")
				return err
			}))
		}))
	if err := tap.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := nested.Cleanup(); err != nil {
		t.Fatal(err)
	}
	records, readErr := readExchanges(dir)
	if err != nil || readErr != nil || len(records) != 1 || w.Body.String() != "done" || string(records[0].Response.Data) != "done" {
		t.Fatalf("response %q, records %+v, errors %v and %v", w.Body.String(), records, err, readErr)
	}
}

func TestProvisionDefaultsAndInvalidDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "logs")
	tap := &Tap{Identity: "default", Directory: dir}
	provision(t, tap)
	if tap.QueueSize != 64 || tap.MaxBodyBytes != 1<<20 || tap.RollSize != 100<<20 ||
		tap.RollInterval != 0 || tap.RollKeep != 10 || time.Duration(tap.RollKeepFor) != 90*24*time.Hour {
		t.Fatalf("defaults %+v", tap)
	}
	info, err := os.Stat(dir)
	if err != nil || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		t.Fatalf("log directory stat %v, error %v", info, err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	invalid := &Tap{Identity: "valid", Directory: file}
	if err := invalid.Provision(caddy.Context{}); err == nil {
		t.Fatal("provisioning a regular file as the output directory succeeded")
	}
	if err := invalid.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := new(Tap).Provision(caddy.Context{}); err == nil {
		t.Fatal("provisioning an invalid configuration succeeded")
	}
}

func TestShutdownDrainsQueueAndLeavesLateResponsesAlone(t *testing.T) {
	dir := t.TempDir()
	tap := &Tap{Identity: "api", Directory: dir, QueueSize: 2}
	provision(t, tap)
	next := caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		_, err := io.WriteString(w, "ok")
		return err
	})
	if err := tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/queued", nil), next); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	err := tap.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/active", nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
			drainWorkers(context.Background())
			return next.ServeHTTP(w, r)
		}))
	if err != nil || w.Body.String() != "ok" {
		t.Fatalf("late response %q, error %v", w.Body.String(), err)
	}
	if err := tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/after", nil), next); err != nil {
		t.Fatal(err)
	}
	records, err := readExchanges(dir)
	if err != nil || len(records) != 1 || records[0].Request.URL != "/queued" {
		t.Fatalf("records %+v, error %v", records, err)
	}
}

func TestDropWarningDuringAnActiveExchange(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	warned := make(chan struct{}, 1)
	logger := zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Level == zap.WarnLevel {
			warned <- struct{}{}
		}
		return nil
	}))
	rt := &tapRuntime{
		slots: make(chan struct{}, 1), queue: make(chan queuedRecord, 1),
		dropNotice: make(chan struct{}, 1), done: make(chan struct{}), logger: logger,
	}
	rt.slots <- struct{}{}
	go rt.writeRecords(&fileSink{logger: logger}, "")
	tap := &Tap{Identity: "api", runtime: rt}
	t.Cleanup(func() { _ = tap.Cleanup() })
	w := httptest.NewRecorder()
	err := tap.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
			_, err := io.WriteString(w, "ok")
			return err
		}))
	if err != nil || w.Body.String() != "ok" {
		t.Fatalf("response %q, error %v", w.Body.String(), err)
	}
	select {
	case <-warned:
		entries := logs.All()
		if len(entries) != 1 || entries[0].ContextMap()["dropped_total"] != uint64(1) {
			t.Fatalf("warnings %+v", entries)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not report a drop while its only slot was occupied")
	}
	<-rt.slots
}

func TestStrictPersistenceFailurePreservesCommittedResponse(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "removed")
	tap := &Tap{Identity: "api", Directory: dir, Mode: "strict"}
	provision(t, tap)
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	err := tap.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
			w.WriteHeader(http.StatusAccepted)
			_, err := io.WriteString(w, "committed")
			return err
		}))
	if err != nil || w.Code != http.StatusAccepted || w.Body.String() != "committed" {
		t.Fatalf("status %d, response %q, error %v", w.Code, w.Body.String(), err)
	}
}

func TestRecoveryAfterProcessExit(t *testing.T) {
	const childDir = "CADDY_TAP_TEST_CRASH_DIR"
	if dir := os.Getenv(childDir); dir != "" {
		tap := &Tap{Identity: "api", Directory: dir, Mode: "strict", OmitFields: []string{"identity"}}
		provision(t, tap)
		captureTestPath(t, tap, "/before-restart")
		path := oneLogFile(t, dir, "*.ndjson.part")
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(`{"request":`); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if t.Failed() {
			os.Exit(1)
		}
		os.Exit(0) // Leave the writer and its lock open until the process exits.
	}

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryAfterProcessExit$")
	cmd.Env = append(os.Environ(), childDir+"="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash fixture: %v, %s", err, out)
	}
	part := oneLogFile(t, dir, "*.ndjson.part")
	before, err := os.ReadFile(part)
	if err != nil || !bytes.HasSuffix(before, []byte(`{"request":`)) {
		t.Fatalf("missing torn tail: %v", err)
	}
	validation := &Tap{Identity: "api", Directory: dir}
	provision(t, validation)
	if err := validation.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if unchanged, err := os.ReadFile(part); err != nil || !bytes.Equal(unchanged, before) {
		t.Fatalf("config validation touched the part: %v", err)
	}
	first := &Tap{Identity: "api", Directory: dir, Mode: "strict"}
	second := &Tap{Identity: "api", Directory: dir, Mode: "strict"}
	provision(t, first)
	provision(t, second)
	var wg sync.WaitGroup
	wg.Go(func() { captureTestPath(t, first, "/first-after-restart") })
	wg.Go(func() { captureTestPath(t, second, "/second-after-restart") })
	wg.Wait()
	if got := oneLogFile(t, dir, "*.ndjson.part"); got != part {
		t.Fatalf("resumed %q, want %q", got, part)
	}
	_ = first.Cleanup()
	_ = second.Cleanup()
	oneLogFile(t, dir, "*.ndjson")
	records, err := readExchanges(dir)
	if err != nil || len(records) != 3 || records[0].Request.URL != "/before-restart" ||
		records[0].Identity != "" || records[1].Identity != "api" || records[2].Identity != "api" {
		t.Fatalf("recovered records %+v, error %v", records, err)
	}
	urls := map[string]bool{}
	for _, record := range records[1:] {
		urls[record.Request.URL] = true
	}
	if !urls["/first-after-restart"] || !urls["/second-after-restart"] {
		t.Fatalf("missing path in recovered log: %v", urls)
	}
}

func TestSharedWriterRejectsConflictingSettings(t *testing.T) {
	dir := t.TempDir()
	tap := &Tap{Identity: "api", Directory: dir, QueueSize: 2}
	provision(t, tap)
	conflict := &Tap{Identity: "api", Directory: dir, QueueSize: 3}
	if err := conflict.Provision(caddy.Context{}); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("conflicting settings: %v", err)
	}
	if conflict.runtime != nil || tap.runtime.refs != 1 {
		t.Fatal("failed provision changed the writer group")
	}
}

func TestProvisionWaitsForClosingWriter(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		// Keep the old writer registered while it finishes closing its files.
		lockPath := filepath.Join(dir, ".tap-api.lock")
		old := &tapRuntime{closed: true, done: make(chan struct{})}
		writerGroups.Lock()
		writerGroups.byPath[lockPath] = old
		writerGroups.Unlock()

		tap := &Tap{Identity: "api", Directory: dir}
		ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- tap.Provision(ctx) }()
		synctest.Wait()
		returnedEarly := len(result) != 0

		writerGroups.Lock()
		if writerGroups.byPath[lockPath] == old {
			delete(writerGroups.byPath, lockPath)
		}
		writerGroups.Unlock()
		close(old.done)

		if err := <-result; err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := tap.Cleanup(); err != nil {
				t.Error(err)
			}
		})
		if returnedEarly || tap.runtime == old {
			t.Fatal("provision returned before the old writer finished")
		}
	})
}

func TestIdleRotationPublishesPart(t *testing.T) {
	dir := t.TempDir()
	tap := &Tap{Identity: "api", Directory: dir, Mode: "strict", RollInterval: caddy.Duration(50 * time.Millisecond)}
	provision(t, tap)
	captureTestPath(t, tap, "/idle")
	for deadline := time.Now().Add(3 * time.Second); ; {
		paths, err := filepath.Glob(filepath.Join(dir, "*.ndjson"))
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle writer did not publish its file")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if parts, err := filepath.Glob(filepath.Join(dir, "*.ndjson.part")); err != nil || len(parts) != 0 {
		t.Fatalf("idle writer left parts %v, error %v", parts, err)
	}
}

func TestWriterLockContentionLeavesPartUntouched(t *testing.T) {
	dir := t.TempDir()
	lock, err := lockWriter(filepath.Join(dir, ".tap-api.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	part := filepath.Join(dir, "api-"+time.Now().UTC().Format(fileTimeFormat)+"-123.ndjson.part")
	if err := os.WriteFile(part, []byte("{}\n{"), 0600); err != nil {
		t.Fatal(err)
	}
	tap := &Tap{Identity: "api", Directory: dir, Mode: "strict"}
	provision(t, tap)
	err = tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/blocked", nil),
		caddyhttp.HandlerFunc(func(http.ResponseWriter, *http.Request) error { return nil }))
	var httpErr caddyhttp.HandlerError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("got %v, want Caddy HTTP 500", err)
	}
	if data, err := os.ReadFile(part); err != nil || string(data) != "{}\n{" {
		t.Fatalf("locked part contains %q, error %v", data, err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	captureTestPath(t, tap, "/after-lock-release")
	if got := oneLogFile(t, dir, "*.ndjson.part"); got != part {
		t.Fatalf("resumed %q, want %q", got, part)
	}
}

func TestNestedStrictCaptureReturns503WhenFull(t *testing.T) {
	dir := t.TempDir()
	outer := &Tap{Identity: "outer", Directory: dir, Mode: "strict", QueueSize: 1}
	inner := &Tap{Identity: "inner", Directory: dir, Mode: "strict", QueueSize: 1}
	provision(t, outer)
	provision(t, inner)

	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), caddyhttp.VarsCtxKey, make(map[string]any)), time.Second)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	innerHandler := caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		_, err := io.WriteString(w, "ok")
		return err
	})
	handler := caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		return inner.ServeHTTP(w, r, innerHandler)
	})
	inner.runtime.slots <- struct{}{}
	blocked := httptest.NewRecorder()
	err := outer.ServeHTTP(blocked, request, handler)
	<-inner.runtime.slots
	var httpErr caddyhttp.HandlerError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusServiceUnavailable || blocked.Body.Len() != 0 {
		t.Fatalf("nested capture returned %v and body %q, want HTTP 503 before the next handler", err, blocked.Body.String())
	}
	if len(outer.runtime.slots) != 0 || len(inner.runtime.slots) != 0 {
		t.Fatal("rejected nested capture leaked a slot")
	}
	inner.runtime.slots <- struct{}{}
	canceled := httptest.NewRecorder()
	err = outer.ServeHTTP(canceled, request, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
		ctx, cancel := context.WithCancel(r.Context())
		cancel()
		return handler.ServeHTTP(w, r.WithContext(ctx))
	}))
	if !errors.Is(err, context.Canceled) || canceled.Body.Len() != 0 ||
		len(outer.runtime.slots) != 0 || len(inner.runtime.slots) != 1 {
		t.Fatalf("canceled nested capture returned %v and body %q, slots %d/%d",
			err, canceled.Body.String(), len(outer.runtime.slots), len(inner.runtime.slots))
	}
	<-inner.runtime.slots
	allowed := httptest.NewRecorder()
	if err := outer.ServeHTTP(allowed, request, handler); err != nil || allowed.Body.String() != "ok" {
		t.Fatalf("nested capture with capacity returned %v and body %q", err, allowed.Body.String())
	}
}
