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
	"path/filepath"
	"sync"
	"testing"
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
	paths, err := filepath.Glob(filepath.Join(dir, "tap-*.ndjson"))
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

func TestConcurrentStrictRecordsAndReload(t *testing.T) {
	dir := t.TempDir()
	first := &Tap{Identity: "api", Directory: dir, Mode: "strict", QueueSize: 4, RollKeep: 1}
	second := &Tap{Identity: "api", Directory: dir, Mode: "strict", QueueSize: 4, RollKeep: 1}
	provision(t, first)
	provision(t, second)
	var wg sync.WaitGroup
	for range 24 {
		for _, tap := range []*Tap{first, second} {
			wg.Go(func() {
				err := tap.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/concurrent", nil),
					caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
						_, err := io.WriteString(w, "done")
						return err
					}))
				if err != nil {
					t.Error(err)
				}
			})
		}
	}
	wg.Wait()
	records, err := readExchanges(dir)
	if err != nil || len(records) != 48 {
		t.Fatalf("got %d records, error %v", len(records), err)
	}
	wg.Go(func() { _ = first.Cleanup() })
	wg.Go(func() { _ = first.Cleanup() })
	wg.Go(func() { _ = second.Cleanup() })
	wg.Wait()
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
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), caddyhttp.VarsCtxKey, make(map[string]any)), time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	err := tap.ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
			return tap.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
				_, err := io.WriteString(w, "done")
				return err
			}))
		}))
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
	if err != nil || info.Mode().Perm()&0077 != 0 {
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
	go rt.writeRecords(&fileSink{logger: logger})
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
