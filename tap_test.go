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
		for decoder.More() {
			var record exchange
			if err := decoder.Decode(&record); err != nil {
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
