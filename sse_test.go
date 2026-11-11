package tap

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func TestSSECapture(t *testing.T) {
	const first = ": heartbeat\r\n\r\nretry: 1500\r\nid: 1\r\ndata: 你好\r\n\r\n"
	const second = "id: 2\nevent: update\ndata: first line\ndata: second line\n\n"
	for _, tc := range []struct {
		name, mode string
		limit      int64
		disconnect bool
	}{
		{name: "complete", mode: "strict"},
		{name: "truncated log only", mode: "drop", limit: int64(len(first) + 4)},
		{name: "client disconnect", mode: "strict", disconnect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tap := &Tap{Identity: "sse", Directory: dir, Mode: tc.mode, MaxBodyBytes: tc.limit}
			provision(t, tap)

			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			finished := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				finished <- tap.ServeHTTP(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, r *http.Request) error {
					w.Header().Set("Content-Type", "text/event-stream")
					w.Header().Set("Cache-Control", "no-cache")
					if _, err := io.WriteString(w, first); err != nil {
						return err
					}
					if err := http.NewResponseController(w).Flush(); err != nil {
						return err
					}
					select {
					case <-r.Context().Done():
						return r.Context().Err()
					case <-release:
					}
					if _, err := io.WriteString(w, second[:10]); err != nil {
						return err
					}
					_, err := io.WriteString(w, second[10:])
					return err
				}))
			}))
			defer server.Close()
			defer unblock()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/events", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Accept", "text/event-stream")
			resp, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
				t.Fatalf("response status=%d, headers=%v", resp.StatusCode, resp.Header)
			}

			prefix := make([]byte, len(first))
			if _, err := io.ReadFull(resp.Body, prefix); err != nil || string(prefix) != first {
				t.Fatalf("first SSE bytes %q, error %v", prefix, err)
			}
			if records, err := readExchanges(dir); err != nil || len(records) != 0 {
				t.Fatalf("open stream wrote %d records, error %v", len(records), err)
			}
			body := first
			if tc.disconnect {
				cancel()
				if err := resp.Body.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				unblock()
				rest, err := io.ReadAll(resp.Body)
				if err != nil || string(rest) != second {
					t.Fatalf("remaining SSE bytes %q, error %v", rest, err)
				}
				body += second
			}
			select {
			case err := <-finished:
				if (tc.disconnect && !errors.Is(err, context.Canceled)) || (!tc.disconnect && err != nil) {
					t.Fatalf("stream handler error %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("SSE handler did not return")
			}
			if err := tap.Cleanup(); err != nil {
				t.Fatal(err)
			}
			records, err := readExchanges(dir)
			if err != nil || len(records) != 1 {
				t.Fatalf("logged %d records, error %v", len(records), err)
			}
			e := records[0]
			want := body
			if tc.limit > 0 {
				want = body[:tc.limit]
			}
			if e.Request.URL != "/events" || e.Request.Header.Get("Accept") != "text/event-stream" ||
				e.Response.Status != http.StatusOK || e.Response.Header.Get("Content-Type") != "text/event-stream" ||
				e.Response.Header.Get("Cache-Control") != "no-cache" || e.Response.Bytes != int64(len(body)) ||
				e.Response.Truncated != (tc.limit > 0) || string(e.Response.Data) != want || e.Response.Error != "" || e.Aborted {
				t.Fatalf("SSE record %+v, decoded body %q", e, e.Response.Data)
			}
			wantError := ""
			if tc.disconnect {
				wantError = context.Canceled.Error()
			}
			if e.Error != wantError {
				t.Fatalf("logged error %q, want %q", e.Error, wantError)
			}
		})
	}
}
