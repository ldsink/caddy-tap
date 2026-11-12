package tap

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func recordExchange(t *testing.T, w http.ResponseWriter, r *http.Request, limit int64, next caddyhttp.HandlerFunc) (exchange, error) {
	t.Helper()
	var record exchange
	err := capture(w, r, next, limit, "test", func(e exchange) { record = e })
	return record, err
}

func TestCaptureInfersHTTPSForRelativeURL(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/path", nil)
	req.TLS = &tls.ConnectionState{}
	e, err := recordExchange(t, httptest.NewRecorder(), req, 0,
		func(http.ResponseWriter, *http.Request) error { return nil })
	if err != nil || e.Request.Scheme != "https" || e.Request.URL != "/path" {
		t.Fatalf("request %+v, error %v", e.Request, err)
	}
}

func TestCapturePreservesExchange(t *testing.T) {
	payload := []byte{0, 0xff, '\n', 'a', 'b'}
	req := httptest.NewRequest("PATCH", "https://example.test/api/a%2Fb?q=a%20b", bytes.NewReader(payload))
	req.Header.Add("X-Multi", "one")
	req.Header.Add("X-Multi", "two")
	req.Header.Set("Authorization", "Bearer fixture")
	req.Trailer = http.Header{"X-Request-End": nil}
	recorder := httptest.NewRecorder()
	e, err := recordExchange(t, recorder, req, 3, func(w http.ResponseWriter, r *http.Request) error {
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, payload) || r != req {
			t.Fatalf("downstream request body %x, err %v", body, err)
		}
		r.Header.Set("X-Multi", "rewritten")
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.Header().Set("Trailer", "X-Response-End")
		w.WriteHeader(http.StatusCreated)
		w.Header().Set("X-Late", "after commit")
		// Hide WriterTo so the response wrapper's ReaderFrom is exercised.
		if _, err := io.Copy(w, struct{ io.Reader }{bytes.NewReader(payload)}); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("X-Response-End", "done")
		w.Header().Set(http.TrailerPrefix+"X-Extra-End", "extra")
		return http.NewResponseController(w).Flush()
	})
	if err != nil || recorder.Code != 201 || !bytes.Equal(recorder.Body.Bytes(), payload) || !recorder.Flushed {
		t.Fatalf("downstream response status=%d body=%x flushed=%v error=%v", recorder.Code, recorder.Body.Bytes(), recorder.Flushed, err)
	}
	if e.Request.Method != "PATCH" || e.Request.Host != "example.test" || e.Request.Scheme != "https" ||
		e.Request.URL != "https://example.test/api/a%2Fb?q=a%20b" ||
		e.Request.Header.Get("Authorization") != "Bearer fixture" ||
		!reflect.DeepEqual(e.Request.Header.Values("X-Multi"), []string{"one", "two"}) {
		t.Fatalf("request metadata %+v", e.Request)
	}
	if e.Response.Status != 201 || e.Response.Header.Get("X-Late") != "" ||
		!reflect.DeepEqual(e.Response.Header.Values("Set-Cookie"), []string{"a=1", "b=2"}) ||
		e.Response.Trailer.Get("X-Response-End") != "done" || e.Response.Trailer.Get("X-Extra-End") != "extra" {
		t.Fatalf("response metadata %+v", e.Response)
	}
	for _, body := range []bodyRecord{e.Request.bodyRecord, e.Response.bodyRecord} {
		if !body.Truncated || body.Bytes != 5 || !bytes.Equal(body.Data, payload[:3]) {
			t.Fatalf("body capture %+v", body)
		}
	}
	if !e.Request.BodyComplete || e.Identity != "test" || e.Timestamp.IsZero() || e.DurationNS < 0 {
		t.Fatalf("exchange %+v", e)
	}
}

type brokenBody struct{}

func (brokenBody) Read(p []byte) (int, error) {
	p[0] = 'a'
	return 1, io.ErrUnexpectedEOF
}
func (brokenBody) Close() error { return nil }

type failingWriter struct{ *httptest.ResponseRecorder }

func (w failingWriter) Write(p []byte) (int, error) {
	_, _ = w.ResponseRecorder.Write(p[:1])
	return 1, io.ErrClosedPipe
}

func TestCaptureErrorsAndUnreadBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader("abc"))
	req.Body = brokenBody{}
	e, err := recordExchange(t, failingWriter{httptest.NewRecorder()}, req, 0, func(w http.ResponseWriter, r *http.Request) error {
		_, readErr := io.ReadAll(r.Body)
		if !errors.Is(readErr, io.ErrUnexpectedEOF) {
			t.Fatal(readErr)
		}
		_, err := w.Write([]byte("xyz"))
		return err
	})
	if !errors.Is(err, io.ErrClosedPipe) || e.Error == "" || e.Response.Error == "" ||
		e.Request.BodyComplete || e.Request.Error == "" || string(e.Request.Data) != "a" || string(e.Response.Data) != "x" {
		t.Fatalf("record %+v, error %v", e, err)
	}
	e, err = recordExchange(t, httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader("unread")), 0,
		func(http.ResponseWriter, *http.Request) error { return nil })
	if err != nil || e.Request.BodyComplete || e.Request.Bytes != 0 || e.Response.Status != 0 {
		t.Fatalf("unread body exchange %+v, error %v", e, err)
	}
}

type headerWriter struct {
	header http.Header
	codes  []int
}

func (w *headerWriter) Header() http.Header         { return w.header }
func (w *headerWriter) WriteHeader(code int)        { w.codes = append(w.codes, code) }
func (w *headerWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *headerWriter) FlushError() error {
	w.codes = append(w.codes, http.StatusOK)
	return nil
}

func TestInformationalAndFlushCommit(t *testing.T) {
	underlying := &headerWriter{header: make(http.Header)}
	e, err := recordExchange(t, underlying, httptest.NewRequest("GET", "/", nil), 0,
		func(w http.ResponseWriter, _ *http.Request) error {
			w.Header().Set("Link", "first")
			w.WriteHeader(http.StatusEarlyHints)
			w.Header().Set("Link", "second")
			w.WriteHeader(http.StatusProcessing)
			w.Header().Del("Link")
			return http.NewResponseController(w).Flush()
		})
	if err != nil || e.Response.Status != 200 || len(e.Response.Informational) != 2 ||
		e.Response.Informational[0].Header.Get("Link") != "first" ||
		e.Response.Informational[1].Header.Get("Link") != "second" ||
		e.Response.Header.Get("Link") != "" || !reflect.DeepEqual(underlying.codes, []int{103, 102, 200}) {
		t.Fatalf("codes %v, response %+v, error %v", underlying.codes, e.Response, err)
	}
}

func TestCaptureAbortPropagates(t *testing.T) {
	var e exchange
	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler || !e.Aborted || string(e.Response.Data) != "partial" {
			t.Fatalf("panic %v, record %+v", recovered, e)
		}
	}()
	_ = capture(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
		_, _ = io.WriteString(w, "partial")
		panic(http.ErrAbortHandler)
	}), 0, "test", func(record exchange) { e = record })
}

func TestCaptureRejectedStatusDoesNotCommit(t *testing.T) {
	recorder := httptest.NewRecorder()
	e, err := recordExchange(t, recorder, httptest.NewRequest("GET", "/", nil), 0,
		func(w http.ResponseWriter, _ *http.Request) error {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("invalid status did not panic")
					}
				}()
				w.WriteHeader(99)
			}()
			w.WriteHeader(http.StatusNoContent)
			return nil
		})
	if err != nil || recorder.Code != http.StatusNoContent || e.Response.Status != recorder.Code {
		t.Fatalf("recorded status %d, response status %d, error %v", e.Response.Status, recorder.Code, err)
	}
}

type panicBeforeWrite struct{ *httptest.ResponseRecorder }

func (panicBeforeWrite) Write([]byte) (int, error) { panic(http.ErrAbortHandler) }

func TestCaptureAbortedImplicitWriteDoesNotCommit(t *testing.T) {
	var e exchange
	recorder := httptest.NewRecorder()
	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler || !e.Aborted ||
			e.Response.Status != 0 || e.Response.Bytes != 0 {
			t.Fatalf("panic %v, record %+v", recovered, e)
		}
	}()
	_ = capture(panicBeforeWrite{recorder}, httptest.NewRequest("GET", "/", nil),
		caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
			_, err := w.Write([]byte("unwritten"))
			return err
		}), 0, "test", func(record exchange) { e = record })
}

type hijackedWriter struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (w *hijackedWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	conn, peer := net.Pipe()
	_ = peer.Close()
	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)), nil
}

func (w *hijackedWriter) WriteHeader(code int) {
	if !w.hijacked {
		w.ResponseRecorder.WriteHeader(code)
	}
}

func (w *hijackedWriter) Write(p []byte) (int, error) {
	if w.hijacked {
		return 0, http.ErrHijacked
	}
	return w.ResponseRecorder.Write(p)
}

func TestCaptureWritesAfterHijackDoNotCommit(t *testing.T) {
	for _, status := range []int{0, http.StatusSwitchingProtocols} {
		underlying := &hijackedWriter{ResponseRecorder: httptest.NewRecorder()}
		e, err := recordExchange(t, underlying, httptest.NewRequest("GET", "/", nil), 0,
			func(w http.ResponseWriter, _ *http.Request) error {
				if status != 0 {
					w.WriteHeader(status)
				}
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				w.WriteHeader(http.StatusOK)
				_, err = w.Write([]byte("late"))
				return err
			})
		if !errors.Is(err, http.ErrHijacked) || !e.Response.Hijacked || e.Response.Status != status ||
			e.Response.Bytes != 0 || e.Response.bodyRecord.Error != http.ErrHijacked.Error() {
			t.Fatalf("recorded response %+v, error %v", e.Response, err)
		}
	}
}

type closeErrorBody struct{ io.Reader }

func (closeErrorBody) Close() error { return io.ErrClosedPipe }

func TestCaptureCopyAndCloseErrors(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Body = closeErrorBody{strings.NewReader("request")}
	req.ContentLength = 7
	e, err := recordExchange(t, httptest.NewRecorder(), req, 0, func(w http.ResponseWriter, r *http.Request) error {
		if _, err := io.ReadFull(r.Body, make([]byte, 7)); err != nil {
			t.Fatal(err)
		}
		if err := r.Body.Close(); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("close: %v", err)
		}
		if n, err := io.Copy(w, brokenBody{}); n != 1 || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("copied %d bytes, error %v", n, err)
		}
		return nil
	})
	if err != nil || !e.Request.BodyComplete || e.Request.Error != io.ErrClosedPipe.Error() ||
		e.Response.Error != io.ErrUnexpectedEOF.Error() || string(e.Response.Data) != "a" {
		t.Fatalf("record %+v, error %v", e, err)
	}
}

func TestCaptureImplicitResponseTrailer(t *testing.T) {
	e, err := recordExchange(t, httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), 0,
		func(w http.ResponseWriter, _ *http.Request) error {
			w.Header().Set(http.TrailerPrefix+"X-End", "done")
			return nil
		})
	if err != nil || e.Response.Status != 0 || e.Response.Trailer.Get("X-End") != "done" || len(e.Response.Header) != 0 {
		t.Fatalf("record %+v, error %v", e, err)
	}
}

// Override promotion of FlushError by exposing only the basic writer.
type basicWriter struct{ http.ResponseWriter }

type flushFailureWriter struct{ http.ResponseWriter }

func (flushFailureWriter) FlushError() error { return io.ErrClosedPipe }

func TestCaptureResponseController(t *testing.T) {
	recorder := httptest.NewRecorder()
	e, err := recordExchange(t, recorder, httptest.NewRequest(http.MethodGet, "/", nil), 0,
		func(w http.ResponseWriter, _ *http.Request) error {
			w.(http.Flusher).Flush()
			if _, _, err := http.NewResponseController(w).Hijack(); !errors.Is(err, http.ErrNotSupported) {
				t.Fatalf("unsupported hijack returned %v", err)
			}
			return nil
		})
	if err != nil || !recorder.Flushed || e.Response.Status != 200 || e.Response.Hijacked {
		t.Fatalf("record %+v, error %v", e, err)
	}
	for _, tc := range []struct {
		writer    http.ResponseWriter
		err       error
		status    int
		bodyError string
	}{
		{basicWriter{httptest.NewRecorder()}, http.ErrNotSupported, 0, ""},
		{flushFailureWriter{httptest.NewRecorder()}, io.ErrClosedPipe, 200, io.ErrClosedPipe.Error()},
	} {
		e, err := recordExchange(t, tc.writer, httptest.NewRequest(http.MethodGet, "/", nil), 0,
			func(w http.ResponseWriter, _ *http.Request) error { return http.NewResponseController(w).Flush() })
		if !errors.Is(err, tc.err) || e.Response.Status != tc.status || e.Response.Error != tc.bodyError {
			t.Fatalf("record %+v, error %v", e, err)
		}
	}
}

func TestResponseTrailerOrderMatchesHTTP1(t *testing.T) {
	records := make(chan exchange, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = capture(w, r, caddyhttp.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) error {
			w.Header().Set("Trailer", "X-End, X-End")
			_, err := io.WriteString(w, "body")
			w.Header().Set(http.TrailerPrefix+"X-End", "prefixed")
			w.Header().Set("X-End", "declared")
			return err
		}), 0, "test", func(e exchange) { records <- e })
	}))
	defer server.Close()
	resp, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	want := []string{"prefixed", "declared", "declared"}
	got := (<-records).Response.Trailer.Values("X-End")
	if !reflect.DeepEqual(resp.Trailer.Values("X-End"), want) || !reflect.DeepEqual(got, want) {
		t.Fatalf("HTTP trailers %v, logged trailers %v, want %v", resp.Trailer, got, want)
	}
}

func TestCaptureSkipsForbiddenDeclaredTrailer(t *testing.T) {
	e, err := recordExchange(t, httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), 0,
		func(w http.ResponseWriter, _ *http.Request) error {
			w.Header().Set("Trailer", "Content-Length, X-End")
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Length", "4")
			w.Header().Set("X-End", "done")
			return nil
		})
	want := http.Header{"X-End": {"done"}}
	if err != nil || !reflect.DeepEqual(e.Response.Trailer, want) {
		t.Fatalf("trailers %v, want %v, error %v", e.Response.Trailer, want, err)
	}
}
