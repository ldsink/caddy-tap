package tap

import (
	"bytes"
	"errors"
	"io"
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
