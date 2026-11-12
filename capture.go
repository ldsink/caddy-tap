package tap

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"golang.org/x/net/http/httpguts"
)

type exchange struct {
	Timestamp  time.Time      `json:"timestamp"`
	DurationNS int64          `json:"duration_ns"`
	Identity   string         `json:"identity"`
	Request    requestRecord  `json:"request"`
	Response   responseRecord `json:"response"`
	Aborted    bool           `json:"aborted,omitempty"`
	Error      string         `json:"error,omitempty"`
}

type bodyRecord struct {
	Data      []byte `json:"body_base64"`
	Bytes     int64  `json:"body_bytes"`
	Truncated bool   `json:"body_truncated"`
	Error     string `json:"body_error,omitempty"`
}

func (b *bodyRecord) append(data []byte, limit int64) {
	b.Bytes += int64(len(data))
	if limit > 0 {
		remaining := limit - int64(len(b.Data))
		if int64(len(data)) > remaining {
			b.Truncated = true
			data = data[:remaining]
		}
	}
	b.Data = append(b.Data, data...)
}

type requestRecord struct {
	Method           string              `json:"method"`
	URL              string              `json:"url"`
	RequestURI       string              `json:"request_uri"`
	Host             string              `json:"host"`
	Scheme           string              `json:"scheme"`
	Protocol         string              `json:"protocol"`
	RemoteAddr       string              `json:"remote_addr"`
	ContentLength    int64               `json:"content_length"`
	TransferEncoding []string            `json:"transfer_encoding,omitempty"`
	Header           http.Header         `json:"header"`
	Trailer          http.Header         `json:"trailer,omitempty"`
	HeaderBase64     map[string][][]byte `json:"header_base64,omitempty"`
	TrailerBase64    map[string][][]byte `json:"trailer_base64,omitempty"`
	URLBase64        []byte              `json:"url_base64,omitempty"`
	RequestURIBase64 []byte              `json:"request_uri_base64,omitempty"`
	BodyComplete     bool                `json:"body_complete"`
	bodyRecord
}

type responseHead struct {
	Status       int                 `json:"status"`
	Header       http.Header         `json:"header"`
	HeaderBase64 map[string][][]byte `json:"header_base64,omitempty"`
}

type responseRecord struct {
	responseHead
	Informational []responseHead      `json:"informational,omitempty"`
	Trailer       http.Header         `json:"trailer,omitempty"`
	TrailerBase64 map[string][][]byte `json:"trailer_base64,omitempty"`
	Hijacked      bool                `json:"hijacked,omitempty"`
	bodyRecord
}

func capture(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler, limit int64, identity string, finish func(exchange)) (err error) {
	start := time.Now()
	scheme := r.URL.Scheme
	if scheme == "" {
		scheme = "http"
		if r.TLS != nil {
			scheme = "https"
		}
	}
	req := requestRecord{
		Method: r.Method, URL: r.URL.String(), RequestURI: r.RequestURI, Host: r.Host,
		Scheme: scheme, Protocol: r.Proto, RemoteAddr: r.RemoteAddr,
		ContentLength: r.ContentLength, TransferEncoding: append([]string(nil), r.TransferEncoding...),
		Header: r.Header.Clone(), Trailer: r.Trailer.Clone(), BodyComplete: true,
	}
	var reader *requestBody
	if r.Body != nil && r.Body != http.NoBody {
		reader = &requestBody{ReadCloser: r.Body, original: r, limit: limit}
		r.Body = reader
	}
	writer := &responseWriter{
		ResponseWriterWrapper: caddyhttp.ResponseWriterWrapper{ResponseWriter: w},
		limit:                 limit,
	}
	completed := false
	defer func() {
		if reader != nil {
			reader.mu.Lock()
			req.bodyRecord = reader.record
			reader.record.Data = nil
			reader.finished = true
			req.BodyComplete = reader.eof || (req.ContentLength > 0 && req.Bytes == req.ContentLength && !reader.readFailed)
			if reader.eof {
				req.Trailer = reader.trailer
			}
			reader.mu.Unlock()
		}

		resp := writer.record
		if resp.Status == 0 {
			resp.Header = snapshotResponseHeader(w.Header())
		}
		if !resp.Hijacked && (resp.Status != 0 || (completed && err == nil)) {
			resp.Trailer = responseTrailers(resp.Header, w.Header())
		}
		record := exchange{Timestamp: start.UTC(), DurationNS: time.Since(start).Nanoseconds(), Identity: identity, Aborted: !completed, Request: req, Response: resp}
		if err != nil {
			record.Error = err.Error()
		}
		finish(record)
	}()
	err = next.ServeHTTP(writer, r)
	completed = true
	return err
}

type requestBody struct {
	io.ReadCloser
	mu         sync.Mutex
	original   *http.Request
	limit      int64
	record     bodyRecord
	eof        bool
	readFailed bool
	trailer    http.Header
	finished   bool
}

func (b *requestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.finished {
		b.record.append(p[:n], b.limit)
		if errors.Is(err, io.EOF) && !b.eof {
			b.eof = true
			b.trailer = b.original.Trailer.Clone()
		} else if err != nil && !errors.Is(err, io.EOF) {
			b.readFailed = true
			b.record.Error = err.Error()
		}
	}
	return n, err
}

func (b *requestBody) Close() error {
	err := b.ReadCloser.Close()
	if err != nil {
		b.mu.Lock()
		if !b.finished && b.record.Error == "" {
			b.record.Error = err.Error()
		}
		b.mu.Unlock()
	}
	return err
}

type responseWriter struct {
	caddyhttp.ResponseWriterWrapper
	limit  int64
	record responseRecord
}

func (w *responseWriter) WriteHeader(status int) {
	if w.record.Status == 0 {
		head := responseHead{Status: status, Header: snapshotResponseHeader(w.Header())}
		if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
			w.record.Informational = append(w.record.Informational, head)
		} else {
			w.record.responseHead = head
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if w.record.Status == 0 {
		w.record.responseHead = responseHead{Status: http.StatusOK, Header: snapshotResponseHeader(w.Header())}
	}
	n, err := w.ResponseWriter.Write(p)
	w.record.bodyRecord.append(p[:n], w.limit)
	if err != nil {
		w.record.bodyRecord.Error = err.Error()
	}
	return n, err
}

func (w *responseWriter) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.Copy(struct{ io.Writer }{w}, r)
	if err != nil {
		w.record.bodyRecord.Error = err.Error()
	}
	return n, err
}

func (w *responseWriter) Flush() {
	_ = w.FlushError()
}

func (w *responseWriter) FlushError() error {
	var header http.Header
	if w.record.Status == 0 {
		header = snapshotResponseHeader(w.Header())
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if w.record.Status == 0 && !errors.Is(err, http.ErrNotSupported) {
		w.record.responseHead = responseHead{Status: http.StatusOK, Header: header}
	}
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.record.bodyRecord.Error = err.Error()
	}
	return err
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.record.Hijacked = true
	}
	return conn, rw, err
}

func snapshotResponseHeader(header http.Header) http.Header {
	snapshot := header.Clone()
	for key := range snapshot {
		if strings.HasPrefix(key, http.TrailerPrefix) {
			delete(snapshot, key)
		}
	}
	return snapshot
}

func responseTrailers(committed, final http.Header) http.Header {
	trailers := make(http.Header)
	for name, values := range final {
		if key, ok := strings.CutPrefix(name, http.TrailerPrefix); ok {
			key = http.CanonicalHeaderKey(key)
			trailers[key] = append(trailers[key], values...)
		}
	}
	for _, declared := range committed.Values("Trailer") {
		for name := range strings.SplitSeq(declared, ",") {
			key := http.CanonicalHeaderKey(strings.TrimSpace(name))
			if !httpguts.ValidTrailerHeader(key) {
				continue
			}
			if values := final.Values(key); len(values) != 0 {
				trailers[key] = append(trailers[key], values...)
			}
		}
	}
	return trailers
}
