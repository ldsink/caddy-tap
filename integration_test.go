package tap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func localAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestCaddyBinary(t *testing.T) {
	binary := os.Getenv("CADDY_TAP_BIN")
	if binary == "" {
		t.Skip("set CADDY_TAP_BIN to a Caddy binary with http.handlers.tap")
	}
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(dir, "data"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := func(args ...string) []byte {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("caddy %v: %v\n%s%s", args, err, out, stderr.Bytes())
		}
		return out
	}
	if out := run("list-modules"); !bytes.Contains(out, []byte("http.handlers.tap\n")) {
		t.Fatal("Caddy binary does not contain http.handlers.tap")
	}
	readmeConfig := filepath.Join(dir, "readme.Caddyfile")
	for _, example := range readmeExamples(t, "README.md", "caddyfile") {
		if err := os.WriteFile(readmeConfig, []byte(example), 0600); err != nil {
			t.Fatal(err)
		}
		run("adapt", "--config", readmeConfig, "--adapter", "caddyfile")
	}

	requestPayload := []byte{0, 0xff, 'x', 'y', 'z'}
	firstChunk, lastChunk := []byte(": heartbeat\n\nid: 1\ndata: 你好\n\n"), []byte("id: 2\nevent: update\ndata: stream-end\n\n")
	rawHeader := string([]byte{0xff, 'x'})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Path == "/logged/upgrade" {
			conn, rw, err := http.NewResponseController(w).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: fixture\r\n\r\n"); err != nil {
				t.Error(err)
				return
			}
			if err := rw.Flush(); err != nil {
				t.Error(err)
				return
			}
			frame, err := rw.ReadByte()
			if err != nil {
				t.Error(err)
				return
			}
			if err := rw.WriteByte(frame); err != nil {
				t.Error(err)
				return
			}
			if err := rw.Flush(); err != nil {
				t.Error(err)
			}
			return
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(payload, requestPayload) ||
			r.Trailer.Get("X-Request-End") != "request-end" || r.Header.Get("X-Raw") != rawHeader ||
			!reflect.DeepEqual(r.Header.Values("X-Multi"), []string{"one", "two"}) {
			t.Errorf("upstream request body=%x trailer=%v header=%v error=%v", payload, r.Trailer, r.Header, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Raw", rawHeader)
		w.Header().Add("Set-Cookie", "first=1")
		w.Header().Add("Set-Cookie", "second=2")
		w.Header().Set("Trailer", "X-Response-End")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(firstChunk)
		w.(http.Flusher).Flush()
		<-release
		_, _ = w.Write(lastChunk)
		w.Header().Set("X-Response-End", "response-end")
	}))
	defer upstream.Close()
	defer unblock()

	addr, admin := localAddress(t), localAddress(t)
	config := filepath.Join(dir, "Caddyfile")
	logs := filepath.Join(dir, "logs")
	filteredLogs := filepath.Join(dir, "filtered")
	writeConfig := func(mode, limit string) {
		t.Helper()
		cfg := fmt.Sprintf(`{
	admin %s
	persist_config off
	servers {
		protocols h1 h2c
	}
}
http://%s {
	@logged path /logged/*
	tap @logged {
		identity binary
		directory "%s"
		mode %s
		%s
		roll_size 1KiB
	}
	tap /omit/header {
		identity filtered
		directory "%s"
		mode strict
		omit_fields identity request.header
	}
	tap /omit/header-and-base64 {
		identity filtered
		directory "%s"
		mode strict
		omit_fields identity request.header request.header_base64
	}
	reverse_proxy %s
}
`, admin, addr, logs, mode, limit, filteredLogs, filteredLogs, upstream.URL)
		if err := os.WriteFile(config, []byte(cfg), 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig("drop", "max_body_bytes 3")
	run("validate", "--config", config, "--adapter", "caddyfile")

	serverLog, err := os.Create(filepath.Join(dir, "caddy.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverLog.Close()
	cmd := exec.CommandContext(ctx, binary, "run", "--config", config, "--adapter", "caddyfile")
	cmd.Stdout, cmd.Stderr = serverLog, serverLog
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := os.ReadFile(serverLog.Name())
			t.Logf("Caddy output:\n%s", logs)
		}
	})
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	baseURL := "http://" + addr
	for deadline := time.Now().Add(5 * time.Second); ; {
		resp, err := client.Get(baseURL + "/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("unmatched route got %d", resp.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	send := func(client *http.Client, protocol string, path string, onFirstChunk func()) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, "POST", baseURL+path, bytes.NewReader(requestPayload))
		if err != nil {
			t.Fatal(err)
		}
		req.ContentLength = -1
		req.Trailer = http.Header{"X-Request-End": {"request-end"}}
		req.Header["X-Multi"] = []string{"one", "two"}
		req.Header.Set("X-Raw", rawHeader)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		start := make([]byte, len(firstChunk))
		if _, err := io.ReadFull(resp.Body, start); err != nil || !bytes.Equal(start, firstChunk) {
			t.Fatalf("streamed first chunk %x, error %v", start, err)
		}
		if onFirstChunk != nil {
			onFirstChunk()
		}
		unblock()
		rest, err := io.ReadAll(resp.Body)
		if err != nil || resp.Proto != protocol || resp.StatusCode != http.StatusOK || !bytes.Equal(rest, lastChunk) ||
			!reflect.DeepEqual(resp.Header.Values("Set-Cookie"), []string{"first=1", "second=2"}) ||
			resp.Trailer.Get("X-Response-End") != "response-end" || resp.Header.Get("X-Raw") != rawHeader ||
			resp.Header.Get("Content-Type") != "text/event-stream" {
			t.Fatalf("response protocol=%s status=%d body=%x header=%v trailer=%v error=%v", resp.Proto, resp.StatusCode, rest, resp.Header, resp.Trailer, err)
		}
	}
	send(client, "HTTP/1.1", "/logged/drop?q=%2B", func() {
		writeConfig("strict", "")
		jsonConfig := filepath.Join(dir, "caddy.json")
		if err := os.WriteFile(jsonConfig, run("adapt", "--config", config, "--adapter", "caddyfile"), 0600); err != nil {
			t.Fatal(err)
		}
		run("reload", "--config", jsonConfig, "--address", admin)
	})
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	h2Client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Protocols: &protocols}}
	defer h2Client.CloseIdleConnections()
	send(h2Client, "HTTP/2.0", "/logged/strict", nil)
	send(client, "HTTP/1.1", "/omit/header", nil)
	send(h2Client, "HTTP/2.0", "/omit/header-and-base64", nil)

	upgradeCtx, cancelUpgrade := context.WithTimeout(ctx, 5*time.Second)
	defer cancelUpgrade()
	req, err := http.NewRequestWithContext(upgradeCtx, "GET", baseURL+"/logged/upgrade", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "fixture")
	upgraded, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Body.Close()
	tunnel, ok := upgraded.Body.(io.ReadWriteCloser)
	if !ok || upgraded.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status=%d, body type=%T", upgraded.StatusCode, upgraded.Body)
	}
	if _, err := tunnel.Write([]byte{0xfe}); err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 1)
	if _, err := io.ReadFull(tunnel, frame); err != nil || frame[0] != 0xfe {
		t.Fatalf("upgrade echo %x, error %v", frame, err)
	}
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
	h2Client.CloseIdleConnections()
	run("stop", "--address", admin)
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}

	paths, err := filepath.Glob(filepath.Join(logs, "binary-*.ndjson"))
	if err != nil || len(paths) != 3 {
		t.Fatalf("got files %v, error %v", paths, err)
	}
	records, err := readExchanges(logs)
	if err != nil || len(records) != 3 {
		t.Fatalf("got %d exchanges, error %v", len(records), err)
	}
	seen := map[string]bool{}
	for _, e := range records {
		if seen[e.Request.URL] {
			t.Fatalf("duplicate record for %q", e.Request.URL)
		}
		seen[e.Request.URL] = true
		if e.Request.URL == "/logged/upgrade" {
			if e.Response.Status != 101 || !e.Response.Hijacked || e.Response.Bytes != 0 ||
				e.Response.Header.Get("Upgrade") != "fixture" {
				t.Fatalf("upgrade record %+v", e)
			}
			continue
		}
		requestBody, responseBody := requestPayload, append(bytes.Clone(firstChunk), lastChunk...)
		drop := e.Request.URL == "/logged/drop?q=%2B"
		protocol := "HTTP/2.0"
		if drop {
			protocol = "HTTP/1.1"
			requestBody, responseBody = requestBody[:3], responseBody[:3]
		}
		if (!drop && e.Request.URL != "/logged/strict") ||
			e.Request.Method != "POST" || e.Request.Protocol != protocol || !e.Request.BodyComplete || e.Response.Status != http.StatusOK ||
			e.Request.Truncated != drop || e.Response.Truncated != drop ||
			e.Response.Header.Get("Content-Type") != "text/event-stream" ||
			!reflect.DeepEqual(e.Request.HeaderBase64["X-Raw"], [][]byte{[]byte(rawHeader)}) ||
			!reflect.DeepEqual(e.Response.HeaderBase64["X-Raw"], [][]byte{[]byte(rawHeader)}) ||
			e.Request.Trailer.Get("X-Request-End") != "request-end" ||
			e.Response.Trailer.Get("X-Response-End") != "response-end" ||
			!bytes.Equal(e.Request.Data, requestBody) || !bytes.Equal(e.Response.Data, responseBody) {
			t.Fatalf("logged exchange %+v", e)
		}
	}

	data, err := os.ReadFile(oneLogFile(t, filteredLogs, "filtered-*.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for _, path := range []string{"/omit/header", "/omit/header-and-base64"} {
		var record struct {
			Identity json.RawMessage
			Request  struct {
				URL          string
				Header       json.RawMessage
				HeaderBase64 json.RawMessage `json:"header_base64"`
			}
		}
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record.Request.URL != path || record.Identity != nil || record.Request.Header != nil {
			t.Fatalf("unexpected filtered record for %q: %+v", path, record)
		}
		if path == "/omit/header" {
			var raw map[string][][]byte
			if err := json.Unmarshal(record.Request.HeaderBase64, &raw); err != nil ||
				!reflect.DeepEqual(raw["X-Raw"], [][]byte{[]byte(rawHeader)}) {
				t.Fatalf("request.header_base64 = %s, error %v", record.Request.HeaderBase64, err)
			}
		} else if record.Request.HeaderBase64 != nil {
			t.Fatalf("request.header_base64 was retained: %s", record.Request.HeaderBase64)
		}
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		t.Fatalf("expected two filtered records, next decode: %v", err)
	}
}
