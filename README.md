# caddy-tap

English | [简体中文](README.zh-CN.md)

`caddy-tap` is HTTP middleware for Caddy v2 with the module ID `http.handlers.tap`. It copies requests and responses on selected routes and writes them to NDJSON in the background. In the default mode, tap drops the entire capture record when it runs out of capture capacity and continues serving the HTTP request. Strict mode is intended for deployments that need to retain all observable data and can tolerate backpressure from log writes.

## Build

This version uses Go 1.26 and Caddy v2.11.9. Run the following from the repository root:

```sh
go install github.com/caddyserver/xcaddy/cmd/xcaddy@v0.4.7
xcaddy build v2.11.9 --with "caddy-tap=$(pwd)" --output /tmp/caddy-tap
/tmp/caddy-tap list-modules | grep -Fx http.handlers.tap
```

`caddy-tap` is the local Go module path. Once the module is published, use the repository's Go module path with `xcaddy --with` to build from the remote repository.

## Caddyfile

This configuration captures only `/api/*`. Remove `@api` to enable capture for the whole site. You can also use tap inside a `handle` block or place it before a response handler such as `reverse_proxy` inside a `route` block, which runs handlers in the order written.

```caddyfile
:8080 {
    @api path /api/*
    tap @api {
        identity api
        directory /var/log/caddy/tap
        mode drop
        queue_size 64
        max_body_bytes 1MiB
        roll_size 100MiB
        roll_interval 24h
        roll_keep 10
        roll_keep_for 720h
    }

    reverse_proxy localhost:9000
}
```

```sh
/tmp/caddy-tap validate --config Caddyfile --adapter caddyfile
/tmp/caddy-tap run --config Caddyfile --adapter caddyfile
```

| Parameter        | Default  | Behavior                                                                                                                                                                                 |
| ---------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `identity`       | Required | 1–64 ASCII letters, digits, underscores, or hyphens. Must start with a letter or digit. Use distinct identities for tap blocks sharing a directory.                                      |
| `directory`      | Required | Output directory. Relative paths resolve against the Caddy process's working directory. New directories have permissions `0700`.                                                         |
| `mode`           | `drop`   | `drop` discards the entire capture record when capacity runs out. `strict` waits for capacity, disables body truncation, and waits for the log file to sync.                             |
| `queue_size`     | `64`     | Maximum number of exchanges being captured, queued, or written per configured instance, from 1 to 65536.                                                                                 |
| `max_body_bytes` | `1MiB`   | In default mode, the byte limit applies separately to request and response bodies. The log still reports the total observed bytes and flags truncation. Omit this option in strict mode. |
| `roll_size`      | `100MiB` | Rotates before appending a record that would exceed the threshold. A record larger than the threshold gets its own file.                                                                 |
| `roll_interval`  | Disabled | Rotates on the next write after the specified time has elapsed since the current file was created. Supports durations such as `24h` and `1d`.                                            |
| `roll_keep`      | `10`     | Maximum number of closed files to keep for the same directory and identity. Set to `-1` to disable the limit.                                                                            |
| `roll_keep_for`  | `2160h`  | Maximum retention age for closed files, based on the UTC timestamp in the filename. Set to `-1` to disable the limit.                                                                    |

Size settings accept byte counts and units such as `KiB` and `MiB`. Retention runs after the first record is successfully written to a new file and applies all enabled limits. No scheduled deletion runs while idle. Use separate output directories or identities for multiple Caddy processes so each can manage its own retention.

## JSON configuration

Place this object in the HTTP route's `handle` array before the handler you want to observe. Configure path matchers in the route's `match` field. JSON sizes are integer byte counts. Durations can be strings or integer nanosecond counts. To disable `roll_keep_for`, use the number `-1` in JSON or `roll_keep_for -1` in a Caddyfile.

```json
{
  "handler": "tap",
  "identity": "api",
  "directory": "/var/log/caddy/tap",
  "mode": "strict",
  "queue_size": 16,
  "roll_size": 104857600,
  "roll_interval": "24h",
  "roll_keep": 10,
  "roll_keep_for": "720h"
}
```

## Log format and capture boundaries

- Filenames look like `api-20261111T120000-123456789.ndjson`. They contain the identity, a UTC timestamp with second precision, and a numeric suffix to avoid collisions. Retention applies to files in this format. New files have permissions `0600`.
- Each line contains `identity`, `timestamp`, `duration_ns`, `request`, and `response`. Request fields include `method`, `url`, `request_uri`, `host`, `scheme`, `protocol`, `remote_addr`, `content_length`, and `transfer_encoding` when available. Use `scheme` and `host` with a relative URL. Request and response `header` and `trailer` fields preserve repeated values in string arrays. The response also includes `status`, with `informational` and `hijacked` when available.
- When a header or trailer value contains non-UTF-8 bytes, `header_base64` or `trailer_base64` stores every raw value for that name in the same order as the text field. Use `url_base64` or `request_uri_base64` to recover the original bytes of a non-UTF-8 URL or request target.
- Decode `body_base64` to recover the captured bytes. The field is `null` when no bytes were captured. `body_bytes` is the total number of bytes observed by the middleware, and `body_truncated` flags truncation at the capture limit. For requests, `body_complete` indicates that tap observed EOF or that the full known length was read. Tap records I/O errors in `body_error` and errors returned by downstream handlers in `error`. If a downstream handler panics, tap records `aborted: true` and lets Caddy handle the panic.
- Capture happens at the Caddy handler boundary. Tap copies only the request body bytes that downstream handlers read and the response body bytes that the writer accepts. Headers added later by the server, responses generated outside the handler chain, and bytes written directly to an upgraded connection are outside this boundary. `status: 0` means tap observed no final status being committed. This can happen with an empty handler, Caddy's outer error handling, or an upgrade handshake written directly to the connection.
- In default mode, tap aggregates counts of records dropped due to capacity and reports write errors in the background. During a hot reload, the old configuration continues recording exchanges it has already started capturing. Graceful shutdown drains queued records. In-flight requests that exceed Caddy's shutdown grace period and records still pending on forced termination may be lost.
- In strict mode, tap holds captured data in memory and waits for the file to sync before the handler returns. A request canceled while waiting for capture capacity stops waiting. Queued records are still written. Response bytes streamed to the client may arrive before the sync completes. Tap reports disk write failures in the Caddy log and returns HTTP 500 if the response is still uncommitted and the downstream handler returned no error. Large bodies increase memory use, and slow disks increase request latency.

If the same request re-enters the same tap instance in a named route, the outer invocation records it once.

Logs store credentials, cookies, query parameters, and bodies as received. Use a directory accessible only to Caddy and authorized readers.

### Server-Sent Events (SSE)

For responses with `Content-Type: text/event-stream`, tap forwards events and passes flush calls through. It writes one record per captured SSE request when the handler returns. Decode `response.body_base64` to recover the captured SSE bytes, including heartbeat comments, line breaks, event separators, and multiline data. If the handler returns after a client disconnects, the record preserves the bytes the writer accepted before then.

In default mode, a stream that exceeds `max_body_bytes` is truncated in the log while the client can continue receiving events. Strict mode retains all observable bytes in memory until the stream ends. A connection that stays open occupies a capture slot and produces no per-event records.

## Tests

```sh
make test
```

This runs the test suite with coverage and race detection, followed by `go vet`. To include the Caddy binary integration test, run `CADDY_TAP_BIN=/tmp/caddy-tap make test` after building the binary above.

## License

[MIT](LICENSE)
