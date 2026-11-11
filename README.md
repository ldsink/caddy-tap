# caddy-tap

`caddy-tap` 是 Caddy v2 的 HTTP 中间件，模块 ID 为 `http.handlers.tap`。它在指定路由旁路复制请求与响应，在后台写入 NDJSON。默认模式在采集容量用完时丢弃整条日志，HTTP 请求继续处理。严格模式适合需要保留全部可观察数据且允许日志写入施加背压的场景。

## 构建

当前版本使用 Go 1.26 和 Caddy v2.11.9。在仓库根目录执行：

```sh
go install github.com/caddyserver/xcaddy/cmd/xcaddy@v0.4.7
xcaddy build v2.11.9 --with "caddy-tap=$(pwd)" --output /tmp/caddy-tap
/tmp/caddy-tap list-modules | grep -Fx http.handlers.tap
```

`caddy-tap` 是本地 Go 模块路径。发布到代码托管平台后，可将它替换为仓库的 Go 模块路径，再通过 `xcaddy --with` 从远程构建。

## Caddyfile

以下配置只记录 `/api/*`。去掉 `@api` 即可在站点级启用。也可在 `handle` 块内使用，或在 `route` 块内按书写顺序将它放在 `reverse_proxy` 等响应处理器前。

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

| 参数 | 默认值 | 行为 |
| --- | --- | --- |
| `identity` | 必填 | 1–64 个 ASCII 字母、数字、下划线或连字符，以字母或数字开头。建议在同一目录为不同采集块分配不同标识。 |
| `directory` | 必填 | 输出目录。相对路径以 Caddy 进程的工作目录为准。新建目录权限为 `0700`。 |
| `mode` | `drop` | `drop` 在容量用尽时丢弃整条采集记录。 `strict` 等待容量，关闭 body 截断，并等待日志文件同步。 |
| `queue_size` | `64` | 每个配置实例允许同时采集、排队或写入的交换总数，范围 1–65536。 |
| `max_body_bytes` | `1MiB` | 默认模式下请求体、响应体各自的字节上限。日志仍记录观察到的字节总数，并设置截断标记。严格模式应省略此项。 |
| `roll_size` | `100MiB` | 追加一条记录会超过阈值时，先滚动到新文件。单条记录超过阈值时，独占一个文件。 |
| `roll_interval` | 关闭 | 从当前文件创建起经过指定时长后，在下一条记录写入时滚动。支持 `24h`、`1d` 等时长。 |
| `roll_keep` | `10` | 同一目录、同一标识最多保留的已关闭文件数。设为 `-1` 关闭该限制。 |
| `roll_keep_for` | `2160h` | 以文件名中的 UTC 时间判断已关闭文件的最大保留时长。设为 `-1` 关闭该限制。 |

大小参数支持字节数和 `KiB`、`MiB` 等单位。保留策略在新文件打开时执行，同时应用已启用的限制。空闲期间不会定时删除文件。多个 Caddy 进程使用独立的输出目录或标识，以便各自管理保留策略。

## JSON 配置

将该对象放入 HTTP 路由的 `handle` 数组，置于需要观察的 handler 前。路径匹配器属于该路由的 `match` 字段。JSON 大小参数为字节整数，时长可用字符串或纳秒整数。

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

## 日志格式与捕获边界

- 文件名形如 `tap-api--20261111T120000.000000000Z-123456789.ndjson`，包含标识、UTC 时间和避免冲突的数字后缀。新建文件权限为 `0600`。
- 每行包含 `identity`、`timestamp`、`duration_ns`、`request` 和 `response`。请求字段包含 `method`、`url`、`request_uri`、`host`、`scheme`、`protocol`、`remote_addr`、`content_length` 和可用的 `transfer_encoding`。相对形式的 URL 可结合 `scheme` 和 `host` 使用。请求和响应的 `header`、`trailer` 以字符串数组保留重复值。响应另有 `status` 和可用的 `informational`、`hijacked`。
- `body_base64` 可还原捕获到的字节，无字节时为 `null`。`body_bytes` 是中间件观察到的总字节数，`body_truncated` 表示达到采集上限。请求的 `body_complete` 表示观察到 EOF 或已读取完整的已知长度。I/O 错误记为 `body_error`，下游返回的错误记为 `error`，下游 panic 时记录 `aborted: true` 并让 Caddy 继续处理。
- 捕获发生在 Caddy handler 边界。请求体只复制下游实际读取的字节。响应体复制写入器接受的字节，服务器后来补充的 header、链路外生成的响应和连接升级后直接写入连接的数据超出捕获范围。 `status: 0` 表示未观察到最终状态的提交，可能包括空 handler、Caddy 外层错误处理或直接写入升级握手的场景。
- 默认模式在后台汇总容量丢弃计数及写入错误，热重载时，旧配置继续记录它已经开始采集的交换。优雅关闭会排空已进入写入队列的记录。超出 Caddy 关闭宽限期的在途请求，以及强制终止时未落盘的记录，可能丢失。
- 严格模式将采集数据保留在内存中，并在 handler 返回前等待文件同步。已经流式写给客户端的响应可能先到达。磁盘写入失败会记录到 Caddy 日志，尚未提交响应且下游没有返回错误时会返回 HTTP 500。大 body 会增加内存占用，慢磁盘会提高请求延迟。

日志按原样保存凭证、Cookie、查询参数和 body。使用仅供 Caddy 和获授权读取方访问的目录。

## 测试

```sh
go test ./...
go test -race ./...
go vet ./...
CADDY_TAP_BIN=/tmp/caddy-tap go test -run '^TestCaddyBinary$' -v
```

二进制集成测试启动真实 Caddy 和本地 HTTP 上游，检查匹配范围、反向代理、流式响应、请求和响应 trailer、响应期间热重载、HTTP/1.1 连接升级、文件轮转和日志落盘。

## 许可证

[MIT](LICENSE)
