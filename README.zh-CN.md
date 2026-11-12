# caddy-tap

[English](README.md) | 简体中文

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

推荐把共用的 tap 配置集中写在一个 [Caddyfile 片段（snippet）](https://caddyserver.com/docs/caddyfile/directives/import) 中，再在需要采集的入口用 `import` 引用。以下示例使用同一套设置采集 `/api/*` 和 `/webhooks/*`，每次引用只传入请求匹配器。

```caddyfile
(capture_api) {
    tap {args[0]} {
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
}

:8080 {
    @api path /api/*
    @webhooks path /webhooks/*
    import capture_api @api
    import capture_api @webhooks

    reverse_proxy localhost:9000
}
```

```sh
/tmp/caddy-tap validate --config Caddyfile --adapter caddyfile
/tmp/caddy-tap run --config Caddyfile --adapter caddyfile
```

| 参数             | 默认值   | 行为                                                                                                     |
| ---------------- | -------- | -------------------------------------------------------------------------------------------------------- |
| `identity`       | 必填     | 1–64 个 ASCII 字母、数字、下划线或连字符，以字母或数字开头。同一目录中的路由可通过相同标识共用日志文件。 |
| `directory`      | 必填     | 输出目录。相对路径以 Caddy 进程的工作目录为准。Unix 上新建目录的权限为 `0700`。                          |
| `mode`           | `drop`   | `drop` 在容量用尽时丢弃整条采集记录。 `strict` 等待容量，关闭 body 截断，并等待日志文件同步。            |
| `queue_size`     | `64`     | 同一目录和标识下允许同时采集、排队或写入的交换总数，范围 1–65536。                                       |
| `max_body_bytes` | `1MiB`   | 默认模式下请求体、响应体各自的字节上限。日志仍记录观察到的字节总数，并设置截断标记。严格模式应省略此项。 |
| `roll_size`      | `100MiB` | 追加一条记录会超过阈值时，先滚动到新文件。单条记录超过阈值时，独占一个文件。                             |
| `roll_interval`  | 关闭     | 当前文件创建后达到指定时长即封存，即使没有新记录。支持 `24h`、`1d` 等时长。                              |
| `roll_keep`      | `10`     | 同一目录、同一标识最多保留的已封存文件数。设为 `-1` 关闭该限制。                                         |
| `roll_keep_for`  | `2160h`  | 以文件名中的 UTC 时间判断已封存文件的最大保留时长。设为 `-1` 关闭该限制。                                |

大小参数支持字节数和 `KiB`、`MiB` 等单位。保留策略在新文件的首条记录成功写入后和文件封存后执行，只处理已封存文件。同一个 UTC 秒内创建的文件，按最后修改时间优先保留较新的文件。

### 严格模式的副作用

- **可能增加请求延迟。** 共享采集槽位用完时，严格模式的请求会先等到有可用采集容量，再调用下游 handler。tap handler 返回前还会等待记录写入和文件同步，慢磁盘会进一步增加延迟。流式响应的数据可能在同步完成前到达客户端。
- **可能显著增加内存占用。** 严格模式关闭 body 采集上限，把观察到的请求和响应 body 保留在内存中。大 body 和长时间运行的流会消耗更多内存。
- **磁盘故障可能导致 HTTP 500。** 持久化失败时，tap 会记录错误。如果响应尚未提交且下游 handler 没有返回错误，tap 会返回 HTTP 500。

如果内层严格模式 tap 属于另一个写入组且该组的采集槽位已满，内层会返回 HTTP 503，避免在外层持有槽位时继续等待。请求在等待采集容量时取消，会结束等待。已入队的记录仍会继续写入。Windows 上严格模式会同步日志文件，系统崩溃或断电时，目录项仍可能丢失。

## JSON 配置

将该对象放入 HTTP 路由的 `handle` 数组，置于需要观察的 handler 前。路径匹配器属于该路由的 `match` 字段。JSON 大小参数为字节整数，时长可用字符串或纳秒整数。JSON 中使用数字 `-1` 关闭 `roll_keep_for`，在 Caddyfile 中使用 `roll_keep_for -1`。

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

## 日志与捕获

### 日志文件与上传

在同一个 Caddy 进程中，解析后的输出目录和 `identity` 相同的 tap 定义共用一个队列、一个写入 worker 和一个当前 `.ndjson.part`。任一路由的流量都会占用共享队列容量，可用 `request.url` 区分记录来源。

- **`queue_size` 或任意 `roll_*` 设置不同。** Caddy 拒绝加载配置。如果热重载失败，旧配置会继续运行。修改正在运行的组的这些设置需要重启 Caddy。
- **`mode` 或 `max_body_bytes` 不同。** 各路由使用自己的采集设置，仍共用写入 worker。如果同一个 HTTP 请求进入同组的多个 tap handler，只由最外层实际开始采集的 handler 按自己的设置记录一次。
- **解析后的输出目录不同。** 即使 `identity` 相同，各目录也有独立的队列、写入 worker 和文件。

写入中的文件形如 `api-20261111T120000-123456789.ndjson.part`，在 Unix 上权限为 `0600`。Windows 上的访问权限由输出目录的 ACL 决定。按大小或时间滚动，以及优雅关闭时，先同步并关闭文件，再在同一目录内将其改名为 `api-20261111T120000-123456789.ndjson`。同步、关闭文件或改名失败时会保留 part，等待恢复。

启动后收到首条采集记录时，写入 worker 会锁定该组并检查遗留的 part。它保留完整的 UTF-8 JSON 行，截掉末尾半行，再续写最近修改的可恢复文件。续写沿用文件原始创建时间和已有字节数，仍按配置滚动。更早的可恢复文件会封存，空 part 会删除，完整行损坏或出现 I/O 错误的文件会原地保留以便检查。多个 Caddy 进程请使用不同的目录或标识，并保留隐藏的 `.tap-<identity>.lock` 文件。配置校验不会改动已有日志。

上传进程只需处理已封存的 `*.ndjson` 文件。若从直接写入 `.ndjson` 的旧版本升级，请先停止旧进程再启动上传监听。启动时扫描一次，并定期补扫，以覆盖目录监听漏掉的事件。若每个文件都必须保留到上传成功，请将 `roll_keep` 和 `roll_keep_for` 都设为 `-1`，由上传进程在成功后删除本地文件。

### 日志格式与捕获边界

- 每行包含 `identity`、`timestamp`、`duration_ns`、`request` 和 `response`。请求字段包含 `method`、`url`、`request_uri`、`host`、`scheme`、`protocol`、`remote_addr`、`content_length` 和可用的 `transfer_encoding`。相对形式的 URL 可结合 `scheme` 和 `host` 使用。请求和响应的 `header`、`trailer` 以字符串数组保留重复值。响应另有 `status` 和可用的 `informational`、`hijacked`。
- 当 header 或 trailer 的某个值包含非 UTF-8 字节时，`header_base64` 或 `trailer_base64` 保存对应名称下的全部原始值，顺序和文本字段一致。对于非 UTF-8 的 URL 或请求目标，使用 `url_base64` 或 `request_uri_base64` 还原原始字节。
- `body_base64` 可还原捕获到的字节，无字节时为 `null`。`body_bytes` 是中间件观察到的总字节数，`body_truncated` 表示观察到的数据超过采集上限。请求的 `body_complete` 表示观察到 EOF 或已读取完整的已知长度。I/O 错误记为 `body_error`，下游返回的错误记为 `error`，下游 panic 时记录 `aborted: true` 并让 Caddy 继续处理。
- 捕获发生在 Caddy handler 边界。请求体只复制下游实际读取的字节。响应体复制写入器接受的字节，服务器后来补充的 header、链路外生成的响应和连接升级后直接写入连接的数据超出捕获范围。 `status: 0` 表示未观察到最终状态的提交，可能包括空 handler、Caddy 外层错误处理或直接写入升级握手的场景。
- 默认模式在后台汇总容量丢弃计数并报告写入错误。热重载时，旧配置继续记录它已经开始采集的交换。优雅关闭会排空已进入写入队列的记录。超出 Caddy 关闭宽限期的在途请求，以及强制终止时未落盘的记录，可能丢失。

日志按原样保存凭证、Cookie、查询参数和 body。使用仅供 Caddy 和获授权读取方访问的目录。

### Server-Sent Events（SSE）

对 `Content-Type: text/event-stream` 的响应，tap 将事件照常转发并透传 flush。日志在 handler 返回时生成，每个采集到的 SSE 请求对应一条记录。解码 `response.body_base64` 可还原捕获的 SSE 字节，包括心跳注释、换行、事件分隔符和多行数据。客户端断开后，如果 handler 随之返回，日志保留此前写入器接受的字节。

默认模式下，长流超过 `max_body_bytes` 时只截断日志，客户端仍可继续接收事件。**严格模式保留全部可观察字节，并在流结束前将它们保存在内存中。**持续开放的连接会占用采集槽位，期间不会生成逐事件日志。

## 测试

```sh
make test
```

该命令运行测试并开启覆盖率统计和竞态检测，随后执行 `go vet`。要同时运行 Caddy 二进制集成测试，请按上文构建二进制文件后执行 `CADDY_TAP_BIN=/tmp/caddy-tap make test`。

## 许可证

[MIT](LICENSE)
