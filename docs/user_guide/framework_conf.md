English | [中文](framework_conf.zh_CN.md)

# tRPC-Python Framework Configuration

tRPC-Python reads one YAML configuration file when the process starts. `create_server(conf_path)` calls `TrpcConfig.load`, which parses the `global`, `server`, and `client` sections and fills them into the in-process configuration object. The `plugins` section is kept as a raw dict for each plugin to read with `get_plugins_config()`. Restart the process after changing the file.

In the example [examples/helloworld/trpc_main.py](../../examples/helloworld/trpc_main.py), the entry script takes the configuration path from the `--conf` command-line argument (the default is `trpc_python.yaml` in the same directory). `--conf` is parsed by the business entry script. `create_server` itself only receives the path string.

The file has four top-level sections:

| Section | Role |
| --- | --- |
| `global` | Environment (namespace, environment name, set name), the `uvloop` switch, `rpcz` sampling, the process and thread model, and persisting listen addresses |
| `server` | This process as a server: application and server name, timeout and queue control, listen endpoints (`service`), filters, the Admin port, and related settings |
| `client` | This process as a client calling downstream: shared defaults, and each downstream `service`'s address, timeout, filters, and connection-pool parameters |
| `plugins` | A dict of plugin-specific configuration. Keys and structure are defined by each plugin |

Defaults for omitted fields: a string is `""`, an integer is `0`, a boolean is `false`, and a list is `[]`. The fields below are only those the current framework reads and applies. For the process model and timeout behavior, see [tRPC-Python Server Development Guide](server/overview.md) and [Timeout Control](timeout_control.md).

## 1. `global`

```yaml
global:
  namespace: Development            # Namespace (for example Development / Production)
  env_name: test                    # Sub-environment name
  container_name: ""                # Container name
  local_ip: ""                      # Local IP address
  enable_set: false                 # Enable set routing
  full_set_name: ""                 # Full set name (used when enable_set is true)
  enable_uvloop: false              # Use the uvloop event loop
  save_bind_info:                   # Persist listen addresses to a file
    enable: false                   # Write each bound name/ip/port to the file
    file_name: ./bind_info.log      # Output file path
  rpcz:                             # rpcz sampling and cache
    sample_rate: 1000               # Sample step (sample when span_id % sample_rate == 0; 0 turns it off)
    lower_water_level: 50           # Low watermark of samples per second
    high_water_level: 200           # High watermark of samples per second
    sample_minimum_timecost: 0      # Minimum span cost to keep, in milliseconds
    collect_interval_ms: 500        # Sample collection interval, in milliseconds
    remove_interval_ms: 1000        # Expired-span cleanup interval, in milliseconds
    print_spans_num: 10             # Number of span summaries the Admin API returns by default
    cache_expire_interval: 60       # In-memory span cache lifetime, in seconds
    service_whitelist: []           # Service-name allowlist (an empty list does not filter)
  processmodel:                     # Process and thread model
    mode: single_process            # single_process or multi_process
    io_handle_type: merge           # How I/O relates to business handling: merge or separate
    io_num: 1                       # I/O threads (single process), or I/O processes (fixed at 1 in multi-process separate mode)
    handle_num: 1                   # Business threads (single process) or business child processes (multi-process; 0 means CPU count)
    task_num: 1                     # Default child-process count for an async Task plugin
```

### Environment and the Event Loop

- **Environment and set routing:** `namespace` and `env_name` are written onto the runtime options of every server `Service`. When `enable_set` is `true`, `full_set_name` is this service's set name and is passed to the `Selector` with the call options. This repository has no naming-service implementation. Set routing and multi-environment routing are decided by an external `Selector` plugin.
- **`enable_uvloop`:** When this is `true` and the `uvloop` package is installed, the service event loop uses `uvloop`.
- **`save_bind_info`:** When `enable: true`, `create_server` appends each `service`'s bound `name ip port` to the file named by `file_name` (default `./bind_info.log`), so an external script can read a dynamically chosen port.

### `global.rpcz`

`global.rpcz` configures `rpcz` sampling and caching. `rpcz` also requires the filter name `rpcz` in `server.filter` or `client.filter` (see [How to Write a Filter Plugin](../developer_guide/develop_plugins/filter.md); the query API is in [Admin Commands](admin.md)).

| Field | Default | Meaning |
| --- | --- | --- |
| `sample_rate` | `1000` | Proportional sample step. A span is sampled only when `span_id % sample_rate == 0`. `0` turns proportional sampling off |
| `lower_water_level` | `50` | Low watermark of samples per second. When the count of samples already taken this second is below this value, a span is kept even if it misses the `sample_rate` ratio |
| `high_water_level` | `200` | High watermark of samples per second. Extra sampling pauses after this limit |
| `sample_minimum_timecost` | `0` | Minimum cost, in milliseconds. A span cheaper than this is not kept |
| `print_spans_num` | `10` | Number of span summaries returned by the Admin API `GET /cmds/rpcz` |
| `cache_expire_interval` | `60` | How long a span stays in the in-memory cache, in seconds |
| `service_whitelist` | `[]` | Service-name allowlist. An empty list does not filter by service name |

### `global.processmodel`

`global.processmodel` configures the process and thread model. Without `mode`, the default is one process and one event loop.

| `mode` | `io_handle_type` | Effect |
| --- | --- | --- |
| `single_process` | `merge` (default) | **Single-process merge:** start `io_num` threads. Each thread does network I/O and runs business handlers. `io_num` and `handle_num` must match (if only one is set, both take that value; if they differ, a warning is logged and `handle_num` is used) |
| `single_process` | `separate` | **Single-process thread separation:** in one process, start `io_num` I/O threads plus `handle_num` business handle threads (`handle_num` must be greater than `0`) |
| `multi_process` | `separate` (default) | **Multi-process leader/worker separation:** one main I/O process receives packets and splits frames, then hands them over pipes to `handle_num` `RpcTaskProcess` business child processes (`io_num` only supports `1`; any other value logs a warning and `1` is used) |
| `multi_process` | `merge` | **Multi-process peer mode:** start `handle_num` equal Worker processes. Each binds every listen address with `SO_REUSEPORT` (`io_num` has no effect) |

Notes:

- Under `multi_process`, `handle_num: 0` starts one child process per available CPU on the machine.
- `task_num` is the default child-process count for an async task (a Task plugin) that does not set its own process count. `0` or omitted means `1`.

## 2. `server`

```yaml
server:
  app: test                         # Application name
  server: Greeter                   # Server process name
  bin_path: /usr/local/trpc/bin/    # Executable directory
  conf_path: /usr/local/trpc/conf/  # Configuration directory
  data_path: /usr/local/trpc/data/  # Data directory
  handler_max_concurrency: 0        # Max in-flight requests per handle thread (0 means no limit; single-process separate mode only)
  handler_queue_size: 1024          # Overflow wait-queue length after the concurrency limit
  tcp_reuse_port: false             # Enable SO_REUSEPORT on the TCP listen socket
  ready_timeout: 0                  # Ready timeout for a worker thread or child process, in milliseconds (0 means 300000)
  close_wait_time: 0                # Graceful-exit wait for in-flight requests, in milliseconds
  io_wait_count: 0                  # inc_io_wait_count calls to wait for before listening (0 means no wait)
  heartbeat_interval: 0             # Parent heartbeat check of child processes, in seconds (0 turns it off)
  check_timeout: false              # Enable the server full-link timeout check
  check_idle_timeout: 0             # Idle-connection check interval, in milliseconds (0 turns it off)
  queue_wait_timeout: 0             # Max time a request may wait in the queue, in milliseconds
  queue_wait_policy: 0              # Queue timeout policy (0 drops immediately; or combine with queue_cb)
  queue_cb:                         # Queue-timeout circuit breaker
    ratio: 0.1                      # Timeout-ratio threshold that trips the breaker
    threshold: 100                  # Minimum sample count inside the window
    interval: 60000                 # Statistics window, in milliseconds
  registry_delay: 0                 # Delay before registering with the naming service after startup, in milliseconds
  registry_option: 0                # Service registration switch (0 off, 1 on)
  max_rbuf_size: 8388608            # Max read buffer per connection, in bytes (default 8 MiB)
  filter: []                        # Shared server filter list
  admin:                            # Admin management service
    ip: 127.0.0.1                   # Admin listen IP
    port: 9028                      # Admin listen port
  memory_inspection:                # Memory-allocation snapshot (triggered by SIGUSR2)
    enabled: false                  # Enable tracemalloc
    filename_prefix: tracemalloc    # Snapshot file-name prefix
    line_limit: 50                  # Max statistic lines written per snapshot
  service:                          # Server listen endpoints
    - name: trpc.trpc_python_group.helloworld.Greeter  # Registered service name (trpc.{app}.{server}.{service})
      ip: 127.0.0.1                 # Listen IP
      port: 8000                    # Listen port (0 lets the OS pick a free port)
      network: tcp                  # Transport (tcp / udp)
      protocol: trpc                # Application protocol (trpc / http, or a custom protocol)
      timeout: 5000                 # Per-request processing budget, in milliseconds
      disable_request_timeout: false  # Ignore the remaining timeout carried in the upstream frame
```

### Common Server Fields

- **Identity and directories:** `app` and `server` are the application name and process name of this service. `bin_path`, `conf_path`, and `data_path` are path fields agreed by the business. After parsing, they hang on `ServerConfig` for business code to read.
- **Concurrency and queue limits:** `handler_max_concurrency` and `handler_queue_size` take effect only under `single_process` + `io_handle_type: separate`. See [tRPC-Python Architecture](../developer_guide/architecture.md) for each mode.
- **Graceful exit and heartbeat:**
  - `close_wait_time` (milliseconds): after a stop signal, the process stops `accept`ing new connections and keeps writing responses for requests already in progress during this window.
  - `heartbeat_interval` (seconds): when greater than `0`, the parent process checks child-process heartbeats at this interval. `0` turns the check off.
- **Startup barrier (`io_wait_count`):** when greater than `0`, the process that owns the listen sockets binds no address until task processes have called `inc_io_wait_count()` this many times in total, for example while a Task loads a model. Peer mode has no task, so it logs a warning and ignores the value.
- **Admin (`admin`):** When both `admin.ip` and `admin.port` are non-empty, the root process starts a separate thread after `fork` to serve Admin HTTP. See [Admin Commands](admin.md).
- **Memory snapshots (`memory_inspection`):** When `enabled: true`, a Worker turns on the standard-library `tracemalloc` before the event loop starts. Sending `SIGUSR2` to that Worker writes `{filename_prefix}_{pid}_{index}.txt` in the current working directory. The first signal dumps allocations since startup. Each later signal dumps the delta from the previous snapshot. `line_limit` is the maximum number of statistic lines written each time (default `50`). `filename_prefix` defaults to `tracemalloc`.
- **Timeout and other controls:** How `check_timeout`, `queue_wait_timeout`, `queue_wait_policy`, and `queue_cb` work together is in [Timeout Control](timeout_control.md). `registry_delay` and `max_rbuf_size` are in the [tRPC-Python Server Development Guide](server/overview.md).

### `server.service[]` Listen Endpoints

| Field | Meaning |
| --- | --- |
| `name` | Service name registered with the naming service, commonly `trpc.{app}.{server}.{service}` |
| `ip` / `port` | Listen IP and port. When `port` is `0`, the operating system picks a free port |
| `nic` | NIC name. When it is set together with `ip`, `ip` wins |
| `address` | A full listen-address string. When set, it overrides `ip:port` |
| `network` | Transport, `tcp` or `udp` |
| `protocol` | Protocol name. Built-in values are `trpc` and `http`, or the name of a registered custom protocol plugin |
| `socket_type` | Socket type. The default is `net` (a network socket). `unix` uses a Unix domain socket, and the path is `unix_path` |
| `timeout` | Per-request processing budget of this service, in milliseconds |
| `disable_request_timeout` | When `true`, ignore the remaining timeout the upstream put in the request frame and use only this service's `timeout` |
| `idletime` | Idle timeout of one connection, in milliseconds. `server.check_idle_timeout` is a separate service-level idle check |
| `filter` | Filter names for this service, appended after the shared `server.filter` list |
| `tls_key` / `tls_cert` | TLS private key and certificate paths for an HTTP service. They must be set as a pair |
| `registry` | Naming-service registry plugin name |
| `max_window_size` | Streaming RPC receive window, in bytes. The default is `65535` |
| `enable_stream_filter` | Whether a streaming RPC goes through the filter `run` method |

## 3. `client`

```yaml
client:
  timeout: 1000                     # Fallback client timeout, in milliseconds (used only when no service entry matches)
  namespace: Development            # Default callee namespace
  filter: []                        # Shared client filter list
  service:                          # Downstream service list
    - name: trpc.test.helloworld.Greeter  # Downstream service name (also the lookup key when callee is omitted)
      network: tcp                  # Transport (tcp / udp)
      protocol: trpc                # Application protocol (trpc / http, or a custom protocol)
      timeout: 800                  # Per-call timeout for this downstream service, in milliseconds
      target: ip://127.0.0.1:8000   # Target URI (scheme://endpoint)
```

### Lookup and Inheritance

- **Lookup key:** When loading `client.service[]`, the framework uses `callee` as the lookup key if it is set, and `name` otherwise.
- **Defaults, and the special rule for `client.timeout`:** When no `client.service[]` entry matches the callee service name, the call falls back to the shared settings on the top-level `client` section (`client.timeout` is the fallback timeout in that case). If a matching `client.service[]` entry is found but that entry omits `timeout`, the entry's `timeout` stays `0` (this call does not set another client deadline, and it does **not** inherit `client.timeout`).

### Common `client.service[]` Fields

| Field | Meaning |
| --- | --- |
| `callee` | Callee service name the stub writes onto the `message` (used to match this YAML entry) |
| `name` | Name this target service is registered under on the naming service |
| `target` | Target URI, `scheme://endpoint`. Built-in schemes are `ip`, `http`, `https`, `dns`, and `dsn` |
| `namespace` / `env_name` / `set_name` | Namespace, target environment name, and target set name passed to the routing `Selector` |
| `network` / `protocol` | Network and protocol. The defaults are `tcp` and `trpc` |
| `timeout` | Client per-call timeout, in milliseconds. `0` means the client does not set another deadline |
| `serialization` / `compression` | Integer enums for serialization and compression. `0` means Protobuf (`PB`) and no compression (`Noop`), respectively |
| `filter` | Client filters for this downstream service, appended after the shared `client.filter` list |
| `discovery` / `loadbalance` / `circuitbreaker` | Service-discovery, load-balancing, and circuit-breaker plugin names used when `target` is not set and the call goes through the `TrpcSelector` chain |
| `password` | Downstream authentication password. A custom protocol transport (for example a database plugin) reads it from `options.password` |
| `short_conn` | When `true`, use a short connection: open a new connection for each request, close it when finished, and do not enter the connection pool |
| `dial_timeout` | Connection-establishment timeout, in milliseconds. The default is `200` |
| `idle_time` | Maximum time an idle pooled connection is kept, in seconds. The default is `60` |
| `max_idle_conn` | Maximum number of idle connections kept in the pool. The default is `5` |
| `max_active_conn` / `is_wait_active_conn` | Maximum number of active connections in the pool, and whether to wait in a queue after that limit |
| `max_conn_lifetime` | Maximum lifetime of a pooled connection since it was created, in seconds. `0` means no limit |
| `disable_reuse_http_session` | Whether the HTTP client is forbidden from reusing one `ClientSession` across URLs |
| `enable_stream_filter` | Whether a client streaming call goes through the filter `run` method |

Connection-pool and short-connection behavior is in [Client Connection Modes](client/connection_mode.md). The full client call flow is in the [Client Development Guide](client/overview.md).

## 4. `plugins` and Logging

`plugins` is a free-form dict. The framework does not construct objects from the keys under `plugins`. A plugin registers when its code is `import`ed, then reads its own section from `plugins` during initialization.

### Built-in Log Configuration (`plugins.log`)

The default log backend is `loguru`. The settings that take effect directly under `plugins.log` are the flat `writer` and `level`:

```yaml
plugins:
  log:                              # Log plugin
    default:                        # Default logger name (and its output list)
      - writer: console             # Output plugin name (built-in: console / file)
        level: info                 # Level: trace / debug / info / warning / error / fatal
```

- **`level` values:** `trace`, `debug`, `info`, `warning`, `error`, and `fatal` (`trace` and `debug` both map to standard `logging.DEBUG`).
- Nested `format_config` and `writer_config` are not converted into configuration objects today. Field limits and how to write a custom writer are in [How to Write a Log Writer Plugin](../developer_guide/develop_plugins/log.md).

Business code can import the default `logger` directly. To attach fields from the current request, call `with_context_fields(ctx, ...)` first, then log with a `*_context` function:

```python
from trpc_sdk.log import info_context
from trpc_sdk.log import logger
from trpc_sdk.log import with_context_fields

logger.info("listening")
with_context_fields(ctx, {"trace_id": "abc"})
info_context(ctx, "say hello %s", request.msg)
```

`get_logger(name)` returns the named logger configured under `plugins.log` (an unregistered name falls back to `default`). To change the log level while the service is running, use `PUT /cmds/loglevel` in [Admin Commands](admin.md).

> To load a business configuration file of your own, rather than `trpc_python.yaml`, use `ConfigLoaderFactory`. See [How to Write a Config Plugin](../developer_guide/develop_plugins/config.md).
