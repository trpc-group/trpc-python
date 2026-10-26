English | [中文](connection_mode.zh_CN.md)

# Client Connection Modes

In tRPC-Python, TCP and Unix Domain Socket transports for the `trpc` protocol use a **connection pool** by default and reuse long connections. Setting `short_conn` to `True` switches to **short-connection** mode: each request opens a new connection and closes it as soon as the request ends.

Keep the following transport behavior in mind:

- **Streaming calls force a long connection.** A streaming RPC forces `short_conn` to `False` before it takes a connection, because later frames of the same stream must stay on that TCP connection.
- **HTTP is managed separately.** The HTTP client does not use the TCP pool above. It manages connections with `aiohttp.ClientSession` (see the end of this page).

## 1. Connection Pool (Default)

By default the client keeps downstream connections in a pool. Each event loop has its own pool manager and stores connections by downstream address. A request holds the connection exclusively while it runs. When the request finishes, the connection returns to the idle queue for a later request.

Pool parameters live on `ConnPoolOptions`. Set them in YAML or with `Options.set_pool_opts`. The fields correspond as follows:

| YAML field | `ConnPoolOptions` parameter | Default | Meaning |
| --- | --- | --- | --- |
| `dial_timeout` | `dial_timeout` | `200` | Dial timeout, in milliseconds |
| `idle_time` | `idle_timeout` | `60` | How long an idle connection stays in the pool, in seconds. `0` means idle time does not reclaim it |
| `max_conn_lifetime` | `max_conn_lifetime` | `0` | Maximum lifetime from creation, in seconds. `0` means no limit |
| `max_idle_conn` | `max_idle` | `5` | Maximum length of the idle queue. `0` means no upper bound |
| `max_active_conn` | `max_active` | `0` | Upper bound on connections that exist at the same time. `0` means no limit |
| `is_wait_active_conn` | `wait` | `false` | What happens when the active limit is reached. `true` waits for a free connection. `false` raises `ClientPoolLimitError` |

Set pool parameters in code:

```python
from trpc_sdk.client import Options
from trpc_sdk.transport import ConnPoolOptions

options = (
    Options()
    .set_target("ip://127.0.0.1:8000")
    .set_protocol("trpc")
    .set_pool_opts(ConnPoolOptions(max_idle=8, idle_timeout=30, max_active=32, wait=True))
)
```

Set pool parameters in `trpc_python.yaml`:

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      target: ip://127.0.0.1:8000
      network: tcp
      protocol: trpc
      dial_timeout: 200
      idle_time: 60
      max_conn_lifetime: 0
      max_idle_conn: 5
      max_active_conn: 0
      is_wait_active_conn: false
```

## 2. Short-Connection Mode

When `short_conn` is `True`, that request uses `ShortConnFactory`: a new TCP connection is opened before the request, closed as soon as the response arrives, and not returned to the idle queue. This fits an infrequent one-shot call, or a downstream service that cannot handle several requests in a row on one connection.

Turn on short connections in code (this overrides YAML):

```python
options = Options().set_target("ip://127.0.0.1:8000").set_protocol("trpc").set_short_conn(True)
```

Turn on short connections in YAML:

```yaml
client:
  service:
    - name: trpc.test.helloworld.Greeter
      target: ip://127.0.0.1:8000
      protocol: trpc
      short_conn: true
```

## 3. HTTP Client Connection Reuse

The HTTP client manages connections with `aiohttp.ClientSession`. It does not go through the tRPC TCP pool above.

- **Shared session by default.** All HTTP requests share one `ClientSession`.
- **One session per URL (`disable_reuse_http_session: true`).** With this on, a `ClientSession` is created and kept per target URL. Different target addresses no longer share one session.
- **One-shot short connection (`short_conn: true`).** With this on, the `ClientSession` used by each HTTP request is closed as soon as that request ends.

In code:

```python
options = (
    Options()
    .set_protocol("http")
    .set_disable_reuse_http_session(True)
)
```

In YAML:

```yaml
client:
  service:
    - name: example.http
      protocol: http
      disable_reuse_http_session: true
```

To send an HTTP request directly by URL, see the [HTTP Client](http.md).
