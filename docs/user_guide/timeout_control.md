English | [中文](timeout_control.zh_CN.md)

# Timeout Control

One RPC call can have three time budgets at once, all in **milliseconds**. The framework takes the tightest one (the smallest value) as the effective timeout, and writes the time still available into the frame sent downstream:

1. **Upstream budget.** The `timeout` carried in the request frame header. The current service inherits it by default. With `service.disable_request_timeout: true` in the server configuration, the upstream value is ignored and only this service's own `service.timeout` is used.
2. **This service's processing budget.** `server.service[].timeout`. When it exists together with the upstream budget, the smaller one is used. `0` means this service sets no processing upper bound.
3. **Downstream client-call budget.** `client.service[].timeout`, or the timeout set in code with `Options.set_timeout` (code overrides YAML). `0` means this client call does not set another deadline.

Example:

```yaml
server:
  check_timeout: false
  queue_wait_timeout: 0
  queue_wait_policy: 0
  service:
    - name: trpc.app.server.service
      timeout: 1000
      disable_request_timeout: false

client:
  timeout: 1000
  service:
    - name: trpc.app.server.downstream
      timeout: 500
```

> The top-level `client.timeout` is only a fallback. It applies when no `client.service[]` entry is found for the callee service name. If a `client.service[]` entry omits `timeout`, that service's timeout stays `0`.

## How the Client Chooses a Deadline, and Who the Timeout Is Charged To

When the client starts a call and the configured timeout is greater than `0`, the framework compares it with the current context deadline `ctx.deadline` and uses whichever expires sooner:

- **Downstream timeout vs. upstream timeout:**
  - If the client's own timeout expires sooner (or the upstream context has no `deadline`), a later timeout is charged to the **downstream call**.
  - If the upstream `deadline` expires sooner, the timeout is charged to the **current service**. It is not reported to the naming-service `Selector` as a failure of the downstream node.
  - If the time left is already less than or equal to `0` before the call is sent, the call is aborted before any network request and raises `ClientTimeoutError` (code `101`).
- **Reading the time left, and which context to pass:**
  - Business code can read the timeout on the current context with `ctx.get_timeout_ms()`, `get_timeout_us()`, and `get_timeout_sec()`. A return value `<= 0` means unlimited.
  - Stub code clones the context by default (`need_clone_ctx=True`), so the caller's `ctx` is not modified by a single downstream call. To share the remaining budget of the current request chain with the downstream call, pass the `ctx` received by the server handler into the client call.

## Server Queue-Wait Checks

With `check_timeout: true` on the server, the framework checks how long the request has already waited in the queue before it enters the business handler:

- **Policy `0` (`queue_wait_policy: 0`, drop at a fixed threshold):**
  - Once the queue wait reaches `queue_wait_timeout`, the framework returns `ServerTimeoutError` (code `21`) and does not run the business handler.
  - When `queue_wait_timeout` is `0`, the merged total timeout of this request is the queue-wait upper bound. If that merged timeout is also `0`, the check is not applied.
- **Policy `1` (`queue_wait_policy: 1`, drop only when the sliding window is overloaded):**
  - The request is dropped only when the wait has reached the threshold **and** the sliding window reports overload.
  - Sliding-window parameters are under `server.queue_cb`: `ratio` (timeout-ratio threshold, default `0.1`), `threshold` (minimum sample count, default `100`), and `interval` (window length, default `60000` milliseconds).

A timeout during server processing also raises `ServerTimeoutError` (code `21`). The protocol enum defines full-link timeout codes `24` and `102`, but timeout exceptions raised by this process itself use `21` (server) and `101` (client). See [Error Codes](error_code.md) for the full table.

## Idle Timeout

`server.check_idle_timeout` and `server.service[].idletime` control how long a TCP connection can go without new data before it is closed (milliseconds; they take effect when greater than `0`). They control the connection lifetime, not the processing budget of a single RPC.

> Once a `Service` registers a streaming RPC method, the framework turns off the idle-connection timeout check on that `Service`'s transport, so a long-lived stream with a long call or infrequent traffic is not closed by mistake.
