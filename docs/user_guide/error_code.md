English | [中文](error_code.zh_CN.md)

# Error Codes

In tRPC-Python, framework errors and business errors are both a `TrpcException`. It has three fields: `type` (error type), `code` (error code), and `msg` (description).

## Error Types and Constructors

`type` comes from the enum `TrpcErrorType` and distinguishes where the error came from:

| Value | Name | Meaning |
| --- | --- | --- |
| `1` | `FRAMEWORK` | Local framework error |
| `2` | `BUSINESS` | Business logic error |
| `3` | `CALLEE_FRAMEWORK` | Downstream (callee) framework error |

`code` comes from the enum `TrpcErrorCode`. Standard protocol codes are loaded from `trpc.TrpcRetCode` at runtime, and the numbers match `trpc.proto`:

- **Raise a business error** with `create_business_error(code, msg)`. `type` is fixed to `BUSINESS`, and `code` is chosen by the business.
- **Raise a framework error** with `create_frame_error(code, msg)`, or raise one of the exception classes in the tables below.

When a server handler raises `TrpcException`, the framework encodes the code and description into the response frame (the response body is empty) and returns it to the client. The current Worker keeps running and continues to handle later requests. Any other Python exception is wrapped as `UnknownError` (code `999`) and returned.

```python
from trpc_sdk.exceptions import create_business_error

async def SayHello(self, ctx, request):
    if not request.msg:
        raise create_business_error(10001, "empty msg")
```

## Server Error Codes

| Code | Name | Exception class | Description |
| --- | --- | --- | --- |
| `0` | `TRPC_INVOKE_SUCCESS` | — | Call succeeded |
| `1` | `TRPC_SERVER_DECODE_ERR` | `ServerDecodeError` | Server protocol-frame decode or deserialization failed |
| `2` | `TRPC_SERVER_ENCODE_ERR` | `ServerEncodeError` | Server response encode or serialization failed |
| `11` | `TRPC_SERVER_NOSERVICE_ERR` | — | The requested service name does not exist |
| `12` | `TRPC_SERVER_NOFUNC_ERR` | `ServerFuncNotFoundError` | The requested RPC method is not registered |
| `21` | `TRPC_SERVER_TIMEOUT_ERR` | `ServerTimeoutError` | Server processing timeout or queue-wait timeout |
| `22` | `TRPC_SERVER_OVERLOAD_ERR` | — | Server overload (protocol reserved code) |
| `23` | `TRPC_SERVER_LIMITED_ERR` | — | Server rate limit (protocol reserved code) |
| `24` | `TRPC_SERVER_FULL_LINK_TIMEOUT_ERR` | — | Server full-link timeout |
| `31` | `TRPC_SERVER_SYSTEM_ERR` | `ServerSystemError` | Server internal system error |
| `41` | `TRPC_SERVER_AUTH_ERR` | — | Server authentication failed (protocol reserved code) |
| `51` | `TRPC_SERVER_VALIDATE_ERR` | — | Server parameter validation failed (protocol reserved code) |

> `22`, `23`, `41`, and `51` are standard reserved tRPC protocol codes for external plugins such as overload protection, rate limiting, authentication, and parameter validation. This core repository does not ship built-in implementations of those plugins.

## Client Error Codes

| Code | Name | Exception class | Description |
| --- | --- | --- | --- |
| `101` | `TRPC_CLIENT_INVOKE_TIMEOUT_ERR` | `ClientTimeoutError` | Client call timeout |
| `102` | `TRPC_CLIENT_FULL_LINK_TIMEOUT_ERR` | — | Client full-link timeout |
| `111` | `TRPC_CLIENT_CONNECT_ERR` | `ClientConnectionError` | Client failed to establish a connection |
| `121` | `TRPC_CLIENT_ENCODE_ERR` | `ClientEncodeError` | Client request encode or serialization failed |
| `122` | `TRPC_CLIENT_DECODE_ERR` | `ClientDecodeError` | Client response decode or deserialization failed |
| `123` | `TRPC_CLIENT_LIMITED_ERR` | — | Client rate limit |
| `124` | `TRPC_CLIENT_OVERLOAD_ERR` | — | Client overload |
| `131` | `TRPC_CLIENT_ROUTER_ERR` | `ClientRouteError` | Client naming-service route selection failed |
| `141` | `TRPC_CLIENT_NETWORK_ERR` | — | Client network error |
| `151` | `TRPC_CLIENT_VALIDATE_ERR` | `ClientPoolLimitError` | Client connection pool reached the active-connection limit (reuses protocol code `151`) |
| `152` | `TRPC_CLIENT_POOL_CLOSED` | `ClientPoolClosedError` | Client connection pool is closed (Python extension code) |
| `153` | `TRPC_CLIENT_POOL_CONN_CLOSED` | `ClientPoolConnClosedError` | A connection in the pool is closed (Python extension code) |
| `161` | `TRPC_CLIENT_CANCELED_ERR` | — | Client call was canceled |
| `171` | `TRPC_CLIENT_READ_FRAME_ERR` | — | Client failed to read a protocol frame |
| `999` | `TRPC_INVOKE_UNKNOWN_ERR` | `UnknownError`, `ClientInvokeError` | Unclassified unknown error |

> `152` and `153` are tRPC-Python connection-pool extension codes and are not defined in the standard `trpc.proto`. `ClientPoolLimitError` uses protocol code `151`.

## Streaming Error Codes

Streaming RPC codes also come from `TrpcRetCode`. Server stream codes are `201`–`255`, client stream codes are `301`–`361`, and an unclassified stream error is `1000` (`TRPC_STREAM_UNKNOWN_ERR`):

| Code (server / client) | Name | Description |
| --- | --- | --- |
| `201` / `301` | `TRPC_STREAM_SERVER_NETWORK_ERR` / `TRPC_STREAM_CLIENT_NETWORK_ERR` | Streaming network error |
| `211` / `311` | `TRPC_STREAM_SERVER_MSG_EXCEED_LIMIT_ERR` / `TRPC_STREAM_CLIENT_MSG_EXCEED_LIMIT_ERR` | Streaming message exceeds the size limit |
| `221` / `321` | `TRPC_STREAM_SERVER_ENCODE_ERR` / `TRPC_STREAM_CLIENT_ENCODE_ERR` | Streaming frame encode failed |
| `222` / `322` | `TRPC_STREAM_SERVER_DECODE_ERR` / `TRPC_STREAM_CLIENT_DECODE_ERR` | Streaming frame decode failed |
| `231`–`234` / `331`–`334` | `TRPC_STREAM_*_WRITE_*` | Stream write error, in order: write end, write buffer overflow, write connection closed, write timeout |
| `251`–`255` / `351`–`355` | `TRPC_STREAM_*_READ_*` / `*_IDLE_TIMEOUT_ERR` | Stream read error, in order: read end, read connection closed, read empty, read timeout, stream idle timeout |
| `361` | `TRPC_STREAM_CLIENT_INIT_ERR` | Client stream initialization failed |

## Python Framework Extension Codes

The following codes are specific to the tRPC-Python runtime:

| Code | Name | Exception class | When it is raised |
| --- | --- | --- | --- |
| `200` | `TRPC_TASK_HANDLE_ERR` | `TaskHandleError` | A Task child process failed to handle the task (the Worker cannot find the Task pipe, the wait inside the Task process timed out, the task queue is full, the return value is illegal, or the user task function raised) |
| `202` | `TRPC_PLUGIN_INIT_ERR` | `PluginInitError` (`trpc_sdk.plugin`) | Plugin initialization failed |
| `203` | `TRPC_CONFIG_ERR` | `ConfigError` (`trpc_sdk.config`) | Configuration file load or parse failed |
