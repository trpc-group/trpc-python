# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

# Cython declarations for ServerOption and ServiceOption (implemented in _option.pyx).
# Other Cython modules ``cimport`` these types. Python callers import the compiled
# ``trpc_sdk.server._option`` module (re-exported from ``trpc_sdk.server``). Defaults,
# setter bodies, and transport/codec side effects live in the .pyx; this header
# is the C-level layout and API.


cdef class ServerOption:
    # Process-wide server options (YAML ``server:``, one per Server).
    cdef:
        cdef public str mode                    # "merge" / "separate".
        cdef public int worker_num              # Worker / executor process count.
        cdef public int worker_thread_num       # Merge-mode event-loop threads; 1 stays single-threaded.
        cdef public int rpc_task_num            # Separate-mode rpc task process count.
        cdef public int service_num             # Number of ServiceOption entries.
        cdef public int task_wait_interval      # Task-process batch wait, in ms.
        cdef public int task_batch_size         # Max tasks pulled in one batch.
        cdef public int task_queue_size         # Task-process inbound queue capacity.
        cdef public int task_consumer_num       # Consumer threads per task process.
        cdef public object on_worker_message    # Optional master -> worker callback.
        cdef public bint reboot_child           # Restart a crashed worker/task child.
        cdef public object loop                 # Event loop assigned by the engine.
        cdef public int close_wait_time         # SIGTERM graceful-shutdown wait, in ms.


cdef class ServiceOption:
    # One listen endpoint / named service (YAML ``server.service[]``).
    cdef:
        cdef public object app                  # Owning Server instance.
        cdef public str namespace               # Naming namespace: Production vs Development.
        cdef public str env_name                # Naming environment name.
        cdef public str set_name                # Polaris set name; empty unless enable_set is Y.
        cdef public str service_name            # RPC service name, e.g. trpc.app.server.Service.
        cdef public str timerSpec               # Cron spec; empty means not a timer service.
        cdef public object address              # Bind address: (host, port) or (unix_path, 0).
        cdef public object socket_type          # Socket family/type from YAML (tcp / unix / ...).
        cdef public int timeout                 # Request timeout in ms; also transport idle timeout.
        cdef public bint disable_request_timeout  # True: ignore upstream remaining timeout.
        cdef public bint enable_stream_filter   # Run stream filters on this service.
        cdef public int queue_wait_timeout      # Queue-wait timeout in ms; 0 disables the check.
        cdef public int queue_wait_policy       # Queue-wait circuit-breaker policy; 0 = off.
        cdef public int current_serialization_type  # Override serialization; -1 = protocol default.
        cdef public int current_compress_type   # Override compression; -1 = protocol default.
        cdef public str protocol                # Wire protocol name: "trpc", "http", ...
        cdef public bint handler_set            # True after a custom transport handler is installed.
        cdef public object transport_option     # Listen/transport knobs (address, network, checker, TLS).
        cdef public object transport            # Server transport implementation.
        cdef public object registry             # Service registry plugin; default is a no-op.
        cdef public int registry_delay          # Delay before registering, in ms.
        cdef public int registry_option         # Registry control: 0 = off, 1 = on.
        cdef public object codec                # Server codec for this protocol.
        cdef public object filters              # Server-side filter chain.
        cdef public object stream_handle        # Streaming RPC handler; None if unary-only.
        cdef public object stream_transport     # Streaming transport; None uses the default.
        cdef public int max_window_size         # Stream flow-control window; 0 = protocol default.

    # Naming / identity.
    cpdef void set_namespace(self, str namespace)
    cpdef void set_env_name(self, str env_name)
    cpdef void set_setname(self, str set_name)
    cpdef void set_service_name(self, str service_name)
    # Interceptors.
    cpdef void add_filter(self, object f)
    # Listen / transport.
    cpdef void set_address(self, object address)
    cpdef void set_socket_type(self, object socket_type)
    cpdef void set_server_tls(self, str cert_file, str key_file)
    cpdef void set_network(self, str network)
    cpdef void set_timeout(self, int timeout)
    cpdef void set_disable_request_timeout(self, bint disable_request_timeout)
    cpdef void set_enable_stream_filter(self, bint enable_stream_filter)
    cpdef void set_queue_wait_timeout(self, int queue_wait_timeout)
    cpdef void set_queue_wait_policy(self, int queue_wait_policy)
    cpdef void set_transport(self, object transport)
    # Registry.
    cpdef void set_registry(self, str registry_name)
    cpdef void set_registry_delay(self, int registry_delay)
    cpdef void set_registry_option(self, int registry_option)
    # Protocol / handler / timer.
    cpdef void set_protocol(self, str protocol)
    cpdef void set_handler(self, object handler)
    cpdef void set_timer(self, str spec)
    # Streaming RPC.
    cpdef void set_stream_handle(self, object stream_handle)
    cpdef void set_stream_transport(self, object stream_transport)
    cpdef void set_max_window_size(self, int window_size)
    # Codec overrides.
    cpdef void set_current_serialization_type(self, int current_serialization_type)
    cpdef void set_current_compress_type(self, int current_compress_type)
