# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

# cython: language_level=3, boundscheck=False, wraparound=False

"""Per-service handler path after the transport has a complete request frame.

Transport delivers a complete ``req_buf``. This module owns everything that
happens next: codec decode/encode, timeout-budget merge, queue-wait circuit
breaking, the filter chain, and encoding the response. Errors are written
into the response frame rather than raised to the transport.

Naming-service registration lives in :mod:`trpc_sdk.server._registry`;
a :class:`ServiceAdapter` only holds the ``registrar`` it is given there.
"""

import threading
import traceback
from typing import Callable

from trpc_sdk._inner import trpc_pb2
from trpc_sdk.abc import StreamDesc
from trpc_sdk.abc._cython cimport BaseService
from trpc_sdk.abc._cython cimport TrpcContext
from trpc_sdk.abc._cython cimport TrpcMessage
from trpc_sdk.abc._cython cimport get_message
from trpc_sdk.compressor import compress
from trpc_sdk.compressor import get_compress
from trpc_sdk.config import TrpcConfig
from trpc_sdk.exceptions import ServerFuncNotFoundError
from trpc_sdk.exceptions import ServerTimeoutError
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.exceptions import UnknownError
from trpc_sdk.filters._cython._filter cimport FilterChain
from trpc_sdk.log import logger
from trpc_sdk.serialization import deserialize
from trpc_sdk.serialization import serialize
from trpc_sdk.telemetry import report_attr
from trpc_sdk.transport import DefaultServerTransport
from trpc_sdk.transport import DefaultServerTransportOption
from trpc_sdk.transport import resolve_server_transport
from trpc_sdk.utils import now_ms
from trpc_sdk.utils cimport current_time_ms
from .._option import ServiceOption
from .._types import QueueWaitPolicy
from .._types import QueueWindow

# Cached for the unary hot path: IntEnum compare would box on every request.
cdef int _POLICY_SLIDING_WINDOW = int(QueueWaitPolicy.SLIDING_WINDOW)


cdef class ServiceAdapter(BaseService):
    """Per-service handler path after the transport has a complete request frame.

    One instance per YAML ``server.service[]`` entry. ``opt`` is the
    :class:`~trpc_sdk.server._option.ServiceOption` filled by ``create_server``.
    Unary methods live in ``unary_handlers``; stream methods in
    ``stream_handlers``. Both are populated by :meth:`register`.
    """

    def __cinit__(self, object service_option):
        # Per-endpoint options from YAML ``server.service[]``; filled by
        # ``create_server``. Listen address, codec, filters, and registry live here.
        self.opt = service_option
        # Unary RPC name -> ``Method``. Empty until :meth:`register`.
        self.unary_handlers = {}
        # Stream RPC name -> ``StreamDesc``. Empty until :meth:`register`.
        self.stream_handlers = {}
        # ``ServiceRegistrar`` set by ``start_service_registry`` only when
        # ``opt.registry`` is configured; stopped by :meth:`close`.
        self.registrar = None
        # YAML ``server.queue_cb``; overrides the default interval / ratio /
        # threshold of every ``QueueWindow`` this service builds.
        self._queue_cb_cfg = TrpcConfig.server_config().queue_cb
        # One ``QueueWindow`` per serving thread: several serving loops update
        # the bucket counters concurrently, and a shared window would lose
        # increments. Built lazily, on the first request a thread handles.
        self._queue_windows = threading.local()

    @property
    def queue_window(self):
        """Sliding-window samples of the calling thread, for ``SLIDING_WINDOW``."""
        return self._queue_window()

    cdef object _queue_window(self):
        """Return this thread's ``QueueWindow``, building it on first use."""
        window = getattr(self._queue_windows, "window", None)
        if window is None:
            cfg = self._queue_cb_cfg
            if cfg is not None:
                window = QueueWindow(interval=cfg.interval, ratio=cfg.ratio, threshold=cfg.threshold)
            else:
                window = QueueWindow()
            self._queue_windows.window = window
        return window

    cdef float _merge_timeout_budget(self, TrpcContext ctx):
        """Merge this service's timeout with the upstream remaining budget.

        ``ctx.timeout`` is already YAML ``service.timeout`` in milliseconds
        (the worker writes it when building the context). ``msg.timeout`` is
        the remaining budget from the request frame, also in milliseconds.

        When the frame carries a timeout and ``opt.disable_request_timeout``
        is False, the tighter of the two budgets wins. ``msg.timeout == 0``
        is treated as "caller sent no timeout" and is not inherited.

        ``ctx.deadline`` is the absolute millisecond timestamp later used
        when this service calls downstream. A non-positive budget clears
        timeout and deadline and returns ``-1`` so the filter runs
        ``wait_for(..., None)``.

        Args:
            ctx: Server context; ``timeout`` is read and rewritten here, and
                ``ctx.msg.timeout`` is read.

        Returns:
            Handler budget in seconds, or ``-1`` for no limit.
        """
        cdef TrpcMessage msg = ctx.msg
        if msg.timeout and not self.opt.disable_request_timeout:
            if ctx.timeout <= 0:
                ctx.timeout = msg.timeout
            else:
                ctx.timeout = min(msg.timeout, ctx.timeout)

        ctx.deadline = int(now_ms() + ctx.timeout)

        cdef float handler_timeout = ctx.get_timeout_sec()

        # Unlimited: filter uses -1; client treats deadline 0 as no deadline.
        if ctx.timeout <= 0:
            ctx.timeout = 0
            ctx.deadline = 0
            handler_timeout = -1

        return handler_timeout

    cdef bint _check_queue_wait(self, int limit_ms, int waited_ms, double now_ms_ts):
        """Return whether this request may still be handled after queue wait.

        Under ``QueueWaitPolicy.DEFAULT``, any wait at or above ``limit_ms``
        is a timeout. Under ``QueueWaitPolicy.SLIDING_WINDOW``, a timeout is
        recorded on :class:`~trpc_sdk.server._types.QueueWindow` and the request
        is dropped only when that window is also overloaded.

        ``limit_ms <= 0`` means no queue-wait limit: the request passes and
        no sample is recorded on the window.

        Args:
            limit_ms: Queue-wait limit in milliseconds; ``<= 0`` for none.
            waited_ms: Time already spent in the queue, in milliseconds.
            now_ms_ts: Current timestamp in milliseconds (CLOCK_REALTIME).

        Returns:
            ``True`` if the handler may run; ``False`` if the request
            should be answered with a queue-wait timeout.
        """
        if limit_ms <= 0:
            return True
        if self.opt.queue_wait_policy == _POLICY_SLIDING_WINDOW:
            if limit_ms <= waited_ms:
                if not self._queue_window().report_queue_time(now_ms_ts, False):
                    return False
            else:
                self._queue_window().report_queue_time(now_ms_ts, True)
        else:
            if limit_ms <= waited_ms:
                return False
        return True

    cdef void _fill_msg_from_option(self, TrpcContext ctx):
        """Copy this service's naming and address fields onto ``ctx.msg``.

        Callee identity is this service (``opt.service_name``), not the
        caller. Remote/local addresses are written again so a decode that
        mutated them cannot stick.

        Args:
            ctx: Server context holding ``remote_ip`` / ``local_ip``; its
                message's naming fields are overwritten.
        """
        cdef TrpcMessage msg = ctx.msg
        msg.set_namespace(self.opt.namespace)
        msg.set_env_name(self.opt.env_name)
        msg.set_setname(self.opt.set_name)
        msg.set_callee_service_name(self.opt.service_name)
        # Rewrite after decode so a codec cannot leave stale addresses.
        msg.set_remote_addr(ctx.remote_ip)
        msg.set_local_ip(ctx.local_ip)

    cdef (float, long, long) _prepare_request_budget(self, TrpcContext ctx):
        """Fill ``ctx.msg`` from ``opt`` and compute the unary timeout budgets.

        If ``opt.queue_wait_timeout`` is 0, the queue-wait limit falls back
        to the merged handler budget in milliseconds. An unlimited handler
        budget leaves that at 0, which :meth:`_check_queue_wait` treats as
        no queue-wait limit.

        Args:
            ctx: Server context; ``timeout`` / ``deadline`` are rewritten and
                its message's naming fields are written.

        Returns:
            ``(handler_timeout, waited_ms, queue_wait_timeout)``.
            ``handler_timeout`` is seconds, or ``-1`` for no limit;
            ``queue_wait_timeout`` is milliseconds, or ``0`` for no limit.
        """
        self._fill_msg_from_option(ctx)

        cdef double handler_timeout = self._merge_timeout_budget(ctx)

        cdef double now_ms_ts = current_time_ms()

        # Must follow the merge: reads the merged ``ctx.timeout``.
        cdef long queue_wait_timeout = self.opt.queue_wait_timeout
        if queue_wait_timeout == 0:
            queue_wait_timeout = ctx.get_timeout_ms()
        cdef long waited_ms = <int>(now_ms_ts - ctx.recv_time)  # in ms
        report_attr("trpc_queue_wait_time_cost", waited_ms)
        return handler_timeout, waited_ms, queue_wait_timeout

    cdef bytes _encode_queue_timeout_rsp(self, TrpcContext ctx):
        """Encode a ``ServerTimeoutError`` response for a queue-wait timeout.

        Args:
            ctx: Server context; the request message is read from it and
                its ``server_rsp_err`` is set here.

        Returns:
            Encoded response frame.
        """
        cdef TrpcMessage msg = get_message(ctx)
        report_attr("trpc_queue_wait_timeout", 1)
        logger.error("%s package timeout", str(msg.get_server_rpc_name()))
        msg.set_server_rsp_err(ServerTimeoutError("package timeout"))
        cdef bytes rsp_buf = self.opt.codec.encode_data(ctx, bytes("", encoding='utf8'))
        return rsp_buf

    cdef bytes _encode_func_not_found_rsp(self, TrpcContext ctx):
        """Encode a ``ServerFuncNotFoundError`` response for an unknown RPC name.

        Args:
            ctx: Server context; the request message is read from it.

        Returns:
            Encoded response frame.
        """
        cdef TrpcMessage msg = get_message(ctx)
        logger.error("[_encode_func_not_found_rsp] func not exist %s, remote_ip %s, remote_addr: %s",
                    str(msg.get_server_rpc_name()), str(ctx.remote_ip), str(msg.remote_addr))
        msg.set_server_rsp_err(ServerFuncNotFoundError("func not exist"))
        cdef bytes rsp_buf = self.opt.codec.encode_data(ctx, bytes("", encoding='utf8'))
        return rsp_buf

    async def _handle_stream(self, TrpcContext ctx, bytes req_buf, object stream_handle) -> bytes:
        """Dispatch a stream frame to ``opt.stream_handle``.

        Called from :meth:`handle` after decode, before timeout / queue-wait
        checks: stream frames do not go through the unary circuit breaker.

        Args:
            ctx: Server context.
            req_buf: Decoded request body (not the full frame).
            stream_handle: ``StreamDesc.impl_func`` for this RPC name.

        Returns:
            Encoded stream-frame response.

        Raises:
            ValueError: If ``opt.stream_handle`` is None.
        """
        if self.opt.stream_handle:
            return await self.opt.stream_handle.stream_handle_func(ctx, stream_handle, req_buf)
        raise ValueError("stream_handle is None")

    async def handle(self, TrpcContext ctx, bytes req_buf) -> bytes:
        """Unary (and stream-frame) path: decode, run filters, encode.

        Stream frames return from :meth:`_handle_stream` **before** timeout
        merge and queue-wait checks. Unary errors are encoded into a
        response frame rather than raised.

        Args:
            ctx: Server context; ``trpc_sdk.abc.TrpcContext``.
            req_buf: Complete request frame from transport.

        Returns:
            Encoded response frame.
        """
        cdef FilterChain filters
        cdef double now_ms_ts
        cdef TrpcMessage msg = get_message(ctx)

        try:
            req_body_buf = await self.opt.codec.decode_data(ctx, req_buf)

            if msg.frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
                if msg.get_server_rpc_name() not in self.stream_handlers:
                    logger.error("stream func not exist %s, remote_ip %s, remote_addr: %s", str(msg.get_server_rpc_name()),
                    str(ctx.remote_ip), str(msg.remote_addr))
                    msg.set_server_rsp_err(ServerFuncNotFoundError("stream func not exist"))
                    rsp_buf = self.opt.codec.encode_data(ctx, bytes("", encoding='utf8'))
                    return rsp_buf

                stream_handler: StreamDesc = self.stream_handlers[msg.get_server_rpc_name()]
                return await self._handle_stream(ctx, req_body_buf, stream_handler.impl_func)

            handler_timeout, waited_ms, queue_wait_timeout = self._prepare_request_budget(ctx)

            now_ms_ts = current_time_ms()

            if TrpcConfig.server_config().check_timeout is True:
                if not self._check_queue_wait(queue_wait_timeout, waited_ms, now_ms_ts):
                    return self._encode_queue_timeout_rsp(ctx)

            handler = self.unary_handlers.get(msg.get_server_rpc_name(), None)

            if handler is None:
                logger.error(f"[handle] func not exist server_rpc_name is {msg.get_server_rpc_name()}, "
                             f"registered handlers are {list(self.unary_handlers.keys())}")
                return self._encode_func_not_found_rsp(ctx)

            req = handler.req_cls()

            # Decompress + deserialize in place, then run the filter chain.
            filters = self._prepare_filter_chain(ctx, req_body_buf, req)

            # error must be subtype of TrpcException
            rsp_obj, error = await filters.handle(ctx, req, handler.impl_func, handler_timeout)

            if error is not None:
                logger.error(f"filters handle failed server_rpc_name is {msg.get_server_rpc_name()} error is {error}")
                return self._encode_error_rsp(ctx, error)

            rsp_body_buf = serialize(msg.serialization_type, rsp_obj)
            rsp_body_buf = compress(msg.compress_type, rsp_body_buf)

            rsp_buf = self.opt.codec.encode_data(ctx, rsp_body_buf)

        except TrpcException as err:
            logger.error(f"catch TrpcBaseError exception {err}, server_rpc_name is {msg.get_server_rpc_name()}")
            rsp_buf = self._encode_error_rsp(ctx, err)
        except Exception as err:  # pylint: disable=broad-except
            # Catch-all so a handler crash cannot take down the worker.
            logger.error(f"unknown error {err} server_rpc_name is {msg.get_server_rpc_name()}", exc_info=True)
            msg.set_server_rsp_err(UnknownError(str(err)))
            rsp_buf = self.opt.codec.encode_data(ctx, bytes("", encoding='utf8'))

        return rsp_buf

    cdef bytes _encode_error_rsp(self, TrpcContext ctx, object error):
        """Encode a tRPC error into the response frame.

        Non-``TrpcException`` values are wrapped as ``UnknownError``. An
        existing ``server_rsp_err`` that is not ``TRPC_INVOKE_SUCCESS``
        is left untouched.

        Args:
            ctx: Server context; its message's ``server_rsp_err`` may be
                set here, and the HTTP codec reads the response off it.
            error: Error returned by the filter chain or caught in
                :meth:`handle` / :meth:`handle_http`.

        Returns:
            Encoded response frame with an empty body.
        """
        cdef TrpcMessage msg = get_message(ctx)
        empty_buf = b""
        if not isinstance(error, TrpcException):
            error = UnknownError(repr(error))
        if msg.server_rsp_err is None or msg.server_rsp_err.code == TrpcErrorCode.TRPC_INVOKE_SUCCESS:
            msg.set_server_rsp_err(error)
        rsp_buf = self.opt.codec.encode_data(ctx, empty_buf)
        return rsp_buf

    async def handle_http(self, TrpcContext ctx, route_func: Callable):
        """HTTP route path; returns the filter-chain result, not a tRPC frame.

        Uses ``opt.filters`` directly. The request object is already on
        ``ctx.msg.server_raw_req``, so :meth:`_prepare_filter_chain`
        (decompress + deserialize) is skipped.

        Args:
            ctx: Server context; ``trpc_sdk.abc.TrpcContext``.
            route_func: User HTTP handler.

        Returns:
            Filter-chain result (aiohttp response, bytes, or similar).
        """
        msg = get_message(ctx)
        self._fill_msg_from_option(ctx)

        try:
            handler_timeout = self._merge_timeout_budget(ctx)

            req = msg.server_raw_req

            rsp_buf, error = await self.opt.filters.handle(ctx, req, route_func, handler_timeout)
            if error is not None:
                return self._encode_error_rsp(ctx, error)

        except TrpcException as err:
            rsp_buf = self._encode_error_rsp(ctx, err)
        except Exception as err:  # pylint: disable=broad-except
            logger.error("catch unknown exception %s", str(err))
            traceback.print_tb(err.__traceback__)
            msg.set_server_rsp_err(UnknownError(str(err)))
            rsp_buf = self.opt.codec.encode_data(ctx, bytes("", encoding='utf8'))

        return rsp_buf

    cdef FilterChain _prepare_filter_chain(self, TrpcContext ctx, bytes req_body_buf, object req_object):
        """Decompress and deserialize ``req_body_buf`` into ``req_object``.

        Returns ``opt.filters``; it does not build a new chain. The
        decompress + deserialize happen in place so the filter sees a
        populated request object.

        Args:
            ctx: Server context; ``ctx.msg.compress_type`` /
                ``serialization_type`` select the codecs.
            req_body_buf: Body bytes from ``codec.decode``.
            req_object: Empty request instance (``handler.req_cls()``).

        Returns:
            ``self.opt.filters``.
        """
        cdef TrpcMessage msg = ctx.msg
        req_body_buf = get_compress(msg.compress_type).decompress(req_body_buf)

        deserialize(msg.serialization_type, req_body_buf, req_object)

        return self.opt.filters

    cpdef void register(self, object service_desc, bint fix_rpc_name):
        """Install methods and streams from ``service_desc``.

        A stream handler also disables transport idle timeout (idle close
        would tear down long-lived streams) and calls ``stream_handle.init``.

        Args:
            service_desc: Generated ``ServiceDesc`` of methods / streams.
            fix_rpc_name: If True, rewrite the service-name segment of each
                method name to ``opt.service_name``.
        """
        if service_desc.stream_handler:
            self.opt.stream_handle = service_desc.stream_handler
            # Streams cannot use idle timeout: an idle close would drop the
            # connection. Recreate the transport option without the default.
            self.opt.transport_option.set_idle_timeout(0)
            self.opt.stream_handle.init(self.opt)

        for method in service_desc.methods:
            if fix_rpc_name:
                # Rewrite the ServiceName segment of "/<service>/<method>".
                parts = method.name.split('/', 2)
                parts[1] = self.opt.service_name
                method.name = '/'.join(parts)
            self.unary_handlers[method.name] = method

        if service_desc.streams is not None:
            for stream_handler in service_desc.streams:
                self.stream_handlers[stream_handler.name] = stream_handler

    cpdef void run(self):
        """Resolve and bind this service's transport, then register it on the engine.

        Runs before fork, from ``Server.serve``. The transport is resolved
        here rather than in ``create_server`` because a protocol starter may
        be registered after ``create_server`` and before ``serve``. ``bind``
        only records the options; the engine then checks the endpoint and
        appends it to ``process_info`` so the worker can listen on it after
        fork.
        """
        self.opt.transport = resolve_server_transport(self.opt.protocol, self.opt.transport)
        self.opt.transport.bind(self.opt.transport_option)
        self.opt.app.server_engine.add_listen_endpoint(self.opt.transport)

    cpdef object close(self):
        """Stop naming-service registration if this service has a registrar.

        Always returns ``None``. The registry thread still deregisters and
        then releases ``registry_semaphore`` so the engine can wait.

        Returns:
            ``None``.
        """
        if self.registrar is not None:
            self.registrar.stop()

        return None


def create_service(service_name: str, service_option: ServiceOption) -> ServiceAdapter:
    """Build one :class:`ServiceAdapter` for a listen endpoint.

    Fills in the default transport and transport option when the caller
    omitted them, installs :meth:`ServiceAdapter.handle` unless a handler is
    already set, and binds the service onto ``transport_option`` so the
    listen path can reach the handler.

    Args:
        service_name: YAML ``server.service[].name``.
        service_option: Per-endpoint options; a new :class:`ServiceOption`
            is constructed when this is ``None``.

    Returns:
        A ``ServiceAdapter`` ready to be passed to :meth:`Server.add_service`.
    """
    if service_option is None:
        service_option = ServiceOption()

    if service_option.transport is None:
        service_option.transport = DefaultServerTransport

    if service_option.transport_option is None:
        service_option.transport_option = DefaultServerTransportOption()

    service = ServiceAdapter(service_option=service_option)

    if not service.opt.handler_set:
        service.opt.set_handler(service.handle)

    service.opt.set_service_name(service_name)

    service_option.transport_option.set_service(service)

    return service
