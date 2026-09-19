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

Naming-service register / heartbeat / deregister run on a
:class:`~trpc_sdk.server._registry.RegistryThread`, not the master's event loop.
"""

import asyncio
import traceback
from typing import Callable

from posix.time cimport CLOCK_REALTIME
from posix.time cimport clock_gettime
from posix.time cimport timespec

from trpc_sdk._inner import remove_lock_file
from trpc_sdk._inner import register_spinlock
from trpc_sdk._inner import trpc_pb2
from trpc_sdk._inner import warm_up
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
from trpc_sdk.utils import now_ms
from .._option import ServiceOption
from .._registry import RegistryThread
from .._types import QueueWaitPolicy
from .._types import QueueWindow

# Cached for the unary hot path: IntEnum compare would box on every request.
cdef int _POLICY_SLIDING_WINDOW = int(QueueWaitPolicy.SLIDING_WINDOW)


cdef class Service(BaseService):
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
        # Started by :meth:`start_registry` when ``opt.registry`` is set.
        # Runs register/heartbeat on its own loop, not the master's.
        self.registry_thread = None
        # Sliding-window samples for ``QueueWaitPolicy.SLIDING_WINDOW``.
        # Built even for DEFAULT so ``_check_queue_wait`` need not branch
        # on whether the window exists. YAML ``server.queue_cb`` overrides
        # the default interval / ratio / threshold when present.
        queue_cb_cfg = TrpcConfig.server_config().queue_cb
        if queue_cb_cfg is not None:
            self.queue_window = QueueWindow(
                interval=queue_cb_cfg.interval,
                ratio=queue_cb_cfg.ratio,
                threshold=queue_cb_cfg.threshold,
            )
        else:
            self.queue_window = QueueWindow()

    cdef float _merge_timeout_budget(self, TrpcContext ctx, TrpcMessage msg):
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
            ctx: Server context; ``timeout`` is read and rewritten here.
            msg: Decoded request; only ``timeout`` is read.

        Returns:
            Handler budget in seconds, or ``-1`` for no limit.
        """
        if msg.timeout and not self.opt.disable_request_timeout:
            if ctx.timeout <= 0:
                ctx.timeout = msg.timeout
            else:
                ctx.timeout = min(msg.timeout, ctx.timeout)

        ctx.deadline = int(now_ms() + ctx.timeout)

        cdef float handler_timeout = float(ctx.timeout) / 1000

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

        Args:
            limit_ms: Queue-wait limit in milliseconds.
            waited_ms: Time already spent in the queue, in milliseconds.
            now_ms_ts: Current timestamp in milliseconds (CLOCK_REALTIME).

        Returns:
            ``True`` if the handler may run; ``False`` if the request
            should be answered with a queue-wait timeout.
        """
        if self.opt.queue_wait_policy == _POLICY_SLIDING_WINDOW:
            if limit_ms <= waited_ms:
                if not self.queue_window.report_queue_time(now_ms_ts, False):
                    return False
            else:
                self.queue_window.report_queue_time(now_ms_ts, True)
        else:
            if limit_ms <= waited_ms:
                return False
        return True

    cdef double _current_time_ms(self):
        """Return CLOCK_REALTIME in milliseconds.

        Used on the unary hot path instead of :func:`trpc_sdk.utils.now_ms` to
        avoid a Python call. Same clock as ``ctx.recv_time``.

        Returns:
            Current timestamp in milliseconds.
        """
        cdef timespec ts
        cdef double current
        clock_gettime(CLOCK_REALTIME, &ts)
        current = ts.tv_sec * 1000 + (ts.tv_nsec / 1000000)
        return current

    cdef void _fill_msg_from_option(self, TrpcMessage msg, TrpcContext ctx):
        """Copy this service's naming and address fields onto ``msg``.

        Callee identity is this service (``opt.service_name``), not the
        caller. Remote/local addresses are written again so a decode that
        mutated them cannot stick.

        Args:
            msg: Request message whose naming fields are overwritten.
            ctx: Server context holding ``remote_ip`` / ``local_ip``.
        """
        msg.set_namespace(self.opt.namespace)
        msg.set_env_name(self.opt.env_name)
        msg.set_setname(self.opt.set_name)
        msg.set_callee_service_name(self.opt.service_name)
        # Rewrite after decode so a codec cannot leave stale addresses.
        msg.set_remote_addr(ctx.remote_ip)
        msg.set_local_addr(ctx.local_ip)

    cdef (float, long, long) _prepare_request_budget(self, TrpcMessage msg, TrpcContext ctx):
        """Fill ``msg`` from ``opt`` and compute the unary timeout budgets.

        If ``opt.queue_wait_timeout`` is 0, the queue-wait limit falls back
        to the handler budget in milliseconds (``handler_timeout * 1000``).

        Args:
            msg: Decoded request; naming fields are written here.
            ctx: Server context; ``timeout`` / ``deadline`` are rewritten.

        Returns:
            ``(handler_timeout, waited_ms, queue_wait_timeout)``.
            ``handler_timeout`` is seconds, or ``-1`` for no limit.
        """
        self._fill_msg_from_option(msg, ctx)

        cdef double handler_timeout = self._merge_timeout_budget(ctx, msg)

        cdef double now_ms_ts = self._current_time_ms()

        cdef long queue_wait_timeout = self.opt.queue_wait_timeout
        queue_wait_timeout = int(handler_timeout * 1000) if queue_wait_timeout == 0 else queue_wait_timeout
        cdef long waited_ms = <int>(now_ms_ts - ctx.recv_time)  # in ms
        report_attr("trpc_queue_wait_time_cost", waited_ms)
        return handler_timeout, waited_ms, queue_wait_timeout

    cdef bytes _encode_queue_timeout_rsp(self, TrpcMessage msg):
        """Encode a ``ServerTimeoutError`` response for a queue-wait timeout.

        Args:
            msg: Request message; ``server_rsp_err`` is set here.

        Returns:
            Encoded response frame.
        """
        report_attr("trpc_queue_wait_timeout", 1)
        logger.error("%s package timeout", str(msg.get_server_rpc_name()))
        msg.set_server_rsp_err(ServerTimeoutError("package timeout"))
        cdef bytes rsp_buf = self.opt.codec.encode(msg, bytes("", encoding='utf8'))
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
        cdef bytes rsp_buf = self.opt.codec.encode(msg, bytes("", encoding='utf8'))
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
            Exception: If ``opt.stream_handle`` is None.
        """
        if self.opt.stream_handle:
            return await self.opt.stream_handle.stream_handle_func(ctx, stream_handle, req_buf)
        raise Exception("stream_handle is None")

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
            req_body_buf = await self.opt.codec.decode(msg, req_buf)

            if msg.frame_head.frame_type != trpc_pb2.TRPC_UNARY_FRAME:
                if msg.get_server_rpc_name() not in self.stream_handlers:
                    logger.error("stream func not exist %s, remote_ip %s, remote_addr: %s", str(msg.get_server_rpc_name()),
                    str(ctx.remote_ip), str(msg.remote_addr))
                    msg.set_server_rsp_err(ServerFuncNotFoundError("stream func not exist"))
                    rsp_buf = self.opt.codec.encode(msg, bytes("", encoding='utf8'))
                    return rsp_buf

                stream_handler: StreamDesc = self.stream_handlers[msg.get_server_rpc_name()]
                return await self._handle_stream(ctx, req_body_buf, stream_handler.impl_func)

            handler_timeout, waited_ms, queue_wait_timeout = self._prepare_request_budget(msg, ctx)

            now_ms_ts = self._current_time_ms()

            if TrpcConfig.server_config().check_timeout is True:
                if not self._check_queue_wait(queue_wait_timeout, waited_ms, now_ms_ts):
                    return self._encode_queue_timeout_rsp(msg)

            handler = self.unary_handlers.get(msg.get_server_rpc_name(), None)

            if handler is None:
                logger.error(f"[handle] func not exist server_rpc_name is {msg.get_server_rpc_name()}, "
                             f"registered handlers are {list(self.unary_handlers.keys())}")
                return self._encode_func_not_found_rsp(ctx)

            req = handler.req_cls()

            # Decompress + deserialize in place, then run the filter chain.
            filters = self._prepare_filter_chain(msg, req_body_buf, req)

            # error must be subtype of TrpcException
            rsp_obj, error = await filters.handle(ctx, req, handler.impl_func, handler_timeout)

            if error is not None:
                logger.error(f"filters handle failed server_rpc_name is {msg.get_server_rpc_name()} error is {error}")
                return self._encode_error_rsp(msg, error)

            rsp_body_buf = serialize(msg.serialization_type, rsp_obj)
            rsp_body_buf = compress(msg.compress_type, rsp_body_buf)

            rsp_buf = self.opt.codec.encode(msg, rsp_body_buf)

        except TrpcException as err:
            logger.error(f"catch TrpcBaseError exception {err}, server_rpc_name is {msg.get_server_rpc_name()}")
            rsp_buf = self._encode_error_rsp(msg, err)
        except Exception as err:  # pylint: disable=broad-except
            # Catch-all so a handler crash cannot take down the worker.
            logger.error(f"unknown error {err} server_rpc_name is {msg.get_server_rpc_name()}", exc_info=True)
            msg.set_server_rsp_err(UnknownError(str(err)))
            rsp_buf = self.opt.codec.encode(msg, bytes("", encoding='utf8'))

        return rsp_buf

    cdef bytes _encode_error_rsp(self, object msg, object error):
        """Encode a tRPC error into the response frame.

        Non-``TrpcException`` values are wrapped as ``UnknownError``. An
        existing ``server_rsp_err`` that is not ``TRPC_INVOKE_SUCCESS``
        is left untouched.

        Args:
            msg: Request message; ``server_rsp_err`` may be set here.
            error: Error returned by the filter chain or caught in
                :meth:`handle` / :meth:`handle_http`.

        Returns:
            Encoded response frame with an empty body.
        """
        empty_buf = b""
        if not isinstance(error, TrpcException):
            error = UnknownError(repr(error))
        if msg.server_rsp_err is None or msg.server_rsp_err.code == TrpcErrorCode.TRPC_INVOKE_SUCCESS:
            msg.set_server_rsp_err(error)
        rsp_buf = self.opt.codec.encode(msg, empty_buf)
        return rsp_buf

    async def handle_http(self, TrpcContext ctx, route_func: Callable):
        """HTTP route path; returns the filter-chain result, not a tRPC frame.

        Uses ``opt.filters`` directly. The request object is already on
        ``ctx.get_meta_data("request")``, so :meth:`_prepare_filter_chain`
        (decompress + deserialize) is skipped.

        Args:
            ctx: Server context; ``trpc_sdk.abc.TrpcContext``.
            route_func: User HTTP handler.

        Returns:
            Filter-chain result (aiohttp response, bytes, or similar).
        """
        msg = get_message(ctx)
        self._fill_msg_from_option(msg, ctx)

        try:
            handler_timeout = self._merge_timeout_budget(ctx, msg)

            req = ctx.get_meta_data("request")

            rsp_buf, error = await self.opt.filters.handle(ctx, req, route_func, handler_timeout)
            if error is not None:
                return self._encode_error_rsp(msg, error)

        except TrpcException as err:
            rsp_buf = self._encode_error_rsp(msg, err)
        except Exception as err:  # pylint: disable=broad-except
            logger.error("catch unknown exception %s", str(err))
            traceback.print_tb(err.__traceback__)
            msg.set_server_rsp_err(UnknownError(str(err)))
            rsp_buf = self.opt.codec.encode(msg, bytes("", encoding='utf8'))

        return rsp_buf

    cdef FilterChain _prepare_filter_chain(self, TrpcMessage msg, bytes req_body_buf, object req_object):
        """Decompress and deserialize ``req_body_buf`` into ``req_object``.

        Returns ``opt.filters``; it does not build a new chain. The
        decompress + deserialize happen in place so the filter sees a
        populated request object.

        Args:
            msg: Decoded request; ``compress_type`` / ``serialization_type``
                select the codecs.
            req_body_buf: Body bytes from ``codec.decode``.
            req_object: Empty request instance (``handler.req_cls()``).

        Returns:
            ``self.opt.filters``.
        """
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
        """Blocking ``listen_and_serve`` on this service's transport."""
        self.opt.transport.listen_and_serve(self.opt.transport_option, self.opt.app.server_engine)

    cpdef void start_registry(self):
        """Start a :class:`RegistryThread` if ``opt.registry`` is set.

        ``registry_option == 1`` warms shared-memory spinlocks before the
        thread starts. A service with no registry is a no-op and does not
        consume a ``registry_semaphore`` permit.
        """
        if self.opt.registry is not None:
            if self.opt.registry_option == 1:
                warm_up()
            self.registry_thread = RegistryThread(self._register_coro,
                                                  self._deregister_coro,
                                                  self.opt.app.server_engine.registry_semaphore)
            self.registry_thread.start()

    cpdef object close(self):
        """Cancel the registry thread if one is running.

        Always returns ``None``. The thread's ``run`` still deregisters and
        then releases ``registry_semaphore`` so the engine can wait.

        Returns:
            ``None``.
        """
        if self.opt.registry is not None and self.registry_thread is not None:
            self.registry_thread.terminate()

        return None

    async def _register_coro(self):
        """Register this service; runs on ``RegistryThread``'s loop.

        ``registry_option == 1`` takes the shared-memory spinlock first;
        otherwise the thread sleeps ``registry_delay`` milliseconds.
        """
        if self.opt.registry_option == 1:
            await register_spinlock()
        else:
            await asyncio.sleep(self.opt.registry_delay / 1000)

        await self.opt.registry.register(self.opt.service_name, self.opt.address)

    async def _deregister_coro(self):
        """Deregister this service; runs on ``RegistryThread``'s loop."""
        await self.opt.registry.deregister(self.opt.service_name, self.opt.address)
        remove_lock_file()


def start_service_registry(server):
    """Start naming-service registration for every service on ``server``.

    :func:`create_server` registers this as a fork hook. When YAML
    ``server.registry_after_fork`` is true it runs in the parent after
    fork; otherwise it runs in the parent before fork, so children inherit
    the already-started :class:`RegistryThread`. There is no master
    process.

    Args:
        server: The :class:`~trpc_sdk.server.Server` whose ``services``
            map is walked. Untyped to keep this Cython module from
            importing ``_server``.
    """
    for service in server.services.values():
        service.start_registry()


def create_service(service_name: str, service_option: ServiceOption) -> Service:
    """Build one :class:`Service` for a listen endpoint.

    Fills in the default transport and transport option when the caller
    omitted them, installs :meth:`Service.handle` unless a handler is
    already set, and binds the service onto ``transport_option`` so the
    listen path can reach the handler.

    Args:
        service_name: YAML ``server.service[].name``.
        service_option: Per-endpoint options; a new :class:`ServiceOption`
            is constructed when this is ``None``.

    Returns:
        A ``Service`` ready to be passed to :meth:`Server.add_service`.
    """
    if service_option is None:
        service_option = ServiceOption()

    if service_option.transport is None:
        service_option.transport = DefaultServerTransport

    if service_option.transport_option is None:
        service_option.transport_option = DefaultServerTransportOption()

    service = Service(service_option=service_option)

    if not service.opt.handler_set:
        service.opt.set_handler(service.handle)

    service.opt.set_service_name(service_name)

    service_option.transport_option.set_service(service)

    return service
