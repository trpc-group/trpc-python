# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Admin HTTP server.

Runs in the root process on its own thread and event loop. It serves the
``/version`` and ``/cmds/*`` HTTP endpoints and accepts WS connections from
worker processes on ``/_/ws``. Commands that need worker state, such as log
level changes and rpcz span collection, are pushed to the workers over those
WS connections.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from typing import Any
from typing import List
from typing import Optional

from aiohttp import WSMsgType
from aiohttp import web

from trpc_sdk.config import TrpcConfig
from trpc_sdk.log import DEFAULT_LOG_NAME
from trpc_sdk.log import LEVEL_NAME_TO_LOGGING_LEVEL
from trpc_sdk.log import logger
from trpc_sdk.log._logger import _LoggerManager
from trpc_sdk.rpcz import Span
from trpc_sdk.utils import now_ms
from trpc_sdk.utils import now_us
from trpc_sdk.version import __version__
from ._constants import RPCZ_FILTER_PLUGIN_NAME
from ._constants import WS_PATH
from ._constants import WS_USER_AGENT_PREFIX
from ._loglevel import getLoglevel
from ._loglevel import setLoglevel


class Configer:
    """Registry of WS commands and config getters, plus connected worker state.

    Attributes:
        handlers: Command name to ``async handler(ws_sock, data)`` for
            messages sent by workers.
        getters: Config key to ``async getter(value=None)``; their results
            are pushed to workers in a ``config`` command.
        clients: Connected worker WS sockets, guarded by ``clients_lock``.
        rpcz_pending: ``request_id`` to the future awaiting a worker's rpcz
            reply.
        rpcz_sorted_spans: Collected spans, newest first, guarded by
            ``rpcz_lock``.
        loop: The admin server's running loop, bound at app startup so the
            rpcz collector thread can schedule coroutines on it.
        rr_idx: Round-robin cursor used when no valid ``child_index`` is given.
    """

    def __init__(self):
        self.handlers = {}
        self.getters = {}
        self.clients: List = []
        self.clients_lock = threading.Lock()
        self.rpcz_pending: dict = {}
        self.rpcz_sorted_spans: List[Span] = []
        self.rpcz_lock = threading.Lock()
        self.loop = None
        self.rr_idx: int = 0

    def reg_cmd(self, cmd):
        """Register a handler for a WS command sent by workers.

        Args:
            cmd: Command name, the first word of a WS text message.

        Returns:
            A decorator that stores ``func`` and returns it unchanged.
        """

        def deco(func):
            self.handlers[cmd] = func
            return func

        return deco

    async def handle(self, ws_sock, data):
        """Dispatch a ``'<cmd> <data>'`` WS text message to its handler.

        Args:
            ws_sock: The worker WS connection the message arrived on.
            data: Raw message text.
        """
        cmd, _, data = data.partition(' ')
        data = data.strip()
        await self.handlers[cmd](ws_sock, data)

    def reg_cfg(self, name):
        """Register a getter whose value is pushed to workers as config.

        Args:
            name: Config key inside the ``config`` command payload.

        Returns:
            A decorator that stores ``func`` and returns it unchanged.
        """

        def deco(func):
            self.getters[name] = func
            return func

        return deco

    def add_client(self, ws_sock):
        """Track a newly connected worker WS socket."""
        with self.clients_lock:
            if ws_sock not in self.clients:
                self.clients.append(ws_sock)
            total = len(self.clients)
        logger.info('configer: worker connected, total=%d', total)

    def remove_client(self, ws_sock):
        """Stop tracking a worker WS socket; unknown sockets are ignored."""
        with self.clients_lock:
            try:
                self.clients.remove(ws_sock)
            except ValueError:
                pass
            total = len(self.clients)
        logger.info('configer: worker disconnected, total=%d', total)

    def _healthy_clients(self) -> List:
        with self.clients_lock:
            return [ws for ws in self.clients if not ws.closed]

    async def config(self, items, wss=None):
        """Evaluate getters and push the result as a ``config`` command.

        Args:
            items: A ``{key: arg}`` dict calls each getter with ``arg``; an
                iterable of keys calls them without arguments; ``None`` calls
                every registered getter.
            wss: Target sockets. Defaults to every healthy worker.
        """
        data = {}
        if items and isinstance(items, dict):
            for key, value in items.items():
                data[key] = await self.getters[key](value)
        else:
            for key in (items or self.getters):
                data[key] = await self.getters[key]()
        data = 'config ' + json.dumps(data)
        targets = wss or self._healthy_clients()
        await asyncio.gather(*[ws_sock.send_str(data) for ws_sock in targets])

    async def rpcz(self, payload: dict, timeout: float = 10.0, child_index=None) -> Optional[dict]:
        """Ask one worker for its rpcz spans and wait for the reply.

        Args:
            payload: Extra fields merged into the request.
            timeout: Seconds to wait for the worker's reply.
            child_index: Index into the healthy worker list. Out-of-range or
                ``None`` falls back to round-robin.

        Returns:
            The worker's reply payload, or ``None`` when no worker is
            available, the send fails, or the reply times out.
        """
        clients = self._healthy_clients()
        if not clients:
            logger.debug('rpcz: no healthy workers available, cannot dispatch payload=%s', payload)
            return None

        if child_index is not None and 0 <= child_index < len(clients):
            target = clients[child_index]
            logger.debug('rpcz: selecting worker by index=%d (total=%d)', child_index, len(clients))
        else:
            target = clients[self.rr_idx % len(clients)]
            self.rr_idx += 1
            logger.debug('rpcz: child_index=%s invalid or absent, selecting worker by '
                         'round-robin (total=%d)', child_index, len(clients))

        request_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self.rpcz_pending[request_id] = future

        msg = 'rpcz ' + json.dumps({**payload, 'request_id': request_id, 'child_index': child_index})
        try:
            await target.send_str(msg)
        except Exception as e:  # pylint: disable=broad-except
            self.rpcz_pending.pop(request_id, None)
            logger.debug('rpcz: failed to send msg to worker, request_id=%s, error=%s', request_id, e)
            return None

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self.rpcz_pending.pop(request_id, None)
            logger.debug('rpcz: timed out waiting for worker response, request_id=%s', request_id)
            return None

    def _resolve_rpcz_pending(self, request_id: Optional[str], payload: dict) -> None:
        future = self.rpcz_pending.pop(request_id, None)
        if future and not future.done():
            future.set_result(payload)

    @staticmethod
    def _span_first_log_us(span: Span) -> int:
        if span.span_type.value == 'server':
            return span.server_pre_sched_recv_msg_us
        return span.client_pre_rpc_invoke_us

    @staticmethod
    def _span_rename_id(span: Span, child_index: int) -> int:
        """Make a span id unique across workers: child index high, local id low 32 bits."""
        local_id = int(span.span_id) & 0xFFFFFFFF
        child = int(child_index) & 0xFFFFFFFF
        return (child << 32) | local_id

    def _rebuild_sorted_spans(self) -> None:
        self.rpcz_sorted_spans.sort(key=self._span_first_log_us, reverse=True)

    def evict_expired_spans(self, cache_expire_interval_s: int) -> None:
        """Drop cached spans older than ``cache_expire_interval_s`` seconds.

        The caller must hold ``rpcz_lock``.
        """
        expire_before = now_us() - int(cache_expire_interval_s) * 1000 * 1000
        before_count = len(self.rpcz_sorted_spans)
        self.rpcz_sorted_spans = [
            span for span in self.rpcz_sorted_spans if self._span_first_log_us(span) >= expire_before
        ]
        after_count = len(self.rpcz_sorted_spans)
        evicted_count = before_count - after_count
        logger.debug('evict_expired_spans: cache_expire_interval_s=%s, evicted=%d, '
                     'before=%d, after=%d', cache_expire_interval_s, evicted_count, before_count, after_count)

    def _decode_span(self, item) -> Optional[Span]:
        if isinstance(item, Span):
            logger.debug('_decode_span: span object passthrough, span_id=%s', item.span_id)
            return item
        if isinstance(item, dict):
            try:
                span = Span.from_dict(item)
                logger.debug('_decode_span: decoded from dict, span_id=%s', span.span_id)
                return span
            except Exception as e:  # pylint: disable=broad-except
                logger.error('_decode_span: failed to decode dict span, error=%s', e)
                return None
        if isinstance(item, str):
            try:
                span = Span.from_dict(json.loads(item))
                logger.debug('_decode_span: decoded from json string, span_id=%s', span.span_id)
                return span
            except Exception as e:  # pylint: disable=broad-except
                logger.debug('_decode_span: failed to decode json string span, error=%s', e)
                return None
        logger.debug('_decode_span: unsupported span payload type=%s', type(item).__name__)
        return None

    def on_subprocess_rpcz(self, request_id: Optional[str], payload: dict) -> None:
        """Merge a worker's rpcz reply into the span cache and resolve its request.

        Spans that fail to decode or rename are skipped. The pending future is
        resolved with the raw payload in every case, including errors.

        Args:
            request_id: The id echoed back by the worker.
            payload: The worker's reply with ``child_index`` and ``spans``.
        """
        child_index = payload.get('child_index', -1)
        raw_spans = payload.get('spans', [])
        if not raw_spans:
            logger.debug('on_subprocess_rpcz: empty spans payload, skip rebuild, '
                         'request_id=%s, child_index=%s', request_id, child_index)
            self._resolve_rpcz_pending(request_id, payload)
            return

        parsed_spans: List[Span] = []
        decode_failed = 0
        rename_failed = 0
        try:
            for raw_span in raw_spans:
                span = self._decode_span(raw_span)
                if span is None:
                    logger.debug(
                        'on_subprocess_rpcz: failed to decode span, '
                        'request_id=%s, child_index=%s, raw_span=%s', request_id, child_index, raw_span)
                    decode_failed += 1
                    continue
                try:
                    span.span_id = self._span_rename_id(span, child_index)
                except Exception as e:  # pylint: disable=broad-except
                    rename_failed += 1
                    logger.debug(
                        'on_subprocess_rpcz: failed to rename span id, '
                        'request_id=%s, child_index=%s, error=%s', request_id, child_index, e)
                    continue
                parsed_spans.append(span)

            if not parsed_spans:
                logger.debug(
                    'on_subprocess_rpcz: no valid spans after decode/rename, '
                    'skip rebuild, request_id=%s, child_index=%s, '
                    'decode_failed=%d, rename_failed=%d', request_id, child_index, decode_failed, rename_failed)
                self._resolve_rpcz_pending(request_id, payload)
                return

            with self.rpcz_lock:
                self.rpcz_sorted_spans.extend(parsed_spans)
                self._rebuild_sorted_spans()
                total_sorted = len(self.rpcz_sorted_spans)

            logger.debug(
                'on_subprocess_rpcz: done, request_id=%s, child_index=%s, accepted=%d, '
                'decode_failed=%d, rename_failed=%d, total_sorted=%d', request_id, child_index, len(parsed_spans),
                decode_failed, rename_failed, total_sorted)
        except Exception as e:  # pylint: disable=broad-except
            logger.debug('on_subprocess_rpcz: failed to process payload, request_id=%s, '
                         'child_index=%s, error=%s', request_id, child_index, e)

        self._resolve_rpcz_pending(request_id, payload)

    async def collect_rpcz_once(self, timeout: float = 5.0) -> None:
        """Request rpcz spans from every healthy worker concurrently.

        Args:
            timeout: Per-worker reply timeout in seconds.
        """
        logger.debug('collect_rpcz_once: entered, timeout=%s', timeout)
        clients = self._healthy_clients()
        if not clients:
            logger.debug('collect_rpcz_once: no healthy workers available, skip collect, timeout=%s', timeout)
            return
        tasks = [
            self.rpcz(payload={'type': 'collect'}, timeout=timeout, child_index=idx) for idx in range(len(clients))
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


def make_response(res):
    """Build a JSON response, defaulting ``errorcode`` to 0 and ``message`` to ''."""
    res.setdefault('errorcode', 0)
    res.setdefault('message', '')
    return web.Response(text=json.dumps(res, indent=2), content_type='application/json')


def set_error(res, code, msg):
    """Set ``errorcode`` and ``message`` on ``res`` and return it."""
    res['errorcode'] = code
    res['message'] = msg
    return res


configer = Configer()
routes = web.RouteTableDef()


@configer.reg_cmd('config')
async def handle_config(ws_sock, data):
    """Reply to a worker's ``config key1,key2`` request with those values."""
    await configer.config(data.split(','), [ws_sock])


@configer.reg_cmd('rpcz')
async def handle_subprocess_rpcz(ws_sock, data):
    """Receive a worker's rpcz reply and merge it into the local span cache."""
    try:
        payload = json.loads(data)
        request_id = payload.get('request_id')
        child_index = payload.get('child_index')
        span_count = len(payload.get('spans', []) or [])
        logger.debug('handle_subprocess_rpcz: parsed payload, request_id=%s, child_index=%s, '
                     'span_count=%d', request_id, child_index, span_count)
        if not request_id:
            logger.debug('handle_subprocess_rpcz: no request_id in payload, skip, payload=%s', payload)
            return

        configer.on_subprocess_rpcz(request_id, payload)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug('failed to parse subprocess rpcz response: %s', e)


@routes.get(WS_PATH)
async def handler_ws(request):
    """Accept a worker WS connection and dispatch its messages until it closes."""
    if not request.headers.get('User-Agent', '').startswith(WS_USER_AGENT_PREFIX):
        return make_response({})
    ws_sock = web.WebSocketResponse()
    await ws_sock.prepare(request)
    configer.add_client(ws_sock)
    try:
        async for msg in ws_sock:
            if msg.type == WSMsgType.TEXT:
                await configer.handle(ws_sock, msg.data)
    finally:
        configer.remove_client(ws_sock)
    return ws_sock


@routes.get('/version')
async def handler_version(request):
    """get trpc version"""
    return make_response({'version': __version__})


@routes.get("/cmds")
async def handler_cmds(request):
    """list supported cmds"""
    cmds = {}
    for route in routes:
        if route.path.startswith('/_/'):
            continue
        cmds.setdefault(route.path, {})[route.method] = route.handler.__doc__
    return make_response({'cmds': list(cmds), 'cmds_': cmds})


_LEVEL_NAMES = {level: name for name, level in LEVEL_NAME_TO_LOGGING_LEVEL.items()}


@configer.reg_cfg('loglevel')
async def handler_loglevel(lognames=None):
    """Return ``{logger_name: level}`` for the given or all registered loggers."""
    loggers = _LoggerManager._loggers  # pylint: disable=protected-access
    return {name: getLoglevel(loggers[name]) for name in (lognames or loggers)}


def get_logger(logname):
    """Resolve a logger name, falling back to the default logger.

    Args:
        logname: Requested logger name; empty or unknown names resolve to the
            default logger.

    Returns:
        ``(resolved_name, logger)``.
    """
    loggers = _LoggerManager._loggers  # pylint: disable=protected-access
    if logname and logname in loggers:
        return logname, loggers[logname]
    default_logger = loggers.get(DEFAULT_LOG_NAME, _LoggerManager._default_logger)  # pylint: disable=protected-access
    return DEFAULT_LOG_NAME, default_logger


@routes.get("/cmds/loglevel")
async def handler_loglevel_get(request):
    """get trpc log level. (params eg. logger=default)"""
    logname, log_instance = get_logger(request.query.get('logger'))
    return make_response({'logger': logname, 'level': _LEVEL_NAMES.get(getLoglevel(log_instance))})


@routes.put("/cmds/loglevel")
async def handler_loglevel_put(request):
    """set trpc log level. (content eg. logger=default&value=warning)"""
    res = {}
    data = await request.post()
    logname, log_instance = get_logger(data.get('logger'))
    level = LEVEL_NAME_TO_LOGGING_LEVEL.get(data.get('value'))
    if level is None:
        set_error(res, -1, 'wrong content, please use value={LEVEL}')
    else:
        res['logger'] = logname
        res['prelevel'] = _LEVEL_NAMES.get(getLoglevel(log_instance))
        setLoglevel(log_instance, level)
        if logname == DEFAULT_LOG_NAME:
            logging.getLogger().setLevel(log_instance.min_level)
        await configer.config({'loglevel': [logname]})
        res['level'] = _LEVEL_NAMES.get(getLoglevel(log_instance))
    return make_response(res)


@routes.get("/cmds/config")
async def handler_config(request):
    """view configuration content"""
    parser = TrpcConfig.config_parser()
    content = parser._raw_config_dict if parser else {}  # pylint: disable=protected-access
    return make_response({'content': content})


@routes.get("/cmds/rpcz")
async def handler_rpcz(request):
    """query rpcz spans. (?span_id=123 for detail)"""
    span_id_str = request.query.get('span_id', '').strip()
    with configer.rpcz_lock:
        if span_id_str:
            if not span_id_str.isdigit():
                return make_response({'errorcode': -1, 'message': 'wrong parameter, span_id must be integer'})
            span_id = int(span_id_str)
            for span in configer.rpcz_sorted_spans:
                if span.span_id == span_id:
                    return make_response({'span': span.to_dict()})
            return make_response({'errorcode': -1, 'message': 'span_id not found'})

        print_spans_num = TrpcConfig.global_config().rpcz.print_spans_num
        brief = [span.brief_span_info() for span in configer.rpcz_sorted_spans[:print_spans_num]]
        return make_response({'spans': brief})


@routes.get("/cmds/debug/language")
async def handler_language(request):
    """get the programming language of this service"""
    return make_response({'language': 'python'})


def is_rpcz_filter_enabled_in_config(cfg: Optional[Any] = None) -> bool:
    """Return whether the ``rpcz`` filter is configured anywhere.

    Checks the server-level, every service-level, the client-level and every
    backend-level ``filters`` list. Used to skip the rpcz collector thread and
    its periodic logging when rpcz is not enabled.

    Args:
        cfg: The typed config to inspect. Defaults to
            :meth:`TrpcConfig.config`.

    Returns:
        ``True`` if any of those lists contains ``rpcz``; ``False`` otherwise
        or when the config cannot be inspected.
    """
    if cfg is None:
        cfg = TrpcConfig.config()
    name = RPCZ_FILTER_PLUGIN_NAME
    try:
        server = getattr(cfg, "server", None)
        if server is not None:
            if name in (getattr(server, "filters", None) or []):
                return True
            for svc in getattr(server, "service", None) or []:
                if name in (getattr(svc, "filters", None) or []):
                    return True

        client = getattr(cfg, "client", None)
        if client is not None:
            if name in (getattr(client, "filters", None) or []):
                return True
            for backend in getattr(client, "service", None) or []:
                if name in (getattr(backend, "filters", None) or []):
                    return True
    except Exception as e:  # pylint: disable=broad-except
        logger.debug(
            "is_rpcz_filter_enabled_in_config: failed to inspect config, treat as disabled, err=%s",
            e,
        )
        return False
    return False


def init():
    """Build the admin aiohttp application.

    When rpcz is enabled, also starts the rpcz collector thread once per
    process. That thread periodically collects spans from workers through the
    admin loop and evicts expired ones from the cache.

    Returns:
        The aiohttp application with every admin route registered.
    """
    enable_rpcz_collector = is_rpcz_filter_enabled_in_config()

    def rpcz_collector():
        last_collect_ms = 0
        last_remove_ms = 0
        logger.debug('rpcz_collector: thread started')
        while True:
            try:
                rpcz_cfg = TrpcConfig.global_config().rpcz
                now = now_ms()

                if now >= last_collect_ms + rpcz_cfg.collect_interval_ms:
                    loop_obj = configer.loop
                    logger.debug(
                        'rpcz_collector: loop status before collect, loop_is_none=%s, loop_id=%s, '
                        'is_running=%s, is_closed=%s, collector_thread=%s',
                        loop_obj is None,
                        id(loop_obj) if loop_obj is not None else None,
                        loop_obj.is_running() if loop_obj is not None else None,
                        loop_obj.is_closed() if loop_obj is not None else None,
                        threading.current_thread().name,
                    )
                    if loop_obj is None or (not loop_obj.is_running()) or loop_obj.is_closed():
                        logger.debug('rpcz_collector: loop not ready, skip collect')
                        last_collect_ms = now
                        continue
                    logger.debug('rpcz_collector: trigger collect, now_ms=%d, interval_ms=%d', now,
                                 rpcz_cfg.collect_interval_ms)
                    collect_timeout_s = max(1.0, rpcz_cfg.collect_interval_ms / 1000.0 + 1.0)
                    fut = asyncio.run_coroutine_threadsafe(
                        configer.collect_rpcz_once(timeout=collect_timeout_s),
                        configer.loop,
                    )
                    try:
                        fut.result(timeout=collect_timeout_s + 1.0)
                        with configer.rpcz_lock:
                            total_sorted = len(configer.rpcz_sorted_spans)
                        logger.debug('rpcz_collector: collect finished, total_sorted_spans=%d', total_sorted)
                    except Exception as e:  # pylint: disable=broad-except
                        logger.debug('rpcz_collector collect failed: %s', e)
                    last_collect_ms = now

                if now >= last_remove_ms + rpcz_cfg.remove_interval_ms:
                    logger.debug('rpcz_collector: trigger evict, now_ms=%d, interval_ms=%d', now,
                                 rpcz_cfg.remove_interval_ms)
                    with configer.rpcz_lock:
                        configer.evict_expired_spans(rpcz_cfg.cache_expire_interval)
                        remain = len(configer.rpcz_sorted_spans)
                    logger.debug('rpcz_collector: evict finished, remain_spans=%d', remain)
                    last_remove_ms = now
            except Exception as e:  # pylint: disable=broad-except
                logger.debug('rpcz_collector loop error: %s', e)
            time.sleep(0.05)

    if enable_rpcz_collector and not getattr(init, "_rpcz_collector_started", False):
        collector_thread = threading.Thread(target=rpcz_collector, name="rpcz_collector", daemon=True)
        collector_thread.start()
        init._rpcz_collector_started = True
    elif not enable_rpcz_collector:
        logger.debug(
            "admin init: rpcz collector thread not started (no %r in server/client filter config)",
            RPCZ_FILTER_PLUGIN_NAME,
        )

    app = web.Application()

    if enable_rpcz_collector:

        async def _bind_running_loop(_):
            loop = asyncio.get_running_loop()
            configer.loop = loop
            logger.debug("init: bound running loop for rpcz collector, loop_id=%s", id(loop))

        app.on_startup.append(_bind_running_loop)
    app.router.add_routes(routes)
    return app


def run(host, port, **kw):
    """Serve the admin application; blocks until the server stops.

    Args:
        host: Bind host.
        port: Bind port.
        **kw: Extra keyword arguments for ``aiohttp.web.run_app``.
    """
    web.run_app(init(), host=host, port=port, **kw)
