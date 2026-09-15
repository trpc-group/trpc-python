# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Per-call options the client transport reads on a round trip.

:class:`RoundTripOptions` is the bag of knobs :class:`ClientTransport`
passes into the TCP / UDP / stream helpers. Request enums
(:class:`RequestType`, :class:`ClientTimeoutReason`) live in
:mod:`trpc_sdk.transport._constants` and are re-exported at the package
level; this module only holds the options object itself.
"""

from __future__ import annotations

from trpc_sdk.abc import BaseClientTransportOption
from trpc_sdk.codec import Message
from trpc_sdk.codec import TrpcClientParser
from trpc_sdk.pool import ConnPoolOptions
from trpc_sdk.pool import ConnectionPoolManager
from trpc_sdk.pool import Multiplexed
from ._constants import RequestType


class RoundTripOptions(BaseClientTransportOption):
    """Options for one client round trip.

    Constructed empty and filled by setters (or by assigning fields
    directly). The TCP / UDP helpers then read ``network``, ``address``,
    ``req_type``, and the pool / multiplexed fields to pick a path.

    Attributes:
        checker: Frame parser used to split the response stream.
        password: Optional password for the callee.
        pool: Caller-supplied connection pool. When set, the TCP helper
            uses this instead of the framework default.
        pool_opts: Options for the framework-built pool. Ignored when
            ``pool`` is already set.
        req_type: :attr:`RequestType.SendAndRecv` waits for a response;
            :attr:`RequestType.SendOnly` writes and returns.
        enable_multiplexed: When True, the TCP helper uses the multiplexed
            virtual-connection path instead of a pooled one-shot conn.
        multiplexed: Multiplexed pool instance used when
            ``enable_multiplexed`` is True.
        msg: Request :class:`~trpc_sdk.codec.Message`; the multiplexed path
            needs it to match responses by ``request_id``.
        ssl_context: TLS context for HTTPS.
        is_task_stream: When True, :class:`ClientConn` must not close the
            pooled connection after the stream ends; the task process
            still owns it.
        disable_reuse_http_session: When True, do not reuse the HTTP
            session across calls. Default is False (reuse is allowed).
    """

    def __init__(self):
        super().__init__()
        self.checker = TrpcClientParser()
        self.password = None
        self.pool = None
        self.pool_opts = None

        self.req_type = RequestType.SendAndRecv
        self.enable_multiplexed = False
        self.multiplexed = None
        self.msg = None

        self.ssl_context = None
        self.is_task_stream = False
        self.disable_reuse_http_session = False

    def set_req_type(self, req_type: int):
        """Set whether this call waits for a response.

        Args:
            req_type: :attr:`RequestType.SendAndRecv` or
                :attr:`RequestType.SendOnly`.
        """
        self.req_type = req_type

    def set_disable_reuse_http_session(self, disable_reuse_http_session: bool):
        """Disable (or re-enable) HTTP session reuse for this call.

        Args:
            disable_reuse_http_session: True to force a fresh session.
        """
        self.disable_reuse_http_session = disable_reuse_http_session

    def set_password(self, password: str):
        """Set the request password.

        Args:
            password: Password sent with the call.
        """
        self.password = password

    def set_pool(self, pool: ConnectionPoolManager):
        """Use a caller-supplied connection pool instead of the default.

        Args:
            pool: Pool manager the TCP helper will ``get`` from.
        """
        self.pool = pool

    def set_pool_options(self, pool_opts: ConnPoolOptions):
        """Set options for the framework-built connection pool.

        Ignored when :meth:`set_pool` has already supplied a pool.

        Args:
            pool_opts: Dial / size knobs for the built-in pool.
        """
        self.pool_opts = pool_opts

    def set_enable_multiplexed(self, enable: bool):
        """Switch the TCP helper onto the multiplexed virtual-connection path.

        Args:
            enable: True to multiplex many requests onto one TCP conn.
        """
        self.enable_multiplexed = enable

    def set_multiplexed(self, multiplexed: Multiplexed):
        """Set the multiplexed pool used when multiplexed mode is on.

        Args:
            multiplexed: Pool that vends :class:`VirtualConnection` objects.
        """
        self.multiplexed = multiplexed

    def set_msg(self, msg: Message):
        """Attach the request message so multiplexed responses can be matched.

        Args:
            msg: Request message whose ``request_id`` keys the virtual conn.
        """
        self.msg = msg
