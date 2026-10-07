# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""HTTP-client option assembly for ``Client.invoke``.

:func:`get_options` turns a parsed URL plus extra
:class:`~trpc_sdk.client.Options` into the list the HTTP client passes to
tRPC ``invoke``. It is not :func:`trpc_sdk.client.get_options`, which
builds options from a :class:`~trpc_sdk.abc.TrpcMessage`.
"""

from __future__ import annotations

from typing import List
from typing import Optional
from urllib.parse import ParseResult

from trpc_sdk.client import Options
from trpc_sdk.naming import TrpcSelector
from trpc_sdk.serialization import SerializationType


def get_options(url: ParseResult, opts: Optional[List]) -> List:
    """Build tRPC client options from a parsed URL plus extra options.

    ``trpc://host`` is treated as a service name for the Polaris selector;
    any other scheme becomes ``set_target("{scheme}://{netloc}")``.
    Protocol is always ``http`` and serialization is ``Noop`` because
    the HTTP client sends the body as-is.

    Args:
        url: Already-parsed URL.
        opts: Extra :class:`~trpc_sdk.client.Options` to append.

    Returns:
        A list of options to pass to ``Client.invoke``.
    """
    base = (
        Options()
        .set_protocol("http")
        .set_serialization_type(SerializationType.Noop)
    )

    trpc_selector = TrpcSelector()
    if url.scheme == trpc_selector.get_selector_name():
        base.set_service_name(url.netloc)
    else:
        base.set_target(f"{url.scheme}://{url.netloc}")

    options = [base]
    if opts:
        options.extend(opts)

    return options
