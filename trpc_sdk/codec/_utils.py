# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Attachment accessors for the raw bytes carried after a tRPC body.

The attachment accessors are the public read/write path for the raw
bytes carried after the body in a tRPC frame. They read the
:class:`~trpc_sdk.abc.TrpcMessage` hanging off a ``TrpcContext`` rather
than taking a message directly, because that is the only object
handlers and client stubs have. ``trpc_sdk.abc`` does not import
``trpc_sdk.codec``, so ``TrpcContext`` can be imported at runtime.
"""

from __future__ import annotations

from typing import Union

from trpc_sdk.abc import TrpcContext
from trpc_sdk.abc import get_message


def set_request_attachment(ctx: TrpcContext, attachment: Union[bytes, str]) -> None:
    """Attach raw bytes to the outgoing request frame.

    Args:
        ctx: TrpcContext holding the current :class:`~trpc_sdk.abc.TrpcMessage`.
        attachment: Bytes, or a string encoded as UTF-8 first.
    """
    message = get_message(ctx)
    if isinstance(attachment, str):
        attachment = attachment.encode('utf-8')
    message.set_request_attachment(attachment)


def get_request_attachment(ctx: TrpcContext, is_str: bool = False) -> Union[bytes, str]:
    """Return the attachment carried by the request frame.

    Args:
        ctx: TrpcContext holding the current :class:`~trpc_sdk.abc.TrpcMessage`.
        is_str: Decode the bytes as UTF-8 before returning.
    """
    message = get_message(ctx)
    if is_str:
        return message.request_attachment.decode('utf-8')
    return message.request_attachment


def set_response_attachment(ctx: TrpcContext, attachment: Union[bytes, str]) -> None:
    """Attach raw bytes to the outgoing response frame.

    Args:
        ctx: TrpcContext holding the current :class:`~trpc_sdk.abc.TrpcMessage`.
        attachment: Bytes, or a string encoded as UTF-8 first.
    """
    message = get_message(ctx)
    if isinstance(attachment, str):
        attachment = attachment.encode('utf-8')
    message.set_response_attachment(attachment)


def get_response_attachment(ctx: TrpcContext, is_str: bool = False) -> Union[bytes, str]:
    """Return the attachment carried by the response frame.

    Args:
        ctx: TrpcContext holding the current :class:`~trpc_sdk.abc.TrpcMessage`.
        is_str: Decode the bytes as UTF-8 before returning.
    """
    message = get_message(ctx)
    if is_str:
        return message.response_attachment.decode('utf-8')
    return message.response_attachment
