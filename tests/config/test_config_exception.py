# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test config exception types."""

from trpc_sdk.config import ConfigDecoderError
from trpc_sdk.config import GetConfigKeyError
from trpc_sdk.config import ProviderError
from trpc_sdk.config import ProviderNotExist


def test_get_config_key_error_fields():
    err = GetConfigKeyError("foo", "missing")
    assert err.key == "foo"
    assert err.msg == "missing"


def test_decode_error_repr():
    err = ConfigDecoderError("yaml", "bad indent")
    assert repr(err) == "Decoder yaml decode error: bad indent"


def test_provider_error_repr():
    err = ProviderError("file", "not readable")
    assert repr(err) == "not readable"


def test_provider_not_exist_fields_and_repr():
    err = ProviderNotExist("file:///tmp/x", "gone")
    assert err.provider == "file:///tmp/x"
    assert err.msg == "gone"
    assert repr(err) == "provider: file:///tmp/x not exist"
