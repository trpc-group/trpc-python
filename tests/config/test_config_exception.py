# -*- coding: utf-8 -*-
"""Test config exception types."""

from trpc_sdk.config.exception import (
    DataSourceError,
    DataSourceNotExist,
    DecodeError,
    GetConfigKeyError,
)


def test_get_config_key_error_fields():
    err = GetConfigKeyError("foo", "missing")
    assert err.key == "foo"
    assert err.msg == "missing"


def test_decode_error_repr():
    err = DecodeError("yaml", "bad indent")
    assert repr(err) == "Decoder yaml decode error: bad indent"


def test_data_source_error_repr():
    err = DataSourceError("file", "not readable")
    assert repr(err) == "not readable"


def test_data_source_not_exist_fields_and_repr():
    err = DataSourceNotExist("file:///tmp/x", "gone")
    assert err.data_source == "file:///tmp/x"
    assert err.msg == "gone"
    assert repr(err) == "datasource: file:///tmp/x not exist"
