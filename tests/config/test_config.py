# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test config"""

import pytest

from trpc_sdk.abc import BaseConfigProvider
from trpc_sdk.abc import PluginType
from trpc_sdk.config import Config
from trpc_sdk.config import ConfigLoader
from trpc_sdk.config import ConfigLoaderFactory
from trpc_sdk.config import NoopDecoder
from trpc_sdk.config import ServerConfig
from trpc_sdk.config import ServerService
from trpc_sdk.config import SocketType
from trpc_sdk.config import TrpcConfig
from trpc_sdk.config import TrpcConfigParser
from trpc_sdk.config import YamlDecoder
from trpc_sdk.config import with_codec
from trpc_sdk.config import with_provider
from trpc_sdk.http import register_http_session_params
from trpc_sdk.http._client_transport import _http_session_params
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin
from trpc_sdk.utils import UNIX_TYPE


def _parser(path, decoder):
    return TrpcConfigParser("file", decoder, {"file_path": path}, {"code": "utf8"})


def test_trpc_config_entrypoint():
    """TrpcConfig is a classmethod namespace, and socket types stay string values."""
    with pytest.raises(TypeError):
        TrpcConfig()
    assert SocketType.UNIX == "unix"
    assert SocketType.UNIX == UNIX_TYPE
    assert SocketType.NET == "net"


def test_file_config_with_yaml_decoder():
    """Test yaml decoder"""
    data = {
        'Server': {
            'Selector': {
                'Name': 'direct',
                'IP': '123.122.11.1',
                'Port': 3312,
                'Filter': None,
                'Desc': '加一句来测试中文'
            }
        }
    }
    trpc_config = _parser("./tests/test_config_data/config.yaml", "yaml")
    assert trpc_config.load() == data


def test_file_config_with_json_decoder():
    """Test json decoder"""
    data = {"test-config-1": 1, "test-config-2": "hello trpc"}
    trpc_config = _parser("./tests/test_config_data/config.json", "json")
    assert trpc_config.load() == data


def test_file_config_with_noop_decoder():
    """Noop decoder returns the file bytes unchanged. The kv decoder is gone."""
    path = "./tests/test_config_data/config.txt"
    with open(path, "rb") as config_file:
        raw = config_file.read()
    trpc_config = _parser(path, "noop")
    assert trpc_config.load() == raw


def test_get_key_value():
    """Test get key"""
    trpc_config = _parser("./tests/test_config_data/config.json", "json")
    trpc_config.load()
    assert trpc_config.get('test-config-1') == 1


def test_get_key_default_value():
    """Test default value"""
    trpc_config = _parser("./tests/test_config_data/config.json", "json")
    trpc_config.load()
    assert trpc_config.get('test-config-999', 999) == 999


def test_exception_decoder_error():
    """Test decoder error"""
    from trpc_sdk.config import ConfigDecoderError
    trpc_config = _parser("./tests/test_config_data/config.yaml", "json")
    with pytest.raises(ConfigDecoderError):
        trpc_config.load()


def test_exception_data_source_error():
    """Missing file_path currently fails while building ProviderError."""
    trpc_config = TrpcConfigParser('file', 'yaml', {}, {})
    with pytest.raises(TypeError, match="data_source"):
        trpc_config.load()


def test_config_parse_server_section():
    """Parse the server section of a framework YAML."""
    trpc_config = _parser("./tests/test_config_data/unmarshal.yaml", "yaml")
    trpc_config.load()
    server = ServerConfig()
    trpc_config.parse({'server': server})
    assert server.service[0].name == 'trpc.test.helloworld.Greeter'


def test_config_parse_all():
    """Parse a whole framework YAML into Config."""
    trpc_config = _parser("./tests/test_config_data/unmarshal.yaml", "yaml")
    trpc_config.load()
    all_option = Config()
    trpc_config.parse(all_option)
    assert all_option.server.service[1].name == 'trpc.test.helloworld.Greeter2'
    assert all_option.global_config.namespace == 'Development'
    assert all_option.client.filters == []


def test_config_parse_service_list():
    """Parse a service list into pre-built objects."""
    trpc_config = _parser("./tests/test_config_data/unmarshal.yaml", "yaml")
    trpc_config.load()
    service_option_0 = ServerService()
    service_option_1 = ServerService()
    trpc_config.parse({'server': {'service': [service_option_0, service_option_1]}})
    assert service_option_0.name == 'trpc.test.helloworld.Greeter'
    assert service_option_1.name == 'trpc.test.helloworld.Greeter2'


def test_sub_config():
    """Parse only the server subtree."""
    trpc_config = _parser("./tests/test_config_data/unmarshal.yaml", "yaml")
    trpc_config.load()
    server = ServerConfig()
    trpc_config.parse({'server': server})
    assert server.service[1].name == 'trpc.test.helloworld.Greeter2'


class _MockProvider(BaseConfigProvider):
    """Provider that reports one YAML document through hot reload."""

    def load(self, file_path, options=None):
        return b"a: 1\n"

    def hot_reload(self, key, callback, options=None):
        callback(b"a: 1\n")

    def set_options(self, options):
        return None


def test_config_loader_hot_reload():
    """ConfigLoader pairs a provider with a decoder and hot-reloads."""
    loader = ConfigLoader("config.yaml", provider=_MockProvider(), decoder=YamlDecoder())
    assert loader.path == "config.yaml"
    assert isinstance(loader.provider, _MockProvider)
    loader.hot_reload()
    assert loader.get("a", None) == 1


@register_plugin(PluginType.CONFIG_PROVIDER, "unit_test_provider")
class _RegisteredProvider(BaseConfigProvider):
    """Provider registered so the plugin table can be looked up."""

    def load(self, file_path, options=None):
        return b""

    def hot_reload(self, key, callback, options=None):
        return None

    def set_options(self, options):
        return None


def test_register_provider():
    """Registered config providers are returned by get_plugin."""
    assert isinstance(get_plugin(PluginType.CONFIG_PROVIDER, "unit_test_provider"), _RegisteredProvider)


def test_load_global_config():
    """Test load global config"""
    file_path = './tests/test_config_data/trpc_python.yaml'
    loaded = TrpcConfig.load(file_path, 'utf-8')
    assert loaded.server.app == 'test'
    assert TrpcConfig.server_config().app == 'test'


def test_load_config_from_file():
    """Load an application YAML through the file provider and yaml decoder."""
    file_path = './tests/test_config_data/trpc_python.yaml'
    test_config = ConfigLoaderFactory.sync_load(file_path, with_provider('file'), with_codec('yaml'))
    assert test_config.get('server', '').get('app') == 'test'


class TestConfigDecoder:
    """Test config decoder class"""

    def test_noop_decoder(self):
        """Test noop decoder"""
        decoder = get_plugin(PluginType.CONFIG_DECODER, "noop")
        assert isinstance(decoder, NoopDecoder)
        decoder = NoopDecoder()
        config_data = b"test"
        decoded_config = decoder.decode(config_data)
        assert decoded_config == config_data


def test_register_http_session_params():
    """HTTP session kwargs are stored under the session name."""
    register_http_session_params({
        "max_headers": 32768 * 2,
        "max_field_size": 8190 * 2
    }, "trpc_sdk.py_stream.helloworld.Greeter")
    assert "test_register_http_kwargs" not in _http_session_params
    http_kwargs = _http_session_params["trpc_sdk.py_stream.helloworld.Greeter"]
    assert http_kwargs["max_headers"] == 32768 * 2
    assert http_kwargs["max_field_size"] == 8190 * 2
