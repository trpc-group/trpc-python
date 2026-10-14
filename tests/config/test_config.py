# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test config"""

from typing import List

import pytest

from trpc_sdk import plugin
from trpc_sdk.config import TRPCConfig
from trpc_sdk.config import config
from trpc_sdk.config import global_provider_map
from trpc_sdk.config import new_config
from trpc_sdk.config import register_provider
from trpc_sdk.config import sync_load
from trpc_sdk.config import with_codec
from trpc_sdk.config import with_provider
from trpc_sdk.config.exception import DataSourceError
from trpc_sdk.config.exception import DecodeError
from trpc_sdk.config.noop_decoder import NoopDecoder
from trpc_sdk.plugin import PluginType
from trpc_sdk.plugin import get_plugin
from trpc_sdk.utils import UNIX_TYPE


def test_trpc_config():
    """Test trpc config"""
    TRPCConfig('FileProvider', 'YamlDecoder', {}, {})
    assert config.SocketType.UNIX == "unix"
    assert config.SocketType.UNIX == UNIX_TYPE


def test_file_config_with_yaml_decoder():
    """Test yam decoder"""
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
    ds_option = {"file_path": "./tests/test_config_data/config.yaml"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "YamlDecoder", ds_option, de_option)
    assert trpc_config.load() == data


def test_file_config_with_json_decoder():
    """Test json decoder"""
    data = {"test-config-1": 1, "test-config-2": "hello trpc"}
    ds_option = {"file_path": "./tests/test_config_data/config.json"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "JsonDecoder", ds_option, de_option)
    assert trpc_config.load() == data


def test_file_config_with_tconf_kv_decoder():
    """Test tconf kv decoder"""
    data = {"test-config-1": '1', "test-config-2": "hello trpc"}
    ds_option = {"file_path": "./tests/test_config_data/config.txt"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "TConfKVDecoder", ds_option, de_option)
    assert trpc_config.load() == data


def test_get_key_value():
    """Test get key"""
    ds_option = {"file_path": "./tests/test_config_data/config.json"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "JsonDecoder", ds_option, de_option)
    trpc_config.load()
    assert trpc_config.get('test-config-1') == 1


def test_get_key_default_value():
    """Test default value"""
    ds_option = {"file_path": "./tests/test_config_data/config.json"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "JsonDecoder", ds_option, de_option)
    trpc_config.load()
    assert trpc_config.get('test-config-999', 999) == 999


def test_exception_decoder_error():
    """Test decoder error"""
    ds_option = {"file_path": "./tests/test_config_data/config.yaml"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "JsonDecoder", ds_option, de_option)
    with pytest.raises(DecodeError):
        trpc_config.load()


def test_exception_data_source_error():
    """Test exception"""
    trpc_config = TRPCConfig('FileProvider', 'YamlDecoder', {}, {})
    with pytest.raises(DataSourceError):
        trpc_config.load()


class MockGlobal:
    """Mock global config"""

    def __init__(self, namespace: str = '', env_name: str = ''):
        self.namespace = namespace
        self.env_name = env_name


class MockService:
    """Mock service config"""

    def __init__(self,
                 name: str = '',
                 ip: str = '',
                 port: int = 0,
                 network: str = '',
                 protocol: str = '',
                 timeout: int = 0):
        """
        MockService
        :param name: `name` name
        :param ip: `ip` ip
        :param port: `port` port
        :param network: `network` network
        :param protocol: `protocol` protocol
        :param timeout: `timeout` timeout
        """
        self.name = name
        self.ip = ip
        self.port = port
        self.network = network
        self.protocol = protocol
        self.timeout = timeout


class MockServer:
    """Mock server config"""

    def __init__(self,
                 app: str = '',
                 server: str = '',
                 bin_path: str = '',
                 conf_path: str = '',
                 data_path: str = '',
                 service: List[MockService] = None):
        """
        MockServer
        :param app: `app` app
        :param server: `server` server
        :param bin_path: `bin_path` bin_path
        :param conf_path: `conf_path` conf_path
        :param data_path: `data_path` data_path
        :param service: `service` service
        """
        self.app = app
        self.server = server
        self.bin_path = bin_path
        self.conf_path = conf_path
        self.data_path = data_path
        self.service = service or []


class MockClient:
    """Mock client config"""

    def __init__(self,
                 timeout: int = 0,
                 namespace: str = '',
                 filters: List[str] = None,
                 service: List[MockService] = None):
        self.timeout = timeout
        self.namespace = namespace
        self.filters = filters or []
        self.service = service or []


class MockConfig:
    """Mock config"""

    def __init__(self, global_config: MockGlobal = None, server: MockServer = None, client: MockClient = None):
        """
        MockConfig
        :param global_config: `global` global
        :param server: `server` server
        :param client: `client` client
        """
        self.global_config = global_config
        self.server = server
        self.client = client


class MockSubConfig:
    """Mock sub config"""

    def __init__(self, server: MockServer = None):
        self.server = server


def test_config_unmarshal_part():
    """Test unmarshal config"""
    ds_option = {"file_path": "./tests/test_config_data/unmarshal.yaml"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "YamlDecoder", ds_option, de_option)
    trpc_config.load()
    server_option = MockServer()
    trpc_config.unmarshal({'server': server_option})
    assert server_option.service[0].name == 'trpc_sdk.test.helloworld.Greeter'


def test_config_unmarshal_all():
    """Test unmarshal all config"""
    ds_option = {"file_path": "./tests/test_config_data/unmarshal.yaml"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "YamlDecoder", ds_option, de_option)
    trpc_config.load()
    all_option = MockConfig()
    trpc_config.unmarshal(all_option)
    assert all_option.server.service[1].name == 'trpc_sdk.test.helloworld.Greeter2'
    assert all_option.global_config.namespace == 'Development'
    assert all_option.client.filters == []


def test_config_unmarshal_list():
    """Test unmarshal list config"""
    ds_option = {"file_path": "./tests/test_config_data/unmarshal.yaml"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "YamlDecoder", ds_option, de_option)
    trpc_config.load()
    service_option_0 = MockService()
    service_option_1 = MockService()
    trpc_config.unmarshal({'server': {'service': [service_option_0, service_option_1]}})
    assert service_option_0.name == 'trpc_sdk.test.helloworld.Greeter'
    assert service_option_1.name == 'trpc_sdk.test.helloworld.Greeter2'


def test_sub_config():
    """Test sub config"""
    ds_option = {"file_path": "./tests/test_config_data/unmarshal.yaml"}
    de_option = {"code": "utf8"}
    trpc_config = TRPCConfig("FileProvider", "YamlDecoder", ds_option, de_option)
    trpc_config.load()
    sub_option = MockSubConfig()
    trpc_config.unmarshal(sub_option)
    assert sub_option.server.service[1].name == 'trpc_sdk.test.helloworld.Greeter2'


class MockProvider:
    """Mock provider"""

    def watch(self, file_name: str, cb):
        """Watch config"""
        cb(file_name.encode("utf8"))


def test_new_config():
    """Test new config"""
    global_provider_map["tconf"] = MockProvider()
    t_config = new_config("config.yaml", with_codec("YamlDecoder"), with_provider("tconf"))
    assert t_config.path == "config.yaml"
    assert isinstance(t_config.provider, MockProvider)
    t_config.watch()


def test_register_provider():
    """Test register provider"""
    register_provider("test", "test_provider")
    assert global_provider_map["test_provider"] == "test"


def test_load_global_config():
    """Test load global config"""
    file_path = './tests/test_config_data/trpc_python.yaml'
    # 为了正常加载FileDataProvider插件，正常情况下，框架起服务的时候会帮我们加载好插件
    config.load_global_config(file_path, 'utf-8')
    assert config.global_config.get('server', '')['app'] == 'test'


# 测试加载自定义配置文件
def test_load_config_from_file():
    """Test load global config"""
    file_path = './tests/test_config_data/trpc_python.yaml'
    # 为了正常加载FileDataProvider插件，正常情况下，框架起服务的时候会帮我们加载好插件
    config.load_global_config(file_path, 'utf-8')
    plugin.setup_master()
    test_config = sync_load(file_path, with_provider('file'), with_codec('YamlDecoder'))
    assert test_config.get('server', '').get('app') == 'test'


class TestConfigDecoder:
    """Test config decoder class"""

    def test_noop_decoder(self):
        """Test noop decoder"""
        # test register
        decoder = get_plugin(PluginType.CONFIG_DECODER, "NoopDecoder")
        assert isinstance(decoder, NoopDecoder)
        # test decoder
        decoder = NoopDecoder()
        config_data = b"test"
        decoded_config = decoder.decode(config_data)
        assert decoded_config == config_data


def test_register_http_kwargs():
    """Test register_http_kwargs"""
    config.register_http_kwargs("trpc_sdk.py_stream.helloworld.Greeter", {
        "max_headers": 32768 * 2,
        "max_field_size": 8190 * 2
    })
    assert len(config.get_http_kwargs("test_register_http_kwargs")) == 0
    http_kwargs = config.get_http_kwargs("trpc_sdk.py_stream.helloworld.Greeter")
    assert len(http_kwargs)
    assert http_kwargs["max_headers"] == 32768 * 2
    assert http_kwargs["max_field_size"] == 8190 * 2
