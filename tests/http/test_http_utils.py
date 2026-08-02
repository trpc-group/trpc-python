# -*- coding: utf-8 -*-
"""Test http utils"""
from unittest.mock import MagicMock

import pytest
from aiohttp import hdrs

from trpc_sdk.exceptions import TRPCBaseError, ServerDecodeError
from trpc_sdk.http._utils import InvalidURL, UrlParser
from trpc_sdk.context import TrpcContext
from trpc_sdk.http import _api
from trpc_sdk.http._http_response import HttpClientRspHeader, TrpcHttpResponse
from trpc_sdk.http.dict_pb_convertor import dict_to_protobuf
from trpc_sdk.http.http_ctx_manager import _BaseRequestContextManager, _RequestContextManager
from trpc_sdk.http.serialization_form import FormSerializer

from .. import helloworld_pb2


class TestUrlParser:
    """Test UrlParser class"""

    def test_parse_standard_http_url(self):
        """Test parsing standard http url"""
        parser = UrlParser('http://example.com:8080/path?key=value')
        result = parser.parse()
        assert result.scheme == 'http'
        assert result.netloc == 'example.com:8080'
        assert result.path == '/path'
        assert result.query == 'key=value'

    def test_parse_standard_https_url(self):
        """Test parsing standard https url"""
        parser = UrlParser('https://example.com/path')
        result = parser.parse()
        assert result.scheme == 'https'
        assert result.netloc == 'example.com'
        assert result.path == '/path'

    def test_parse_custom_scheme_url(self):
        """Test parsing custom scheme url like http_test://a.b/c"""
        parser = UrlParser('http_test://a.b/c')
        result = parser.parse()
        assert result.scheme == 'http_test'
        assert result.netloc == 'a.b'
        assert result.path == '/c'

    def test_parse_polaris_scheme_url(self):
        """Test parsing polaris scheme url"""
        parser = UrlParser('polaris://trpc_sdk.a.b.c/a/b/c?a=1')
        result = parser.parse()
        assert result.scheme == 'polaris'
        assert result.netloc == 'trpc_sdk.a.b.c'
        assert result.path == '/a/b/c'
        assert result.query == 'a=1'

    def test_parse_url_missing_separator(self):
        """Test parsing url without :// separator"""
        parser = UrlParser('http/example.com/path')
        with pytest.raises(InvalidURL) as exc_info:
            parser.parse()
        assert "missing '://'" in str(exc_info.value)

    def test_parse_url_multiple_separators(self):
        """Test parsing url with multiple :// separators"""
        parser = UrlParser('custom_scheme://example://com/path')
        with pytest.raises(InvalidURL) as exc_info:
            parser.parse()
        assert "multiple '://' found" in str(exc_info.value)

    def test_parse_url_empty_scheme(self):
        """Test parsing url with empty scheme"""
        parser = UrlParser('://example.com/path')
        with pytest.raises(InvalidURL) as exc_info:
            parser.parse()
        assert "empty scheme" in str(exc_info.value)

    def test_check_url_valid(self):
        """Test check_url with valid url"""
        parser = UrlParser('http://example.com/path')
        parser.parse()
        assert parser.check_url() is True

    def test_get_host(self):
        """Test get_host method"""
        parser = UrlParser('http://example.com:8080/path')
        parser.parse()
        assert parser.get_host() == 'example.com:8080'

    def test_get_http_url_without_addr(self):
        """Test get_http_url without addr parameter"""
        parser = UrlParser('polaris://trpc_sdk.a.b.c/a/b/c?a=1')
        parser.parse()
        headers = {}
        http_url = parser.get_http_url(headers)
        assert http_url == 'polaris://trpc_sdk.a.b.c/a/b/c?a=1'
        assert headers[hdrs.HOST] == 'trpc_sdk.a.b.c'

    def test_get_http_url_with_addr(self):
        """Test get_http_url with addr parameter"""
        parser = UrlParser('polaris://trpc_sdk.a.b.c/a/b/c?a=1')
        parser.parse()
        headers = {}
        http_url = parser.get_http_url(headers, '10.10.10.10:1123')
        assert http_url == 'http://10.10.10.10:1123/a/b/c?a=1'
        assert headers[hdrs.HOST] == '10.10.10.10:1123'

    def test_get_http_url_with_addr_complex_path(self):
        """Test get_http_url with addr and complex path"""
        parser = UrlParser('custom_scheme://service.name:9090/api/v1/users?id=123&name=test')
        parser.parse()
        headers = {}
        http_url = parser.get_http_url(headers, '192.168.1.100:8080')
        assert http_url == 'http://192.168.1.100:8080/api/v1/users?id=123&name=test'
        assert headers[hdrs.HOST] == '192.168.1.100:8080'

    def test_get_http_url_preserves_fragment(self):
        """Test get_http_url preserves fragment"""
        parser = UrlParser('polaris://service.name/path?query=1#fragment')
        parser.parse()
        headers = {}
        http_url = parser.get_http_url(headers, '10.0.0.1:8000')
        assert http_url == 'http://10.0.0.1:8000/path?query=1#fragment'
        assert headers[hdrs.HOST] == '10.0.0.1:8000'

    def test_get_http_url_without_parse(self):
        """Test get_http_url without calling parse first"""
        parser = UrlParser('polaris://trpc_sdk.a.b.c/a/b/c')
        headers = {}
        with pytest.raises(InvalidURL) as exc_info:
            parser.get_http_url(headers)
        assert "not parsed yet" in str(exc_info.value)

    def test_get_http_url_with_empty_path(self):
        """Test get_http_url with empty path"""
        parser = UrlParser('polaris://service.name')
        parser.parse()
        headers = {}
        http_url = parser.get_http_url(headers, '127.0.0.1:8080')
        assert http_url == 'http://127.0.0.1:8080'
        assert headers[hdrs.HOST] == '127.0.0.1:8080'

    def test_get_http_url_replaces_any_scheme(self):
        """Test get_http_url replaces any custom scheme with http"""
        schemes = ['polaris', 'consul', 'etcd', 'custom_discovery', 'trpc']
        for scheme in schemes:
            parser = UrlParser(f'{scheme}://service.name/path')
            parser.parse()
            headers = {}
            http_url = parser.get_http_url(headers, '1.2.3.4:5678')
            assert http_url == 'http://1.2.3.4:5678/path'
            assert headers[hdrs.HOST] == '1.2.3.4:5678'

    def test_invalid_url_exception_type(self):
        """Test InvalidURL exception is TRPCBaseError"""
        parser = UrlParser('invalid_url')
        with pytest.raises(TRPCBaseError):
            parser.parse()

    def test_parse_url_with_port(self):
        """Test parsing url with port in custom scheme"""
        parser = UrlParser('custom://host.name:9090/path')
        result = parser.parse()
        assert result.scheme == 'custom'
        assert result.netloc == 'host.name:9090'
        assert result.path == '/path'

    def test_get_http_url_with_params(self):
        """Test get_http_url preserves params"""
        parser = UrlParser('polaris://service/path;params?query=1')
        parser.parse()
        headers = {}
        http_url = parser.get_http_url(headers, '10.0.0.1:8000')
        assert 'http://10.0.0.1:8000/path' in http_url
        assert 'params' in http_url
        assert 'query=1' in http_url
        assert headers[hdrs.HOST] == '10.0.0.1:8000'

    def test_get_http_url_with_existing_host_header(self):
        """Test get_http_url with existing HOST header"""
        parser = UrlParser('polaris://service.name/path')
        parser.parse()
        headers = {hdrs.HOST: 'existing.host:9999'}
        http_url = parser.get_http_url(headers, '127.0.0.1:8080')
        assert http_url == 'http://127.0.0.1:8080/path'
        # HOST header should not be overwritten
        assert headers[hdrs.HOST] == 'existing.host:9999'

    def test_get_http_url_without_addr_sets_host_header(self):
        """Test get_http_url without addr sets HOST header from netloc"""
        parser = UrlParser('polaris://original.service:8080/path')
        parser.parse()
        headers = {}
        http_url = parser.get_http_url(headers)
        assert http_url == 'polaris://original.service:8080/path'
        assert headers[hdrs.HOST] == 'original.service:8080'
