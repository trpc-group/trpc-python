# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test http request module"""

from aiohttp import hdrs

from trpc_sdk.http import HTTP
from trpc_sdk.http import HttpClientReqHeader
from trpc_sdk.http import UrlParser


class TestHttpClientReqHeader:
    """Test HttpClientReqHeader class"""

    def test_init_with_defaults(self):
        """Test HttpClientReqHeader initialization with default values"""
        req_header = HttpClientReqHeader()

        assert req_header.method == hdrs.METH_GET
        assert req_header.host == ""
        assert not req_header.headers
        assert req_header.stream is False
        assert req_header.scheme == HTTP
        assert req_header.parsed_url is None
        assert req_header.url_parser is None

    def test_init_with_custom_values(self):
        """Test HttpClientReqHeader initialization with custom values"""
        headers = {"Content-Type": "application/json"}
        req_header = HttpClientReqHeader(method=hdrs.METH_POST,
                                     host="example.com",
                                     headers=headers,
                                     stream=True,
                                     scheme="https")

        assert req_header.method == hdrs.METH_POST
        assert req_header.host == "example.com"
        assert req_header.headers == headers
        assert req_header.stream is True
        assert req_header.scheme == "https"

    def test_init_with_url_parser(self):
        """Test HttpClientReqHeader initialization with url_parser"""
        url = "http://example.com:8080/path?query=1"
        url_parser = UrlParser(url)
        url_parser.parse()

        req_header = HttpClientReqHeader(url_parser=url_parser)

        assert req_header.url_parser is not None
        assert req_header.parsed_url is not None
        assert req_header.scheme == "http"
        assert req_header.parsed_url.netloc == "example.com:8080"

    def test_init_with_parsed_url(self):
        """Test HttpClientReqHeader initialization with parsed_url"""
        url = "https://test.com/api"
        url_parser = UrlParser(url)
        parsed_url = url_parser.parse()

        req_header = HttpClientReqHeader(parsed_url=parsed_url, url_parser=url_parser)

        assert req_header.parsed_url == parsed_url
        assert req_header.scheme == "https"

    def test_full_method_with_url_parser(self):
        """Test _full method updates scheme from url_parser"""
        url = "https://secure.example.com/api/v1"
        url_parser = UrlParser(url)
        url_parser.parse()

        req_header = HttpClientReqHeader(
            scheme="http",  # Initial scheme
            url_parser=url_parser)

        # _full should update scheme to https from url_parser
        assert req_header.scheme == "https"

    def test_full_method_without_url_parser(self):
        """Test _full method does nothing without url_parser"""
        req_header = HttpClientReqHeader(scheme="http")

        # Should remain unchanged
        assert req_header.scheme == "http"
        assert req_header.parsed_url is None

    def test_full_method_with_custom_scheme_url(self):
        """Test _full method with custom scheme URL"""
        url = "polaris://trpc_sdk.service.name/path"
        url_parser = UrlParser(url)
        url_parser.parse()

        req_header = HttpClientReqHeader(url_parser=url_parser)

        assert req_header.scheme == "polaris"
        assert req_header.parsed_url.netloc == "trpc_sdk.service.name"

    def test_str_method(self):
        """Test __str__ method returns formatted string"""
        headers = {"Authorization": "Bearer token"}
        url = "http://api.example.com/endpoint"
        url_parser = UrlParser(url)
        parsed_url = url_parser.parse()

        req_header = HttpClientReqHeader(method=hdrs.METH_POST,
                                     host="api.example.com",
                                     headers=headers,
                                     stream=True,
                                     scheme="http",
                                     parsed_url=parsed_url,
                                     url_parser=url_parser)

        result = str(req_header)

        assert "method: POST" in result
        assert "host: api.example.com" in result
        assert "Authorization" in result
        assert "stream: True" in result
        assert "scheme: http" in result
        assert "parsed_url:" in result

    def test_str_method_with_defaults(self):
        """Test __str__ method with default values"""
        req_header = HttpClientReqHeader()
        result = str(req_header)

        assert "method: GET" in result
        assert "host: " in result
        assert "stream: False" in result
        assert "scheme: http" in result

    def test_with_different_http_methods(self):
        """Test HttpClientReqHeader with different HTTP methods"""
        methods = [
            hdrs.METH_GET, hdrs.METH_POST, hdrs.METH_PUT, hdrs.METH_DELETE, hdrs.METH_PATCH, hdrs.METH_HEAD,
            hdrs.METH_OPTIONS
        ]

        for method in methods:
            req_header = HttpClientReqHeader(method=method)
            assert req_header.method == method

    def test_with_complex_headers(self):
        """Test HttpClientReqHeader with complex headers"""
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer token123",
            "X-Custom-Header": "custom-value",
            "Accept": "application/json",
            "User-Agent": "trpc-python/1.0"
        }

        req_header = HttpClientReqHeader(headers=headers)
        assert req_header.headers == headers
        assert len(req_header.headers) == 5

    def test_scheme_override_by_url_parser(self):
        """Test that url_parser scheme overrides initial scheme"""
        url = "https://example.com/path"
        url_parser = UrlParser(url)
        url_parser.parse()

        # Initialize with http scheme
        req_header = HttpClientReqHeader(scheme="http", url_parser=url_parser)

        # Should be overridden to https by url_parser
        assert req_header.scheme == "https"

    def test_parsed_url_not_overridden_if_provided(self):
        """Test that provided parsed_url is not overridden"""
        url1 = "http://example1.com/path1"
        url2 = "http://example2.com/path2"

        url_parser1 = UrlParser(url1)
        parsed_url1 = url_parser1.parse()

        url_parser2 = UrlParser(url2)
        url_parser2.parse()

        # Provide both parsed_url and url_parser
        req_header = HttpClientReqHeader(parsed_url=parsed_url1, url_parser=url_parser2)

        # parsed_url should remain as parsed_url1
        assert req_header.parsed_url == parsed_url1
        assert req_header.parsed_url.netloc == "example1.com"

    def test_with_url_containing_query_and_fragment(self):
        """Test HttpClientReqHeader with URL containing query and fragment"""
        url = "http://example.com/path?key=value&foo=bar#section"
        url_parser = UrlParser(url)
        url_parser.parse()

        req_header = HttpClientReqHeader(url_parser=url_parser)

        assert req_header.parsed_url.query == "key=value&foo=bar"
        assert req_header.parsed_url.fragment == "section"
        assert req_header.parsed_url.path == "/path"

    def test_with_url_containing_port(self):
        """Test HttpClientReqHeader with URL containing port"""
        url = "http://example.com:8080/api"
        url_parser = UrlParser(url)
        url_parser.parse()

        req_header = HttpClientReqHeader(url_parser=url_parser)

        assert req_header.parsed_url.netloc == "example.com:8080"
        assert req_header.parsed_url.port == 8080

    def test_stream_flag(self):
        """Test stream flag behavior"""
        req_header_no_stream = HttpClientReqHeader(stream=False)
        req_header_with_stream = HttpClientReqHeader(stream=True)

        assert req_header_no_stream.stream is False
        assert req_header_with_stream.stream is True

    def test_host_attribute(self):
        """Test host attribute"""
        hosts = ["example.com", "api.service.com", "localhost", "127.0.0.1"]

        for host in hosts:
            req_header = HttpClientReqHeader(host=host)
            assert req_header.host == host
