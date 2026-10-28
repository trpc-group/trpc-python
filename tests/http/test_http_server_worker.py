# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test http module"""
import tempfile

from aiohttp import web

from trpc_sdk.abc import BaseServerTransportOption
from trpc_sdk.http import HttpServerTransport
from trpc_sdk.http import RegisteredRoutes
from trpc_sdk.http import StaticFileRouters
from trpc_sdk.http import register_static
from trpc_sdk.http import route
from trpc_sdk.server import ServiceOption
from trpc_sdk.server import create_service_adapter
from trpc_sdk.transport import ssl_context_from_opts


def test_https_cert():
    """Test https cert"""
    opt = BaseServerTransportOption()
    opt.tls_key = "./examples/helloworld/cert/domain_srv.key"
    opt.tls_cert = "./examples/helloworld/cert/domain_srv.crt"
    try:
        ssl_context_from_opts(opt)
    except Exception:  # pylint: disable=broad-except
        assert False
    else:
        assert True


def test_https_cert_empty():
    """Test https empty cert"""
    opt = BaseServerTransportOption()
    opt.tls_key = ""
    opt.tls_cert = ""
    try:
        ret = ssl_context_from_opts(opt)
    except Exception:  # pylint: disable=broad-except
        assert False
    else:
        assert ret is None


def test_https_without_cert_key():
    """Test https cert key"""
    opt = BaseServerTransportOption()
    opt.tls_key = "fake"
    opt.tls_cert = "fake"
    try:
        ssl_context_from_opts(opt)
    except KeyError:  # pylint: disable=broad-except
        assert True
    else:
        assert False


def test_https_without_cert():
    """Test https without cert"""
    opt = BaseServerTransportOption()
    opt.tls_key = "./examples/helloworld/cert/domain_srv.key"
    opt.tls_cert = "fake"
    try:
        ssl_context_from_opts(opt)
    except ValueError:  # pylint: disable=broad-except
        assert True
    else:
        assert False


def test_register_static():
    """Test register static"""
    saved = dict(StaticFileRouters)
    try:
        register_static("/static", "static")
        assert StaticFileRouters["/static"] == "static"
    finally:
        StaticFileRouters.clear()
        StaticFileRouters.update(saved)


async def _service_1(_, __) -> bytes:  # return rsp
    return b"_service_1"


async def _service_2(_, __) -> bytes:  # return rsp
    return b"_service_2"


async def _other_service(_, __) -> bytes:
    return b"_other_service"


def test_many_service():
    """User routes for this service, and only this service, land on the app."""
    saved_routes = dict(RegisteredRoutes)
    saved_static = dict(StaticFileRouters)
    RegisteredRoutes.clear()
    StaticFileRouters.clear()
    try:
        route("/test", service_name="trpc_sdk.test.many.service")(_service_1)
        route("/test1")(_service_2)
        route("/other", service_name="trpc_sdk.test.other.service")(_other_service)

        adapter = create_service_adapter("trpc_sdk.test.many.service", ServiceOption())
        transport = HttpServerTransport()
        transport.bind(adapter.opt.transport_option)

        with tempfile.TemporaryDirectory() as static_dir:
            register_static("/static", static_dir)
            web_app = transport._build_app()

        paths = []
        for resource in web_app.router.resources():
            canonical = getattr(resource, "canonical", None)
            if canonical:
                paths.append(canonical)
        assert "/test" in paths
        assert "/test1" in paths
        assert "/other" not in paths
        assert any(isinstance(resource, web.StaticResource) for resource in web_app.router.resources())
    finally:
        RegisteredRoutes.clear()
        RegisteredRoutes.update(saved_routes)
        StaticFileRouters.clear()
        StaticFileRouters.update(saved_static)
