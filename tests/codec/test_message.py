# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

from trpc_sdk._inner.pb import trpc_pb2
from trpc_sdk.abc import TrpcContext
from trpc_sdk.codec import FrameHead
from trpc_sdk.codec import Message
from trpc_sdk.codec import clone_message
from trpc_sdk.codec import create_message
from trpc_sdk.codec import get_message
from trpc_sdk.compressor import CompressType
from trpc_sdk.exceptions import TrpcErrorCode
from trpc_sdk.exceptions import TrpcException
from trpc_sdk.exceptions import create_frame_error
from trpc_sdk.serialization import SerializationType


def test_message():
    ctx = TrpcContext()
    msg = get_message(ctx)
    assert msg == ctx.msg
    ctx, msg = create_message(ctx)
    assert isinstance(msg, Message)
    assert get_message(ctx) is msg

    req_head = trpc_pb2.RequestProtocol()
    rsp_head = trpc_pb2.ResponseProtocol()
    frame_head = FrameHead()
    msg.set_frame_head(frame_head)
    assert frame_head == msg.frame_head
    msg.set_timeout(1000)
    assert 1000 == msg.timeout
    msg.set_serialization_type(SerializationType.PB)
    assert SerializationType.PB == msg.serialization_type
    msg.set_compress_type(CompressType.Noop)
    assert CompressType.Noop == msg.compress_type
    msg.set_dyeing(True)
    assert True is msg.dyeing
    msg.set_dyeing_key("hellotrpc")
    assert "hellotrpc" == msg.dyeing_key
    msg.set_server_rpc_name("/package.service/method")
    assert "/package.service/method" == msg.server_rpc_name
    msg.set_client_rpc_name("/package.service1/method")
    assert "/package.service1/method" == msg.client_rpc_name
    msg.set_server_meta_data({"trpc": "test".encode("utf8")})
    assert {"trpc": "test".encode("utf8")} == msg.server_meta_data
    msg.set_client_meta_data({})
    assert {} == msg.client_meta_data
    msg.set_local_ip("127.0.0.1")
    assert "127.0.0.1" == msg.local_ip
    msg.set_remote_addr("127.0.0.1")
    assert "127.0.0.1" == msg.remote_addr
    msg.set_caller_service_name("package.service")
    assert "package.service" == msg.caller_service_name
    msg.set_callee_service_name("package.service1")
    assert "package.service1" == msg.callee_service_name
    msg.set_caller_server("callerServer")
    assert "callerServer" == msg.caller_server
    msg.set_callee_server("calleeServer")
    assert "calleeServer" == msg.callee_server
    msg.set_caller_app("callerApp")
    assert "callerApp" == msg.caller_app
    msg.set_callee_app("calleeApp")
    assert "calleeApp" == msg.callee_app
    msg.set_caller_service("callerService")
    assert "callerService" == msg.caller_service
    msg.set_callee_service("calleeService")
    assert "calleeService" == msg.callee_service
    msg.set_caller_method("callerMethod")
    assert "callerMethod" == msg.caller_method
    msg.set_callee_method("calleeMethod")
    assert "calleeMethod" == msg.callee_method
    msg.set_namespace("development")
    assert "development" == msg.namespace
    msg.set_env_name("envName")
    assert "envName" == msg.env_name
    msg.set_env_transfer("envTransfer")
    assert "envTransfer" == msg.env_transfer
    msg.set_logger(None)
    msg.set_server_rsp_err(create_frame_error(TrpcErrorCode.TRPC_INVOKE_SUCCESS, ""))
    assert isinstance(msg.server_rsp_err, TrpcException)
    assert TrpcErrorCode.TRPC_INVOKE_SUCCESS == msg.server_rsp_err.code
    msg.set_client_rsp_err(create_frame_error(TrpcErrorCode.TRPC_INVOKE_SUCCESS, ""))
    assert isinstance(msg.client_rsp_err, TrpcException)
    msg.set_server_req_head(req_head)
    assert isinstance(msg.server_req_head, trpc_pb2.RequestProtocol)
    msg.set_client_req_head(req_head)
    assert isinstance(msg.client_req_head, trpc_pb2.RequestProtocol)
    msg.set_server_rsp_head(rsp_head)
    assert isinstance(msg.server_rsp_head, trpc_pb2.ResponseProtocol)
    msg.set_client_rsp_head(rsp_head)
    assert isinstance(msg.client_rsp_head, trpc_pb2.ResponseProtocol)
    msg.set_request_attachment(b"request attachment")
    assert msg.request_attachment == b"request attachment"
    msg.set_response_attachment(b"response attachment")
    assert msg.response_attachment == b"response attachment"

    assert msg == get_message(ctx)

    ctx, new_msg = clone_message(ctx)
    assert isinstance(new_msg, Message)
    assert new_msg.caller_service_name == msg.callee_service_name
    assert new_msg.caller_app == msg.callee_app
    assert new_msg.caller_server == msg.callee_server
    assert new_msg.caller_service == msg.callee_service
    assert new_msg.caller_method == msg.callee_method
    assert new_msg.request_attachment == b"request attachment"
    assert new_msg.response_attachment == b"response attachment"
