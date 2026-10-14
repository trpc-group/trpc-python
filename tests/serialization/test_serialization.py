# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import json

from examples.helloworld.stub.trpc_trpc_python_group_helloworld import helloworld_pb2
from trpc_sdk.codec import SerializationType
from trpc_sdk.codec import deserialize
from trpc_sdk.codec import serialize
from trpc_sdk.codec.serialization_noop import Body


def test_serialization():
    # unsupported
    body = helloworld_pb2.HelloRequest()
    body.msg = 'test'
    assert serialize(SerializationType.Unsupported, body) == b''

    body = helloworld_pb2.HelloRequest()
    data = b'\n\x04test'
    assert deserialize(SerializationType.Unsupported, data, body) is None

    # Noop
    body = Body()
    body.data = b'test'
    assert serialize(SerializationType.Noop, body) == b'test'

    body = Body()
    data = b'\n\x04test'
    assert deserialize(SerializationType.Noop, data, body) is None
    assert body.data == data

    # body or data empty
    body = None
    assert serialize(SerializationType.PB, body) == b''

    body = helloworld_pb2.HelloRequest()
    assert deserialize(SerializationType.PB, None, body) is None

    body = None
    data = b'\n\x04test'
    assert deserialize(SerializationType.PB, data, body) is None
    body = helloworld_pb2.HelloRequest()
    data = b''
    assert deserialize(SerializationType.PB, data, body) is None

    # pb
    body = helloworld_pb2.HelloRequest()
    body.msg = 'testpb'
    data = serialize(SerializationType.PB, body)
    assert data == b'\n\x06testpb'

    body = helloworld_pb2.HelloRequest()
    deserialize(SerializationType.PB, data, body)
    assert body.msg == 'testpb'

    # json
    body = helloworld_pb2.HelloRequest()
    body.msg = 'testjson'
    data = serialize(SerializationType.JSON, body)
    parsed = json.loads(data)
    assert parsed['msg'] == 'testjson'
    # Empty repeated fields may be omitted depending on protobuf version / JSON options.
    assert parsed.get('msg_list', []) == []

    body = helloworld_pb2.HelloRequest()
    deserialize(SerializationType.JSON, data, body)
    assert body.msg == 'testjson'
