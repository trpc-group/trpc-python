# -*- coding: utf-8 -*-
"""Test http module"""
import pytest

from tests import extend_pb2
from tests import helloworld_pb2
from trpc_sdk.exceptions import ServerDecodeError
from trpc_sdk.http import serialization_form


def test_dict_to_protobuf():
    """Test dict to pb"""
    req_class = helloworld_pb2.HelloRequest
    data = b'msg=1'
    qs = serialization_form.parse_qs(data.decode("utf-8"))
    req = serialization_form.dict_to_protobuf(req_class, qs)
    assert req.msg == "1"
    # 测试扩展
    data = {'___X': {'100': 123.4, '101': 'string', '102': 4}}
    pb = extend_pb2.MessageOfTypes()
    deser = serialization_form.dict_to_protobuf(pb, data)
    assert deser.Extensions[extend_pb2.extDouble] == 123.4
    assert deser.Extensions[extend_pb2.extString] == 'string'
    assert deser.Extensions[extend_pb2.NestedExtension.extInt] == 4
    with pytest.raises(ValueError):
        data['___X']['abc'] = 100
        serialization_form.dict_to_protobuf(extend_pb2.MessageOfTypes, data)
    with pytest.raises(KeyError):
        data = {'___X': {'100': 123.4, '101': 'string', '102': 4}}
        data['___X'][-1] = 123.4
        serialization_form.dict_to_protobuf(extend_pb2.MessageOfTypes, data)

    with pytest.raises(KeyError):
        data = {'xxxx': 'aaa'}
        serialization_form.dict_to_protobuf(extend_pb2.MessageOfTypes, data)
    data = {
        'flot': 3.3999999521443642e+38,
        'i32': 2147483647,
        'i64': 9223372036854775807,
        'bol': 'True',
        'strng': 'string',
        'byts': b'\n\x14\x1e',
        'enm': 'C',
        'range': [0, 1, 2, 3, 4],
        'enmRepeated': ['A', 'C']
    }
    deser = serialization_form.dict_to_protobuf(extend_pb2.MessageOfTypes, data)
    assert deser.flot == 3.3999999521443642e+38
    assert deser.i32 == 2147483647
    assert deser.i64 == 9223372036854775807
    assert deser.bol is True
    assert deser.strng == 'string'
    assert deser.enm == extend_pb2.MessageOfTypes.C

    data = {
        'dubl': 1.7e+308,
        'flot': 3.3999999521443642e+38,
        'i32': 2147483647,
        'i64': 9223372036854775807,
        'ui32': 4294967295,
        'ui64': 18446744073709551615,
        'si32': -2147483647,
        'si64': -9223372036854775807,
        'f32': 2147483647,
        'sf32': -2147483647,
        'sf64': -9223372036854775807,
        'bol': 'True',
        'strng': 'string',
        'byts': b'\n\x14\x1e',
        'nested': {
            'req': 'req'
        },
        'enm': 'C',
        'f64': 9223372036854775807,
        'range': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        'enmRepeated': ['A', 'C']
    }
    deser = serialization_form.dict_to_protobuf(extend_pb2.MessageOfTypes, data)
    assert deser.dubl == 1.7e+308
    data1 = {'enm': 'MEOW'}
    with pytest.raises(RuntimeError):
        type_map = {1: int}
        serialization_form.dict_to_protobuf(extend_pb2.MessageOfTypes, data1, type_map)
    with pytest.raises(KeyError):
        data['enm'] = 'A'
        data['enmRepeated'] = ['CAT']
        serialization_form.dict_to_protobuf(extend_pb2.MessageOfTypes, data)


def test_serialize():
    """Test serialize"""
    req = helloworld_pb2.HelloRequest()
    serialization_form.FormSerializer.deserialize(b'msg=1', req)
    assert req.msg == "1"


def test_error_serialize():
    """Test error serialize"""
    with pytest.raises(ServerDecodeError):
        test_data = b'name=Alice&age=25'
        # curl -G "http://localhost:{PORT}/trpc.trpc_python_group.helloworld.Greeter1/SayHello" --data-urlencode
        # "name=Alice" --data-urlencode "age=25" -v
        serialization_form.FormSerializer.deserialize(test_data, helloworld_pb2.HelloRequest())
