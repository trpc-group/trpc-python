# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test codec module"""

import json

import pytest

# Import protobuf messages for testing
from tests.helloworld_pb2 import HelloReply
from tests.helloworld_pb2 import HelloRequest

# Import JsonSerializer and JsonData for testing
from trpc_sdk.abc import BaseSerializer
from trpc_sdk.serialization import JsonData
from trpc_sdk.serialization import JsonSerializer
from trpc_sdk.serialization import json_to_pb_kwargs
from trpc_sdk.serialization import pb_to_json_kwargs
from trpc_sdk.serialization._serialization import _PROTOBUF_VERSION
from trpc_sdk.serialization._serialization import _USE_NEW_PARAM


def register_pb_json_format_kwargs(pb_kwargs=None, json_kwargs=None):
    """Update the serializer kwargs dicts in place.

    An empty mapping clears that side so JsonSerializer falls back to its
    protobuf-version defaults. ``None`` leaves that side unchanged.
    """
    if pb_kwargs is not None:
        pb_to_json_kwargs.clear()
        pb_to_json_kwargs.update(pb_kwargs)
    if json_kwargs is not None:
        json_to_pb_kwargs.clear()
        json_to_pb_kwargs.update(json_kwargs)


# JsonSerializer tests
def test_json_data_creation():
    """Test JsonData creation with default and custom data"""
    # Test default empty dict
    json_data = JsonData()
    assert json_data.data == {}

    # Test with custom data
    custom_data = {"key": "value", "number": 123}
    json_data = JsonData(data=custom_data)
    assert json_data.data == custom_data


def test_json_serializer_creation():
    """Test JsonSerializer instance creation"""
    serializer = JsonSerializer()
    assert isinstance(serializer, JsonSerializer)


def test_json_serialize_deserialize():
    """Test JsonData serialization and deserialization"""
    serializer = JsonSerializer()

    # Test with simple data
    original_data = {"name": "test", "value": 42}
    json_data = JsonData(data=original_data)

    # Serialize
    serialized = serializer.serialize(json_data)
    assert isinstance(serialized, bytes)

    # Deserialize
    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)
    assert new_json_data.data == original_data


def test_json_serialize_empty_data():
    """Test serialization of empty JsonData"""
    serializer = JsonSerializer()
    json_data = JsonData()  # Empty data

    serialized = serializer.serialize(json_data)
    assert isinstance(serialized, bytes)

    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)
    assert new_json_data.data == {}


def test_json_serialize_nested_data():
    """Test serialization of nested data structures"""
    serializer = JsonSerializer()

    nested_data = {
        "user": {
            "name": "Alice",
            "age": 30,
            "address": {
                "city": "Beijing",
                "country": "China"
            }
        },
        "scores": [95, 87, 92]
    }

    json_data = JsonData(data=nested_data)
    serialized = serializer.serialize(json_data)

    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)
    assert new_json_data.data == nested_data


def test_json_serialize_special_characters():
    """Test serialization with special characters"""
    serializer = JsonSerializer()

    special_data = {"message": "Hello, 世界!", "unicode": "🎉🎊", "special_chars": "!@#$%^&*()"}

    json_data = JsonData(data=special_data)
    serialized = serializer.serialize(json_data)

    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)
    assert new_json_data.data == special_data


def test_json_serializer_methods():
    """Test individual JsonSerializer methods"""
    serializer = JsonSerializer()

    # Test _json_serialize method
    json_data = JsonData(data={"test": "value"})
    serialized = serializer._json_serialize(json_data)
    assert isinstance(serialized, bytes)

    # Test _json_deserialize method
    new_json_data = JsonData()
    serializer._json_deserialize(serialized, new_json_data)
    assert new_json_data.data == {"test": "value"}


def test_json_serializer_with_invalid_data():
    """Test JsonSerializer with invalid data types"""
    serializer = JsonSerializer()

    # Test with non-JsonData object in serialize
    with pytest.raises(Exception):
        serializer.serialize("invalid_data")

    # Test with non-JsonData object in deserialize
    with pytest.raises(Exception):
        serializer.deserialize(b'{"test": "value"}', "invalid_target")


def test_json_serializer_edge_cases():
    """Test edge cases for JsonSerializer"""
    serializer = JsonSerializer()

    # Test with very large data
    large_data = {"key" + str(i): "value" + str(i) for i in range(1000)}
    json_data = JsonData(data=large_data)

    serialized = serializer.serialize(json_data)
    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)
    assert new_json_data.data == large_data

    # Test with empty bytes
    json_data = JsonData()
    with pytest.raises(Exception):
        serializer.deserialize(b'', json_data)

    serializer.deserialize(serialized, new_json_data)
    # assert new_json_data.data == original_data


# Protobuf tests
def test_protobuf_message_creation():
    """Test protobuf message creation"""
    # Test HelloRequest creation
    request = HelloRequest()
    request.msg = "Hello World"
    request.msg_list.append(b"message1")
    request.msg_list.append(b"message2")

    assert request.msg == "Hello World"
    assert len(request.msg_list) == 2
    assert request.msg_list[0] == b"message1"
    assert request.msg_list[1] == b"message2"

    # Test HelloReply creation
    reply = HelloReply()
    reply.msg = "Hello Response"
    reply.msg_list.append(b"response1")

    assert reply.msg == "Hello Response"
    assert len(reply.msg_list) == 1


def test_protobuf_serialize_deserialize():
    """Test protobuf message serialization and deserialization"""
    serializer = JsonSerializer()

    # Create and populate HelloRequest
    original_request = HelloRequest()
    original_request.msg = "Test Message"
    original_request.msg_list.append(b"data1")
    original_request.msg_list.append(b"data2")

    # Serialize protobuf message
    serialized = serializer.serialize(original_request)
    assert isinstance(serialized, bytes)

    # Deserialize into new protobuf message
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)

    # Verify data integrity
    assert new_request.msg == original_request.msg
    assert len(new_request.msg_list) == len(original_request.msg_list)
    assert new_request.msg_list[0] == original_request.msg_list[0]
    assert new_request.msg_list[1] == original_request.msg_list[1]


def test_protobuf_serialize_empty_message():
    """Test serialization of empty protobuf message"""
    serializer = JsonSerializer()

    # Test with empty HelloRequest
    empty_request = HelloRequest()
    serialized = serializer.serialize(empty_request)
    assert isinstance(serialized, bytes)

    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)

    # Empty message should have default values
    assert new_request.msg == ""  # Default empty string
    assert len(new_request.msg_list) == 0


def test_protobuf_serializer_methods():
    """Test individual protobuf serializer methods"""
    serializer = JsonSerializer()

    # Test _pb_serialize method
    request = HelloRequest()
    request.msg = "Test Message"
    serialized = serializer._pb_serialize(request)
    assert isinstance(serialized, bytes)

    # Test _pb_deserialize method
    new_request = HelloRequest()
    serializer._pb_deserialize(serialized, new_request)
    assert new_request.msg == "Test Message"


def test_protobuf_with_special_characters():
    """Test protobuf serialization with special characters"""
    serializer = JsonSerializer()

    request = HelloRequest()
    request.msg = "Hello, 世界! 🎉🎊"
    request.msg_list.append("特殊字符!@#$%".encode('utf-8'))

    serialized = serializer.serialize(request)
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)

    assert new_request.msg == "Hello, 世界! 🎉🎊"
    assert new_request.msg_list[0] == "特殊字符!@#$%".encode('utf-8')


def test_protobuf_repeated_fields():
    """Test protobuf serialization with repeated fields"""
    serializer = JsonSerializer()

    request = HelloRequest()
    request.msg = "Multiple messages"

    # Add multiple repeated fields
    for i in range(5):
        request.msg_list.append(f"message_{i}".encode('utf-8'))

    serialized = serializer.serialize(request)
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)

    assert new_request.msg == "Multiple messages"
    assert len(new_request.msg_list) == 5
    for i in range(5):
        assert new_request.msg_list[i] == f"message_{i}".encode('utf-8')


def test_protobuf_different_message_types():
    """Test serialization with different protobuf message types"""
    serializer = JsonSerializer()

    # Test HelloRequest
    request = HelloRequest()
    request.msg = "Request Message"
    request.msg_list.append(b"request_data")

    request_serialized = serializer.serialize(request)
    request_deserialized = HelloRequest()
    serializer.deserialize(request_serialized, request_deserialized)
    assert request_deserialized.msg == "Request Message"

    # Test HelloReply
    reply = HelloReply()
    reply.msg = "Reply Message"
    reply.msg_list.append(b"reply_data")

    reply_serialized = serializer.serialize(reply)
    reply_deserialized = HelloReply()
    serializer.deserialize(reply_serialized, reply_deserialized)
    assert reply_deserialized.msg == "Reply Message"


def test_protobuf_serializer_with_invalid_data():
    """Test protobuf serializer with invalid data types"""
    serializer = JsonSerializer()

    # Test with non-protobuf object in serialize
    with pytest.raises(Exception):
        serializer.serialize("invalid_protobuf")

    # Test with non-protobuf object in deserialize
    with pytest.raises(Exception):
        serializer.deserialize(b'{"msg": "test"}', "invalid_target")

    # Test with mismatched protobuf types - create incompatible data
    # HelloRequest and HelloReply have identical field structures, so they are compatible
    # Instead, test with data that has incompatible structure
    request = HelloRequest()
    request.msg = "Test"
    request.msg_list.append(b"data1")

    # Create JSON data that has incompatible structure for HelloReply
    incompatible_data = json.dumps({
        "incompatible_field": "this_field_does_not_exist_in_HelloReply",
        "msg": "test"
    }).encode('utf-8')

    # Try to deserialize incompatible data into HelloReply
    reply = HelloReply()
    # This should succeed because ignore_unknown_fields=True allows unknown fields
    serializer.deserialize(incompatible_data, reply)
    assert reply.msg == "test"  # Known field should be populated

    # Test with completely invalid protobuf data
    invalid_protobuf_data = b'invalid_protobuf_format'
    with pytest.raises(Exception):
        serializer.deserialize(invalid_protobuf_data, reply)


def test_serializer_type_detection():
    """Test serializer's automatic type detection"""
    serializer = JsonSerializer()

    # Test JsonData detection
    json_data = JsonData(data={"test": "value"})
    assert isinstance(json_data, JsonData)

    # Test protobuf detection
    request = HelloRequest()
    request.msg = "Test"

    # Serialize the request first
    serialized = serializer.serialize(request)
    assert isinstance(serialized, bytes)

    # Test deserialization into JsonData (should work due to ignore_unknown_fields)
    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)
    # Since we're deserializing protobuf data into JsonData, it should contain the JSON representation
    assert "msg" in new_json_data.data


# Test register_pb_json_format_kwargs function
def test_register_pb_map_json_kwargs_with_pb_kwargs():
    """Test register_pb_json_format_kwargs with pb_kwargs"""
    # Register custom pb_kwargs
    custom_pb_kwargs = {"preserving_proto_field_name": False, "use_integers_for_enums": False}
    register_pb_json_format_kwargs(pb_kwargs=custom_pb_kwargs)

    serializer = JsonSerializer()
    request = HelloRequest()
    request.msg = "Test with custom kwargs"

    # Serialize with custom kwargs
    serialized = serializer.serialize(request)
    assert isinstance(serialized, bytes)

    # Reset to default by registering empty dict
    register_pb_json_format_kwargs(pb_kwargs={})


def test_register_pb_map_json_kwargs_with_json_kwargs():
    """Test register_pb_json_format_kwargs with json_kwargs"""
    # Register custom json_kwargs
    custom_json_kwargs = {"ignore_unknown_fields": False}
    register_pb_json_format_kwargs(json_kwargs=custom_json_kwargs)

    serializer = JsonSerializer()
    request = HelloRequest()
    request.msg = "Test"

    serialized = serializer.serialize(request)

    # Deserialize with custom kwargs
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)
    assert new_request.msg == "Test"

    # Reset to default
    register_pb_json_format_kwargs(json_kwargs={})


def test_register_pb_map_json_kwargs_with_both():
    """Test register_pb_json_format_kwargs with both pb_kwargs and json_kwargs"""
    custom_pb_kwargs = {"use_integers_for_enums": True}
    custom_json_kwargs = {"ignore_unknown_fields": True}

    register_pb_json_format_kwargs(pb_kwargs=custom_pb_kwargs, json_kwargs=custom_json_kwargs)

    serializer = JsonSerializer()
    request = HelloRequest()
    request.msg = "Test both kwargs"

    serialized = serializer.serialize(request)
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)
    assert new_request.msg == "Test both kwargs"

    # Reset to default
    register_pb_json_format_kwargs(pb_kwargs={}, json_kwargs={})


def test_register_pb_map_json_kwargs_with_none():
    """Test register_pb_json_format_kwargs with None parameters"""
    # Should not raise any exception
    register_pb_json_format_kwargs(pb_kwargs=None, json_kwargs=None)

    serializer = JsonSerializer()
    request = HelloRequest()
    request.msg = "Test with None"

    serialized = serializer.serialize(request)
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)
    assert new_request.msg == "Test with None"


# Test protobuf version compatibility
def test_protobuf_version_detection():
    """Test protobuf version detection"""
    # Check that version is detected correctly
    assert isinstance(_PROTOBUF_VERSION, tuple)
    assert len(_PROTOBUF_VERSION) >= 2
    assert all(isinstance(v, int) for v in _PROTOBUF_VERSION)

    # Check that _USE_NEW_PARAM is a boolean
    assert isinstance(_USE_NEW_PARAM, bool)

    # Verify version logic
    if _PROTOBUF_VERSION >= (5, 26):
        assert _USE_NEW_PARAM is True
    else:
        assert _USE_NEW_PARAM is False


def test_protobuf_serialization_with_version_compatibility():
    """Test protobuf serialization works correctly regardless of version"""
    serializer = JsonSerializer()

    # Create a message with all field types
    request = HelloRequest()
    request.msg = "Version compatibility test"
    request.msg_list.append(b"data1")
    request.msg_list.append(b"data2")

    # Serialize
    serialized = serializer.serialize(request)
    assert isinstance(serialized, bytes)

    # Deserialize
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)

    # Verify data integrity
    assert new_request.msg == request.msg
    assert len(new_request.msg_list) == len(request.msg_list)
    assert new_request.msg_list[0] == request.msg_list[0]
    assert new_request.msg_list[1] == request.msg_list[1]


def test_protobuf_default_values_included():
    """Test that default values are included in serialization for scalar fields

    Note: The JSON serializer asks protobuf to print fields with no presence,
    so an empty repeated field is emitted as an empty list.
    """
    serializer = JsonSerializer()

    # Create message with only some fields set
    request = HelloRequest()
    request.msg = "Test"
    # msg_list is left empty (default value for repeated field)

    serialized = serializer.serialize(request)
    json_str = serialized.decode('utf-8')
    json_obj = json.loads(json_str)

    # Check that msg field is present (scalar field with value)
    assert "msg" in json_obj
    assert json_obj["msg"] == "Test"

    # Default-value printing includes an empty repeated field as [].
    repeated = json_obj.get("msg_list", json_obj.get("msgList"))
    assert repeated == []

    # Test with non-empty repeated field
    request2 = HelloRequest()
    request2.msg = "Test2"
    request2.msg_list.append(b"data1")

    serialized2 = serializer.serialize(request2)
    json_str2 = serialized2.decode('utf-8')
    json_obj2 = json.loads(json_str2)

    # Now msg_list should be present because it's not empty
    assert "msg_list" in json_obj2 or "msgList" in json_obj2


def test_pb_serialize_with_custom_kwargs():
    """Test _pb_serialize with custom kwargs registered"""
    # Register custom kwargs that will be used
    custom_kwargs = {"preserving_proto_field_name": True, "use_integers_for_enums": True}
    register_pb_json_format_kwargs(pb_kwargs=custom_kwargs)

    serializer = JsonSerializer()
    request = HelloRequest()
    request.msg = "Custom kwargs test"

    serialized = serializer._pb_serialize(request)
    assert isinstance(serialized, bytes)

    # Reset
    register_pb_json_format_kwargs(pb_kwargs={})


def test_pb_deserialize_with_custom_kwargs():
    """Test _pb_deserialize with custom kwargs registered"""
    serializer = JsonSerializer()

    # First serialize a message
    request = HelloRequest()
    request.msg = "Deserialize test"
    serialized = serializer.serialize(request)

    # Register custom json_kwargs
    custom_kwargs = {"ignore_unknown_fields": True}
    register_pb_json_format_kwargs(json_kwargs=custom_kwargs)

    # Deserialize with custom kwargs
    new_request = HelloRequest()
    serializer._pb_deserialize(serialized, new_request)
    assert new_request.msg == "Deserialize test"

    # Reset
    register_pb_json_format_kwargs(json_kwargs={})


def test_json_data_field_default_factory():
    """Test JsonData field default factory"""
    # Create multiple JsonData instances
    json_data1 = JsonData()
    json_data2 = JsonData()

    # Modify one instance
    json_data1.data["key"] = "value"

    # Verify they are independent (default_factory creates new dict for each instance)
    assert "key" in json_data1.data
    assert "key" not in json_data2.data
    assert json_data1.data != json_data2.data


def test_serializer_inheritance():
    """Test that JsonSerializer inherits from BaseSerializer"""
    serializer = JsonSerializer()
    assert isinstance(serializer, BaseSerializer)


def test_protobuf_with_bytes_data():
    """Test protobuf serialization with various bytes data"""
    serializer = JsonSerializer()

    request = HelloRequest()
    request.msg = "Bytes test"

    # Add various bytes data
    request.msg_list.append(b"")  # Empty bytes
    request.msg_list.append(b"\x00\x01\x02")  # Binary data
    request.msg_list.append(b"Normal text")  # Normal text as bytes
    request.msg_list.append("Unicode 中文".encode('utf-8'))  # Unicode as bytes

    serialized = serializer.serialize(request)
    new_request = HelloRequest()
    serializer.deserialize(serialized, new_request)

    assert new_request.msg == "Bytes test"
    assert len(new_request.msg_list) == 4
    assert new_request.msg_list[0] == b""
    assert new_request.msg_list[1] == b"\x00\x01\x02"
    assert new_request.msg_list[2] == b"Normal text"
    assert new_request.msg_list[3] == "Unicode 中文".encode('utf-8')


def test_json_serialize_with_none_values():
    """Test JsonData serialization with None values"""
    serializer = JsonSerializer()

    json_data = JsonData(data={"key1": None, "key2": "value", "key3": None})
    serialized = serializer.serialize(json_data)

    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)

    assert new_json_data.data["key1"] is None
    assert new_json_data.data["key2"] == "value"
    assert new_json_data.data["key3"] is None


def test_json_serialize_with_boolean_values():
    """Test JsonData serialization with boolean values"""
    serializer = JsonSerializer()

    json_data = JsonData(data={"true_val": True, "false_val": False, "mixed": [True, False, True]})
    serialized = serializer.serialize(json_data)

    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)

    assert new_json_data.data["true_val"] is True
    assert new_json_data.data["false_val"] is False
    assert new_json_data.data["mixed"] == [True, False, True]


def test_json_serialize_with_numeric_types():
    """Test JsonData serialization with various numeric types"""
    serializer = JsonSerializer()

    json_data = JsonData(data={
        "int": 42,
        "float": 3.14,
        "negative": -100,
        "zero": 0,
        "large": 999999999999,
        "scientific": 1.23e-4
    })
    serialized = serializer.serialize(json_data)

    new_json_data = JsonData()
    serializer.deserialize(serialized, new_json_data)

    assert new_json_data.data["int"] == 42
    assert new_json_data.data["float"] == 3.14
    assert new_json_data.data["negative"] == -100
    assert new_json_data.data["zero"] == 0
    assert new_json_data.data["large"] == 999999999999
    assert abs(new_json_data.data["scientific"] - 1.23e-4) < 1e-10
