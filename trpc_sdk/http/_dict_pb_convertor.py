# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Fill a protobuf message from a dict (form / query-string path).

Adapted from https://github.com/kaporzhu/protobuf-to-dict. Only the
dict-to-protobuf direction is kept: nested messages are skipped, and
repeated fields only accept scalar items. Extension fields travel
under :data:`~trpc_sdk.http.EXTENSION_CONTAINER`.
"""

from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Tuple

import six
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message as PbMessage

from ._constants import EXTENSION_CONTAINER
from ._types import TYPE_CALLABLE_MAP


def dict_to_protobuf(
        pb_class_or_instance: PbMessage,
        values: Dict[str, Any],
        type_callable_map: Optional[Dict[Any, Any]] = None,
        strict: bool = True,
        ignore_none: bool = False,
) -> PbMessage:
    """Populate a protobuf message from a dictionary.

    Repeated and scalar fields are supported; nested messages are
    ignored. Unknown keys raise ``KeyError`` when ``strict`` is true.

    Args:
        pb_class_or_instance: Protobuf class or already-built instance.
        values: Field name to value. Repeated and nested values are
            fully supported at the dict layer.
        type_callable_map: Mapping of protobuf types to callables used
            to coerce values. Defaults to :data:`TYPE_CALLABLE_MAP`.
        strict: Raise if a key is not a field on the message.
        ignore_none: Skip fields whose value is falsy.

    Returns:
        The filled protobuf instance.
    """
    if isinstance(pb_class_or_instance, PbMessage):
        instance = pb_class_or_instance
    else:
        instance = pb_class_or_instance()
    type_callable_map = type_callable_map or TYPE_CALLABLE_MAP
    return _dict_to_protobuf(instance, values, type_callable_map, strict, ignore_none)


def _get_field_mapping(
        pb: PbMessage,
        values: Dict[str, Any],
        strict: bool,
) -> List[Tuple[FieldDescriptor, Any, object]]:
    field_mapping = []
    for key, value in values.items():
        if key == EXTENSION_CONTAINER:
            continue
        if key not in pb.DESCRIPTOR.fields_by_name:
            if strict:
                raise KeyError(f"{type(pb)} does not have a field called `{key}`")
            continue
        field_mapping.append((pb.DESCRIPTOR.fields_by_name[key], value, getattr(pb, key, None)))

    for ext_num, ext_val in values.get(EXTENSION_CONTAINER, {}).items():
        try:
            ext_num = int(ext_num)
        except ValueError:
            raise ValueError("Extension keys must be integers.")
        if ext_num not in pb._extensions_by_number:
            if strict:
                raise KeyError(
                    f"{type(pb)}  does not have a extension with number `{key}`. Perhaps you forgot to import it?")
            continue
        ext_field = pb._extensions_by_number[ext_num]
        pb_val = pb.Extensions[ext_field]
        field_mapping.append((ext_field, ext_val, pb_val))

    return field_mapping


def _dict_to_protobuf(
        pb: PbMessage,
        values: Dict[str, Any],
        type_callable_map: Dict[Any, Any],
        strict: bool,
        ignore_none: bool,
) -> PbMessage:
    fields = _get_field_mapping(pb, values, strict)

    for field, input_value, pb_value in fields:
        if ignore_none and not input_value:
            continue

        if field.label == FieldDescriptor.LABEL_REPEATED:
            _handle_repeated_field(field, input_value, pb_value, type_callable_map, strict)
            continue

        if isinstance(input_value, list):
            input_value = input_value[0]
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            continue

        if field.type in type_callable_map:
            input_value = _string_to_pbvalue(field, input_value, type_callable_map, strict)
            if not input_value:
                continue

        if field.is_extension:
            pb.Extensions[field] = input_value
            continue

        try:
            setattr(pb, field.name, input_value)
        except Exception as exc:  # pylint: disable=broad-except
            raise RuntimeError(f"type: {type(pb)}, field: `{field.name}`, value: `{values}`") from exc

    return pb


def _handle_repeated_field(
        field: FieldDescriptor,
        input_values: Iterable,
        pb_value: object,
        type_callable_map: Dict[Any, Any],
        strict: bool,
) -> NoReturn:
    if field.message_type and field.message_type.has_options and field.message_type.GetOptions().map_entry:
        return
    for item in input_values:
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            continue
        elif field.type == FieldDescriptor.TYPE_ENUM and isinstance(item, six.string_types):
            pb_value.append(_string_to_enum(field, item, strict))
        else:
            item = _string_to_pbvalue(field, item, type_callable_map, strict)
            if not item:
                continue
            pb_value.append(item)


def _string_to_pbvalue(
        field: FieldDescriptor,
        input_value: Any,
        type_callable_map: Dict[Any, Any],
        strict: bool,
) -> Optional[Any]:
    try:
        if field.type in type_callable_map:
            if field.type == FieldDescriptor.TYPE_BYTES:
                return type_callable_map[field.type](input_value.encode("utf-8"))
            if field.type == FieldDescriptor.TYPE_ENUM and isinstance(input_value, six.string_types):
                return _string_to_enum(field, input_value, strict)
            return type_callable_map[field.type](input_value)
        else:
            return None
    except Exception:  # pylint: disable=broad-except
        return None


def _string_to_enum(field: FieldDescriptor, input_value: Any, strict: bool = False) -> Any:
    try:
        input_value = field.enum_type.values_by_name[input_value].number
    except KeyError:
        if strict:
            raise KeyError("`%s` is not a valid value for field `%s`" % (input_value, field.name))
        else:
            return _string_to_enum(field, input_value.upper(), strict=True)
    return input_value
