# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Fill a protobuf message from a dict (form / query-string path).

Adapted from https://github.com/kaporzhu/protobuf-to-dict. Only the
dict-to-protobuf direction is kept: nested messages are skipped, and
repeated fields only accept scalar items. Extension fields travel
under :data:`EXTENSION_CONTAINER`.
"""

from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

import six
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message as PbMessage

from ._str_utils import strtobool

# Sentinel dict key under which extension field numbers travel.
EXTENSION_CONTAINER = "___X"

# Protobuf scalar field type to the callable that coerces a form/query string.
TYPE_CALLABLE_MAP = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_INT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_UINT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_SINT32: int,
    FieldDescriptor.TYPE_SINT64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_FIXED32: int,
    FieldDescriptor.TYPE_FIXED64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_SFIXED32: int,
    FieldDescriptor.TYPE_SFIXED64: int if six.PY3 else six.integer_types[1],
    FieldDescriptor.TYPE_BOOL: strtobool,
    FieldDescriptor.TYPE_STRING: six.text_type,
    FieldDescriptor.TYPE_BYTES: six.binary_type,
    FieldDescriptor.TYPE_ENUM: int,
}


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
        pb_msg = pb_class_or_instance
    else:
        pb_msg = pb_class_or_instance()
    type_callable_map = type_callable_map or TYPE_CALLABLE_MAP
    return _fill_message(pb_msg, values, type_callable_map, strict, ignore_none)


def _collect_field_entries(
        pb_msg: PbMessage,
        values: Dict[str, Any],
        strict: bool,
) -> List[Tuple[FieldDescriptor, Any, object]]:
    """Resolve dict keys to ``(descriptor, input_value, target)`` entries.

    ``target`` is the current attribute on ``pb_msg`` (the repeated
    container for repeated fields). Extension entries come after regular
    fields and are keyed by field number under ``EXTENSION_CONTAINER``.

    Args:
        pb_msg: Message being filled.
        values: Input dict.
        strict: Raise ``KeyError`` for unknown field names or extension
            numbers instead of skipping them.

    Returns:
        Entries in input order, regular fields first.
    """
    entries = []
    for field_name, input_value in values.items():
        if field_name == EXTENSION_CONTAINER:
            continue
        if field_name not in pb_msg.DESCRIPTOR.fields_by_name:
            if strict:
                raise KeyError(f"{type(pb_msg)} does not have a field called `{field_name}`")
            continue
        entries.append((pb_msg.DESCRIPTOR.fields_by_name[field_name], input_value, getattr(pb_msg, field_name, None)))

    for ext_number, ext_input in values.get(EXTENSION_CONTAINER, {}).items():
        try:
            ext_number = int(ext_number)
        except ValueError:
            raise ValueError("Extension keys must be integers.")
        if ext_number not in pb_msg._extensions_by_number:
            if strict:
                # ``field_name`` is the leftover loop variable from above, not ``ext_number``.
                raise KeyError(f"{type(pb_msg)}  does not have a extension with number `{field_name}`. "
                               "Perhaps you forgot to import it?")
            continue
        ext_field = pb_msg._extensions_by_number[ext_number]
        ext_target = pb_msg.Extensions[ext_field]
        entries.append((ext_field, ext_input, ext_target))

    return entries


def _fill_message(
        pb_msg: PbMessage,
        values: Dict[str, Any],
        type_callable_map: Dict[Any, Any],
        strict: bool,
        ignore_none: bool,
) -> PbMessage:
    """Write every resolved entry of ``values`` onto ``pb_msg``.

    Scalar fields given as a list take the first item. Singular message
    fields are skipped, and values that coerce to a falsy result are not
    written.

    Args:
        pb_msg: Message being filled in place.
        values: Input dict.
        type_callable_map: Protobuf field type to coercion callable.
        strict: Forwarded to key resolution and enum lookup.
        ignore_none: Skip entries whose input value is falsy.

    Returns:
        ``pb_msg`` itself.

    Raises:
        RuntimeError: ``setattr`` rejected the coerced value.
    """
    entries = _collect_field_entries(pb_msg, values, strict)

    for field, input_value, target in entries:
        if ignore_none and not input_value:
            continue

        if field.label == FieldDescriptor.LABEL_REPEATED:
            _extend_repeated_field(field, input_value, target, type_callable_map, strict)
            continue

        if isinstance(input_value, list):
            input_value = input_value[0]
        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            continue

        if field.type in type_callable_map:
            input_value = _coerce_scalar(field, input_value, type_callable_map, strict)
            if not input_value:
                continue

        if field.is_extension:
            pb_msg.Extensions[field] = input_value
            continue

        try:
            setattr(pb_msg, field.name, input_value)
        except Exception as exc:  # pylint: disable=broad-except
            raise RuntimeError(f"type: {type(pb_msg)}, field: `{field.name}`, value: `{values}`") from exc

    return pb_msg


def _extend_repeated_field(
        field: FieldDescriptor,
        items: Iterable,
        target: object,
        type_callable_map: Dict[Any, Any],
        strict: bool,
) -> None:
    """Append coerced scalar ``items`` to the repeated container ``target``.

    Map fields and message items are skipped. Enum names are resolved
    directly; other items that coerce to a falsy result are dropped.

    Args:
        field: Descriptor of the repeated field.
        items: Raw input values.
        target: The message's repeated container for ``field``.
        type_callable_map: Protobuf field type to coercion callable.
        strict: Forwarded to enum lookup.
    """
    if field.message_type and field.message_type.has_options and field.message_type.GetOptions().map_entry:
        return
    for item in items:
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            continue
        elif field.type == FieldDescriptor.TYPE_ENUM and isinstance(item, six.string_types):
            target.append(_enum_name_to_number(field, item, strict))
        else:
            item = _coerce_scalar(field, item, type_callable_map, strict)
            if not item:
                continue
            target.append(item)


def _coerce_scalar(
        field: FieldDescriptor,
        input_value: Any,
        type_callable_map: Dict[Any, Any],
        strict: bool,
) -> Optional[Any]:
    """Convert one raw value to the Python type expected by ``field``.

    Bytes fields encode the input as UTF-8 first; enum fields given as a
    string are resolved by name.

    Args:
        field: Target field descriptor.
        input_value: Raw value, usually a string from a form or query.
        type_callable_map: Protobuf field type to coercion callable.
        strict: Forwarded to enum lookup.

    Returns:
        The coerced value, or ``None`` when the field type has no
        coercion callable or any conversion error occurs.
    """
    try:
        if field.type in type_callable_map:
            if field.type == FieldDescriptor.TYPE_BYTES:
                return type_callable_map[field.type](input_value.encode("utf-8"))
            if field.type == FieldDescriptor.TYPE_ENUM and isinstance(input_value, six.string_types):
                return _enum_name_to_number(field, input_value, strict)
            return type_callable_map[field.type](input_value)
        else:
            return None
    except Exception:  # pylint: disable=broad-except
        return None


def _enum_name_to_number(field: FieldDescriptor, input_value: Any, strict: bool = False) -> Any:
    """Look up an enum value number by name.

    In non-strict mode a miss retries once with the upper-cased name in
    strict mode.

    Args:
        field: Enum field descriptor.
        input_value: Enum value name.
        strict: Raise on a miss instead of retrying upper-cased.

    Returns:
        The enum value number.

    Raises:
        KeyError: The name (or its upper-cased form) is not defined.
    """
    try:
        input_value = field.enum_type.values_by_name[input_value].number
    except KeyError:
        if strict:
            raise KeyError("`%s` is not a valid value for field `%s`" % (input_value, field.name))
        else:
            return _enum_name_to_number(field, input_value.upper(), strict=True)
    return input_value
