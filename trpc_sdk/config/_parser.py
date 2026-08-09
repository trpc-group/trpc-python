# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Config loading and dict-to-object deserializing.

:class:`TrpcConfigParser` reads raw bytes through a data provider plugin, decodes them
into a dict through a decoder plugin, and then assigns that dict onto typed
Python objects such as the dataclasses in :mod:`trpc_sdk.config._types`.

Config-file keys are matched to attributes by name, or by the
``:param attr: \\`config_key\\``` aliases declared in the target class
docstring. The declared type annotation drives the conversion, so nested dicts
and lists recurse into nested dataclasses.
"""

from __future__ import annotations

import os
import re
from asyncio import AbstractEventLoop
from functools import lru_cache
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from typing import get_type_hints

from trpc_sdk.abc import BaseConfig
from trpc_sdk.abc import BaseDataProvider
from trpc_sdk.abc import BaseDecoder
from trpc_sdk.plugin import DefaultPluginManager
from trpc_sdk.plugin import PluginType
from ._exceptions import DecodeError


class TrpcConfigParser(BaseConfig):
    """tRPC framework configuration parser.

    Loads config from a data source, decodes it, and parses it
    into typed Python objects.
    """

    def __init__(self, data_source: str, decoder: str, ds_options: dict, de_options: dict):
        super().__init__(data_source, decoder, ds_options, de_options)
        self.data_source: BaseDataProvider = DefaultPluginManager.get(PluginType.CONFIG_DATA_SOURCE, data_source)()
        self.decoder: BaseDecoder = DefaultPluginManager.get(PluginType.CONFIG_DECODER, decoder)()
        self.data_source.set_options(ds_options)
        self.decoder.set_options(de_options)
        self.config = None

    def load(self) -> dict:
        """Load and decode config file data(bytes) into a dictionary."""
        config_data = self.data_source.load(self.ds_options.get('file_path', None), self.ds_options)
        if config_data:
            config_data = os.path.expandvars(config_data)
        try:
            config_dict = self.decoder.decode(config_data, self.de_options)
        except Exception as exc:  # pylint: disable=broad-except
            raise DecodeError(self.decoder_name, str(exc)) from exc
        self.config = config_dict
        return config_dict

    def get(self, key: str, default: Optional[object] = None) -> object:
        """Get a config value by key."""
        return self.config.get(key, default)

    @classmethod
    def _resolve_attr_type(cls, target: object, attr_name: str) -> type:
        """Get the type annotation of a named attribute on an object.

        Resolves ``Optional[T]`` (i.e. ``Union[T, NoneType]``) to ``T``
        so that the caller always receives the concrete type.

        Args:
            target: The object to inspect.
            attr_name: The attribute name whose type to retrieve.

        Returns:
            The resolved type.
        """
        attr_type = get_type_hints(type(target).__init__).get(attr_name)
        # Handle the issue where a parameter defaulting to None causes the type annotation
        # to automatically become typing.Union[MyType, NoneType].
        if getattr(attr_type, '__origin__', None) == Union:
            attr_type = next(
                (t for t in attr_type.__args__ if t is not type(None)), attr_type
            )
        return attr_type

    @classmethod
    def _assign_dict_attr(
        cls, attr_type: type, target: object, attr_name: str, config_value: dict
    ) -> None:
        """Assign a dict config value to a typed attribute.

        If the attribute type is ``dict``, assigns directly. Otherwise,
        initialises the attribute (if absent or None) and recurses into it.

        Args:
            attr_type: The declared type of the attribute.
            target: The parent object owning the attribute.
            attr_name: The attribute name.
            config_value: The dict value to assign.
        """
        # Stop recursing when the user declared the attribute type as dict.
        if issubclass(attr_type, dict):
            config_value = attr_type(config_value)
            setattr(target, attr_name, config_value)
            return

        # Initialise the attribute when it is missing or None.
        if not hasattr(target, attr_name) or getattr(target, attr_name) is None:
            setattr(target, attr_name, attr_type())

        # Keep recursing into the dict subtree.
        cls.populate_from_dict(config_value, getattr(target, attr_name))

    @classmethod
    def _assign_list_attr(
        cls, attr_type: type, target: object, attr_name: str, config_value: List
    ) -> None:
        """Assign a list config value to a typed attribute.

        Args:
            attr_type: The declared type of the attribute (e.g. ``List[ServerService]``).
            target: The parent object owning the attribute.
            attr_name: The attribute name.
            config_value: The list value to assign.
        """
        type_args = getattr(attr_type, '__args__', None)
        if type_args is None:
            config_value = attr_type(config_value)
            setattr(target, attr_name, config_value)
        elif type_args[0] is str or type_args[0] is int:
            setattr(target, attr_name, config_value)
        else:
            for item in config_value:
                # if user specified the type is list, but the original attribute does not
                # exist or is not a list (e.g. None), it will reset the attribute to an
                # empty list, e.g. target = ServerConfig, attr_name = service
                if not hasattr(target, attr_name) or not isinstance(getattr(target, attr_name), list):
                    setattr(target, attr_name, [])
                if isinstance(item, dict):
                    # e.g. attr_type = List[ServerService], type_args[0] = ServerService
                    elem = getattr(attr_type, '__args__')[0]()
                    # e.g. target = ServerConfig, attr_name = service
                    getattr(target, attr_name).append(elem)
                    cls.populate_from_dict(item, elem)
                elif isinstance(item, list):
                    # Local import: avoids pulling the full log runtime into config import.
                    from trpc_sdk.log import warning
                    warning(
                        "Nested list in config key '%s' is not supported and will be skipped.",
                        attr_name,
                    )
                elif isinstance(item, str):
                    getattr(target, attr_name).append(item)

    @classmethod
    @lru_cache(maxsize=None)
    def _build_config_key_to_attr_map(cls, object_class: type) -> Dict[str, str]:
        """Build a mapping from config-file keys to class attribute names.

        Parses docstrings for ``:param attr_name: \\`config_key\\``` patterns
        to support renaming between YAML keys and Python attributes.

        Args:
            object_class: The class whose docstring to parse.

        Returns:
            A dict mapping config keys to attribute names.
        """
        attribute_map = {}
        # __init__ is the constructor of the class, __doc__ is the documentation string of dataclass.
        docstring = getattr(object_class, '__init__').__doc__ or getattr(object_class, '__doc__', None)

        if docstring is None:
            return attribute_map

        map_pattern = r'^\s*:param (.*):\s*`(.*)`.*$'
        lines = docstring.split('\n')
        for line in lines:
            match = re.match(map_pattern, line)
            if match is not None:
                attr_name, config_key = match.groups()
                attribute_map[config_key] = attr_name
        return attribute_map

    @classmethod
    def populate_from_dict(cls, source: Dict, target: object) -> None:
        """Populate target object attributes from a config dictionary.

        Args:
            source: Parsed YAML dictionary (or a subtree of it).
            target: Config object (or a subtree of it).
        """
        # Make a mapping from config-file keys to class attribute names.
        config_key_to_attr_map = cls._build_config_key_to_attr_map(type(target))
        for config_key, config_value in source.items():
            # if the config-file key is in the mapping dictionary, use the attribute name in the mapping dictionary,
            # otherwise use the config-file key
            attr_name = config_key_to_attr_map.get(config_key, config_key)
            # if the target object does not have the attribute, skip
            if not hasattr(target, attr_name):
                continue
            # Get the concrete type of the attribute.
            attr_type = cls._resolve_attr_type(target, attr_name)
            if isinstance(config_value, dict):
                cls._assign_dict_attr(attr_type, target, attr_name, config_value)
            elif isinstance(config_value, list):
                cls._assign_list_attr(attr_type, target, attr_name, config_value)
            else:
                if config_value is None:
                    attr_type = getattr(attr_type, '__origin__', attr_type)
                    config_value = attr_type()
                else:
                    config_value = attr_type(config_value)
                setattr(target, attr_name, config_value)

    @classmethod
    def _parse(cls, source: Dict, target: object) -> None:
        """Parse a config dictionary into typed objects following a
        structure specification.

        When ``target`` is not a dict, it is treated as a single
        top-level object to parse into.

        Args:
            source: The parsed config dictionary (or subtree of it) from yaml file.
            target: A dict or object specifying the target structure (or subtree of it) from trpc_sdk config.
        """
        # if target is a dict, it means to get a part of the config, otherwise it means to get the whole config
        # e.g. target = {"client": ClientConfig}
        if not isinstance(target, dict):
            root_key = target.__class__.__name__
            source = {root_key: source}
            target = {root_key: target}

        for key, sub_target in target.items():
            if source is None or key not in source:
                continue
            sub_source = source[key]
            if isinstance(sub_target, dict):
                cls._parse(sub_source, sub_target)
            elif isinstance(sub_target, list):
                if sub_source is None:
                    continue
                for index, item in enumerate(sub_target):
                    cls._parse(sub_source[index], item)
            else:
                cls.populate_from_dict(sub_source, sub_target)

    def parse(self, target: object) -> None:
        """Parse the loaded config into the given structure."""
        TrpcConfigParser._parse(self.config, target)

    def hot_reload(self, callback: Callable, interval: int, event_loop: AbstractEventLoop) -> None:
        """Hot-reload config source changes and trigger a callback."""
        key = self.ds_options.get("file_path")
        self.data_source.hot_reload(
            key, callback, {"interval": interval, "event_loop": event_loop}
        )
