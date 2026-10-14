# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Plugin abstract base class"""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from enum import IntEnum
from enum import unique


@unique
class PluginInitPos(IntEnum):
    """Process stage that eagerly initializes a plugin.

    A position is only a hint about *when* the plugin manager builds the
    instance up front. Whatever is not covered by an eager pass is still
    built on demand the first time ``get_plugin`` asks for it.

    Attributes:
        UNKNOWN: Not declared. Instances are only built lazily.
        WORKER: Initialized by ``init_worker_plugins()``, which runs in
            processes that execute user handlers.
        TASK: Initialized by ``init_task_plugins()``. Shared by task
            children and the dispatcher, neither of which runs user
            handlers, so user-defined plugins are skipped there.
        TIMER: Reserved for the timer child. Nothing registers or
            initializes this position today.
    """

    UNKNOWN = 0
    WORKER = 1
    TASK = 2
    TIMER = 3


@unique
class PluginType(IntEnum):
    """Plugin type"""
    UNKNOWN = 0

    SERVER_TRANSPORT = 1

    SERVER_CODEC = 2

    SERVER_PARSER = 3

    CLIENT_TRANSPORT = 4

    CLIENT_CODEC = 5

    CLIENT_PARSER = 6

    CONFIG = 7

    CONFIG_PROVIDER = 8

    CONFIG_DECODER = 9

    SERIALIZATION = 10

    COMPRESS = 11

    SERVER_FILTER_COMPONENT = 12

    CLIENT_FILTER_COMPONENT = 13

    # plugin - log
    LOG = 50

    LOG_LISTENER = 51

    LOG_WRITER = 52

    LOG_COMPONENT = 53


    # plugin - trace
    TRACE = 60

    # plugin - naming
    NAMING = 70

    NAMING_REGISTRY = 71

    NAMING_SELECTOR = 72

    NAMING_LOADBALANCE = 73

    NAMING_DISCOVERY = 74

    NAMING_CIRCUIT_BREAKER = 75

    NAMING_SERVICE_ROUTER = 76

    # plugin - metrics
    METRICS = 80

    # plugin - admin
    ADMIN = 90

    # plugin - task
    TASK = 100

    # plugin - user-defined
    USER_DEFINED = 1000


class BasePlugin(ABC):
    """Plugin abstract base class"""

    def __init__(self):
        """Initialize plugin"""
        self._name: str = ""
        self._version: str = ""
        self._type: PluginType = PluginType.UNKNOWN
        self._init_pos: PluginInitPos = PluginInitPos.UNKNOWN
    
    @property
    def name(self) -> str:
        """Get plugin name"""
        return self._name
    
    @property
    def version(self) -> str:
        """Get plugin version"""
        return self._version
    
    @property
    def type(self) -> PluginType:
        """Get plugin type"""
        return self._type
    
    @property
    def type_name(self) -> str:
        """Get plugin type name"""
        return self._type.name

    @property
    def init_pos(self) -> PluginInitPos:
        """Get plugin init position"""
        return self._init_pos
    
    def init(self):
        """Initialize plugin"""
        return

    def start(self):
        """Start plugin"""
        return
    
    def stop(self):
        """Stop plugin"""
        return


class BasePluginFactory(BasePlugin):
    """Plugin that hands out per-use instances instead of being one.

    Some plugin slots cannot be filled by a shared singleton: a server
    transport is bound to one listening socket, and a protocol parser
    buffers bytes across reads so it belongs to a single connection. The
    factory itself is what the plugin manager stores and initializes;
    callers ask it for a fresh object through :meth:`create`.
    """

    @abstractmethod
    def create(self):
        """Return a new instance for the caller to own.

        Returns:
            A newly built object of whatever type this slot provides.
        """
