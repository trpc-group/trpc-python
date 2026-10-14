# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import pytest

from trpc_sdk.exceptions import PluginInitError
from trpc_sdk.plugin import BasePlugin
from trpc_sdk.plugin import PluginCls
from trpc_sdk.plugin import PluginInitPos
from trpc_sdk.plugin import PluginManager
from trpc_sdk.plugin import PluginType
from trpc_sdk.plugin import check_new_trpc_naming_polaris
from trpc_sdk.plugin import register_plugin as frame_register_plugin
from trpc_sdk.plugin import set_plugin_order
from trpc_sdk.plugin import setup

DummyPluginManager = PluginManager()


class DummyPlugin:
    """A dummy plugin class for testing purposes."""
    pass


class EmptyPlugin(BasePlugin):
    """A dummy plugin class for testing purposes."""

    def __init__(self):
        self.master_start = False
        self.worker_start = False

    def on_master_start(self):
        self.master_start = True

    def on_worker_start(self):
        self.worker_start = True


def register_plugin(cls, name, ptype=PluginType.USER_DEFINED, version=None, pos=PluginInitPos.WORKER, flag=False):
    plugin_spec = PluginCls(cls, pos, ptype, name, version, flag)
    DummyPluginManager.register(ptype, name, plugin_spec)
    return plugin_spec


def test_plugin_type():
    """Test the functionality of PluginType enum values and names."""
    plugin_type = PluginType.USER_DEFINED
    assert plugin_type.value == 1000
    assert plugin_type.name == "USER_DEFINED"


def test_plugin_register():
    """Test the registration of a plugin with PluginManager."""
    plugin_spec = register_plugin(DummyPlugin, "runtime_test", version="0.0.1")

    plugin_spec_obj = DummyPluginManager.plugin_container[PluginType.USER_DEFINED]["runtime_test"]
    assert isinstance(plugin_spec_obj, PluginCls)

    assert plugin_spec_obj.cls == DummyPlugin
    assert plugin_spec_obj.version == plugin_spec.version
    assert not plugin_spec_obj.flag


def test_plugin_specs():
    """Test the retrieval of all plugin specifications from PluginManager."""
    plugin_spec = register_plugin(DummyPlugin, "runtime_test", version="0.0.1", flag=True)

    specs = DummyPluginManager.all_plugin_specs()
    assert len(specs) == 1
    assert specs[0] == plugin_spec


def test_default_plugins():
    """Test the functionality of DefaultPluginManager and its plugin specifications."""
    from trpc_sdk.plugin import DefaultPluginManager

    plugins = DefaultPluginManager.all_plugin_specs()

    for p in plugins:
        print(type(p.plugin_name), p.plugin_name, p.version)

    assert len(plugins) > 0


def test_plugin_instance():
    """Test the instantiation of plugin classes and their inheritance from BasePlugin."""
    plugin_spec = register_plugin(DummyPlugin, "dummy_plugin")
    instance = plugin_spec.cls()

    assert isinstance(instance, DummyPlugin)
    assert not isinstance(instance, BasePlugin)

    plugin_spec = register_plugin(EmptyPlugin, "base_plugin_test")
    instance = plugin_spec.cls()

    assert isinstance(instance, EmptyPlugin)
    assert isinstance(instance, BasePlugin)


class DummyInvalidPlugin:

    def __init__(self) -> None:
        raise PluginInitError("test")


def test_plugin_init_err():
    """Test the handling of plugin initialization errors."""
    plugin_spec = register_plugin(DummyInvalidPlugin, "dummy_invalid_plugin", pos=PluginInitPos.MASTER)
    with pytest.raises(PluginInitError):
        plugin_spec.cls()


def test_check_new_trpc_naming_polaris():
    """Test the version check functionality for trpc_naming_polaris module."""
    import sys

    class _MockTrpcNamingPolaris:
        pass

    obj = _MockTrpcNamingPolaris()
    setattr(obj, "__version__", '0.6.1')
    sys.modules["trpc_naming_polaris"] = obj
    assert check_new_trpc_naming_polaris()


def test_plugin_order():
    """Test the order of plugin initialization."""

    result = []

    @frame_register_plugin(PluginType.USER_DEFINED, "user_plugin_one")
    class _UserPluginOne:

        def __init__(self) -> None:
            self.name = "user_plugin_one"
            result.append(self.name)

    @frame_register_plugin(PluginType.USER_DEFINED, "user_plugin_three")
    class _UserPluginThree:

        def __init__(self) -> None:
            self.name = "user_plugin_three"
            result.append(self.name)

    @frame_register_plugin(PluginType.USER_DEFINED, "user_plugin_two")
    class _UserPluginTwo:

        def __init__(self) -> None:
            self.name = "user_plugin_two"
            result.append(self.name)

    @frame_register_plugin(PluginType.USER_DEFINED, "user_plugin_err")
    class _UserPluginError:

        def __init__(self) -> None:
            self.name = "user_plugin_two"
            raise ValueError("test")

    set_plugin_order(PluginType.USER_DEFINED, ["user_plugin_one", "user_plugin_two", "user_plugin_three"])
    setup(without_user_define=False)
    assert result == ["user_plugin_one", "user_plugin_two", "user_plugin_three"]
