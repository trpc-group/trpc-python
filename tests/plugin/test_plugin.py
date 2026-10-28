# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import pytest

from trpc_sdk.abc import BasePlugin
from trpc_sdk.abc import PluginInitPos
from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import DEFAULT_PLUGIN_MANAGER
from trpc_sdk.plugin import PluginInitError
from trpc_sdk.plugin import init_plugins
from trpc_sdk.plugin import register_plugin as frame_register_plugin
from trpc_sdk.plugin import set_plugin_order
from trpc_sdk.plugin._plugin import PluginSpec
from trpc_sdk.plugin._plugin import _PluginManager

DummyPluginManager = _PluginManager()


class DummyPlugin:
    """A dummy plugin class for testing purposes."""
    pass


class EmptyPlugin(BasePlugin):
    """A dummy plugin class for testing purposes."""

    def __init__(self):
        super().__init__()
        self.master_start = False
        self.worker_start = False

    def on_master_start(self):
        self.master_start = True

    def on_worker_start(self):
        self.worker_start = True


def register_plugin(cls, name, ptype=PluginType.USER_DEFINED, version="", pos=PluginInitPos.WORKER, report=False):
    plugin_spec = PluginSpec(cls=cls,
                             init_pos=pos,
                             plugin_type=ptype,
                             plugin_name=name,
                             version=version,
                             report=report)
    DummyPluginManager.register(plugin_spec)
    return plugin_spec


def test_plugin_type():
    """Test the functionality of PluginType enum values and names."""
    plugin_type = PluginType.USER_DEFINED
    assert plugin_type.value == 1000
    assert plugin_type.name == "USER_DEFINED"


def test_plugin_register():
    """Test the registration of a plugin with PluginManager."""
    plugin_spec = register_plugin(DummyPlugin, "runtime_test", version="0.0.1")

    plugin_spec_obj = DummyPluginManager.get_plugin_spec(PluginType.USER_DEFINED, "runtime_test")
    assert isinstance(plugin_spec_obj, PluginSpec)

    assert plugin_spec_obj.cls == DummyPlugin
    assert plugin_spec_obj.version == plugin_spec.version
    assert not plugin_spec_obj.report


def test_plugin_specs():
    """Test the retrieval of all plugin specifications from PluginManager."""
    plugin_spec = register_plugin(DummyPlugin, "runtime_test", version="0.0.1", report=True)

    specs = DummyPluginManager.all_plugin_specs()
    assert len(specs) == 1
    assert specs[0] == plugin_spec


def test_default_plugins():
    """Built-in plugins are registered once their packages are imported."""
    import trpc_sdk.naming  # pylint: disable=unused-import

    plugins = DEFAULT_PLUGIN_MANAGER.all_plugin_specs()

    for plugin_spec in plugins:
        print(type(plugin_spec.plugin_name), plugin_spec.plugin_name, plugin_spec.version)

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
    plugin_spec = register_plugin(DummyInvalidPlugin, "dummy_invalid_plugin", pos=PluginInitPos.WORKER)
    with pytest.raises(PluginInitError):
        plugin_spec.cls()


def test_plugin_spec_records_version_and_report():
    """Plugin metadata lives on PluginSpec, including version and the report flag."""
    plugin_spec = register_plugin(DummyPlugin, "versioned_plugin", version="0.6.1", report=True)
    assert plugin_spec.version == "0.6.1"
    assert plugin_spec.report is True


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
    init_plugins(without_user_define=False)
    ours = [name for name in result if name.startswith("user_plugin_")]
    assert ours[:3] == ["user_plugin_one", "user_plugin_two", "user_plugin_three"]
