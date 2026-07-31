# -*- coding: utf-8 -*-
from trpc.plugin import PluginType
from trpc.plugin import get_plugin
from multiprocessing import Value

defaultRegistry = None
__counter = Value('i', 0)


class GetRegistry(object):
    pass


def get_registry(name: str):
    return get_plugin(PluginType.NAMING_REGISTRY, name)


def pause_heartbeat():
    global __counter
    with __counter.get_lock():
        __counter.value += 1


def resume_heartbeat():
    global __counter
    with __counter.get_lock():
        if __counter.value > 0:
            __counter.value -= 1


def should_heartbeat():
    global __counter
    with __counter.get_lock():
        return __counter.value == 0
