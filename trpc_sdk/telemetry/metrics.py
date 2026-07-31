# -*- coding: utf-8 -*-
"""
    trpc metrics output interface
"""
from trpc.plugin import PluginType
from trpc.plugin import get_plugin
from trpc.trpc_abc.metrics import ActiveData
from trpc.trpc_abc.metrics import BaseMetrics
from trpc.trpc_abc.metrics import CallResult
from trpc.trpc_abc.metrics import CustomData
from trpc.trpc_abc.metrics import PassiveData
from trpc.trpc_abc.metrics import StatPolicy

DEFAULT_METRICS = None


def set_default_metrics(metrics: BaseMetrics):
    """Set default metrics"""
    global DEFAULT_METRICS
    DEFAULT_METRICS = metrics


def report_active(active_data: ActiveData, call_result: CallResult) -> bool:
    """Report active"""
    if DEFAULT_METRICS:
        return DEFAULT_METRICS.report_active(active_data, call_result)
    return False


def report_passive(passive_data: PassiveData, call_result: CallResult) -> bool:
    """Report passive"""
    if DEFAULT_METRICS:
        return DEFAULT_METRICS.report_passive(passive_data, call_result)
    return False


def report_attr(key: str, value: float, policy: StatPolicy = StatPolicy.SUM) -> bool:
    """Report attr"""
    if DEFAULT_METRICS:
        return DEFAULT_METRICS.report_attr(key, value, policy)
    return False


def report_custom(data: CustomData) -> bool:
    """Report custom"""
    if DEFAULT_METRICS:
        return DEFAULT_METRICS.report_custom(data)
    return False


def get_metrics(name: str) -> BaseMetrics:
    """Get metrics object"""
    return get_plugin(PluginType.METRICS, name)
