# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Telemetry metrics API for tRPC-Python."""

from __future__ import annotations

from typing import Optional

from trpc_sdk.abc import PluginType
from trpc_sdk.abc import ActiveData
from trpc_sdk.abc import BaseMetrics
from trpc_sdk.abc import CallResult
from trpc_sdk.abc import CustomData
from trpc_sdk.abc import PassiveData
from trpc_sdk.abc import StatPolicy
from trpc_sdk.plugin import get_plugin

DEFAULT_METRICS: Optional[BaseMetrics] = None


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
