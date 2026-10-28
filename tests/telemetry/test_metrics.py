# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test metrics"""
import pytest

from trpc_sdk.abc import ActiveData
from trpc_sdk.abc import BaseMetrics
from trpc_sdk.abc import CallResult
from trpc_sdk.abc import CustomData
from trpc_sdk.abc import PassiveData
from trpc_sdk.abc import StatPolicy
from trpc_sdk.abc import StatValue
from trpc_sdk.telemetry import report_active
from trpc_sdk.telemetry import report_attr
from trpc_sdk.telemetry import report_custom
from trpc_sdk.telemetry import report_passive
from trpc_sdk.telemetry import set_default_metrics


class _TestMetrics(BaseMetrics):

    def report_active(self, active_data: ActiveData, call_result: CallResult) -> bool:
        print(f"report_active -> active_data: {active_data} call_result: {call_result}")
        return True

    def report_passive(self, passive_data: PassiveData, call_result: CallResult) -> bool:
        print(f"report_passive -> passive_data: {passive_data} call_result: {call_result}")
        return True

    def report_attr(self, key: str, value: float, policy: StatPolicy) -> bool:
        print(f"report_attr -> key: {key} value: {value} policy: {policy}")
        return True

    def report_custom(self, data: CustomData) -> bool:
        if hasattr(data, "tags"):
            assert "a" in data.tags
        print("report_custom")
        return True


@pytest.mark.asyncio
async def test_metrics():
    """Test metrics"""
    set_default_metrics(_TestMetrics())

    assert report_active(ActiveData(), CallResult())
    assert report_passive(PassiveData(), CallResult())
    assert report_attr("k", 1, StatPolicy.SUM)
    custom = CustomData("custom", ["dim"], [StatValue(1.1, 1, StatPolicy.SUM)])
    custom.tags = {"a": "b"}
    assert report_custom(custom)
