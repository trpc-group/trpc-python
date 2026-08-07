# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Metrics extension interfaces and shared report models.

This module defines statistics policies, call-dimension payloads, and the
abstract metrics reporter used by pluggable monitoring backends.
"""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from enum import IntEnum
from enum import unique
from typing import Dict
from typing import List


@unique
class StatPolicy(Enum):
    """Define aggregation policies for reported metric values."""

    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"
    SET = "SET"
    HISTOGRAM = "HISTOGRAM"


@dataclass
class ActiveData:
    """Describe dimensions for an outbound (caller-side) metric report.

    Attributes:
        active_service: Caller service name.
        active_interface: Caller method or interface name.
        passive_app: Callee application name.
        passive_server: Callee server name.
        passive_service: Callee service name.
        passive_interface: Callee method or interface name.
        passive_ip: Callee endpoint IP address.
        passive_container: Callee container name.
        passive_consetid: Callee container set identifier.
        ret_code: Application or framework return code.
    """

    active_service: str = ""
    active_interface: str = ""
    passive_app: str = ""
    passive_server: str = ""
    passive_service: str = ""
    passive_interface: str = ""
    passive_ip: str = ""
    passive_container: str = ""
    passive_consetid: str = ""
    ret_code: int = 0


@dataclass
class PassiveData:
    """Describe dimensions for an inbound (callee-side) metric report.

    Attributes:
        active_app: Caller application name.
        active_server: Caller server name.
        active_service: Caller service name.
        active_interface: Caller method or interface name.
        active_ip: Caller endpoint IP address.
        active_container: Caller container name.
        active_consetid: Caller container set identifier.
        passive_service: Callee service name.
        passive_interface: Callee method or interface name.
        ret_code: Application or framework return code.
    """

    active_app: str = ""
    active_server: str = ""
    active_service: str = ""
    active_interface: str = ""
    active_ip: str = ""
    active_container: str = ""
    active_consetid: str = ""
    passive_service: str = ""
    passive_interface: str = ""
    ret_code: int = 0


class ReportStatus(IntEnum):
    """Define call outcome statuses used by metric reports."""

    SUCCESS = 0
    EXCEPTION = 1
    TIMEOUT = 2


@dataclass
class CallResult:
    """Carry the outcome and cost of one RPC call.

    Attributes:
        status: Call completion status.
        time: Call duration in milliseconds.
    """

    status: ReportStatus = ReportStatus.SUCCESS
    time: int = 0


@dataclass
class StatValue:
    """Carry one aggregated metric sample.

    Attributes:
        value: Numeric sample value.
        count: Number of observations contributing to this sample.
        policy: Aggregation policy applied to the sample.
    """

    value: float = 0
    count: int = 0
    policy: StatPolicy = StatPolicy.SUM


@dataclass
class CustomData:
    """Carry one custom multi-dimension metric report.

    Attributes:
        name: Metric name.
        dimensions: Ordered dimension values associated with the metric.
        stat_values: Aggregated samples reported with the metric.
        tags: Additional key and value labels.
    """

    name: str = ""
    dimensions: List[str] = field(default_factory=list)
    stat_values: List[StatValue] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)


class BaseMetrics(ABC):
    """Define caller, callee, attribute, and custom metric reporting."""

    @abstractmethod
    def report_active(self, active_data: ActiveData, call_result: CallResult) -> bool:
        """Report one outbound call metric.

        Args:
            active_data: Caller-side metric dimensions.
            call_result: Call status and duration.

        Returns:
            ``True`` if the report was accepted, otherwise ``False``.
        """
        ...

    @abstractmethod
    def report_passive(self, passive_data: PassiveData, call_result: CallResult) -> bool:
        """Report one inbound call metric.

        Args:
            passive_data: Callee-side metric dimensions.
            call_result: Call status and duration.

        Returns:
            ``True`` if the report was accepted, otherwise ``False``.
        """
        ...

    @abstractmethod
    def report_attr(self, key: str, value: float, policy: StatPolicy) -> bool:
        """Report one named attribute metric.

        Args:
            key: Attribute metric name.
            value: Numeric sample value.
            policy: Aggregation policy applied to the sample.

        Returns:
            ``True`` if the report was accepted, otherwise ``False``.
        """
        ...

    @abstractmethod
    def report_custom(self, data: CustomData) -> bool:
        """Report one custom multi-dimension metric.

        Args:
            data: Custom metric payload.

        Returns:
            ``True`` if the report was accepted, otherwise ``False``.
        """
        ...
