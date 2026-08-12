# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Internal data models for the Loguru logging backend."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

# cSpell:ignore trpc


@dataclass
class LevelHolder:
    """A mutable level number read by the sink filters at emit time."""

    level_no: int


@dataclass
class SinkBinding:
    """One trpc output of one trpc logger, attached to a physical sink."""

    logger_name: str
    gate: LevelHolder
    level_holder: LevelHolder
    line_format: Union[str, Callable]
    filename: Optional[str]
    capture_third_party: bool
    third_party_level: int
    options: Dict[str, Any]


@dataclass
class PhysicalSink:
    """Represent one physical Loguru sink shared by logical logger bindings."""

    rotation_signature: tuple
    sink_id: Optional[int] = None
    bindings: List[SinkBinding] = field(default_factory=list)
