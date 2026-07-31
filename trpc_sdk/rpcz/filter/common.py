# -*- coding: utf-8 -*-
"""Shared helpers for rpcz client/server filters."""

import time


class SpanIdGenerator:
    """Per-process span id generator."""

    def __init__(self):
        self._next_id = 0

    def generate_id(self) -> int:
        span_id = self._next_id
        self._next_id += 1
        return span_id


class SampleRateLimiter:
    """Per-second sample counter."""

    def __init__(self):
        self._second = 0
        self._count = 0

    def _sync(self) -> None:
        current_second = int(time.time())
        if current_second != self._second:
            self._second = current_second
            self._count = 0

    def increase(self) -> int:
        self._sync()
        self._count += 1
        return self._count

    def get_sample_num(self) -> int:
        self._sync()
        return self._count


class ProbabilitySampler:
    """Simple modulo sampler."""

    @staticmethod
    def sample(span_id: int, sample_rate: int) -> bool:
        if sample_rate <= 0:
            return False
        return span_id % sample_rate == 0


class HighLowWaterLevelSampler:
    """High/low water-level sampler."""

    def __init__(self):
        self._limiter = SampleRateLimiter()

    def sample(self, span_id: int, sample_rate: int, lower_water_level: int, high_water_level: int) -> bool:
        if ProbabilitySampler.sample(span_id, sample_rate):
            if self._limiter.increase() > high_water_level:
                return False
            return True

        if self._limiter.get_sample_num() < lower_water_level:
            self._limiter.increase()
            return True
        return False
