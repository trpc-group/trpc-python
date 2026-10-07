# -*- coding: utf-8 -*-
"""Span id allocation and sampling decisions shared by both rpcz filters.

Every object here is per-process and deliberately unsynchronized: span
ids only have to be unique within the process that produced them, and
the rate counters are advisory. The client filter keeps one module-level
id generator so sibling calls share a sequence, while each filter owns
its own sampler.
"""

from __future__ import annotations

import time


class SpanIdGenerator:
    """Per-process span id generator."""

    def __init__(self):
        self._next_id = 0

    def generate_id(self) -> int:
        """Return the next id in this process's sequence.

        Returns:
            A monotonically increasing integer starting at zero.
        """
        span_id = self._next_id
        self._next_id += 1
        return span_id


class SampleRateLimiter:
    """Per-second sample counter."""

    def __init__(self):
        self._second = 0
        self._count = 0

    def _sync(self) -> None:
        """Reset the counter when the wall-clock second has rolled over."""
        current_second = int(time.time())
        if current_second != self._second:
            self._second = current_second
            self._count = 0

    def increase(self) -> int:
        """Count one sample in the current second.

        Returns:
            The number of samples taken so far this second.
        """
        self._sync()
        self._count += 1
        return self._count

    def get_sample_num(self) -> int:
        """Return this second's sample count without counting one.

        Returns:
            The number of samples taken so far this second.
        """
        self._sync()
        return self._count


class ProbabilitySampler:
    """Simple modulo sampler."""

    @staticmethod
    def sample(span_id: int, sample_rate: int) -> bool:
        """Decide whether ``span_id`` falls on the sampling stride.

        Args:
            span_id: Id allocated by :class:`SpanIdGenerator`.
            sample_rate: Keep one span out of every ``sample_rate``. A
                non-positive rate turns sampling off entirely.

        Returns:
            True when this span should be sampled.
        """
        if sample_rate <= 0:
            return False
        return span_id % sample_rate == 0


class HighLowWaterLevelSampler:
    """High/low water-level sampler.

    Wraps :class:`ProbabilitySampler` with a per-second budget so a
    traffic spike cannot flood the span containers: above the high water
    mark a span that hit the stride is dropped anyway, and below the low
    water mark a span that missed the stride is kept so that a quiet
    service still produces data.
    """

    def __init__(self):
        self._limiter = SampleRateLimiter()

    def sample(self, span_id: int, sample_rate: int, lower_water_level: int, high_water_level: int) -> bool:
        """Decide whether to sample one span under the per-second budget.

        Args:
            span_id: Id allocated by :class:`SpanIdGenerator`.
            sample_rate: Stride passed to :class:`ProbabilitySampler`.
            lower_water_level: Samples per second below which off-stride
                spans are kept anyway.
            high_water_level: Samples per second above which on-stride
                spans are dropped.

        Returns:
            True when this span should be sampled.
        """
        if ProbabilitySampler.sample(span_id, sample_rate):
            if self._limiter.increase() > high_water_level:
                return False
            return True

        if self._limiter.get_sample_num() < lower_water_level:
            self._limiter.increase()
            return True
        return False
