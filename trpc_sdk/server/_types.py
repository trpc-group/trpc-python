# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

"""Shared types for the server service layer.

Holds the queue-wait policy and sliding window used by ``ServiceAdapter``. The
process-model records live in :mod:`trpc_sdk.runtime._types`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from enum import unique
from typing import List


@unique
class QueueWaitPolicy(IntEnum):
    """Queue-wait circuit-breaker policy (YAML ``server.queue_wait_policy``).

    Compared with ``ServiceOption.queue_wait_policy`` in
    :meth:`trpc_sdk.server._cython._service.ServiceAdapter._check_queue_wait`.
    """

    # Drop the request as soon as it exceeds the queue-wait limit.
    DEFAULT = 0
    # Drop only when the sliding window (QueueWindow) is also overloaded.
    SLIDING_WINDOW = 1


@dataclass
class Bucket:
    """One time-slice of queue-wait samples in a two-bucket sliding window."""

    # Number of requests recorded in this time slice.
    total: int = 0
    # Number of requests that exceeded the queue-wait timeout.
    failed: int = 0
    # Time-slice index (``current_ms // interval``) currently stored in this bucket.
    index: int = 0


class QueueWindow:
    """Two-bucket sliding window used for queue-wait circuit breaking.

    Time is divided into slices of ``interval`` milliseconds. The current and
    previous slices occupy ``windows_list[index % 2]``. When a slice exceeds
    ``threshold`` samples and its failure ratio is above ``ratio``, the window
    is treated as overloaded and ``report_queue_time`` returns ``False``.
    """

    def __init__(self, interval: int = 60 * 1000, ratio: float = 0.1, threshold: int = 10):
        # Two rotating buckets for the current and previous time slices.
        self.windows_list: List[Bucket] = [Bucket() for _ in range(2)]
        # Time-slice index of the bucket most recently written.
        self.cur: int = 0
        # Reserved previous-slice index; unused by the current algorithm.
        self.last: int = 0
        # Length of one time slice, in milliseconds.
        self.interval: int = interval
        # Failure-ratio limit; exceeding it marks the window unhealthy.
        self.ratio: float = ratio
        # Minimum samples in a slice before the failure ratio is considered.
        self.threshold: int = threshold

    def report_queue_time(self, current: float, status: bool) -> bool:
        """Record one queue-wait sample and return whether the window is healthy.

        Args:
            current: Current timestamp in milliseconds.
            status: ``True`` if the request did not time out in the queue;
                ``False`` if it exceeded the queue-wait limit.

        Returns:
            ``False`` if the current or previous slice is overloaded;
            ``True`` otherwise.
        """
        # Map the timestamp onto a slice, then onto one of the two rotating buckets.
        time_index = int(current / self.interval)
        bucket_index = time_index % 2

        bucket = self.windows_list[bucket_index]
        if time_index != bucket.index:
            # This bucket still holds an older slice; reset it for the new one.
            self.cur = time_index
            self.windows_list[bucket_index].total = 0
            self.windows_list[bucket_index].failed = 0
            self.windows_list[bucket_index].index = time_index

        bucket.total = bucket.total + 1
        if status is False:
            bucket.failed = bucket.failed + 1

        if bucket.total > self.threshold and bucket.failed / bucket.total > self.ratio:
            return False

        return self.get_last_windows_status(current)

    def get_last_windows_status(self, current: float) -> bool:
        """Return whether the previous time slice is healthy.

        Args:
            current: Current timestamp in milliseconds.

        Returns:
            ``False`` if the previous slice exists and is overloaded;
            ``True`` if it is missing or still within limits.
        """
        current_index = int(current / self.interval)
        last_time_index = current_index - 1
        last_bucket = self.windows_list[last_time_index % 2]
        if last_bucket.index != last_time_index:
            # No samples were recorded in the previous slice.
            return True

        if last_bucket.total > self.threshold and last_bucket.failed / last_bucket.total > self.ratio:
            return False

        return True
