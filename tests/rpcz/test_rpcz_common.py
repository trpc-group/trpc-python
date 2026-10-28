# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import time

from trpc_sdk.rpcz import HighLowWaterLevelSampler
from trpc_sdk.rpcz import SampleRateLimiter
from trpc_sdk.rpcz import SpanIdGenerator


def test_span_id_generator_increase():
    gen = SpanIdGenerator()
    assert gen.generate_id() == 0
    assert gen.generate_id() == 1
    assert gen.generate_id() == 2


def test_sample_rate_limiter_reset_by_second():
    limiter = SampleRateLimiter()
    assert limiter.increase() == 1
    assert limiter.increase() == 2

    limiter._second = int(time.time()) - 1  # pylint: disable=protected-access
    assert limiter.get_sample_num() == 0


def test_high_low_water_level_sampler_basic_behavior():
    sampler = HighLowWaterLevelSampler()

    # First one is selected by probability path.
    assert sampler.sample(span_id=0, sample_rate=2, lower_water_level=1, high_water_level=10) is True
    # Non-probability path can still be selected by low-water guarantee.
    assert sampler.sample(span_id=1, sample_rate=2, lower_water_level=3, high_water_level=10) is True
