# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test singleton module"""

from trpc_sdk.utils import singleton


@singleton
class MockData:
    """Test class"""

    def __init__(self) -> None:
        self.data = 1


def test_singleton():
    """Test singleton"""
    m_data1 = MockData()
    m_data1.data = 2
    m_data2 = MockData()
    assert m_data2.data == m_data1.data
    assert id(m_data2) == id(m_data1)
