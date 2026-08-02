# -*- coding: utf-8 -*-
"""Test singleton module"""

from trpc_sdk.utils.singleton import singleton


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
