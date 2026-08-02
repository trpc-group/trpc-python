# -*- coding: utf-8 -*-
#
# Copyright @ 2025 Tencent.com
"""Test cases for string utilities in TRPC framework.

This module contains unit tests for verifying the behavior of string utility
functions provided by str_utils module.
"""

# System imports
import pytest

# Local imports
from trpc_sdk.utils.str_utils import strtobool

# Third-party imports
# (None required)



def test_true_values():
    """Verify all true string representations are correctly converted."""
    true_strings = ['y', 'yes', 't', 'true', 'on', '1']
    for s in true_strings:
        assert strtobool(s) == 1


def test_false_values():
    """Verify all false string representations are correctly converted."""
    false_strings = ['n', 'no', 'f', 'false', 'off', '0']
    for s in false_strings:
        assert strtobool(s) == 0


def test_case_insensitivity():
    """Verify function is case insensitive."""
    mixed_case = ['Yes', 'TRUE', 'On', 'NO', 'FaLsE', 'oFf']
    expected = [1, 1, 1, 0, 0, 0]
    for s, exp in zip(mixed_case, expected):
        assert strtobool(s) == exp


def test_invalid_values():
    """Verify invalid strings raise ValueError."""
    invalid_strings = ['', 'maybe', '2', 'yesno', None, 123]
    for s in invalid_strings:
        with pytest.raises(ValueError):
            strtobool(s) if isinstance(s, str) else strtobool(str(s))


def test_whitespace_handling():
    """Verify whitespace is properly handled."""
    assert strtobool(' yes ') == 1
    assert strtobool(' no ') == 0
    with pytest.raises(ValueError):
        strtobool(' ')
