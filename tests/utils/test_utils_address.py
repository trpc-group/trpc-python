# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.
"""Test cases for address utilities in TRPC framework.

This module contains unit tests for verifying the behavior of address utility
functions provided by _address module.
"""

# System imports
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from trpc_sdk.utils import convert_address_to_str
from trpc_sdk.utils import get_addr_from_transport
from trpc_sdk.utils import get_address_from_str
from trpc_sdk.utils import make_addr_info_to_bytes
from trpc_sdk.utils import parse_addr_info_from_bytes
from trpc_sdk.utils import set_addr_to_ctx_from_transport
from trpc_sdk.utils._types import AddrType


class TestAddressUtilities:
    """Test suite for address utility functions."""

    def test_get_address_from_str_valid(self):
        """Verify valid address string is correctly parsed."""
        # Test IPv4 address
        result = get_address_from_str("192.168.1.1:8080")
        assert result == ("192.168.1.1", 8080)

        # Test localhost
        result = get_address_from_str("localhost:3000")
        assert result == ("localhost", 3000)

        # Test hostname with port
        result = get_address_from_str("example.com:443")
        assert result == ("example.com", 443)

    def test_get_address_from_str_invalid(self):
        """Verify invalid address strings raise appropriate errors."""
        # Missing port
        with pytest.raises(ValueError):
            get_address_from_str("192.168.1.1")

        # Invalid port format
        with pytest.raises(ValueError):
            get_address_from_str("192.168.1.1:not_a_port")

        # Empty string
        with pytest.raises(ValueError):
            get_address_from_str("")

    def test_convert_address_to_str_from_tuple(self):
        """Verify address tuple is correctly converted to string."""
        # IPv4 address
        result = convert_address_to_str(("192.168.1.1", 8080))
        assert result == "192.168.1.1:8080"

        # Localhost
        result = convert_address_to_str(("localhost", 3000))
        assert result == "localhost:3000"

        # IPv6 address
        result = convert_address_to_str(("[::1]", 9000))
        assert result == "[::1]:9000"

    def test_convert_address_to_str_from_string(self):
        """Verify address string is returned unchanged."""
        address_str = "192.168.1.1:8080"
        result = convert_address_to_str(address_str)
        assert result == address_str

    def test_get_addr_from_transport(self):
        """Verify addresses are correctly extracted from transport."""
        # Mock transport with address information
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("192.168.1.1", 8080),
            'peername': ("10.0.0.1", 9090)
        }[key]

        local_addr, peer_addr = get_addr_from_transport(mock_transport)

        assert local_addr == "192.168.1.1:8080"
        assert peer_addr == "10.0.0.1:9090"

    def test_get_addr_from_transport_none_addresses(self):
        """Verify function handles None addresses gracefully."""
        # Mock transport with None addresses
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {'sockname': None, 'peername': None}[key]

        local_addr, peer_addr = get_addr_from_transport(mock_transport)

        # Should convert None to default address
        assert local_addr == "0.0.0.0:0"
        assert peer_addr == "0.0.0.0:0"

    def test_set_addr_to_ctx_from_transport(self):
        """Verify addresses are correctly set to context."""
        # Mock transport and context
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("192.168.1.1", 8080),
            'peername': ("10.0.0.1", 9090)
        }[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Verify message methods were called
        mock_msg.set_local_ip.assert_called_once_with("192.168.1.1")
        mock_msg.set_remote_addr.assert_called_once_with("10.0.0.1")

        # Verify context attributes were set
        assert mock_ctx.remote_ip == "10.0.0.1"
        assert mock_ctx.remote_port == 9090
        assert mock_ctx.local_ip == "192.168.1.1"
        assert mock_ctx.local_port == 8080

    def test_set_addr_to_ctx_from_transport_none_addresses(self):
        """Verify function handles None addresses in context setting."""
        # Mock transport with None addresses
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {'sockname': None, 'peername': None}[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Verify default addresses were set
        mock_msg.set_local_ip.assert_called_once_with("0.0.0.0")
        mock_msg.set_remote_addr.assert_called_once_with("0.0.0.0")

        assert mock_ctx.remote_ip == "0.0.0.0"
        assert mock_ctx.remote_port == 0
        assert mock_ctx.local_ip == "0.0.0.0"
        assert mock_ctx.local_port == 0

    def test_set_addr_to_ctx_from_transport_comprehensive(self):
        """Comprehensive test for set_addr_to_ctx_from_transport covering all code paths."""
        # Test 1: Normal case with tuple addresses
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("192.168.1.1", 8080),
            'peername': ("10.0.0.1", 9090)
        }[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Verify message methods were called with correct arguments
        mock_msg.set_local_ip.assert_called_once_with("192.168.1.1")
        mock_msg.set_remote_addr.assert_called_once_with("10.0.0.1")

        # Verify context attributes were set correctly
        assert mock_ctx.remote_ip == "10.0.0.1"
        assert mock_ctx.remote_port == 9090
        assert mock_ctx.local_ip == "192.168.1.1"
        assert mock_ctx.local_port == 8080

    def test_set_addr_to_ctx_from_transport_none_addresses(self):
        """Test handling of None addresses."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {'sockname': None, 'peername': None}[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Verify default addresses were set
        mock_msg.set_local_ip.assert_called_once_with("0.0.0.0")
        mock_msg.set_remote_addr.assert_called_once_with("0.0.0.0")

        assert mock_ctx.remote_ip == "0.0.0.0"
        assert mock_ctx.remote_port == 0
        assert mock_ctx.local_ip == "0.0.0.0"
        assert mock_ctx.local_port == 0

    def test_set_addr_to_ctx_from_transport_string_addresses(self):
        """Test handling of string addresses (converted to tuple with port 0)."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {'sockname': "192.168.1.1", 'peername': "10.0.0.1"}[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Verify string addresses were converted to tuples with port 0
        mock_msg.set_local_ip.assert_called_once_with("192.168.1.1")
        mock_msg.set_remote_addr.assert_called_once_with("10.0.0.1")

        assert mock_ctx.remote_ip == "10.0.0.1"
        assert mock_ctx.remote_port == 0
        assert mock_ctx.local_ip == "192.168.1.1"
        assert mock_ctx.local_port == 0

    def test_set_addr_to_ctx_from_transport_invalid_local_addr(self):
        """Test handling of invalid local address format."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("invalid", ),  # Invalid tuple length
            'peername': ("10.0.0.1", 9090)
        }[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        with pytest.raises(ValueError, match="Invalid local address"):
            set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

    def test_set_addr_to_ctx_from_transport_invalid_peer_addr(self):
        """Test handling of invalid peer address format."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("192.168.1.1", 8080),
            'peername': ("invalid", )  # Invalid tuple length
        }[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        with pytest.raises(ValueError, match="Invalid peer address"):
            set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

    def test_set_addr_to_ctx_from_transport_mixed_address_types(self):
        """Test handling of mixed address types (tuple and string)."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("192.168.1.1", 8080),  # Tuple address
            'peername': "10.0.0.1"  # String address
        }[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Verify both types were handled correctly
        mock_msg.set_local_ip.assert_called_once_with("192.168.1.1")
        mock_msg.set_remote_addr.assert_called_once_with("10.0.0.1")

        assert mock_ctx.remote_ip == "10.0.0.1"
        assert mock_ctx.remote_port == 0  # String address gets port 0
        assert mock_ctx.local_ip == "192.168.1.1"
        assert mock_ctx.local_port == 8080

    def test_set_addr_to_ctx_from_transport_empty_tuple(self):
        """Test handling of empty tuple addresses - treated as falsy and replaced with defaults."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': (),  # Empty tuple (falsy value)
            'peername': ("10.0.0.1", 9090)
        }[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Empty tuple is falsy, so default address is used
        mock_msg.set_local_ip.assert_called_once_with("0.0.0.0")
        mock_msg.set_remote_addr.assert_called_once_with("10.0.0.1")

        assert mock_ctx.remote_ip == "10.0.0.1"
        assert mock_ctx.remote_port == 9090
        assert mock_ctx.local_ip == "0.0.0.0"
        assert mock_ctx.local_port == 0

    def test_set_addr_to_ctx_from_transport_three_element_tuple(self):
        """Test handling of tuple with more than 2 elements - raises ValueError."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("192.168.1.1", 8080, "extra"),  # 3-element tuple
            'peername': ("10.0.0.1", 9090)
        }[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        with pytest.raises(ValueError, match="Invalid local address"):
            set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

    def test_set_addr_to_ctx_from_transport_none_with_string(self):
        """Test handling of None with string conversion."""
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {'sockname': None, 'peername': "10.0.0.1"}[key]

        mock_ctx = Mock()
        mock_msg = Mock()
        mock_ctx.get_message.return_value = mock_msg

        set_addr_to_ctx_from_transport(mock_ctx, mock_transport)

        # Verify None was handled as default, string was converted
        mock_msg.set_local_ip.assert_called_once_with("0.0.0.0")
        mock_msg.set_remote_addr.assert_called_once_with("10.0.0.1")

        assert mock_ctx.remote_ip == "10.0.0.1"
        assert mock_ctx.remote_port == 0
        assert mock_ctx.local_ip == "0.0.0.0"
        assert mock_ctx.local_port == 0

    def test_make_addr_info_to_bytes(self):
        """Verify address information is correctly serialized to bytes."""
        # Mock transport
        mock_transport = Mock()
        mock_transport.get_extra_info.side_effect = lambda key: {
            'sockname': ("192.168.1.1", 8080),
            'peername': ("10.0.0.1", 9090)
        }[key]

        result = make_addr_info_to_bytes(mock_transport)

        expected = b"192.168.1.1:8080|10.0.0.1:9090"
        assert result == expected

    def test_parse_addr_info_from_bytes(self):
        """Verify address information is correctly parsed from bytes."""
        # Test data
        addr_bytes = b"192.168.1.1:8080|10.0.0.1:9090"

        local_addr, peer_addr = parse_addr_info_from_bytes(addr_bytes)

        assert local_addr == ("192.168.1.1", 8080)
        assert peer_addr == ("10.0.0.1", 9090)

    def test_parse_addr_info_from_bytes_ipv6(self):
        """Verify IPv6 addresses are correctly parsed."""
        # Test data with IPv6 addresses
        addr_bytes = b"[::1]:8080|[2001:db8::1]:9090"

        local_addr, peer_addr = parse_addr_info_from_bytes(addr_bytes)

        assert local_addr == ("[::1]", 8080)
        assert peer_addr == ("[2001:db8::1]", 9090)

    def test_parse_addr_info_from_bytes_invalid(self):
        """Verify invalid byte data raises appropriate errors."""
        # Missing separator
        with pytest.raises(ValueError):
            parse_addr_info_from_bytes(b"192.168.1.1:8080")

        # Invalid format
        with pytest.raises(ValueError):
            parse_addr_info_from_bytes(b"invalid_data")

    def test_addr_type_consistency(self):
        """Verify AddrType is consistently used throughout."""
        # Test that functions work correctly with AddrType
        addr_tuple: AddrType = ("192.168.1.1", 8080)

        # Convert to string and back
        addr_str = convert_address_to_str(addr_tuple)
        parsed_addr = get_address_from_str(addr_str)

        assert parsed_addr == addr_tuple
        assert isinstance(parsed_addr, tuple)
        assert len(parsed_addr) == 2
        assert isinstance(parsed_addr[0], str)
        assert isinstance(parsed_addr[1], int)
