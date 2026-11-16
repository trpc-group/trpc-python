# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import pytest

from trpc_sdk.compressor import CompressGZip
from trpc_sdk.compressor import CompressNoop
from trpc_sdk.compressor import CompressSnappy
from trpc_sdk.compressor import CompressStreamSnappy
from trpc_sdk.compressor import CompressType
from trpc_sdk.compressor import CompressZlib
from trpc_sdk.compressor import compress
from trpc_sdk.compressor import decompress
from trpc_sdk.compressor import get_compress


def test_class_attribute_access():
    """Test accessing class attributes."""
    assert CompressType.NOOP == 0
    assert CompressType.GZIP == 1
    assert CompressType.SNAPPY == 2
    assert CompressType.ZLIB == 3
    assert CompressType.SNAPPY_STREAM == 4
    assert CompressType.SNAPPY_BLOCK == 5
    assert CompressType.LZ4_FRAME == 6
    assert CompressType.LZ4_BLOCK == 7
    assert CompressType.ZSTD == 8


def test_lookup_by_wire_value():
    """Test resolving a decoded content_encoding back to its member."""
    assert CompressType(0) is CompressType.NOOP
    assert CompressType(1) is CompressType.GZIP
    assert CompressType(2) is CompressType.SNAPPY
    assert CompressType(3) is CompressType.ZLIB
    assert CompressType(4) is CompressType.SNAPPY_STREAM
    assert CompressType(5) is CompressType.SNAPPY_BLOCK
    assert CompressType(6) is CompressType.LZ4_FRAME
    assert CompressType(7) is CompressType.LZ4_BLOCK
    assert CompressType(8) is CompressType.ZSTD
    with pytest.raises(ValueError):
        CompressType(99)


def test_attribute_not_found():
    """Test AttributeError when attribute not found in both places."""
    with pytest.raises(AttributeError):
        _ = CompressType.NonExistentType


def test_get_compress():
    """Test get_compress function."""
    compressor = get_compress(CompressType.NOOP)
    assert isinstance(compressor, CompressNoop)
    compressor = get_compress(CompressType.GZIP)
    assert isinstance(compressor, CompressGZip)
    compressor = get_compress(CompressType.SNAPPY)
    assert isinstance(compressor, CompressSnappy)
    compressor = get_compress(CompressType.SNAPPY_STREAM)
    assert isinstance(compressor, CompressStreamSnappy)
    compressor = get_compress(CompressType.SNAPPY_BLOCK)
    assert isinstance(compressor, CompressSnappy)
    compressor = get_compress(CompressType.ZLIB)
    assert isinstance(compressor, CompressZlib)


def test_compress():
    """Test compress function."""
    src = b'test_data'
    compress_result = compress(CompressType.GZIP, src)
    decompress_result = decompress(CompressType.GZIP, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.NOOP, src)
    decompress_result = decompress(CompressType.NOOP, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.ZLIB, src)
    decompress_result = decompress(CompressType.ZLIB, compress_result)
    assert decompress_result == src


def test_snappy_compress():
    """Test snappy compress function when python-snappy is installed."""
    pytest.importorskip("snappy")
    src = b'test_data'
    compress_result = compress(CompressType.SNAPPY, src)
    decompress_result = decompress(CompressType.SNAPPY, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.SNAPPY_STREAM, src)
    decompress_result = decompress(CompressType.SNAPPY_STREAM, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.SNAPPY_BLOCK, src)
    decompress_result = decompress(CompressType.SNAPPY_BLOCK, compress_result)
    assert decompress_result == src


def test_actual_compress_operations():
    """Test actual compress operations."""
    unsupport_compress = get_compress(99)
    assert unsupport_compress is None

    noop_compress = get_compress(CompressType.NOOP)
    assert noop_compress.compress(b'') == b''
    assert noop_compress.decompress(b'') == b''
    body = b"this is noop test data"
    compress_body = b"this is noop test data"
    assert noop_compress.compress(body) == compress_body
    assert noop_compress.decompress(compress_body) == body

    zlib_compress = get_compress(CompressType.ZLIB)
    assert zlib_compress.compress(b'') == b''
    assert zlib_compress.decompress(b'') == b''
    body = b"this is zlib test data"
    compress_body = b"x\x9c+\xc9\xc8,V\x00\xa2\xaa\x9c\xcc$\x85\x92\xd4\xe2\x12\x85\x94\xc4\x92D\x00]\xdb\x08 "
    assert zlib_compress.compress(body) == compress_body
    assert zlib_compress.decompress(compress_body) == body

    gzip_compress = get_compress(CompressType.GZIP)
    assert gzip_compress.compress(b'') == b''
    assert gzip_compress.decompress(b'') == b''
    body = b"this is gzip test data"
    compress_body = b"\x1f\x8b\x08\x00\x90R\xce^\x02\xff+\xc9\xc8,V\x00\xa2\xf4\xaa\xcc\x02\x85\x92" \
                    b"\xd4\xe2\x12\x85\x94\xc4\x92D\x00\xf7R\x15<\x16\x00\x00\x00"
    assert gzip_compress.decompress(compress_body) == body
    # gzip output includes the current mtime, so a historical blob does not repeat.
    assert gzip_compress.compress(body) != compress_body


def test_actual_snappy_operations():
    """Test snappy byte layout when python-snappy is installed."""
    pytest.importorskip("snappy")
    snappy_compress = get_compress(CompressType.SNAPPY)
    assert snappy_compress.compress(b'') == b''
    assert snappy_compress.decompress(b'') == b''
    body = b"this is snappy test data"
    compress_body = b'\x18\\this is snappy test data'
    assert snappy_compress.compress(body) == compress_body
    assert snappy_compress.decompress(compress_body) == body
