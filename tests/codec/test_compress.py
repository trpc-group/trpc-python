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
    assert CompressType.Noop == 0
    assert CompressType.Gzip == 1
    assert CompressType.Snappy == 2
    assert CompressType.ZLib == 3
    assert CompressType.SnappyStream == 4
    assert CompressType.SnappyBlock == 5
    assert CompressType.Lz4Frame == 6
    assert CompressType.Lz4Block == 7
    assert CompressType.Zstd == 8


def test_instance_attribute_access():
    """Test accessing instance attributes."""
    compress_type = CompressType()
    assert compress_type.Noop == 0
    assert compress_type.Gzip == 1
    assert compress_type.Snappy == 2
    assert compress_type.ZLib == 3
    assert compress_type.SnappyStream == 4
    assert compress_type.SnappyBlock == 5
    assert compress_type.Lz4Frame == 6
    assert compress_type.Lz4Block == 7
    assert compress_type.Zstd == 8


def test_attribute_not_found():
    """Test AttributeError when attribute not found in both places."""
    with pytest.raises(AttributeError):
        _ = CompressType.NonExistentType


def test_get_compress():
    """Test get_compress function."""
    compressor = get_compress(CompressType.Noop)
    assert isinstance(compressor, CompressNoop)
    compressor = get_compress(CompressType.Gzip)
    assert isinstance(compressor, CompressGZip)
    compressor = get_compress(CompressType.Snappy)
    assert isinstance(compressor, CompressSnappy)
    compressor = get_compress(CompressType.SnappyStream)
    assert isinstance(compressor, CompressStreamSnappy)
    compressor = get_compress(CompressType.SnappyBlock)
    assert isinstance(compressor, CompressSnappy)
    compressor = get_compress(CompressType.ZLib)
    assert isinstance(compressor, CompressZlib)


def test_compress():
    """Test compress function."""
    src = b'test_data'
    compress_result = compress(CompressType.Gzip, src)
    decompress_result = decompress(CompressType.Gzip, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.Noop, src)
    decompress_result = decompress(CompressType.Noop, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.ZLib, src)
    decompress_result = decompress(CompressType.ZLib, compress_result)
    assert decompress_result == src


def test_snappy_compress():
    """Test snappy compress function when python-snappy is installed."""
    pytest.importorskip("snappy")
    src = b'test_data'
    compress_result = compress(CompressType.Snappy, src)
    decompress_result = decompress(CompressType.Snappy, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.SnappyStream, src)
    decompress_result = decompress(CompressType.SnappyStream, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.SnappyBlock, src)
    decompress_result = decompress(CompressType.SnappyBlock, compress_result)
    assert decompress_result == src


def test_actual_compress_operations():
    """Test actual compress operations."""
    unsupport_compress = get_compress(99)
    assert unsupport_compress is None

    noop_compress = get_compress(CompressType.Noop)
    assert noop_compress.compress(b'') == b''
    assert noop_compress.decompress(b'') == b''
    body = b"this is noop test data"
    compress_body = b"this is noop test data"
    assert noop_compress.compress(body) == compress_body
    assert noop_compress.decompress(compress_body) == body

    zlib_compress = get_compress(CompressType.ZLib)
    assert zlib_compress.compress(b'') == b''
    assert zlib_compress.decompress(b'') == b''
    body = b"this is zlib test data"
    compress_body = b"x\x9c+\xc9\xc8,V\x00\xa2\xaa\x9c\xcc$\x85\x92\xd4\xe2\x12\x85\x94\xc4\x92D\x00]\xdb\x08 "
    assert zlib_compress.compress(body) == compress_body
    assert zlib_compress.decompress(compress_body) == body

    gzip_compress = get_compress(CompressType.Gzip)
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
    snappy_compress = get_compress(CompressType.Snappy)
    assert snappy_compress.compress(b'') == b''
    assert snappy_compress.decompress(b'') == b''
    body = b"this is snappy test data"
    compress_body = b'\x18\\this is snappy test data'
    assert snappy_compress.compress(body) == compress_body
    assert snappy_compress.decompress(compress_body) == body
