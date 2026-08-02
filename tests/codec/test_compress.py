# -*- coding: utf-8 -*-
#
# Copyright @ 2025 Tencent.com

import pytest

from trpc_sdk.codec.compress import CompressType
from trpc_sdk.codec.compress import compress
from trpc_sdk.codec.compress import decompress
from trpc_sdk.codec.compress import get_compress
from trpc_sdk.codec.compress_abc import AbstractCompress
from trpc_sdk.codec.compress_gzip import CompressGZip
from trpc_sdk.codec.compress_noop import CompressNoop
from trpc_sdk.codec.compress_snappy import CompressSnappy
from trpc_sdk.codec.compress_snappy import CompressStreamSnappy
from trpc_sdk.codec.compress_zlib import CompressZlib
from trpc_sdk.plugin import PluginType


def test_class_attribute_access():
    """Test accessing class attributes."""
    assert CompressType.Noop == 0
    assert CompressType.Gzip == 1
    assert CompressType.Snappy == 2
    assert CompressType.ZLib == 3
    assert CompressType.TRPC_SNAPPY_STREAM_COMPRESS == 4
    assert CompressType.TRPC_SNAPPY_BLOCK_COMPRESS == 5
    assert CompressType.TRPC_LZ4_FRAME_COMPRESS == 6
    assert CompressType.TRPC_LZ4_BLOCK_COMPRESS == 7


def test_instance_attribute_access():
    """Test accessing instance attributes."""
    compress_type = CompressType()
    assert compress_type.TRPC_DEFAULT_COMPRESS == 0
    assert compress_type.TRPC_GZIP_COMPRESS == 1
    assert compress_type.TRPC_SNAPPY_COMPRESS == 2
    assert compress_type.TRPC_ZLIB_COMPRESS == 3
    assert compress_type.Noop == 0
    assert compress_type.Gzip == 1
    assert compress_type.Snappy == 2
    assert compress_type.ZLib == 3
    assert compress_type.TRPC_SNAPPY_STREAM_COMPRESS == 4
    assert compress_type.TRPC_SNAPPY_BLOCK_COMPRESS == 5
    assert compress_type.TRPC_LZ4_FRAME_COMPRESS == 6
    assert compress_type.TRPC_LZ4_BLOCK_COMPRESS == 7


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
    compressor = get_compress(CompressType.TRPC_SNAPPY_STREAM_COMPRESS)
    assert isinstance(compressor, CompressStreamSnappy)
    compressor = get_compress(CompressType.TRPC_SNAPPY_BLOCK_COMPRESS)
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

    compress_result = compress(CompressType.Snappy, src)
    decompress_result = decompress(CompressType.Snappy, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.TRPC_SNAPPY_STREAM_COMPRESS, src)
    decompress_result = decompress(CompressType.TRPC_SNAPPY_STREAM_COMPRESS, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.TRPC_SNAPPY_BLOCK_COMPRESS, src)
    decompress_result = decompress(CompressType.TRPC_SNAPPY_BLOCK_COMPRESS, compress_result)
    assert decompress_result == src

    compress_result = compress(CompressType.ZLib, src)
    decompress_result = decompress(CompressType.ZLib, compress_result)
    assert decompress_result == src


def test_actual_compress_operations():
    """Test actual compress operations."""
    # unsupported
    unsupport_compress = get_compress(99)
    assert unsupport_compress is None

    # noop
    noop_compress = get_compress(CompressType.Noop)
    assert noop_compress.compress(b'') == b''
    assert noop_compress.decompress(b'') == b''
    body = b"this is noop test data"
    compress_body = b"this is noop test data"
    assert noop_compress.compress(body) == compress_body
    assert noop_compress.decompress(compress_body) == body

    # zlib
    zlib_compress = get_compress(CompressType.ZLib)
    assert zlib_compress.compress(b'') == b''
    assert zlib_compress.decompress(b'') == b''
    body = b"this is zlib test data"
    compress_body = b"x\x9c+\xc9\xc8,V\x00\xa2\xaa\x9c\xcc$\x85\x92\xd4\xe2\x12\x85\x94\xc4\x92D\x00]\xdb\x08 "
    assert zlib_compress.compress(body) == compress_body
    assert zlib_compress.decompress(compress_body) == body

    # gzip
    gzip_compress = get_compress(CompressType.Gzip)
    assert gzip_compress.compress(b'') == b''
    assert gzip_compress.decompress(b'') == b''
    body = b"this is gzip test data"
    # pylint: disable=
    compress_body = b"\x1f\x8b\x08\x00\x90R\xce^\x02\xff+\xc9\xc8,V\x00\xa2\xf4\xaa\xcc\x02\x85\x92" \
                    b"\xd4\xe2\x12\x85\x94\xc4\x92D\x00\xf7R\x15<\x16\x00\x00\x00"
    assert gzip_compress.decompress(compress_body) == body
    # gzip压缩与时间相关，不可重复输出（python3.8以后可以指定mtime）
    assert gzip_compress.compress(body) != compress_body

    # snappy
    snappy_compress = get_compress(CompressType.Snappy)
    assert snappy_compress.compress(b'') == b''
    assert snappy_compress.decompress(b'') == b''
    body = b"this is snappy test data"
    compress_body = b'\x18\\this is snappy test data'
    assert snappy_compress.compress(body) == compress_body
    assert snappy_compress.decompress(compress_body) == body
