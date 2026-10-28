# Tencent is pleased to support the open source community by making tRPC-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Python is licensed under Apache-2.0.

import asyncio
from client import test_http_client


async def test_client():
    await test_http_client()


if __name__ == "__main__":
    asyncio.run(test_client())
