from trpc_sdk import utils
from trpc_sdk.pool import threadpool


def test():
    return 1


@utils.trpc_async_pytest
async def test_send_to_task():
    threadpool.GlobalThreadPool = None
    ret = await threadpool.submit(test)
    assert ret == 1
