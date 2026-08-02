import multiprocessing

from trpc_sdk.naming.registry import pause_heartbeat
from trpc_sdk.naming.registry import resume_heartbeat
from trpc_sdk.naming.registry import should_heartbeat


def handler():
    resume_heartbeat()


def test_heartbeat():
    # Heartbeat counter uses module-level multiprocessing.Value. That only
    # shares state across children when start method is "fork", which matches
    # trpc server startup (see trpc/server/server.py). Python 3.14 defaults to
    # forkserver on Linux, where each child re-imports registry with counter=0.
    ctx = multiprocessing.get_context('fork')
    num = 2
    pause_heartbeat()
    pause_heartbeat()
    assert not should_heartbeat()

    processes = [ctx.Process(target=handler) for _ in range(num)]
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    assert should_heartbeat()
