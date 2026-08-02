"""Test meme share"""
import multiprocessing

from trpc_sdk.share_mem.share_mem import RegisterStatus
from trpc_sdk.share_mem.share_mem import dec_io_wait_count
from trpc_sdk.share_mem.share_mem import get_register_status
from trpc_sdk.share_mem.share_mem import register_enable
from trpc_sdk.share_mem.share_mem import register_spinlock
from trpc_sdk.share_mem.share_mem import remove_lock_file
from trpc_sdk.share_mem.share_mem import set_register_status
from trpc_sdk.share_mem.share_mem import wait_notify_in_io
from trpc_sdk.share_mem.share_mem import warm_up


def test_get_register_status():
    """Test register"""
    warm_up()
    assert get_register_status() == 0
    remove_lock_file()


def test_set_register_status():
    """Test set register"""
    warm_up()
    set_register_status(0)
    assert get_register_status() == 0
    set_register_status(1)
    assert get_register_status() == 1
    remove_lock_file()


def test_register_enable():
    """Test register enable"""
    warm_up()
    set_register_status(0)
    register_enable()
    assert get_register_status() == RegisterStatus.register.value
    remove_lock_file()


def test_register_loop():
    """Test register loop"""
    register_enable()
    register_spinlock()
    remove_lock_file()


def _func(interval: int):
    if not interval:
        print("wait for dispatcher")
        wait_notify_in_io(3)
        print("wait for dispatcher end")
        return
    print("dec_io_wait_count")
    dec_io_wait_count()


def test_dispatcher_wait():
    """Test dispatcher wait"""
    with multiprocessing.Pool(5) as p:
        p.map(_func, [0, 1, 2, 3])
