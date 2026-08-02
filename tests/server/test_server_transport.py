# -*- coding: utf-8 -*-
"""Test transport module"""
import threading
import time

from tests.common_util import unused_port
from tests.transport_util import TestChecker
from trpc_sdk.server import option
from trpc_sdk.server import server_engine
from trpc_sdk.transport import server_transport


def test_server_transport():
    """Test server transport"""
    engine = server_engine.TrpcServerEngine(option.ServerOption())
    engine.init_processes()
    transport = server_transport.TrpcServerTransport()
    trans_option = server_transport.DefaultServerTransportOption()
    trans_option.network = "tcp"
    trans_option.address = "127.0.0.1"
    trans_option.port = unused_port()
    try:
        transport.listen_and_serve(trans_option, engine)
    except Exception:  # pylint: disable=broad-except
        assert False

    threading.Thread(target=engine.start_processes(), daemon=True).start()
    time.sleep(1)
    try:
        transport.close()
    except Exception as ex:  # pylint: disable=broad-except
        print("receive exception when close transport ", str(ex))
        assert False


def test_server_transport_udp():
    """Test server transport udp"""
    engine = server_engine.TrpcServerEngine(option.ServerOption())
    engine.process_info.add_checker(TestChecker())
    engine.init_processes()
    transport = server_transport.TrpcServerTransport()
    trans_option = server_transport.DefaultServerTransportOption()
    trans_option.network = "udp"
    trans_option.address = "127.0.0.1"
    trans_option.port = unused_port()

    try:
        transport.listen_and_serve(trans_option, engine)
    except Exception:  # pylint: disable=broad-except
        assert False

    threading.Thread(target=engine.start_processes()).start()
    time.sleep(1)
    try:
        transport.close()
    except Exception as ex:  # pylint: disable=broad-except
        print("receive exception when close transport ", str(ex))
        assert False

    assert not engine.find_child_process(1)

    engine.restart_child_process(1)
