#!/bin/sh

SUCCESS=0
# Shell exit status is unsigned (0-255). Use 255 to represent "-1".
FAILURE=255
ret=$SUCCESS
server_pid=

cleanup() {
    if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null
        wait "$server_pid" 2>/dev/null
    fi
}

trap cleanup EXIT INT TERM

check_server_ready() {
    python3 - <<'PY'
import socket
import sys

tcp_addrs = [
    ("127.0.0.1", 8000),
    ("127.0.0.1", 8001),
    ("127.0.0.1", 8002),
    ("127.0.0.1", 8003),
]
unix_paths = ["/tmp/trpc.sock"]

for host, port in tcp_addrs:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.2)
    try:
        sock.connect((host, port))
    except OSError:
        sys.exit(1)
    finally:
        sock.close()

for path in unix_paths:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(0.2)
    try:
        sock.connect(path)
    except OSError:
        sys.exit(1)
    finally:
        sock.close()

sys.exit(0)
PY
}

wait_server_ready() {
    retry=0
    max_retry=100
    while [ "$retry" -lt "$max_retry" ]; do
        if ! kill -0 "$server_pid" 2>/dev/null; then
            return 1
        fi
        if check_server_ready; then
            return 0
        fi
        retry=$((retry + 1))
        sleep 0.1
    done
    return 1
}

rm -f ./*.log
rm -f /tmp/trpc.sock

PYTHONWARNINGS="ignore:resource_tracker:UserWarning:multiprocessing.resource_tracker" \
    python3 -m examples.helloworld.main &
server_pid=$!

if wait_server_ready; then
    python3 -m examples.helloworld.test_service_rpc || ret=$FAILURE
else
    echo "examples.helloworld server failed to become ready" >&2
    ret=$FAILURE
fi

echo "run_example.sh ret: $ret"

exit $ret

# python3 examples/helloworld/trpc_main.py > trpc_out.log 2>&1 &
#python3 -m examples.hellostream.trpc_main
#python3 -m examples.hellostream.test_service_rpc
