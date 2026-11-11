#!/usr/bin/env python3
"""Protocol boundary tests for the RPC server.

Usage:
    ./rpc_server 4
    python3 wire_test.py
    python3 wire_test.py --host 127.0.0.1 --port 8080

The script talks to the server over a raw TCP socket and checks both
successful responses and JSON-RPC error responses. It does not start the
server automatically.
"""

import argparse
import json
import socket
import sys
import time


BUFFER_SIZE = 4096
DEFAULT_TIMEOUT = 5.0


class WireClient:
    """Small helper that keeps the leftover bytes after a newline."""

    def __init__(self, host, port, timeout=DEFAULT_TIMEOUT):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buffer = b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def send(self, data):
        self.sock.sendall(data)

    def send_ignore_disconnect(self, data):
        """Best-effort send used by the oversize test.

        The server may close the connection while the client is still
        writing the oversized payload, so a reset here is expected.
        """
        try:
            self.sock.sendall(data)
            return True
        except OSError:
            return False

    def recv_line(self):
        while b"\n" not in self.buffer:
            chunk = self.sock.recv(BUFFER_SIZE)
            if not chunk:
                raise ConnectionError(
                    "server closed connection before sending a complete line"
                )
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        return json.loads(line.decode("utf-8"))

    def request(self, method, params, request_id):
        self.send(build_request(method, params, request_id) + b"\n")
        return self.recv_line()


def build_request(method, params, request_id):
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params,
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def assert_equal(actual, expected, label):
    if actual != expected:
        raise AssertionError("%s: expected %r, got %r" %
                             (label, expected, actual))


def error_code(response):
    error = response.get("error")
    if not isinstance(error, dict):
        return None
    return error.get("code")


def error_data(response):
    error = response.get("error")
    if not isinstance(error, dict):
        return {}
    data = error.get("data")
    return data if isinstance(data, dict) else {}


def test_basic(host, port):
    client = WireClient(host, port)
    try:
        response = client.request("math.add", [12, 30], 1)
        assert_equal(response.get("id"), 1, "basic id")
        assert_equal(response.get("result"), 42, "basic result")
    finally:
        client.close()


def test_parse_error(host, port):
    client = WireClient(host, port)
    try:
        # A newline ends this deliberately invalid JSON object.
        client.send(b'{"jsonrpc":"2.0",\n')
        response = client.recv_line()
        assert_equal(error_code(response), -32700, "parse error code")
    finally:
        client.close()


def test_unknown_method(host, port):
    client = WireClient(host, port)
    try:
        response = client.request("math.div", [1, 2], 2)
        assert_equal(error_code(response), -32601, "unknown method code")
        assert_equal(error_data(response).get("method"), "math.div",
                     "unknown method data.method")
    finally:
        client.close()


def test_invalid_params_count(host, port):
    client = WireClient(host, port)
    try:
        response = client.request("math.add", [1], 3)
        assert_equal(error_code(response), -32602, "params count code")
    finally:
        client.close()


def test_invalid_params_type(host, port):
    client = WireClient(host, port)
    try:
        response = client.request("math.add", [1, "x"], 4)
        assert_equal(error_code(response), -32602, "params type code")
    finally:
        client.close()


def test_partial_packet(host, port):
    client = WireClient(host, port)
    try:
        payload = build_request("math.add", [12, 30], 5) + b"\n"
        for byte in payload:
            client.send(bytes([byte]))
            time.sleep(0.001)

        response = client.recv_line()
        assert_equal(response.get("id"), 5, "partial id")
        assert_equal(response.get("result"), 42, "partial result")
    finally:
        client.close()


def test_sticky_packets(host, port):
    client = WireClient(host, port)
    try:
        payload = (
            build_request("math.add", [1, 2], 6) + b"\n" +
            build_request("math.add", [3, 4], 7) + b"\n"
        )
        client.send(payload)

        first = client.recv_line()
        second = client.recv_line()

        assert_equal(first.get("id"), 6, "sticky first id")
        assert_equal(first.get("result"), 3, "sticky first result")
        assert_equal(second.get("id"), 7, "sticky second id")
        assert_equal(second.get("result"), 7, "sticky second result")
    finally:
        client.close()


def test_half_close(host, port):
    client = WireClient(host, port)
    try:
        client.send(build_request("math.add", [12, 30], 8) + b"\n")
        client.sock.shutdown(socket.SHUT_WR)

        response = client.recv_line()
        assert_equal(response.get("id"), 8, "half-close id")
        assert_equal(response.get("result"), 42, "half-close result")
    finally:
        client.close()


def test_oversize(host, port):
    client = WireClient(host, port, timeout=10.0)
    try:
        # 1.1 MiB without a newline: the server must reject it once the
        # per-connection input buffer exceeds the limit. The extra data also
        # exercises the best-effort drain before close.
        payload = b"x" * ((11 * 1024 * 1024) // 10)
        client.send_ignore_disconnect(payload)

        response = client.recv_line()
        assert_equal(error_code(response), -32600, "oversize error code")

        # The server should close after sending -32600. A reset is acceptable.
        client.sock.settimeout(2.0)
        try:
            remaining = client.sock.recv(BUFFER_SIZE)
            assert_equal(remaining, b"", "oversize connection should be closed")
        except socket.timeout:
            raise AssertionError(
                "server did not close connection after sending -32600"
            )
        except ConnectionResetError:
            pass
    finally:
        client.close()


TESTS = (
    ("basic math.add", test_basic),
    ("JSON parse error -32700", test_parse_error),
    ("unknown method -32601", test_unknown_method),
    ("invalid params count -32602", test_invalid_params_count),
    ("invalid params type -32602", test_invalid_params_type),
    ("partial packet", test_partial_packet),
    ("sticky packets", test_sticky_packets),
    ("half close", test_half_close),
    ("oversize > 1 MiB", test_oversize),
)


def main():
    parser = argparse.ArgumentParser(
        description="Run RPC protocol boundary tests against a running server."
    )
    parser.add_argument("--host", default="127.0.0.1",
                        help="server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080,
                        help="server port (default: 8080)")
    args = parser.parse_args()

    passed = 0
    failed = 0

    for name, test in TESTS:
        try:
            test(args.host, args.port)
            print("PASS  " + name)
            passed += 1
        except Exception as exc:  # test harness: report and continue
            print("FAIL  %s: %s" % (name, exc))
            failed += 1

    print("%d/%d tests passed" % (passed, passed + failed))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
