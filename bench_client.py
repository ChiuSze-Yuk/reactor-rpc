#!/usr/bin/env python3
"""第三阶段 RPC 服务端压测工具。

用法：
    python3 bench_client.py                          # 32 连接 x 100 次 keep-alive 请求
    python3 bench_client.py -c 64 -n 200             # 64 连接 x 200 次
    python3 bench_client.py -c 32 -n 100 --pipeline  # 每连接一次发 100 个请求（粘包）

每条连接使用独立的 id，并校验 result，确保高并发下响应没有串线或丢失。
"""
import argparse
import json
import socket
import sys
import threading
import time


def recv_line(sock, buffer):
    """从 socket 读出一行 JSON；返回 (object, 剩余缓冲区)。"""
    while b"\n" not in buffer:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("connection closed before newline")
        buffer += chunk
    line, buffer = buffer.split(b"\n", 1)
    return json.loads(line.decode("utf-8")), buffer


def make_request(req_id, a, b):
    """构造紧凑单行 JSON；dump(4) 的换行会破坏消息边界。"""
    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "math.add",
        "params": [a, b],
    }
    return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")


class Result:
    def __init__(self):
        self.lock = threading.Lock()
        self.errors = []
        self.latencies = []
        self.completed = 0

    def add_error(self, message):
        with self.lock:
            if len(self.errors) < 20:
                self.errors.append(message)

    def add_latency(self, seconds, count):
        with self.lock:
            self.latencies.append(seconds)
            self.completed += count


def worker(worker_id, args, barrier, result):
    sock = None
    try:
        sock = socket.create_connection((args.host, args.port), timeout=args.timeout)
        sock.settimeout(args.timeout)

        if args.pipeline:
            ids = [
                worker_id * (args.requests + 1) + i
                for i in range(args.requests)
            ]
            payload = b"".join(
                make_request(req_id, i, 1) for i, req_id in enumerate(ids)
            )
            barrier.wait()
            started = time.perf_counter()
            sock.sendall(payload)

            buffer = b""
            processed = 0
            for i, req_id in enumerate(ids):
                response, buffer = recv_line(sock, buffer)
                processed += 1
                expected = i + 1
                if response.get("id") != req_id or response.get("result") != expected:
                    result.add_error(
                        "pipeline worker=%d id=%d got=%r expected=%d"
                        % (worker_id, req_id, response, expected)
                    )
                    break
            result.add_latency(time.perf_counter() - started, processed)
        else:
            buffer = b""
            barrier.wait()
            for i in range(args.requests):
                req_id = worker_id * (args.requests + 1) + i
                started = time.perf_counter()
                sock.sendall(make_request(req_id, i, worker_id))
                response, buffer = recv_line(sock, buffer)
                elapsed = time.perf_counter() - started
                expected = i + worker_id
                if response.get("id") != req_id or response.get("result") != expected:
                    result.add_error(
                        "keep-alive worker=%d id=%d got=%r expected=%d"
                        % (worker_id, req_id, response, expected)
                    )
                    break
                result.add_latency(elapsed, 1)
    except Exception as error:  # 连接失败时要叫醒其他等待 barrier 的线程
        result.add_error("worker=%d exception: %s" % (worker_id, error))
        try:
            barrier.abort()
        except Exception:
            pass
    finally:
        if sock is not None:
            sock.close()


def parse_args():
    parser = argparse.ArgumentParser(description="RPC 服务端并发压测")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("-c", "--connections", type=int, default=32,
                        help="并发连接数（默认 32）")
    parser.add_argument("-n", "--requests", type=int, default=100,
                        help="每条连接的请求数（默认 100）")
    parser.add_argument("--pipeline", action="store_true",
                        help="每条连接一次发出全部请求，测试粘包/流水线")
    parser.add_argument("--timeout", type=float, default=10.0,
                        help="socket 超时秒数（默认 10）")
    args = parser.parse_args()
    if args.connections <= 0 or args.requests <= 0:
        parser.error("connections 和 requests 必须大于 0")
    return args


def percentile(sorted_values, fraction):
    if not sorted_values:
        return 0.0
    index = int(len(sorted_values) * fraction)
    if index >= len(sorted_values):
        index = len(sorted_values) - 1
    return sorted_values[index]


def main():
    args = parse_args()
    result = Result()
    barrier = threading.Barrier(args.connections)
    threads = []

    wall_start = time.perf_counter()
    try:
        for worker_id in range(args.connections):
            thread = threading.Thread(
                target=worker, args=(worker_id, args, barrier, result)
            )
            thread.start()
            threads.append(thread)
    except Exception as error:
        result.add_error("failed to start threads: %s" % error)
        try:
            barrier.abort()
        except Exception:
            pass

    for thread in threads:
        thread.join()
    wall_elapsed = time.perf_counter() - wall_start

    requested = args.connections * args.requests
    mode = "pipeline" if args.pipeline else "keep-alive"
    print(
        "mode=%s connections=%d requests/conn=%d requested=%d"
        % (mode, args.connections, args.requests, requested)
    )
    print(
        "completed=%d errors=%d wall=%.3fs throughput=%.1f req/s"
        % (
            result.completed,
            len(result.errors),
            wall_elapsed,
            result.completed / wall_elapsed if wall_elapsed > 0 else 0.0,
        )
    )

    if result.latencies:
        values = sorted(result.latencies)
        print(
            "latency p50=%.3fms p95=%.3fms max=%.3fms"
            % (
                percentile(values, 0.50) * 1000.0,
                percentile(values, 0.95) * 1000.0,
                values[-1] * 1000.0,
            )
        )

    if result.errors:
        print("first errors:")
        for error in result.errors[:5]:
            print("  " + error)
        return 1

    if result.completed != requested:
        print("FAIL: completed != requested")
        return 1

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
