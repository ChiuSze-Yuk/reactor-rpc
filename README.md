# 轻量级 C++ RPC 服务端（epoll ET + 线程池）

单进程 TCP RPC 服务端：epoll 边缘触发（ET）+ 固定大小线程池的 Reactor 模型，
消息以 `\n` 分帧，协议为 JSON-RPC 2.0 子集。不使用 muduo / Boost.Asio 等网络框架，
仅依赖 C++11 标准库与 header-only 的 `nlohmann/json`（服务端为单文件实现）。

[![build-and-test](https://github.com/jacobSze-Yukchiu/reactor-rpc/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/jacobSze-Yukchiu/reactor-rpc/actions/workflows/build-and-test.yml)

```text
主线程（Reactor）
  epoll_wait
    ├─ listen fd  -> accept 循环读到 EAGAIN -> 新 fd 注册 epoll
    └─ client fd  -> 把“读取 + 处理”任务丢进 thread pool

worker（固定数量）
  recv 循环读到 EAGAIN
  -> 按 '\n' 从每连接 input_buffer 切出完整 JSON
  -> json::parse -> validate -> dispatch_request
  -> send 响应 -> EPOLL_CTL_MOD 重新注册（rearm）
```

## 1. 关键设计

| 设计 | 作用 |
|---|---|
| 所有 socket 设 `O_NONBLOCK` | Reactor 不会卡在 `accept` / `recv` / `send` |
| `EPOLLET`（边缘触发） | 每次事件必须 `recv` / `accept` 到 `EAGAIN`，否则会漏事件 |
| `EPOLLONESHOT` | 同一 fd 一次只交给一个 worker，避免 `input_buffer` 竞态与响应乱序 |
| `epoll_event.data.u64` 存 connection id | fd 关闭后可能被新连接重用，用 id 避免旧事件误命中新连接 |
| 每连接 `input_buffer` | TCP 是字节流，解决粘包（多个 JSON 粘在一起）与半包（JSON 被拆开） |
| 固定大小线程池 | 避免每个请求建立 thread 的开销；主线程只做 epoll_wait / 分发 |
| 半关闭处理 | 收到 `recv()==0` 或 `EPOLLRDHUP` 时，先处理完缓冲区内完整请求并回包，再关闭 |
| 加锁日志 | 多个 worker 同时输出时不会把日志行交错 |
| 每连接 1 MiB 接收缓冲上限 | 防止恶意客户端无限发送数据耗尽内存；超过回 `-32600` 并关闭 |

工作线程数量默认为 `std::thread::hardware_concurrency()`，也可用命令行参数指定
（范围 1–256）。IO 密集型服务通常可设为 CPU 核心数的 1–2 倍。

## 2. 协议格式

一条消息 = **紧凑 JSON + `\n`**。发送时必须使用 `dump()`，不能用 `dump(4)`；
pretty-print 产生的换行会被当成消息边界。

请求：

```json
{"jsonrpc":"2.0","id":1,"method":"math.add","params":[12,30]}
```

成功响应：

```json
{"jsonrpc":"2.0","id":1,"result":42,"error":null}
```

错误响应（本项目同时保留 `result` 和 `error` 字段，方便客户端统一解析）：

```json
{"jsonrpc":"2.0","id":2,"result":null,"error":{"code":-32601,"message":"Method not found","data":{"method":"math.div"}}}
```

| 错误码 | 含义 |
|---:|---|
| `-32700` | JSON 解析失败 |
| `-32600` | 请求结构非法（含每连接接收缓冲超过 1 MiB） |
| `-32601` | 方法不存在 |
| `-32602` | 参数数量或类型错误 |
| `-32603` | 服务端内部错误 |

## 3. 依赖

`nlohmann/json` 是 header-only 库。本目录已包含 `nlohmann/json.hpp`，
编译时加 `-I.` 即可；也可以安装系统软件包：

```bash
# Ubuntu / Debian
sudo apt install nlohmann-json3-dev

# macOS（Homebrew）
brew install nlohmann-json
```

## 4. 编译

Linux（epoll 是 Linux 专有 API）：

```bash
g++ -std=c++11 -O2 -Wall -Wextra -pedantic -I. \
  rpc_server.cpp -o rpc_server -pthread

g++ -std=c++11 -O2 -Wall -Wextra -pedantic -I. \
  rpc_client.cpp -o rpc_client
```

`-pthread` 不能省略：`std::thread`、`std::mutex`、`std::condition_variable`
都需要它。macOS / Windows 没有 `sys/epoll.h`，请在 Linux、WSL 或 Docker 中编译运行。

## 5. 运行

```bash
./rpc_server          # 工作线程 = CPU 核心数
./rpc_server 4        # 指定 4 个 worker
RPC_VERBOSE=1 ./rpc_server 4   # 打印每个请求与连接生命周期
```

终端二发送一个请求：

```bash
./rpc_client 127.0.0.1 8080
```

## 6. 测试高并发效果

项目附带 `bench_client.py`，会建立多条连接、校验每个响应的 `id` 和 `result`，
所以“跑得快”之外也能确认没有串线、丢包或顺序错乱。

### 6.1 keep-alive（一问一答，多连接并行）

```bash
python3 bench_client.py -c 32 -n 100
```

`-c` 是连接数，`-n` 是每条连接的请求数。这种模式贴近一般 RPC 客户端：
连接建立后重复“发一个请求 -> 等一个响应”。

### 6.2 pipeline（一次发多个请求，测试粘包）

```bash
python3 bench_client.py -c 32 -n 100 --pipeline
```

每条连接把 100 个请求一次 `sendall`，服务端必须靠 `input_buffer` 切成 100 条，
再按顺序回包。这个模式通常比 keep-alive 吞吐高很多，因为省掉了大量往返延迟。

### 6.3 连接突发（测试 accept + ET）

```bash
python3 bench_client.py -c 200 -n 1
```

200 条连接同时建立，每条只发一个请求，主要观察 listen fd 的 ET accept 循环、
`EPOLLONESHOT` 分发，以及 server 是否会漏掉连接。

### 6.4 协议边界测试（半包 / 粘包 / 错误码 / 超限）

项目附带 `wire_test.py`，直接操作 TCP socket，不依赖 `bench_client.py` 的压测流程。
先启动服务端，再执行：

```bash
python3 wire_test.py
python3 wire_test.py --host 127.0.0.1 --port 8080
```

它会逐项检查：

- 基本 `math.add` 成功响应；
- JSON 解析错误 `-32700`；
- 未知方法 `-32601`，且 `data.method` 正确；
- 参数数量/类型错误 `-32602`；
- 半包（逐字节发送）；
- 粘包（一次发送多个请求）；
- 客户端 `shutdown(SHUT_WR)` 半关闭后仍收到响应；
- 1.1 MiB 无换行超限时回 `-32600` 并关闭连接。

全部通过时返回 0，任何一项失败返回 1；适合放进 CI。

### 6.5 对比不同 worker 数量

```bash
for t in 1 2 4 8; do
  ./rpc_server "$t" >"/tmp/rpc_server_$t.log" 2>&1 &
  pid=$!
  sleep 1
  echo "--- workers=$t ---"
  python3 bench_client.py -c 32 -n 100
  kill "$pid"
  wait "$pid" 2>/dev/null
done
```

示例输出（实际数字依机器、核心数与客户端性能而异）：

```text
mode=keep-alive connections=32 requests/conn=100 requested=3200
completed=3200 errors=0 wall=0.205s throughput=15638.3 req/s
latency p50=1.783ms p95=3.511ms max=5.879ms
PASS
```

预期现象：

- `completed == requested` 且 `errors=0`、最后打印 `PASS`。
- worker 从 1 增加到 CPU 核心数时，keep-alive 吞吐通常会上升；
  超过核心数后收益变小，甚至因上下文切换而下降。
- `--pipeline` 的吞吐明显高于 keep-alive，但单条连接的响应延迟会变大，
  因为一个 worker 会连续处理整批消息。
- 客户端是 Python 多线程，受 GIL 限制；要测更极限的吞吐可用多进程客户端
  或 C++ 压测工具。服务端本身的并发能力不以这个数字为唯一标准。

### 6.6 观察日志与线程

```bash
RPC_VERBOSE=1 ./rpc_server 4
```

verbose 模式会打印 accept / close、每个请求的解析结果与分派结果。
压测时请保持默认（关闭），否则日志 I/O 会成为瓶颈。

```bash
top -H -p "$(pgrep -n rpc_server)"     # 查看 worker CPU 使用
ss -ltnp | grep 8080                   # 查看监听与连接（Linux）
```

## 7. 性能实测

测试环境：2 核 / 2 GiB Linux 云服务器，服务端与压测客户端同机运行；客户端为
`bench_client.py` 的 Python 多线程实现，受 GIL 限制，因此下表的数字更能说明
“服务端不成为瓶颈”，而不是服务端的吞吐上限。

| 场景 | 请求数 | 耗时 | 吞吐 | p50 | p95 | max | 错误 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| keep-alive（64 连接 × 200） | 12,800 | 1.100 s | 11,641.7 req/s | 4.206 ms | 13.098 ms | 41.277 ms | 0 |
| pipeline（64 连接 × 200） | 12,800 | 0.152 s | 84,470.4 req/s | 35.508 ms | 62.657 ms | 64.442 ms | 0 |

原始输出见 `keep_alive_report.txt`、`pipeline_report.txt`；协议边界与防御测试的
原始输出见 `wire_test_report.txt`（9/9 PASS）。

怎么读这两行数字：

- **pipeline 的 84,470.4 req/s 不等于“单个请求更快”**：一次 `sendall` 把 200 个请求
  一起推给内核，省掉了 12,800 次网络往返，代价是同一条连接内的响应排队——
  p50 从 4.2 ms 涨到 35.5 ms。
- 两种模式下客户端都逐条校验响应的 `id` 与 `result`，`errors=0` 说明没有串线、
  丢包或顺序错乱，而不只是“看起来快”。
- 换机器、换 worker 数量、换客户端实现都会改变这些数字；仓库里的报告文件是
  同一台机器、同一次运行的原始输出。

## 8. 设计取舍与目前限制

1. **worker 内的 `send` 可能等待**：发送缓冲区满时会 `poll(POLLOUT)` 最多 1 秒。
   少数“只连接不读响应”的慢客户端可能暂时占住 worker。
   生产级实现通常改成每连接输出队列 + `EPOLLOUT`，让 worker 立即返回。
2. **没有空闲连接超时**：恶意客户端可以建立大量空连接。可加入 last-active 时间
   与定时清扫。
3. **没有 TLS / 认证 / 限流**：目前定位为单机自用与压测，不适合直接暴露公网。
4. **单条连接的批量上限**：一次事件中若缓冲区有很多完整消息，会由同一个 worker
   连续处理；这保证顺序，但可能让该 worker 在这批消息期间忙碌。
5. **停机是“排空式”的**：收到 `SIGINT` / `SIGTERM` 后先停止 epoll 循环、不再接收新连接，
   线程池把队列里剩余任务跑完再 `join`；队列积压较多时退出需要等一会儿，且不会通知客户端。
6. **任务队列没有上限**：极端流量下若 worker 跟不上，主线程分发的任务会持续堆积；
   生产环境应加入队列长度上限或背压策略。
7. **超限关闭是尽力而为**：送出 `-32600` 后会有限量 drain 已到达的输入数据，
   降低 close 时因未读数据触发 RST 而丢失错误响应的概率；若客户端持续灌数据，
   仍可能在 close 时收到 RST。

## 9. 常见问题

**`sys/epoll.h: No such file or directory`**

epoll 是 Linux 专有 API。请在 Linux、WSL2 或 Docker 中编译；macOS 的 kqueue
不能直接替代，本项目源码不包含跨平台兼容层。

**`bind` 失败：`Address already in use`**

先找占用端口的进程：

```bash
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

服务端已设置 `SO_REUSEADDR`，正常重启不会被 `TIME_WAIT` 卡住；若仍有旧进程，
结束它再启动。

**客户端一直等不到响应**

- 请求必须以 `\n` 结尾，且发送时要用 `dump()`，不能用 `dump(4)`。
- 检查服务端是否打印 `-32700` / `-32600`。
- 若每连接接收缓冲超过 1 MiB，服务端会回 `-32600` 并关闭连接；可用 `wire_test.py` 验证。

**链接时出现 `undefined reference to pthread_create`**

编译命令漏了 `-pthread`。

## 10. 下一步可扩展方向

1. 每连接输出缓冲区 + `EPOLLOUT`，彻底避免 worker 等待慢客户端。
2. 空闲连接超时与心跳。
3. 请求级超时与取消：给长耗时业务设置 deadline。
4. 请求计数、延迟直方图、当前连接数等 metrics。
5. TLS、认证、按 IP 限流。

---

> **开发说明**：本项目自 2026-09 起在本地迭代，2026-11 整理后上传到 GitHub；
> 压测报告为整理前的原始输出，未做任何修饰。
