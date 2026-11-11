// =============================================================================
// rpc_server.cpp
//
// RPC 服务端：epoll(ET) + 线程池的 Reactor 高并发模型
//
//   1. 监听 socket 与客户端 socket 全部设为非阻塞（O_NONBLOCK）。
//   2. 用 epoll_create1 / epoll_ctl / epoll_wait 做 IO 多路复用，
//      统一使用边缘触发（EPOLLET），并搭配 EPOLLONESHOT 避免多线程重复处理。
//   3. epoll_event.data.u64 存单调递增的 connection id（0 保留给监听 socket），
//      避免 fd 关闭后被重用时，旧事件误作用到新连接。
//   4. 主线程（Reactor）只负责 epoll_wait、accept、分发任务，不做耗时工作。
//   5. 每条连接维护应用层接收缓冲区：recv 循环读到 EAGAIN 为止，
//      再按 '\n' 切割消息，解决 TCP 粘包/半包。
//   6. JSON 解析与业务分派（dispatch_request）交给线程池并行处理。
//   7. 路由表、math_add 等业务函数在 worker 中执行，错误统一转 JSON-RPC 错误码。
//
// 编译环境：Linux
// 编译命令：
//   g++ -std=c++11 -O2 -Wall -Wextra -pedantic -I. rpc_server.cpp -o rpc_server -pthread
// =============================================================================

#include <nlohmann/json.hpp>
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <climits>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <queue>
#include <stdexcept>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <utility>
#include <vector>

// 给 nlohmann::json 起一个短名字，后面的代码会更清楚。
using json = nlohmann::json;

namespace {

// 服务端固定监听 8080 端口。
const int kPort = 8080;

// epoll_event.data.u64 的约定：0 代表监听 socket，其余是 Connection::id。
// 使用 id 而不是 fd，避免 fd 被重用后旧事件命中新连接。
const std::uint64_t kListenToken = 0;

// 每条连接接收缓冲区的最大长度，避免恶意客户端无限发送数据耗尽内存。
// 注意：这是“整个 input_buffer”的上限，不是单条 JSON 的上限。
// 超过时服务端回 -32600 并关闭连接，不再 rearm。
const std::size_t kMaxMessageBytes = 1024 * 1024;  // 1 MiB

// 超限关闭前，最多丢弃多少已到达的输入数据。
// 目的：尽量排空内核接收缓冲，避免 close 时因“有未读数据”触发 RST，
// 让刚刚送出的 -32600 响应丢失。这只是 best-effort，不解析、不无限读。
const std::size_t kMaxDrainBytes = 1024 * 1024;  // 1 MiB

// epoll_wait 一次最多取回的事件数。
const int kMaxEvents = 128;

// Reactor 主循环在 epoll_wait 的最长等待时间（毫秒）。
// 不用 -1 无限阻塞，而是用有限超时：glibc 的 signal() 可能带 SA_RESTART，
// 让 epoll_wait 自动重启而不回 EINTR；有限超时能保证停止信号最终被检查到。
const int kEpollWaitTimeoutMs = 1000;

// listen() 的等待队列长度。取较大值，让突发连接在 accept 队列里排得下，
// 减少因队列满导致的 SYN 丢弃与客户端重连。
const int kListenBacklog = 512;

// 每次 recv 使用的缓冲区大小。
const std::size_t kReadBufferSize = 4096;

// 发送遇到 EAGAIN 时，最多等待可写的时间（毫秒）。
const int kSendTimeoutMs = 1000;

// 工作线程数量上限，避免命令行参数过大耗尽系统资源。
const std::size_t kMaxWorkerThreads = 256;

// 客户端 socket 注册到 epoll 时使用的事件：
//   EPOLLIN      : 有数据可读
//   EPOLLET      : 边缘触发，必须一次读到 EAGAIN
//   EPOLLONESHOT : 一次事件只唤醒一个 worker，处理完再手动 rearm，
//                  避免同一连接被多个线程同时 recv/parse/send
//   EPOLLRDHUP   : 对端关闭写端（half-close）时也能收到通知
#ifdef EPOLLRDHUP
const std::uint32_t kClientEvents =
    EPOLLIN | EPOLLET | EPOLLONESHOT | EPOLLRDHUP;
#else
const std::uint32_t kClientEvents = EPOLLIN | EPOLLET | EPOLLONESHOT;
#endif

// 日志会被多个 worker 同时使用，必须加锁避免行与行交错。
std::mutex g_log_mutex;

// 是否打印每个请求。高并发压测时建议关闭（默认关闭），
// 可通过环境变量 RPC_VERBOSE=1 打开。
bool g_verbose = false;

// 收到 SIGINT/SIGTERM 时设置此标志位，让 Reactor 主循环优雅退出，
// 进而正常触发 RpcServer 的析构流程（join worker、关闭连接）。
// sig_atomic_t + volatile 是“信号处理器写、主线程读”的标准安全用法。
volatile std::sig_atomic_t g_stop_requested = 0;

void handle_stop_signal(int /*signum*/) {
    g_stop_requested = 1;
}

void log_line(const std::string& text) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    // endl 会 flush；否则日志重定向到文件时，进程被 kill 可能看不到最后几行。
    std::cout << text << std::endl;
}

void log_error(const std::string& text) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cerr << text << "\n";
}

// JSON-RPC 错误码。负数区间沿用 JSON-RPC 2.0 的惯例。
enum RpcErrorCode {
    kParseError = -32700,      // JSON 本身无法解析
    kInvalidRequest = -32600,  // JSON 可以解析，但缺少必要字段或字段类型错误
    kMethodNotFound = -32601,  // 路由表里找不到这个 method
    kInvalidParams = -32602,   // 方法存在，但 params 数量或类型不对
    kInternalError = -32603    // 服务端内部错误
};

// 关闭 socket 的小封装。传入 -1 时直接忽略，方便错误处理。
void close_socket(int fd) {
    if (fd >= 0) {
        ::close(fd);
    }
}

// 把 fd 设为非阻塞。本项目所有 socket 都必须是非阻塞的：
//   - 监听 socket：accept 循环才能靠 EAGAIN 停下来；
//   - 客户端 socket：recv/send 才能靠 EAGAIN 判断“这次读/写完了”。
bool set_nonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

// 非阻塞版本的 send_all。
//
// 这里的 socket 是 O_NONBLOCK：
//   - send 可能只送出一部分，所以要循环；
//   - 内核发送缓冲区满时 send 会返回 EAGAIN/EWOULDBLOCK，
//     此时用 poll(POLLOUT) 等一小段时间再重试，避免直接丢弃响应。
//
// 这个函数只会被 worker 线程调用，所以即使 poll 等待也不会卡住 Reactor。
bool send_all_nonblocking(int fd, const std::string& data) {
    std::size_t sent = 0;

    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);

        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            pollfd pfd;
            pfd.fd = fd;
            pfd.events = POLLOUT;
            pfd.revents = 0;

            const int ready = ::poll(&pfd, 1, kSendTimeoutMs);
            if (ready > 0) {
                continue;  // 可写了，重试 send
            }
            if (ready < 0 && errno == EINTR) {
                continue;
            }
            return false;  // 超时或 poll 出错
        }

        return false;  // EPIPE / ECONNRESET 等
    }

    return true;
}

// 每条客户端连接的应用层状态。
//
// 为什么需要 input_buffer？
// TCP 是字节流，一次 recv 可能拿到“半个 JSON”或“三个 JSON 粘在一起”。
// 非阻塞模式下 recv 会一直读到 EAGAIN，我们把读到的字节全部累积起来，
// 再按 '\n' 切割；切不出一整行的部分留在缓冲区，等下一次 EPOLLIN。
struct Connection {
    Connection(int socket_fd, std::uint64_t connection_id)
        : fd(socket_fd), id(connection_id), closed(false) {}

    int fd;
    // 连接的唯一识别码。epoll_event.data 存这个值而不是 fd，
    // 避免 fd 关闭后被新连接重用时，旧事件误作用到新连接。
    std::uint64_t id;
    std::string input_buffer;  // 应用层接收缓冲区（解决粘包/半包）
    std::mutex io_mutex;       // 同一连接的 recv/process/send 串行化
    std::atomic<bool> closed;  // 是否已关闭，防止重复清理
};

// 尽量把 socket 中已到达的数据读掉（不解析）。
//
// 为什么需要这个函数？
//   超限时我们只读到“刚好超过 kMaxMessageBytes”就停止，内核接收缓冲里
//   通常还有未读数据。若直接 shutdown/close，Linux 可能因为有未读数据
//   而送出 RST，把刚刚写入发送缓冲的 -32600 响应一并丢掉。
//   先做一次有限量的 drain，可以大幅降低客户端只看到 connection reset
//   而收不到错误响应的概率。
//
// 注意：
//   - socket 是 O_NONBLOCK，读到 EAGAIN 就返回，不会卡住 worker；
//   - 最多丢弃 kMaxDrainBytes，避免恶意客户端用无穷数据流拖住关闭流程；
//   - 这里只丢弃数据，不解析，也不修改 input_buffer。
void drain_input(const std::shared_ptr<Connection>& connection) {
    if (!connection || connection->closed.load()) {
        return;
    }

    char discard[kReadBufferSize];
    std::size_t drained = 0;

    while (drained < kMaxDrainBytes) {
        const ssize_t n = ::recv(connection->fd, discard, sizeof(discard), 0);

        if (n > 0) {
            drained += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        break;  // EAGAIN / 对端关闭 / 其他错误
    }
}

// =============================================================================
// 线程池：std::thread + std::mutex + std::condition_variable + 任务队列
// =============================================================================
//
// 为什么需要线程池？
//   Reactor 主线程必须一直待在 epoll_wait 里，才能及时接受新连接、
//   发现新数据。JSON 解析、业务计算、send 响应这些工作如果也在主线程做，
//   一个慢请求就会拖住所有连接。
//
//   把这些工作包成 std::function<void()> 放进任务队列，由固定数量的 worker
//   并行处理；避免“每个请求建立一个 thread”的巨大开销。
class ThreadPool {
public:
    explicit ThreadPool(std::size_t thread_count) : stopping_(false) {
        if (thread_count == 0) {
            thread_count = 4;
        }

        workers_.reserve(thread_count);
        for (std::size_t i = 0; i < thread_count; ++i) {
            workers_.push_back(std::thread(&ThreadPool::worker_loop, this));
        }
    }

    // thread / mutex / condition_variable 都不可复制，线程池当然也不能复制。
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    ~ThreadPool() { shutdown(); }

    // 把任务放进队列，并唤醒一个 worker。
    // 返回 false 表示线程池正在关闭，任务没有被接受。
    bool enqueue(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (stopping_) {
                return false;
            }
            tasks_.push(std::move(task));
        }
        condition_.notify_one();
        return true;
    }

    // 停止接收新任务，唤醒并 join 所有 worker。
    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (stopping_) {
                return;
            }
            stopping_ = true;
        }
        condition_.notify_all();

        for (std::size_t i = 0; i < workers_.size(); ++i) {
            if (workers_[i].joinable()) {
                workers_[i].join();
            }
        }
        workers_.clear();
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> task;

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);

                // 没有任务时睡眠，避免忙等浪费 CPU。
                // 被唤醒的条件：有新任务，或线程池要关闭。
                condition_.wait(lock, [this] {
                    return stopping_ || !tasks_.empty();
                });

                // 关闭且任务已清空，worker 可以退出。
                // 若关闭时还有任务，会先把剩下的任务做完再退出。
                if (stopping_ && tasks_.empty()) {
                    return;
                }

                task = std::move(tasks_.front());
                tasks_.pop();
            }

            // 任务在锁外执行，让多个 worker 真正并行。
            try {
                task();
            } catch (const std::exception& exception) {
                // worker 线程里逃出异常会直接 std::terminate，必须兜底。
                log_error("[worker] 未捕获异常：" +
                          std::string(exception.what()));
            } catch (...) {
                log_error("[worker] 未捕获的未知异常");
            }
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    bool stopping_;
};

// 从请求中提取 id。若 id 缺失或类型不对，返回 JSON null。
// 错误响应也尽量带回可识别的 id，方便客户端把请求和响应配对。
json extract_id(const json& request) {
    if (!request.is_object()) {
        return nullptr;
    }

    const json::const_iterator id_it = request.find("id");
    if (id_it == request.end()) {
        return nullptr;
    }

    if (id_it->is_string() || id_it->is_number_integer() ||
        id_it->is_number_unsigned()) {
        return *id_it;
    }

    return nullptr;
}

// 验证请求的基本结构。
// params 约定为数组（JSON-RPC 2.0 子集）；若需要具名参数，可扩展为 object。
bool validate_request(const json& request, std::string& error_reason) {
    if (!request.is_object()) {
        error_reason = "request must be a JSON object";
        return false;
    }

    const json::const_iterator version_it = request.find("jsonrpc");
    if (version_it == request.end() || !version_it->is_string() ||
        version_it->get<std::string>() != "2.0") {
        error_reason = "field 'jsonrpc' must be the string '2.0'";
        return false;
    }

    const json::const_iterator id_it = request.find("id");
    if (id_it == request.end() ||
        !(id_it->is_string() || id_it->is_number_integer() ||
          id_it->is_number_unsigned())) {
        error_reason = "field 'id' must be a string or an integer";
        return false;
    }

    const json::const_iterator method_it = request.find("method");
    if (method_it == request.end() || !method_it->is_string() ||
        method_it->get<std::string>().empty()) {
        error_reason = "field 'method' must be a non-empty string";
        return false;
    }

    const json::const_iterator params_it = request.find("params");
    if (params_it == request.end() || !params_it->is_array()) {
        error_reason = "field 'params' must be an array";
        return false;
    }

    // meta 是选填字段；如果提供，则必须是 object。
    const json::const_iterator meta_it = request.find("meta");
    if (meta_it != request.end() && !meta_it->is_object()) {
        error_reason = "optional field 'meta' must be an object";
        return false;
    }

    return true;
}

// 建立错误响应。
// 统一保留 result 与 error 两个字段，让客户端每次都能用同一套结构解析。
json make_error_response(const json& id,
                         int code,
                         const std::string& message,
                         const json& data) {
    json response;
    response["jsonrpc"] = "2.0";
    response["id"] = id;
    response["result"] = nullptr;
    response["error"]["code"] = code;
    response["error"]["message"] = message;
    response["error"]["data"] = data;
    return response;
}

// 建立成功响应。
// id     ：原样带回请求的 id，让客户端能把请求和响应配对。
// result ：业务函数真正的返回值（例如 math.add 算出的 42）。
json make_success_response(const json& id, const json& result) {
    json response;
    response["jsonrpc"] = "2.0";
    response["id"] = id;
    response["result"] = result;
    response["error"] = nullptr;
    return response;
}

// =============================================================================
// 业务函数 + 路由表
// =============================================================================

// 业务函数执行失败时抛出这个异常对象。
//
// 为什么用异常？
// 业务函数的签名被固定为 json(const json&)，没有多余的返回值可以表达“失败”。
// 用异常可以把错误码、错误消息和附加数据一起带回 dispatch，
// 由 dispatch 统一转成 JSON-RPC 错误响应；业务函数本身不必关心响应格式。
class RpcException : public std::runtime_error {
public:
    RpcException(int code, const std::string& message, const json& data = nullptr)
        : std::runtime_error(message), code_(code), data_(data) {}

    int code() const { return code_; }
    const json& data() const { return data_; }

private:
    int code_;
    json data_;
};

// 真正的业务函数：计算 params[0] + params[1]。
//
// 请求示例：{"method":"math.add","params":[12,30]}
// 返回值    ：{"result":42}
//
// 注意：这里只负责“业务计算”和“参数校验”，
// 不负责拼接 jsonrpc/id/error，那些由 dispatch_request 统一处理。
json math_add(const json& params) {
    // 1. 参数数量校验：math.add 必须恰好收到两个数字。
    if (!params.is_array() || params.size() != 2) {
        throw RpcException(kInvalidParams, "Invalid params",
                           "math.add expects exactly two numbers, e.g. [12, 30]");
    }

    const json& lhs = params.at(0);
    const json& rhs = params.at(1);

    // 2. 参数类型校验：两个都必须是 JSON number（整数或浮点数）。
    if (!lhs.is_number() || !rhs.is_number()) {
        throw RpcException(kInvalidParams, "Invalid params",
                           "both params[0] and params[1] must be numbers");
    }

    // 3. 计算结果。整数与整数相加时保留整数类型，
    //    避免 12 + 30 被输出成 42.0；只要有一个是浮点数就用 double。
    json output;
    if (lhs.is_number_integer() && rhs.is_number_integer()) {
        const long long l = lhs.get<long long>();
        const long long r = rhs.get<long long>();

        // 有符号整数相加溢出是未定义行为，必须先做范围检查再相加。
        if ((r > 0 && l > LLONG_MAX - r) ||
            (r < 0 && l < LLONG_MIN - r)) {
            throw RpcException(kInvalidParams, "Invalid params",
                               "integer overflow");
        }

        output["result"] = l + r;
    } else {
        output["result"] = lhs.get<double>() + rhs.get<double>();
    }

    if (g_verbose) {
        log_line("  [math.add] " + lhs.dump() + " + " + rhs.dump() +
                 " = " + output.at("result").dump());
    }
    return output;
}

// 路由表中每个“处理器”的类型：输入 params，输出业务结果。
// std::function 可以存放普通函数、lambda、成员函数绑定等，后续扩展很方便。
using RpcHandler = std::function<json(const json&)>;

// 路由表：方法名（例如 "math.add"）-> 处理器。
// 完整类型就是 std::unordered_map<std::string, std::function<json(const json&)>>，
// 这里用 RpcHandler 别名只是为了让函数签名更短、可读性更好。
using RpcRouter = std::unordered_map<std::string, RpcHandler>;

// 注册方法的小封装。
// 单独写成函数的好处：以后新增方法时只要多调用一行，
// 而且注册日志可以统一在这里打印。
void register_method(RpcRouter& router,
                     const std::string& method,
                     RpcHandler handler) {
    router[method] = std::move(handler);
    log_line("已注册 RPC 方法：" + method);
}

// 服务端启动时注册所有内置方法。
void register_builtin_methods(RpcRouter& router) {
    register_method(router, "math.add", math_add);
    // 以后新增业务函数时，只需在这里再加一行：
    // register_method(router, "math.sub", math_sub);
}

// 打印收到的请求。默认关闭：高并发压测时，日志本身会成为瓶颈，
// 也可能把终端刷爆。设置环境变量 RPC_VERBOSE=1 后才会逐条输出。
// 所有日志都经过 g_log_mutex，避免多个 worker 的输出互相穿插。
void print_request(const json& request) {
    if (!g_verbose) {
        return;
    }

    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cout << "[request] id=" << request.at("id").dump()
              << " method=" << request.at("method").get<std::string>()
              << " params=" << request.at("params").dump() << "\n";
}

// 方法分派：校验通过后按 method 查路由表，执行业务并转成响应。
//
// 流程：
//   1. 取出请求中的 method 和 params（validate_request 已确保这两个字段存在）。
//   2. 用 method 在路由表中查找处理器。
//   3. 找不到 -> 返回 -32601 Method not found。
//   4. 找到   -> 执行处理器，把返回值包成成功响应。
//   5. 处理器抛出 RpcException（例如参数错误）-> 转成对应错误码。
//   6. 其他未预期异常 -> 返回 -32603，保证服务端不会因为单个请求崩溃。
json dispatch_request(const RpcRouter& router, const json& request) {
    const json id = extract_id(request);
    const std::string method = request.at("method").get<std::string>();
    const json& params = request.at("params");

    const RpcRouter::const_iterator handler_it = router.find(method);

    if (handler_it == router.end()) {
        // 路由表中没有这个方法：返回 -32601。
        // data 中带上方法名，方便客户端发现是拼写错误还是服务端未实现。
        if (g_verbose) {
            log_line("  [dispatch] 找不到方法：" + method);
        }

        json error_data;
        error_data["method"] = method;
        return make_error_response(id, kMethodNotFound, "Method not found",
                                   error_data);
    }

    try {
        // 执行真正的业务函数（例如 math.add）。
        const json handler_output = handler_it->second(params);

        // 统一约定：业务函数用 {"result": 真正的返回值} 包裹结果。
        // 这里把 result 字段取出来，放进 JSON-RPC 响应顶层的 result，
        // 所以客户端看到的是 "result": 42，而不是嵌套的 {"result": {"result": 42}}。
        // 如果某个处理器直接返回非对象值，也原样放进 result。
        const json result =
            handler_output.is_object() && handler_output.contains("result")
                ? handler_output.at("result")
                : handler_output;

        return make_success_response(id, result);
    } catch (const RpcException& exception) {
        // 业务函数主动抛出的可预期错误（例如 -32602 参数错误）。
        if (g_verbose) {
            log_line("  [dispatch] 业务错误：" + std::string(exception.what()));
        }
        return make_error_response(id, exception.code(), exception.what(),
                                   exception.data());
    } catch (const std::exception& exception) {
        // 未预期的异常（例如类型转换失败），一律回 -32603。
        log_error("  [dispatch] 内部错误：" + std::string(exception.what()));
        return make_error_response(id, kInternalError, "Internal error",
                                   exception.what());
    }
}

// =============================================================================
// RpcServer：Reactor（主线程）+ 线程池（worker）
// =============================================================================
//
// 职责划分：
//   主线程：epoll_wait -> accept / 把已就绪连接的“读取 + 处理”任务丢给池
//   worker  ：recv 循环读到 EAGAIN -> 按 '\n' 切消息 -> json::parse
//             -> dispatch_request -> send 响应 -> 重新注册 fd（rearm）
//
// 为什么使用 EPOLLONESHOT？
//   只用 EPOLLET 时，同一个 fd 可能在多个 worker 之间被同时处理，
//   造成 input_buffer 的数据竞争和响应顺序混乱。
//   EPOLLONESHOT 让每个 fd 一次只被唤醒一次；worker 处理完再 EPOLL_CTL_MOD。
//   处理期间到达的新数据不会丢：重新注册时内核会再次报告可读。
class RpcServer {
public:
    RpcServer(int port, std::size_t thread_count)
        : port_(port),
          listen_fd_(-1),
          epoll_fd_(-1),
          router_(),
          next_connection_id_(1),
          pool_(thread_count) {
        // 路由表只在启动阶段写入，之后所有 worker 只读，因此不需要加锁。
        register_builtin_methods(router_);
    }

    ~RpcServer() {
        // 先停掉 worker，确保没有线程还在用连接对象，再关闭 fd。
        pool_.shutdown();

        close_socket(epoll_fd_);
        close_socket(listen_fd_);

        for (std::unordered_map<std::uint64_t,
                                std::shared_ptr<Connection>>::iterator it =
                 connections_.begin();
             it != connections_.end(); ++it) {
            close_socket(it->second->fd);
        }
        connections_.clear();
    }

    bool init() {
        return setup_listen_socket() && setup_epoll();
    }

    void run() {
        std::vector<epoll_event> events(kMaxEvents);

        while (!g_stop_requested) {
            const int ready_count =
                ::epoll_wait(epoll_fd_, events.data(),
                             static_cast<int>(events.size()),
                             kEpollWaitTimeoutMs);

            if (ready_count < 0) {
                if (errno == EINTR) {
                    continue;
                }
                log_error("epoll_wait 失败：" +
                          std::string(std::strerror(errno)));
                return;
            }

            for (int i = 0; i < ready_count; ++i) {
                const std::uint64_t token = events[i].data.u64;
                const std::uint32_t event_mask = events[i].events;

                // 监听 socket：边缘触发，必须循环 accept 到 EAGAIN。
                if (token == kListenToken) {
                    accept_new_connections();
                    continue;
                }

                const std::shared_ptr<Connection> connection =
                    find_connection(token);
                if (!connection) {
                    continue;  // 可能是刚关闭的连接留下的旧事件
                }

                // EPOLLIN      : 有新数据
                // EPOLLRDHUP   : 对端半关闭，还要把缓冲区剩余消息处理完
                // EPOLLERR/HUP : 连接异常，也交给 worker 走统一的读取/清理流程
                if ((event_mask &
                     (EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP)) != 0) {
                    const bool force_close =
                        (event_mask & (EPOLLERR | EPOLLHUP)) != 0;

                    // worker 内任何未捕获异常都会让 EPOLLONESHOT 的 fd
                    // 永久不再收到事件，所以这里必须做最后一层兜底：
                    // 记录错误并关闭连接，确保 fd 和 map 条目都被清理。
                    const bool accepted =
                        pool_.enqueue([this, connection, force_close] {
                            try {
                                handle_client(connection, force_close);
                            } catch (const std::exception& exception) {
                                log_error(
                                    "[worker] handle_client 异常：" +
                                    std::string(exception.what()));
                                close_connection(connection);
                            } catch (...) {
                                log_error("[worker] handle_client 未知异常");
                                close_connection(connection);
                            }
                        });

                    if (!accepted) {
                        // 正常运行时只有停机阶段会走到这里；避免留下
                        // 永远不会再被处理的 fd。
                        log_error(
                            "[pool] 任务队列已关闭，关闭连接 fd=" +
                            std::to_string(connection->fd));
                        close_connection(connection);
                    }
                }
            }
        }

        // 主循环因停止信号退出，稍后由 main() 返回、触发 RpcServer 析构，
        // 统一 join worker 并关闭连接。
        log_line("收到停止信号，开始关闭连接与 worker…");
    }

private:
    // 建立监听 socket：socket -> SO_REUSEADDR -> 非阻塞 -> bind -> listen。
    bool setup_listen_socket() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            log_error("建立 socket 失败：" +
                      std::string(std::strerror(errno)));
            return false;
        }

        // 允许程序重启后立即重用 8080 端口，避免 TIME_WAIT 造成 bind 失败。
        int reuse = 1;
        if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse,
                         sizeof(reuse)) < 0) {
            log_error("设置 SO_REUSEADDR 失败：" +
                      std::string(std::strerror(errno)));
            close_socket(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        // 监听 socket 也必须非阻塞，配合 ET 的 accept 循环。
        if (!set_nonblocking(listen_fd_)) {
            log_error("设置监听 socket 非阻塞失败：" +
                      std::string(std::strerror(errno)));
            close_socket(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        sockaddr_in server_address;
        std::memset(&server_address, 0, sizeof(server_address));
        server_address.sin_family = AF_INET;
        server_address.sin_addr.s_addr = htonl(INADDR_ANY);  // 监听所有网卡
        server_address.sin_port = htons(static_cast<std::uint16_t>(port_));

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&server_address),
                   sizeof(server_address)) < 0) {
            log_error("绑定 " + std::to_string(port_) + " 端口失败：" +
                      std::string(std::strerror(errno)));
            close_socket(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        if (::listen(listen_fd_, kListenBacklog) < 0) {
            log_error("监听失败：" + std::string(std::strerror(errno)));
            close_socket(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        return true;
    }

    // 建立 epoll 实例，并把监听 socket 注册进去。
    bool setup_epoll() {
        // EPOLL_CLOEXEC：exec 后自动关闭，避免 fd 泄漏到子进程。
        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) {
            log_error("epoll_create1 失败：" +
                      std::string(std::strerror(errno)));
            close_socket(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        epoll_event event;
        std::memset(&event, 0, sizeof(event));
        event.events = EPOLLIN | EPOLLET;  // 监听 fd 不需要 ONESHOT
        event.data.u64 = kListenToken;

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &event) < 0) {
            log_error("epoll_ctl(ADD listen) 失败：" +
                      std::string(std::strerror(errno)));
            close_socket(epoll_fd_);
            epoll_fd_ = -1;
            close_socket(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        return true;
    }

    // 边缘触发的 accept：必须一直 accept 到 EAGAIN，否则会漏掉连接。
    void accept_new_connections() {
        while (true) {
            sockaddr_in client_address;
            socklen_t client_address_length = sizeof(client_address);

            const int client_fd =
                ::accept(listen_fd_,
                         reinterpret_cast<sockaddr*>(&client_address),
                         &client_address_length);

            if (client_fd < 0) {
                const int error_code = errno;

                // ECONNABORTED：客户端在 accept 完成前断开，这不是致命错误，
                // 继续 accept 下一条连接即可。
                if (error_code == EINTR || error_code == ECONNABORTED) {
                    continue;
                }
                // EAGAIN/EWOULDBLOCK：这批连接已经 accept 完，ET 的正确退出方式。
                if (error_code == EAGAIN || error_code == EWOULDBLOCK) {
                    break;
                }
                // fd 耗尽：记录明确提示，方便用 ulimit -n 排查。
                if (error_code == EMFILE || error_code == ENFILE) {
                    log_error("accept 失败：fd 耗尽，请提高 ulimit -n（" +
                              std::string(std::strerror(error_code)) + "）");
                    break;
                }
                log_error("accept 失败：" +
                          std::string(std::strerror(error_code)));
                break;
            }

            if (!set_nonblocking(client_fd)) {
                log_error("设置客户端 socket 非阻塞失败，关闭该连接");
                close_socket(client_fd);
                continue;
            }

            // RPC 请求和响应都很小，关闭 Nagle 算法避免额外的延迟。
            // 设置失败不影响功能，忽略即可。
            int no_delay = 1;
            ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &no_delay,
                         sizeof(no_delay));

            const std::uint64_t connection_id =
                next_connection_id_.fetch_add(1, std::memory_order_relaxed);
            const std::shared_ptr<Connection> connection =
                std::make_shared<Connection>(client_fd, connection_id);

            epoll_event event;
            std::memset(&event, 0, sizeof(event));
            event.events = kClientEvents;
            event.data.u64 = connection_id;

            if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &event) < 0) {
                log_error("epoll_ctl(ADD client) 失败：" +
                          std::string(std::strerror(errno)));
                close_socket(client_fd);
                continue;
            }

            {
                std::lock_guard<std::mutex> lock(connections_mutex_);
                connections_[connection_id] = connection;
            }

            if (g_verbose) {
                char client_ip[INET_ADDRSTRLEN] = {0};
                ::inet_ntop(AF_INET, &client_address.sin_addr, client_ip,
                            sizeof(client_ip));
                log_line("[connection] accepted " + std::string(client_ip) +
                         ":" +
                         std::to_string(ntohs(client_address.sin_port)) +
                         " fd=" + std::to_string(client_fd) +
                         " id=" + std::to_string(connection_id));
            }
        }
    }

    // worker 的核心：读取 -> 切消息 -> 逐条处理 -> rearm 或关闭。
    // force_close 表示 epoll 回报了 EPOLLERR/EPOLLHUP：
    // 仍先把缓冲区里完整的请求处理完，之后强制关闭，避免反复收到同一个异常事件。
    void handle_client(const std::shared_ptr<Connection>& connection,
                       bool force_close) {
        // 同一连接的读取、解析、发送全部串行化。
        // EPOLLONESHOT 已经保证一次只有一个 worker 被唤醒，
        // 这把锁主要用来防止“关闭流程”与“读取流程”交叉。
        std::unique_lock<std::mutex> io_lock(connection->io_mutex);

        if (connection->closed.load()) {
            return;
        }

        bool peer_closed = false;
        bool socket_error = false;
        bool input_overflow = false;
        char buffer[kReadBufferSize];

        // 1. 非阻塞 recv 循环：一直读到 EAGAIN，这才是 ET 的正确读法。
        //    若只 recv 一次，边缘触发不会再通知剩下的数据，连接会“卡死”。
        while (true) {
            const ssize_t n = ::recv(connection->fd, buffer, sizeof(buffer), 0);

            if (n > 0) {
                connection->input_buffer.append(
                    buffer, static_cast<std::size_t>(n));

                // 防止单个连接把内存吃光。
                // 注意：这里不能只是 break 后又 rearm。超过上限代表
                // 我们没有把内核接收缓冲读到 EAGAIN，ET 模式下很容易
                // 留下永远不再被通知的数据。统一标记后回 -32600 并关闭。
                if (connection->input_buffer.size() > kMaxMessageBytes) {
                    input_overflow = true;
                    break;
                }
                continue;
            }

            if (n == 0) {
                // 对端关闭写端。注意：缓冲区里可能还有完整请求要处理，
                // 所以先标记，等消息处理完、响应发完再关闭。
                peer_closed = true;
                break;
            }

            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // 这次真的读完了
            }

            socket_error = true;
            break;
        }

        // 2. 超限：不再尝试解析后面的数据，直接回 -32600 并关闭。
        //    先 send 再 drain，尽量避免内核因“有未读数据”在 close 时
        //    发 RST，把刚刚的错误响应丢掉。
        if (input_overflow) {
            json data;
            data["reason"] = "message exceeds 1 MiB";

            if (send_json(connection,
                          make_error_response(nullptr, kInvalidRequest,
                                              "Invalid Request", data))) {
                drain_input(connection);
            }
            close_connection(connection);
            return;
        }

        // 3. 从缓冲区里按 '\n' 切出所有完整消息。
        //    一次 send 可能包含多个请求（粘包），这里全部处理，保证顺序。
        //
        //    这里用 consumed 偏移量扫描，最后只 erase 一次；
        //    若每切一行就 erase(0, n)，大量流水线请求时会反复搬移整个缓冲区。
        std::string line;
        std::size_t consumed = 0;
        while (true) {
            const std::size_t newline_pos =
                connection->input_buffer.find('\n', consumed);
            if (newline_pos == std::string::npos) {
                break;
            }

            line.assign(connection->input_buffer, consumed,
                        newline_pos - consumed);
            consumed = newline_pos + 1;

            // 兼容 Windows 风格的 "\r\n"。
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }

            if (line.empty()) {
                continue;  // 容忍空行（例如心跳）
            }

            if (!process_line(connection, line)) {
                return;  // 发送失败 / 连接已关闭
            }
        }

        if (consumed > 0) {
            connection->input_buffer.erase(0, consumed);
        }

        // 4. 缓冲区里剩下的是“半条消息”，保留到下一次 EPOLLIN。
        //    上面的 input_overflow 已经保证总量不超过上限，这里不必重复检查。
        if (peer_closed || socket_error || force_close) {
            // 半关闭/异常：响应已经在上一段发送完，现在可以安全关闭。
            close_connection(connection);
            return;
        }

        // 5. 处理完毕，重新注册 EPOLLONESHOT，等待下一次可读事件。
        rearm(connection);
    }

    // 处理一条完整的 JSON 消息：解析 -> 验证 -> 分派 -> 发送。
    // 返回 false 表示连接已关闭，调用者应立即停止处理后续消息。
    bool process_line(const std::shared_ptr<Connection>& connection,
                      const std::string& line) {
        json request;

        try {
            request = json::parse(line);
        } catch (const std::exception& exception) {
            if (g_verbose) {
                log_line("[parse] JSON 解析失败：" +
                         std::string(exception.what()));
            }
            return send_json(connection,
                             make_error_response(nullptr, kParseError,
                                                 "Parse error",
                                                 exception.what()));
        }

        std::string validation_error;
        if (!validate_request(request, validation_error)) {
            json data;
            data["reason"] = validation_error;
            return send_json(connection,
                             make_error_response(extract_id(request),
                                                 kInvalidRequest,
                                                 "Invalid Request", data));
        }

        print_request(request);

        json response;
        try {
            // 真正的业务分派：路由表查找 + 执行 math_add 等处理器。
            // 不同连接的这一部会在多个 worker 上并行执行。
            response = dispatch_request(router_, request);
        } catch (const std::exception& exception) {
            // dispatch_request 内部已经有 try/catch，这里是额外保险：
            // worker 线程里抛出未捕获异常会直接 std::terminate。
            response = make_error_response(extract_id(request),
                                           kInternalError, "Internal error",
                                           exception.what());
        }

        return send_json(connection, response);
    }

    bool send_json(const std::shared_ptr<Connection>& connection,
                   const json& response) {
        // 连接可能已被其他路径关闭，先检查 closed，
        // 避免对已关闭的 fd 做无效 send（EBADF）并触发多余的 close_connection。
        if (connection->closed.load()) {
            return false;
        }

        // dump() 生成紧凑单行 JSON；绝不能用 dump(4)，
        // 否则 pretty-print 的换行会破坏“一行一个消息”的边界。
        const std::string wire = response.dump() + "\n";

        if (!send_all_nonblocking(connection->fd, wire)) {
            if (g_verbose) {
                log_line("[send] 发送失败，关闭连接 fd=" +
                         std::to_string(connection->fd));
            }
            close_connection(connection);
            return false;
        }
        return true;
    }

    // EPOLLONESHOT 的 fd 处理完后必须重新注册，否则再也收不到事件。
    // 若 worker 处理期间有新数据到达，这次 MOD 会让内核再次报告可读，
    // 所以不会出现“数据到了但没人处理”的丢事件问题。
    void rearm(const std::shared_ptr<Connection>& connection) {
        if (connection->closed.load()) {
            return;
        }

        epoll_event event;
        std::memset(&event, 0, sizeof(event));
        event.events = kClientEvents;
        event.data.u64 = connection->id;

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, connection->fd, &event) < 0) {
            if (g_verbose) {
                log_line("[epoll] rearm 失败，关闭连接 fd=" +
                         std::to_string(connection->fd));
            }
            close_connection(connection);
        }
    }

    // 关闭连接：保证只执行一次；map 以 connection id 为 key，
    // 即使 fd 立刻被新连接重用，也不会误删新连接。
    void close_connection(const std::shared_ptr<Connection>& connection) {
        bool expected = false;
        if (!connection->closed.compare_exchange_strong(expected, true)) {
            return;  // 已经被其他路径关闭
        }

        const int fd = connection->fd;
        const std::uint64_t connection_id = connection->id;

        // 先从 map 移除，再 close fd。
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            std::unordered_map<std::uint64_t,
                               std::shared_ptr<Connection>>::iterator it =
                connections_.find(connection_id);
            if (it != connections_.end() &&
                it->second.get() == connection.get()) {
                connections_.erase(it);
            }
        }

        if (epoll_fd_ >= 0) {
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);

        if (g_verbose) {
            log_line("[connection] closed fd=" + std::to_string(fd));
        }
    }

    // 主线程收到事件后，用 connection id 找回对应的连接对象。
    std::shared_ptr<Connection> find_connection(std::uint64_t connection_id) {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        std::unordered_map<std::uint64_t,
                           std::shared_ptr<Connection>>::iterator it =
            connections_.find(connection_id);
        if (it == connections_.end()) {
            return std::shared_ptr<Connection>();
        }
        return it->second;
    }

    int port_;
    int listen_fd_;
    int epoll_fd_;
    RpcRouter router_;
    std::mutex connections_mutex_;
    std::unordered_map<std::uint64_t, std::shared_ptr<Connection>>
        connections_;
    std::atomic<std::uint64_t> next_connection_id_;
    // pool_ 最后声明：析构时最先销毁，确保 worker 先 join，
    // 再清理 connections_ 等其他成员。
    ThreadPool pool_;
};

}  // namespace

int main(int argc, char* argv[]) {
    // 客户端提前断开时，向已关闭的 socket 发送数据可能触发 SIGPIPE。
    // 忽略该信号，让 send() 返回错误（EPIPE），由程序自行清理连接。
    std::signal(SIGPIPE, SIG_IGN);

    // Ctrl+C（SIGINT）/ kill（SIGTERM）时不是直接终止进程，而是设置
    // g_stop_requested，让 Reactor 主循环退出并走正常析构。
    std::signal(SIGINT, handle_stop_signal);
    std::signal(SIGTERM, handle_stop_signal);

    // 工作线程数量：默认使用 CPU 核心数，也可用命令行参数指定。
    // 例如 ./rpc_server 8
    std::size_t thread_count = std::thread::hardware_concurrency();
    if (thread_count == 0) {
        thread_count = 4;
    }
    if (thread_count > kMaxWorkerThreads) {
        thread_count = kMaxWorkerThreads;
    }

    if (argc >= 2) {
        char* end = nullptr;
        const unsigned long value = std::strtoul(argv[1], &end, 10);
        if (end == argv[1] || *end != '\0' || value == 0 ||
            value > kMaxWorkerThreads) {
            std::cerr << "用法：" << argv[0] << " [thread_count(1-"
                      << kMaxWorkerThreads << ")]\n";
            return 1;
        }
        thread_count = static_cast<std::size_t>(value);
    }

    if (argc > 2) {
        std::cerr << "用法：" << argv[0] << " [thread_count]\n";
        return 1;
    }

    // 默认不打印每个请求，避免压测时日志成为瓶颈。
    // 需要观察协议细节时：RPC_VERBOSE=1 ./rpc_server
    const char* verbose = std::getenv("RPC_VERBOSE");
    g_verbose = (verbose != nullptr && std::strcmp(verbose, "1") == 0);

    // 路由表在 RpcServer 构造时注册；之后所有 worker 只读，无需加锁。
    RpcServer server(kPort, thread_count);

    if (!server.init()) {
        return 1;
    }

    std::cout << "RPC 服务端已启动（epoll ET + 线程池）\n";
    std::cout << "监听：0.0.0.0:" << kPort << "\n";
    std::cout << "工作线程：" << thread_count << "\n";
    std::cout << "协议：紧凑 JSON + '\\n'；同一连接可连续发送多个请求。\n";
    std::cout << "提示：设置 RPC_VERBOSE=1 可打印每个请求。\n";
    std::cout.flush();

    // 进入 Reactor 主循环：主线程只做 epoll_wait / accept / 分发，
    // 不会阻塞在任何一个客户端的 recv 或业务计算上。
    server.run();
    return 0;
}
